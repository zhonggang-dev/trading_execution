// walletdrawdown samples one Polymarket wallet's equity (on-chain pUSD plus
// Data API position marks), stores it and posts a Feishu card when the drop
// from the trailing-window peak exceeds a threshold. It is meant to run from
// a scheduler every slot (default 10 minutes). It never trades.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "walletdrawdown:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, getenv func(string) string, out io.Writer) error {
	flags := flag.NewFlagSet("walletdrawdown", flag.ContinueOnError)
	dbEnv := flags.String("database-env", "TRADING_EXECUTION_DATABASE_URL", "environment variable containing the connection; never supply a URL on the command line")
	dryRun := flags.Bool("dry-run", false, "sample and evaluate only; write nothing and send nothing")
	timeout := flags.Duration("timeout", 3*time.Minute, "total bounded run duration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 || *timeout > 15*time.Minute {
		return fmt.Errorf("invalid arguments")
	}
	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	target := pipelinemonitor.FeishuTarget{
		WebhookURL: getenv("WALLET_DRAWDOWN_FEISHU_WEBHOOK_URL"), Secret: getenv("WALLET_DRAWDOWN_FEISHU_SECRET"),
		AppID: getenv("WALLET_DRAWDOWN_FEISHU_APP_ID"), AppSecret: getenv("WALLET_DRAWDOWN_FEISHU_APP_SECRET"),
		ChatID: getenv("WALLET_DRAWDOWN_FEISHU_CHAT_ID"),
	}
	var notifier walletdrawdown.Notifier
	if target.WebhookURL != "" || target.AppID != "" || target.AppSecret != "" || target.ChatID != "" {
		sender, err := pipelinemonitor.NewFeishuSender(target, &http.Client{Timeout: 10 * time.Second})
		if err != nil {
			return err
		}
		notifier = sender
	}
	rpcURL := strings.TrimSpace(getenv("POLYGON_RPC_URL"))
	if rpcURL == "" {
		return fmt.Errorf("POLYGON_RPC_URL is required")
	}
	dataAPI := strings.TrimSpace(getenv("POLYMARKET_DATA_API_URL"))
	if dataAPI == "" {
		dataAPI = "https://data-api.polymarket.com"
	}
	dsn := getenv(*dbEnv)
	if dsn == "" {
		return fmt.Errorf("database environment %s is required", *dbEnv)
	}
	dbConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	dbConfig.RuntimeParams["application_name"] = "wallet-drawdown"
	dbConfig.RuntimeParams["search_path"] = "public,pg_temp"
	db := stdlib.OpenDB(*dbConfig)
	defer db.Close()
	db.SetMaxOpenConns(2)
	store, err := postgres.NewWalletDrawdownStore(db)
	if err != nil {
		return err
	}
	httpClient := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	positions, err := polymarketdata.NewPositionClient(polymarketdata.PositionClientParams{BaseURL: dataAPI, HTTPClient: httpClient})
	if err != nil {
		return err
	}
	cash, err := evmrpc.NewERC20BalanceClient(evmrpc.ERC20BalanceParams{
		RPCURL: rpcURL, TokenAddress: polymarket.PUSDAddress, Asset: "pUSD", Decimals: 6, HTTPClient: httpClient,
	})
	if err != nil {
		return err
	}
	chain, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{RPCURL: rpcURL, RequiredConfirmations: 1, HTTPClient: httpClient})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	report, runErr := walletdrawdown.Run(ctx, cfg, store, sources{cash: cash, positions: positions, chain: chain}, notifier, time.Now(), *dryRun)
	if report.Sample != nil {
		// Keep the printed report compact; the full position list is in the table.
		compact := *report.Sample
		compact.Positions = nil
		report.Sample = &compact
	}
	if err := json.NewEncoder(out).Encode(report); err != nil {
		return err
	}
	return runErr
}

func loadConfig(getenv func(string) string) (walletdrawdown.Config, error) {
	cfg := walletdrawdown.Config{AccountID: "main", Threshold: big.NewRat(50, 1), RenotifyStep: big.NewRat(20, 1), Window: time.Hour, Slot: 10 * time.Minute}
	if v := strings.TrimSpace(getenv("WALLET_DRAWDOWN_ACCOUNT")); v != "" {
		cfg.AccountID = v
	}
	for _, item := range []struct {
		name string
		dst  **big.Rat
	}{{"WALLET_DRAWDOWN_THRESHOLD_USD", &cfg.Threshold}, {"WALLET_DRAWDOWN_RENOTIFY_STEP_USD", &cfg.RenotifyStep}} {
		if v := strings.TrimSpace(getenv(item.name)); v != "" {
			value, ok := new(big.Rat).SetString(v)
			if !ok {
				return cfg, fmt.Errorf("%s must be a decimal", item.name)
			}
			*item.dst = value
		}
	}
	for _, item := range []struct {
		name string
		dst  *time.Duration
	}{{"WALLET_DRAWDOWN_WINDOW", &cfg.Window}, {"WALLET_DRAWDOWN_SLOT", &cfg.Slot}} {
		if v := strings.TrimSpace(getenv(item.name)); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return cfg, fmt.Errorf("%s must be a duration", item.name)
			}
			*item.dst = d
		}
	}
	return cfg, cfg.Validate()
}

type sources struct {
	cash      *evmrpc.ERC20BalanceClient
	positions *polymarketdata.PositionClient
	chain     *evmrpc.OrderFilledEvidenceReader
}

func (s sources) Cash(ctx context.Context, wallet string) (domain.ExternalBalance, error) {
	return s.cash.GetExternalBalance(ctx, wallet, "pUSD")
}

func (s sources) Positions(ctx context.Context, wallet string) ([]polymarketdata.PositionMark, error) {
	return s.positions.ListPositionMarks(ctx, wallet)
}

func (s sources) ChainShares(ctx context.Context, wallet, tokenID string) (domain.Decimal, error) {
	return s.chain.GetKnownPositionBalance(ctx, wallet, tokenID)
}

// chaincashinit opts one execution account into the on-chain pUSD cash ledger.
//
// It is a dry run by default: every RPC read, ledger check, cursor insert,
// transfer insert, and reward/deposit credit runs inside one transaction that
// is rolled back, and the resulting evidence is printed. --execute commits it.
//
// The start block must be final and balanceOf(wallet) at that block must equal
// the ledger total exactly, with no ledger cash movement since then. The
// command then catches up offline: it reads every final pUSD Transfer from
// the start block to the current confirmed head (100-block eth_getLogs chunks,
// without the service's per-run budget), proves the logs are complete by
// checking start balance + net transfers = balanceOf at the catch-up block,
// and stores the cursor at that block together with the classified transfers
// and credits. The running service therefore only processes the increment.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/chaincash"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const polygonChainID = uint64(137)

type chainReader interface {
	ChainID(ctx context.Context) (uint64, error)
	LatestBlock(ctx context.Context) (uint64, error)
	BlockHeader(ctx context.Context, blockNumber uint64) (string, time.Time, error)
	BalanceAt(ctx context.Context, walletAddress string, blockNumber uint64) (domain.Decimal, error)
}

type transferReader interface {
	ReadConfirmedTransfers(ctx context.Context, executionAccountID, walletAddress string, fromBlock, toBlock uint64) ([]domain.ChainCashTransfer, error)
}

type cursorInitializer interface {
	InitializeChainCashCursor(ctx context.Context, params postgresadapter.ChainCashCursorInitialization) (postgresadapter.ChainCashCursorInitializationReport, error)
}

type options struct {
	AccountID     string
	Wallet        string
	StartBlock    uint64
	Confirmations uint64
	Actor         string
	Reason        string
	Execute       bool
	Now           func() time.Time
}

type output struct {
	Mode                string                                               `json:"mode"`
	AccountID           string                                               `json:"execution_account_id"`
	Wallet              string                                               `json:"wallet_address"`
	Token               string                                               `json:"token_address"`
	LatestBlock         uint64                                               `json:"latest_block"`
	StartBlock          uint64                                               `json:"start_block"`
	StartHash           string                                               `json:"start_block_hash"`
	StartTime           time.Time                                            `json:"start_block_time"`
	Confirmations       uint64                                               `json:"start_block_confirmations"`
	Required            uint64                                               `json:"required_confirmations"`
	BalanceOf           domain.Decimal                                       `json:"balance_of_at_start_block"`
	CatchUpBlock        uint64                                               `json:"catch_up_block"`
	CatchUpBalanceOf    domain.Decimal                                       `json:"balance_of_at_catch_up_block"`
	CatchUpNetTransfers domain.Decimal                                       `json:"net_transfers_after_start_block"`
	Classifications     map[domain.ChainCashClassification]int               `json:"classifications"`
	Ledger              *postgresadapter.ChainCashCursorInitializationReport `json:"ledger_checks,omitempty"`
	Error               string                                               `json:"error,omitempty"`
}

func main() {
	if err := mainWithArgs(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func mainWithArgs(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("chaincashinit", flag.ContinueOnError)
	var opts options
	var startBlock string
	var timeout time.Duration
	flags.StringVar(&opts.AccountID, "account", "", "execution account id (for example wallet-6)")
	flags.StringVar(&opts.Wallet, "wallet", "", "expected execution wallet address of the account")
	flags.StringVar(&startBlock, "start-block", "", "final start block; default latest minus the confirmation depth")
	flags.StringVar(&opts.Actor, "actor", "", "accountable operator identity")
	flags.StringVar(&opts.Reason, "reason", "", "approved reason for enabling the chain cash ledger")
	flags.BoolVar(&opts.Execute, "execute", false, "commit the cursor and catch-up; without it everything runs and rolls back")
	flags.DurationVar(&timeout, "timeout", 10*time.Minute, "overall deadline for RPC reads and the database transaction")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if startBlock != "" {
		value, err := strconv.ParseUint(startBlock, 10, 64)
		if err != nil || value == 0 {
			return fmt.Errorf("--start-block must be a positive block number")
		}
		opts.StartBlock = value
	}
	confirmations, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("POLYGON_ORDER_FILLED_CONFIRMATIONS")), 10, 64)
	if err != nil || confirmations == 0 {
		return fmt.Errorf("POLYGON_ORDER_FILLED_CONFIRMATIONS must be a positive integer")
	}
	opts.Confirmations = confirmations
	// Classification must match the running service exactly: a reward read
	// without the configured sender would be stored as UNATTRIBUTED_IN forever.
	rewardSenders, err := chaincash.ParseAddressList(os.Getenv("CHAIN_CASH_REWARD_SENDERS"))
	if err != nil {
		return fmt.Errorf("CHAIN_CASH_REWARD_SENDERS: %w", err)
	}
	if len(rewardSenders) == 0 {
		return fmt.Errorf("CHAIN_CASH_REWARD_SENDERS must be configured before initializing a chain cash cursor")
	}
	depositSources, err := chaincash.ParseDepositSources(os.Getenv("CHAIN_CASH_DEPOSIT_SOURCES"))
	if err != nil {
		return fmt.Errorf("CHAIN_CASH_DEPOSIT_SOURCES: %w", err)
	}
	exchanges := []string{evmrpc.PolygonCTFExchangeV2Address, evmrpc.PolygonNegRiskCTFExchangeV2Address}
	reader, err := evmrpc.NewERC20TransferLogReader(evmrpc.ERC20TransferLogParams{
		RPCURL: os.Getenv("POLYGON_RPC_URL"), TokenAddress: polymarket.PUSDAddress, ExchangeAddresses: exchanges, Decimals: 6,
	})
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.Close()
	store, err := postgresadapter.NewChainCashStore(db)
	if err != nil {
		return err
	}
	classifier, err := chaincash.New(chaincash.Params{
		Logs: reader, Store: store,
		ExchangeAddresses: exchanges,
		RewardSenders:     rewardSenders, DepositSources: depositSources, Confirmations: confirmations,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return run(ctx, opts, reader, classifier, store, stdout)
}

func run(ctx context.Context, opts options, chain chainReader, transfers transferReader, store cursorInitializer, stdout io.Writer) error {
	opts.AccountID = strings.TrimSpace(opts.AccountID)
	opts.Wallet = strings.ToLower(strings.TrimSpace(opts.Wallet))
	if opts.AccountID == "" || opts.Wallet == "" || strings.TrimSpace(opts.Actor) == "" ||
		strings.TrimSpace(opts.Reason) == "" || opts.Confirmations == 0 {
		return fmt.Errorf("--account, --wallet, --actor, --reason, and a positive confirmation depth are required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	result := output{
		Mode: "dry-run", AccountID: opts.AccountID, Wallet: opts.Wallet,
		Token: strings.ToLower(polymarket.PUSDAddress), Required: opts.Confirmations,
		Classifications: map[domain.ChainCashClassification]int{},
	}
	if opts.Execute {
		result.Mode = "execute"
	}
	err := collect(ctx, opts, chain, transfers, store, &result)
	if err != nil {
		result.Error = err.Error()
	}
	encoded, encodeErr := json.MarshalIndent(result, "", "  ")
	if encodeErr != nil {
		return encodeErr
	}
	if _, writeErr := fmt.Fprintln(stdout, string(encoded)); writeErr != nil {
		return writeErr
	}
	return err
}

func collect(
	ctx context.Context, opts options, chain chainReader, reader transferReader, store cursorInitializer, result *output,
) error {
	chainID, err := chain.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("read chain ID: %w", err)
	}
	if chainID != polygonChainID {
		return fmt.Errorf("RPC chain ID is %d; require Polygon %d", chainID, polygonChainID)
	}
	latest, err := chain.LatestBlock(ctx)
	if err != nil {
		return fmt.Errorf("read latest block: %w", err)
	}
	result.LatestBlock = latest
	if latest <= opts.Confirmations {
		return fmt.Errorf("latest block %d is below the confirmation depth", latest)
	}
	// Same finality rule as the service: blocks at or below latest-depth.
	result.CatchUpBlock = latest - opts.Confirmations
	result.StartBlock = opts.StartBlock
	if result.StartBlock == 0 {
		result.StartBlock = result.CatchUpBlock
	}
	if result.StartBlock > result.CatchUpBlock {
		return fmt.Errorf("start block %d is not final: latest %d, require %d confirmations",
			result.StartBlock, latest, opts.Confirmations)
	}
	result.Confirmations = latest - result.StartBlock + 1
	if result.StartHash, result.StartTime, err = chain.BlockHeader(ctx, result.StartBlock); err != nil {
		return fmt.Errorf("read start block: %w", err)
	}
	if result.BalanceOf, err = chain.BalanceAt(ctx, opts.Wallet, result.StartBlock); err != nil {
		return fmt.Errorf("read balanceOf at start block: %w", err)
	}
	var caughtUp []domain.ChainCashTransfer
	if result.CatchUpBlock > result.StartBlock {
		caughtUp, err = reader.ReadConfirmedTransfers(ctx, opts.AccountID, opts.Wallet, result.StartBlock+1, result.CatchUpBlock)
		if err != nil {
			return fmt.Errorf("read pUSD transfers (%d, %d]: %w", result.StartBlock, result.CatchUpBlock, err)
		}
	}
	for _, transfer := range caughtUp {
		result.Classifications[transfer.Classification]++
	}
	if result.CatchUpNetTransfers, err = chaincash.SumSigned(caughtUp); err != nil {
		return err
	}
	if result.CatchUpBalanceOf, err = chain.BalanceAt(ctx, opts.Wallet, result.CatchUpBlock); err != nil {
		return fmt.Errorf("read balanceOf at catch-up block: %w", err)
	}
	// Pure chain arithmetic, independent of the ledger: a lagging or truncated
	// eth_getLogs answer cannot pass this identity.
	expected, err := chaincash.AddDecimals(result.BalanceOf, result.CatchUpNetTransfers)
	if err != nil {
		return err
	}
	if comparison, err := expected.Compare(result.CatchUpBalanceOf); err != nil || comparison != 0 {
		return fmt.Errorf("transfer logs are incomplete: balanceOf %s at block %d plus net transfers %s is %s, but balanceOf at block %d is %s",
			result.BalanceOf, result.StartBlock, result.CatchUpNetTransfers, expected, result.CatchUpBlock, result.CatchUpBalanceOf)
	}
	report, initErr := store.InitializeChainCashCursor(ctx, postgresadapter.ChainCashCursorInitialization{
		ExecutionAccountID: opts.AccountID, WalletAddress: opts.Wallet, TokenAddress: result.Token,
		StartBlock: result.StartBlock, StartBlockTime: result.StartTime, StartBalance: result.BalanceOf,
		CatchUpBlock: result.CatchUpBlock, Transfers: caughtUp,
		InitializedBy: opts.Actor, Reason: opts.Reason, Execute: opts.Execute, AppliedAt: opts.Now().UTC(),
		Evidence: map[string]any{
			"tool": "chaincashinit", "chain_id": chainID, "latest_block": latest,
			"start_block_hash": result.StartHash, "start_block_time": result.StartTime.Format(time.RFC3339),
			"start_block_confirmations": result.Confirmations, "required_confirmations": opts.Confirmations,
			"balance_of_at_start_block": result.BalanceOf.String(), "catch_up_block": result.CatchUpBlock,
			"balance_of_at_catch_up_block":    result.CatchUpBalanceOf.String(),
			"net_transfers_after_start_block": result.CatchUpNetTransfers.String(),
			"transfers_after_start_block":     len(caughtUp),
		},
	})
	result.Ledger = &report
	return initErr
}

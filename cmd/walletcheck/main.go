package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

// main 启动当前命令并在失败时记录错误后退出。
func main() {
	if err := run(); err != nil {
		slog.Error("Polymarket wallet check failed", "error", err)
		os.Exit(1)
	}
}

// run 加载钱包账户并执行只读的 Polymarket 连通性检查。
func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	accountsFile := strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE"))
	if accountsFile == "" {
		return fmt.Errorf("POLYMARKET_ACCOUNTS_FILE is required")
	}
	baseURL := env("POLYMARKET_CLOB_URL", "https://clob.polymarket.com")
	requestTimeout, err := durationEnv("POLYMARKET_REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	checkTimeout, err := durationEnv("POLYMARKET_WALLET_CHECK_TIMEOUT", 30*time.Second)
	if err != nil {
		return err
	}
	bootstrapMissing, err := boolEnv("POLYMARKET_BOOTSTRAP_MISSING_API_CREDS", false)
	if err != nil {
		return err
	}

	bootstrapper, err := polymarket.NewCredentialBootstrapper(polymarket.CredentialBootstrapParams{
		BaseURL: baseURL,
		Timeout: requestTimeout,
	})
	if err != nil {
		return err
	}
	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootContext, checkTimeout)
	defer cancel()

	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{
		Path:                           accountsFile,
		CredentialBootstrapper:         bootstrapper,
		BootstrapMissingAPICredentials: bootstrapMissing,
	})
	if err != nil {
		return err
	}
	provider, err := polymarket.NewStaticCredentialProvider(accounts)
	if err != nil {
		return err
	}
	client, err := polymarket.NewTradingClient(polymarket.TradingClientParams{
		BaseURL:        baseURL,
		Credentials:    provider,
		RequestTimeout: requestTimeout,
	})
	if err != nil {
		return err
	}

	for _, account := range accounts {
		probe, probeErr := client.ProbeAccount(ctx, account.ExecutionAccountID)
		if probeErr != nil {
			return fmt.Errorf("execution account %q: authenticated CLOB probe: %w", account.ExecutionAccountID, probeErr)
		}
		logger.Info("Polymarket wallet connected",
			"execution_account_id", probe.ExecutionAccountID,
			"signer_address", probe.SignerAddress,
			"funder_address", probe.FunderAddress,
			"signature_type", uint8(probe.SignatureType),
			"open_orders", probe.OpenOrderCount,
		)
	}
	logger.Info("Polymarket wallet check completed", "accounts", len(accounts), "orders_mutated", false)
	return nil
}

// env 读取环境变量并在为空时返回默认值。
func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// durationEnv 读取并解析持续时间配置。
func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return parsed, nil
}

// boolEnv 读取并解析布尔配置。
func boolEnv(key string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return parsed, nil
}

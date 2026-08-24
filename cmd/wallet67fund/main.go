package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

func main() {
	options, err := parseOptions(os.Args[1:])
	if err != nil {
		slog.Error("wallet native funding arguments are invalid", "error", err)
		os.Exit(2)
	}
	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootContext, options.timeout)
	defer cancel()
	result, err := runCommand(ctx, options)
	if err != nil {
		slog.Error("wallet native funding failed", "error", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		slog.Error("encode wallet native funding result", "error", err)
		os.Exit(1)
	}
}

func parseOptions(arguments []string) (commandOptions, error) {
	options := commandOptions{
		accountsFile: strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")),
		rpcURL:       strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")),
		journalFile:  strings.TrimSpace(os.Getenv("WALLET67_FUND_JOURNAL_FILE")),
		timeout:      defaultCommandTimeout,
	}
	flags := flag.NewFlagSet("wallet67fund", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.accountsFile, "accounts-file", options.accountsFile, "private Polymarket accounts file")
	flags.StringVar(&options.rpcURL, "rpc-url", options.rpcURL, "Polygon JSON-RPC URL")
	flags.StringVar(&options.journalFile, "journal-file", options.journalFile, "absolute private durable funding journal path")
	flags.StringVar(&options.executeToken, "execute-token", "", "exact fixed-plan execution acknowledgement")
	flags.StringVar(&options.expectedStartingNonce, "expected-starting-nonce", "", "approved decimal main latest=pending nonce for first execution")
	flags.StringVar(&options.expectedMainBalanceWei, "expected-main-balance-wei", "", "approved decimal main pending native balance for first execution")
	flags.StringVar(&options.expectedWallet6BalanceWei, "expected-wallet6-balance-wei", "", "approved decimal wallet-6 latest native balance for first execution")
	flags.StringVar(&options.expectedWallet7BalanceWei, "expected-wallet7-balance-wei", "", "approved decimal wallet-7 latest native balance for first execution")
	flags.DurationVar(&options.timeout, "timeout", options.timeout, "whole-command timeout")
	if err := flags.Parse(arguments); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected positional arguments")
	}
	options.accountsFile = strings.TrimSpace(options.accountsFile)
	options.rpcURL = strings.TrimSpace(options.rpcURL)
	options.journalFile = strings.TrimSpace(options.journalFile)
	if options.accountsFile == "" {
		return commandOptions{}, fmt.Errorf("POLYMARKET_ACCOUNTS_FILE or --accounts-file is required")
	}
	if options.rpcURL == "" {
		return commandOptions{}, fmt.Errorf("POLYGON_RPC_URL or --rpc-url is required")
	}
	if options.timeout <= 0 {
		return commandOptions{}, fmt.Errorf("--timeout must be positive")
	}
	if options.executeToken != "" && options.executeToken != exactExecuteToken {
		return commandOptions{}, fmt.Errorf("--execute-token does not exactly match the fixed native funding plan")
	}
	if options.execute() && options.journalFile == "" {
		return commandOptions{}, fmt.Errorf("execute mode requires WALLET67_FUND_JOURNAL_FILE or --journal-file")
	}
	assertions := []string{options.expectedStartingNonce, options.expectedMainBalanceWei, options.expectedWallet6BalanceWei, options.expectedWallet7BalanceWei}
	if options.execute() {
		for _, assertion := range assertions {
			if assertion == "" {
				return commandOptions{}, fmt.Errorf("execute mode requires all four approved prestate assertions")
			}
		}
	} else {
		for _, assertion := range assertions {
			if assertion != "" {
				return commandOptions{}, fmt.Errorf("prestate assertions are execute-only")
			}
		}
	}
	return options, nil
}

func runCommand(ctx context.Context, options commandOptions) (fundingRunResult, error) {
	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{
		Path:                           options.accountsFile,
		BootstrapMissingAPICredentials: false,
	})
	if err != nil {
		return fundingRunResult{}, fmt.Errorf("load fixed main native funding account: %w", err)
	}
	selected, err := selectFundingAccount(accounts)
	if err != nil {
		return fundingRunResult{}, err
	}
	httpClient := &http.Client{Timeout: defaultRequestTimeout}
	rpc, err := newRPCClient(options.rpcURL, httpClient, defaultRequestTimeout)
	if err != nil {
		return fundingRunResult{}, err
	}
	var store *fileJournalStore
	if options.execute() {
		store, err = newFileJournalStore(options.journalFile)
		if err != nil {
			return fundingRunResult{}, err
		}
		defer store.Close()
	}
	var prestate *fundingPrestate
	if options.execute() {
		prestate, err = parseFundingPrestate(options)
		if err != nil {
			return fundingRunResult{}, err
		}
	}
	return runFundings(ctx, fundingRunParams{
		rpc: rpc, account: selected, journal: store, clock: realClock{},
		pollInterval: defaultReceiptPollInterval, execute: options.execute(), prestate: prestate,
	})
}

func parseFundingPrestate(options commandOptions) (*fundingPrestate, error) {
	nonceValue, ok := parseCanonicalNonNegativeDecimal(options.expectedStartingNonce)
	if !ok || !nonceValue.IsUint64() {
		return nil, fmt.Errorf("--expected-starting-nonce must be a canonical uint64 decimal")
	}
	mainBalance, ok := parseCanonicalNonNegativeDecimal(options.expectedMainBalanceWei)
	if !ok {
		return nil, fmt.Errorf("--expected-main-balance-wei must be a canonical uint256 decimal")
	}
	wallet6Balance, ok := parseCanonicalNonNegativeDecimal(options.expectedWallet6BalanceWei)
	if !ok {
		return nil, fmt.Errorf("--expected-wallet6-balance-wei must be a canonical uint256 decimal")
	}
	wallet7Balance, ok := parseCanonicalNonNegativeDecimal(options.expectedWallet7BalanceWei)
	if !ok {
		return nil, fmt.Errorf("--expected-wallet7-balance-wei must be a canonical uint256 decimal")
	}
	return &fundingPrestate{
		startingNonce: nonceValue.Uint64(), sourceBalance: mainBalance,
		targetBalances: map[string]*big.Int{
			wallet6ExpectedAddress: wallet6Balance,
			wallet7ExpectedAddress: wallet7Balance,
		},
	}, nil
}

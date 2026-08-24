package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
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
		slog.Error("wallet approval arguments are invalid", "error", err)
		os.Exit(2)
	}
	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootContext, options.timeout)
	defer cancel()
	result, err := runCommand(ctx, options)
	if err != nil {
		slog.Error("wallet approval failed", "error", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		slog.Error("encode wallet approval result", "error", err)
		os.Exit(1)
	}
}

func parseOptions(arguments []string) (commandOptions, error) {
	options := commandOptions{
		accountsFile: strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")),
		rpcURL:       strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")),
		journalFile:  strings.TrimSpace(os.Getenv("WALLET_APPROVE_JOURNAL_FILE")),
		timeout:      defaultCommandTimeout,
	}
	flags := flag.NewFlagSet("walletapprove", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.accountsFile, "accounts-file", options.accountsFile, "private Polymarket accounts file")
	flags.StringVar(&options.rpcURL, "rpc-url", options.rpcURL, "Polygon JSON-RPC URL")
	flags.StringVar(&options.journalFile, "journal-file", options.journalFile, "absolute durable execution journal path")
	flags.StringVar(&options.executeToken, "execute-token", "", "exact fixed-plan execution acknowledgement")
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
		return commandOptions{}, fmt.Errorf("--execute-token does not exactly match the fixed approval plan")
	}
	if options.execute() && options.journalFile == "" {
		return commandOptions{}, fmt.Errorf("execute mode requires WALLET_APPROVE_JOURNAL_FILE or --journal-file")
	}
	return options, nil
}

func runCommand(ctx context.Context, options commandOptions) (approvalRunResult, error) {
	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{
		Path:                           options.accountsFile,
		BootstrapMissingAPICredentials: false,
	})
	if err != nil {
		return approvalRunResult{}, fmt.Errorf("load fixed wallet-6/wallet-7 approval accounts: %w", err)
	}
	selected, err := selectApprovalAccounts(accounts)
	if err != nil {
		return approvalRunResult{}, err
	}
	httpClient := &http.Client{Timeout: defaultRequestTimeout}
	rpc, err := newRPCClient(options.rpcURL, httpClient, defaultRequestTimeout)
	if err != nil {
		return approvalRunResult{}, err
	}
	var store *fileJournalStore
	if options.execute() {
		store, err = newFileJournalStore(options.journalFile)
		if err != nil {
			return approvalRunResult{}, err
		}
		defer store.Close()
	}
	return runApprovals(ctx, approvalRunParams{
		rpc: rpc, accounts: selected, journal: store, clock: realClock{},
		pollInterval: defaultReceiptPollInterval, execute: options.execute(),
	})
}

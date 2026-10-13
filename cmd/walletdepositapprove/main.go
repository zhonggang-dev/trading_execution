// walletdepositapprove is an operator-only tool that grants the Polymarket
// trading approvals a Deposit Wallet is missing, so it matches the fixed
// reference wallet (wallet-7). It is a dry run unless --execute-token equals
// the exact fixed plan. Keys are read only through the same accounts file the
// service uses; nothing secret is printed or written.
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
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	targetAccountID = "wallet-6"
	targetSigner    = "0x0aefd80df02cc35e81aede40b34e2e961bb4593f"
	targetWallet    = "0x635d25519789e40c3794d72a88cbb7f25ac443f8"
	executeToken    = "APPROVE_WALLET_6_DEPOSIT_WALLET_NEGRISK_MAX_POLYGON_137"

	negRiskAdapterV1       = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296" // Polymarket "Neg Risk Adapter (CLOB v1)"
	negRiskCollateralAdapt = "0xada2005600dec949baf300f4c6120000bdb6eaab" // NegRiskCtfCollateralAdapter
	ctfCollateralAdapter   = "0xada100db00ca00073811820692005400218fce1f" // CtfCollateralAdapter
)

// fixedApprovals are exactly the approvals wallet-7's Deposit Wallet holds and
// wallet-6's lacks; anything already satisfied on-chain is skipped.
var fixedApprovals = []polymarket.TradingApproval{
	{Kind: polymarket.ApprovalPUSDAllowance, Spender: negRiskAdapterV1},
	{Kind: polymarket.ApprovalPUSDAllowance, Spender: negRiskCollateralAdapt},
	{Kind: polymarket.ApprovalPUSDAllowance, Spender: ctfCollateralAdapter},
	{Kind: polymarket.ApprovalCTFOperator, Spender: negRiskAdapterV1},
	{Kind: polymarket.ApprovalCTFOperator, Spender: negRiskCollateralAdapt},
}

type result struct {
	Account    string                            `json:"account"`
	Wallet     string                            `json:"deposit_wallet"`
	Mode       string                            `json:"mode"`
	Before     []polymarket.TradingApprovalState `json:"before"`
	ToSubmit   []polymarket.TradingApproval      `json:"to_submit"`
	Submission *domain.RedemptionSubmission      `json:"submission,omitempty"`
	After      []polymarket.TradingApprovalState `json:"after,omitempty"`
	AllDone    bool                              `json:"all_satisfied"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("deposit wallet approval failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	accountsFile := flag.String("accounts-file", strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")), "private Polymarket accounts file")
	rpcURL := flag.String("rpc-url", strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")), "Polygon JSON-RPC URL")
	relayerURL := flag.String("relayer-url", strings.TrimSpace(os.Getenv("POLYMARKET_RELAYER_URL")), "Polymarket relayer URL (default official)")
	token := flag.String("execute-token", "", "exact fixed-plan execution acknowledgement; empty = dry run")
	timeout := flag.Duration("timeout", 10*time.Minute, "whole-command timeout")
	flag.Parse()
	if *token != "" && *token != executeToken {
		return fmt.Errorf("--execute-token does not exactly match the fixed approval plan")
	}
	if *accountsFile == "" || *rpcURL == "" {
		return fmt.Errorf("--accounts-file and --rpc-url (or their environment variables) are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{Path: *accountsFile, BootstrapMissingAPICredentials: false})
	if err != nil {
		return fmt.Errorf("load accounts: %w", err)
	}
	var selected []polymarket.TradingAccount
	for _, account := range accounts {
		if strings.TrimSpace(account.ExecutionAccountID) == targetAccountID {
			selected = append(selected, account)
		}
	}
	if len(selected) != 1 {
		return fmt.Errorf("expected exactly one %s account, found %d", targetAccountID, len(selected))
	}
	account := selected[0]
	if account.SignatureType != polymarket.SignatureTypePolyEIP1271 {
		return fmt.Errorf("%s must be a Deposit Wallet (POLY_1271) account", targetAccountID)
	}
	if !strings.EqualFold(account.FunderAddress, targetWallet) || account.Signer == nil || !strings.EqualFold(account.Signer.Address(), targetSigner) {
		return fmt.Errorf("%s signer/wallet differ from the fixed reviewed addresses", targetAccountID)
	}
	if strings.TrimSpace(account.Relayer.Key) == "" || strings.TrimSpace(account.Relayer.Address) == "" {
		return fmt.Errorf("%s has no relayer API key configured", targetAccountID)
	}
	provider, err := polymarket.NewStaticCredentialProvider([]polymarket.TradingAccount{account})
	if err != nil {
		return err
	}
	client, err := polymarket.NewRedemptionClient(polymarket.RedemptionClientParams{
		RelayerURL: *relayerURL, RPCURL: *rpcURL, Credentials: provider, RequestTimeout: 15 * time.Second,
		RequiredConfirmations: 64, HTTPClient: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	})
	if err != nil {
		return err
	}

	out := result{Account: targetAccountID, Wallet: targetWallet, Mode: "DRY_RUN"}
	if out.Before, err = client.ReadTradingApprovals(ctx, targetWallet, fixedApprovals); err != nil {
		return err
	}
	for _, state := range out.Before {
		if !state.Satisfied {
			out.ToSubmit = append(out.ToSubmit, state.TradingApproval)
		}
	}
	if *token == executeToken && len(out.ToSubmit) > 0 {
		out.Mode = "EXECUTE"
		submission, err := client.SubmitTradingApprovals(ctx, targetAccountID, out.ToSubmit, "Approve Neg Risk trading spenders")
		if err != nil {
			return fmt.Errorf("submit approvals: %w", err)
		}
		slog.Info("approval batch submitted", "relayer_transaction_id", submission.Reference, "state", submission.State)
		for deadline := time.Now().Add(5 * time.Minute); time.Now().Before(deadline); {
			resolved, err := client.ResolveRedemptionSubmission(ctx, targetAccountID, submission)
			if err != nil {
				return fmt.Errorf("poll relayer (transaction %s): %w", submission.Reference, err)
			}
			submission = resolved
			if submission.State == domain.RedemptionSubmissionConfirmed || submission.State == domain.RedemptionSubmissionFailed {
				break
			}
			time.Sleep(3 * time.Second)
		}
		out.Submission = &submission
		if submission.State != domain.RedemptionSubmissionConfirmed {
			return writeResult(os.Stdout, out, fmt.Errorf("relayer transaction %s ended in state %s: %s", submission.Reference, submission.State, submission.FailureReason))
		}
		// Re-read until every approval is visible on-chain (RPC nodes lag a block or two).
		for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(3 * time.Second) {
			if out.After, err = client.ReadTradingApprovals(ctx, targetWallet, fixedApprovals); err != nil {
				return err
			}
			if allSatisfied(out.After) || time.Now().After(deadline) {
				break
			}
		}
		out.AllDone = allSatisfied(out.After)
		if !out.AllDone {
			return writeResult(os.Stdout, out, fmt.Errorf("relayer confirmed the batch but not every approval is visible on-chain yet"))
		}
		return writeResult(os.Stdout, out, nil)
	}
	out.AllDone = len(out.ToSubmit) == 0
	return writeResult(os.Stdout, out, nil)
}

func allSatisfied(states []polymarket.TradingApprovalState) bool {
	for _, state := range states {
		if !state.Satisfied {
			return false
		}
	}
	return true
}

func writeResult(w io.Writer, value result, cause error) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return cause
}

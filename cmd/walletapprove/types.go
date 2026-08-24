package main

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

const (
	polygonChainID             = uint64(137)
	approvalAmountBaseUnits    = uint64(200_000_000)
	requiredConfirmations      = uint64(64)
	approvalJournalSchema      = "trading.wallet_approval.v1"
	exactExecuteToken          = "APPROVE_WALLET_6_AND_WALLET_7_PUSD_200000000_POLYGON_137"
	defaultCommandTimeout      = 20 * time.Minute
	defaultRequestTimeout      = 10 * time.Second
	defaultReceiptPollInterval = 2 * time.Second
	maximumRPCResponseBytes    = int64(1 << 20)
	maximumApprovalGasLimit    = uint64(150_000)
	minimumApprovalGasEstimate = uint64(21_000)
	maximumPriorityFeeWei      = uint64(100_000_000_000) // 100 gwei
	maximumBaseFeeWei          = uint64(500_000_000_000) // 500 gwei
	maximumMaxFeePerGasWei     = uint64(1_000_000_000_000)
	approvalGasNumerator       = uint64(120)
	approvalGasDenominator     = uint64(100)
	wallet6ExecutionAccountID  = "wallet-6"
	wallet7ExecutionAccountID  = "wallet-7"
	wallet6ExpectedAddress     = "0x0aefd80df02cc35e81aede40b34e2e961bb4593f"
	wallet7ExpectedAddress     = "0xc9ba353781f13ec9507bc0677156814d805fe6d9"
	standardExchangeV2Address  = "0xe111180000d2663c0091e4f400237545b87b996b"
	negRiskExchangeV2Address   = "0xe2222d279d744050d28e00520010520000310f59"
	polygonPUSDAddress         = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
)

var approvalAmount = new(big.Int).SetUint64(approvalAmountBaseUnits)

type commandOptions struct {
	accountsFile string
	rpcURL       string
	journalFile  string
	executeToken string
	timeout      time.Duration
}

func (options commandOptions) execute() bool { return options.executeToken == exactExecuteToken }

type approvalTarget struct {
	executionAccountID string
	expectedAddress    string
	spender            string
}

var fixedApprovalTargets = []approvalTarget{
	{wallet6ExecutionAccountID, wallet6ExpectedAddress, standardExchangeV2Address},
	{wallet6ExecutionAccountID, wallet6ExpectedAddress, negRiskExchangeV2Address},
	{wallet7ExecutionAccountID, wallet7ExpectedAddress, standardExchangeV2Address},
	{wallet7ExecutionAccountID, wallet7ExpectedAddress, negRiskExchangeV2Address},
}

type approvalAccount struct {
	executionAccountID string
	address            string
	signer             polymarket.DigestSigner
}

func selectApprovalAccounts(accounts []polymarket.TradingAccount) (map[string]approvalAccount, error) {
	required := map[string]string{
		wallet6ExecutionAccountID: wallet6ExpectedAddress,
		wallet7ExecutionAccountID: wallet7ExpectedAddress,
	}
	selected := make(map[string]approvalAccount, len(required))
	for _, account := range accounts {
		expectedAddress, wanted := required[strings.TrimSpace(account.ExecutionAccountID)]
		if !wanted {
			continue
		}
		if _, exists := selected[account.ExecutionAccountID]; exists {
			return nil, fmt.Errorf("duplicate approval account %q", account.ExecutionAccountID)
		}
		if account.SignatureType != polymarket.SignatureTypeEOA {
			return nil, fmt.Errorf("approval account %q must use EOA signature type", account.ExecutionAccountID)
		}
		if account.Signer == nil {
			return nil, fmt.Errorf("approval account %q has no signer", account.ExecutionAccountID)
		}
		signerAddress, err := normalizeAddress(account.Signer.Address())
		if err != nil {
			return nil, fmt.Errorf("approval account %q signer: %w", account.ExecutionAccountID, err)
		}
		funderAddress, err := normalizeAddress(account.FunderAddress)
		if err != nil {
			return nil, fmt.Errorf("approval account %q funder: %w", account.ExecutionAccountID, err)
		}
		if signerAddress != funderAddress {
			return nil, fmt.Errorf("approval account %q signer and funder differ", account.ExecutionAccountID)
		}
		if signerAddress != expectedAddress {
			return nil, fmt.Errorf(
				"approval account %q address is %s; require fixed address %s",
				account.ExecutionAccountID, signerAddress, expectedAddress,
			)
		}
		selected[account.ExecutionAccountID] = approvalAccount{
			executionAccountID: account.ExecutionAccountID,
			address:            signerAddress,
			signer:             account.Signer,
		}
	}
	for executionAccountID := range required {
		if _, exists := selected[executionAccountID]; !exists {
			return nil, fmt.Errorf("required approval account %q is absent", executionAccountID)
		}
	}
	return selected, nil
}

type rpcCaller interface {
	Call(context.Context, string, []any) ([]byte, error)
}

type journalStore interface {
	LoadOrCreate(approvalJournal) (approvalJournal, error)
	Save(approvalJournal) error
}

type clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

func (realClock) Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

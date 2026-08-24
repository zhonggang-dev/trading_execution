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
	requiredConfirmations      = uint64(64)
	fundingJournalSchema       = "trading.wallet67_native_funding.v1"
	exactExecuteToken          = "FUND_WALLET_6_AND_WALLET_7_POL_60000000000000000_POLYGON_137"
	defaultCommandTimeout      = 30 * time.Minute
	defaultRequestTimeout      = 10 * time.Second
	defaultReceiptPollInterval = 2 * time.Second
	maximumRPCResponseBytes    = int64(1 << 20)
	minimumFundingGasEstimate  = uint64(21_000)
	maximumFundingGasLimit     = uint64(50_000)
	maximumPriorityFeeWei      = uint64(100_000_000_000) // 100 gwei
	maximumBaseFeeWei          = uint64(500_000_000_000) // 500 gwei
	maximumMaxFeePerGasWei     = uint64(1_000_000_000_000)
	fundingGasNumerator        = uint64(120)
	fundingGasDenominator      = uint64(100)
	mainExecutionAccountID     = "main"
	mainExpectedAddress        = "0x6c07b5e271ffa2da038ce05e498cff856bf32357"
	wallet6ExecutionAccountID  = "wallet-6"
	wallet7ExecutionAccountID  = "wallet-7"
	wallet6ExpectedAddress     = "0x0aefd80df02cc35e81aede40b34e2e961bb4593f"
	wallet7ExpectedAddress     = "0xc9ba353781f13ec9507bc0677156814d805fe6d9"
)

var fundingAmountWei = mustDecimalBig("60000000000000000")

type commandOptions struct {
	accountsFile              string
	rpcURL                    string
	journalFile               string
	executeToken              string
	expectedStartingNonce     string
	expectedMainBalanceWei    string
	expectedWallet6BalanceWei string
	expectedWallet7BalanceWei string
	timeout                   time.Duration
}

func (options commandOptions) execute() bool { return options.executeToken == exactExecuteToken }

type fundingTarget struct {
	executionAccountID string
	recipient          string
}

var fixedFundingTargets = []fundingTarget{
	{wallet6ExecutionAccountID, wallet6ExpectedAddress},
	{wallet7ExecutionAccountID, wallet7ExpectedAddress},
}

type fundingAccount struct {
	executionAccountID string
	address            string
	signer             polymarket.DigestSigner
}

func selectFundingAccount(accounts []polymarket.TradingAccount) (fundingAccount, error) {
	var selected *fundingAccount
	for _, account := range accounts {
		if strings.TrimSpace(account.ExecutionAccountID) != mainExecutionAccountID {
			continue
		}
		if selected != nil {
			return fundingAccount{}, fmt.Errorf("duplicate funding account %q", mainExecutionAccountID)
		}
		if account.SignatureType != polymarket.SignatureTypeEOA {
			return fundingAccount{}, fmt.Errorf("funding account %q must use EOA signature type 0", mainExecutionAccountID)
		}
		if account.Signer == nil {
			return fundingAccount{}, fmt.Errorf("funding account %q has no signer", mainExecutionAccountID)
		}
		signerAddress, err := normalizeAddress(account.Signer.Address())
		if err != nil {
			return fundingAccount{}, fmt.Errorf("funding account signer: %w", err)
		}
		funderAddress, err := normalizeAddress(account.FunderAddress)
		if err != nil {
			return fundingAccount{}, fmt.Errorf("funding account funder: %w", err)
		}
		if signerAddress != funderAddress {
			return fundingAccount{}, fmt.Errorf("funding account signer and funder differ")
		}
		if signerAddress != mainExpectedAddress {
			return fundingAccount{}, fmt.Errorf("funding account address is %s; require fixed main address %s", signerAddress, mainExpectedAddress)
		}
		value := fundingAccount{executionAccountID: mainExecutionAccountID, address: signerAddress, signer: account.Signer}
		selected = &value
	}
	if selected == nil {
		return fundingAccount{}, fmt.Errorf("required funding account %q is absent", mainExecutionAccountID)
	}
	return *selected, nil
}

type rpcCaller interface {
	Call(context.Context, string, []any) ([]byte, error)
}

type journalStore interface {
	Load(fundingJournal) (fundingJournal, bool, error)
	Create(fundingJournal) error
	Save(fundingJournal) error
}

type fundingPrestate struct {
	startingNonce  uint64
	sourceBalance  *big.Int
	targetBalances map[string]*big.Int
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

func mustDecimalBig(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok {
		panic("invalid fixed decimal integer")
	}
	return parsed
}

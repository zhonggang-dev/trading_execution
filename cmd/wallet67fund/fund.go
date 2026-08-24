package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
)

type fundingRunParams struct {
	rpc          rpcCaller
	account      fundingAccount
	journal      journalStore
	clock        clock
	pollInterval time.Duration
	execute      bool
	expectedPlan fundingJournal
	prestate     *fundingPrestate
}

type fundingRunResult struct {
	DryRun                bool                  `json:"dry_run"`
	ChainID               uint64                `json:"chain_id"`
	FundingSource         string                `json:"funding_source"`
	AmountWei             string                `json:"amount_wei"`
	RequiredConfirmations uint64                `json:"required_confirmations"`
	Targets               []fundingTargetResult `json:"targets"`
}

type fundingTargetResult struct {
	ExecutionAccountID   string `json:"execution_account_id"`
	Recipient            string `json:"recipient"`
	BalanceBefore        string `json:"balance_before,omitempty"`
	Nonce                uint64 `json:"nonce,omitempty"`
	GasLimit             uint64 `json:"gas_limit,omitempty"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas,omitempty"`
	MaxFeePerGas         string `json:"max_fee_per_gas,omitempty"`
	State                string `json:"state"`
	TransactionHash      string `json:"transaction_hash,omitempty"`
	ReceiptBlockNumber   uint64 `json:"receipt_block_number,omitempty"`
	Confirmations        uint64 `json:"confirmations,omitempty"`
	BalanceAfter         string `json:"balance_after,omitempty"`
}

func runFundings(ctx context.Context, params fundingRunParams) (fundingRunResult, error) {
	if params.clock == nil {
		return fundingRunResult{}, fmt.Errorf("funding clock is required")
	}
	if params.account.signer == nil || params.account.executionAccountID != mainExecutionAccountID || params.account.address != mainExpectedAddress {
		return fundingRunResult{}, fmt.Errorf("fixed main funding account identity is unavailable")
	}
	if params.execute && params.prestate == nil {
		return fundingRunResult{}, fmt.Errorf("execute mode requires approved prestate assertions")
	}
	return runFundingPlan(ctx, params, initialFundingJournal(params.clock.Now()))
}

// runFundingPlan lets tests exercise the state machine with a non-production
// key. Production always calls runFundings and therefore uses only the fixed
// main -> wallet-6 -> wallet-7 plan.
func runFundingPlan(ctx context.Context, params fundingRunParams, expected fundingJournal) (fundingRunResult, error) {
	if params.rpc == nil || params.clock == nil {
		return fundingRunResult{}, fmt.Errorf("funding RPC and clock are required")
	}
	if params.execute && params.journal == nil {
		return fundingRunResult{}, fmt.Errorf("execute mode requires a durable journal")
	}
	if params.account.signer == nil || params.account.address != expected.FundingSource {
		return fundingRunResult{}, fmt.Errorf("funding account identity does not match the immutable plan")
	}
	if params.pollInterval <= 0 {
		params.pollInterval = defaultReceiptPollInterval
	}
	if err := readChainID(ctx, params.rpc); err != nil {
		return fundingRunResult{}, err
	}
	if err := validateFundingJournal(expected, expected); err != nil {
		return fundingRunResult{}, fmt.Errorf("funding plan is invalid: %w", err)
	}
	for _, target := range fixedFundingTargets {
		if target.recipient == expected.FundingSource {
			return fundingRunResult{}, fmt.Errorf("funding recipient equals funding source")
		}
		if err := requireEOARecipient(ctx, params.rpc, target.recipient); err != nil {
			return fundingRunResult{}, fmt.Errorf("%s: %w", target.executionAccountID, err)
		}
	}
	params.expectedPlan = expected
	journal := expected
	preflightDone := false
	if params.execute {
		loaded, exists, err := params.journal.Load(expected)
		if err != nil {
			return fundingRunResult{}, err
		}
		if exists {
			journal = loaded
			if err := requireJournalMatchesApprovedPrestate(journal, *params.prestate); err != nil {
				return fundingRunResult{}, err
			}
		} else {
			if err := preflightRemainingBudget(ctx, params, expected); err != nil {
				return fundingRunResult{}, fmt.Errorf("preflight fixed funding plan: %w", err)
			}
			preflightDone = true
			journal, err = prepareInitialJournal(ctx, params, expected, *params.prestate)
			if err != nil {
				return fundingRunResult{}, err
			}
			if err := params.journal.Create(journal); err != nil {
				return fundingRunResult{}, fmt.Errorf("persist complete PENDING funding plan before signing: %w", err)
			}
		}
	}
	if !preflightDone {
		if err := preflightRemainingBudget(ctx, params, journal); err != nil {
			return fundingRunResult{}, fmt.Errorf("preflight fixed funding plan: %w", err)
		}
	}
	result := fundingRunResult{
		DryRun: !params.execute, ChainID: polygonChainID, FundingSource: expected.FundingSource,
		AmountWei: fundingAmountWei.String(), RequiredConfirmations: requiredConfirmations,
		Targets: make([]fundingTargetResult, 0, len(journal.Entries)),
	}
	var dryRunNonce uint64
	var dryRunNonceSet bool
	var err error
	for index := range journal.Entries {
		entry := &journal.Entries[index]
		if params.execute {
			if err := executeJournalEntry(ctx, params, &journal, index); err != nil {
				return result, fmt.Errorf("fund %s: %w", entry.ExecutionAccountID, err)
			}
		} else {
			if !dryRunNonceSet {
				dryRunNonce, err = readNonce(ctx, params.rpc, expected.FundingSource)
				if err != nil {
					return result, err
				}
				dryRunNonceSet = true
			}
			if err := dryRunJournalEntry(ctx, params, entry, dryRunNonce); err != nil {
				return result, fmt.Errorf("dry-run fund %s: %w", entry.ExecutionAccountID, err)
			}
			if dryRunNonce == math.MaxUint64 {
				return result, fmt.Errorf("account nonce overflows")
			}
			dryRunNonce++
		}
		result.Targets = append(result.Targets, targetResult(*entry))
	}
	if params.execute {
		if err := validateFinalBalanceDeltas(ctx, params.rpc, journal); err != nil {
			return result, err
		}
	}
	return result, nil
}

func prepareInitialJournal(ctx context.Context, params fundingRunParams, expected fundingJournal, approved fundingPrestate) (fundingJournal, error) {
	nonce, err := readNonce(ctx, params.rpc, expected.FundingSource)
	if err != nil {
		return fundingJournal{}, err
	}
	if nonce != approved.startingNonce {
		return fundingJournal{}, fmt.Errorf("main starting nonce is %d; approved prestate requires %d", nonce, approved.startingNonce)
	}
	sourceBalance, err := readBalance(ctx, params.rpc, expected.FundingSource, "pending")
	if err != nil {
		return fundingJournal{}, err
	}
	if approved.sourceBalance == nil || sourceBalance.Cmp(approved.sourceBalance) != 0 {
		return fundingJournal{}, fmt.Errorf("main pending balance %s differs from approved prestate", sourceBalance)
	}
	prepared := expected
	prepared.Entries = append([]journalEntry(nil), expected.Entries...)
	prepared.StartingNonce = fmt.Sprintf("%d", nonce)
	prepared.SourceBalanceBefore = sourceBalance.String()
	for index := range prepared.Entries {
		entry := &prepared.Entries[index]
		approvedBalance := approved.targetBalances[entry.Recipient]
		if approvedBalance == nil {
			return fundingJournal{}, fmt.Errorf("approved prestate is missing %s", entry.ExecutionAccountID)
		}
		balance, err := readBalance(ctx, params.rpc, entry.Recipient, "latest")
		if err != nil {
			return fundingJournal{}, err
		}
		if balance.Cmp(approvedBalance) != 0 {
			return fundingJournal{}, fmt.Errorf("%s latest balance %s differs from approved prestate", entry.ExecutionAccountID, balance)
		}
		if nonce > math.MaxUint64-uint64(index) {
			return fundingJournal{}, fmt.Errorf("planned nonce overflows")
		}
		entry.RecipientBalanceBefore = balance.String()
		entry.PlannedNonce = fmt.Sprintf("%d", nonce+uint64(index))
	}
	prepared.UpdatedAt = params.clock.Now().UTC()
	if err := validateFundingJournal(prepared, expected); err != nil {
		return fundingJournal{}, fmt.Errorf("prepared funding journal is invalid: %w", err)
	}
	return prepared, nil
}

func requireJournalMatchesApprovedPrestate(journal fundingJournal, approved fundingPrestate) error {
	if journal.StartingNonce != fmt.Sprintf("%d", approved.startingNonce) || approved.sourceBalance == nil || journal.SourceBalanceBefore != approved.sourceBalance.String() {
		return fmt.Errorf("existing funding journal does not match approved main prestate")
	}
	for _, entry := range journal.Entries {
		balance := approved.targetBalances[entry.Recipient]
		if balance == nil || entry.RecipientBalanceBefore != balance.String() {
			return fmt.Errorf("existing funding journal does not match approved %s prestate", entry.ExecutionAccountID)
		}
	}
	return nil
}

func preflightRemainingBudget(ctx context.Context, params fundingRunParams, journal fundingJournal) error {
	required := new(big.Int)
	for _, entry := range journal.Entries {
		switch entry.State {
		case journalStateConfirmed, journalStateBroadcast:
			continue
		case journalStateSigned:
			found, err := recoverSignedTransaction(ctx, params.rpc, entry)
			if err != nil {
				return fmt.Errorf("recover signed %s: %w", entry.ExecutionAccountID, err)
			}
			if found {
				continue
			}
			maxFee, ok := canonicalDecimalBig(entry.MaxFeePerGas)
			if !ok {
				return fmt.Errorf("journal max fee is invalid")
			}
			required.Add(required, fundingAmountWei)
			required.Add(required, new(big.Int).Mul(new(big.Int).SetUint64(entry.GasLimit), maxFee))
		case journalStatePending:
			if err := simulateFunding(ctx, params.rpc, entry.Source, entry.Recipient); err != nil {
				return fmt.Errorf("simulate %s: %w", entry.ExecutionAccountID, err)
			}
			gasLimit, err := estimateFundingGas(ctx, params.rpc, entry.Source, entry.Recipient)
			if err != nil {
				return fmt.Errorf("estimate %s: %w", entry.ExecutionAccountID, err)
			}
			_, maxFee, err := readFees(ctx, params.rpc)
			if err != nil {
				return fmt.Errorf("fees for %s: %w", entry.ExecutionAccountID, err)
			}
			required.Add(required, fundingAmountWei)
			required.Add(required, new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), maxFee))
		default:
			return fmt.Errorf("unsupported journal state %q", entry.State)
		}
		if required.BitLen() > 256 {
			return fmt.Errorf("fixed funding budget overflows uint256")
		}
	}
	if required.Sign() == 0 {
		return nil
	}
	return requireNativeFundingBudget(ctx, params.rpc, params.account.address, required)
}

func dryRunJournalEntry(ctx context.Context, params fundingRunParams, entry *journalEntry, nonce uint64) error {
	balance, err := readBalance(ctx, params.rpc, entry.Recipient, "latest")
	if err != nil {
		return err
	}
	if err := simulateFunding(ctx, params.rpc, entry.Source, entry.Recipient); err != nil {
		return err
	}
	gasLimit, err := estimateFundingGas(ctx, params.rpc, entry.Source, entry.Recipient)
	if err != nil {
		return err
	}
	priorityFee, maxFee, err := readFees(ctx, params.rpc)
	if err != nil {
		return err
	}
	maximumCost := new(big.Int).Add(new(big.Int).Set(fundingAmountWei), new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), maxFee))
	if err := requireNativeFundingBudget(ctx, params.rpc, entry.Source, maximumCost); err != nil {
		return err
	}
	if _, err := newFundingTransaction(nonce, gasLimit, priorityFee, maxFee, entry.Recipient); err != nil {
		return err
	}
	entry.RecipientBalanceBefore = balance.String()
	entry.PlannedNonce = fmt.Sprintf("%d", nonce)
	entry.Nonce = nonce
	entry.GasLimit = gasLimit
	entry.MaxPriorityFeePerGas = priorityFee.String()
	entry.MaxFeePerGas = maxFee.String()
	entry.Input = "0x"
	return nil
}

func executeJournalEntry(ctx context.Context, params fundingRunParams, journal *fundingJournal, index int) error {
	entry := &journal.Entries[index]
	if entry.State == journalStateConfirmed {
		return revalidateConfirmedEntry(ctx, params, *entry)
	}
	if entry.State == journalStatePending {
		nonce, err := readNonce(ctx, params.rpc, entry.Source)
		if err != nil {
			return err
		}
		plannedNonce, ok := parseCanonicalNonNegativeDecimal(entry.PlannedNonce)
		if !ok || !plannedNonce.IsUint64() || nonce != plannedNonce.Uint64() {
			return fmt.Errorf("current nonce %d differs from journaled planned nonce %s", nonce, entry.PlannedNonce)
		}
		if err := requireExactSourceStageBalance(ctx, params.rpc, *journal, index); err != nil {
			return err
		}
		balance, err := readBalance(ctx, params.rpc, entry.Recipient, "latest")
		if err != nil {
			return err
		}
		if balance.String() != entry.RecipientBalanceBefore {
			return fmt.Errorf("recipient balance %s differs from journaled prestate %s", balance, entry.RecipientBalanceBefore)
		}
		if err := simulateFunding(ctx, params.rpc, entry.Source, entry.Recipient); err != nil {
			return err
		}
		gasLimit, err := estimateFundingGas(ctx, params.rpc, entry.Source, entry.Recipient)
		if err != nil {
			return err
		}
		priorityFee, maxFee, err := readFees(ctx, params.rpc)
		if err != nil {
			return err
		}
		transaction, err := newFundingTransaction(nonce, gasLimit, priorityFee, maxFee, entry.Recipient)
		if err != nil {
			return err
		}
		signed, err := signType2Funding(ctx, transaction, params.account)
		if err != nil {
			return err
		}
		entry.Nonce = nonce
		entry.GasLimit = gasLimit
		entry.MaxPriorityFeePerGas = priorityFee.String()
		entry.MaxFeePerGas = maxFee.String()
		entry.Input = "0x"
		entry.SigningDigest = signed.digest
		entry.RawTransaction = signed.raw
		entry.TransactionHash = signed.hash
		entry.SignedAt = params.clock.Now().UTC()
		entry.State = journalStateSigned
		if err := saveJournal(params, journal); err != nil {
			return fmt.Errorf("persist signed transaction before broadcast: %w", err)
		}
	}
	if entry.State != journalStateSigned && entry.State != journalStateBroadcast {
		return fmt.Errorf("unsupported executable journal state %q", entry.State)
	}
	found, err := recoverSignedTransaction(ctx, params.rpc, *entry)
	if err != nil {
		return err
	}
	if entry.State == journalStateSigned && !found {
		if err := verifySignedJournalEntry(ctx, *entry, params.account); err != nil {
			return err
		}
	}
	if entry.State == journalStateSigned {
		if !found {
			nonce, err := readNonce(ctx, params.rpc, entry.Source)
			if err != nil {
				return err
			}
			if nonce != entry.Nonce {
				return fmt.Errorf("signed nonce %d is no longer the account nonce %d; refusing replacement", entry.Nonce, nonce)
			}
			if err := requireExactSourceStageBalance(ctx, params.rpc, *journal, index); err != nil {
				return err
			}
			if err := requireJournalFundingBalance(ctx, params.rpc, *entry); err != nil {
				return err
			}
			signed := signedFundingTransaction{raw: entry.RawTransaction, hash: entry.TransactionHash}
			sendErr := sendRawTransaction(ctx, params.rpc, signed)
			if sendErr != nil {
				recovered, recoverErr := recoverSignedTransaction(ctx, params.rpc, *entry)
				if recoverErr != nil {
					return errors.Join(sendErr, recoverErr)
				}
				if !recovered {
					if isAmbiguousSendError(sendErr) {
						return fmt.Errorf("ambiguous send outcome for %s; exact raw transaction is durably journaled: %w", entry.TransactionHash, sendErr)
					}
					return sendErr
				}
			}
		}
		entry.State = journalStateBroadcast
		entry.BroadcastAt = params.clock.Now().UTC()
		if err := saveJournal(params, journal); err != nil {
			return fmt.Errorf("persist broadcast transaction: %w", err)
		}
	}
	confirmed, err := waitForFinalReceipt(ctx, params, *entry)
	if err != nil {
		return err
	}
	entry.State = journalStateConfirmed
	entry.ReceiptBlockNumber = confirmed.receipt.blockNumber
	entry.ReceiptBlockHash = confirmed.receipt.blockHash
	entry.Confirmations = confirmed.confirmations
	entry.GasUsed = confirmed.receipt.gasUsed
	entry.EffectiveGasPrice = confirmed.receipt.effectiveGasPrice
	entry.RecipientBalanceAfter = confirmed.recipientBalance.String()
	entry.ConfirmedAt = params.clock.Now().UTC()
	return saveJournal(params, journal)
}

func requireExactSourceStageBalance(ctx context.Context, rpc rpcCaller, journal fundingJournal, entryIndex int) error {
	if entryIndex < 0 || entryIndex >= len(journal.Entries) {
		return fmt.Errorf("funding entry index is invalid")
	}
	expected, ok := parseCanonicalNonNegativeDecimal(journal.SourceBalanceBefore)
	if !ok {
		return fmt.Errorf("journal source pre-balance is invalid")
	}
	for index := 0; index < entryIndex; index++ {
		entry := journal.Entries[index]
		if entry.State != journalStateConfirmed || entry.GasUsed == 0 {
			return fmt.Errorf("prior funding entry %d is not durably confirmed", index)
		}
		effectiveGasPrice, ok := parseCanonicalNonNegativeDecimal(entry.EffectiveGasPrice)
		if !ok || effectiveGasPrice.Sign() <= 0 {
			return fmt.Errorf("prior funding entry %d effective gas price is invalid", index)
		}
		expected.Sub(expected, fundingAmountWei)
		expected.Sub(expected, new(big.Int).Mul(new(big.Int).SetUint64(entry.GasUsed), effectiveGasPrice))
	}
	if expected.Sign() < 0 {
		return fmt.Errorf("expected source stage balance is negative")
	}
	actual, err := readBalance(ctx, rpc, journal.FundingSource, "pending")
	if err != nil {
		return err
	}
	if actual.Cmp(expected) != 0 {
		return fmt.Errorf("main pending balance %s differs from exact journal stage balance %s", actual, expected)
	}
	return nil
}

func verifySignedJournalEntry(ctx context.Context, entry journalEntry, account fundingAccount) error {
	priorityFee, ok := canonicalDecimalBig(entry.MaxPriorityFeePerGas)
	if !ok {
		return fmt.Errorf("journal priority fee is invalid")
	}
	maxFee, ok := canonicalDecimalBig(entry.MaxFeePerGas)
	if !ok {
		return fmt.Errorf("journal max fee is invalid")
	}
	transaction, err := newFundingTransaction(entry.Nonce, entry.GasLimit, priorityFee, maxFee, entry.Recipient)
	if err != nil {
		return err
	}
	if entry.Input != "0x" || len(transaction.data) != 0 {
		return fmt.Errorf("journal input does not match fixed native funding")
	}
	signed, err := signType2Funding(ctx, transaction, account)
	if err != nil {
		return err
	}
	if signed.digest != entry.SigningDigest || signed.raw != entry.RawTransaction || signed.hash != entry.TransactionHash {
		return fmt.Errorf("journaled signed transaction does not reproduce exactly")
	}
	return nil
}

func recoverSignedTransaction(ctx context.Context, rpc rpcCaller, entry journalEntry) (bool, error) {
	receipt, err := readReceipt(ctx, rpc, entry.TransactionHash)
	if err != nil {
		return false, err
	}
	if receipt != nil {
		if _, err := validateFundingReceipt(*receipt, entry); err != nil {
			return false, err
		}
		transaction, err := readTransaction(ctx, rpc, entry.TransactionHash)
		if err != nil || transaction == nil {
			return false, fmt.Errorf("receipt exists but exact transaction is unavailable: %w", err)
		}
		if err := validateRPCTransaction(*transaction, entry); err != nil {
			return false, err
		}
		return true, nil
	}
	transaction, err := readTransaction(ctx, rpc, entry.TransactionHash)
	if err != nil {
		return false, err
	}
	if transaction == nil {
		return false, nil
	}
	if err := validateRPCTransaction(*transaction, entry); err != nil {
		return false, err
	}
	return true, nil
}

type finalReceipt struct {
	receipt          validatedReceipt
	confirmations    uint64
	recipientBalance *big.Int
}

func waitForFinalReceipt(ctx context.Context, params fundingRunParams, entry journalEntry) (finalReceipt, error) {
	var highestLatest uint64
	for {
		receipt, err := readReceipt(ctx, params.rpc, entry.TransactionHash)
		if err != nil {
			return finalReceipt{}, err
		}
		if receipt == nil {
			transaction, err := readTransaction(ctx, params.rpc, entry.TransactionHash)
			if err != nil {
				return finalReceipt{}, fmt.Errorf("read broadcast transaction: %w", err)
			}
			if transaction != nil {
				if err := validateRPCTransaction(*transaction, entry); err != nil {
					return finalReceipt{}, err
				}
			} else if err := rebroadcastMissingTransaction(ctx, params, entry); err != nil {
				return finalReceipt{}, err
			}
			if err := params.clock.Sleep(ctx, params.pollInterval); err != nil {
				return finalReceipt{}, err
			}
			continue
		}
		validated, err := validateFundingReceipt(*receipt, entry)
		if err != nil {
			return finalReceipt{}, err
		}
		transaction, err := readTransaction(ctx, params.rpc, entry.TransactionHash)
		if err != nil || transaction == nil {
			return finalReceipt{}, fmt.Errorf("read exact mined transaction: %w", err)
		}
		if err := validateRPCTransaction(*transaction, entry); err != nil {
			return finalReceipt{}, err
		}
		if err := validateMinedTransactionBlock(*transaction, validated); err != nil {
			return finalReceipt{}, err
		}
		latest, err := readLatestBlock(ctx, params.rpc)
		if err != nil {
			return finalReceipt{}, err
		}
		if highestLatest != 0 && latest < highestLatest {
			return finalReceipt{}, fmt.Errorf("RPC latest block regressed from %d to %d", highestLatest, latest)
		}
		highestLatest = latest
		if latest < validated.blockNumber {
			return finalReceipt{}, fmt.Errorf("RPC latest block %d is behind receipt block %d", latest, validated.blockNumber)
		}
		confirmations := latest - validated.blockNumber + 1
		if confirmations < requiredConfirmations {
			if err := params.clock.Sleep(ctx, params.pollInterval); err != nil {
				return finalReceipt{}, err
			}
			continue
		}
		canonicalBlockHash, err := readBlockHash(ctx, params.rpc, validated.blockNumber)
		if err != nil || canonicalBlockHash != validated.blockHash {
			return finalReceipt{}, fmt.Errorf("receipt block is not canonical")
		}
		secondReceipt, err := readReceipt(ctx, params.rpc, entry.TransactionHash)
		if err != nil || secondReceipt == nil {
			return finalReceipt{}, fmt.Errorf("re-read confirmed receipt: %w", err)
		}
		secondValidated, err := validateFundingReceipt(*secondReceipt, entry)
		if err != nil || secondValidated != validated {
			return finalReceipt{}, fmt.Errorf("confirmed receipt changed across reads: %w", err)
		}
		secondTransaction, err := readTransaction(ctx, params.rpc, entry.TransactionHash)
		if err != nil || secondTransaction == nil {
			return finalReceipt{}, fmt.Errorf("re-read confirmed transaction: %w", err)
		}
		if err := validateRPCTransaction(*secondTransaction, entry); err != nil {
			return finalReceipt{}, err
		}
		if err := validateMinedTransactionBlock(*secondTransaction, validated); err != nil {
			return finalReceipt{}, err
		}
		secondLatest, err := readLatestBlock(ctx, params.rpc)
		if err != nil || secondLatest < latest || secondLatest-validated.blockNumber+1 < requiredConfirmations {
			return finalReceipt{}, fmt.Errorf("confirmation count regressed or fell below %d", requiredConfirmations)
		}
		balance, err := readBalance(ctx, params.rpc, entry.Recipient, quantityHex(secondLatest))
		if err != nil {
			return finalReceipt{}, err
		}
		before, ok := new(big.Int).SetString(entry.RecipientBalanceBefore, 10)
		expectedBalance := new(big.Int).Add(before, fundingAmountWei)
		if !ok || before.Sign() < 0 || balance.Cmp(expectedBalance) != 0 {
			return finalReceipt{}, fmt.Errorf("recipient balance %s does not equal exact pre-balance plus fixed funding value %s", balance, expectedBalance)
		}
		return finalReceipt{receipt: validated, confirmations: secondLatest - validated.blockNumber + 1, recipientBalance: balance}, nil
	}
}

func validateFinalBalanceDeltas(ctx context.Context, rpc rpcCaller, journal fundingJournal) error {
	sourceBefore, ok := parseCanonicalNonNegativeDecimal(journal.SourceBalanceBefore)
	if !ok {
		return fmt.Errorf("journal source pre-balance is invalid")
	}
	expectedSource := new(big.Int).Set(sourceBefore)
	for _, entry := range journal.Entries {
		if entry.State != journalStateConfirmed {
			return fmt.Errorf("funding entry %s is not confirmed", entry.ExecutionAccountID)
		}
		effectiveGasPrice, ok := parseCanonicalNonNegativeDecimal(entry.EffectiveGasPrice)
		if !ok || effectiveGasPrice.Sign() <= 0 {
			return fmt.Errorf("funding entry %s effective gas price is invalid", entry.ExecutionAccountID)
		}
		expectedSource.Sub(expectedSource, fundingAmountWei)
		expectedSource.Sub(expectedSource, new(big.Int).Mul(new(big.Int).SetUint64(entry.GasUsed), effectiveGasPrice))
	}
	if expectedSource.Sign() < 0 {
		return fmt.Errorf("exact funding and gas exceed journaled source balance")
	}
	finalBlock, err := readLatestBlock(ctx, rpc)
	if err != nil {
		return err
	}
	blockTag := quantityHex(finalBlock)
	sourceAfter, err := readBalance(ctx, rpc, journal.FundingSource, blockTag)
	if err != nil {
		return err
	}
	if sourceAfter.Cmp(expectedSource) != 0 {
		return fmt.Errorf("main balance %s does not equal exact pre-balance minus 0.12 POL and effective gas %s", sourceAfter, expectedSource)
	}
	for _, entry := range journal.Entries {
		before, ok := parseCanonicalNonNegativeDecimal(entry.RecipientBalanceBefore)
		if !ok {
			return fmt.Errorf("%s recipient pre-balance is invalid", entry.ExecutionAccountID)
		}
		expectedAfter := new(big.Int).Add(before, fundingAmountWei)
		after, err := readBalance(ctx, rpc, entry.Recipient, blockTag)
		if err != nil {
			return err
		}
		if after.Cmp(expectedAfter) != 0 || entry.RecipientBalanceAfter != expectedAfter.String() {
			return fmt.Errorf("%s recipient balance delta is not exactly %s", entry.ExecutionAccountID, fundingAmountWei)
		}
	}
	return nil
}

func rebroadcastMissingTransaction(ctx context.Context, params fundingRunParams, entry journalEntry) error {
	if err := verifySignedJournalEntry(ctx, entry, params.account); err != nil {
		return err
	}
	nonce, err := readNonce(ctx, params.rpc, entry.Source)
	if err != nil {
		return err
	}
	if nonce != entry.Nonce {
		return fmt.Errorf("broadcast nonce %d is no longer the account nonce %d; refusing replacement", entry.Nonce, nonce)
	}
	if err := requireJournalFundingBalance(ctx, params.rpc, entry); err != nil {
		return err
	}
	signed := signedFundingTransaction{raw: entry.RawTransaction, hash: entry.TransactionHash}
	if err := sendRawTransaction(ctx, params.rpc, signed); err != nil {
		recovered, recoverErr := recoverSignedTransaction(ctx, params.rpc, entry)
		if recoverErr != nil {
			return errors.Join(err, recoverErr)
		}
		if !recovered {
			if isAmbiguousSendError(err) {
				return fmt.Errorf("ambiguous exact rebroadcast outcome for %s: %w", entry.TransactionHash, err)
			}
			return err
		}
	}
	return nil
}

func requireJournalFundingBalance(ctx context.Context, rpc rpcCaller, entry journalEntry) error {
	maxFee, ok := canonicalDecimalBig(entry.MaxFeePerGas)
	if !ok {
		return fmt.Errorf("journal max fee is invalid")
	}
	required := new(big.Int).Add(new(big.Int).Set(fundingAmountWei), new(big.Int).Mul(new(big.Int).SetUint64(entry.GasLimit), maxFee))
	return requireNativeFundingBudget(ctx, rpc, entry.Source, required)
}

func revalidateConfirmedEntry(ctx context.Context, params fundingRunParams, entry journalEntry) error {
	receipt, err := readReceipt(ctx, params.rpc, entry.TransactionHash)
	if err != nil || receipt == nil {
		return fmt.Errorf("re-read confirmed receipt: %w", err)
	}
	validated, err := validateFundingReceipt(*receipt, entry)
	if err != nil {
		return err
	}
	transaction, err := readTransaction(ctx, params.rpc, entry.TransactionHash)
	if err != nil || transaction == nil {
		return fmt.Errorf("re-read confirmed transaction: %w", err)
	}
	if err := validateRPCTransaction(*transaction, entry); err != nil {
		return err
	}
	if err := validateMinedTransactionBlock(*transaction, validated); err != nil {
		return err
	}
	if validated.blockNumber != entry.ReceiptBlockNumber || validated.blockHash != entry.ReceiptBlockHash {
		return fmt.Errorf("confirmed receipt no longer matches journal")
	}
	latest, err := readLatestBlock(ctx, params.rpc)
	if err != nil || latest < validated.blockNumber || latest-validated.blockNumber+1 < requiredConfirmations {
		return fmt.Errorf("confirmed receipt no longer has %d confirmations", requiredConfirmations)
	}
	blockHash, err := readBlockHash(ctx, params.rpc, validated.blockNumber)
	if err != nil || blockHash != validated.blockHash {
		return fmt.Errorf("confirmed receipt block is no longer canonical")
	}
	return nil
}

func validateMinedTransactionBlock(transaction rpcTransaction, receipt validatedReceipt) error {
	blockHash, err := normalizeFixedHex(transaction.BlockHash, 32, "transaction block hash")
	if err != nil || blockHash != receipt.blockHash {
		return fmt.Errorf("mined transaction block hash mismatch")
	}
	blockNumber, err := parseQuantityUint64(transaction.BlockNumber, "transaction block number")
	if err != nil || blockNumber != receipt.blockNumber {
		return fmt.Errorf("mined transaction block number mismatch")
	}
	return nil
}

func saveJournal(params fundingRunParams, journal *fundingJournal) error {
	journal.UpdatedAt = params.clock.Now().UTC()
	if err := validateFundingJournal(*journal, params.expectedPlan); err != nil {
		return fmt.Errorf("refuse invalid funding journal transition: %w", err)
	}
	return params.journal.Save(*journal)
}

func targetResult(entry journalEntry) fundingTargetResult {
	return fundingTargetResult{
		ExecutionAccountID: entry.ExecutionAccountID, Recipient: entry.Recipient,
		BalanceBefore: entry.RecipientBalanceBefore, Nonce: entry.Nonce, GasLimit: entry.GasLimit,
		MaxPriorityFeePerGas: entry.MaxPriorityFeePerGas, MaxFeePerGas: entry.MaxFeePerGas,
		State: entry.State, TransactionHash: entry.TransactionHash,
		ReceiptBlockNumber: entry.ReceiptBlockNumber, Confirmations: entry.Confirmations,
		BalanceAfter: entry.RecipientBalanceAfter,
	}
}

func canonicalDecimalBig(value string) (*big.Int, bool) {
	parsed, ok := parseCanonicalNonNegativeDecimal(value)
	return parsed, ok && parsed.Sign() > 0
}

func parseCanonicalNonNegativeDecimal(value string) (*big.Int, bool) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return nil, false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return nil, false
		}
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	return parsed, ok && parsed.Sign() >= 0 && parsed.BitLen() <= 256
}

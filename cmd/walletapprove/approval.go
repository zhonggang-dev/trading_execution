package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
)

type approvalRunParams struct {
	rpc          rpcCaller
	accounts     map[string]approvalAccount
	journal      journalStore
	clock        clock
	pollInterval time.Duration
	execute      bool
	expectedPlan approvalJournal
}

type approvalRunResult struct {
	DryRun                bool                   `json:"dry_run"`
	ChainID               uint64                 `json:"chain_id"`
	TokenAddress          string                 `json:"token_address"`
	AmountBaseUnits       string                 `json:"amount_base_units"`
	RequiredConfirmations uint64                 `json:"required_confirmations"`
	Targets               []approvalTargetResult `json:"targets"`
}

type approvalTargetResult struct {
	ExecutionAccountID   string `json:"execution_account_id"`
	Owner                string `json:"owner"`
	Spender              string `json:"spender"`
	AllowanceBefore      string `json:"allowance_before"`
	NeedsTransaction     bool   `json:"needs_transaction"`
	Nonce                uint64 `json:"nonce,omitempty"`
	GasLimit             uint64 `json:"gas_limit,omitempty"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas,omitempty"`
	MaxFeePerGas         string `json:"max_fee_per_gas,omitempty"`
	State                string `json:"state"`
	TransactionHash      string `json:"transaction_hash,omitempty"`
	ReceiptBlockNumber   uint64 `json:"receipt_block_number,omitempty"`
	Confirmations        uint64 `json:"confirmations,omitempty"`
	AllowanceAfter       string `json:"allowance_after,omitempty"`
}

func runApprovals(ctx context.Context, params approvalRunParams) (approvalRunResult, error) {
	if params.clock == nil {
		return approvalRunResult{}, fmt.Errorf("approval clock is required")
	}
	expectedJournal := initialApprovalJournal(params.clock.Now())
	return runApprovalPlan(ctx, params, expectedJournal)
}

// runApprovalPlan is split from runApprovals so tests can execute the complete
// state machine with a non-production private key. Production callers always
// enter through runApprovals, which supplies only the fixed wallet-6/7 plan.
func runApprovalPlan(ctx context.Context, params approvalRunParams, expectedJournal approvalJournal) (approvalRunResult, error) {
	if params.rpc == nil || params.clock == nil {
		return approvalRunResult{}, fmt.Errorf("approval RPC and clock are required")
	}
	if params.execute && params.journal == nil {
		return approvalRunResult{}, fmt.Errorf("execute mode requires a durable journal")
	}
	if params.pollInterval <= 0 {
		params.pollInterval = defaultReceiptPollInterval
	}
	if err := readChainID(ctx, params.rpc); err != nil {
		return approvalRunResult{}, err
	}
	if err := validateApprovalJournal(expectedJournal, expectedJournal); err != nil {
		return approvalRunResult{}, fmt.Errorf("approval plan is invalid: %w", err)
	}
	params.expectedPlan = expectedJournal
	journal := expectedJournal
	var err error
	if params.execute {
		journal, err = params.journal.LoadOrCreate(expectedJournal)
		if err != nil {
			return approvalRunResult{}, err
		}
	}
	result := approvalRunResult{
		DryRun: !params.execute, ChainID: polygonChainID, TokenAddress: polygonPUSDAddress,
		AmountBaseUnits: approvalAmount.String(), RequiredConfirmations: requiredConfirmations,
		Targets: make([]approvalTargetResult, 0, len(journal.Entries)),
	}
	for index, entry := range journal.Entries {
		account, exists := params.accounts[entry.ExecutionAccountID]
		if !exists || account.address != entry.Owner || account.signer == nil {
			return result, fmt.Errorf("approval entry %d account identity is unavailable", index)
		}
	}
	if params.execute {
		if err := preflightPendingGasBudget(ctx, params, journal); err != nil {
			return result, fmt.Errorf("preflight fixed approval plan: %w", err)
		}
	}
	dryRunNextNonce := make(map[string]uint64)
	for index := range journal.Entries {
		entry := &journal.Entries[index]
		account := params.accounts[entry.ExecutionAccountID]
		if params.execute {
			if err := executeJournalEntry(ctx, params, &journal, index, account); err != nil {
				return result, fmt.Errorf("approve %s for %s: %w", entry.Spender, entry.ExecutionAccountID, err)
			}
		} else {
			if err := dryRunJournalEntry(ctx, params, entry, account, dryRunNextNonce); err != nil {
				return result, fmt.Errorf("dry-run approve %s for %s: %w", entry.Spender, entry.ExecutionAccountID, err)
			}
		}
		result.Targets = append(result.Targets, targetResult(*entry))
	}
	if params.execute {
		finalBlock, err := readLatestBlock(ctx, params.rpc)
		if err != nil {
			return result, err
		}
		for _, entry := range journal.Entries {
			allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, quantityHex(finalBlock))
			if err != nil {
				return result, err
			}
			if allowance.Cmp(approvalAmount) != 0 {
				return result, fmt.Errorf("final allowance for %s/%s is %s; require exactly %s", entry.ExecutionAccountID, entry.Spender, allowance, approvalAmount)
			}
		}
	}
	return result, nil
}

func preflightPendingGasBudget(ctx context.Context, params approvalRunParams, journal approvalJournal) error {
	requiredByOwner := make(map[string]*big.Int)
	for _, entry := range journal.Entries {
		if entry.State != journalStatePending {
			continue
		}
		allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, "latest")
		if err != nil {
			return fmt.Errorf("read %s/%s allowance: %w", entry.ExecutionAccountID, entry.Spender, err)
		}
		alreadyApproved, err := classifyInitialAllowance(allowance)
		if err != nil {
			return fmt.Errorf("%s/%s: %w", entry.ExecutionAccountID, entry.Spender, err)
		}
		if alreadyApproved {
			continue
		}
		if err := simulateApproval(ctx, params.rpc, entry.Owner, entry.Spender); err != nil {
			return fmt.Errorf("simulate %s/%s: %w", entry.ExecutionAccountID, entry.Spender, err)
		}
		gasLimit, err := estimateApprovalGas(ctx, params.rpc, entry.Owner, entry.Spender)
		if err != nil {
			return fmt.Errorf("estimate %s/%s: %w", entry.ExecutionAccountID, entry.Spender, err)
		}
		_, maxFee, err := readFees(ctx, params.rpc)
		if err != nil {
			return fmt.Errorf("fees for %s/%s: %w", entry.ExecutionAccountID, entry.Spender, err)
		}
		maximumCost := new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), maxFee)
		if requiredByOwner[entry.Owner] == nil {
			requiredByOwner[entry.Owner] = new(big.Int)
		}
		requiredByOwner[entry.Owner].Add(requiredByOwner[entry.Owner], maximumCost)
		if requiredByOwner[entry.Owner].BitLen() > 256 {
			return fmt.Errorf("maximum gas budget for %s overflows uint256", entry.ExecutionAccountID)
		}
	}
	for owner, required := range requiredByOwner {
		if err := requireNativeGasBudget(ctx, params.rpc, owner, required); err != nil {
			return err
		}
	}
	return nil
}

func classifyInitialAllowance(allowance *big.Int) (bool, error) {
	if allowance == nil || allowance.Sign() < 0 {
		return false, fmt.Errorf("pUSD allowance is invalid")
	}
	if allowance.Cmp(approvalAmount) == 0 {
		return true, nil
	}
	if allowance.Sign() != 0 {
		return false, fmt.Errorf(
			"existing pUSD allowance %s is non-zero and differs from required exact allowance %s; refusing overwrite",
			allowance, approvalAmount,
		)
	}
	return false, nil
}

func dryRunJournalEntry(
	ctx context.Context,
	params approvalRunParams,
	entry *journalEntry,
	account approvalAccount,
	nextNonce map[string]uint64,
) error {
	allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, "latest")
	if err != nil {
		return err
	}
	entry.AllowanceBefore = allowance.String()
	alreadyApproved, err := classifyInitialAllowance(allowance)
	if err != nil {
		return err
	}
	if alreadyApproved {
		entry.State = journalStateAlreadyApproved
		entry.AllowanceAfter = approvalAmount.String()
		return nil
	}
	nonce, hasNonce := nextNonce[entry.Owner]
	if !hasNonce {
		nonce, err = readNonce(ctx, params.rpc, entry.Owner)
		if err != nil {
			return err
		}
	}
	if nonce == math.MaxUint64 {
		return fmt.Errorf("account nonce overflows")
	}
	nextNonce[entry.Owner] = nonce + 1
	if err := simulateApproval(ctx, params.rpc, entry.Owner, entry.Spender); err != nil {
		return err
	}
	gasLimit, err := estimateApprovalGas(ctx, params.rpc, entry.Owner, entry.Spender)
	if err != nil {
		return err
	}
	priorityFee, maxFee, err := readFees(ctx, params.rpc)
	if err != nil {
		return err
	}
	if err := requireNativeGasBalance(ctx, params.rpc, entry.Owner, gasLimit, maxFee); err != nil {
		return err
	}
	transaction, err := newApprovalTransaction(nonce, gasLimit, priorityFee, maxFee, entry.Spender)
	if err != nil {
		return err
	}
	entry.Nonce = nonce
	entry.GasLimit = gasLimit
	entry.MaxPriorityFeePerGas = priorityFee.String()
	entry.MaxFeePerGas = maxFee.String()
	entry.CallData = "0x" + hex.EncodeToString(transaction.data)
	entry.State = journalStatePending
	_ = account // proves the fixed target account was selected without invoking its signer.
	return nil
}

func executeJournalEntry(
	ctx context.Context,
	params approvalRunParams,
	journal *approvalJournal,
	index int,
	account approvalAccount,
) error {
	entry := &journal.Entries[index]
	switch entry.State {
	case journalStatePending:
		allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, "latest")
		if err != nil {
			return err
		}
		entry.AllowanceBefore = allowance.String()
		alreadyApproved, err := classifyInitialAllowance(allowance)
		if err != nil {
			return err
		}
		if alreadyApproved {
			entry.State = journalStateAlreadyApproved
			entry.AllowanceAfter = approvalAmount.String()
			return saveJournal(params, journal)
		}
		nonce, err := readNonce(ctx, params.rpc, entry.Owner)
		if err != nil {
			return err
		}
		if err := simulateApproval(ctx, params.rpc, entry.Owner, entry.Spender); err != nil {
			return err
		}
		gasLimit, err := estimateApprovalGas(ctx, params.rpc, entry.Owner, entry.Spender)
		if err != nil {
			return err
		}
		priorityFee, maxFee, err := readFees(ctx, params.rpc)
		if err != nil {
			return err
		}
		if err := requireNativeGasBalance(ctx, params.rpc, entry.Owner, gasLimit, maxFee); err != nil {
			return err
		}
		transaction, err := newApprovalTransaction(nonce, gasLimit, priorityFee, maxFee, entry.Spender)
		if err != nil {
			return err
		}
		signed, err := signType2Approval(ctx, transaction, account)
		if err != nil {
			return err
		}
		entry.Nonce = nonce
		entry.GasLimit = gasLimit
		entry.MaxPriorityFeePerGas = priorityFee.String()
		entry.MaxFeePerGas = maxFee.String()
		entry.CallData = "0x" + hex.EncodeToString(transaction.data)
		entry.SigningDigest = signed.digest
		entry.RawTransaction = signed.raw
		entry.TransactionHash = signed.hash
		entry.SignedAt = params.clock.Now().UTC()
		entry.State = journalStateSigned
		if err := saveJournal(params, journal); err != nil {
			return fmt.Errorf("persist signed transaction before broadcast: %w", err)
		}
	case journalStateAlreadyApproved:
		allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, "latest")
		if err != nil {
			return err
		}
		if allowance.Cmp(approvalAmount) != 0 {
			return fmt.Errorf("previously exact allowance changed to %s", allowance)
		}
		return nil
	case journalStateSignedApproved:
		allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, "latest")
		if err != nil {
			return err
		}
		if allowance.Cmp(approvalAmount) != 0 {
			return fmt.Errorf("signed-but-not-broadcast approval is no longer exact: %s", allowance)
		}
		return nil
	case journalStateConfirmed:
		return revalidateConfirmedEntry(ctx, params, *entry)
	}

	if entry.State != journalStateSigned && entry.State != journalStateBroadcast {
		return fmt.Errorf("unsupported executable journal state %q", entry.State)
	}
	if err := verifySignedJournalEntry(ctx, *entry, account); err != nil {
		return err
	}
	if entry.State == journalStateSigned {
		found, err := recoverSignedTransaction(ctx, params.rpc, *entry)
		if err != nil {
			return err
		}
		if !found {
			nonce, err := readNonce(ctx, params.rpc, entry.Owner)
			if err != nil {
				return err
			}
			if nonce != entry.Nonce {
				return fmt.Errorf("signed nonce %d is no longer the account nonce %d; refusing replacement", entry.Nonce, nonce)
			}
			allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, "latest")
			if err != nil {
				return err
			}
			alreadyApproved, err := classifyInitialAllowance(allowance)
			if err != nil {
				return err
			}
			if alreadyApproved {
				entry.State = journalStateSignedApproved
				entry.AllowanceAfter = approvalAmount.String()
				return saveJournal(params, journal)
			}
			if err := requireJournalGasBalance(ctx, params.rpc, *entry); err != nil {
				return err
			}
			signed := signedApprovalTransaction{raw: entry.RawTransaction, hash: entry.TransactionHash}
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
	entry.AllowanceAfter = approvalAmount.String()
	entry.ConfirmedAt = params.clock.Now().UTC()
	return saveJournal(params, journal)
}

func verifySignedJournalEntry(ctx context.Context, entry journalEntry, account approvalAccount) error {
	priorityFee, ok := canonicalDecimalBig(entry.MaxPriorityFeePerGas)
	if !ok {
		return fmt.Errorf("journal priority fee is invalid")
	}
	maxFee, ok := canonicalDecimalBig(entry.MaxFeePerGas)
	if !ok {
		return fmt.Errorf("journal max fee is invalid")
	}
	transaction, err := newApprovalTransaction(entry.Nonce, entry.GasLimit, priorityFee, maxFee, entry.Spender)
	if err != nil {
		return err
	}
	callData := "0x" + hex.EncodeToString(transaction.data)
	if callData != entry.CallData {
		return fmt.Errorf("journal calldata does not match fixed approval")
	}
	signed, err := signType2Approval(ctx, transaction, account)
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
		if _, err := validateApprovalReceipt(*receipt, entry); err != nil {
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
	receipt       validatedReceipt
	confirmations uint64
}

func waitForFinalReceipt(ctx context.Context, params approvalRunParams, entry journalEntry) (finalReceipt, error) {
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
			} else {
				if err := rebroadcastMissingTransaction(ctx, params, entry); err != nil {
					return finalReceipt{}, err
				}
			}
			if err := params.clock.Sleep(ctx, params.pollInterval); err != nil {
				return finalReceipt{}, err
			}
			continue
		}
		validated, err := validateApprovalReceipt(*receipt, entry)
		if err != nil {
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
		if err != nil {
			return finalReceipt{}, err
		}
		if canonicalBlockHash != validated.blockHash {
			return finalReceipt{}, fmt.Errorf("receipt block hash changed after confirmation")
		}
		secondReceipt, err := readReceipt(ctx, params.rpc, entry.TransactionHash)
		if err != nil {
			return finalReceipt{}, fmt.Errorf("re-read confirmed receipt: %w", err)
		}
		if secondReceipt == nil {
			return finalReceipt{}, fmt.Errorf("confirmed receipt disappeared")
		}
		secondValidated, err := validateApprovalReceipt(*secondReceipt, entry)
		if err != nil {
			return finalReceipt{}, fmt.Errorf("confirmed receipt changed: %w", err)
		}
		if secondValidated != validated {
			return finalReceipt{}, fmt.Errorf("confirmed receipt changed across reads")
		}
		secondLatest, err := readLatestBlock(ctx, params.rpc)
		if err != nil {
			return finalReceipt{}, err
		}
		if secondLatest < latest {
			return finalReceipt{}, fmt.Errorf("RPC latest block regressed from %d to %d", latest, secondLatest)
		}
		secondConfirmations := secondLatest - validated.blockNumber + 1
		if secondConfirmations < requiredConfirmations {
			return finalReceipt{}, fmt.Errorf("confirmation count fell below %d", requiredConfirmations)
		}
		allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, quantityHex(secondLatest))
		if err != nil {
			return finalReceipt{}, err
		}
		if allowance.Cmp(approvalAmount) != 0 {
			return finalReceipt{}, fmt.Errorf("confirmed allowance is %s; require exactly %s", allowance, approvalAmount)
		}
		return finalReceipt{receipt: validated, confirmations: secondConfirmations}, nil
	}
}

func rebroadcastMissingTransaction(ctx context.Context, params approvalRunParams, entry journalEntry) error {
	nonce, err := readNonce(ctx, params.rpc, entry.Owner)
	if err != nil {
		return err
	}
	if nonce != entry.Nonce {
		return fmt.Errorf("broadcast nonce %d is no longer the account nonce %d; refusing replacement", entry.Nonce, nonce)
	}
	allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, "latest")
	if err != nil {
		return err
	}
	alreadyApproved, err := classifyInitialAllowance(allowance)
	if err != nil {
		return err
	}
	if alreadyApproved {
		return fmt.Errorf("broadcast transaction is unavailable but allowance is already exact; refusing blind rebroadcast")
	}
	if err := requireJournalGasBalance(ctx, params.rpc, entry); err != nil {
		return err
	}
	signed := signedApprovalTransaction{raw: entry.RawTransaction, hash: entry.TransactionHash}
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

func requireJournalGasBalance(ctx context.Context, rpc rpcCaller, entry journalEntry) error {
	maxFee, ok := canonicalDecimalBig(entry.MaxFeePerGas)
	if !ok {
		return fmt.Errorf("journal max fee is invalid")
	}
	return requireNativeGasBalance(ctx, rpc, entry.Owner, entry.GasLimit, maxFee)
}

func revalidateConfirmedEntry(ctx context.Context, params approvalRunParams, entry journalEntry) error {
	receipt, err := readReceipt(ctx, params.rpc, entry.TransactionHash)
	if err != nil {
		return fmt.Errorf("re-read confirmed receipt: %w", err)
	}
	if receipt == nil {
		return fmt.Errorf("confirmed receipt disappeared")
	}
	validated, err := validateApprovalReceipt(*receipt, entry)
	if err != nil {
		return err
	}
	if validated.blockNumber != entry.ReceiptBlockNumber || validated.blockHash != entry.ReceiptBlockHash {
		return fmt.Errorf("confirmed receipt no longer matches journal")
	}
	latest, err := readLatestBlock(ctx, params.rpc)
	if err != nil {
		return err
	}
	if latest < validated.blockNumber || latest-validated.blockNumber+1 < requiredConfirmations {
		return fmt.Errorf("confirmed receipt no longer has %d confirmations", requiredConfirmations)
	}
	blockHash, err := readBlockHash(ctx, params.rpc, validated.blockNumber)
	if err != nil || blockHash != validated.blockHash {
		return fmt.Errorf("confirmed receipt block is no longer canonical")
	}
	allowance, err := readAllowance(ctx, params.rpc, entry.Owner, entry.Spender, quantityHex(latest))
	if err != nil {
		return err
	}
	if allowance.Cmp(approvalAmount) != 0 {
		return fmt.Errorf("confirmed allowance changed to %s", allowance)
	}
	return nil
}

func saveJournal(params approvalRunParams, journal *approvalJournal) error {
	journal.UpdatedAt = params.clock.Now().UTC()
	if err := validateApprovalJournal(*journal, params.expectedPlan); err != nil {
		return fmt.Errorf("refuse invalid approval journal transition: %w", err)
	}
	return params.journal.Save(*journal)
}

func targetResult(entry journalEntry) approvalTargetResult {
	return approvalTargetResult{
		ExecutionAccountID:   entry.ExecutionAccountID,
		Owner:                entry.Owner,
		Spender:              entry.Spender,
		AllowanceBefore:      entry.AllowanceBefore,
		NeedsTransaction:     entry.State != journalStateAlreadyApproved && entry.State != journalStateSignedApproved,
		Nonce:                entry.Nonce,
		GasLimit:             entry.GasLimit,
		MaxPriorityFeePerGas: entry.MaxPriorityFeePerGas,
		MaxFeePerGas:         entry.MaxFeePerGas,
		State:                entry.State,
		TransactionHash:      entry.TransactionHash,
		ReceiptBlockNumber:   entry.ReceiptBlockNumber,
		Confirmations:        entry.Confirmations,
		AllowanceAfter:       entry.AllowanceAfter,
	}
}

func canonicalDecimalBig(value string) (*big.Int, bool) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return nil, false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return nil, false
		}
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	return parsed, ok && parsed.Sign() > 0 && parsed.BitLen() <= 256
}

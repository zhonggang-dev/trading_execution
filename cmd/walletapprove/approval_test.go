package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

type memoryJournalStore struct {
	current approvalJournal
	has     bool
	saves   []approvalJournal
}

func (store *memoryJournalStore) LoadOrCreate(expected approvalJournal) (approvalJournal, error) {
	if !store.has {
		store.current = cloneJournal(expected)
		store.has = true
	}
	return cloneJournal(store.current), nil
}

func (store *memoryJournalStore) Save(journal approvalJournal) error {
	store.current = cloneJournal(journal)
	store.has = true
	store.saves = append(store.saves, cloneJournal(journal))
	return nil
}

func cloneJournal(journal approvalJournal) approvalJournal {
	journal.Entries = append([]journalEntry(nil), journal.Entries...)
	return journal
}

type executionRPC struct {
	store              *memoryJournalStore
	owner              string
	spender            string
	sent               bool
	sendErr            error
	sendCalls          int
	allowanceBeforeHex string
	nativeBalanceHex   string
	nativeBalanceCalls int
	nonceHex           string
	receiptUnavailable bool
	transactionVisible bool
}

func (rpc *executionRPC) Call(_ context.Context, method string, params []any) ([]byte, error) {
	switch method {
	case "eth_chainId":
		return []byte(`"0x89"`), nil
	case "eth_getTransactionCount":
		if rpc.nonceHex != "" {
			return json.Marshal(rpc.nonceHex)
		}
		return []byte(`"0x7"`), nil
	case "eth_call":
		call := params[0].(map[string]string)
		data := strings.TrimPrefix(call["data"], "0x")
		if strings.HasPrefix(data, "095ea7b3") {
			return json.Marshal("0x" + strings.Repeat("0", 63) + "1")
		}
		if strings.HasPrefix(data, "dd62ed3e") {
			value := "0x" + strings.Repeat("0", 64)
			if rpc.sent {
				value = fmt.Sprintf("0x%064x", approvalAmount)
			} else if rpc.allowanceBeforeHex != "" {
				value = rpc.allowanceBeforeHex
			}
			return json.Marshal(value)
		}
		return nil, fmt.Errorf("unexpected eth_call data")
	case "eth_estimateGas":
		return []byte(`"0xc350"`), nil
	case "eth_maxPriorityFeePerGas":
		return []byte(`"0x3b9aca00"`), nil
	case "eth_getBlockByNumber":
		if params[0] == "pending" {
			return []byte(`{"baseFeePerGas":"0x3b9aca00"}`), nil
		}
		return json.Marshal(map[string]string{
			"number": "0x64", "hash": "0x" + strings.Repeat("55", 32),
		})
	case "eth_getBalance":
		rpc.nativeBalanceCalls++
		if rpc.nativeBalanceHex != "" {
			return json.Marshal(rpc.nativeBalanceHex)
		}
		return []byte(`"0xde0b6b3a7640000"`), nil
	case "eth_getTransactionReceipt":
		if !rpc.sent || rpc.receiptUnavailable {
			return []byte("null"), nil
		}
		entry := rpc.store.current.Entries[0]
		return json.Marshal(executionReceipt(entry))
	case "eth_getTransactionByHash":
		if rpc.sent && rpc.transactionVisible {
			return json.Marshal(executionTransaction(rpc.store.current.Entries[0]))
		}
		return []byte("null"), nil
	case "eth_sendRawTransaction":
		rpc.sendCalls++
		if !rpc.store.has || len(rpc.store.current.Entries) != 1 ||
			(rpc.store.current.Entries[0].State != journalStateSigned && rpc.store.current.Entries[0].State != journalStateBroadcast) {
			return nil, fmt.Errorf("transaction was not journaled as SIGNED/BROADCAST before send")
		}
		raw := params[0].(string)
		if raw != rpc.store.current.Entries[0].RawTransaction {
			return nil, fmt.Errorf("sent raw transaction differs from journal")
		}
		if rpc.sendErr != nil {
			return nil, rpc.sendErr
		}
		rawBytes, _ := hex.DecodeString(strings.TrimPrefix(raw, "0x"))
		hash := "0x" + hex.EncodeToString(keccak256(rawBytes))
		if hash != rpc.store.current.Entries[0].TransactionHash {
			return nil, fmt.Errorf("sent hash mismatch")
		}
		rpc.sent = true
		return json.Marshal(hash)
	case "eth_blockNumber":
		return []byte(`"0xa3"`), nil
	default:
		return nil, fmt.Errorf("unexpected execution RPC method %s", method)
	}
}

func executionTransaction(entry journalEntry) rpcTransaction {
	return rpcTransaction{
		Hash:                 entry.TransactionHash,
		ChainID:              "0x89",
		Type:                 "0x2",
		From:                 entry.Owner,
		To:                   polygonPUSDAddress,
		Nonce:                quantityHex(entry.Nonce),
		Gas:                  quantityHex(entry.GasLimit),
		MaxPriorityFeePerGas: "0x" + mustDecimalBig(entry.MaxPriorityFeePerGas).Text(16),
		MaxFeePerGas:         "0x" + mustDecimalBig(entry.MaxFeePerGas).Text(16),
		Value:                "0x0",
		Input:                entry.CallData,
	}
}

func mustDecimalBig(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok {
		panic("invalid test decimal")
	}
	return parsed
}

func executionReceipt(entry journalEntry) rpcReceipt {
	removed := false
	eventTopic := "0x" + hex.EncodeToString(keccak256([]byte("Approval(address,address,uint256)")))
	ownerTopic, _ := addressTopic(entry.Owner)
	spenderTopic, _ := addressTopic(entry.Spender)
	blockHash := "0x" + strings.Repeat("55", 32)
	return rpcReceipt{
		TransactionHash:   entry.TransactionHash,
		BlockHash:         blockHash,
		BlockNumber:       "0x64",
		From:              entry.Owner,
		To:                polygonPUSDAddress,
		Type:              "0x2",
		Status:            "0x1",
		GasUsed:           "0xc350",
		EffectiveGasPrice: "0x3b9aca00",
		Logs: []rpcLog{{
			Address:         polygonPUSDAddress,
			Topics:          []string{eventTopic, ownerTopic, spenderTopic},
			Data:            fmt.Sprintf("0x%064x", approvalAmount),
			BlockHash:       blockHash,
			BlockNumber:     "0x64",
			TransactionHash: entry.TransactionHash,
			LogIndex:        "0x0",
			Removed:         &removed,
		}},
	}
}

func testExecutionPlan(now time.Time, owner string) approvalJournal {
	return approvalJournal{
		SchemaVersion:         approvalJournalSchema,
		PlanID:                "test-only-one-account-plan",
		ChainID:               polygonChainID,
		TokenAddress:          polygonPUSDAddress,
		AmountBaseUnits:       approvalAmount.String(),
		RequiredConfirmations: requiredConfirmations,
		CreatedAt:             now,
		UpdatedAt:             now,
		Entries: []journalEntry{{
			ExecutionAccountID: "test-wallet",
			Owner:              owner,
			Spender:            standardExchangeV2Address,
			State:              journalStatePending,
		}},
	}
}

func testTwoSpenderExecutionPlan(now time.Time, owner string) approvalJournal {
	journal := testExecutionPlan(now, owner)
	journal.Entries = append(journal.Entries, journalEntry{
		ExecutionAccountID: "test-wallet",
		Owner:              owner,
		Spender:            negRiskExchangeV2Address,
		State:              journalStatePending,
	})
	return journal
}

func testSignedExecutionJournal(
	t *testing.T,
	now time.Time,
	signer polymarket.DigestSigner,
	priorityFee *big.Int,
	maxFee *big.Int,
) approvalJournal {
	t.Helper()
	data, err := buildApproveCallData(standardExchangeV2Address, approvalAmount)
	if err != nil {
		t.Fatal(err)
	}
	transaction := approvalTransaction{
		chainID: polygonChainID, nonce: 7,
		maxPriorityFeePerGas: new(big.Int).Set(priorityFee),
		maxFeePerGas:         new(big.Int).Set(maxFee),
		gasLimit:             60_000,
		to:                   polygonPUSDAddress,
		value:                new(big.Int),
		data:                 data,
	}
	account := approvalAccount{executionAccountID: "test-wallet", address: signer.Address(), signer: signer}
	signed, err := signType2Approval(context.Background(), transaction, account)
	if err != nil {
		t.Fatal(err)
	}
	journal := testExecutionPlan(now, signer.Address())
	entry := &journal.Entries[0]
	entry.State = journalStateSigned
	entry.AllowanceBefore = "0"
	entry.Nonce = transaction.nonce
	entry.GasLimit = transaction.gasLimit
	entry.MaxPriorityFeePerGas = transaction.maxPriorityFeePerGas.String()
	entry.MaxFeePerGas = transaction.maxFeePerGas.String()
	entry.CallData = "0x" + hex.EncodeToString(transaction.data)
	entry.SigningDigest = signed.digest
	entry.RawTransaction = signed.raw
	entry.TransactionHash = signed.hash
	entry.SignedAt = now
	return journal
}

type boundedSleepClock struct {
	now       time.Time
	remaining int
}

func (clock *boundedSleepClock) Now() time.Time { return clock.now }
func (clock *boundedSleepClock) Sleep(context.Context, time.Duration) error {
	if clock.remaining == 0 {
		return fmt.Errorf("unexpected extra sleep")
	}
	clock.remaining--
	return nil
}

func TestExecutePreflightRejectsNonZeroAllowanceThatIsNotExact(t *testing.T) {
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	store := &memoryJournalStore{}
	signer := &failSigner{address: wallet6ExpectedAddress}
	rpc := &executionRPC{
		store: store, allowanceBeforeHex: "0x" + strings.Repeat("0", 63) + "1",
	}
	_, err := runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: wallet6ExpectedAddress, signer: signer},
		},
		journal: store, clock: fixedClock{now: now}, pollInterval: time.Millisecond, execute: true,
	}, testExecutionPlan(now, wallet6ExpectedAddress))
	if err == nil || !strings.Contains(err.Error(), "non-zero and differs from required exact allowance 200000000") {
		t.Fatalf("non-exact allowance error = %v", err)
	}
	if signer.calls != 0 || rpc.sendCalls != 0 || len(store.saves) != 0 {
		t.Fatalf("rejected execute signer/send/saves = %d/%d/%d", signer.calls, rpc.sendCalls, len(store.saves))
	}
}

func TestExecutePreflightRequiresCumulativeGasBudgetBeforeAnyBroadcast(t *testing.T) {
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	store := &memoryJournalStore{}
	signer := &failSigner{address: wallet6ExpectedAddress}
	rpc := &executionRPC{
		store: store,
		// One planned transaction costs at most 60,000 * 3 gwei = 180e12 wei.
		// This balance covers one but not both pending approvals for the owner.
		nativeBalanceHex: fmt.Sprintf("0x%x", uint64(200_000_000_000_000)),
	}
	_, err := runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: wallet6ExpectedAddress, signer: signer},
		},
		journal: store, clock: fixedClock{now: now}, pollInterval: time.Millisecond, execute: true,
	}, testTwoSpenderExecutionPlan(now, wallet6ExpectedAddress))
	if err == nil || !strings.Contains(err.Error(), "required maximum gas budget 360000000000000") {
		t.Fatalf("cumulative gas budget error = %v", err)
	}
	if signer.calls != 0 || rpc.sendCalls != 0 || len(store.saves) != 0 || rpc.nativeBalanceCalls != 1 {
		t.Fatalf(
			"cumulative preflight signer/send/saves/balance calls = %d/%d/%d/%d",
			signer.calls, rpc.sendCalls, len(store.saves), rpc.nativeBalanceCalls,
		)
	}
}

func TestExecuteStateMachineJournalsBeforeSendAndConfirmsExactAllowance(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	store := &memoryJournalStore{}
	rpc := &executionRPC{store: store, owner: signer.Address(), spender: standardExchangeV2Address}
	result, err := runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal:      store,
		clock:        fixedClock{now: now},
		pollInterval: time.Millisecond,
		execute:      true,
	}, testExecutionPlan(now, signer.Address()))
	if err != nil {
		t.Fatal(err)
	}
	if result.DryRun || len(result.Targets) != 1 || result.Targets[0].State != journalStateConfirmed ||
		result.Targets[0].Confirmations != 64 || result.Targets[0].AllowanceAfter != approvalAmount.String() {
		t.Fatalf("execution result = %#v", result)
	}
	states := make([]string, len(store.saves))
	for index, saved := range store.saves {
		states[index] = saved.Entries[0].State
	}
	if strings.Join(states, ",") != "SIGNED,BROADCAST,CONFIRMED" {
		t.Fatalf("journal states = %v", states)
	}
}

func TestSignedJournalRestoreRejectsFeeAboveCapBeforeBroadcast(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	overCap := new(big.Int).Add(new(big.Int).SetUint64(maximumPriorityFeeWei), big.NewInt(1))
	store := &memoryJournalStore{
		current: testSignedExecutionJournal(t, now, signer, overCap, overCap),
		has:     true,
	}
	rpc := &executionRPC{store: store}
	_, err = runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal: store, clock: fixedClock{now: now}, pollInterval: time.Millisecond, execute: true,
	}, testExecutionPlan(now, signer.Address()))
	if err == nil || !strings.Contains(err.Error(), "priority fee") || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("over-cap signed journal error = %v", err)
	}
	if rpc.sendCalls != 0 || len(store.saves) != 0 {
		t.Fatalf("over-cap restore send/saves = %d/%d", rpc.sendCalls, len(store.saves))
	}
}

func TestSignedJournalWithExactAllowanceIsPreservedButNotBroadcast(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	signedJournal := testSignedExecutionJournal(t, now, signer, big.NewInt(1_000_000_000), big.NewInt(3_000_000_000))
	journaledRaw := signedJournal.Entries[0].RawTransaction
	store := &memoryJournalStore{current: signedJournal, has: true}
	rpc := &executionRPC{
		store: store, allowanceBeforeHex: fmt.Sprintf("0x%064x", approvalAmount),
	}
	result, err := runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal: store, clock: fixedClock{now: now}, pollInterval: time.Millisecond, execute: true,
	}, testExecutionPlan(now, signer.Address()))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Targets) != 1 || result.Targets[0].State != journalStateSignedApproved ||
		result.Targets[0].NeedsTransaction || result.Targets[0].AllowanceAfter != approvalAmount.String() {
		t.Fatalf("signed already-approved result = %#v", result)
	}
	if rpc.sendCalls != 0 || store.current.Entries[0].RawTransaction != journaledRaw ||
		store.current.Entries[0].State != journalStateSignedApproved || !store.current.Entries[0].BroadcastAt.IsZero() {
		t.Fatalf("signed already-approved journal/send = %#v / %d", store.current.Entries[0], rpc.sendCalls)
	}
}

func TestDroppedBroadcastReusesExactRawAndRejectsChangedNonce(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	signedJournal := testSignedExecutionJournal(t, now, signer, big.NewInt(1_000_000_000), big.NewInt(3_000_000_000))
	journaledRaw := signedJournal.Entries[0].RawTransaction
	journaledHash := signedJournal.Entries[0].TransactionHash
	store := &memoryJournalStore{current: signedJournal, has: true}
	rpc := &executionRPC{store: store, receiptUnavailable: true, transactionVisible: true}
	params := approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal: store, clock: fixedClock{now: now}, pollInterval: time.Millisecond, execute: true,
	}
	_, err = runApprovalPlan(context.Background(), params, testExecutionPlan(now, signer.Address()))
	if err == nil || !strings.Contains(err.Error(), "unexpected sleep") {
		t.Fatalf("first broadcast wait error = %v", err)
	}
	if store.current.Entries[0].State != journalStateBroadcast || rpc.sendCalls != 1 {
		t.Fatalf("initial broadcast state/send = %s/%d", store.current.Entries[0].State, rpc.sendCalls)
	}

	// Simulate the exact transaction disappearing from both receipt and transaction lookup.
	rpc.sent = false
	rpc.receiptUnavailable = false
	rpc.transactionVisible = false
	rpc.nonceHex = "0x8"
	_, err = runApprovalPlan(context.Background(), params, testExecutionPlan(now, signer.Address()))
	if err == nil || !strings.Contains(err.Error(), "broadcast nonce 7 is no longer the account nonce 8") {
		t.Fatalf("changed nonce error = %v", err)
	}
	if rpc.sendCalls != 1 {
		t.Fatalf("changed nonce triggered send calls = %d", rpc.sendCalls)
	}

	rpc.nonceHex = "0x7"
	resumeClock := &boundedSleepClock{now: now, remaining: 1}
	params.clock = resumeClock
	result, err := runApprovalPlan(context.Background(), params, testExecutionPlan(now, signer.Address()))
	if err != nil {
		t.Fatal(err)
	}
	if resumeClock.remaining != 0 || rpc.sendCalls != 2 || len(result.Targets) != 1 ||
		result.Targets[0].State != journalStateConfirmed || store.current.Entries[0].RawTransaction != journaledRaw ||
		store.current.Entries[0].TransactionHash != journaledHash {
		t.Fatalf("dropped broadcast resume result/journal/send = %#v / %#v / %d", result, store.current.Entries[0], rpc.sendCalls)
	}
}

func TestAmbiguousSendStopsWithExactSignedTransactionJournaled(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	store := &memoryJournalStore{}
	rpc := &executionRPC{
		store: store, owner: signer.Address(), spender: standardExchangeV2Address,
		sendErr: &rpcCallError{method: "eth_sendRawTransaction", ambiguous: true, cause: fmt.Errorf("connection reset")},
	}
	_, err = runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal:      store,
		clock:        fixedClock{now: now},
		pollInterval: time.Millisecond,
		execute:      true,
	}, testExecutionPlan(now, signer.Address()))
	if err == nil || !strings.Contains(err.Error(), "ambiguous send outcome") {
		t.Fatalf("ambiguous send error = %v", err)
	}
	if !store.has || store.current.Entries[0].State != journalStateSigned || store.current.Entries[0].RawTransaction == "" ||
		store.current.Entries[0].TransactionHash == "" || rpc.sent {
		t.Fatalf("ambiguous journal/rpc = %#v / sent=%v", store.current.Entries[0], rpc.sent)
	}
	journaledRaw := store.current.Entries[0].RawTransaction
	journaledHash := store.current.Entries[0].TransactionHash
	rpc.sendErr = nil
	rpc.allowanceBeforeHex = "0x" + strings.Repeat("0", 63) + "1"
	_, err = runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal: store, clock: fixedClock{now: now}, pollInterval: time.Millisecond, execute: true,
	}, testExecutionPlan(now, signer.Address()))
	if err == nil || !strings.Contains(err.Error(), "non-zero and differs from required exact allowance") || rpc.sendCalls != 1 {
		t.Fatalf("signed non-exact allowance error/send calls = %v/%d", err, rpc.sendCalls)
	}
	rpc.allowanceBeforeHex = ""
	rpc.nativeBalanceHex = "0x1"
	_, err = runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal: store, clock: fixedClock{now: now}, pollInterval: time.Millisecond, execute: true,
	}, testExecutionPlan(now, signer.Address()))
	if err == nil || !strings.Contains(err.Error(), "below required maximum gas budget") || rpc.sendCalls != 1 {
		t.Fatalf("signed low-balance error/send calls = %v/%d", err, rpc.sendCalls)
	}
	rpc.nativeBalanceHex = ""
	result, err := runApprovalPlan(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			"test-wallet": {executionAccountID: "test-wallet", address: signer.Address(), signer: signer},
		},
		journal:      store,
		clock:        fixedClock{now: now},
		pollInterval: time.Millisecond,
		execute:      true,
	}, testExecutionPlan(now, signer.Address()))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Targets) != 1 || result.Targets[0].State != journalStateConfirmed ||
		store.current.Entries[0].RawTransaction != journaledRaw || store.current.Entries[0].TransactionHash != journaledHash ||
		rpc.sendCalls != 2 {
		t.Fatalf("resumed result/journal = %#v / %#v", result, store.current.Entries[0])
	}
}

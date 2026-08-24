package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

type memoryJournal struct {
	current     fundingJournal
	has         bool
	saves       []fundingJournal
	creates     []fundingJournal
	afterCreate func()
}

func cloneFundingJournal(journal fundingJournal) fundingJournal {
	journal.Entries = append([]journalEntry(nil), journal.Entries...)
	return journal
}

func (store *memoryJournal) Load(fundingJournal) (fundingJournal, bool, error) {
	if !store.has {
		return fundingJournal{}, false, nil
	}
	return cloneFundingJournal(store.current), true, nil
}

func (store *memoryJournal) Create(journal fundingJournal) error {
	if store.has {
		return fmt.Errorf("journal exists")
	}
	store.current = cloneFundingJournal(journal)
	store.has = true
	store.creates = append(store.creates, cloneFundingJournal(journal))
	if store.afterCreate != nil {
		store.afterCreate()
	}
	return nil
}

func (store *memoryJournal) Save(journal fundingJournal) error {
	store.current = cloneFundingJournal(journal)
	store.has = true
	store.saves = append(store.saves, cloneFundingJournal(journal))
	return nil
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time                       { return clock.now }
func (fixedClock) Sleep(context.Context, time.Duration) error { return nil }

type stoppingClock struct{ now time.Time }

func (clock stoppingClock) Now() time.Time                       { return clock.now }
func (stoppingClock) Sleep(context.Context, time.Duration) error { return fmt.Errorf("stop polling") }

type countingSigner struct {
	delegate polymarket.DigestSigner
	calls    int
	before   func(int) error
}

func (signer *countingSigner) Address() string { return signer.delegate.Address() }
func (signer *countingSigner) SignDigest(ctx context.Context, digest []byte) ([]byte, error) {
	signer.calls++
	if signer.before != nil {
		if err := signer.before(signer.calls); err != nil {
			return nil, err
		}
	}
	return signer.delegate.SignDigest(ctx, digest)
}

func testAccount(t *testing.T) (fundingAccount, *countingSigner) {
	t.Helper()
	delegate, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	signer := &countingSigner{delegate: delegate}
	return fundingAccount{executionAccountID: mainExecutionAccountID, address: signer.Address(), signer: signer}, signer
}

func testPlan(now time.Time, source string, recipients ...string) fundingJournal {
	entries := make([]journalEntry, len(recipients))
	for index, recipient := range recipients {
		entries[index] = journalEntry{
			ExecutionAccountID: fmt.Sprintf("test-wallet-%d", index+1),
			Source:             source, Recipient: recipient, State: journalStatePending,
		}
	}
	return fundingJournal{
		SchemaVersion: fundingJournalSchema, PlanID: "test-fixed-native-funding-plan",
		ChainID: polygonChainID, FundingSource: source, AmountWei: fundingAmountWei.String(),
		RequiredConfirmations: requiredConfirmations, CreatedAt: now, UpdatedAt: now, Entries: entries,
	}
}

type fundingRPC struct {
	store                *memoryJournal
	source               string
	baseNonce            uint64
	sourceBalance        *big.Int
	recipientBalances    map[string]*big.Int
	sent                 map[string]bool
	sendErr              error
	sendCalls            int
	receiptUnavailable   bool
	transactionHidden    bool
	receiptCalls         int
	transactionCalls     int
	hideReceiptAfter     int
	hideTransactionAfter int
	latestBlock          uint64
	blockHash            string
	receiptBlockHash     string
	receiptMutator       func(*rpcReceipt)
	transactionMutator   func(*rpcTransaction)
}

func newFundingRPC(store *memoryJournal, source string) *fundingRPC {
	return &fundingRPC{
		store: store, source: source, baseNonce: 7,
		sourceBalance: mustDecimalBig("10000000000000000000"),
		recipientBalances: map[string]*big.Int{
			wallet6ExpectedAddress: mustDecimalBig("15000000000000000"),
			wallet7ExpectedAddress: mustDecimalBig("16000000000000000"),
		},
		sent: make(map[string]bool), latestBlock: 163,
		blockHash: "0x" + strings.Repeat("55", 32), receiptBlockHash: "0x" + strings.Repeat("55", 32),
	}
}

func approvedPrestate(rpc *fundingRPC) *fundingPrestate {
	targetBalances := make(map[string]*big.Int, len(rpc.recipientBalances))
	for address, balance := range rpc.recipientBalances {
		targetBalances[address] = new(big.Int).Set(balance)
	}
	return &fundingPrestate{
		startingNonce: rpc.baseNonce, sourceBalance: new(big.Int).Set(rpc.sourceBalance), targetBalances: targetBalances,
	}
}

func mustAddressTopic(address string) string {
	topic, err := addressTopic(address)
	if err != nil {
		panic(err)
	}
	return topic
}

func encodedLogWords(values ...*big.Int) string {
	var builder strings.Builder
	builder.WriteString("0x")
	for _, value := range values {
		word := value.Text(16)
		builder.WriteString(strings.Repeat("0", 64-len(word)))
		builder.WriteString(word)
	}
	return builder.String()
}

func polygonFundingLogs(entry journalEntry, blockHash, blockNumber string) []rpcLog {
	removed := false
	valueInput1 := mustDecimalBig("1000000000000000000")
	valueInput2 := mustDecimalBig("2000000000000000000")
	valueOutput1 := new(big.Int).Sub(new(big.Int).Set(valueInput1), fundingAmountWei)
	valueOutput2 := new(big.Int).Add(new(big.Int).Set(valueInput2), fundingAmountWei)
	priorityFee := mustDecimalBig(entry.MaxPriorityFeePerGas)
	feeAmount := new(big.Int).Mul(big.NewInt(21_000), priorityFee)
	return []rpcLog{
		{
			Address:   polygonNativeTokenAddress,
			Topics:    []string{polygonLogTransferTopic, mustAddressTopic(polygonNativeTokenAddress), mustAddressTopic(entry.Source), mustAddressTopic(entry.Recipient)},
			Data:      encodedLogWords(fundingAmountWei, valueInput1, valueInput2, valueOutput1, valueOutput2),
			BlockHash: blockHash, BlockNumber: blockNumber, TransactionHash: entry.TransactionHash,
			LogIndex: "0x1", Removed: &removed,
		},
		{
			Address:   polygonNativeTokenAddress,
			Topics:    []string{polygonLogFeeTransferTopic, mustAddressTopic(polygonNativeTokenAddress), mustAddressTopic(entry.Source), mustAddressTopic("0x7ee41d8a25641000661b1ef5e6ae8a00400466b0")},
			Data:      encodedLogWords(feeAmount, big.NewInt(9), big.NewInt(11), big.NewInt(8), big.NewInt(12)),
			BlockHash: blockHash, BlockNumber: blockNumber, TransactionHash: entry.TransactionHash,
			LogIndex: "0x2", Removed: &removed,
		},
	}
}

func (rpc *fundingRPC) entryByHash(hash string) (journalEntry, bool) {
	if rpc.store == nil || !rpc.store.has {
		return journalEntry{}, false
	}
	for _, entry := range rpc.store.current.Entries {
		if entry.TransactionHash == hash {
			return entry, true
		}
	}
	return journalEntry{}, false
}

func (rpc *fundingRPC) sentTo(recipient string) int {
	count := 0
	if rpc.store != nil && rpc.store.has {
		for _, entry := range rpc.store.current.Entries {
			if entry.Recipient == recipient && rpc.sent[entry.TransactionHash] {
				count++
			}
		}
	}
	return count
}

func (rpc *fundingRPC) Call(_ context.Context, method string, params []any) ([]byte, error) {
	switch method {
	case "eth_chainId":
		return json.Marshal("0x89")
	case "eth_getCode":
		return json.Marshal("0x")
	case "eth_getTransactionCount":
		return json.Marshal(quantityHex(rpc.baseNonce + uint64(len(rpc.sent))))
	case "eth_call":
		return json.Marshal("0x")
	case "eth_estimateGas":
		return json.Marshal("0x5208")
	case "eth_maxPriorityFeePerGas":
		return json.Marshal("0x3b9aca00")
	case "eth_getBalance":
		address := params[0].(string)
		if address == rpc.source {
			balance := new(big.Int).Set(rpc.sourceBalance)
			for range rpc.sent {
				balance.Sub(balance, fundingAmountWei)
				balance.Sub(balance, mustDecimalBig("21000000000000"))
			}
			return json.Marshal(quantityHexBig(balance))
		}
		base := new(big.Int)
		if value := rpc.recipientBalances[address]; value != nil {
			base.Set(value)
		}
		if count := rpc.sentTo(address); count != 0 {
			base.Add(base, new(big.Int).Mul(fundingAmountWei, big.NewInt(int64(count))))
		}
		return json.Marshal(quantityHexBig(base))
	case "eth_getBlockByNumber":
		if params[0] == "pending" {
			return []byte(`{"baseFeePerGas":"0x3b9aca00"}`), nil
		}
		return json.Marshal(map[string]string{"number": params[0].(string), "hash": rpc.blockHash})
	case "eth_sendRawTransaction":
		rpc.sendCalls++
		raw := params[0].(string)
		var entry *journalEntry
		if rpc.store != nil && rpc.store.has {
			for index := range rpc.store.current.Entries {
				candidate := &rpc.store.current.Entries[index]
				if candidate.RawTransaction == raw {
					entry = candidate
					break
				}
			}
		}
		if entry == nil || (entry.State != journalStateSigned && entry.State != journalStateBroadcast) {
			return nil, fmt.Errorf("raw transaction was not durably journaled before send")
		}
		if rpc.sendErr != nil {
			return nil, rpc.sendErr
		}
		rawBytes, _ := hex.DecodeString(strings.TrimPrefix(raw, "0x"))
		hash := "0x" + hex.EncodeToString(keccak256(rawBytes))
		if hash != entry.TransactionHash {
			return nil, fmt.Errorf("raw transaction hash mismatch")
		}
		rpc.sent[hash] = true
		return json.Marshal(hash)
	case "eth_getTransactionByHash":
		rpc.transactionCalls++
		hash := params[0].(string)
		entry, ok := rpc.entryByHash(hash)
		if !ok || !rpc.sent[hash] || rpc.transactionHidden || (rpc.hideTransactionAfter > 0 && rpc.transactionCalls > rpc.hideTransactionAfter) {
			return []byte("null"), nil
		}
		transaction := rpcTransaction{
			Hash: hash, ChainID: "0x89", Type: "0x2", From: entry.Source, To: entry.Recipient,
			Nonce: quantityHex(entry.Nonce), Gas: quantityHex(entry.GasLimit),
			MaxPriorityFeePerGas: quantityHexBig(mustDecimalBig(entry.MaxPriorityFeePerGas)),
			MaxFeePerGas:         quantityHexBig(mustDecimalBig(entry.MaxFeePerGas)),
			Value:                quantityHexBig(fundingAmountWei), Input: "0x",
			BlockHash: rpc.receiptBlockHash, BlockNumber: "0x64",
		}
		if rpc.transactionMutator != nil {
			rpc.transactionMutator(&transaction)
		}
		return json.Marshal(transaction)
	case "eth_getTransactionReceipt":
		rpc.receiptCalls++
		hash := params[0].(string)
		entry, ok := rpc.entryByHash(hash)
		if !ok || !rpc.sent[hash] || rpc.receiptUnavailable || (rpc.hideReceiptAfter > 0 && rpc.receiptCalls > rpc.hideReceiptAfter) {
			return []byte("null"), nil
		}
		receipt := rpcReceipt{
			TransactionHash: hash, BlockHash: rpc.receiptBlockHash, BlockNumber: "0x64",
			From: entry.Source, To: entry.Recipient, Type: "0x2", Status: "0x1",
			GasUsed: "0x5208", EffectiveGasPrice: "0x3b9aca00",
		}
		receipt.Logs = polygonFundingLogs(entry, receipt.BlockHash, receipt.BlockNumber)
		if rpc.receiptMutator != nil {
			rpc.receiptMutator(&receipt)
		}
		return json.Marshal(receipt)
	case "eth_blockNumber":
		return json.Marshal(quantityHex(rpc.latestBlock))
	default:
		return nil, fmt.Errorf("unexpected RPC method %s", method)
	}
}

func TestParseOptionsDefaultsDryRunAndRequiresExactToken(t *testing.T) {
	t.Setenv("POLYMARKET_ACCOUNTS_FILE", "/private/accounts.json")
	t.Setenv("POLYGON_RPC_URL", "https://polygon.example.invalid")
	t.Setenv("WALLET67_FUND_JOURNAL_FILE", "")
	options, err := parseOptions(nil)
	if err != nil || options.execute() {
		t.Fatalf("dry-run options/error = %#v/%v", options, err)
	}
	if _, err := parseOptions([]string{"--execute-token", "yes"}); err == nil || !strings.Contains(err.Error(), "exactly match") {
		t.Fatalf("wrong execute token error = %v", err)
	}
	if _, err := parseOptions([]string{"--execute-token", exactExecuteToken}); err == nil || !strings.Contains(err.Error(), "journal") {
		t.Fatalf("missing journal error = %v", err)
	}
	options, err = parseOptions([]string{
		"--execute-token", exactExecuteToken, "--journal-file", "/secure/fund.json",
		"--expected-starting-nonce", "63", "--expected-main-balance-wei", "1000000000000000000",
		"--expected-wallet6-balance-wei", "0", "--expected-wallet7-balance-wei", "0",
	})
	if err != nil || !options.execute() {
		t.Fatalf("execute options/error = %#v/%v", options, err)
	}
}

type addressOnlySigner string

func (signer addressOnlySigner) Address() string                             { return string(signer) }
func (addressOnlySigner) SignDigest(context.Context, []byte) ([]byte, error) { return nil, nil }

func TestSelectFundingAccountRequiresFixedMainType0SignerFunder(t *testing.T) {
	valid := polymarket.TradingAccount{
		ExecutionAccountID: mainExecutionAccountID, FunderAddress: mainExpectedAddress,
		SignatureType: polymarket.SignatureTypeEOA, Signer: addressOnlySigner(mainExpectedAddress),
	}
	selected, err := selectFundingAccount([]polymarket.TradingAccount{valid})
	if err != nil || selected.address != mainExpectedAddress {
		t.Fatalf("selected/error = %#v/%v", selected, err)
	}
	wrongAddress := valid
	wrongAddress.FunderAddress = wallet6ExpectedAddress
	wrongAddress.Signer = addressOnlySigner(wallet6ExpectedAddress)
	if _, err := selectFundingAccount([]polymarket.TradingAccount{wrongAddress}); err == nil || !strings.Contains(err.Error(), "fixed main") {
		t.Fatalf("wrong address error = %v", err)
	}
	wrongSigner := valid
	wrongSigner.Signer = addressOnlySigner(wallet6ExpectedAddress)
	if _, err := selectFundingAccount([]polymarket.TradingAccount{wrongSigner}); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Fatalf("signer/funder error = %v", err)
	}
	proxy := valid
	proxy.SignatureType = polymarket.SignatureTypePolyProxy
	if _, err := selectFundingAccount([]polymarket.TradingAccount{proxy}); err == nil || !strings.Contains(err.Error(), "type 0") {
		t.Fatalf("signature type error = %v", err)
	}
}

func TestNativeType2SigningIsDeterministicAndFixed(t *testing.T) {
	account, _ := testAccount(t)
	transaction, err := newFundingTransaction(7, 25_200, big.NewInt(1_000_000_000), big.NewInt(3_000_000_000), wallet6ExpectedAddress)
	if err != nil {
		t.Fatal(err)
	}
	first, err := signType2Funding(context.Background(), transaction, account)
	if err != nil {
		t.Fatal(err)
	}
	second, err := signType2Funding(context.Background(), transaction, account)
	if err != nil || first.raw != second.raw || first.hash != second.hash || first.digest != second.digest {
		t.Fatalf("non-deterministic signing: %#v/%#v/%v", first, second, err)
	}
	if !strings.HasPrefix(first.raw, "0x02") || first.yParity > 1 || first.s.Cmp(secp256k1HalfOrder) > 0 {
		t.Fatalf("invalid type2 signature = %#v", first)
	}
	const wantDigest = "0x2bdfffa03c02bab36fa3012c114bbbd5a066288ced56962c5f9e0f4dd11b0749"
	const wantRaw = "0x02f872818907843b9aca0084b2d05e00826270940aefd80df02cc35e81aede40b34e2e961bb4593f87d529ae9e86000080c001a006f3d20e08684fcab1e57331e8b5898b168ce198fb02b1afbd89341e6b63cbd9a02a9e8cbb7bb5efba9852643052d2cf4d4e955d15c6282b8692796f7200342098"
	const wantHash = "0x8ed39d0371892f46ac0cf4f57c57609b8c33ecfb568d99ccfa5be03eb9a0460b"
	if first.digest != wantDigest || first.raw != wantRaw || first.hash != wantHash {
		t.Fatalf("frozen native type2 vector = %s/%s/%s", first.digest, first.raw, first.hash)
	}
	if transaction.value.Cmp(fundingAmountWei) != 0 || transaction.to != wallet6ExpectedAddress || len(transaction.data) != 0 {
		t.Fatalf("transaction is not fixed native funding: %#v", transaction)
	}
	if _, err := newFundingTransaction(7, 25_200, big.NewInt(1), big.NewInt(2), "0x1111111111111111111111111111111111111111"); err == nil || !strings.Contains(err.Error(), "wallet-6 or wallet-7") {
		t.Fatalf("unexpected recipient accepted: %v", err)
	}
	if _, err := newFundingTransaction(7, maximumFundingGasLimit+1, big.NewInt(1), big.NewInt(2), wallet6ExpectedAddress); err == nil || !strings.Contains(err.Error(), "gas limit") {
		t.Fatalf("excessive gas accepted: %v", err)
	}
	if _, err := newFundingTransaction(7, 25_200, new(big.Int).Add(new(big.Int).SetUint64(maximumPriorityFeeWei), big.NewInt(1)), new(big.Int).SetUint64(maximumMaxFeePerGasWei), wallet6ExpectedAddress); err == nil || !strings.Contains(err.Error(), "priority fee") {
		t.Fatalf("excessive priority fee accepted: %v", err)
	}
	wrongIdentity := account
	wrongIdentity.address = wallet7ExpectedAddress
	if _, err := signType2Funding(context.Background(), transaction, wrongIdentity); err == nil || !strings.Contains(err.Error(), "recovered") {
		t.Fatalf("wrong signer identity accepted: %v", err)
	}
}

func TestDryRunNeverSignsOrPersists(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, signer := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	signer.before = func(call int) error {
		if call != 1 {
			return nil
		}
		if len(store.creates) != 1 || len(store.creates[0].Entries) != 2 {
			return fmt.Errorf("complete plan was not created before first signature")
		}
		for _, entry := range store.creates[0].Entries {
			if entry.State != journalStatePending || entry.PlannedNonce == "" || entry.RecipientBalanceBefore == "" || entry.RawTransaction != "" {
				return fmt.Errorf("created plan is not complete PENDING prestate")
			}
		}
		return nil
	}
	result, err := runFundingPlan(context.Background(), fundingRunParams{
		rpc: rpc, account: account, clock: fixedClock{now: now},
	}, testPlan(now, account.address, wallet6ExpectedAddress, wallet7ExpectedAddress))
	if err != nil {
		t.Fatal(err)
	}
	if !result.DryRun || len(result.Targets) != 2 || result.Targets[0].Nonce != 7 || result.Targets[1].Nonce != 8 {
		t.Fatalf("dry-run result = %#v", result)
	}
	if signer.calls != 0 || store.has || rpc.sendCalls != 0 {
		t.Fatalf("dry-run signer/journal/send = %d/%v/%d", signer.calls, store.has, rpc.sendCalls)
	}
}

func TestExecutePreflightRequiresCumulativeValueAndGasBeforeSigning(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, signer := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	// Enough for one 0.06 POL transfer but not the fixed two-transfer plan.
	rpc.sourceBalance = mustDecimalBig("70000000000000000")
	_, err := runFundingPlan(context.Background(), fundingRunParams{
		rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approvedPrestate(rpc),
	}, testPlan(now, account.address, wallet6ExpectedAddress, wallet7ExpectedAddress))
	if err == nil || !strings.Contains(err.Error(), "fixed value plus maximum gas budget") {
		t.Fatalf("cumulative balance error = %v", err)
	}
	if signer.calls != 0 || len(store.saves) != 0 || rpc.sendCalls != 0 {
		t.Fatalf("preflight signed/saved/sent = %d/%d/%d", signer.calls, len(store.saves), rpc.sendCalls)
	}
}

func TestApprovedPrestateDriftNeverCreatesJournalOrSigns(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, signer := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	approved := approvedPrestate(rpc)
	approved.targetBalances[wallet6ExpectedAddress].Add(approved.targetBalances[wallet6ExpectedAddress], big.NewInt(1))
	_, err := runFundingPlan(context.Background(), fundingRunParams{
		rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approved,
	}, testPlan(now, account.address, wallet6ExpectedAddress, wallet7ExpectedAddress))
	if err == nil || !strings.Contains(err.Error(), "differs from approved prestate") {
		t.Fatalf("prestate drift error = %v", err)
	}
	if store.has || signer.calls != 0 || rpc.sendCalls != 0 {
		t.Fatalf("prestate drift journal/sign/send = %v/%d/%d", store.has, signer.calls, rpc.sendCalls)
	}
}

func TestSourceBalanceDriftAfterJournalCreationStopsBeforeSigning(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, signer := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	store.afterCreate = func() { rpc.sourceBalance.Add(rpc.sourceBalance, big.NewInt(1)) }
	_, err := runFundingPlan(context.Background(), fundingRunParams{
		rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approvedPrestate(rpc),
	}, testPlan(now, account.address, wallet6ExpectedAddress, wallet7ExpectedAddress))
	if err == nil || !strings.Contains(err.Error(), "exact journal stage balance") {
		t.Fatalf("post-journal source drift error = %v", err)
	}
	if !store.has || signer.calls != 0 || rpc.sendCalls != 0 || store.current.Entries[0].State != journalStatePending {
		t.Fatalf("post-journal drift state/sign/send = %v/%d/%d/%s", store.has, signer.calls, rpc.sendCalls, store.current.Entries[0].State)
	}
}

func TestExecuteSerializesJournalsBeforeSendAndReplaysConfirmed(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, signer := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	params := fundingRunParams{rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approvedPrestate(rpc)}
	plan := testPlan(now, account.address, wallet6ExpectedAddress, wallet7ExpectedAddress)
	result, err := runFundingPlan(context.Background(), params, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Targets) != 2 || result.Targets[0].State != journalStateConfirmed || result.Targets[1].State != journalStateConfirmed ||
		result.Targets[0].Nonce != 7 || result.Targets[1].Nonce != 8 || rpc.sendCalls != 2 || signer.calls < 2 {
		t.Fatalf("execution result/sign/send = %#v/%d/%d", result, signer.calls, rpc.sendCalls)
	}
	if store.creates[0].StartingNonce != "7" || store.creates[0].SourceBalanceBefore != "10000000000000000000" ||
		store.creates[0].Entries[0].PlannedNonce != "7" || store.creates[0].Entries[1].PlannedNonce != "8" {
		t.Fatalf("initial journal prestate = %#v", store.creates[0])
	}
	for _, entry := range store.current.Entries {
		if entry.RawTransaction == "" || entry.Input != "0x" || entry.Confirmations != 64 || entry.RecipientBalanceAfter == "" {
			t.Fatalf("confirmed journal entry = %#v", entry)
		}
	}
	beforeSendCalls := rpc.sendCalls
	if _, err := runFundingPlan(context.Background(), params, plan); err != nil {
		t.Fatal(err)
	}
	if rpc.sendCalls != beforeSendCalls {
		t.Fatalf("confirmed replay rebroadcast: %d -> %d", beforeSendCalls, rpc.sendCalls)
	}
}

func TestBroadcastRecoveryWithVisibleReceiptNeverCallsSigner(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, signer := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	params := fundingRunParams{rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approvedPrestate(rpc)}
	plan := testPlan(now, account.address, wallet6ExpectedAddress)
	if _, err := runFundingPlan(context.Background(), params, plan); err != nil {
		t.Fatal(err)
	}
	entry := &store.current.Entries[0]
	entry.State = journalStateBroadcast
	entry.ReceiptBlockNumber = 0
	entry.ReceiptBlockHash = ""
	entry.Confirmations = 0
	entry.GasUsed = 0
	entry.EffectiveGasPrice = ""
	entry.RecipientBalanceAfter = ""
	entry.ConfirmedAt = time.Time{}
	signer.calls = 0
	signer.before = func(int) error { return fmt.Errorf("signer must not be called during visible broadcast recovery") }
	beforeSendCalls := rpc.sendCalls
	result, err := runFundingPlan(context.Background(), params, plan)
	if err != nil {
		t.Fatal(err)
	}
	if signer.calls != 0 || rpc.sendCalls != beforeSendCalls || result.Targets[0].State != journalStateConfirmed {
		t.Fatalf("visible broadcast recovery signer/send/state = %d/%d/%s", signer.calls, rpc.sendCalls-beforeSendCalls, result.Targets[0].State)
	}
}

func TestBroadcastRecoveryVisibilityRaceValidatesRawBeforeAnyRebroadcast(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, signer := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	params := fundingRunParams{rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approvedPrestate(rpc)}
	plan := testPlan(now, account.address, wallet6ExpectedAddress)
	if _, err := runFundingPlan(context.Background(), params, plan); err != nil {
		t.Fatal(err)
	}
	entry := &store.current.Entries[0]
	entry.State = journalStateBroadcast
	entry.ReceiptBlockNumber = 0
	entry.ReceiptBlockHash = ""
	entry.Confirmations = 0
	entry.GasUsed = 0
	entry.EffectiveGasPrice = ""
	entry.RecipientBalanceAfter = ""
	entry.ConfirmedAt = time.Time{}
	rpc.receiptCalls = 0
	rpc.transactionCalls = 0
	rpc.hideReceiptAfter = 1
	rpc.hideTransactionAfter = 1
	signer.calls = 0
	signer.before = func(int) error { return fmt.Errorf("refuse local raw verification in visibility-race test") }
	beforeSendCalls := rpc.sendCalls
	_, err := runFundingPlan(context.Background(), params, plan)
	if err == nil || !strings.Contains(err.Error(), "visibility-race test") {
		t.Fatalf("visibility-race recovery error = %v", err)
	}
	if signer.calls != 1 || rpc.sendCalls != beforeSendCalls || store.current.Entries[0].State != journalStateBroadcast {
		t.Fatalf("visibility-race signer/send/state = %d/%d/%s", signer.calls, rpc.sendCalls-beforeSendCalls, store.current.Entries[0].State)
	}
}

func TestAmbiguousSendLeavesExactRawSignedAndResumesSameTransaction(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, _ := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	rpc.sendErr = &rpcCallError{method: "eth_sendRawTransaction", ambiguous: true, cause: fmt.Errorf("connection reset")}
	params := fundingRunParams{rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approvedPrestate(rpc)}
	plan := testPlan(now, account.address, wallet6ExpectedAddress)
	_, err := runFundingPlan(context.Background(), params, plan)
	if err == nil || !strings.Contains(err.Error(), "ambiguous send outcome") {
		t.Fatalf("ambiguous send error = %v", err)
	}
	entry := store.current.Entries[0]
	if entry.State != journalStateSigned || entry.RawTransaction == "" || entry.TransactionHash == "" {
		t.Fatalf("ambiguous journal = %#v", entry)
	}
	raw, hash := entry.RawTransaction, entry.TransactionHash
	wrongApproved := *params.prestate
	wrongApproved.sourceBalance = new(big.Int).Add(wrongApproved.sourceBalance, big.NewInt(1))
	wrongParams := params
	wrongParams.prestate = &wrongApproved
	beforeWrongResume := rpc.sendCalls
	if _, err := runFundingPlan(context.Background(), wrongParams, plan); err == nil || !strings.Contains(err.Error(), "does not match approved main prestate") || rpc.sendCalls != beforeWrongResume {
		t.Fatalf("wrong resume prestate error/send = %v/%d", err, rpc.sendCalls)
	}
	rpc.sendErr = nil
	result, err := runFundingPlan(context.Background(), params, plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Targets[0].State != journalStateConfirmed || store.current.Entries[0].RawTransaction != raw || store.current.Entries[0].TransactionHash != hash || rpc.sendCalls != 2 {
		t.Fatalf("resume changed exact transaction = %#v/%#v", result, store.current.Entries[0])
	}
}

func TestSignedRecoveryRejectsChangedNonceWithoutBroadcast(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, _ := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	rpc.sendErr = &rpcCallError{method: "eth_sendRawTransaction", ambiguous: true, cause: fmt.Errorf("timeout")}
	params := fundingRunParams{rpc: rpc, account: account, journal: store, clock: fixedClock{now: now}, execute: true, prestate: approvedPrestate(rpc)}
	plan := testPlan(now, account.address, wallet6ExpectedAddress)
	_, _ = runFundingPlan(context.Background(), params, plan)
	rpc.sendErr = nil
	rpc.baseNonce = 8
	before := rpc.sendCalls
	_, err := runFundingPlan(context.Background(), params, plan)
	if err == nil || !strings.Contains(err.Error(), "signed nonce 7 is no longer") || rpc.sendCalls != before {
		t.Fatalf("changed nonce error/send = %v/%d", err, rpc.sendCalls)
	}
}

func TestTransactionAndReceiptValidationFailClosed(t *testing.T) {
	entry := journalEntry{
		Source: wallet7ExpectedAddress, Recipient: wallet6ExpectedAddress, Nonce: 7, GasLimit: 25_200,
		MaxPriorityFeePerGas: "1000000000", MaxFeePerGas: "3000000000", Input: "0x",
		TransactionHash: "0x" + strings.Repeat("11", 32),
	}
	transaction := rpcTransaction{
		Hash: entry.TransactionHash, ChainID: "0x89", Type: "0x2", From: entry.Source, To: entry.Recipient,
		Nonce: "0x7", Gas: quantityHex(entry.GasLimit), MaxPriorityFeePerGas: "0x3b9aca00",
		MaxFeePerGas: "0xb2d05e00", Value: quantityHexBig(fundingAmountWei), Input: "0x",
	}
	if err := validateRPCTransaction(transaction, entry); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*rpcTransaction)
		want   string
	}{
		{"from", func(tx *rpcTransaction) { tx.From = wallet6ExpectedAddress }, "sender"},
		{"to", func(tx *rpcTransaction) { tx.To = wallet7ExpectedAddress }, "destination"},
		{"value", func(tx *rpcTransaction) { tx.Value = "0x1" }, "value"},
		{"input", func(tx *rpcTransaction) { tx.Input = "0x00" }, "input"},
		{"nonce", func(tx *rpcTransaction) { tx.Nonce = "0x8" }, "nonce"},
		{"chain", func(tx *rpcTransaction) { tx.ChainID = "0x1" }, "chain ID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := transaction
			test.mutate(&candidate)
			if err := validateRPCTransaction(candidate, entry); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
	receipt := rpcReceipt{
		TransactionHash: entry.TransactionHash, BlockHash: "0x" + strings.Repeat("55", 32), BlockNumber: "0x64",
		From: entry.Source, To: entry.Recipient, Type: "0x2", Status: "0x1", GasUsed: "0x5208",
		EffectiveGasPrice: "0x3b9aca00",
	}
	receipt.Logs = polygonFundingLogs(entry, receipt.BlockHash, receipt.BlockNumber)
	if _, err := validateFundingReceipt(receipt, entry); err != nil {
		t.Fatal(err)
	}
	reverted := receipt
	reverted.Status = "0x0"
	if _, err := validateFundingReceipt(reverted, entry); err == nil || !strings.Contains(err.Error(), "reverted") {
		t.Fatalf("reverted receipt error = %v", err)
	}
	wrongSignature := receipt
	wrongSignature.Logs = append([]rpcLog(nil), receipt.Logs...)
	wrongSignature.Logs[0].Topics = append([]string(nil), receipt.Logs[0].Topics...)
	wrongSignature.Logs[0].Topics[0] = polygonLogFeeTransferTopic
	if _, err := validateFundingReceipt(wrongSignature, entry); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("receipt log signature error = %v", err)
	}
	missingLogs := receipt
	missingLogs.Logs = nil
	if _, err := validateFundingReceipt(missingLogs, entry); err == nil || !strings.Contains(err.Error(), "exactly two") {
		t.Fatalf("missing receipt logs error = %v", err)
	}
}

func TestPolygonNativeFundingLogsRejectUnknownOrInconsistentShape(t *testing.T) {
	entry := journalEntry{
		Source: mainExpectedAddress, Recipient: wallet6ExpectedAddress, Nonce: 63, GasLimit: 25_200,
		MaxPriorityFeePerGas: "1000000000", MaxFeePerGas: "3000000000", Input: "0x",
		TransactionHash: "0x" + strings.Repeat("11", 32),
	}
	receipt := rpcReceipt{
		TransactionHash: entry.TransactionHash, BlockHash: "0x" + strings.Repeat("55", 32), BlockNumber: "0x64",
		From: entry.Source, To: entry.Recipient, Type: "0x2", Status: "0x1", GasUsed: "0x5208",
		EffectiveGasPrice: "0x3b9aca00",
	}
	receipt.Logs = polygonFundingLogs(entry, receipt.BlockHash, receipt.BlockNumber)
	clone := func() rpcReceipt {
		payload, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		var result rpcReceipt
		if err := json.Unmarshal(payload, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	tests := []struct {
		name   string
		mutate func(*rpcReceipt)
		want   string
	}{
		{"unknown address", func(value *rpcReceipt) { value.Logs[0].Address = wallet7ExpectedAddress }, "address"},
		{"wrong topic count", func(value *rpcReceipt) { value.Logs[0].Topics = value.Logs[0].Topics[:3] }, "four topics"},
		{"wrong data size", func(value *rpcReceipt) { value.Logs[0].Data = "0x00" }, "five uint256"},
		{"removed", func(value *rpcReceipt) { removed := true; value.Logs[0].Removed = &removed }, "removed"},
		{"missing removed", func(value *rpcReceipt) { value.Logs[0].Removed = nil }, "removed"},
		{"wrong block", func(value *rpcReceipt) { value.Logs[0].BlockHash = "0x" + strings.Repeat("66", 32) }, "block hash"},
		{"wrong block number", func(value *rpcReceipt) { value.Logs[0].BlockNumber = "0x65" }, "block number"},
		{"wrong second block", func(value *rpcReceipt) { value.Logs[1].BlockHash = "0x" + strings.Repeat("66", 32) }, "block hash"},
		{"wrong transaction", func(value *rpcReceipt) { value.Logs[0].TransactionHash = "0x" + strings.Repeat("66", 32) }, "transaction hash"},
		{"wrong second transaction", func(value *rpcReceipt) { value.Logs[1].TransactionHash = "0x" + strings.Repeat("66", 32) }, "transaction hash"},
		{"nonconsecutive indexes", func(value *rpcReceipt) { value.Logs[1].LogIndex = "0x3" }, "not consecutive"},
		{"wrong token", func(value *rpcReceipt) { value.Logs[0].Topics[1] = mustAddressTopic(wallet7ExpectedAddress) }, "token"},
		{"wrong source", func(value *rpcReceipt) { value.Logs[0].Topics[2] = mustAddressTopic(wallet7ExpectedAddress) }, "source"},
		{"fee recipient is target", func(value *rpcReceipt) { value.Logs[1].Topics[3] = mustAddressTopic(entry.Recipient) }, "independent fee recipient"},
		{
			"wrong value amount",
			func(value *rpcReceipt) {
				words, _ := polygonLogWords(value.Logs[0].Data, "test value log")
				words[0] = big.NewInt(1)
				value.Logs[0].Data = encodedLogWords(words[:]...)
			},
			"fixed balance transfer",
		},
		{
			"wrong value source delta",
			func(value *rpcReceipt) {
				words, _ := polygonLogWords(value.Logs[0].Data, "test value log")
				words[3].Add(words[3], big.NewInt(1))
				value.Logs[0].Data = encodedLogWords(words[:]...)
			},
			"fixed balance transfer",
		},
		{
			"wrong value recipient delta",
			func(value *rpcReceipt) {
				words, _ := polygonLogWords(value.Logs[0].Data, "test value log")
				words[4].Sub(words[4], big.NewInt(1))
				value.Logs[0].Data = encodedLogWords(words[:]...)
			},
			"fixed balance transfer",
		},
		{
			"zero fee",
			func(value *rpcReceipt) {
				words, _ := polygonLogWords(value.Logs[1].Data, "test fee log")
				words[0] = new(big.Int)
				value.Logs[1].Data = encodedLogWords(words[:]...)
			},
			"not positive",
		},
		{
			"fee not divisible by gas used",
			func(value *rpcReceipt) {
				words, _ := polygonLogWords(value.Logs[1].Data, "test fee log")
				words[0].Add(words[0], big.NewInt(1))
				value.Logs[1].Data = encodedLogWords(words[:]...)
			},
			"signed EIP-1559 fee",
		},
		{
			"fee exceeds effective gas cost",
			func(value *rpcReceipt) {
				words, _ := polygonLogWords(value.Logs[1].Data, "test fee log")
				words[0] = new(big.Int).Mul(big.NewInt(21_000), big.NewInt(1_000_000_001))
				value.Logs[1].Data = encodedLogWords(words[:]...)
			},
			"signed EIP-1559 fee",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := clone()
			test.mutate(&candidate)
			if _, err := validateFundingReceipt(candidate, entry); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateFundingReceipt() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestPolygonNativeFundingLogsAcceptObservedFeeLogAboveSignedPriority(t *testing.T) {
	entry := journalEntry{
		Source: mainExpectedAddress, Recipient: wallet6ExpectedAddress, Nonce: 63, GasLimit: 25_200,
		MaxPriorityFeePerGas: "29711445052", MaxFeePerGas: "517461510098", Input: "0x",
		TransactionHash: "0x" + strings.Repeat("11", 32),
	}
	receipt := rpcReceipt{
		TransactionHash: entry.TransactionHash, BlockHash: "0x" + strings.Repeat("55", 32), BlockNumber: "0x5846f80",
		From: entry.Source, To: entry.Recipient, Type: "0x2", Status: "0x1", GasUsed: "0x5208",
		EffectiveGasPrice: "0x40b7453301",
	}
	receipt.Logs = polygonFundingLogs(entry, receipt.BlockHash, receipt.BlockNumber)
	feeWords, err := polygonLogWords(receipt.Logs[1].Data, "test fee log")
	if err != nil {
		t.Fatal(err)
	}
	feeWords[0] = new(big.Int).Mul(big.NewInt(21_000), big.NewInt(30_000_000_000))
	receipt.Logs[1].Data = encodedLogWords(feeWords[:]...)
	if _, err := validateFundingReceipt(receipt, entry); err != nil {
		t.Fatalf("validateFundingReceipt() rejected observed Polygon fee-log semantics: %v", err)
	}
}

func TestConfirmationGateRequires64CanonicalBlocks(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	account, _ := testAccount(t)
	store := &memoryJournal{}
	rpc := newFundingRPC(store, account.address)
	params := fundingRunParams{rpc: rpc, account: account, journal: store, clock: stoppingClock{now: now}, execute: true, prestate: approvedPrestate(rpc)}
	plan := testPlan(now, account.address, wallet6ExpectedAddress)
	// First stop immediately after the transaction is broadcast but before 64 confirmations.
	rpc.latestBlock = 162
	_, err := runFundingPlan(context.Background(), params, plan)
	if err == nil || !strings.Contains(err.Error(), "stop polling") || store.current.Entries[0].State != journalStateBroadcast {
		t.Fatalf("63-confirmation error/state = %v/%s", err, store.current.Entries[0].State)
	}
	// A changed canonical block hash is rejected before confirmation.
	rpc.latestBlock = 163
	rpc.blockHash = "0x" + strings.Repeat("66", 32)
	_, err = runFundingPlan(context.Background(), params, plan)
	if err == nil || !strings.Contains(err.Error(), "block") {
		t.Fatalf("changed canonical block error = %v", err)
	}
}

func TestFileJournalCreatesExclusivelyMode0600AndNeverBlindlyRecreates(t *testing.T) {
	now := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	tempBase := os.TempDir()
	if _, err := os.Stat(tempBase); err != nil {
		tempBase = "/tmp"
	}
	resolvedTemp, err := filepath.EvalSymlinks(tempBase)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(resolvedTemp, "wallet67fund-journal-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "wallet67fund.json")
	store, err := newFileJournalStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	static := testPlan(now, wallet7ExpectedAddress, wallet6ExpectedAddress, wallet7ExpectedAddress)
	prepared := cloneFundingJournal(static)
	prepared.StartingNonce = "7"
	prepared.SourceBalanceBefore = "1000000000000000000"
	prepared.Entries[0].RecipientBalanceBefore = "1"
	prepared.Entries[0].PlannedNonce = "7"
	prepared.Entries[1].RecipientBalanceBefore = "2"
	prepared.Entries[1].PlannedNonce = "8"
	if err := validateFundingJournal(prepared, static); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(prepared); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %v", info.Mode())
	}
	if err := store.Create(prepared); err == nil || !strings.Contains(err.Error(), "exclusively") {
		t.Fatalf("second exclusive create error = %v", err)
	}
	loaded, exists, err := store.Load(static)
	if err != nil || !exists || loaded.StartingNonce != "7" || len(loaded.Entries) != 2 {
		t.Fatalf("loaded journal/exists/error = %#v/%v/%v", loaded, exists, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(prepared); err == nil || !strings.Contains(err.Error(), "refusing non-exclusive recreation") {
		t.Fatalf("missing journal update error = %v", err)
	}
}

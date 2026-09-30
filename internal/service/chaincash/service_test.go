package chaincash

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	testWallet6      = "0x6666666666666666666666666666666666666666"
	testWallet7      = "0x7777777777777777777777777777777777777777"
	testExchange     = "0xe111180000d2663c0091e4f400237545b87b996b"
	testNegRisk      = "0xe2222d279d744050d28e00520010520000310f59"
	testRewardSender = "0x607c8c9866ef3b4665c5a384188706be738d8bf8"
	testEOA6         = "0x0aefd80d000000000000000000000000000593f0"
	testEOA7         = "0xc9ba353700000000000000000000000000e6d900"
	testStranger     = "0x9999999999999999999999999999999999999999"
)

var testNow = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func hash(seed int) string { return fmt.Sprintf("0x%064x", seed) }

func transfer(block uint64, logIndex uint64, direction domain.ChainCashDirection, counterparty string, amount domain.Decimal) domain.ChainCashTransfer {
	return domain.ChainCashTransfer{
		TransactionHash: hash(int(block*100 + logIndex)), LogIndex: logIndex, BlockNumber: block,
		BlockHash: hash(int(block)), Direction: direction, Counterparty: counterparty, Amount: amount, ObservedAt: testNow,
	}
}

type listCall struct {
	wallet   string
	from, to uint64
}

type fakeLogs struct {
	byWallet   map[string][]domain.ChainCashTransfer
	fills      map[string][]domain.ChainCashFill
	calls      []listCall
	failCall   int
	requireErr error
	required   []uint64
}

func (logs *fakeLogs) ReadWalletLogs(_ context.Context, wallet string, from, to uint64) (domain.ChainCashLogs, error) {
	logs.calls = append(logs.calls, listCall{wallet: wallet, from: from, to: to})
	if logs.failCall == len(logs.calls) {
		return domain.ChainCashLogs{}, errors.New("eth_getLogs chunk failed")
	}
	result := domain.ChainCashLogs{Transfers: make([]domain.ChainCashTransfer, 0)}
	for _, value := range logs.byWallet[wallet] {
		if value.BlockNumber >= from && value.BlockNumber <= to {
			result.Transfers = append(result.Transfers, value)
		}
	}
	for _, value := range logs.fills[wallet] {
		if value.BlockNumber >= from && value.BlockNumber <= to {
			result.Fills = append(result.Fills, value)
		}
	}
	return result, nil
}

// fillFor returns an OrderFilled emitted by exchange in the transaction of
// transfer, naming wallet as maker (taker is a stranger).
func fillFor(transfer domain.ChainCashTransfer, exchange, wallet string, logIndex uint64) domain.ChainCashFill {
	return domain.ChainCashFill{
		TransactionHash: transfer.TransactionHash, LogIndex: logIndex, BlockNumber: transfer.BlockNumber,
		BlockHash: transfer.BlockHash, Exchange: exchange, Maker: wallet, Taker: testStranger,
	}
}

func (logs *fakeLogs) RequireBlock(_ context.Context, block uint64) error {
	logs.required = append(logs.required, block)
	return logs.requireErr
}

type fakeStore struct {
	cursors     map[string]domain.ChainCashCursor
	redemptions map[string]map[string]struct{}
	applied     []domain.ChainCashRange
	applyErr    error
	ledger      domain.Decimal
	offset      domain.Decimal
}

func (store *fakeStore) GetChainCashCursor(_ context.Context, accountID string) (domain.ChainCashCursor, bool, error) {
	cursor, found := store.cursors[accountID]
	return cursor, found, nil
}

func (store *fakeStore) ListRedemptionTransactionHashes(_ context.Context, accountID string, hashes []string) (map[string]struct{}, error) {
	result := map[string]struct{}{}
	for _, value := range hashes {
		if _, found := store.redemptions[accountID][value]; found {
			result[value] = struct{}{}
		}
	}
	return result, nil
}

func (store *fakeStore) ApplyChainCashRange(_ context.Context, value domain.ChainCashRange) (domain.ChainCashRangeResult, error) {
	if store.applyErr != nil {
		return domain.ChainCashRangeResult{}, store.applyErr
	}
	store.applied = append(store.applied, value)
	cursor := store.cursors[value.ExecutionAccountID]
	cursor.ProcessedBlock = value.ToBlock
	store.cursors[value.ExecutionAccountID] = cursor
	return domain.ChainCashRangeResult{ProcessedBlock: value.ToBlock, LedgerTotal: store.ledger, PersistedOffset: store.offset}, nil
}

func newTestService(t *testing.T, logs *fakeLogs, store *fakeStore, maxBlocks uint64) *Service {
	t.Helper()
	service, err := New(Params{
		Logs: logs, Store: store, ExchangeAddresses: []string{testExchange, strings.ToUpper(testNegRisk[:2]) + testNegRisk[2:]},
		RewardSenders:  []string{testRewardSender},
		DepositSources: map[string][]string{"wallet-6": {testEOA6}, "wallet-7": {testEOA7}},
		Confirmations:  64, MaxBlocksPerRun: maxBlocks, Now: func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestClassificationCoversEveryClass(t *testing.T) {
	redemptionMint := transfer(10, 2, domain.ChainCashIn, domain.ChainCashZeroAddress, "20")
	foreignMint := transfer(10, 3, domain.ChainCashIn, domain.ChainCashZeroAddress, "5")
	store := &fakeStore{redemptions: map[string]map[string]struct{}{"wallet-6": {redemptionMint.TransactionHash: {}}}}
	service := newTestService(t, &fakeLogs{}, store, 0)
	for _, test := range []struct {
		name     string
		account  string
		transfer domain.ChainCashTransfer
		want     domain.ChainCashClassification
	}{
		{"BUY settlement to the standard exchange", "wallet-6", transfer(10, 0, domain.ChainCashOut, testExchange, "8.8"), domain.ChainCashTrade},
		{"SELL proceeds from the neg-risk exchange", "wallet-7", transfer(10, 1, domain.ChainCashIn, testNegRisk, "17.946"), domain.ChainCashTrade},
		{"own redemption mint", "wallet-6", redemptionMint, domain.ChainCashRedemption},
		{"mint in a transaction that is not an own redemption", "wallet-6", foreignMint, domain.ChainCashUnattributedIn},
		{"own redemption hash from a non-mint sender", "wallet-6", func() domain.ChainCashTransfer {
			value := transfer(10, 4, domain.ChainCashIn, testStranger, "1")
			value.TransactionHash = redemptionMint.TransactionHash
			return value
		}(), domain.ChainCashUnattributedIn},
		{"holding reward", "wallet-6", transfer(94681378, 7, domain.ChainCashIn, testRewardSender, "0.0015"), domain.ChainCashReward},
		{"deposit from own EOA", "wallet-6", transfer(10, 5, domain.ChainCashIn, testEOA6, "50"), domain.ChainCashDeposit},
		{"deposit source of another account", "wallet-6", transfer(10, 6, domain.ChainCashIn, testEOA7, "50"), domain.ChainCashUnattributedIn},
		{"unknown sender", "wallet-6", transfer(10, 7, domain.ChainCashIn, testStranger, "3"), domain.ChainCashUnattributedIn},
		{"unknown recipient", "wallet-6", transfer(10, 8, domain.ChainCashOut, testStranger, "3"), domain.ChainCashUnattributedOut},
		{"outgoing to a reward sender is still unattributed", "wallet-6", transfer(10, 9, domain.ChainCashOut, testRewardSender, "1"), domain.ChainCashUnattributedOut},
		{"outgoing to own EOA is still unattributed", "wallet-6", transfer(10, 10, domain.ChainCashOut, testEOA6, "1"), domain.ChainCashUnattributedOut},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := []domain.ChainCashTransfer{test.transfer}
			if err := service.classify(context.Background(), test.account, values, nil); err != nil {
				t.Fatal(err)
			}
			if values[0].Classification != test.want || values[0].ExecutionAccountID != test.account {
				t.Fatalf("classification = %s (account %s), want %s", values[0].Classification, values[0].ExecutionAccountID, test.want)
			}
		})
	}
}

// Real V2 settlement shapes replayed from Polygon (wallets 6/7, blocks
// 94600000-94698250): many fills settle peer-to-peer, so the pUSD Transfer
// counterparty is the matched maker or the fee collector, not an exchange.
const (
	testFeeCollector = "0x115f48dc2a731aa16251c6d6e1befc42f92accc9"
	testMakerA       = "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	testMakerB       = "0xb1ca909e00000000000000000000000000000001"
	testFakeVenue    = "0xfa4efa4efa4efa4efa4efa4efa4efa4efa4efa4e"
)

func TestClassificationOfPeerToPeerSettlement(t *testing.T) {
	// Transaction 0xc939c707...: wallet-6 pays two makers 4.80764 + 4.33236
	// and a 0.02358 fee, all in one tx whose OrderFilled names wallet-6.
	buyTx := hash(0xc939c707)
	makerA := transfer(94650000, 11, domain.ChainCashOut, testMakerA, "4.80764")
	makerB := transfer(94650000, 13, domain.ChainCashOut, testMakerB, "4.33236")
	fee := transfer(94650000, 15, domain.ChainCashOut, testFeeCollector, "0.02358")
	for _, value := range []*domain.ChainCashTransfer{&makerA, &makerB, &fee} {
		value.TransactionHash = buyTx
	}
	// Incoming SELL proceeds paid directly by a counterparty; wallet-6 is the
	// taker of this OrderFilled on the neg-risk exchange.
	sell := transfer(94650100, 4, domain.ChainCashIn, testMakerB, "10.01")
	sellFill := fillFor(sell, testNegRisk, testStranger, 7)
	sellFill.Taker = testWallet6
	// A transfer to a stranger in a transaction without a wallet OrderFilled.
	stranger := transfer(94650200, 2, domain.ChainCashOut, testStranger, "1")
	// An OrderFilled-shaped log from a contract that is not a venue exchange.
	spoofed := transfer(94650300, 2, domain.ChainCashOut, testMakerA, "2")
	spoofedIn := transfer(94650300, 3, domain.ChainCashIn, testMakerA, "3")
	logs := &fakeLogs{
		byWallet: map[string][]domain.ChainCashTransfer{testWallet6: {makerA, makerB, fee, sell, stranger, spoofed, spoofedIn}},
		fills: map[string][]domain.ChainCashFill{testWallet6: {
			fillFor(makerA, testExchange, testWallet6, 12),
			fillFor(makerB, testExchange, testWallet6, 14),
			sellFill,
			fillFor(spoofed, testFakeVenue, testWallet6, 1),
		}},
	}
	service := newTestService(t, logs, &fakeStore{}, 0)
	classified, err := service.ReadConfirmedTransfers(context.Background(), "wallet-6", testWallet6, 94650000, 94650300)
	if err != nil {
		t.Fatal(err)
	}
	want := []domain.ChainCashClassification{
		domain.ChainCashTrade, domain.ChainCashTrade, domain.ChainCashTrade, domain.ChainCashTrade,
		domain.ChainCashUnattributedOut, domain.ChainCashUnattributedOut, domain.ChainCashUnattributedIn,
	}
	if len(classified) != len(want) {
		t.Fatalf("classified = %#v", classified)
	}
	for index, value := range classified {
		if value.Classification != want[index] {
			t.Fatalf("transfer %d (%s -> %s) = %s, want %s", index, value.Direction, value.Counterparty, value.Classification, want[index])
		}
	}
}

func TestClassificationFillEvidenceMustAgreeWithTransfers(t *testing.T) {
	payment := transfer(500, 1, domain.ChainCashOut, testMakerA, "4")
	for name, fill := range map[string]domain.ChainCashFill{
		"forked block hash": func() domain.ChainCashFill {
			value := fillFor(payment, testExchange, testWallet6, 2)
			value.BlockHash = hash(999)
			return value
		}(),
		"malformed fill": func() domain.ChainCashFill {
			value := fillFor(payment, testExchange, testWallet6, 2)
			value.Exchange = "0x12"
			return value
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			logs := &fakeLogs{
				byWallet: map[string][]domain.ChainCashTransfer{testWallet6: {payment}},
				fills:    map[string][]domain.ChainCashFill{testWallet6: {fill}},
			}
			service := newTestService(t, logs, &fakeStore{}, 0)
			if classified, err := service.ReadConfirmedTransfers(context.Background(), "wallet-6", testWallet6, 400, 600); err == nil {
				t.Fatalf("inconsistent fill evidence accepted: %#v", classified)
			}
		})
	}
}

func TestSyncCountsPendingPeerToPeerSettlementAsTrade(t *testing.T) {
	// Unconfirmed SELL proceeds from a counterparty belong to the fill
	// ledger's finality window; counting them would double the cash.
	logs, store := syncFixture(1000, "10", "0")
	proceeds := transfer(1190, 3, domain.ChainCashIn, testMakerB, "7.5")
	logs.byWallet[testWallet6] = []domain.ChainCashTransfer{proceeds}
	logs.fills = map[string][]domain.ChainCashFill{testWallet6: {fillFor(proceeds, testExchange, testWallet6, 4)}}
	service := newTestService(t, logs, store, 0)
	result, err := service.Sync(context.Background(), SyncParams{ExecutionAccountID: "wallet-6", WalletAddress: testWallet6, SnapshotBlock: 1200})
	if err != nil {
		t.Fatal(err)
	}
	if result.PendingOffset != "0" || result.Offset != "0" {
		t.Fatalf("pending peer-to-peer proceeds were counted: %#v", result)
	}
}

func TestClassificationOfWalletToWalletTransferIsPerAccount(t *testing.T) {
	// One Transfer log from wallet-6 to wallet-7 is read once per wallet.
	shared := transfer(20, 4, domain.ChainCashOut, testWallet7, "12.5")
	incoming := shared
	incoming.Direction, incoming.Counterparty = domain.ChainCashIn, testWallet6
	logs := &fakeLogs{byWallet: map[string][]domain.ChainCashTransfer{testWallet6: {shared}, testWallet7: {incoming}}}
	service := newTestService(t, logs, &fakeStore{}, 0)
	six, err := service.ReadConfirmedTransfers(context.Background(), "wallet-6", testWallet6, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	seven, err := service.ReadConfirmedTransfers(context.Background(), "wallet-7", testWallet7, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(six) != 1 || six[0].Classification != domain.ChainCashUnattributedOut || six[0].ExecutionAccountID != "wallet-6" {
		t.Fatalf("wallet-6 view = %#v", six)
	}
	if len(seven) != 1 || seven[0].Classification != domain.ChainCashUnattributedIn || seven[0].ExecutionAccountID != "wallet-7" {
		t.Fatalf("wallet-7 view = %#v", seven)
	}
	if six[0].Reference() != seven[0].Reference() ||
		domain.ChainCashAccountEventID(six[0]) == domain.ChainCashAccountEventID(seven[0]) {
		t.Fatal("the shared log must keep one reference but distinct per-account event identities")
	}

	// Configured as a funding source, the sibling wallet becomes a deposit.
	funded, err := New(Params{
		Logs: logs, Store: &fakeStore{}, ExchangeAddresses: []string{testExchange},
		DepositSources: map[string][]string{"wallet-7": {testWallet6}}, Confirmations: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	seven, err = funded.ReadConfirmedTransfers(context.Background(), "wallet-7", testWallet7, 1, 100)
	if err != nil || seven[0].Classification != domain.ChainCashDeposit {
		t.Fatalf("configured sibling deposit = %#v %v", seven, err)
	}
}

func TestClassificationOfMixedRange(t *testing.T) {
	redemption := transfer(30, 1, domain.ChainCashIn, domain.ChainCashZeroAddress, "20")
	values := []domain.ChainCashTransfer{
		transfer(30, 0, domain.ChainCashOut, testExchange, "8.8"),
		redemption,
		transfer(31, 0, domain.ChainCashIn, testRewardSender, "0.0015"),
		transfer(31, 1, domain.ChainCashIn, testEOA6, "100"),
		transfer(32, 0, domain.ChainCashIn, testStranger, "0.5"),
		transfer(32, 1, domain.ChainCashOut, testStranger, "0.25"),
	}
	store := &fakeStore{redemptions: map[string]map[string]struct{}{"wallet-6": {redemption.TransactionHash: {}}}}
	service := newTestService(t, &fakeLogs{byWallet: map[string][]domain.ChainCashTransfer{testWallet6: values}}, store, 0)
	classified, err := service.ReadConfirmedTransfers(context.Background(), "wallet-6", testWallet6, 30, 32)
	if err != nil {
		t.Fatal(err)
	}
	want := []domain.ChainCashClassification{
		domain.ChainCashTrade, domain.ChainCashRedemption, domain.ChainCashReward,
		domain.ChainCashDeposit, domain.ChainCashUnattributedIn, domain.ChainCashUnattributedOut,
	}
	for index, value := range classified {
		if value.Classification != want[index] {
			t.Fatalf("transfer %d = %s, want %s", index, value.Classification, want[index])
		}
	}
	net, err := SumSigned(classified)
	if err != nil || net != "111.4515" {
		t.Fatalf("net = %s %v", net, err)
	}
}

func TestSyncWithoutCursorIsDisabled(t *testing.T) {
	logs := &fakeLogs{}
	service := newTestService(t, logs, &fakeStore{cursors: map[string]domain.ChainCashCursor{}}, 0)
	result, err := service.Sync(context.Background(), SyncParams{ExecutionAccountID: "main", WalletAddress: testWallet6, SnapshotBlock: 1000})
	if err != nil || result.Enabled || len(logs.calls) != 0 || len(logs.required) != 0 {
		t.Fatalf("result=%#v err=%v calls=%#v", result, err, logs.calls)
	}
}

func syncFixture(processed uint64, ledger, offset domain.Decimal, transfers ...domain.ChainCashTransfer) (*fakeLogs, *fakeStore) {
	logs := &fakeLogs{byWallet: map[string][]domain.ChainCashTransfer{testWallet6: transfers}}
	store := &fakeStore{
		cursors: map[string]domain.ChainCashCursor{"wallet-6": {
			ExecutionAccountID: "wallet-6", WalletAddress: testWallet6, StartBlock: 100, ProcessedBlock: processed,
		}},
		ledger: ledger, offset: offset,
	}
	return logs, store
}

func TestSyncPersistsConfirmedRangeAndCountsUnconfirmedTransfersInMemory(t *testing.T) {
	logs, store := syncFixture(1000, "10", "0.5",
		transfer(1100, 0, domain.ChainCashIn, testRewardSender, "0.0015"),
		transfer(1190, 0, domain.ChainCashIn, testRewardSender, "0.25"),
		transfer(1195, 0, domain.ChainCashOut, testExchange, "3"),
		transfer(1200, 0, domain.ChainCashOut, testStranger, "0.1"),
	)
	service := newTestService(t, logs, store, 0)
	result, err := service.Sync(context.Background(), SyncParams{
		ExecutionAccountID: "wallet-6", WalletAddress: strings.ToUpper(testWallet6[:2]) + testWallet6[2:], SnapshotBlock: 1200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs.required) != 1 || logs.required[0] != 1200 {
		t.Fatalf("snapshot block availability not checked: %#v", logs.required)
	}
	if len(logs.calls) != 2 || logs.calls[0] != (listCall{testWallet6, 1001, 1136}) || logs.calls[1] != (listCall{testWallet6, 1137, 1200}) {
		t.Fatalf("log reads = %#v", logs.calls)
	}
	if len(store.applied) != 1 || store.applied[0].FromBlock != 1000 || store.applied[0].ToBlock != 1136 ||
		len(store.applied[0].Transfers) != 1 || store.applied[0].Transfers[0].Classification != domain.ChainCashReward {
		t.Fatalf("applied = %#v", store.applied)
	}
	// Pending: +0.25 reward only. The 3 pUSD trade is left to the fill
	// ledger's own finality explanation, and the unconfirmed 0.1 unattributed
	// outflow never explains a deficit before its blocking issue exists.
	if !result.Enabled || !result.CaughtUp || result.LedgerTotal != "10" || result.PersistedOffset != "0.5" ||
		result.PendingOffset != "0.25" || result.Offset != "0.75" || result.ProcessedBlock != 1136 || result.ConfirmedBlock != 1136 {
		t.Fatalf("result = %#v", result)
	}
}

func TestSyncCapsBlocksPerRunAndReportsNotCaughtUp(t *testing.T) {
	logs, store := syncFixture(1000, "10", "0")
	service := newTestService(t, logs, store, 3000)
	result, err := service.Sync(context.Background(), SyncParams{ExecutionAccountID: "wallet-6", WalletAddress: testWallet6, SnapshotBlock: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if result.CaughtUp || result.ProcessedBlock != 4000 || result.ConfirmedBlock != 9936 || len(logs.calls) != 1 ||
		logs.calls[0] != (listCall{testWallet6, 1001, 4000}) {
		t.Fatalf("result=%#v calls=%#v", result, logs.calls)
	}
}

func TestSyncReadFailureNeverAdvancesCursor(t *testing.T) {
	for _, failCall := range []int{1, 2} {
		t.Run(fmt.Sprintf("read %d fails", failCall), func(t *testing.T) {
			logs, store := syncFixture(1000, "10", "0", transfer(1100, 0, domain.ChainCashIn, testRewardSender, "1"))
			logs.failCall = failCall
			service := newTestService(t, logs, store, 0)
			result, err := service.Sync(context.Background(), SyncParams{ExecutionAccountID: "wallet-6", WalletAddress: testWallet6, SnapshotBlock: 1200})
			if err == nil || !result.Enabled || len(store.applied) != 0 || store.cursors["wallet-6"].ProcessedBlock != 1000 {
				t.Fatalf("result=%#v err=%v applied=%#v", result, err, store.applied)
			}
		})
	}
	logs, store := syncFixture(1000, "10", "0")
	logs.requireErr = errors.New("RPC backend does not serve block 1200 yet")
	service := newTestService(t, logs, store, 0)
	if _, err := service.Sync(context.Background(), SyncParams{ExecutionAccountID: "wallet-6", WalletAddress: testWallet6, SnapshotBlock: 1200}); err == nil ||
		len(logs.calls) != 0 || len(store.applied) != 0 {
		t.Fatalf("lagging backend accepted: %v", err)
	}
}

func TestSyncRejectsInconsistentSnapshots(t *testing.T) {
	for _, test := range []struct {
		name     string
		wallet   string
		snapshot uint64
	}{
		{name: "wallet changed", wallet: testWallet7, snapshot: 1200},
		{name: "snapshot behind cursor", wallet: testWallet6, snapshot: 999},
		{name: "snapshot inside confirmation depth", wallet: testWallet6, snapshot: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs, store := syncFixture(1000, "10", "0")
			service := newTestService(t, logs, store, 0)
			result, err := service.Sync(context.Background(), SyncParams{ExecutionAccountID: "wallet-6", WalletAddress: test.wallet, SnapshotBlock: test.snapshot})
			if err == nil || !result.Enabled || len(store.applied) != 0 {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
}

func TestParseAddressConfiguration(t *testing.T) {
	addresses, err := ParseAddressList(" 0x607C8C9866EF3B4665C5A384188706BE738D8BF8 ,, " + testStranger)
	if err != nil || strings.Join(addresses, ",") != testRewardSender+","+testStranger {
		t.Fatalf("addresses = %#v %v", addresses, err)
	}
	if empty, err := ParseAddressList(" "); err != nil || len(empty) != 0 {
		t.Fatalf("empty = %#v %v", empty, err)
	}
	for _, invalid := range []string{"0x1234", "607c8c9866ef3b4665c5a384188706be738d8bf8", "0xzz7c8c9866ef3b4665c5a384188706be738d8bf8", domain.ChainCashZeroAddress} {
		if _, err := ParseAddressList(invalid); err == nil {
			t.Fatalf("%q accepted", invalid)
		}
	}
	sources, err := ParseDepositSources("wallet-6=0x0AEFD80D000000000000000000000000000593F0, wallet-7=" + testEOA7 + ",wallet-6=" + testStranger)
	if err != nil || len(sources) != 2 || strings.Join(sources["wallet-6"], ",") != testEOA6+","+testStranger || sources["wallet-7"][0] != testEOA7 {
		t.Fatalf("sources = %#v %v", sources, err)
	}
	for _, invalid := range []string{testEOA6, "=" + testEOA6, "wallet-6=0x12"} {
		if _, err := ParseDepositSources(invalid); err == nil {
			t.Fatalf("%q accepted", invalid)
		}
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	base := Params{Logs: &fakeLogs{}, Store: &fakeStore{}, ExchangeAddresses: []string{testExchange}, Confirmations: 64}
	for name, mutate := range map[string]func(*Params){
		"no logs":          func(params *Params) { params.Logs = nil },
		"no confirmations": func(params *Params) { params.Confirmations = 0 },
		"no exchanges":     func(params *Params) { params.ExchangeAddresses = nil },
		"bad reward":       func(params *Params) { params.RewardSenders = []string{"0x12"} },
		"bad deposit":      func(params *Params) { params.DepositSources = map[string][]string{"wallet-6": {"0x12"}} },
	} {
		params := base
		mutate(&params)
		if _, err := New(params); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

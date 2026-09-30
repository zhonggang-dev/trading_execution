package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/chaincash"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	chainCashTestToken    = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
	chainCashRewardSender = "0x607c8c9866ef3b4665c5a384188706be738d8bf8"
	chainCashStranger     = "0x9999999999999999999999999999999999999999"
	chainCashExchange     = "0xe111180000d2663c0091e4f400237545b87b996b"
	chainCashWallet6      = "0x6666666666666666666666666666666666666666"
	chainCashWallet7      = "0x7777777777777777777777777777777777777777"
	chainCashLegacyWallet = "0x5555555555555555555555555555555555555555"
)

var chainCashIDSequence atomic.Int64

// memoryTransferLogs is the eth_getLogs view of the chain used by the
// integration tests: pUSD Transfers and venue OrderFilled logs per wallet.
// Every range read is recorded so tests can prove which blocks the service
// asked for.
type memoryTransferLogs struct {
	mu       sync.Mutex
	byWallet map[string][]domain.ChainCashTransfer
	fills    map[string][]domain.ChainCashFill
	calls    [][3]uint64
	fail     bool
}

// addFill records an OrderFilled emitted by exchange in the transaction of
// transfer that names wallet as the maker.
func (logs *memoryTransferLogs) addFill(wallet, exchange string, transfer domain.ChainCashTransfer) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	if logs.fills == nil {
		logs.fills = map[string][]domain.ChainCashFill{}
	}
	logs.fills[wallet] = append(logs.fills[wallet], domain.ChainCashFill{
		TransactionHash: transfer.TransactionHash, LogIndex: transfer.LogIndex + 1, BlockNumber: transfer.BlockNumber,
		BlockHash: transfer.BlockHash, Exchange: exchange, Maker: wallet, Taker: chainCashStranger,
	})
}

func (logs *memoryTransferLogs) add(wallet string, transfer domain.ChainCashTransfer) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	logs.byWallet[wallet] = append(logs.byWallet[wallet], transfer)
}

func (logs *memoryTransferLogs) ReadWalletLogs(_ context.Context, wallet string, from, to uint64) (domain.ChainCashLogs, error) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	logs.calls = append(logs.calls, [3]uint64{0, from, to})
	if logs.fail {
		return domain.ChainCashLogs{}, errors.New("read IN transfer logs [1001, 1036]: RPC HTTP status 502")
	}
	result := domain.ChainCashLogs{Transfers: make([]domain.ChainCashTransfer, 0)}
	for _, transfer := range logs.byWallet[wallet] {
		if transfer.BlockNumber >= from && transfer.BlockNumber <= to {
			result.Transfers = append(result.Transfers, transfer)
		}
	}
	for _, fill := range logs.fills[wallet] {
		if fill.BlockNumber >= from && fill.BlockNumber <= to {
			result.Fills = append(result.Fills, fill)
		}
	}
	return result, nil
}

func (logs *memoryTransferLogs) RequireBlock(context.Context, uint64) error { return nil }

func (logs *memoryTransferLogs) callCount() int {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return len(logs.calls)
}

func chainCashTransfer(block, logIndex uint64, direction domain.ChainCashDirection, counterparty string, amount domain.Decimal) domain.ChainCashTransfer {
	return domain.ChainCashTransfer{
		TransactionHash: fmt.Sprintf("0x%064x", block*1000+logIndex), LogIndex: logIndex, BlockNumber: block,
		BlockHash: fmt.Sprintf("0x%064x", block), Direction: direction, Counterparty: counterparty, Amount: amount,
		ObservedAt: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
	}
}

type chainCashHarness struct {
	t         *testing.T
	db        *sql.DB
	store     *ChainCashStore
	recorder  *ReconciliationRecorder
	logs      *memoryTransferLogs
	deposits  map[string][]string
	maxBlocks uint64
	now       time.Time
}

func newChainCashHarness(t *testing.T) *chainCashHarness {
	t.Helper()
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	store, err := NewChainCashStore(db)
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE execution_risk_global_control SET kill_switch=FALSE, reason='', version=version+1 WHERE singleton=TRUE`); err != nil {
		t.Fatal(err)
	}
	return &chainCashHarness{
		t: t, db: db, store: store, recorder: recorder,
		logs:     &memoryTransferLogs{byWallet: map[string][]domain.ChainCashTransfer{}},
		deposits: map[string][]string{}, now: time.Now().UTC().Truncate(time.Microsecond),
	}
}

func (harness *chainCashHarness) chainCash() *chaincash.Service {
	harness.t.Helper()
	service, err := chaincash.New(chaincash.Params{
		Logs: harness.logs, Store: harness.store, ExchangeAddresses: []string{chainCashExchange},
		RewardSenders: []string{chainCashRewardSender}, DepositSources: harness.deposits,
		Confirmations: 64, MaxBlocksPerRun: harness.maxBlocks, Now: func() time.Time { return harness.now },
	})
	if err != nil {
		harness.t.Fatal(err)
	}
	return service
}

type chainCashFakeVenue struct{}

func (chainCashFakeVenue) ListReconciliationOpenOrders(context.Context, string) ([]domain.VenueOrderSnapshot, error) {
	return nil, nil
}
func (chainCashFakeVenue) ListReconciliationTrades(context.Context, string, time.Time) ([]domain.VenueTradeSnapshot, error) {
	return nil, nil
}

type chainCashFakeFills struct{}

func (chainCashFakeFills) SyncOrder(context.Context, string) (port.FillSyncResult, error) {
	return port.FillSyncResult{}, nil
}

type chainCashFakeRefresher struct{}

func (chainCashFakeRefresher) Refresh(context.Context, string) (domain.Order, error) {
	return domain.Order{}, errors.New("no orders in chain cash fixtures")
}
func (chainCashFakeRefresher) FinalizeCancellation(context.Context, string) (domain.Order, error) {
	return domain.Order{}, errors.New("no orders in chain cash fixtures")
}

type chainCashFakePositions struct{}

func (chainCashFakePositions) ListExternalPositions(context.Context, string) ([]domain.ExternalPosition, error) {
	return nil, nil
}

type chainCashFakeBalance struct {
	amount domain.Decimal
	block  uint64
	at     time.Time
}

func (source chainCashFakeBalance) GetExternalBalance(context.Context, string, string) (domain.ExternalBalance, error) {
	return domain.ExternalBalance{Asset: "USDC", Amount: source.amount, Source: "EVM_ERC20_ETH_CALL", BlockNumber: source.block, ObservedAt: source.at}, nil
}

// reconcile runs the real reconciliation service against PostgreSQL with the
// external venue, positions, and balance replaced by deterministic fakes.
func (harness *chainCashHarness) reconcile(accountID string, chain domain.Decimal, snapshotBlock uint64) reconciliation.Result {
	harness.t.Helper()
	orders, err := NewOrderRepository(harness.db)
	if err != nil {
		harness.t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: harness.db})
	if err != nil {
		harness.t.Fatal(err)
	}
	baselines, err := NewExternalPositionBaselineRepository(harness.db)
	if err != nil {
		harness.t.Fatal(err)
	}
	redemptions, err := NewRedemptionProgressReader(harness.db)
	if err != nil {
		harness.t.Fatal(err)
	}
	leases, err := NewOrderRecoveryLeaseStore(harness.db)
	if err != nil {
		harness.t.Fatal(err)
	}
	now := harness.now
	guard, err := orderrecovery.NewGuard(orderrecovery.GuardParams{Store: leases, Now: func() time.Time { return now }})
	if err != nil {
		harness.t.Fatal(err)
	}
	service, err := reconciliation.New(reconciliation.Params{
		Orders: orders, Venue: chainCashFakeVenue{}, PositionSources: []port.ExternalPositionSource{chainCashFakePositions{}},
		PositionBaselines: baselines, PositionDispositionTrades: baselines,
		BalanceSources: []port.ExternalBalanceSource{chainCashFakeBalance{amount: chain, block: snapshotBlock, at: now}},
		Ledger:         ledger, Fills: chainCashFakeFills{}, OrderRefresher: chainCashFakeRefresher{}, Recorder: harness.recorder,
		Redemptions: redemptions, Recovery: guard, ChainCash: harness.chainCash(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now },
		NewID: func() string { return fmt.Sprintf("chain-cash-%d", chainCashIDSequence.Add(1)) },
	})
	if err != nil {
		harness.t.Fatal(err)
	}
	result, err := service.RunAccount(context.Background(), reconciliation.RunAccountParams{
		ExecutionAccountID: accountID, Trigger: domain.ReconciliationTriggerScheduled,
	})
	// A run with no local orders or positions ends FAILED on any source
	// error; callers assert the status they expect.
	if result.Run.Status == "" || (err != nil && result.Run.Status == domain.ReconciliationRunCompleted) {
		harness.t.Fatalf("reconciliation run = %#v err = %v", result.Run, err)
	}
	return result
}

func (harness *chainCashHarness) initialize(
	accountID, wallet string, startBlock uint64, startBalance domain.Decimal, catchUpBlock uint64, execute bool,
) (ChainCashCursorInitializationReport, error) {
	harness.t.Helper()
	transfers := []domain.ChainCashTransfer{}
	if catchUpBlock > startBlock {
		var err error
		transfers, err = harness.chainCash().ReadConfirmedTransfers(context.Background(), accountID, wallet, startBlock+1, catchUpBlock)
		if err != nil {
			harness.t.Fatal(err)
		}
	}
	return harness.store.InitializeChainCashCursor(context.Background(), ChainCashCursorInitialization{
		ExecutionAccountID: accountID, WalletAddress: wallet, TokenAddress: chainCashTestToken,
		StartBlock: startBlock, StartBlockTime: harness.now.Add(-time.Hour), StartBalance: startBalance,
		CatchUpBlock: catchUpBlock, Transfers: transfers, InitializedBy: "integration-test",
		Reason: "chain cash integration test", Evidence: map[string]any{"tool": "test"},
		AppliedAt: harness.now, Execute: execute,
	})
}

func (harness *chainCashHarness) mustInitialize(accountID, wallet string, startBlock uint64, balance domain.Decimal) {
	harness.t.Helper()
	if report, err := harness.initialize(accountID, wallet, startBlock, balance, startBlock, true); err != nil || !report.Executed {
		harness.t.Fatalf("initialize %s: %#v %v", accountID, report, err)
	}
}

func (harness *chainCashHarness) advance(duration time.Duration) {
	harness.now = harness.now.Add(duration)
}

func (harness *chainCashHarness) processedBlock(accountID string) int64 {
	harness.t.Helper()
	var processed int64
	if err := harness.db.QueryRow(`SELECT processed_block FROM execution_chain_cash_cursors WHERE execution_account_id=$1`, accountID).Scan(&processed); err != nil {
		harness.t.Fatal(err)
	}
	return processed
}

func (harness *chainCashHarness) count(query string, args ...any) int {
	harness.t.Helper()
	var count int
	if err := harness.db.QueryRow(query, args...).Scan(&count); err != nil {
		harness.t.Fatalf("%s: %v", query, err)
	}
	return count
}

func (harness *chainCashHarness) openIssues(accountID string, issueType domain.ReconciliationIssueType) []domain.ReconciliationIssue {
	harness.t.Helper()
	rows, err := harness.db.Query(`SELECT issue_id, resolution, impact_scope, COALESCE(local_value::text,''), COALESCE(remote_value::text,''),
		observed_at, first_observed_at, venue_trade_id FROM reconciliation_issues
		WHERE execution_account_id=$1 AND issue_type=$2 AND status='OPEN' ORDER BY issue_id`, accountID, string(issueType))
	if err != nil {
		harness.t.Fatal(err)
	}
	defer rows.Close()
	result := make([]domain.ReconciliationIssue, 0)
	for rows.Next() {
		var issue domain.ReconciliationIssue
		var firstObserved time.Time
		if err := rows.Scan(&issue.IssueID, &issue.Resolution, &issue.ImpactScope, &issue.LocalValue, &issue.RemoteValue,
			&issue.ObservedAt, &firstObserved, &issue.VenueTradeID); err != nil {
			harness.t.Fatal(err)
		}
		// Details carries first_observed_at for the lifecycle assertions.
		issue.Details = firstObserved.UTC().Format(time.RFC3339Nano)
		result = append(result, issue)
	}
	if err := rows.Err(); err != nil {
		harness.t.Fatal(err)
	}
	return result
}

// reserve places a live BUY through the atomic PostgreSQL risk authorization.
func (harness *chainCashHarness) reserve(accountID, orderID string) error {
	harness.t.Helper()
	provisionLiveRisk(harness.t, harness.db, liveRiskFixture{
		accountID: accountID, now: harness.now, binding: true,
		maxOrder: "100", maxMarket: "100", maxStrategy: "100", maxWallet: "100", maxDaily: "100",
	})
	now := harness.now
	manager, err := NewReservationManager(ReservationManagerParams{DB: harness.db, Now: func() time.Time { return now }, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		harness.t.Fatal(err)
	}
	_, err = manager.Reserve(context.Background(), liveIntegrationOrder(orderID, accountID, "token-"+orderID, "1", "0.5", now))
	return err
}

func (harness *chainCashHarness) removeRiskFixture(accountID string) {
	harness.t.Helper()
	for _, statement := range []string{
		`DELETE FROM asset_reservations WHERE execution_account_id=$1`,
		`DELETE FROM execution_risk_controls WHERE execution_account_id=$1`,
		`DELETE FROM execution_risk_policies WHERE execution_account_id=$1`,
		`DELETE FROM reconciliation_runs WHERE run_id='risk-run-'||$1`,
		`UPDATE execution_accounts SET available_balance=total_balance, reserved_balance=0 WHERE execution_account_id=$1`,
	} {
		if _, err := harness.db.Exec(statement, accountID); err != nil {
			harness.t.Fatalf("%s: %v", statement, err)
		}
	}
}

// TestChainCashWallet6RewardScenarioPostgresIntegration replays the production
// wallet-6 case: the ledger equals balanceOf at block 94681377, a 0.0015 pUSD
// holding reward arrives at 94681378, and an account-wide BALANCE_DRIFT is
// open. The offline catch-up credits the reward and one reconciliation run
// then closes every open balance issue without manual SQL.
func TestChainCashWallet6RewardScenarioPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	ctx := context.Background()
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "1.700022", "1.700022", "0")
	reward := chainCashTransfer(94681378, 511, domain.ChainCashIn, chainCashRewardSender, "0.0015")
	harness.logs.add(chainCashWallet6, reward)

	// The drift recorded by the release before the chain cash ledger.
	old := startReconciliationFixtureRun(t, harness.recorder, "wallet-6", "wallet-6-before-upgrade", harness.now.Add(-2*time.Hour))
	if err := harness.recorder.RecordIssue(ctx, domain.ReconciliationIssue{
		IssueID: "wallet-6-legacy-drift", RunID: old.RunID, Fingerprint: "legacy-amount-bound-fingerprint",
		ExecutionAccountID: "wallet-6", Type: domain.ReconciliationIssueBalanceDrift, Status: domain.ReconciliationIssueOpen,
		Resolution: domain.ReconciliationResolutionManual, LocalValue: "1.700022", RemoteValue: "1.701522",
		RemoteBlockNumber: 94681400, Source: "EVM_ERC20_ETH_CALL", Details: "legacy drift",
		ObservedAt: harness.now.Add(-2 * time.Hour), ImpactScope: domain.ReconciliationImpactAccount,
	}); err != nil {
		t.Fatal(err)
	}
	completeReconciliationFixtureRun(t, harness.recorder, old, domain.ReconciliationRunAttentionRequired, harness.now.Add(-2*time.Hour))

	// New release, cursor not yet initialized: exact legacy behaviour.
	legacy := harness.reconcile("wallet-6", "1.701522", 94700064)
	if harness.logs.callCount() != 0 || len(harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)) != 2 ||
		!legacy.Impact.AccountWide {
		t.Fatalf("legacy run: calls=%d issues=%#v impact=%#v", harness.logs.callCount(), legacy.Issues, legacy.Impact)
	}
	assertRejectionCode(t, harness.reserve("wallet-6", "wallet-6-before"), "RISK_STATE_HAS_OPEN_ISSUES")
	harness.removeRiskFixture("wallet-6")

	// Dry run: every write is rolled back.
	report, err := harness.initialize("wallet-6", chainCashWallet6, 94681377, "1.700022", 94700000, false)
	if err != nil || report.Executed || len(report.Credited) != 1 || !report.LedgerTotalAfter.Equal("1.701522") {
		t.Fatalf("dry run report = %#v err = %v", report, err)
	}
	if harness.count(`SELECT count(*) FROM execution_chain_cash_cursors`) != 0 ||
		harness.count(`SELECT count(*) FROM execution_chain_cash_transfers`) != 0 ||
		harness.count(`SELECT count(*) FROM execution_account_events`) != 0 {
		t.Fatal("dry run persisted chain cash state")
	}
	assertAccount(t, harness.db, "wallet-6", "1.700022", "1.700022", "0")

	// Execute: offline catch-up from before the reward to the confirmed head.
	report, err = harness.initialize("wallet-6", chainCashWallet6, 94681377, "1.700022", 94700000, true)
	if err != nil || !report.Executed || report.TransfersPersisted != 1 || len(report.Credited) != 1 ||
		!report.LedgerTotalAfter.Equal("1.701522") || !report.PersistedOffsetAfter.Equal("0") {
		t.Fatalf("execute report = %#v err = %v", report, err)
	}
	assertAccount(t, harness.db, "wallet-6", "1.701522", "1.701522", "0")
	if harness.processedBlock("wallet-6") != 94700000 {
		t.Fatalf("processed block = %d", harness.processedBlock("wallet-6"))
	}
	eventID := "account-chain-cash:wallet-6:" + reward.TransactionHash + ":511"
	if harness.count(`SELECT count(*) FROM execution_account_events WHERE account_event_id=$1 AND event_type='CHAIN_CASH_REWARD'
		AND total_balance_delta=0.0015 AND available_balance_delta=0.0015 AND reserved_balance_delta=0 AND total_balance_after=1.701522`, eventID) != 1 ||
		harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE account_event_id=$1 AND classification='REWARD'`, eventID) != 1 ||
		harness.count(`SELECT count(*) FROM execution_outbox WHERE topic='trading.account.chain_cash_credited.v1' AND event_key=$1`, eventID) != 1 {
		t.Fatal("reward credit evidence is incomplete")
	}
	if _, err := harness.initialize("wallet-6", chainCashWallet6, 94681377, "1.700022", 94700000, true); err == nil ||
		!strings.Contains(err.Error(), "already has a chain cash cursor") {
		t.Fatalf("second initialization error = %v", err)
	}

	// The service only processes the increment and closes both balance issues.
	harness.advance(5 * time.Minute)
	result := harness.reconcile("wallet-6", "1.701522", 94700100)
	if result.Run.Status != domain.ReconciliationRunCompleted || result.Impact.AccountWide ||
		result.Run.Summary["verified_balance:"] != 1 {
		t.Fatalf("run = %#v issues = %#v", result.Run, result.Issues)
	}
	calls := harness.logs.calls[len(harness.logs.calls)-2:]
	if calls[0][1] != 94700001 || calls[0][2] != 94700036 || calls[1][1] != 94700037 || calls[1][2] != 94700100 {
		t.Fatalf("increment reads = %#v", harness.logs.calls)
	}
	if open := harness.count(`SELECT count(*) FROM reconciliation_issues WHERE execution_account_id='wallet-6' AND status='OPEN'`); open != 0 {
		t.Fatalf("open issues = %d", open)
	}
	if harness.count(`SELECT count(*) FROM reconciliation_issues WHERE issue_id='wallet-6-legacy-drift' AND status='RESOLVED'
		AND resolution='AUTOMATIC' AND details LIKE '%chain cash ledger reconciled%'`) != 1 {
		t.Fatal("legacy drift was not closed by the chain cash resolver")
	}
	assertAccount(t, harness.db, "wallet-6", "1.701522", "1.701522", "0")
	if err := harness.reserve("wallet-6", "wallet-6-after"); err != nil {
		t.Fatalf("reserve after reconciliation: %v", err)
	}
}

func TestChainCashRewardCreditIsIdempotentPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-7", chainCashWallet7, "100", "80", "20")
	harness.mustInitialize("wallet-7", chainCashWallet7, 1000, "100")
	reward := chainCashTransfer(1010, 2, domain.ChainCashIn, chainCashRewardSender, "0.25")
	harness.logs.add(chainCashWallet7, reward)
	harness.logs.add(chainCashWallet7, chainCashTransfer(1020, 0, domain.ChainCashOut, chainCashExchange, "5"))
	harness.logs.add(chainCashWallet7, chainCashTransfer(1021, 0, domain.ChainCashIn, chainCashExchange, "5"))

	for run := 0; run < 3; run++ {
		harness.advance(time.Minute)
		result := harness.reconcile("wallet-7", "100.25", 1100)
		if result.Run.Status != domain.ReconciliationRunCompleted || result.Impact.AccountWide || result.Run.Summary["verified_balance:"] != 1 {
			t.Fatalf("run %d = %#v issues = %#v", run, result.Run, result.Issues)
		}
		// Reserved cash is untouched; the reward is spendable immediately.
		assertAccount(t, harness.db, "wallet-7", "100.25", "80.25", "20")
		if harness.count(`SELECT count(*) FROM execution_account_events WHERE execution_account_id='wallet-7'`) != 1 ||
			harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE execution_account_id='wallet-7'`) != 3 ||
			harness.count(`SELECT count(*) FROM execution_outbox WHERE topic='trading.account.chain_cash_credited.v1'`) != 1 {
			t.Fatalf("run %d duplicated the credit", run)
		}
		if harness.processedBlock("wallet-7") != 1036 {
			t.Fatalf("processed block = %d", harness.processedBlock("wallet-7"))
		}
	}
	if harness.count(`SELECT count(*) FROM execution_accounts WHERE execution_account_id='wallet-7'
		AND total_balance=available_balance+reserved_balance`) != 1 {
		t.Fatal("balance identity broken")
	}
	if harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE classification='TRADE' AND account_event_id IS NULL`) != 2 {
		t.Fatal("trade settlements must be classified but never credited")
	}
	if n := harness.count(`SELECT count(*) FROM reconciliation_issues WHERE execution_account_id='wallet-7'`); n != 0 {
		t.Fatalf("reward produced %d issues", n)
	}
}

// TestChainCashPeerToPeerTradeSettlementPostgresIntegration replays the V2
// settlement shape found on Polygon: pUSD moves directly between the wallet
// and counterparty makers (plus the fee collector) inside a transaction whose
// exchange OrderFilled names the wallet. Neither the BUY payments nor the SELL
// proceeds are unattributed cash, confirmed or not.
func TestChainCashPeerToPeerTradeSettlementPostgresIntegration(t *testing.T) {
	const (
		makerA       = "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
		makerB       = "0xb1ca909e00000000000000000000000000000001"
		feeCollector = "0x115f48dc2a731aa16251c6d6e1befc42f92accc9"
	)
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")

	// Transaction 0xc939c707...: two maker payments and a fee in one BUY.
	buyTx := fmt.Sprintf("0x%064x", 0xc939c707)
	buy := []domain.ChainCashTransfer{
		chainCashTransfer(1010, 11, domain.ChainCashOut, makerA, "4.80764"),
		chainCashTransfer(1010, 13, domain.ChainCashOut, makerB, "4.33236"),
		chainCashTransfer(1010, 15, domain.ChainCashOut, feeCollector, "0.02358"),
	}
	for index := range buy {
		buy[index].TransactionHash = buyTx
		harness.logs.add(chainCashWallet6, buy[index])
	}
	harness.logs.addFill(chainCashWallet6, chainCashExchange, buy[0])
	setLedger := func(total domain.Decimal) {
		t.Helper()
		// The fill ledger has booked the trade; only the chain cash view is under test.
		if _, err := harness.db.Exec(`UPDATE execution_accounts SET total_balance=$2, available_balance=$2, version=version+1
			WHERE execution_account_id=$1`, "wallet-6", string(total)); err != nil {
			t.Fatal(err)
		}
	}
	setLedger("0.83642")
	assertClean := func(label string, result reconciliation.Result) {
		t.Helper()
		if result.Run.Status != domain.ReconciliationRunCompleted || result.Impact.AccountWide ||
			result.Run.Summary["verified_balance:"] != 1 {
			t.Fatalf("%s: run = %#v issues = %#v", label, result.Run, result.Issues)
		}
		if n := harness.count(`SELECT count(*) FROM reconciliation_issues WHERE execution_account_id='wallet-6'`); n != 0 {
			t.Fatalf("%s: peer-to-peer settlement produced %d issues", label, n)
		}
	}
	assertClean("confirmed BUY", harness.reconcile("wallet-6", "0.83642", 1100))
	if harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE execution_account_id='wallet-6'
		AND classification='TRADE' AND transaction_hash=$1 AND account_event_id IS NULL`, buyTx) != 3 {
		t.Fatal("peer-to-peer BUY payments must be stored as uncredited TRADE")
	}

	// SELL proceeds paid directly by a counterparty; unconfirmed at 1200.
	proceeds := chainCashTransfer(1150, 4, domain.ChainCashIn, makerB, "10.01")
	harness.logs.add(chainCashWallet6, proceeds)
	harness.logs.addFill(chainCashWallet6, chainCashExchange, proceeds)
	setLedger("10.84642")
	harness.advance(5 * time.Minute)
	assertClean("unconfirmed SELL", harness.reconcile("wallet-6", "10.84642", 1200))
	harness.advance(5 * time.Minute)
	assertClean("confirmed SELL", harness.reconcile("wallet-6", "10.84642", 1300))
	if harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE execution_account_id='wallet-6'
		AND classification LIKE 'UNATTRIBUTED_%'`) != 0 ||
		harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE execution_account_id='wallet-6'
		AND classification='TRADE'`) != 4 {
		t.Fatal("peer-to-peer settlement was not classified as TRADE")
	}
	if harness.count(`SELECT count(*) FROM execution_account_events WHERE execution_account_id='wallet-6'`) != 0 {
		t.Fatal("trade settlement must never be credited by the chain cash ledger")
	}
	if err := harness.reserve("wallet-6", "p2p-after"); err != nil {
		t.Fatalf("reserve after peer-to-peer settlement: %v", err)
	}
}

// TestChainCashPaymentWithoutWalletFillBlocksPostgresIntegration proves the
// fill evidence is per transaction: an outflow to a stranger in a transaction
// without a wallet OrderFilled stays UNATTRIBUTED_OUT even while another
// transaction in the same range is a peer-to-peer settlement.
func TestChainCashPaymentWithoutWalletFillBlocksPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	settled := chainCashTransfer(1010, 1, domain.ChainCashOut, chainCashStranger, "1")
	harness.logs.add(chainCashWallet6, settled)
	harness.logs.addFill(chainCashWallet6, chainCashExchange, settled)
	unknown := chainCashTransfer(1011, 1, domain.ChainCashOut, chainCashStranger, "0.5")
	harness.logs.add(chainCashWallet6, unknown)
	if _, err := harness.db.Exec(`UPDATE execution_accounts SET total_balance=9, available_balance=9 WHERE execution_account_id='wallet-6'`); err != nil {
		t.Fatal(err)
	}
	result := harness.reconcile("wallet-6", "8.5", 1100)
	issues := harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashOut)
	if !result.Impact.AccountWide || len(issues) != 1 || issues[0].VenueTradeID != unknown.Reference() {
		t.Fatalf("impact = %#v outgoing issues = %#v", result.Impact, issues)
	}
	assertRejectionCode(t, harness.reserve("wallet-6", "no-fill"), "RISK_STATE_HAS_OPEN_ISSUES")
}

func TestChainCashResumesFromCursorAcrossBudgetedRunsPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	harness.maxBlocks = 50
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	harness.logs.add(chainCashWallet6, chainCashTransfer(1060, 0, domain.ChainCashIn, chainCashRewardSender, "1"))

	wantProcessed := []int64{1050, 1100, 1136, 1136}
	for index, want := range wantProcessed {
		harness.advance(time.Minute)
		result := harness.reconcile("wallet-6", "11", 1200)
		if got := harness.processedBlock("wallet-6"); got != want {
			t.Fatalf("run %d processed = %d, want %d", index, got, want)
		}
		caughtUp := want == 1136
		if caughtUp != (result.Run.Summary["verified_balance:"] == 1) {
			t.Fatalf("run %d verified=%d summary=%#v", index, result.Run.Summary["verified_balance:"], result.Run.Summary)
		}
		unavailable := harness.openIssues("wallet-6", domain.ReconciliationIssueSourceUnavailable)
		if caughtUp != (len(unavailable) == 0) {
			t.Fatalf("run %d source unavailable = %#v", index, unavailable)
		}
		if !caughtUp && (unavailable[0].ImpactScope != domain.ReconciliationImpactAccount || !result.Impact.AccountWide) {
			t.Fatalf("catching up must block the account: %#v", unavailable)
		}
	}
	assertAccount(t, harness.db, "wallet-6", "11", "11", "0")
	if harness.count(`SELECT count(*) FROM execution_account_events WHERE execution_account_id='wallet-6'`) != 1 {
		t.Fatal("reward credited more than once across resumed runs")
	}
}

func TestChainCashLogFailureFailsClosedAndRecoversPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	harness.logs.add(chainCashWallet6, chainCashTransfer(1010, 0, domain.ChainCashIn, chainCashRewardSender, "0.5"))

	harness.logs.fail = true
	result := harness.reconcile("wallet-6", "10.5", 1100)
	unavailable := harness.openIssues("wallet-6", domain.ReconciliationIssueSourceUnavailable)
	if len(unavailable) != 1 || unavailable[0].ImpactScope != domain.ReconciliationImpactAccount || !result.Impact.AccountWide ||
		result.Run.Summary["verified_balance:"] == 1 {
		t.Fatalf("failed read: issues=%#v impact=%#v", unavailable, result.Impact)
	}
	if harness.processedBlock("wallet-6") != 1000 || harness.count(`SELECT count(*) FROM execution_chain_cash_transfers`) != 0 {
		t.Fatal("failed read advanced the cursor or persisted transfers")
	}
	assertAccount(t, harness.db, "wallet-6", "10", "10", "0")
	if harness.count(`SELECT count(*) FROM reconciliation_issues WHERE issue_type='BALANCE_DRIFT'`) != 0 {
		t.Fatal("an unreadable source was compared as if it had no transfers")
	}
	assertRejectionCode(t, harness.reserve("wallet-6", "blocked-by-source"), "RISK_STATE_HAS_OPEN_ISSUES")
	harness.removeRiskFixture("wallet-6")

	harness.logs.fail = false
	harness.advance(5 * time.Minute)
	result = harness.reconcile("wallet-6", "10.5", 1100)
	if result.Run.Status != domain.ReconciliationRunCompleted || result.Run.Summary["verified_balance:"] != 1 {
		t.Fatalf("recovered run = %#v issues = %#v", result.Run, result.Issues)
	}
	if len(harness.openIssues("wallet-6", domain.ReconciliationIssueSourceUnavailable)) != 0 || harness.processedBlock("wallet-6") != 1036 {
		t.Fatal("recovered run did not resolve the outage")
	}
	assertAccount(t, harness.db, "wallet-6", "10.5", "10.5", "0")
}

func TestChainCashUnattributedIncomingIsObservationOnlyPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	unknown := chainCashTransfer(1010, 4, domain.ChainCashIn, chainCashStranger, "0.75")
	harness.logs.add(chainCashWallet6, unknown)

	for run := 0; run < 2; run++ {
		harness.advance(5 * time.Minute)
		result := harness.reconcile("wallet-6", "10.75", 1100)
		if result.Impact.AccountWide || result.Run.Summary["verified_balance:"] != 1 {
			t.Fatalf("run %d = %#v impact = %#v", run, result.Run, result.Impact)
		}
		issues := harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashIn)
		if len(issues) != 1 || issues[0].Resolution != domain.ReconciliationResolutionObserved ||
			issues[0].ImpactScope != domain.ReconciliationImpactNone || issues[0].VenueTradeID != unknown.Reference() {
			t.Fatalf("run %d unattributed issues = %#v", run, issues)
		}
	}
	// Never credited: the ledger keeps its own total.
	assertAccount(t, harness.db, "wallet-6", "10", "10", "0")
	if harness.count(`SELECT count(*) FROM execution_account_events`) != 0 {
		t.Fatal("unattributed cash was credited")
	}
	if err := harness.reserve("wallet-6", "unknown-in"); err != nil {
		t.Fatalf("unattributed incoming cash blocked trading: %v", err)
	}
}

func TestChainCashSurplusBeyondKnownTransfersIsObservationOnlyPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	result := harness.reconcile("wallet-6", "10.2", 1100)
	drifts := harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)
	if len(drifts) != 1 || drifts[0].Resolution != domain.ReconciliationResolutionObserved ||
		drifts[0].ImpactScope != domain.ReconciliationImpactNone || result.Impact.AccountWide {
		t.Fatalf("surplus drift = %#v impact = %#v", drifts, result.Impact)
	}
	if err := harness.reserve("wallet-6", "surplus"); err != nil {
		t.Fatalf("surplus blocked trading: %v", err)
	}
}

func TestChainCashUnexplainedDeficitBlocksAccountPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	firstAt := harness.now
	result := harness.reconcile("wallet-6", "9", 1100)
	drifts := harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)
	if len(drifts) != 1 || drifts[0].Resolution != domain.ReconciliationResolutionManual ||
		drifts[0].ImpactScope != domain.ReconciliationImpactAccount || !result.Impact.AccountWide {
		t.Fatalf("deficit drift = %#v impact = %#v", drifts, result.Impact)
	}
	assertRejectionCode(t, harness.reserve("wallet-6", "deficit"), "RISK_STATE_HAS_OPEN_ISSUES")
	harness.removeRiskFixture("wallet-6")

	// A changed amount updates the same OPEN row and keeps its first discovery.
	harness.advance(5 * time.Minute)
	harness.reconcile("wallet-6", "8.5", 1200)
	drifts = harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)
	if len(drifts) != 1 || !drifts[0].LocalValue.Equal("10") || !drifts[0].RemoteValue.Equal("8.5") ||
		!drifts[0].ObservedAt.Equal(harness.now) || drifts[0].Details != firstAt.Format(time.RFC3339Nano) {
		t.Fatalf("drift after amount change = %#v (first observed %s)", drifts, firstAt)
	}
	if harness.count(`SELECT count(*) FROM reconciliation_issues WHERE execution_account_id='wallet-6' AND issue_type='BALANCE_DRIFT'`) != 1 {
		t.Fatal("amount change opened another balance issue")
	}

	// Once the balance reconciles again the issue closes automatically.
	harness.advance(5 * time.Minute)
	result = harness.reconcile("wallet-6", "10", 1300)
	if result.Run.Status != domain.ReconciliationRunCompleted || len(harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)) != 0 {
		t.Fatalf("reconciled run = %#v", result.Run)
	}
}

func TestChainCashUnattributedOutgoingBlocksUntilManualReviewPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	outgoing := chainCashTransfer(1010, 1, domain.ChainCashOut, chainCashStranger, "0.25")
	harness.logs.add(chainCashWallet6, outgoing)

	for run := 0; run < 2; run++ {
		harness.advance(5 * time.Minute)
		result := harness.reconcile("wallet-6", "9.75", 1100)
		// The ledger was never debited, so the outflow never explains the
		// deficit: BALANCE_DRIFT keeps gating next to the outflow's own issue,
		// and neither is auto-closed.
		drifts := harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)
		if result.Run.Summary["verified_balance:"] == 1 || !result.Impact.AccountWide || len(drifts) != 1 ||
			drifts[0].Resolution != domain.ReconciliationResolutionManual {
			t.Fatalf("run %d = %#v impact = %#v drifts = %#v", run, result.Run, result.Impact, drifts)
		}
		issues := harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashOut)
		if len(issues) != 1 || issues[0].ImpactScope != domain.ReconciliationImpactAccount ||
			issues[0].Resolution != domain.ReconciliationResolutionManual || issues[0].VenueTradeID != outgoing.Reference() {
			t.Fatalf("run %d outgoing issues = %#v", run, issues)
		}
	}
	assertRejectionCode(t, harness.reserve("wallet-6", "outgoing"), "RISK_STATE_HAS_OPEN_ISSUES")
}

// TestChainCashUnconfirmedOutgoingNeverExplainsADeficitPostgresIntegration
// proves there is no window in which an unknown outflow leaves the account
// unblocked: the deficit is a manual BALANCE_DRIFT before and after finality,
// and once final the outflow's own UNATTRIBUTED_CASH_OUT issue gates as well.
func TestChainCashUnconfirmedOutgoingNeverExplainsADeficitPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	outgoing := chainCashTransfer(1050, 1, domain.ChainCashOut, chainCashStranger, "0.25")
	harness.logs.add(chainCashWallet6, outgoing)

	// Snapshot 1100: confirmed head 1036, the outflow at 1050 is unconfirmed.
	result := harness.reconcile("wallet-6", "9.75", 1100)
	drifts := harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)
	if result.Run.Summary["verified_balance:"] == 1 || !result.Impact.AccountWide || len(drifts) != 1 ||
		drifts[0].Resolution != domain.ReconciliationResolutionManual || drifts[0].ImpactScope != domain.ReconciliationImpactAccount ||
		len(harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashOut)) != 0 {
		t.Fatalf("unconfirmed outflow run = %#v drifts = %#v", result.Run, drifts)
	}
	assertRejectionCode(t, harness.reserve("wallet-6", "unconfirmed-out"), "RISK_STATE_HAS_OPEN_ISSUES")
	harness.removeRiskFixture("wallet-6")

	// Snapshot 1200: the outflow is final and persisted; both issues gate.
	harness.advance(5 * time.Minute)
	result = harness.reconcile("wallet-6", "9.75", 1200)
	if result.Run.Summary["verified_balance:"] == 1 || !result.Impact.AccountWide ||
		len(harness.openIssues("wallet-6", domain.ReconciliationIssueBalanceDrift)) != 1 ||
		len(harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashOut)) != 1 {
		t.Fatalf("confirmed outflow run = %#v issues = %#v", result.Run, result.Issues)
	}
	assertRejectionCode(t, harness.reserve("wallet-6", "confirmed-out"), "RISK_STATE_HAS_OPEN_ISSUES")
}

func TestChainCashWalletToWalletTransferIsStoredPerAccountPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	harness.deposits["wallet-7"] = []string{chainCashWallet6}
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	insertAccount(t, harness.db, "wallet-7", chainCashWallet7, "3", "3", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	harness.mustInitialize("wallet-7", chainCashWallet7, 1000, "3")
	shared := chainCashTransfer(1010, 9, domain.ChainCashOut, chainCashWallet7, "5")
	incoming := shared
	incoming.Direction, incoming.Counterparty = domain.ChainCashIn, chainCashWallet6
	harness.logs.add(chainCashWallet6, shared)
	harness.logs.add(chainCashWallet7, incoming)

	harness.reconcile("wallet-6", "5", 1100)
	seven := harness.reconcile("wallet-7", "8", 1100)
	if seven.Run.Status != domain.ReconciliationRunCompleted || seven.Run.Summary["verified_balance:"] != 1 {
		t.Fatalf("wallet-7 run = %#v issues = %#v", seven.Run, seven.Issues)
	}
	if harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE transaction_hash=$1 AND log_index=9`, shared.TransactionHash) != 2 ||
		harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE execution_account_id='wallet-6' AND classification='UNATTRIBUTED_OUT'`) != 1 ||
		harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE execution_account_id='wallet-7' AND classification='DEPOSIT'
			AND account_event_id=$1`, "account-chain-cash:wallet-7:"+shared.TransactionHash+":9") != 1 {
		t.Fatal("wallet-to-wallet transfer was not stored once per account")
	}
	assertAccount(t, harness.db, "wallet-6", "10", "10", "0")
	assertAccount(t, harness.db, "wallet-7", "8", "8", "0")
	if len(harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashOut)) != 1 {
		t.Fatal("outgoing side must still require manual review")
	}
}

func TestChainCashRedemptionMintIsReattributedPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	mint := chainCashTransfer(1010, 3, domain.ChainCashIn, domain.ChainCashZeroAddress, "20")
	harness.logs.add(chainCashWallet6, mint)

	// The payout is final before autoredeem learned the relayer transaction.
	harness.reconcile("wallet-6", "30", 1100)
	if len(harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashIn)) != 1 {
		t.Fatal("unknown mint was not recorded for audit")
	}
	if _, err := harness.db.Exec(`INSERT INTO polymarket_redemptions (
		execution_account_id, condition_id, wallet_address, neg_risk, status, transaction_hash, last_error
	) VALUES ('wallet-6', $1, $2, FALSE, 'MANUAL_REVIEW', $3, 'test fixture')`,
		"0x"+strings.Repeat("c", 64), chainCashWallet6, mint.TransactionHash); err != nil {
		t.Fatal(err)
	}
	// The redemption ledger books the payout.
	if _, err := harness.db.Exec(`UPDATE execution_accounts SET total_balance=30, available_balance=30 WHERE execution_account_id='wallet-6'`); err != nil {
		t.Fatal(err)
	}
	harness.advance(5 * time.Minute)
	result := harness.reconcile("wallet-6", "30", 1200)
	if result.Run.Status != domain.ReconciliationRunCompleted || result.Run.Summary["verified_balance:"] != 1 ||
		result.Run.Summary["chain_cash_transfers_reattributed"] != 1 {
		t.Fatalf("run = %#v issues = %#v", result.Run, result.Issues)
	}
	if harness.count(`SELECT count(*) FROM execution_chain_cash_transfers WHERE classification='REDEMPTION' AND account_event_id IS NULL`) != 1 ||
		len(harness.openIssues("wallet-6", domain.ReconciliationIssueUnattributedCashIn)) != 0 {
		t.Fatal("redemption mint was not re-attributed")
	}
}

func TestChainCashLegacyAccountWithoutCursorIsUnchangedPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "main", chainCashLegacyWallet, "100", "100", "0")
	harness.logs.add(chainCashLegacyWallet, chainCashTransfer(1010, 0, domain.ChainCashIn, chainCashRewardSender, "1"))
	result := harness.reconcile("main", "101", 1100)
	drifts := harness.openIssues("main", domain.ReconciliationIssueBalanceDrift)
	if len(drifts) != 1 || drifts[0].Resolution != domain.ReconciliationResolutionManual || !drifts[0].LocalValue.Equal("100") ||
		!drifts[0].RemoteValue.Equal("101") || !result.Impact.AccountWide {
		t.Fatalf("legacy drift = %#v", drifts)
	}
	if harness.logs.callCount() != 0 || harness.count(`SELECT count(*) FROM execution_chain_cash_transfers`) != 0 ||
		result.Run.Summary["verified_source:EVM_ERC20_TRANSFER_LOGS"] != 0 {
		t.Fatal("legacy account used the chain cash ledger")
	}
	assertAccount(t, harness.db, "main", "100", "100", "0")
	// A verified legacy run does not use the chain cash resolver.
	harness.advance(5 * time.Minute)
	harness.reconcile("main", "100", 1200)
	if len(harness.openIssues("main", domain.ReconciliationIssueBalanceDrift)) != 1 {
		t.Fatal("legacy balance drift was closed without attributable evidence")
	}
}

func TestChainCashInitializationRejectsUnsafeBaselinesPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	for name, test := range map[string]struct {
		wallet  string
		balance domain.Decimal
		want    string
	}{
		"ledger differs from balanceOf": {wallet: chainCashWallet6, balance: "10.5", want: "differs from balanceOf"},
		"wallet mismatch":               {wallet: chainCashWallet7, balance: "10", want: "does not match"},
	} {
		t.Run(name, func(t *testing.T) {
			report, err := harness.initialize("wallet-6", test.wallet, 1000, test.balance, 1000, true)
			if err == nil || !strings.Contains(err.Error(), test.want) || report.Executed {
				t.Fatalf("report=%#v err=%v", report, err)
			}
		})
	}
	// Cash booked after the start block breaks the baseline.
	if _, err := harness.db.Exec(`INSERT INTO execution_account_events (
		account_event_id, execution_account_id, event_type, total_balance_delta, available_balance_delta,
		reserved_balance_delta, total_balance_after, available_balance_after, reserved_balance_after, occurred_at
	) VALUES ('late-fill','wallet-6','FILL_SETTLED',1,1,0,10,10,0,$1)`, harness.now); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.initialize("wallet-6", chainCashWallet6, 1000, "10", 1000, true); err == nil ||
		!strings.Contains(err.Error(), "changed at or after the start block") {
		t.Fatalf("late ledger event accepted: %v", err)
	}
	if harness.count(`SELECT count(*) FROM execution_chain_cash_cursors`) != 0 {
		t.Fatal("rejected initialization persisted a cursor")
	}
}

func TestChainCashSchemaGuardsPostgresIntegration(t *testing.T) {
	harness := newChainCashHarness(t)
	insertAccount(t, harness.db, "wallet-6", chainCashWallet6, "10", "10", "0")
	harness.mustInitialize("wallet-6", chainCashWallet6, 1000, "10")
	harness.logs.add(chainCashWallet6, chainCashTransfer(1010, 0, domain.ChainCashIn, chainCashRewardSender, "1"))
	harness.logs.add(chainCashWallet6, chainCashTransfer(1011, 0, domain.ChainCashIn, chainCashStranger, "2"))
	harness.reconcile("wallet-6", "13", 1100)

	for name, statement := range map[string]string{
		"transfer amount":        `UPDATE execution_chain_cash_transfers SET amount=amount+1 WHERE classification='REWARD'`,
		"transfer delete":        `DELETE FROM execution_chain_cash_transfers WHERE classification='REWARD'`,
		"credit relink":          `UPDATE execution_chain_cash_transfers SET account_event_id=NULL WHERE classification='REWARD'`,
		"reclassify":             `UPDATE execution_chain_cash_transfers SET classification='REWARD' WHERE classification='UNATTRIBUTED_IN'`,
		"cursor moves backwards": `UPDATE execution_chain_cash_cursors SET processed_block=processed_block-1`,
		"cursor start changes":   `UPDATE execution_chain_cash_cursors SET start_balance=11`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := harness.db.Exec(statement); err == nil {
				t.Fatalf("%s was accepted", name)
			}
		})
	}
	// A credit must point at an account event that matches it exactly.
	if _, err := harness.db.Exec(`INSERT INTO execution_account_events (
		account_event_id, execution_account_id, event_type, total_balance_delta, available_balance_delta,
		reserved_balance_delta, total_balance_after, available_balance_after, reserved_balance_after, occurred_at
	) VALUES ('mismatched-credit','wallet-6','CHAIN_CASH_REWARD',1,1,0,11,11,0,now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.db.Exec(`INSERT INTO execution_chain_cash_transfers (
		execution_account_id, transaction_hash, log_index, block_number, block_hash, direction, counterparty,
		amount, classification, account_event_id, observed_at
	) VALUES ('wallet-6', '0x' || repeat('e', 64), 0, 1012, '0x' || repeat('e', 64), 'IN', $1,
		5, 'REWARD', 'mismatched-credit', now())`, chainCashRewardSender); err == nil ||
		!strings.Contains(err.Error(), "does not match the transfer exactly") {
		t.Fatalf("mismatched credit error = %v", err)
	}
	var first, after time.Time
	if err := harness.db.QueryRow(`SELECT first_observed_at FROM reconciliation_issues WHERE issue_type='UNATTRIBUTED_CASH_IN'`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.db.Exec(`UPDATE reconciliation_issues SET first_observed_at=first_observed_at - interval '1 day'`); err != nil {
		t.Fatal(err)
	}
	if err := harness.db.QueryRow(`SELECT first_observed_at FROM reconciliation_issues WHERE issue_type='UNATTRIBUTED_CASH_IN'`).Scan(&after); err != nil || !after.Equal(first) {
		t.Fatalf("first_observed_at changed from %s to %s (%v)", first, after, err)
	}
}

func TestHealthCheckerRequiresChainCashSchemaPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "cursor table", sql: `DROP TABLE execution_chain_cash_cursors CASCADE`},
		{name: "first observed column", sql: `ALTER TABLE reconciliation_issues DROP COLUMN first_observed_at CASCADE`},
		{name: "credit constraint", sql: `ALTER TABLE execution_chain_cash_transfers DROP CONSTRAINT execution_chain_cash_transfers_credit_shape`},
		{name: "append-only trigger", sql: `DROP TRIGGER execution_chain_cash_transfers_guard_trigger ON execution_chain_cash_transfers`},
		{name: "disabled cursor trigger", sql: `ALTER TABLE execution_chain_cash_cursors DISABLE TRIGGER execution_chain_cash_cursors_guard_trigger`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := newIntegrationDatabase(t, databaseURL)
			checker, err := NewHealthChecker(db)
			if err != nil {
				t.Fatal(err)
			}
			if err := checker.Check(context.Background()); err != nil {
				t.Fatalf("complete schema: %v", err)
			}
			if _, err := db.Exec(test.sql); err != nil {
				t.Fatal(err)
			}
			if err := checker.Check(context.Background()); err == nil {
				t.Fatalf("readiness passed without %s", test.name)
			}
		})
	}
}

// TestHealthCheckerRequiresChainCashPrivilegesPostgresIntegration runs the
// readiness check as a non-owner application role: migration 0033 is applied
// by an administrative role, so a missing grant must fail startup.
func TestHealthCheckerRequiresChainCashPrivilegesPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	var schema string
	var canCreateRole bool
	if err := db.QueryRow(`SELECT current_schema(), rolsuper OR rolcreaterole FROM pg_roles WHERE rolname=current_user`).Scan(&schema, &canCreateRole); err != nil {
		t.Fatal(err)
	}
	if !canCreateRole {
		t.Skip("the test database user cannot create an application role")
	}
	role := fmt.Sprintf("chain_cash_app_%d", time.Now().UnixNano())
	for _, statement := range []string{
		`CREATE ROLE ` + role + ` NOLOGIN`,
		`GRANT USAGE ON SCHEMA ` + schema + ` TO ` + role,
		`GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ` + schema + ` TO ` + role,
		`REVOKE INSERT ON execution_chain_cash_transfers FROM ` + role,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP OWNED BY ` + role)
		_, _ = db.Exec(`DROP ROLE ` + role)
	})
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	config.RuntimeParams["role"] = role
	appDB := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = appDB.Close() })
	checker, err := NewHealthChecker(appDB)
	if err != nil {
		t.Fatal(err)
	}
	check := func() error { return checker.Check(context.Background()) }
	if err := check(); err == nil || !strings.Contains(err.Error(), "chain cash table privileges") {
		t.Fatalf("role without INSERT on chain cash transfers readiness error = %v", err)
	}
	if _, err := db.Exec(`GRANT INSERT ON execution_chain_cash_transfers TO ` + role); err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatalf("fully granted role failed the privilege check: %v", err)
	}
}

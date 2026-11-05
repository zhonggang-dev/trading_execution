package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshisettlement"
)

// kalshiLedgerSyncHarness drives SyncKalshiLedger the way the sweep does:
// snapshot first, then the exchange observation.
type kalshiLedgerSyncHarness struct {
	t        *testing.T
	database *sql.DB
	store    *KalshiSettlementStore
	account  string
	clock    time.Time
}

func newKalshiLedgerSyncHarness(t *testing.T, account, balance string) *kalshiLedgerSyncHarness {
	t.Helper()
	database := kalshiSettlementDatabase(t)
	seedKalshiSettlementAccount(t, database, account, balance)
	store, err := NewKalshiSettlementStore(database, []string{account})
	if err != nil {
		t.Fatal(err)
	}
	return &kalshiLedgerSyncHarness{t: t, database: database, store: store, account: account,
		clock: time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)}
}

// observation is one sweep's exchange view with positions read and checked.
func (harness *kalshiLedgerSyncHarness) observation(exchange string, positions ...domain.KalshiMarketPosition) domain.KalshiLedgerObservation {
	harness.t.Helper()
	snapshot, err := harness.store.LedgerSnapshot(context.Background(), harness.account)
	if err != nil {
		harness.t.Fatal(err)
	}
	harness.clock = harness.clock.Add(5 * time.Minute)
	if positions == nil {
		positions = []domain.KalshiMarketPosition{}
	}
	return domain.KalshiLedgerObservation{
		AccountVersionBefore: snapshot.AccountVersion, ExchangeBalance: domain.Decimal(exchange),
		PositionCheckEnabled: true, Positions: positions, Threshold: "0.01", ObservedAt: harness.clock,
	}
}

func (harness *kalshiLedgerSyncHarness) sync(observation domain.KalshiLedgerObservation) domain.KalshiLedgerSyncResult {
	harness.t.Helper()
	result, err := harness.store.SyncKalshiLedger(context.Background(), harness.account, observation)
	if err != nil {
		harness.t.Fatal(err)
	}
	return result
}

func (harness *kalshiLedgerSyncHarness) syncs() string {
	return scalar(harness.t, harness.database, `SELECT COUNT(*)::text FROM execution_account_events
		WHERE execution_account_id=$1 AND event_type='KALSHI_BALANCE_SYNC'`, harness.account)
}

func (harness *kalshiLedgerSyncHarness) check(column string) string {
	return scalar(harness.t, harness.database, `SELECT COALESCE(`+column+`::text,'') FROM kalshi_ledger_checks
		WHERE execution_account_id=$1`, harness.account)
}

func (harness *kalshiLedgerSyncHarness) driftStatus() string {
	return scalar(harness.t, harness.database, `SELECT COALESCE((SELECT status FROM kalshi_balance_drift
		WHERE execution_account_id=$1),'NONE')`, harness.account)
}

func kalshiPosition(ticker, position string) domain.KalshiMarketPosition {
	return domain.KalshiMarketPosition{Ticker: ticker, PositionFP: domain.Decimal(position),
		Raw: []byte(fmt.Sprintf(`{"ticker":%q,"position_fp":%q}`, ticker, position))}
}

func newKalshiSyncService(t *testing.T, database *sql.DB, accountID string, exchange *fakeKalshiExchange) *kalshisettlement.Service {
	t.Helper()
	store, err := NewKalshiSettlementStore(database, []string{accountID})
	if err != nil {
		t.Fatal(err)
	}
	service, err := kalshisettlement.New(kalshisettlement.Params{
		ExecutionAccountID: accountID, Source: exchange, Store: store,
		Interval: time.Minute, BalanceDriftThreshold: "0.01",
		BalanceSyncEnabled: true, PositionCheckEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

// A deposit made while the process runs reaches the ledger on the second
// identical sweep, without a restart, through the running service.
func TestKalshiRunningSyncBooksADepositWithoutRestartPostgresIntegration(t *testing.T) {
	ctx := context.Background()
	const account = "kalshi:sync-deposit"
	database := kalshiSettlementDatabase(t)
	seedKalshiSettlementAccount(t, database, account, "100")
	seedKalshiHolding(t, database, account, "KXDEP-1", "YES", "10", "4")
	exchange := &fakeKalshiExchange{balance: "1010", positions: []domain.KalshiMarketPosition{kalshiPosition("KXDEP-1", "10.00")}}
	service := newKalshiSyncService(t, database, account, exchange)

	if err := service.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	assertKalshiCash(t, database, account, "100")
	if got := scalar(t, database, `SELECT balance_outcome||'/'||status FROM kalshi_ledger_checks, kalshi_balance_drift
		WHERE kalshi_ledger_checks.execution_account_id=$1 AND kalshi_balance_drift.execution_account_id=$1`, account); got != "PENDING_CONFIRMATION/OPEN" {
		t.Fatalf("first sweep = %s, want PENDING_CONFIRMATION with an OPEN drift", got)
	}
	if err := service.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	assertKalshiCash(t, database, account, "1010")
	var eventID, delta, after, reservedAfter string
	if err := database.QueryRow(`SELECT account_event_id, total_balance_delta::text, available_balance_after::text, reserved_balance_after::text
		FROM execution_account_events WHERE execution_account_id=$1 AND event_type='KALSHI_BALANCE_SYNC'`, account).
		Scan(&eventID, &delta, &after, &reservedAfter); err != nil {
		t.Fatal(err)
	}
	if !sameNumeric(delta, "910") || !sameNumeric(after, "1010") || !sameNumeric(reservedAfter, "0") ||
		!strings.HasPrefix(eventID, "kalshi-balance-sync:"+account+":") {
		t.Fatalf("sync event = %s %s %s %s", eventID, delta, after, reservedAfter)
	}
	if got := scalar(t, database, `SELECT local_balance_before::text||'/'||exchange_balance::text||'/'||difference::text||'/'||observations
		FROM kalshi_balance_syncs WHERE account_event_id=$1`, eventID); got != "100/1010/910/2" {
		t.Fatalf("sync audit = %s", got)
	}
	if got := scalar(t, database, `SELECT status FROM kalshi_balance_drift WHERE execution_account_id=$1`, account); got != "RESOLVED" {
		t.Fatalf("drift = %s, want RESOLVED after the sync", got)
	}
	if got := scalar(t, database, `SELECT balance_outcome||'/'||last_sync_event_id||'/'||positions_outcome FROM kalshi_ledger_checks
		WHERE execution_account_id=$1`, account); got != "SYNCED/"+eventID+"/MATCHED" {
		t.Fatalf("ledger check = %s", got)
	}
	if err := service.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got := scalar(t, database, `SELECT balance_outcome FROM kalshi_ledger_checks WHERE execution_account_id=$1`, account); got != "IN_SYNC" {
		t.Fatalf("third sweep = %s, want IN_SYNC", got)
	}
	if exchange.positionRead != 3 {
		t.Fatalf("position reads = %d, want one per quiet sweep", exchange.positionRead)
	}
}

func TestKalshiRunningSyncCorrectsAWithdrawalPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-withdraw", "1010")
	if result := harness.sync(harness.observation("10")); result.BalanceOutcome != domain.KalshiBalancePendingConfirmation {
		t.Fatalf("first = %#v", result)
	}
	result := harness.sync(harness.observation("10"))
	if result.BalanceOutcome != domain.KalshiBalanceSynced || result.SyncEventID == "" || !result.Difference.Equal("-1000") {
		t.Fatalf("second = %#v, want SYNCED -1000", result)
	}
	assertKalshiCash(t, harness.database, harness.account, "10")
	if got := scalar(t, harness.database, `SELECT total_balance_delta::text FROM execution_account_events
		WHERE account_event_id=$1`, result.SyncEventID); !sameNumeric(got, "-1000") {
		t.Fatalf("event delta = %s", got)
	}
}

// Two differing observations never book: the difference must repeat exactly.
func TestKalshiRunningSyncNeedsTwoIdenticalObservationsPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-twice", "100")
	for _, exchange := range []string{"150", "151", "150"} {
		if result := harness.sync(harness.observation(exchange)); result.BalanceOutcome != domain.KalshiBalancePendingConfirmation {
			t.Fatalf("%s: %#v, want PENDING_CONFIRMATION", exchange, result)
		}
	}
	assertKalshiCash(t, harness.database, harness.account, "100")
	if result := harness.sync(harness.observation("150")); result.BalanceOutcome != domain.KalshiBalanceSynced {
		t.Fatalf("repeat = %#v, want SYNCED", result)
	}
	assertKalshiCash(t, harness.database, harness.account, "150")
	// A difference within the threshold is in sync and books nothing.
	if result := harness.sync(harness.observation("150.005")); result.BalanceOutcome != domain.KalshiBalanceInSync {
		t.Fatalf("within threshold = %#v", result)
	}
	if harness.syncs() != "1" {
		t.Fatalf("sync events = %s, want 1", harness.syncs())
	}
}

func TestKalshiRunningSyncWaitsForAQuietWindowPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-busy", "100")
	ctx := context.Background()
	reservations, err := NewReservationManager(ReservationManagerParams{DB: harness.database, MaxBuyFeeRateBPS: "2000"})
	if err != nil {
		t.Fatal(err)
	}
	order := integrationOrder("sync-busy-buy", harness.account, "kalshi:KXBUSY-1:YES", domain.SideBuy, "10", "0.5")
	if _, err := reservations.Reserve(ctx, order); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		result := harness.sync(harness.observation("200"))
		if result.BalanceOutcome != domain.KalshiBalanceSkipped || result.SkipReason != domain.KalshiSyncSkipReservationActive ||
			result.ConsecutiveSkips != int64(attempt) || result.TotalSkips != int64(attempt) {
			t.Fatalf("attempt %d = %#v, want RESERVATION_ACTIVE", attempt, result)
		}
	}
	if harness.driftStatus() != "NONE" || harness.check("positions_outcome") != "SKIPPED" {
		t.Fatalf("drift %s positions %s, want nothing compared while busy", harness.driftStatus(), harness.check("positions_outcome"))
	}
	if total, _, reserved := accountBalances(t, harness.database, harness.account); !sameNumeric(total, "100") || sameNumeric(reserved, "0") {
		t.Fatalf("ledger moved while reserved: total %s reserved %s", total, reserved)
	}
	observedAt := time.Now().UTC()
	order.Status = domain.OrderStatusCanceled
	order.VenueLastObservedAt = &observedAt
	if _, err := reservations.Reconcile(ctx, order); err != nil {
		t.Fatal(err)
	}
	if result := harness.sync(harness.observation("200")); result.BalanceOutcome != domain.KalshiBalancePendingConfirmation ||
		result.ConsecutiveSkips != 0 || result.TotalSkips != 2 {
		t.Fatalf("after release = %#v", result)
	}
	if harness.check("balance_skipping_since") != "" {
		t.Fatal("a non-skipped check must clear skipping_since")
	}
	if result := harness.sync(harness.observation("200")); result.BalanceOutcome != domain.KalshiBalanceSynced {
		t.Fatalf("quiet window = %#v, want SYNCED", result)
	}
	assertKalshiCash(t, harness.database, harness.account, "200")

	// A non-terminal order skips too, and a pending observation does not
	// survive a skip in between.
	if result := harness.sync(harness.observation("300")); result.BalanceOutcome != domain.KalshiBalancePendingConfirmation {
		t.Fatalf("pending = %#v", result)
	}
	seedKalshiHolding(t, harness.database, harness.account, "KXBUSY-2", "YES", "1", "0.5")
	if _, err := harness.database.Exec(`UPDATE execution_orders SET status='LIVE' WHERE execution_account_id=$1`, harness.account); err != nil {
		t.Fatal(err)
	}
	if result := harness.sync(harness.observation("300", kalshiPosition("KXBUSY-2", "1"))); result.SkipReason != domain.KalshiSyncSkipOrderInFlight {
		t.Fatalf("live order = %#v, want ORDER_IN_FLIGHT", result)
	}
	if _, err := harness.database.Exec(`UPDATE execution_orders SET status='FILLED' WHERE execution_account_id=$1`, harness.account); err != nil {
		t.Fatal(err)
	}
	if result := harness.sync(harness.observation("300", kalshiPosition("KXBUSY-2", "1"))); result.BalanceOutcome != domain.KalshiBalancePendingConfirmation {
		t.Fatalf("after the skip = %#v, want a fresh PENDING_CONFIRMATION", result)
	}
}

func TestKalshiRunningSyncRejectsAStaleSnapshotPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-stale", "100")
	harness.sync(harness.observation("150"))
	observation := harness.observation("150")
	// A fill lands between the balance read and the locked check.
	if _, err := harness.database.Exec(`UPDATE execution_accounts SET version=version+1 WHERE execution_account_id=$1`, harness.account); err != nil {
		t.Fatal(err)
	}
	if result := harness.sync(observation); result.SkipReason != domain.KalshiSyncSkipLedgerChanged {
		t.Fatalf("stale snapshot = %#v, want LEDGER_CHANGED", result)
	}
	assertKalshiCash(t, harness.database, harness.account, "100")
}

func TestKalshiRunningSyncNeverBooksASettlementPayoutPostgresIntegration(t *testing.T) {
	ctx := context.Background()
	t.Run("payout reaches the exchange balance before the ledger", func(t *testing.T) {
		const account = "kalshi:sync-payout"
		database, exchange := settledWhileDown(t, account)
		exchange.positions = []domain.KalshiMarketPosition{}
		service := newKalshiSyncService(t, database, account, exchange)
		for sweep := 1; sweep <= 3; sweep++ {
			if err := service.Sweep(ctx); err != nil {
				t.Fatal(err)
			}
			assertKalshiCash(t, database, account, "110")
		}
		if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_account_events
			WHERE execution_account_id=$1 AND event_type='KALSHI_BALANCE_SYNC'`, account); got != "0" {
			t.Fatalf("balance syncs = %s, the payout must be booked by settlement only", got)
		}
	})
	t.Run("settlement held for review", func(t *testing.T) {
		harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-held", "100")
		seedKalshiHolding(t, harness.database, harness.account, "KXHELD-1", "YES", "10", "4")
		exchange := &fakeKalshiExchange{balance: "109.5", settlements: map[string][]domain.KalshiSettlement{
			"KXHELD-1": {kalshiRecord("KXHELD-1", "yes", "10", "0", "9.5", "1")}}}
		if err := newKalshiSettlementService(t, harness.database, harness.account, exchange).Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			if result := harness.sync(harness.observation("109.5")); result.SkipReason != domain.KalshiSyncSkipSettlementPending {
				t.Fatalf("held settlement = %#v, want SETTLEMENT_PENDING", result)
			}
		}
		assertKalshiCash(t, harness.database, harness.account, "100")
	})
	t.Run("sweep saw a settlement record", func(t *testing.T) {
		harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-activity", "100")
		harness.sync(harness.observation("150"))
		observation := harness.observation("150")
		observation.SettlementActivity = true
		if result := harness.sync(observation); result.SkipReason != domain.KalshiSyncSkipSettlementPending {
			t.Fatalf("activity = %#v", result)
		}
		observation = harness.observation("150")
		observation.SettlementReadFailed = true
		if result := harness.sync(observation); result.SkipReason != domain.KalshiSyncSkipSettlementPending {
			t.Fatalf("read failure = %#v", result)
		}
		assertKalshiCash(t, harness.database, harness.account, "100")
	})
}

func TestKalshiRunningSyncWaitsForDeferredNettingPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-netting", "100")
	seedKalshiHolding(t, harness.database, harness.account, "KXNETP-1", "YES", "5", "2")
	seedKalshiHolding(t, harness.database, harness.account, "KXNETP-1", "NO", "2", "1")
	// The exchange already netted two pairs and paid 2 dollars for them.
	for attempt := 0; attempt < 2; attempt++ {
		result := harness.sync(harness.observation("102", kalshiPosition("KXNETP-1", "3")))
		if result.SkipReason != domain.KalshiSyncSkipNettingPending || !result.Drifted {
			t.Fatalf("both sides = %#v, want NETTING_PENDING with the drift recorded", result)
		}
	}
	assertKalshiCash(t, harness.database, harness.account, "100")
	if got := scalar(t, harness.database, `SELECT drift_kind||'/'||status||'/'||observations FROM kalshi_position_drift
		WHERE execution_account_id=$1 AND ticker='KXNETP-1'`, harness.account); got != "LOCAL_BOTH_SIDES/OPEN/2" {
		t.Fatalf("position drift = %s", got)
	}
	if harness.driftStatus() != "OPEN" {
		t.Fatalf("balance drift = %s, want OPEN", harness.driftStatus())
	}
}

func TestKalshiPositionCheckRecordsDifferencesWithoutTouchingTheLedgerPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-positions", "100")
	seedKalshiHolding(t, harness.database, harness.account, "KXPOS-1", "YES", "10", "4")
	seedKalshiHolding(t, harness.database, harness.account, "KXPOS-2", "NO", "5", "2")
	before := ledgerState(t, harness.database)
	// The exchange holds 8 of KXPOS-1, nothing of KXPOS-2, and 2 of an unknown market.
	wrong := []domain.KalshiMarketPosition{kalshiPosition("KXPOS-1", "8"), kalshiPosition("KXPOS-3", "2")}
	for attempt := 1; attempt <= 2; attempt++ {
		result := harness.sync(harness.observation("120", wrong...))
		if result.SkipReason != domain.KalshiSyncSkipPositionMismatch || result.PositionsOutcome != domain.KalshiPositionsDrifted ||
			result.OpenPositionDrifts != 3 {
			t.Fatalf("attempt %d = %#v, want POSITION_MISMATCH with 3 drifts", attempt, result)
		}
	}
	if after := ledgerState(t, harness.database); after != before {
		t.Fatal("the position check changed the ledger")
	}
	rows, err := harness.database.Query(`SELECT ticker, drift_kind, local_yes_shares::text, local_no_shares::text,
		exchange_position::text, observations, exchange_record IS NOT NULL
		FROM kalshi_position_drift WHERE execution_account_id=$1 AND status='OPEN' ORDER BY ticker`, harness.account)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var ticker, kind, yes, no, position string
		var observations int
		var record bool
		if err := rows.Scan(&ticker, &kind, &yes, &no, &position, &observations, &record); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s %s %s/%s %s x%d %v", ticker, kind, yes, no, position, observations, record))
	}
	_ = rows.Close()
	want := []string{
		"KXPOS-1 NET_MISMATCH 10/0 8 x2 true",
		"KXPOS-2 NET_MISMATCH 0/5 0 x2 false",
		"KXPOS-3 NET_MISMATCH 0/0 2 x2 true",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("position drifts = %v, want %v", got, want)
	}
	if harness.driftStatus() != "OPEN" {
		t.Fatalf("balance drift = %s, want OPEN while the cash stays unsynced", harness.driftStatus())
	}

	// Positions unread or unreadable: still never synced.
	unread := harness.observation("120")
	unread.Positions = nil
	if result := harness.sync(unread); result.SkipReason != domain.KalshiSyncSkipPositionsUnverified ||
		result.PositionsOutcome != domain.KalshiPositionsSkipped {
		t.Fatalf("unread = %#v", result)
	}
	unreadable := harness.observation("120")
	unreadable.Positions, unreadable.PositionsError = nil, "KALSHI_RATE_LIMITED"
	if result := harness.sync(unreadable); result.SkipReason != domain.KalshiSyncSkipPositionsUnverified ||
		result.PositionsOutcome != domain.KalshiPositionsError {
		t.Fatalf("unreadable = %#v", result)
	}

	// Once the exchange agrees, the drifts resolve and cash syncs normally.
	right := []domain.KalshiMarketPosition{kalshiPosition("KXPOS-1", "10"), kalshiPosition("KXPOS-2", "-5")}
	if result := harness.sync(harness.observation("120", right...)); result.BalanceOutcome != domain.KalshiBalancePendingConfirmation ||
		result.PositionsOutcome != domain.KalshiPositionsMatched || result.OpenPositionDrifts != 0 || result.PreviousOpenPositionDrifts != 3 {
		t.Fatalf("matching = %#v", result)
	}
	if got := scalar(t, harness.database, `SELECT COUNT(*)::text FROM kalshi_position_drift
		WHERE execution_account_id=$1 AND status='RESOLVED' AND resolved_at IS NOT NULL`, harness.account); got != "3" {
		t.Fatalf("resolved drifts = %s, want 3", got)
	}
	if result := harness.sync(harness.observation("120", right...)); result.BalanceOutcome != domain.KalshiBalanceSynced {
		t.Fatalf("second matching = %#v, want SYNCED", result)
	}
	assertKalshiCash(t, harness.database, harness.account, "120")
}

// With the position check off, cash still syncs (positions are not required).
func TestKalshiRunningSyncWithoutPositionCheckPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-nopos", "100")
	seedKalshiHolding(t, harness.database, harness.account, "KXNOPOS-1", "YES", "10", "4")
	for range 2 {
		observation := harness.observation("130")
		observation.PositionCheckEnabled, observation.Positions = false, nil
		harness.sync(observation)
	}
	assertKalshiCash(t, harness.database, harness.account, "130")
	if harness.check("positions_outcome") != "NEVER" {
		t.Fatalf("positions outcome = %s, want untouched", harness.check("positions_outcome"))
	}
}

// Reserve and release race the sync on the same account row: the sync only
// ever books with nothing reserved, and the cash identity always holds.
func TestKalshiRunningSyncRacesReservationsSafelyPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-race", "100")
	ctx := context.Background()
	reservations, err := NewReservationManager(ReservationManagerParams{DB: harness.database, MaxBuyFeeRateBPS: "2000"})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(1)
	reserveErr := make(chan error, 1)
	go func() {
		defer wait.Done()
		for index := 0; ; index++ {
			select {
			case <-stop:
				return
			default:
			}
			order := integrationOrder(fmt.Sprintf("race-%d", index), harness.account, "kalshi:KXRACE-1:YES", domain.SideBuy, "10", "0.5")
			if _, err := reservations.Reserve(ctx, order); err != nil {
				reserveErr <- err
				return
			}
			time.Sleep(2 * time.Millisecond)
			observedAt := time.Now().UTC()
			order.Status = domain.OrderStatusCanceled
			order.VenueLastObservedAt = &observedAt
			if _, err := reservations.Reconcile(ctx, order); err != nil {
				reserveErr <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	synced := false
	for attempt := 0; attempt < 400 && !synced; attempt++ {
		observation := harness.observation("500")
		observation.PositionCheckEnabled = false
		result, err := harness.store.SyncKalshiLedger(ctx, harness.account, observation)
		if err != nil {
			t.Fatal(err)
		}
		synced = result.BalanceOutcome == domain.KalshiBalanceSynced
	}
	close(stop)
	wait.Wait()
	select {
	case err := <-reserveErr:
		t.Fatal(err)
	default:
	}
	for attempt := 0; attempt < 2 && !synced; attempt++ {
		observation := harness.observation("500")
		observation.PositionCheckEnabled = false
		synced = harness.sync(observation).BalanceOutcome == domain.KalshiBalanceSynced
	}
	if !synced {
		t.Fatal("the sync never found a quiet window")
	}
	assertKalshiCash(t, harness.database, harness.account, "500")
	if got := scalar(t, harness.database, `SELECT COUNT(*)::text FROM execution_account_events
		WHERE execution_account_id=$1 AND event_type='KALSHI_BALANCE_SYNC' AND reserved_balance_after <> 0`, harness.account); got != "0" {
		t.Fatalf("%s syncs booked while cash was reserved", got)
	}
	if harness.syncs() != "1" {
		t.Fatalf("sync events = %s, want exactly one", harness.syncs())
	}
}

func TestKalshiRunningSyncScopeAndAuditPostgresIntegration(t *testing.T) {
	harness := newKalshiLedgerSyncHarness(t, "kalshi:sync-scope", "100")
	ctx := context.Background()
	insertAccount(t, harness.database, "main", "0xmain", "100", "100", "0")
	if _, err := harness.store.SyncKalshiLedger(ctx, "main", harness.observation("150")); err == nil {
		t.Fatal("a Polymarket account must be outside the sync scope")
	}
	if _, err := harness.store.LedgerSnapshot(ctx, "kalshi:wallet-7"); err == nil {
		t.Fatal("an unconfigured Kalshi account must be outside the sync scope")
	}
	negative := harness.observation("-1")
	if _, err := harness.store.SyncKalshiLedger(ctx, harness.account, negative); err == nil {
		t.Fatal("a negative exchange balance must be rejected")
	}
	harness.sync(harness.observation("150"))
	harness.sync(harness.observation("150"))
	for _, statement := range []string{
		`UPDATE kalshi_balance_syncs SET observations=3`,
		`DELETE FROM kalshi_balance_syncs`,
		`TRUNCATE kalshi_balance_syncs`,
	} {
		if _, err := harness.database.Exec(statement); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("%s: err = %v, want append-only rejection", statement, err)
		}
	}
	if harness.syncs() != "1" {
		t.Fatal("audit protection must not lose the sync")
	}
}

// A database without migration 0045 is detected, and the drift-only service
// keeps working against it.
func TestKalshiLedgerSyncTablesMissingFallsBackPostgresIntegration(t *testing.T) {
	ctx := context.Background()
	const account = "kalshi:sync-legacy"
	database := kalshiSettlementDatabase(t)
	if present, err := KalshiLedgerSyncTablesPresent(ctx, database); err != nil || !present {
		t.Fatalf("present = %v, %v, want the migrated tables found", present, err)
	}
	for _, statement := range []string{
		`DROP TABLE kalshi_settlement_reviews`, `DROP TABLE kalshi_balance_syncs`,
		`DROP TABLE kalshi_position_drift`, `DROP TABLE kalshi_ledger_checks`,
	} {
		if _, err := database.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if present, err := KalshiLedgerSyncTablesPresent(ctx, database); err != nil || present {
		t.Fatalf("present = %v, %v, want missing", present, err)
	}
	seedKalshiSettlementAccount(t, database, account, "100")
	exchange := &fakeKalshiExchange{balance: "150"}
	if err := newKalshiSettlementService(t, database, account, exchange).Sweep(ctx); err != nil {
		t.Fatalf("drift-only sweep without 0045 = %v", err)
	}
	if got := scalar(t, database, `SELECT status FROM kalshi_balance_drift WHERE execution_account_id=$1`, account); got != "OPEN" {
		t.Fatalf("drift = %s, want the legacy drift row", got)
	}
	assertKalshiCash(t, database, account, "100")
}

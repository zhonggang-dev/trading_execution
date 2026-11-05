package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/nofillrelease"
)

// newNoFillDatabase applies migration 0046 on top of the shared fixture.
func newNoFillDatabase(t *testing.T, databaseURL string) *sql.DB {
	t.Helper()
	db := newIntegrationDatabase(t, databaseURL)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0046_order_no_fill_resolution.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("apply 0046: %v", err)
	}
	return db
}

func noFillDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	return databaseURL
}

type noFillClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *noFillClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *noFillClock) Set(value time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = value
}

type noFillPlacement struct {
	hash      string
	expiresAt time.Time
}

func (placement noFillPlacement) ExpectedVenueOrderID() string { return placement.hash }
func (placement noFillPlacement) VenueExpiresAt() (time.Time, bool) {
	return placement.expiresAt, !placement.expiresAt.IsZero()
}

// noFillVenue is a Polymarket CLOB whose POST answers 5xx and whose order
// read afterwards answers HTTP 200 null: the 10-26 / 10-29 incident shape.
type noFillVenue struct {
	clock    *noFillClock
	lifetime time.Duration // zero: GTC without venue expiry
	hashes   map[string]string
}

func (*noFillVenue) Name() string { return "polymarket" }

func (venue *noFillVenue) PreparePlace(_ context.Context, order domain.Order) (port.PreparedPlacement, error) {
	digest := sha256.Sum256([]byte(order.ID))
	hash := "0x" + hex.EncodeToString(digest[:])
	venue.hashes[order.ID] = hash
	placement := noFillPlacement{hash: hash}
	if venue.lifetime > 0 {
		placement.expiresAt = venue.clock.Now().Add(venue.lifetime).Truncate(time.Second)
	}
	return placement, nil
}

func (venue *noFillVenue) PlacePrepared(_ context.Context, order domain.Order, _ port.PreparedPlacement) (port.VenueOrder, error) {
	return port.VenueOrder{}, &port.VenueError{
		Kind: port.VenueErrorAmbiguous, Code: "CLOB_SERVER_ERROR", HTTPStatus: 500,
		Message: "rpc error: code = DeadlineExceeded", VenueOrderID: order.VenueOrderID,
	}
}

func (*noFillVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, errors.New("legacy Place must not be used")
}

func (*noFillVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, errors.New("cancel must not be called")
}

func (*noFillVenue) Get(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_ORDER_NOT_FOUND",
		VenueOrderID: order.VenueOrderID, Message: "CLOB returned no order"}
}

type noFillGuard struct{}

func (noFillGuard) Check(context.Context, domain.OrderIntent) error { return nil }

type noFillValidator struct{}

func (noFillValidator) Validate(context.Context, domain.OrderIntent) (domain.MarketValidation, error) {
	return domain.MarketValidation{Mode: "TEST"}, nil
}

type noFillScope struct{}

func (noFillScope) IsActive(string) bool  { return true }
func (noFillScope) IsManaged(string) bool { return true }

type noFillSync struct{}

func (noFillSync) SyncOrder(_ context.Context, orderID string) (port.FillSyncResult, error) {
	return port.FillSyncResult{OrderID: orderID}, nil
}

// noFillEvidence fakes the CLOB evidence reads and the chain scan.
type noFillEvidence struct {
	readErr   error
	tradesErr error
	trades    []domain.NoFillTradeReference
	openErr   error
	open      bool
	chainErr  error
	matches   []domain.NoFillChainMatch
}

func (evidence *noFillEvidence) ReadOrder(context.Context, domain.Order) (domain.NoFillOrderRead, error) {
	return domain.NoFillOrderRead{NotFound: true, Code: "CLOB_ORDER_NOT_FOUND"}, evidence.readErr
}

func (evidence *noFillEvidence) TradeReferences(_ context.Context, order domain.Order, after time.Time) (domain.NoFillTradeScan, error) {
	references := evidence.trades
	if references == nil {
		references = []domain.NoFillTradeReference{}
	}
	return domain.NoFillTradeScan{Market: order.Intent.ConditionID, After: after, Scanned: 40, References: references}, evidence.tradesErr
}

func (evidence *noFillEvidence) OpenOrders(_ context.Context, order domain.Order) (domain.NoFillOpenOrderScan, error) {
	return domain.NoFillOpenOrderScan{Market: order.Intent.ConditionID, TokenID: order.Intent.TokenID, Contains: evidence.open}, evidence.openErr
}

func (evidence *noFillEvidence) ScanOrderFilled(_ context.Context, request domain.NoFillChainScanRequest) (domain.NoFillChainEvidence, error) {
	if evidence.chainErr != nil {
		return domain.NoFillChainEvidence{}, evidence.chainErr
	}
	matches := evidence.matches
	if matches == nil {
		matches = []domain.NoFillChainMatch{}
	}
	return domain.NoFillChainEvidence{
		ChainID: 137, Topic1: request.OrderHash, FromBlock: 78_000_000, ToBlock: 78_001_400,
		FromBlockTime: request.FromNotAfter, ToBlockTime: request.ToNotBefore,
		ToBlockHashBefore: "0xaa", ToBlockHashAfter: "0xaa", Matches: matches,
		Controls: []domain.NoFillChainControl{{BlockNumber: 78_001_400, Logs: 15}, {BlockNumber: 78_000_700, Logs: 14}, {BlockNumber: 78_000_000, Logs: 16}},
		RPCHost:  "polygon.example",
	}, nil
}

type noFillHarness struct {
	db         *sql.DB
	clock      *noFillClock
	repository *OrderRepository
	store      *NoFillReleaseStore
	evidence   *noFillEvidence
	resolver   *nofillrelease.Resolver
	service    *execution.Service
	venue      *noFillVenue
}

func newNoFillHarness(t *testing.T, db *sql.DB, mode nofillrelease.Mode, lifetime time.Duration, start time.Time) *noFillHarness {
	t.Helper()
	clock := &noFillClock{now: start}
	repository, err := NewOrderRepository(db, WithOrderAttemptVenueExpiry(), WithSubmitInFlightGrace(90*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0", Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewNoFillReleaseStore(db, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	evidence := &noFillEvidence{}
	resolver, err := nofillrelease.New(nofillrelease.Params{
		Store: store, Venue: evidence, Chain: evidence, Mode: mode,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	venue := &noFillVenue{clock: clock, lifetime: lifetime, hashes: map[string]string{}}
	var unaccepted port.UnacceptedOrderResolver = resolver
	if mode == nofillrelease.ModeOff {
		unaccepted = nil
	}
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue, Guard: noFillGuard{}, MarketValidator: noFillValidator{},
		Reservations: reservations, FillSynchronizer: noFillSync{}, AuthoritativeFills: true,
		RequirePreparedPlacement: true, AccountScope: noFillScope{}, UnacceptedResolver: unaccepted,
		Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &noFillHarness{db: db, clock: clock, repository: repository, store: store, evidence: evidence,
		resolver: resolver, service: service, venue: venue}
}

// submitUnknown places one order whose POST answers 5xx and records the
// order-scoped recovery issues reconciliation would have opened.
func (harness *noFillHarness) submitUnknown(t *testing.T, name, accountID, tokenID string, side domain.Side, size, worstPrice string) domain.Order {
	t.Helper()
	intent := integrationOrder(name, accountID, tokenID, side, size, worstPrice).Intent
	result, err := harness.service.Submit(context.Background(), intent)
	if err == nil || result.Order.Status != domain.OrderStatusUnknown {
		t.Fatalf("Submit(%s) = %s, %v; want UNKNOWN after a 5xx", name, result.Order.Status, err)
	}
	runID := "run-" + name
	if _, err := harness.db.Exec(`INSERT INTO reconciliation_runs (run_id,execution_account_id,trigger,status,started_at,completed_at)
		VALUES ($1,$2,'ORDER_UNKNOWN','ATTENTION_REQUIRED',now(),now())`, runID, accountID); err != nil {
		t.Fatal(err)
	}
	for index, issue := range []struct{ kind, source, resolution string }{
		{"SOURCE_UNAVAILABLE", "CLOB_ORDER", "RETRY_LATER"},
		{"ORDER_RECOVERY_PENDING", "ORDER_RECOVERY", "RETRY_LATER"},
		{"ORDER_RECOVERY_STALLED", "ORDER_RECOVERY", "MANUAL_REVIEW"},
	} {
		if _, err := harness.db.Exec(`INSERT INTO reconciliation_issues
			(issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,order_id,venue_order_id,
			 market_id,condition_id,token_id,source,details,observed_at,impact_scope)
			VALUES ($1,$2,$1,$3,$4,$5,'OPEN',$6,$7,'market-1','condition-1',$8,$9,'fixture',now(),'ORDER')`,
			fmt.Sprintf("issue-%s-%d", name, index), runID, accountID, issue.kind, issue.resolution,
			result.Order.ID, result.Order.VenueOrderID, tokenID, issue.source); err != nil {
			t.Fatal(err)
		}
	}
	return result.Order
}

func (harness *noFillHarness) refreshAt(t *testing.T, orderID string, at time.Time) (domain.Order, error) {
	t.Helper()
	harness.clock.Set(at)
	return harness.service.Refresh(context.Background(), orderID)
}

type noFillState struct {
	status, failureCode string
	revision            int64
	reservation         string
	openIssues          int
	audits              map[string]int
}

func readNoFillState(t *testing.T, db *sql.DB, orderID string) noFillState {
	t.Helper()
	state := noFillState{audits: map[string]int{}}
	if err := db.QueryRow(`SELECT status, failure_code, revision FROM execution_orders WHERE order_id=$1`, orderID).
		Scan(&state.status, &state.failureCode, &state.revision); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM asset_reservations WHERE order_id=$1`, orderID).Scan(&state.reservation); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM reconciliation_issues WHERE order_id=$1 AND status='OPEN'`, orderID).Scan(&state.openIssues); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT mode, count(*) FROM execution_order_no_fill_resolutions WHERE order_id=$1 GROUP BY mode`, orderID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var mode string
		var count int
		if err := rows.Scan(&mode, &count); err != nil {
			t.Fatal(err)
		}
		state.audits[mode] = count
	}
	return state
}

func TestNoFillReleaseShadowOnlyAuditsPostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	start := time.Date(2026, 10, 26, 17, 20, 0, 0, time.UTC)
	insertAccount(t, db, "main", "0xmain", "100", "100", "0")
	harness := newNoFillHarness(t, db, nofillrelease.ModeShadow, 5*time.Minute, start)
	order := harness.submitUnknown(t, "shadow-buy", "main", "token-shadow", domain.SideBuy, "4", "0.266")
	assertAccount(t, db, "main", "100", "98.936", "1.064")

	if _, err := harness.refreshAt(t, order.ID, start.Add(20*time.Minute)); err == nil {
		t.Fatal("Refresh() before the time gate must keep the order UNKNOWN")
	}
	if state := readNoFillState(t, db, order.ID); state.status != "UNKNOWN" || len(state.audits) != 0 {
		t.Fatalf("before deadline state = %#v", state)
	}
	if _, err := harness.refreshAt(t, order.ID, start.Add(36*time.Minute)); err == nil {
		t.Fatal("shadow Refresh() must keep reporting the venue read failure")
	}
	state := readNoFillState(t, db, order.ID)
	if state.status != "UNKNOWN" || state.reservation != "RECONCILIATION_REQUIRED" || state.openIssues != 3 ||
		state.audits["SHADOW"] != 1 || state.audits["AUTO"] != 0 {
		t.Fatalf("shadow state = %#v; want an audit row and nothing else changed", state)
	}
	assertAccount(t, db, "main", "100", "98.936", "1.064")
	var released string
	if err := db.QueryRow(`SELECT released_balance::text FROM execution_order_no_fill_resolutions WHERE order_id=$1`, order.ID).Scan(&released); err != nil || !sameNumeric(released, "0") {
		t.Fatalf("shadow released_balance = %s, %v", released, err)
	}
	// A second evaluation does not write a second shadow row.
	_, _ = harness.refreshAt(t, order.ID, start.Add(50*time.Minute))
	if state := readNoFillState(t, db, order.ID); state.audits["SHADOW"] != 1 {
		t.Fatalf("shadow rows = %d, want 1", state.audits["SHADOW"])
	}
	if _, err := db.Exec(`UPDATE execution_order_no_fill_resolutions SET actor='x'`); err == nil {
		t.Fatal("audit table accepted an UPDATE")
	}
}

func TestNoFillReleaseOnCancelsAndReleasesInOneTransactionPostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	start := time.Date(2026, 10, 26, 17, 20, 0, 0, time.UTC)
	insertAccount(t, db, "main", "0xmain", "100", "100", "0")
	insertAccount(t, db, "wallet-6", "0xwallet6", "0", "0", "0")
	if _, err := db.Exec(`INSERT INTO execution_positions (execution_account_id, market_id, token_id, total_shares, available_shares, reserved_shares)
		VALUES ('wallet-6','market-1','token-sell',12,12,0)`); err != nil {
		t.Fatal(err)
	}
	insertOpenLotFixture(t, db, "wallet-6", "token-sell", "12")
	harness := newNoFillHarness(t, db, nofillrelease.ModeOn, 5*time.Minute, start)
	buy := harness.submitUnknown(t, "on-buy", "main", "token-buy", domain.SideBuy, "4", "0.266")
	sell := harness.submitUnknown(t, "on-sell", "wallet-6", "token-sell", domain.SideSell, "12", "0.5")
	assertPosition(t, db, "wallet-6", "token-sell", "12", "0", "12")
	before := readNoFillState(t, db, buy.ID)

	refreshed, err := harness.refreshAt(t, buy.ID, start.Add(36*time.Minute))
	if err != nil || refreshed.Status != domain.OrderStatusCancelled || refreshed.FailureCode != domain.NoFillReleaseAutoReasonCode {
		t.Fatalf("Refresh(BUY) = %s/%s, %v", refreshed.Status, refreshed.FailureCode, err)
	}
	state := readNoFillState(t, db, buy.ID)
	// +2 for the reconciliation read of this Refresh, +2 for UNKNOWN->RECONCILING->CANCELLED.
	if state.status != "CANCELLED" || state.revision != before.revision+4 || state.reservation != "RELEASED" ||
		state.openIssues != 0 || state.audits["AUTO"] != 1 {
		t.Fatalf("released state = %#v (before %#v)", state, before)
	}
	assertAccount(t, db, "main", "100", "100", "0")
	var resolution, resolvedDetails string
	if err := db.QueryRow(`SELECT DISTINCT resolution, details FROM reconciliation_issues WHERE order_id=$1 AND issue_type='SOURCE_UNAVAILABLE'`, buy.ID).
		Scan(&resolution, &resolvedDetails); err != nil || resolution != "AUTOMATIC" || !strings.Contains(resolvedDetails, domain.NoFillReleaseAutoReasonCode) {
		t.Fatalf("issue resolution = %s %q, %v", resolution, resolvedDetails, err)
	}
	events, err := harness.repository.Events(context.Background(), buy.ID)
	if err != nil {
		t.Fatal(err)
	}
	last, previous := events[len(events)-1], events[len(events)-2]
	if previous.ToStatus != domain.OrderStatusReconciling || last.ToStatus != domain.OrderStatusCancelled ||
		last.ReasonCode != domain.NoFillReleaseAutoReasonCode || last.Trigger != domain.TransitionTriggerReconciliation ||
		last.VenueStatus != domain.NoFillReleaseVenueStatus || !strings.Contains(last.Reason, "digest=") {
		t.Fatalf("release events = %#v / %#v", previous, last)
	}
	attempts, _ := harness.repository.Attempts(context.Background(), buy.ID)
	final := attempts[len(attempts)-1]
	var evidenceText, storedDigest string
	if err := db.QueryRow(`SELECT evidence::text, evidence_sha256 FROM execution_order_no_fill_resolutions WHERE order_id=$1 AND mode='AUTO'`, buy.ID).
		Scan(&evidenceText, &storedDigest); err != nil {
		t.Fatal(err)
	}
	if final.Kind != domain.OrderAttemptReconcile || final.Outcome != domain.AttemptOutcomeSucceeded || final.RequestFingerprint != storedDigest {
		t.Fatalf("release attempt = %#v", final)
	}
	// The stored evidence document re-encodes to exactly the stored digest.
	var evidence nofillrelease.Evidence
	if err := json.Unmarshal([]byte(evidenceText), &evidence); err != nil {
		t.Fatal(err)
	}
	if _, digest, err := evidence.Encode(); err != nil || digest != storedDigest {
		t.Fatalf("recomputed digest = %s, %v; stored %s", digest, err, storedDigest)
	}
	if evidence.Submit.VenueExpiresAt == nil || !evidence.Release.Balance.Equal("1.064") || len(evidence.IssuesResolved) != 3 {
		t.Fatalf("evidence = %#v", evidence)
	}

	refreshedSell, err := harness.refreshAt(t, sell.ID, start.Add(37*time.Minute))
	if err != nil || refreshedSell.Status != domain.OrderStatusCancelled {
		t.Fatalf("Refresh(SELL) = %s, %v", refreshedSell.Status, err)
	}
	assertPosition(t, db, "wallet-6", "token-sell", "12", "12", "0")
	var releasedShares string
	if err := db.QueryRow(`SELECT released_shares::text FROM execution_order_no_fill_resolutions WHERE order_id=$1`, sell.ID).Scan(&releasedShares); err != nil || !sameNumeric(releasedShares, "12") {
		t.Fatalf("released shares = %s, %v", releasedShares, err)
	}
	// The released order sits on the existing CANCELLED + RELEASED path; a
	// second release attempt is refused by the unique index and the state check.
	if _, err := harness.store.Apply(context.Background(), domain.NoFillResolution{
		Mode: domain.NoFillModeAuto, OrderID: buy.ID, ExecutionAccountID: "main", Actor: "x", ReasonCode: "X",
		ExpectedOrderRevision: state.revision, Evidence: []byte(`{}`), EvidenceSHA256: strings.Repeat("0", 64), ResolvedAt: start,
	}); !errors.Is(err, ErrNoFillReleaseStale) {
		t.Fatalf("second release error = %v, want stale", err)
	}
}

func TestNoFillReleaseKeepsUnknownOnIncompleteEvidencePostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	start := time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		lifetime time.Duration
		at       time.Duration
		mutate   func(*noFillEvidence)
	}{
		{name: "rpc unsupported block", lifetime: 5 * time.Minute, at: 40 * time.Minute, mutate: func(e *noFillEvidence) {
			e.chainErr = errors.New("RPC error -32000: unsupported block number")
		}},
		{name: "rpc timeout", lifetime: 5 * time.Minute, at: 40 * time.Minute, mutate: func(e *noFillEvidence) { e.chainErr = context.DeadlineExceeded }},
		{name: "trades 503", lifetime: 5 * time.Minute, at: 40 * time.Minute, mutate: func(e *noFillEvidence) { e.tradesErr = errors.New("CLOB 503") }},
		{name: "open orders timeout", lifetime: 5 * time.Minute, at: 40 * time.Minute, mutate: func(e *noFillEvidence) { e.openErr = context.DeadlineExceeded }},
		{name: "trade names the order", lifetime: 5 * time.Minute, at: 40 * time.Minute, mutate: func(e *noFillEvidence) {
			e.trades = []domain.NoFillTradeReference{{TradeID: "t-1", Status: "RETRYING", Role: "MAKER"}}
		}},
		{name: "OrderFilled on chain", lifetime: 5 * time.Minute, at: 40 * time.Minute, mutate: func(e *noFillEvidence) {
			e.matches = []domain.NoFillChainMatch{{TransactionHash: "0x01", BlockNumber: 78_000_500}}
		}},
		{name: "before deadline", lifetime: 5 * time.Minute, at: 34 * time.Minute, mutate: func(*noFillEvidence) {}},
		{name: "historical GTC without expiry", lifetime: 0, at: 62 * time.Hour, mutate: func(*noFillEvidence) {}},
	}
	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			accountID := fmt.Sprintf("acct-missing-%d", index)
			insertAccount(t, db, accountID, "0x"+accountID, "100", "100", "0")
			harness := newNoFillHarness(t, db, nofillrelease.ModeOn, testCase.lifetime, start)
			testCase.mutate(harness.evidence)
			order := harness.submitUnknown(t, fmt.Sprintf("missing-%d", index), accountID, "token-missing", domain.SideBuy, "10", "0.58")
			refreshed, err := harness.refreshAt(t, order.ID, start.Add(testCase.at))
			state := readNoFillState(t, db, order.ID)
			if err == nil || refreshed.Status != domain.OrderStatusUnknown || state.status != "UNKNOWN" ||
				state.reservation != "RECONCILIATION_REQUIRED" || state.openIssues != 3 || len(state.audits) != 0 {
				t.Fatalf("state = %#v, refresh err %v; want UNKNOWN, frozen, no audit", state, err)
			}
			assertAccount(t, db, accountID, "100", "94.2", "5.8")
		})
	}
}

func TestNoFillReleaseNeverJudgesAnOrderWithFillsPostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	start := time.Date(2026, 10, 26, 17, 20, 0, 0, time.UTC)
	insertAccount(t, db, "acct-partial", "0xpartial", "100", "100", "0")
	harness := newNoFillHarness(t, db, nofillrelease.ModeOn, 5*time.Minute, start)
	order := harness.submitUnknown(t, "partial", "acct-partial", "token-partial", domain.SideBuy, "10", "0.5")
	// A matched-but-unconfirmed fill component of the order (MINED).
	if _, err := db.Exec(`
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id, execution_account_id,
			market_id, condition_id, token_id, side, liquidity_role, status, shares, price,
			gross_notional, fee_rate_bps, platform_fee, builder_fee_rate_bps, builder_fee,
			total_fee, net_cash_delta, fee_source, matched_at, first_observed_at, last_observed_at
		) VALUES ('fill-partial','polymarket','trade-partial',$1,$2,'acct-partial','market-1','condition-1',
			'token-partial','BUY','TAKER','MINED',3,0.5,1.5,0,0,0,0,0,-1.5,'fixture',now(),now(),now())`,
		order.ID, order.VenueOrderID); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.refreshAt(t, order.ID, start.Add(40*time.Minute)); err == nil {
		t.Fatal("Refresh() must keep the order UNKNOWN")
	}
	if state := readNoFillState(t, db, order.ID); state.status != "UNKNOWN" || state.reservation != "RECONCILIATION_REQUIRED" || len(state.audits) != 0 {
		t.Fatalf("partially filled state = %#v", state)
	}
}

func TestNoFillReleaseRollsBackEveryStepPostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	start := time.Date(2026, 10, 26, 17, 20, 0, 0, time.UTC)
	insertAccount(t, db, "acct-atomic", "0xatomic", "100", "100", "0")
	harness := newNoFillHarness(t, db, nofillrelease.ModeOn, 5*time.Minute, start)
	order := harness.submitUnknown(t, "atomic", "acct-atomic", "token-atomic", domain.SideBuy, "10", "0.5")
	// Fail the reservation release, the step after the order already moved.
	if _, err := db.Exec(`
		CREATE FUNCTION fail_no_fill_release() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.event_type = 'RELEASED' THEN RAISE EXCEPTION 'injected release failure'; END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER fail_no_fill_release_trigger BEFORE INSERT ON asset_reservation_events
		FOR EACH ROW EXECUTE FUNCTION fail_no_fill_release();`); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.refreshAt(t, order.ID, start.Add(40*time.Minute)); err == nil {
		t.Fatal("Refresh() must report the failed release")
	}
	state := readNoFillState(t, db, order.ID)
	if state.status != "UNKNOWN" || state.reservation != "RECONCILIATION_REQUIRED" || state.openIssues != 3 || len(state.audits) != 0 {
		t.Fatalf("state after injected failure = %#v; want nothing committed", state)
	}
	var cancelledEvents int
	if err := db.QueryRow(`SELECT count(*) FROM execution_order_events WHERE order_id=$1 AND to_status='CANCELLED'`, order.ID).Scan(&cancelledEvents); err != nil || cancelledEvents != 0 {
		t.Fatalf("CANCELLED events = %d, %v", cancelledEvents, err)
	}
	assertAccount(t, db, "acct-atomic", "100", "95", "5")
	if _, err := db.Exec(`DROP TRIGGER fail_no_fill_release_trigger ON asset_reservation_events`); err != nil {
		t.Fatal(err)
	}
	if refreshed, err := harness.refreshAt(t, order.ID, start.Add(41*time.Minute)); err != nil || refreshed.Status != domain.OrderStatusCancelled {
		t.Fatalf("retry after the failure = %s, %v", refreshed.Status, err)
	}
	assertAccount(t, db, "acct-atomic", "100", "100", "0")
}

func TestNoFillReleaseRefusesStaleEvidencePostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	start := time.Date(2026, 10, 26, 17, 20, 0, 0, time.UTC)
	insertAccount(t, db, "acct-stale", "0xstale", "100", "100", "0")
	harness := newNoFillHarness(t, db, nofillrelease.ModeOff, 5*time.Minute, start)
	order := harness.submitUnknown(t, "stale", "acct-stale", "token-stale", domain.SideBuy, "10", "0.5")
	harness.clock.Set(start.Add(40 * time.Minute))
	evaluation, err := harness.resolver.Evaluate(context.Background(), order.ID, nofillrelease.EvaluateOptions{Mode: domain.NoFillModeAuto})
	if err != nil || evaluation.Decision.Kind != nofillrelease.DecisionResolve {
		t.Fatalf("Evaluate() = %#v, %v", evaluation.Decision, err)
	}
	// A fill arrives between evidence collection and the transaction.
	if _, err := db.Exec(`
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id, execution_account_id,
			market_id, condition_id, token_id, side, liquidity_role, status, shares, price,
			gross_notional, fee_rate_bps, platform_fee, builder_fee_rate_bps, builder_fee,
			total_fee, net_cash_delta, fee_source, matched_at, first_observed_at, last_observed_at
		) VALUES ('fill-stale','polymarket','trade-stale',$1,$2,'acct-stale','market-1','condition-1',
			'token-stale','BUY','TAKER','MATCHED',1,0.5,0.5,0,0,0,0,0,-0.5,'fixture',now(),now(),now())`,
		order.ID, order.VenueOrderID); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.resolver.Release(context.Background(), evaluation); !errors.Is(err, ErrNoFillReleaseStale) {
		t.Fatalf("Release() after a new fill = %v, want stale", err)
	}
	if _, err := db.Exec(`DELETE FROM execution_fills WHERE fill_key='fill-stale'`); err != nil {
		t.Fatal(err)
	}
	// Automatic mode refuses any revision change.
	if _, err := harness.refreshAt(t, order.ID, start.Add(41*time.Minute)); err == nil {
		t.Fatal("refresh with mode off must keep the order UNKNOWN")
	}
	if _, err := harness.resolver.Release(context.Background(), evaluation); !errors.Is(err, ErrNoFillReleaseStale) {
		t.Fatalf("Release() after another read = %v, want stale for AUTO", err)
	}
	// The operator path tolerates further failed reads but refuses a held lease.
	operator, err := nofillrelease.New(nofillrelease.Params{
		Store: harness.store, Venue: harness.evidence, Chain: harness.evidence, Mode: nofillrelease.ModeOff,
		Policy: nofillrelease.Policy{RequireLeaseFree: true}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: harness.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	operatorEvaluation, err := operator.Evaluate(context.Background(), order.ID, nofillrelease.EvaluateOptions{Mode: domain.NoFillModeOperator, Actor: "op", Reason: "test"})
	if err != nil || operatorEvaluation.Decision.Kind != nofillrelease.DecisionResolve {
		t.Fatalf("operator Evaluate() = %#v, %v", operatorEvaluation.Decision, err)
	}
	leases, _ := NewOrderRecoveryLeaseStore(db)
	current, _ := harness.repository.Get(context.Background(), order.ID)
	if _, err := leases.AcquireOrderRecoveryLease(context.Background(), domain.OrderRecoveryLeaseRequest{
		OrderID: order.ID, ExecutionAccountID: "acct-stale", Holder: "ordercoordinator:x", OrderRevision: current.Revision,
		Now: time.Now().UTC(), TTL: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := operator.Release(context.Background(), operatorEvaluation); !errors.Is(err, ErrNoFillReleaseStale) {
		t.Fatalf("operator Release() with a held lease = %v, want stale", err)
	}
	if _, err := db.Exec(`UPDATE order_recovery_leases SET holder='', acquired_at=NULL, expires_at=NULL WHERE order_id=$1`, order.ID); err != nil {
		t.Fatal(err)
	}
	_, _ = harness.refreshAt(t, order.ID, start.Add(42*time.Minute)) // one more failed read since the evidence
	released, err := operator.Release(context.Background(), operatorEvaluation)
	if err != nil || released.Status != domain.OrderStatusCancelled || released.FailureCode != domain.NoFillReleaseOperatorReasonCode {
		t.Fatalf("operator Release() = %s/%s, %v", released.Status, released.FailureCode, err)
	}
	var resolution string
	if err := db.QueryRow(`SELECT DISTINCT resolution FROM reconciliation_issues WHERE order_id=$1`, order.ID).Scan(&resolution); err != nil || resolution != "MANUAL_REVIEW" {
		t.Fatalf("operator issue resolution = %s, %v", resolution, err)
	}
	var trigger string
	if err := db.QueryRow(`SELECT trigger FROM execution_order_events WHERE order_id=$1 ORDER BY revision DESC LIMIT 1`, order.ID).Scan(&trigger); err != nil || trigger != "OPERATOR" {
		t.Fatalf("operator event trigger = %s, %v", trigger, err)
	}
}

// TestNoFillReleaseReplaysIncidentShapesPostgresIntegration replays the
// 10-26 (BUY, main 1.064 and wallet-6 5.80) and 10-29 (SELL) stuck orders. With
// a GTD expiry the automatic path releases exactly the frozen amounts after
// expiry plus grace; as the real historical GTC orders (no expiry) they stay
// frozen automatically and only the operator --legacy-no-expiration path releases them.
func TestNoFillReleaseReplaysIncidentShapesPostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	start := time.Date(2026, 10, 26, 17, 20, 0, 0, time.UTC)
	insertAccount(t, db, "main", "0xmain", "50", "50", "0")
	insertAccount(t, db, "wallet-6", "0xwallet6", "50", "50", "0")
	if _, err := db.Exec(`INSERT INTO execution_positions (execution_account_id, market_id, token_id, total_shares, available_shares, reserved_shares)
		VALUES ('wallet-6','market-1','token-1029',30,30,0)`); err != nil {
		t.Fatal(err)
	}
	insertOpenLotFixture(t, db, "wallet-6", "token-1029", "30")

	gtd := newNoFillHarness(t, db, nofillrelease.ModeOn, 5*time.Minute, start)
	mainBuy := gtd.submitUnknown(t, "1026-main", "main", "token-1026a", domain.SideBuy, "4", "0.266")
	walletBuy := gtd.submitUnknown(t, "1026-wallet6", "wallet-6", "token-1026b", domain.SideBuy, "10", "0.58")
	assertAccount(t, db, "main", "50", "48.936", "1.064")
	assertAccount(t, db, "wallet-6", "50", "44.2", "5.8")
	for _, at := range []time.Duration{2 * time.Minute, 10 * time.Minute, 34 * time.Minute} {
		_, _ = gtd.refreshAt(t, mainBuy.ID, start.Add(at))
		if state := readNoFillState(t, db, mainBuy.ID); state.status != "UNKNOWN" {
			t.Fatalf("released at +%s, before expiry plus grace", at)
		}
	}
	for _, order := range []domain.Order{mainBuy, walletBuy} {
		if refreshed, err := gtd.refreshAt(t, order.ID, start.Add(36*time.Minute)); err != nil || refreshed.Status != domain.OrderStatusCancelled {
			t.Fatalf("%s at +36m = %s, %v", order.ID, refreshed.Status, err)
		}
	}
	assertAccount(t, db, "main", "50", "50", "0")
	assertAccount(t, db, "wallet-6", "50", "50", "0")

	legacyStart := time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)
	legacy := newNoFillHarness(t, db, nofillrelease.ModeOn, 0, legacyStart)
	sell := legacy.submitUnknown(t, "1029-sell", "wallet-6", "token-1029", domain.SideSell, "12", "0.5")
	assertPosition(t, db, "wallet-6", "token-1029", "30", "18", "12")
	if _, err := legacy.refreshAt(t, sell.ID, legacyStart.Add(8*time.Hour+46*time.Minute)); err == nil {
		t.Fatal("historical GTC order must stay UNKNOWN on the automatic path")
	}
	if state := readNoFillState(t, db, sell.ID); state.status != "UNKNOWN" || len(state.audits) != 0 {
		t.Fatalf("legacy automatic state = %#v", state)
	}
	operator, err := nofillrelease.New(nofillrelease.Params{
		Store: legacy.store, Venue: legacy.evidence, Chain: legacy.evidence, Mode: nofillrelease.ModeOff,
		Policy: nofillrelease.Policy{AllowLegacyNoExpiry: true, LegacyMinAge: time.Hour, RequireLeaseFree: true, MaxChunks: 2000},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: legacy.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := operator.Evaluate(context.Background(), sell.ID, nofillrelease.EvaluateOptions{Mode: domain.NoFillModeOperator, Actor: "op", Reason: "10-29 replay"})
	if err != nil || evaluation.Decision.Kind != nofillrelease.DecisionResolve {
		t.Fatalf("legacy operator Evaluate() = %#v, %v", evaluation.Decision, err)
	}
	if released, err := operator.Release(context.Background(), evaluation); err != nil || released.Status != domain.OrderStatusCancelled {
		t.Fatalf("legacy operator Release() = %s, %v", released.Status, err)
	}
	assertPosition(t, db, "wallet-6", "token-1029", "30", "30", "0")

	// A historical order that is already CANCELLED is refused.
	again, err := operator.Evaluate(context.Background(), sell.ID, nofillrelease.EvaluateOptions{Mode: domain.NoFillModeOperator, Actor: "op", Reason: "again"})
	if err != nil || again.Decision.Kind != nofillrelease.DecisionNotEligible {
		t.Fatalf("re-evaluation of a released order = %#v, %v", again.Decision, err)
	}
}

func TestSubmitInFlightSelectionAndVenueExpiryColumnPostgresIntegration(t *testing.T) {
	db := newNoFillDatabase(t, noFillDatabaseURL(t))
	insertAccount(t, db, "acct-select", "0xselect", "100", "100", "0")
	repository, err := NewOrderRepository(db, WithOrderAttemptVenueExpiry(), WithSubmitInFlightGrace(90*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	// The selection bound is now-poll; the filter excludes attempts younger
	// than bound-grace (here 2s + 90s).
	for name, age := range map[string]time.Duration{"young": 3 * time.Second, "stale": 93 * time.Second} {
		order := integrationOrder("select-"+name, "acct-select", "token-select-"+name, domain.SideBuy, "1", "0.5")
		startedAt := now.Add(-age)
		order.Status, order.CreatedAt, order.UpdatedAt = domain.OrderStatusReceived, startedAt.Add(-time.Second), startedAt.Add(-time.Second)
		stored, _, err := repository.Create(ctx, order)
		if err != nil {
			t.Fatal(err)
		}
		stored, event := applyIntegrationTransition(t, stored, domain.OrderStatusValidating, domain.TransitionTriggerValidation, startedAt.Add(-time.Millisecond*2), "")
		if err := repository.Transition(ctx, stored, event); err != nil {
			t.Fatal(err)
		}
		stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, startedAt.Add(-time.Millisecond), "")
		if err := repository.Transition(ctx, stored, event); err != nil {
			t.Fatal(err)
		}
		expires := startedAt.Add(5 * time.Minute).Truncate(time.Second)
		attempt, err := (domain.OrderAttemptParams{ID: "attempt:" + stored.ID + ":1", OrderID: stored.ID, Sequence: 1,
			Kind: domain.OrderAttemptSubmit, StartedAt: startedAt, VenueExpiresAt: &expires}).Build()
		if err != nil {
			t.Fatal(err)
		}
		stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, startedAt, attempt.ID)
		if err := repository.StartAttempt(ctx, stored, event, attempt); err != nil {
			t.Fatal(err)
		}
		attempts, err := repository.Attempts(ctx, stored.ID)
		if err != nil || len(attempts) != 1 || attempts[0].VenueExpiresAt == nil || !attempts[0].VenueExpiresAt.Equal(expires) {
			t.Fatalf("venue expiry round trip = %#v, %v", attempts, err)
		}
	}
	pending, err := repository.ListPendingForAccounts(ctx, []string{"acct-select"}, now.Add(-2*time.Second), 10)
	if err != nil || len(pending) != 1 || pending[0].ID != "order-select-stale" {
		t.Fatalf("ListPendingForAccounts() = %#v, %v; want only the stale submission", pending, err)
	}
	unfiltered, _ := NewOrderRepository(db)
	if all, err := unfiltered.ListPendingForAccounts(ctx, []string{"acct-select"}, now.Add(-2*time.Second), 10); err != nil || len(all) != 2 {
		t.Fatalf("repository without the option = %d orders, %v", len(all), err)
	}
}

// TestNoFillReleaseDegradesWithoutMigrationPostgresIntegration: a database
// without 0046 reports the feature absent and the default repository keeps
// writing attempts (a venue expiry is simply not recorded).
func TestNoFillReleaseDegradesWithoutMigrationPostgresIntegration(t *testing.T) {
	db := newIntegrationDatabase(t, noFillDatabaseURL(t))
	ctx := context.Background()
	present, err := NoFillReleaseTablesPresent(ctx, db)
	if err != nil || present {
		t.Fatalf("NoFillReleaseTablesPresent() = %v, %v; want false without 0046", present, err)
	}
	if column, err := OrderAttemptVenueExpiryPresent(ctx, db); err != nil || column {
		t.Fatalf("OrderAttemptVenueExpiryPresent() = %v, %v", column, err)
	}
	insertAccount(t, db, "acct-old", "0xold", "100", "100", "0")
	repository, err := NewOrderRepository(db, WithSubmitInFlightGrace(90*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	order := integrationOrder("old-schema", "acct-old", "token-old", domain.SideBuy, "1", "0.5")
	order.Status, order.CreatedAt, order.UpdatedAt = domain.OrderStatusReceived, now, now
	stored, _, err := repository.Create(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	stored, event := applyIntegrationTransition(t, stored, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now.Add(time.Millisecond), "")
	if err := repository.Transition(ctx, stored, event); err != nil {
		t.Fatal(err)
	}
	stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, now.Add(2*time.Millisecond), "")
	if err := repository.Transition(ctx, stored, event); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(5 * time.Minute)
	attempt, _ := (domain.OrderAttemptParams{ID: "attempt:old:1", OrderID: stored.ID, Sequence: 1, Kind: domain.OrderAttemptSubmit,
		StartedAt: now.Add(3 * time.Millisecond), VenueExpiresAt: &expires}).Build()
	stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, attempt.StartedAt, attempt.ID)
	if err := repository.StartAttempt(ctx, stored, event, attempt); err != nil {
		t.Fatalf("StartAttempt() on the old schema = %v", err)
	}
	attempts, err := repository.Attempts(ctx, stored.ID)
	if err != nil || len(attempts) != 1 || attempts[0].VenueExpiresAt != nil {
		t.Fatalf("attempts on the old schema = %#v, %v", attempts, err)
	}
}

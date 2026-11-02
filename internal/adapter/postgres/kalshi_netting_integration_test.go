package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// kalshiNettingFixture books real BUY fills through FillLedger.Record.
type kalshiNettingFixture struct {
	t            *testing.T
	database     *sql.DB
	account      string
	repository   *OrderRepository
	reservations *ReservationManager
	ledger       *FillLedger
	clock        time.Time
	sequence     int
}

func newKalshiNettingFixture(t *testing.T, account, balance string) *kalshiNettingFixture {
	t.Helper()
	database := kalshiSettlementDatabase(t)
	seedKalshiSettlementAccount(t, database, account, balance)
	repository, err := NewOrderRepository(database)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: database, MaxBuyFeeRateBPS: "2000"})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &kalshiNettingFixture{
		t: t, database: database, account: account, repository: repository, reservations: reservations,
		clock: time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC),
	}
	fixture.ledger, err = NewFillLedger(FillLedgerParams{DB: database, Now: func() time.Time { return fixture.tick() }})
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *kalshiNettingFixture) tick() time.Time {
	fixture.clock = fixture.clock.Add(time.Minute)
	return fixture.clock
}

// order creates an acknowledged BUY order for one side of a Kalshi market.
func (fixture *kalshiNettingFixture) order(ticker, side, size string) domain.Order {
	fixture.t.Helper()
	return fixture.acknowledged(ticker, side, domain.SideBuy, size, "0.99", "")
}

func (fixture *kalshiNettingFixture) acknowledged(ticker, outcome string, side domain.Side, size, worstPrice, targetLot string) domain.Order {
	fixture.t.Helper()
	fixture.sequence++
	id := fmt.Sprintf("netting-%d", fixture.sequence)
	token := "kalshi:" + ticker + ":" + outcome
	order := integrationOrder(id, fixture.account, token, side, size, worstPrice)
	order.Intent.Venue = "kalshi"
	order.Intent.MarketID = ticker
	order.Intent.ConditionID = "kalshi:" + ticker
	order.Intent.MarketSource = domain.MarketSourceKalshi
	order.Intent.OutcomeID = outcome
	order.Intent.TargetLotID = targetLot
	outcomeIndex := 0
	order.Intent.OutcomeName = "Yes"
	if outcome == "NO" {
		outcomeIndex = 1
		order.Intent.OutcomeName = "No"
	}
	order.Intent.OutcomeIndex = &outcomeIndex
	return createAcknowledgedIntegrationOrder(fixture.t, context.Background(), fixture.repository, fixture.reservations, order, "venue-"+id, fixture.tick())
}

// sell places and fully fills a SELL order aimed at one lot.
func (fixture *kalshiNettingFixture) sell(ticker, outcome, lotID, shares, price, fee string) domain.FillApplication {
	fixture.t.Helper()
	order := fixture.acknowledged(ticker, outcome, domain.SideSell, shares, "0.01", lotID)
	fill := fixture.fill(order, shares, price, fee)
	fill.Side = domain.SideSell
	fill.NetCashDelta = domain.Decimal(scalar(fixture.t, fixture.database, `SELECT ($1::numeric-$2::numeric)::text`, fill.GrossNotional.String(), fee))
	return fixture.record(order, fill)
}

// fill builds a confirmed fill of an order, costing shares*price+fee in cash.
func (fixture *kalshiNettingFixture) fill(order domain.Order, shares, price, fee string) domain.Fill {
	fixture.t.Helper()
	fixture.sequence++
	id := fmt.Sprintf("fill-%d", fixture.sequence)
	gross := domain.Decimal(scalar(fixture.t, fixture.database, `SELECT ($1::numeric*$2::numeric)::text`, shares, price))
	net := domain.Decimal(scalar(fixture.t, fixture.database, `SELECT (-($1::numeric+$2::numeric))::text`, gross.String(), fee))
	at := fixture.tick()
	return domain.Fill{
		Key: "key-" + id, Venue: "kalshi", VenueFillID: id, OrderID: order.ID, VenueOrderID: order.VenueOrderID,
		ExecutionAccountID: fixture.account, MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID,
		TokenID: order.Intent.TokenID, Side: domain.SideBuy, LiquidityRole: domain.LiquidityRoleTaker,
		Status: domain.FillStatusConfirmed, Shares: domain.Decimal(shares), Price: domain.Decimal(price),
		GrossNotional: gross, FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0", PlatformFee: domain.Decimal(fee),
		BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: domain.Decimal(fee), NetCashDelta: net,
		FeeSource: "KALSHI_API", MatchedAt: at, VenueUpdatedAt: at, ObservedAt: at, ConfirmedAt: &at,
	}
}

// buy places and fully fills one order.
func (fixture *kalshiNettingFixture) buy(ticker, side, shares, price, fee string) domain.FillApplication {
	fixture.t.Helper()
	order := fixture.order(ticker, side, shares)
	return fixture.record(order, fixture.fill(order, shares, price, fee))
}

func (fixture *kalshiNettingFixture) record(order domain.Order, fill domain.Fill) domain.FillApplication {
	fixture.t.Helper()
	application, err := fixture.ledger.Record(context.Background(), order, fill)
	if err != nil || !application.Applied {
		fixture.t.Fatalf("record fill: application=%#v err=%v", application, err)
	}
	return application
}

func (fixture *kalshiNettingFixture) assertSide(token, shares, cost, realized string) {
	fixture.t.Helper()
	assertKalshiSide(fixture.t, fixture.database, fixture.account, token, shares, cost, realized)
}

func assertKalshiSide(t *testing.T, database *sql.DB, account, token, shares, cost, realized string) {
	t.Helper()
	var gotShares, gotAvailable, gotCost, gotRealized string
	if err := database.QueryRow(`
		SELECT total_shares::text, available_shares::text, cost_basis::text, realized_pnl::text
		FROM execution_positions WHERE execution_account_id=$1 AND token_id=$2`, account, token).
		Scan(&gotShares, &gotAvailable, &gotCost, &gotRealized); err != nil {
		t.Fatal(err)
	}
	if !sameNumeric(gotShares, shares) || !sameNumeric(gotAvailable, shares) || !sameNumeric(gotCost, cost) || !sameNumeric(gotRealized, realized) {
		t.Fatalf("%s = shares %s (available %s) cost %s realized %s, want %s cost %s realized %s",
			token, gotShares, gotAvailable, gotCost, gotRealized, shares, cost, realized)
	}
}

// assertLotsAddUp is the exact check Kalshi settlement applies later.
func assertLotsAddUp(t *testing.T, database *sql.DB, account, token string) {
	t.Helper()
	if got := scalar(t, database, `
		SELECT (position.total_shares = COALESCE(lots.shares,0) AND position.cost_basis = COALESCE(lots.cost,0))::text
		FROM execution_positions position
		LEFT JOIN (
			SELECT token_id, SUM(remaining_shares) AS shares, SUM(remaining_cost) AS cost
			FROM position_lots WHERE execution_account_id=$1 AND status='OPEN' GROUP BY token_id
		) lots ON lots.token_id=position.token_id
		WHERE position.execution_account_id=$1 AND position.token_id=$2`, account, token); got != "true" {
		t.Fatalf("open lots of %s do not exactly add up to the position", token)
	}
}

// assertNettingCashAddsUp proves a fill's closures allocated exactly pairs dollars
// and that each closure's realized figure is its proceeds minus its cost.
func assertNettingCashAddsUp(t *testing.T, database *sql.DB, fillKey, pairs string) {
	t.Helper()
	if got := scalar(t, database, `
		SELECT (SUM(allocated_net_proceeds) = $2::numeric
		    AND BOOL_AND(allocated_net_proceeds >= 0)
		    AND BOOL_AND(realized_pnl = allocated_net_proceeds - allocated_cost))::text
		FROM position_lot_closures WHERE closing_fill_key=$1`, fillKey, pairs); got != "true" {
		t.Fatalf("closures of %s do not allocate exactly %s", fillKey, pairs)
	}
}

func TestKalshiNettingPartialOffsetPostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:net-partial", "100")
	yes, no := "kalshi:KXNET-1:YES", "kalshi:KXNET-1:NO"
	first := fixture.buy("KXNET-1", "YES", "10", "0.4", "0")
	if len(first.PositionEvents) != 1 {
		t.Fatalf("a fill with nothing to net must keep one position event, got %d", len(first.PositionEvents))
	}
	application := fixture.buy("KXNET-1", "NO", "4", "0.5", "0.2")
	assertAccount(t, fixture.database, fixture.account, "97.8", "97.8", "0")
	// YES gives up 4 of 10 shares at cost 1.6; NO closes whole at cost 2.2.
	// The 4 dollars split by cost: YES 4*1.6/3.8, NO the remainder.
	fixture.assertSide(yes, "6", "2.4", scalar(t, fixture.database, `SELECT (4*1.6/3.8 - 1.6)::text`))
	fixture.assertSide(no, "0", "0", scalar(t, fixture.database, `SELECT (4 - 4*1.6/3.8 - 2.2)::text`))
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	assertLotsAddUp(t, fixture.database, fixture.account, no)
	assertNettingCashAddsUp(t, fixture.database, application.Fill.Key, "4")

	if got := scalar(t, fixture.database, `
		SELECT (SUM(realized_pnl) = 0.2)::text FROM execution_positions WHERE execution_account_id=$1`, fixture.account); got != "true" {
		t.Fatal("realized pnl must add up to the payout minus the closed cost")
	}
	if got := scalar(t, fixture.database, `SELECT lifecycle_status FROM execution_positions WHERE token_id=$1`, no); got != "CLOSED" {
		t.Fatalf("the fully netted side is %s, want CLOSED", got)
	}
	if got := scalar(t, fixture.database, `SELECT lifecycle_status FROM execution_positions WHERE token_id=$1`, yes); got != "OPEN" {
		t.Fatalf("the remaining side is %s, want OPEN", got)
	}
	if got := scalar(t, fixture.database, `
		SELECT (status='CLOSED' AND remaining_shares=0 AND remaining_cost=0 AND closed_at IS NOT NULL)::text
		FROM position_lots WHERE lot_id=$1`, "lot:"+application.Fill.Key); got != "true" {
		t.Fatal("the fill's own lot must be closed")
	}
	if got := scalar(t, fixture.database, `
		SELECT (status='OPEN' AND remaining_shares=6 AND remaining_cost=2.4 AND original_shares=10)::text
		FROM position_lots WHERE lot_id=$1`, "lot:"+first.Fill.Key); got != "true" {
		t.Fatal("the opposite lot must keep the remainder open")
	}

	// The returned position and the outbox payload are the state after netting.
	if application.Position == nil || !application.Position.TotalShares.Equal("0") ||
		application.Position.LifecycleStatus != domain.PositionLifecycleClosed {
		t.Fatalf("returned position = %#v", application.Position)
	}
	if len(application.PositionEvents) != 3 || application.PositionEvents[0].EventType != domain.PositionEventBought ||
		application.PositionEvents[1].EventType != domain.PositionEventSold || application.PositionEvents[2].EventType != domain.PositionEventSold {
		t.Fatalf("returned events = %#v", application.PositionEvents)
	}
	if got := scalar(t, fixture.database, `
		SELECT (payload->'position'->>'total_shares')::numeric::text FROM execution_outbox
		WHERE topic='trading.fill.confirmed.v1' AND event_key=$1`, application.Fill.Key); !sameNumeric(got, "0") {
		t.Fatalf("outbox position shares = %s, want 0", got)
	}

	// The fill's event carries only its own cash; the payout has its own event.
	if got := scalar(t, fixture.database, `
		SELECT (total_balance_delta = -2.2)::text FROM execution_account_events
		WHERE account_event_id=$1 AND event_type='FILL_SETTLED'`, "account-event:"+application.Fill.Key); got != "true" {
		t.Fatal("FILL_SETTLED must not include the netting payout")
	}
	if got := scalar(t, fixture.database, `
		SELECT (event_type='KALSHI_NETTED' AND order_id=$2 AND fill_key=account_event_id
		    AND total_balance_delta=4 AND available_balance_delta=4 AND reserved_balance_delta=0
		    AND total_balance_after=97.8 AND available_balance_after=97.8 AND reserved_balance_after=0)::text
		FROM execution_account_events WHERE account_event_id=$1`, "kalshi-netting:"+application.Fill.Key, application.Order.ID); got != "true" {
		t.Fatal("the KALSHI_NETTED account event is wrong")
	}
	if got := scalar(t, fixture.database, `
		SELECT COUNT(*)::text FROM position_events
		WHERE fill_key=$1 AND event_type='SOLD' AND order_id=$2 AND position_event_id LIKE 'position-netting:%'`,
		application.Fill.Key, application.Order.ID); got != "2" {
		t.Fatalf("netting position events = %s, want one per token", got)
	}
	if got := scalar(t, fixture.database, `
		SELECT (SUM(cash_delta)=4 AND SUM(shares_delta)=-8)::text FROM position_events
		WHERE fill_key=$1 AND event_type='SOLD'`, application.Fill.Key); got != "true" {
		t.Fatal("netting position events must add up to the payout and both sides' shares")
	}
	if got := scalar(t, fixture.database, `
		SELECT (payload->>'pairs')::numeric::text FROM execution_outbox
		WHERE topic='trading.account.kalshi_netted.v1' AND event_key=$1`, "kalshi-netting:"+application.Fill.Key); !sameNumeric(got, "4") {
		t.Fatalf("netting outbox pairs = %s", got)
	}
}

func TestKalshiNettingFullOffsetClosesBothSidesPostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:net-full", "100")
	yes, no := "kalshi:KXNET-2:YES", "kalshi:KXNET-2:NO"
	fixture.buy("KXNET-2", "YES", "5", "0.4", "0")
	application := fixture.buy("KXNET-2", "NO", "5", "0.5", "0")
	assertAccount(t, fixture.database, fixture.account, "100.5", "100.5", "0")
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	assertLotsAddUp(t, fixture.database, fixture.account, no)
	assertNettingCashAddsUp(t, fixture.database, application.Fill.Key, "5")
	if got := scalar(t, fixture.database, `
		SELECT (BOOL_AND(total_shares=0 AND cost_basis=0 AND average_cost_price=0 AND available_shares=0
		    AND lifecycle_status='CLOSED') AND SUM(realized_pnl)=0.5)::text
		FROM execution_positions WHERE execution_account_id=$1`, fixture.account); got != "true" {
		t.Fatal("a full offset must close both positions and realize payout minus cost")
	}
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM position_lots WHERE execution_account_id=$1 AND status='OPEN'`, fixture.account); got != "0" {
		t.Fatalf("open lots = %s, want 0", got)
	}

	// A closed market side reopens on the next buy and nets nothing further.
	fixture.buy("KXNET-2", "YES", "2", "0.4", "0")
	fixture.assertSide(yes, "2", "0.8", scalar(t, fixture.database,
		`SELECT realized_pnl::text FROM execution_positions WHERE token_id=$1`, yes))
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM execution_account_events WHERE event_type='KALSHI_NETTED'`); got != "1" {
		t.Fatalf("netting events = %s, want 1", got)
	}
}

func TestKalshiNettingClosesOppositeLotsFIFOPostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:net-fifo", "100")
	yes, no := "kalshi:KXNET-3:YES", "kalshi:KXNET-3:NO"
	oldest := fixture.buy("KXNET-3", "YES", "3", "0.3", "0")
	middle := fixture.buy("KXNET-3", "YES", "4", "0.4", "0")
	newest := fixture.buy("KXNET-3", "YES", "1", "0.2", "0")
	application := fixture.buy("KXNET-3", "NO", "5", "0.5", "0")
	if got := scalar(t, fixture.database, `SELECT status FROM position_lots WHERE lot_id=$1`, "lot:"+oldest.Fill.Key); got != "CLOSED" {
		t.Fatalf("the oldest lot is %s, want CLOSED", got)
	}
	if got := scalar(t, fixture.database, `
		SELECT (remaining_shares=2 AND remaining_cost=0.8 AND status='OPEN')::text
		FROM position_lots WHERE lot_id=$1`, "lot:"+middle.Fill.Key); got != "true" {
		t.Fatal("the middle lot must lose exactly the 2 shares left after the oldest")
	}
	if got := scalar(t, fixture.database, `SELECT (remaining_shares=1)::text FROM position_lots WHERE lot_id=$1`, "lot:"+newest.Fill.Key); got != "true" {
		t.Fatal("the newest lot must not be touched")
	}
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM position_lot_closures WHERE closing_fill_key=$1`, application.Fill.Key); got != "3" {
		t.Fatalf("closures = %s, want the fill's lot and two opposite lots", got)
	}
	assertNettingCashAddsUp(t, fixture.database, application.Fill.Key, "5")
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	assertLotsAddUp(t, fixture.database, fixture.account, no)
	// 100 - 0.9 - 1.6 - 0.2 - 2.5 + 5
	assertAccount(t, fixture.database, fixture.account, "99.8", "99.8", "0")

	// A later BUY larger than the opposite side closes all of it and keeps its rest.
	larger := fixture.buy("KXNET-3", "NO", "7", "0.5", "0")
	if got := scalar(t, fixture.database, `
		SELECT (remaining_shares=4 AND status='OPEN' AND remaining_cost=2)::text
		FROM position_lots WHERE lot_id=$1`, "lot:"+larger.Fill.Key); got != "true" {
		t.Fatal("the fill's own lot must keep the shares that found no opposite side")
	}
	assertNettingCashAddsUp(t, fixture.database, larger.Fill.Key, "3")
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	assertLotsAddUp(t, fixture.database, fixture.account, no)
	if got := scalar(t, fixture.database, `SELECT (total_shares=0)::text FROM execution_positions WHERE token_id=$1`, yes); got != "true" {
		t.Fatal("YES must be fully closed")
	}
}

func TestKalshiNettingWaitsWhileTheOppositeSideIsReservedPostgresIntegration(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	fixture := newKalshiNettingFixture(t, "kalshi:net-reserved", "100")
	yes, no := "kalshi:KXNET-4:YES", "kalshi:KXNET-4:NO"
	fixture.buy("KXNET-4", "YES", "10", "0.4", "0")
	if _, err := fixture.database.Exec(`UPDATE execution_positions SET available_shares=4, reserved_shares=6 WHERE token_id=$1`, yes); err != nil {
		t.Fatal(err)
	}
	application := fixture.buy("KXNET-4", "NO", "4", "0.5", "0")
	if len(application.PositionEvents) != 1 {
		t.Fatalf("a deferred netting must add no event, got %d", len(application.PositionEvents))
	}
	if !application.Position.TotalShares.Equal("4") {
		t.Fatalf("the fill must still be booked, position = %#v", application.Position)
	}
	assertAccount(t, fixture.database, fixture.account, "94", "94", "0")
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM position_lot_closures`); got != "0" {
		t.Fatalf("closures = %s, want 0", got)
	}
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM execution_account_events WHERE event_type='KALSHI_NETTED'`); got != "0" {
		t.Fatalf("netting events = %s, want 0", got)
	}
	for _, want := range []string{"Kalshi netting deferred", "execution_account_id=" + fixture.account, "market_id=KXNET-4", "fill_key=" + application.Fill.Key} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("warning lacks %q: %s", want, logs.String())
		}
	}

	// The repair tool refuses while the order is open and catches up afterwards.
	store, err := NewKalshiNettingRepairStore(fixture.database, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := KalshiNettingRepairRequest{
		ExecutionAccountID: fixture.account, Ticker: "KXNET-4",
		Evidence: KalshiNettingEvidence{PositionFP: "6.00", Cash: "98"},
	}
	if _, err := store.Run(context.Background(), request); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("repair while reserved error = %v", err)
	}
	if _, err := fixture.database.Exec(`UPDATE execution_positions SET available_shares=10, reserved_shares=0 WHERE token_id=$1`, yes); err != nil {
		t.Fatal(err)
	}
	report, err := store.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Apply, request.Confirmation = true, report.Fingerprint
	if _, err := store.Run(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	assertAccount(t, fixture.database, fixture.account, "98", "98", "0")
	fixture.assertSide(no, "0", "0", scalar(t, fixture.database, `SELECT realized_pnl::text FROM execution_positions WHERE token_id=$1`, no))
	if got := scalar(t, fixture.database, `SELECT (total_shares=6 AND cost_basis=2.4)::text FROM execution_positions WHERE token_id=$1`, yes); got != "true" {
		t.Fatal("the repair must leave 6 YES shares at 2.4")
	}
}

func TestKalshiNettingClosesOnlyTheNewLotWhenItsOwnSideIsReservedPostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:net-own-reserved", "100")
	yes, no := "kalshi:KXNET-8:YES", "kalshi:KXNET-8:NO"
	fixture.buy("KXNET-8", "YES", "10", "0.4", "0")
	if _, err := fixture.database.Exec(`UPDATE execution_positions SET available_shares=0, reserved_shares=10 WHERE token_id=$1`, yes); err != nil {
		t.Fatal(err)
	}
	held := fixture.buy("KXNET-8", "NO", "4", "0.5", "0") // deferred: YES is reserved
	// Now NO has an open sell and YES is free: a further NO buy may only close its own lot.
	if _, err := fixture.database.Exec(`UPDATE execution_positions SET available_shares=10, reserved_shares=0 WHERE token_id=$1`, yes); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.database.Exec(`UPDATE execution_positions SET available_shares=0, reserved_shares=4 WHERE token_id=$1`, no); err != nil {
		t.Fatal(err)
	}
	application := fixture.buy("KXNET-8", "NO", "2", "0.5", "0")
	assertNettingCashAddsUp(t, fixture.database, application.Fill.Key, "2")
	if got := scalar(t, fixture.database, `SELECT status FROM position_lots WHERE lot_id=$1`, "lot:"+application.Fill.Key); got != "CLOSED" {
		t.Fatalf("the new lot is %s, want CLOSED", got)
	}
	if got := scalar(t, fixture.database, `SELECT (status='OPEN' AND remaining_shares=4)::text FROM position_lots WHERE lot_id=$1`, "lot:"+held.Fill.Key); got != "true" {
		t.Fatal("the reserved older lot must not be touched")
	}
	if got := scalar(t, fixture.database, `
		SELECT (total_shares=4 AND reserved_shares=4 AND available_shares=0)::text
		FROM execution_positions WHERE token_id=$1`, no); got != "true" {
		t.Fatal("the reserved shares must stay covered")
	}
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	assertLotsAddUp(t, fixture.database, fixture.account, no)
}

func TestKalshiNettingReplayedFillPaysOncePostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:net-replay", "100")
	fixture.buy("KXNET-5", "YES", "10", "0.4", "0")
	order := fixture.order("KXNET-5", "NO", "4")
	fill := fixture.fill(order, "4", "0.5", "0")
	application := fixture.record(order, fill)
	assertAccount(t, fixture.database, fixture.account, "98", "98", "0")
	replay, err := fixture.ledger.Record(context.Background(), application.Order, fill)
	if err != nil || replay.Applied || !replay.Duplicate {
		t.Fatalf("replay = %#v, %v, want an idempotent duplicate", replay, err)
	}
	assertAccount(t, fixture.database, fixture.account, "98", "98", "0")
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM execution_account_events WHERE event_type='KALSHI_NETTED'`); got != "1" {
		t.Fatalf("netting events = %s, want 1", got)
	}
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM position_lot_closures`); got != "2" {
		t.Fatalf("closures = %s, want 2", got)
	}
}

// injectKalshiFillSerializationFailures makes the next two confirmed-fill outbox
// inserts fail with 40001, after netting has run in that transaction. deferred
// moves the failure to COMMIT. The counter is a sequence because it must survive
// the rollback it causes.
func injectKalshiFillSerializationFailures(t *testing.T, database *sql.DB, deferred bool) {
	t.Helper()
	trigger := `CREATE TRIGGER netting_retry_trigger AFTER INSERT ON execution_outbox FOR EACH ROW`
	if deferred {
		trigger = `CREATE CONSTRAINT TRIGGER netting_retry_trigger AFTER INSERT ON execution_outbox
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW`
	}
	if _, err := database.Exec(`
		CREATE SEQUENCE netting_retry_attempts;
		CREATE FUNCTION netting_retry_fail() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF nextval('netting_retry_attempts') <= 2 THEN
				RAISE EXCEPTION 'injected serialization failure' USING ERRCODE = '40001';
			END IF;
			RETURN NULL;
		END;
		$$;
		` + trigger + `
		WHEN (NEW.topic = 'trading.fill.confirmed.v1') EXECUTE FUNCTION netting_retry_fail();`); err != nil {
		t.Fatal(err)
	}
}

func TestKalshiNettingSerializationRetryPaysAndWarnsOncePostgresIntegration(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	const warning = "Kalshi netting deferred"

	for name, deferred := range map[string]bool{"statement": false, "commit": true} {
		t.Run("netted at "+name, func(t *testing.T) {
			logs.Reset()
			fixture := newKalshiNettingFixture(t, "kalshi:net-retry", "100")
			yes, no := "kalshi:KXNET-9:YES", "kalshi:KXNET-9:NO"
			fixture.buy("KXNET-9", "YES", "10", "0.4", "0")
			injectKalshiFillSerializationFailures(t, fixture.database, deferred)
			application := fixture.buy("KXNET-9", "NO", "4", "0.5", "0")
			if got := scalar(t, fixture.database, `SELECT last_value::text FROM netting_retry_attempts`); got != "3" {
				t.Fatalf("attempts = %s, want two failures and one success", got)
			}
			assertAccount(t, fixture.database, fixture.account, "98", "98", "0")
			if !application.Position.TotalShares.Equal("0") || len(application.PositionEvents) != 3 {
				t.Fatalf("application = %#v", application)
			}
			for _, count := range []struct{ query, want string }{
				{`SELECT COUNT(*)::text FROM execution_account_events WHERE event_type='KALSHI_NETTED'`, "1"},
				{`SELECT COUNT(*)::text FROM execution_account_events WHERE event_type='FILL_SETTLED'`, "2"},
				{`SELECT COUNT(*)::text FROM position_lot_closures`, "2"},
				{`SELECT COUNT(*)::text FROM position_events WHERE event_type='SOLD'`, "2"},
				{`SELECT COUNT(*)::text FROM execution_outbox WHERE topic='trading.account.kalshi_netted.v1'`, "1"},
				{`SELECT COUNT(*)::text FROM execution_outbox WHERE topic='trading.fill.confirmed.v1'`, "2"},
			} {
				if got := scalar(t, fixture.database, count.query); got != count.want {
					t.Fatalf("%s = %s, want %s", count.query, got, count.want)
				}
			}
			assertNettingCashAddsUp(t, fixture.database, application.Fill.Key, "4")
			assertLotsAddUp(t, fixture.database, fixture.account, yes)
			assertLotsAddUp(t, fixture.database, fixture.account, no)
			if got := strings.Count(logs.String(), warning); got != 0 {
				t.Fatalf("warnings = %d, want 0 when netting happened", got)
			}
		})
		t.Run("deferred at "+name, func(t *testing.T) {
			logs.Reset()
			fixture := newKalshiNettingFixture(t, "kalshi:net-retry-warn", "100")
			yes := "kalshi:KXNET-10:YES"
			fixture.buy("KXNET-10", "YES", "10", "0.4", "0")
			if _, err := fixture.database.Exec(`UPDATE execution_positions SET available_shares=4, reserved_shares=6 WHERE token_id=$1`, yes); err != nil {
				t.Fatal(err)
			}
			injectKalshiFillSerializationFailures(t, fixture.database, deferred)
			application := fixture.buy("KXNET-10", "NO", "4", "0.5", "0")
			if got := scalar(t, fixture.database, `SELECT last_value::text FROM netting_retry_attempts`); got != "3" {
				t.Fatalf("attempts = %s, want two failures and one success", got)
			}
			assertAccount(t, fixture.database, fixture.account, "94", "94", "0")
			if !application.Position.TotalShares.Equal("4") || len(application.PositionEvents) != 1 {
				t.Fatalf("application = %#v", application)
			}
			if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM execution_account_events WHERE event_type='KALSHI_NETTED'`); got != "0" {
				t.Fatalf("netting events = %s, want 0", got)
			}
			if got := strings.Count(logs.String(), warning); got != 1 {
				t.Fatalf("warnings = %d, want exactly 1 after two failed attempts: %s", got, logs.String())
			}
		})
	}
}

func TestKalshiSellOfPartlyNettedLotThenSettlesPostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:net-sell", "100")
	ticker, yes, no := "KXNET-11", "kalshi:KXNET-11:YES", "kalshi:KXNET-11:NO"
	// 7 shares at 2.5911 leave a repeating decimal in the lot once 4 are netted.
	netted := fixture.buy(ticker, "YES", "7", "0.37", "0.0011")
	fixture.buy(ticker, "YES", "3", "0.33", "0.0007")
	fixture.buy(ticker, "NO", "4", "0.61", "0.0013")
	nettedLot := "lot:" + netted.Fill.Key
	if got := scalar(t, fixture.database, `SELECT (status='OPEN' AND remaining_shares=3 AND original_shares=7)::text FROM position_lots WHERE lot_id=$1`, nettedLot); got != "true" {
		t.Fatal("the oldest lot must be partly closed by netting before the sale")
	}

	// Part of the remainder, then the rest, so both the partial and full branches of
	// applySell run on a cost that netting already divided.
	partial := fixture.sell(ticker, "YES", nettedLot, "1.5", "0.4", "0.001")
	if len(partial.PositionEvents) != 1 || partial.PositionEvents[0].EventType != domain.PositionEventSold {
		t.Fatalf("a SELL must not net: events = %#v", partial.PositionEvents)
	}
	if !partial.Position.TotalShares.Equal("4.5") {
		t.Fatalf("position after the partial sale = %s, want 4.5", partial.Position.TotalShares)
	}
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	if got := scalar(t, fixture.database, `SELECT (status='OPEN' AND remaining_shares=1.5)::text FROM position_lots WHERE lot_id=$1`, nettedLot); got != "true" {
		t.Fatal("the partial sale must leave 1.5 shares in the lot")
	}
	final := fixture.sell(ticker, "YES", nettedLot, "1.5", "0.45", "0")
	if !final.Position.TotalShares.Equal("3") {
		t.Fatalf("position after the full sale = %s, want 3", final.Position.TotalShares)
	}
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	assertLotsAddUp(t, fixture.database, fixture.account, no)
	if got := scalar(t, fixture.database, `SELECT (status='CLOSED' AND remaining_shares=0 AND remaining_cost=0)::text FROM position_lots WHERE lot_id=$1`, nettedLot); got != "true" {
		t.Fatal("the full sale must close the lot with no cost left")
	}
	if got := scalar(t, fixture.database, `SELECT (SUM(closed_shares)=7 AND SUM(allocated_cost)=2.5911)::text FROM position_lot_closures WHERE lot_id=$1`, nettedLot); got != "true" {
		t.Fatal("netting and the two sales must close exactly the lot's shares and cost")
	}
	// 100 - 2.5911 - 0.9907 - 2.4413 + 4 + 0.599 + 0.675
	assertAccount(t, fixture.database, fixture.account, "99.2509", "99.2509", "0")

	store, err := NewKalshiSettlementStore(fixture.database, []string{fixture.account})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := store.ApplyKalshiSettlement(context.Background(), fixture.account,
		kalshiRecord(ticker, "yes", "3.00", "0.00", "3.00", "1.00"), time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC))
	if err != nil || outcome.Result != domain.KalshiSettlementApplied {
		t.Fatalf("settlement = %#v, %v, want APPLIED", outcome, err)
	}
	assertAccount(t, fixture.database, fixture.account, "102.2509", "102.2509", "0")
	// Every dollar in and out is accounted for: realized equals the change in cash.
	if got := scalar(t, fixture.database, `SELECT (SUM(realized_pnl)=2.2509)::text FROM execution_positions WHERE execution_account_id=$1`, fixture.account); got != "true" {
		t.Fatalf("realized total = %s, want exactly 2.2509", scalar(t, fixture.database,
			`SELECT SUM(realized_pnl)::text FROM execution_positions WHERE execution_account_id=$1`, fixture.account))
	}
	if got := scalar(t, fixture.database, `SELECT COUNT(*)::text FROM position_lots WHERE execution_account_id=$1 AND status='OPEN'`, fixture.account); got != "0" {
		t.Fatalf("open lots after settlement = %s, want 0", got)
	}
}

func TestPolymarketFillsNeverNetPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "net-polymarket"
	seedKalshiSettlementAccount(t, database, account, "100")
	repository, err := NewOrderRepository(database)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: database, MaxBuyFeeRateBPS: "2000"})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: database})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC)
	for index, token := range []string{"poly-token-yes", "poly-token-no"} {
		order := integrationOrder(token, account, token, domain.SideBuy, "4", "0.99")
		outcomeIndex := index
		order.Intent.OutcomeIndex = &outcomeIndex
		order = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, order, "venue-"+token, now)
		at := now.Add(time.Minute)
		application, err := ledger.Record(ctx, order, domain.Fill{
			Key: "key-" + token, Venue: "polymarket", VenueFillID: token, OrderID: order.ID, VenueOrderID: order.VenueOrderID,
			ExecutionAccountID: account, MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID,
			TokenID: token, Side: domain.SideBuy, LiquidityRole: domain.LiquidityRoleTaker,
			Status: domain.FillStatusConfirmed, Shares: "4", Price: "0.5", GrossNotional: "2", FeeRateBPS: "0",
			PlatformFeeRate: "0", FeeExponent: "0", PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
			NetCashDelta: "-2", FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT", MatchedAt: at, VenueUpdatedAt: at, ObservedAt: at, ConfirmedAt: &at,
		})
		if err != nil || !application.Applied || len(application.PositionEvents) != 1 {
			t.Fatalf("record %s: %#v, %v", token, application, err)
		}
	}
	assertAccount(t, database, account, "96", "96", "0")
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM position_lots WHERE execution_account_id=$1 AND status='OPEN' AND remaining_shares=4`, account); got != "2" {
		t.Fatalf("open lots = %s, want 2", got)
	}
	for _, table := range []string{
		`position_lot_closures`,
		`execution_account_events WHERE event_type='KALSHI_NETTED'`,
		`position_events WHERE event_type='SOLD'`,
		`execution_outbox WHERE topic='trading.account.kalshi_netted.v1'`,
	} {
		if got := scalar(t, database, `SELECT COUNT(*)::text FROM `+table); got != "0" {
			t.Fatalf("%s has %s rows, want 0", table, got)
		}
	}
}

func TestKalshiOppositeToken(t *testing.T) {
	for token, want := range map[string][2]string{
		"kalshi:KX-A:YES":      {"KX-A", "kalshi:KX-A:NO"},
		"kalshi:KX-A:NO":       {"KX-A", "kalshi:KX-A:YES"},
		"kalshi:KX:WITH:C:YES": {"KX:WITH:C", "kalshi:KX:WITH:C:NO"},
	} {
		ticker, opposite, ok := kalshiOppositeToken(token)
		if !ok || ticker != want[0] || opposite != want[1] {
			t.Errorf("kalshiOppositeToken(%q) = %q, %q, %v", token, ticker, opposite, ok)
		}
	}
	for _, token := range []string{"", "12345", "kalshi:", "kalshi::YES", "kalshi:KX-A", "kalshi:KX-A:MAYBE", "poly:KX-A:YES"} {
		if _, _, ok := kalshiOppositeToken(token); ok {
			t.Errorf("kalshiOppositeToken(%q) must be rejected", token)
		}
	}
}

func TestKalshiNettedMarketSettlesWithoutReviewPostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:net-settle", "100")
	yes, no := "kalshi:KXNET-6:YES", "kalshi:KXNET-6:NO"
	// Uneven lots force a repeating decimal into the remaining cost.
	fixture.buy("KXNET-6", "YES", "3", "0.33", "0.0007")
	fixture.buy("KXNET-6", "YES", "7", "0.37", "0.0011")
	fixture.buy("KXNET-6", "NO", "4", "0.61", "0.0013")
	assertLotsAddUp(t, fixture.database, fixture.account, yes)
	assertLotsAddUp(t, fixture.database, fixture.account, no)
	cashBefore := scalar(t, fixture.database, `SELECT total_balance::text FROM execution_accounts WHERE execution_account_id=$1`, fixture.account)
	store, err := NewKalshiSettlementStore(fixture.database, []string{fixture.account})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := store.ApplyKalshiSettlement(context.Background(), fixture.account,
		kalshiRecord("KXNET-6", "yes", "6.00", "0.00", "6.00", "1.00"), time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC))
	if err != nil || outcome.Result != domain.KalshiSettlementApplied {
		t.Fatalf("settlement = %#v, %v, want APPLIED", outcome, err)
	}
	if got := scalar(t, fixture.database, `SELECT (total_balance = $2::numeric + 6)::text FROM execution_accounts WHERE execution_account_id=$1`, fixture.account, cashBefore); got != "true" {
		t.Fatal("settlement must credit the 6 winning shares")
	}
	if got := scalar(t, fixture.database, `SELECT status FROM kalshi_settlements WHERE execution_account_id=$1 AND ticker='KXNET-6'`, fixture.account); got != "APPLIED" {
		t.Fatalf("settlement row = %s, want APPLIED", got)
	}
}

// The real KXRT-GOD-92 numbers: 15 YES costing 10.8662, then one NO order filled
// 0.02 for 0.0112 and 13.98 for 7.7923.
const (
	god92Ticker = "KXRT-GOD-92"
	god92Yes    = "kalshi:" + god92Ticker + ":YES"
	god92No     = "kalshi:" + god92Ticker + ":NO"
)

func assertGod92Repaired(t *testing.T, database *sql.DB, account string) {
	t.Helper()
	// Remaining YES cost is 10.8662/15; the rest of the 18.6697 was closed for 14.
	if got := scalar(t, database, `
		SELECT (total_shares=1 AND available_shares=1 AND ROUND(cost_basis,4)=0.7244
		    AND ABS(cost_basis-10.8662/15) < 0.000000001 AND lifecycle_status='OPEN')::text
		FROM execution_positions WHERE execution_account_id=$1 AND token_id=$2`, account, god92Yes); got != "true" {
		t.Fatalf("YES = %s", scalar(t, database, `SELECT total_shares::text||' / '||cost_basis::text FROM execution_positions WHERE token_id=$1`, god92Yes))
	}
	if got := scalar(t, database, `
		SELECT (total_shares=0 AND cost_basis=0 AND lifecycle_status='CLOSED')::text
		FROM execution_positions WHERE execution_account_id=$1 AND token_id=$2`, account, god92No); got != "true" {
		t.Fatal("NO must be closed")
	}
	if got := scalar(t, database, `
		SELECT (ROUND(SUM(realized_pnl),4) = -3.9453)::text
		FROM execution_positions WHERE execution_account_id=$1 AND token_id IN ($2,$3)`, account, god92Yes, god92No); got != "true" {
		t.Fatalf("realized total = %s, want about -3.9453", scalar(t, database,
			`SELECT SUM(realized_pnl)::text FROM execution_positions WHERE execution_account_id=$1`, account))
	}
	assertLotsAddUp(t, database, account, god92Yes)
	assertLotsAddUp(t, database, account, god92No)
	if got := scalar(t, database, `
		SELECT (SUM(allocated_net_proceeds)=14 AND SUM(closed_shares)=28)::text
		FROM position_lot_closures closure JOIN position_lots lot USING (lot_id)
		WHERE lot.execution_account_id=$1 AND lot.token_id IN ($2,$3)`, account, god92Yes, god92No); got != "true" {
		t.Fatal("closures must pay exactly 14 dollars for 14 pairs")
	}
}

func assertGod92SettlesApplied(t *testing.T, database *sql.DB, account string) {
	t.Helper()
	store, err := NewKalshiSettlementStore(database, []string{account})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := store.ApplyKalshiSettlement(context.Background(), account,
		kalshiRecord(god92Ticker, "yes", "1.00", "0.00", "1.00", "1.00"), time.Date(2026, 11, 9, 1, 0, 0, 0, time.UTC))
	if err != nil || outcome.Result != domain.KalshiSettlementApplied {
		t.Fatalf("settlement = %#v, %v, want APPLIED", outcome, err)
	}
}

func TestKalshiNettingGod92LiveFillsPostgresIntegration(t *testing.T) {
	fixture := newKalshiNettingFixture(t, "kalshi:god92-live", "100")
	fixture.buy(god92Ticker, "YES", "15", "0.72", "0.0662")
	order := fixture.order(god92Ticker, "NO", "14")
	first := fixture.record(order, fixture.fill(order, "0.02", "0.55", "0.0002"))
	second := fixture.record(first.Order, fixture.fill(first.Order, "13.98", "0.55", "0.1033"))
	if second.Order.Status != domain.OrderStatusFilled {
		t.Fatalf("order status = %s, want FILLED", second.Order.Status)
	}
	// 100 - 10.8662 - 0.0112 - 7.7923 + 14
	assertAccount(t, fixture.database, fixture.account, "95.3303", "95.3303", "0")
	assertGod92Repaired(t, fixture.database, fixture.account)
	assertNettingCashAddsUp(t, fixture.database, first.Fill.Key, "0.02")
	assertNettingCashAddsUp(t, fixture.database, second.Fill.Key, "13.98")
	assertGod92SettlesApplied(t, fixture.database, fixture.account)
}

// seedBookedKalshiBuy writes the state the ledger left for a BUY before it netted:
// a confirmed applied fill, its lot, and its share of the position.
func seedBookedKalshiBuy(t *testing.T, database *sql.DB, account, ticker, side, orderID, fillKey, shares, cost string, appliedAt time.Time) {
	t.Helper()
	token := "kalshi:" + ticker + ":" + side
	index, name := 0, "Yes"
	if side == "NO" {
		index, name = 1, "No"
	}
	if _, err := database.Exec(`
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id, token_id,
			intent, venue_order_id, status, filled_size, average_fill_price,
			filled_notional, total_fees, revision, created_at, updated_at
		) VALUES ($1,$1,$2,'kalshi',$3,$4,'{"model_id":"model-a","strategy_id":"strategy-v1"}'::jsonb,$1,'FILLED',
		          $5::numeric,0.5,$6::numeric,0,1,$7,$7)
		ON CONFLICT (order_id) DO NOTHING`, orderID, account, ticker, token, shares, cost, appliedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id,
			execution_account_id, market_id, condition_id, token_id, side,
			liquidity_role, status, shares, price, gross_notional, fee_rate_bps,
			platform_fee, builder_fee_rate_bps, builder_fee, total_fee,
			net_cash_delta, fee_source, matched_at, first_observed_at, last_observed_at,
			confirmed_at, applied_at
		) VALUES ($1,'kalshi',$1,$2,$2,$3,$4,$5,$6,'BUY','TAKER','CONFIRMED',$7::numeric,0.5,$8::numeric,0,0,0,0,0,
		          -$8::numeric,'KALSHI_API',$9,$9,$9,$9,$9)`,
		fillKey, orderID, account, ticker, "kalshi:"+ticker, token, shares, cost, appliedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		INSERT INTO execution_positions (
			execution_account_id, market_id, condition_id, token_id, outcome_index, outcome_name,
			total_shares, available_shares, reserved_shares, cost_basis, average_cost_price,
			realized_pnl, market_value, unrealized_pnl, lifecycle_status
		) VALUES ($1,$2,$3,$4,$5,$6,$7::numeric,$7::numeric,0,$8::numeric,$8::numeric/$7::numeric,0,0,0,'OPEN')
		ON CONFLICT (execution_account_id, token_id) DO UPDATE
		SET total_shares=execution_positions.total_shares+EXCLUDED.total_shares,
		    available_shares=execution_positions.available_shares+EXCLUDED.available_shares,
		    cost_basis=execution_positions.cost_basis+EXCLUDED.cost_basis,
		    average_cost_price=(execution_positions.cost_basis+EXCLUDED.cost_basis)/(execution_positions.total_shares+EXCLUDED.total_shares)`,
		account, ticker, "kalshi:"+ticker, token, index, name, shares, cost); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		INSERT INTO position_lots (
			lot_id, execution_account_id, market_id, token_id, model_id, strategy_id,
			opening_order_id, opening_fill_key, original_shares, remaining_shares,
			original_cost, remaining_cost, average_entry_price, status, opened_at,
			condition_id, outcome_index, outcome_name
		) VALUES ($1,$2,$3,$4,'model-a','strategy-v1',$5,$6,$7::numeric,$7::numeric,$8::numeric,$8::numeric,
		          0.5,'OPEN',$9,$10,$11,$12)`,
		"lot:"+fillKey, account, ticker, token, orderID, fillKey, shares, cost, appliedAt, "kalshi:"+ticker, index, name); err != nil {
		t.Fatal(err)
	}
}

// seedGod92Unnetted reproduces the production ledger of KXRT-GOD-92.
func seedGod92Unnetted(t *testing.T, database *sql.DB, account string) {
	t.Helper()
	seedKalshiSettlementAccount(t, database, account, "9.0471")
	at := time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC)
	seedBookedKalshiBuy(t, database, account, god92Ticker, "YES", "god92-yes", "god92-fill-yes", "15", "10.8662", at)
	seedBookedKalshiBuy(t, database, account, god92Ticker, "NO", "god92-no", "god92-fill-no-a", "0.02", "0.0112", at.Add(time.Hour))
	seedBookedKalshiBuy(t, database, account, god92Ticker, "NO", "god92-no", "god92-fill-no-b", "13.98", "7.7923", at.Add(2*time.Hour))
}

// ledgerState hashes every table the repair could touch.
func ledgerState(t *testing.T, database *sql.DB) string {
	t.Helper()
	var state strings.Builder
	for _, table := range []string{
		"execution_accounts", "execution_positions", "position_lots", "position_lot_closures",
		"position_events", "execution_account_events", "execution_outbox", "execution_fills", "execution_orders",
	} {
		state.WriteString(table + "=" + scalar(t, database, `SELECT COALESCE(md5(string_agg(row_text, '|' ORDER BY row_text)),'')
			FROM (SELECT t::text AS row_text FROM `+table+` t) rows`) + ";")
	}
	return state.String()
}

func god92RepairRequest(account string) KalshiNettingRepairRequest {
	return KalshiNettingRepairRequest{
		ExecutionAccountID: account, Ticker: god92Ticker,
		Evidence: KalshiNettingEvidence{PositionFP: "1.00", Cash: "23.0471"},
	}
}

func TestKalshiNettingRepairGod92PostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:god92"
	seedGod92Unnetted(t, database, account)
	at := time.Date(2026, 11, 2, 3, 0, 0, 0, time.UTC)
	store, err := NewKalshiNettingRepairStore(database, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	untouched := ledgerState(t, database)

	report, err := store.Run(ctx, god92RepairRequest(account))
	if err != nil {
		t.Fatal(err)
	}
	if report.Applied || report.Fingerprint == "" {
		t.Fatalf("dry run = %#v", report)
	}
	if got := ledgerState(t, database); got != untouched {
		t.Fatal("the dry run changed the database")
	}
	if !report.Before.Yes.Shares.Equal("15") || !report.Before.No.Shares.Equal("14") || !report.Before.TotalBalance.Equal("9.0471") ||
		!report.After.Yes.Shares.Equal("1") || !report.After.No.Shares.Equal("0") || !report.After.TotalBalance.Equal("23.0471") ||
		!report.CashDifference.Equal("0") {
		t.Fatalf("plan = %#v", report)
	}
	if len(report.Closures) != 4 {
		t.Fatalf("closures = %#v, want both NO lots and the YES lot twice", report.Closures)
	}
	t.Logf("GOD-92 after: YES %s cost %s realized %s; NO %s cost %s realized %s; cash %s",
		report.After.Yes.Shares, report.After.Yes.CostBasis, report.After.Yes.RealizedPnL,
		report.After.No.Shares, report.After.No.CostBasis, report.After.No.RealizedPnL, report.After.TotalBalance)

	// The dry run is repeatable and the fingerprint does not depend on the time.
	at = at.Add(time.Hour)
	again, err := store.Run(ctx, god92RepairRequest(account))
	if err != nil || again.Fingerprint != report.Fingerprint {
		t.Fatalf("second dry run fingerprint = %s, %v, want %s", again.Fingerprint, err, report.Fingerprint)
	}

	wrong := god92RepairRequest(account)
	wrong.Apply, wrong.Confirmation = true, strings.Repeat("0", 64)
	if _, err := store.Run(ctx, wrong); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("wrong confirmation error = %v", err)
	}
	if got := ledgerState(t, database); got != untouched {
		t.Fatal("a rejected confirmation changed the database")
	}

	apply := god92RepairRequest(account)
	apply.Apply, apply.Confirmation = true, report.Fingerprint
	applied, err := store.Run(ctx, apply)
	if err != nil || !applied.Applied {
		t.Fatalf("apply = %#v, %v", applied, err)
	}
	assertAccount(t, database, account, "23.0471", "23.0471", "0")
	assertGod92Repaired(t, database, account)
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_account_events WHERE event_type='KALSHI_NETTED' AND execution_account_id=$1`, account); got != "2" {
		t.Fatalf("netting events = %s, want one per NO fill", got)
	}
	if got := scalar(t, database, `
		SELECT (BOOL_AND(occurred_at = $1) AND SUM(total_balance_delta)=14)::text
		FROM execution_account_events WHERE event_type='KALSHI_NETTED'`, at); got != "true" {
		t.Fatal("repair events must be stamped with the run time so the balance series stays ordered")
	}
	if got := scalar(t, database, `SELECT COUNT(DISTINCT closing_fill_key)::text FROM position_lot_closures`); got != "2" {
		t.Fatalf("closures must stay attributed to the two NO fills, got %s fills", got)
	}

	// Running again finds nothing to do and pays nothing.
	repaired := ledgerState(t, database)
	rerun, err := store.Run(ctx, god92RepairRequest(account))
	if err != nil || len(rerun.Closures) != 0 {
		t.Fatalf("second run = %#v, %v", rerun, err)
	}
	rerunApply := god92RepairRequest(account)
	rerunApply.Apply, rerunApply.Confirmation = true, rerun.Fingerprint
	if _, err := store.Run(ctx, rerunApply); err != nil {
		t.Fatal(err)
	}
	if got := ledgerState(t, database); got != repaired {
		t.Fatal("a second apply changed the database")
	}
	assertGod92SettlesApplied(t, database, account)
}

func TestKalshiNettingRepairRefusesDisagreeingEvidencePostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:god92-refuse"
	seedGod92Unnetted(t, database, account)
	store, err := NewKalshiNettingRepairStore(database, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	untouched := ledgerState(t, database)
	for name, test := range map[string]struct {
		mutate func(*KalshiNettingRepairRequest)
		want   string
	}{
		"exchange holds other shares":   {func(request *KalshiNettingRepairRequest) { request.Evidence.PositionFP = "2.00" }, "local net position"},
		"exchange holds the other side": {func(request *KalshiNettingRepairRequest) { request.Evidence.PositionFP = "-1.00" }, "local net position"},
		"exchange cash differs":         {func(request *KalshiNettingRepairRequest) { request.Evidence.Cash = "24.0471" }, "exchange cash"},
		"cash just outside tolerance": {func(request *KalshiNettingRepairRequest) {
			request.Evidence.Cash = "23.06"
			request.CashTolerance = "0.01"
		}, "exchange cash"},
		"expected difference did not happen": {func(request *KalshiNettingRepairRequest) { request.ExpectedCashDiff = "1" }, "exchange cash"},
	} {
		request := god92RepairRequest(account)
		test.mutate(&request)
		report, err := store.Run(ctx, request)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s: error = %v, want %q", name, err, test.want)
		}
		if report.Applied || report.ExecutionAccountID == "" {
			t.Fatalf("%s: the refused plan must be reported and not applied: %#v", name, report)
		}
		// Even carrying its own fingerprint, a refused plan cannot be written.
		request.Apply, request.Confirmation = true, report.Fingerprint
		if _, err := store.Run(ctx, request); err == nil {
			t.Fatalf("%s: apply succeeded", name)
		}
		if got := ledgerState(t, database); got != untouched {
			t.Fatalf("%s: the database changed", name)
		}
	}

	if _, err := store.Run(ctx, KalshiNettingRepairRequest{ExecutionAccountID: account, Ticker: god92Ticker}); err == nil {
		t.Fatal("missing evidence must be rejected")
	}
	if _, err := store.Run(ctx, KalshiNettingRepairRequest{ExecutionAccountID: "main", Ticker: god92Ticker,
		Evidence: KalshiNettingEvidence{PositionFP: "1", Cash: "1"}}); err == nil {
		t.Fatal("a Polymarket account must be rejected")
	}

	// A fingerprint from before the exchange moved no longer confirms the plan.
	report, err := store.Run(ctx, god92RepairRequest(account))
	if err != nil {
		t.Fatal(err)
	}
	moved := god92RepairRequest(account)
	moved.Evidence.Cash, moved.CashTolerance = "23.05", "0.01"
	moved.Apply, moved.Confirmation = true, report.Fingerprint
	if _, err := store.Run(ctx, moved); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("stale fingerprint error = %v", err)
	}
	if got := ledgerState(t, database); got != untouched {
		t.Fatal("a stale fingerprint changed the database")
	}
}

func TestKalshiNettingRepairHonorsExpectedCashDiffPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:god92-diff"
	seedGod92Unnetted(t, database, account)
	store, err := NewKalshiNettingRepairStore(database, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// A sale elsewhere left the exchange 3.50 ahead of the ledger.
	request := god92RepairRequest(account)
	request.Evidence.Cash = "26.5471"
	if _, err := store.Run(ctx, request); err == nil {
		t.Fatal("an unexplained difference must be refused")
	}
	request.ExpectedCashDiff = "3.50"
	report, err := store.Run(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !report.CashDifference.Equal("3.5") {
		t.Fatalf("reported difference = %s", report.CashDifference)
	}
	// A whole-cents exchange balance needs the explicit tolerance.
	cents := god92RepairRequest(account)
	cents.Evidence.Cash = "23.05"
	if _, err := store.Run(ctx, cents); err == nil {
		t.Fatal("a rounded balance must be refused without a tolerance")
	}
	cents.CashTolerance = "0.01"
	if _, err := store.Run(ctx, cents); err != nil {
		t.Fatal(err)
	}
	request.Apply, request.Confirmation = true, report.Fingerprint
	if _, err := store.Run(ctx, request); err != nil {
		t.Fatal(err)
	}
	assertAccount(t, database, account, "23.0471", "23.0471", "0")
	assertGod92Repaired(t, database, account)
}

func TestKalshiNettingRepairReplaysFillsInBookingOrderPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:net-order"
	seedKalshiSettlementAccount(t, database, account, "50")
	at := time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC)
	seedBookedKalshiBuy(t, database, account, "KXNET-7", "YES", "order-a", "fill-a", "5", "2", at)
	seedBookedKalshiBuy(t, database, account, "KXNET-7", "NO", "order-b", "fill-b", "4", "2", at.Add(time.Hour))
	seedBookedKalshiBuy(t, database, account, "KXNET-7", "YES", "order-c", "fill-c", "3", "1.5", at.Add(2*time.Hour))
	store, err := NewKalshiNettingRepairStore(database, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := KalshiNettingRepairRequest{
		ExecutionAccountID: account, Ticker: "KXNET-7",
		Evidence: KalshiNettingEvidence{PositionFP: "4.00", Cash: "54"},
	}
	report, err := store.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Apply, request.Confirmation = true, report.Fingerprint
	if _, err := store.Run(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	// Only the middle NO fill nets, and only against the lot opened before it.
	if got := scalar(t, database, `SELECT string_agg(DISTINCT closing_fill_key, ',') FROM position_lot_closures`); got != "fill-b" {
		t.Fatalf("closing fills = %s, want fill-b", got)
	}
	if got := scalar(t, database, `SELECT (remaining_shares=1 AND status='OPEN')::text FROM position_lots WHERE lot_id='lot:fill-a'`); got != "true" {
		t.Fatal("the earlier YES lot must take the whole offset")
	}
	if got := scalar(t, database, `SELECT (remaining_shares=3 AND status='OPEN')::text FROM position_lots WHERE lot_id='lot:fill-c'`); got != "true" {
		t.Fatal("the later YES lot opened after the NO fill and must not be touched")
	}
	assertLotsAddUp(t, database, account, "kalshi:KXNET-7:YES")
	assertLotsAddUp(t, database, account, "kalshi:KXNET-7:NO")
	assertAccount(t, database, account, "54", "54", "0")
}

func TestKalshiNettingRepairRefusesASettledMarketPostgresIntegration(t *testing.T) {
	database := kalshiSettlementDatabase(t)
	account := "kalshi:god92-settled"
	seedGod92Unnetted(t, database, account)
	settlements, err := NewKalshiSettlementStore(database, []string{account})
	if err != nil {
		t.Fatal(err)
	}
	// The unnetted shares disagree with the exchange record: this is the failure
	// the repair exists to prevent, and it parks the market for good.
	outcome, err := settlements.ApplyKalshiSettlement(context.Background(), account,
		kalshiRecord(god92Ticker, "yes", "1.00", "0.00", "1.00", "1.00"), time.Now().UTC())
	if err != nil || outcome.Result != domain.KalshiSettlementManualReview {
		t.Fatalf("settlement = %#v, %v, want MANUAL_REVIEW", outcome, err)
	}
	store, err := NewKalshiNettingRepairStore(database, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Run(context.Background(), god92RepairRequest(account)); err == nil || !strings.Contains(err.Error(), "settlement") {
		t.Fatalf("repair of a settled market error = %v", err)
	}
}

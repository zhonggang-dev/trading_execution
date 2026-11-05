package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// reviewState hashes the ledger and every table the review could touch.
func reviewState(t *testing.T, database *sql.DB) string {
	t.Helper()
	state := ledgerState(t, database)
	for _, table := range []string{"kalshi_settlements", "kalshi_settlement_lots", "kalshi_settlement_reviews"} {
		state += table + "=" + scalar(t, database, `SELECT COALESCE(md5(string_agg(row_text, '|' ORDER BY row_text)),'')
			FROM (SELECT t::text AS row_text FROM `+table+` t) rows`) + ";"
	}
	return state
}

func newKalshiReviewStore(t *testing.T, database *sql.DB) *KalshiSettlementReviewStore {
	t.Helper()
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	store, err := NewKalshiSettlementReviewStore(database, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// holdForReview runs the settlement sweep once so the record is parked in
// MANUAL_REVIEW, as production does when it disagrees with the ledger.
func holdForReview(t *testing.T, database *sql.DB, account string, record domain.KalshiSettlement) {
	t.Helper()
	store, err := NewKalshiSettlementStore(database, []string{account})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := store.ApplyKalshiSettlement(context.Background(), account, record, time.Date(2026, 11, 5, 7, 0, 0, 0, time.UTC))
	if err != nil || outcome.Result != domain.KalshiSettlementManualReview {
		t.Fatalf("settlement = %#v, %v, want MANUAL_REVIEW", outcome, err)
	}
}

func reviewRequest(account, ticker string, record domain.KalshiSettlement, cash string, fills ...KalshiReviewFill) KalshiSettlementReviewRequest {
	if fills == nil {
		fills = []KalshiReviewFill{}
	}
	return KalshiSettlementReviewRequest{
		ExecutionAccountID: account, Ticker: ticker, Actor: "ops", Reason: "late fill booked after the sweep",
		Evidence: KalshiSettlementReviewEvidence{
			Settlements: []domain.KalshiSettlement{record}, Fills: fills, PositionFP: "0", Cash: domain.Decimal(cash),
		},
	}
}

// lateFillReview is an account whose settlement was swept while one of its
// two YES fills was not booked yet; that fill has been booked since.
func lateFillReview(t *testing.T, account string) (*sql.DB, domain.KalshiSettlement, []KalshiReviewFill) {
	t.Helper()
	database := kalshiSettlementDatabase(t)
	seedKalshiSettlementAccount(t, database, account, "100")
	at := time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC)
	suffix := strings.TrimPrefix(account, "kalshi:")
	seedBookedKalshiBuy(t, database, account, "KXREV-1", "YES", "order-a-"+suffix, "fill-a-"+suffix, "6", "3", at)
	record := kalshiRecord("KXREV-1", "yes", "10", "0", "10", "1")
	holdForReview(t, database, account, record)
	seedBookedKalshiBuy(t, database, account, "KXREV-1", "YES", "order-b-"+suffix, "fill-b-"+suffix, "4", "2", at.Add(time.Hour))
	return database, record, []KalshiReviewFill{{FillID: "fill-a-" + suffix, Count: "6.00"}, {FillID: "fill-b-" + suffix, Count: "4"}}
}

func TestKalshiSettlementReviewUnblocksTheStartupSyncPostgresIntegration(t *testing.T) {
	ctx := context.Background()
	const account = "kalshi:review-late"
	database, record, fills := lateFillReview(t, account)
	exchange := &fakeKalshiExchange{balance: "110", settlements: map[string][]domain.KalshiSettlement{"KXREV-1": {record}}}
	if deferred, _, err := exchange.sync(ctx, database, account); err != nil || !deferred {
		t.Fatalf("startup sync before review = deferred %v err %v, want deferred", deferred, err)
	}
	store := newKalshiReviewStore(t, database)
	request := reviewRequest(account, "KXREV-1", record, "110", fills...)

	before := reviewState(t, database)
	dry, err := store.Run(ctx, request)
	if err != nil || dry.Applied || dry.Fingerprint == "" || dry.AlreadyApplied {
		t.Fatalf("dry run = %#v, %v", dry, err)
	}
	if !strings.Contains(dry.PreviousReason, "exchange held 10 YES but the ledger holds 6") ||
		!dry.Payout.Equal("10") || !dry.CashDifference.Equal("0") || len(dry.FillIDs) != 2 {
		t.Fatalf("dry plan = %#v", dry)
	}
	if after := reviewState(t, database); after != before {
		t.Fatal("the dry run wrote to the ledger")
	}
	if again, err := store.Run(ctx, request); err != nil || again.Fingerprint != dry.Fingerprint {
		t.Fatalf("second dry run fingerprint = %s, %v, want stable %s", again.Fingerprint, err, dry.Fingerprint)
	}
	wrong := request
	wrong.Apply, wrong.Confirmation = true, strings.Repeat("0", 64)
	if _, err := store.Run(ctx, wrong); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("wrong fingerprint = %v", err)
	}
	if after := reviewState(t, database); after != before {
		t.Fatal("a refused apply wrote to the ledger")
	}
	otherActor := request
	otherActor.Actor, otherActor.Apply, otherActor.Confirmation = "someone-else", true, dry.Fingerprint
	if _, err := store.Run(ctx, otherActor); err == nil {
		t.Fatal("the fingerprint must bind the actor of the dry run")
	}

	apply := request
	apply.Apply, apply.Confirmation = true, dry.Fingerprint
	applied, err := store.Run(ctx, apply)
	if err != nil || !applied.Applied {
		t.Fatalf("apply = %#v, %v", applied, err)
	}
	assertKalshiCash(t, database, account, "110")
	if got := scalar(t, database, `SELECT status||'/'||review_reason||'/'||local_yes_shares::text||'/'||realized_pnl::text
		FROM kalshi_settlements WHERE execution_account_id=$1 AND ticker='KXREV-1'`, account); got != "APPLIED//10/5" {
		t.Fatalf("settlement row = %s", got)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM position_lots
		WHERE execution_account_id=$1 AND status='OPEN'`, account); got != "0" {
		t.Fatalf("open lots = %s, want all closed", got)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_account_events
		WHERE execution_account_id=$1 AND event_type='KALSHI_SETTLED'`, account); got != "1" {
		t.Fatalf("settlement events = %s, want 1", got)
	}
	if got := scalar(t, database, `SELECT previous_reason||'|'||actor||'|'||reason||'|'||fingerprint
		FROM kalshi_settlement_reviews WHERE execution_account_id=$1 AND ticker='KXREV-1'`, account); got !=
		dry.PreviousReason+"|ops|late fill booked after the sweep|"+dry.Fingerprint {
		t.Fatalf("review audit = %s", got)
	}

	if deferred, balance, err := exchange.sync(ctx, database, account); err != nil || deferred || !balance.Equal("110") {
		t.Fatalf("startup sync after review = deferred %v %s err %v, want not deferred", deferred, balance, err)
	}
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_account_events
		WHERE execution_account_id=$1 AND event_type='KALSHI_BALANCE_SYNC'`, account); got != "0" {
		t.Fatalf("balance syncs = %s, the payout must not be booked twice", got)
	}
	settled := reviewState(t, database)
	repeat, err := store.Run(ctx, apply)
	if err != nil || !repeat.AlreadyApplied || repeat.Applied {
		t.Fatalf("repeat = %#v, %v, want already applied", repeat, err)
	}
	if reviewState(t, database) != settled {
		t.Fatal("a repeated review changed the ledger")
	}
	for _, statement := range []string{
		`UPDATE kalshi_settlement_reviews SET actor='x'`, `DELETE FROM kalshi_settlement_reviews`, `TRUNCATE kalshi_settlement_reviews`,
	} {
		if _, err := database.Exec(statement); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("%s: err = %v, want append-only rejection", statement, err)
		}
	}
}

// A market the ledger never netted settles after the netting is replayed.
func TestKalshiSettlementReviewReplaysDeferredNettingPostgresIntegration(t *testing.T) {
	ctx := context.Background()
	const account = "kalshi:review-net"
	database := kalshiSettlementDatabase(t)
	seedKalshiSettlementAccount(t, database, account, "100")
	at := time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC)
	seedBookedKalshiBuy(t, database, account, "KXRNET-1", "YES", "rnet-yes", "rnet-fill-yes", "10", "4", at)
	seedBookedKalshiBuy(t, database, account, "KXRNET-1", "NO", "rnet-no", "rnet-fill-no", "4", "2.4", at.Add(time.Hour))
	// The exchange netted 4 pairs (+4) and settled the remaining 6 YES (+6).
	record := kalshiRecord("KXRNET-1", "yes", "6", "0", "6", "1")
	holdForReview(t, database, account, record)
	store := newKalshiReviewStore(t, database)
	request := reviewRequest(account, "KXRNET-1", record, "110",
		KalshiReviewFill{FillID: "rnet-fill-yes", Count: "10"}, KalshiReviewFill{FillID: "rnet-fill-no", Count: "4"})
	dry, err := store.Run(ctx, request)
	if err != nil || len(dry.NettingClosures) == 0 || !dry.AfterNetting.Yes.Shares.Equal("6") || !dry.AfterNetting.No.Shares.Equal("0") {
		t.Fatalf("dry run = %#v, %v", dry, err)
	}
	request.Apply, request.Confirmation = true, dry.Fingerprint
	if applied, err := store.Run(ctx, request); err != nil || !applied.Applied {
		t.Fatalf("apply = %#v, %v", applied, err)
	}
	assertKalshiCash(t, database, account, "110")
	if got := scalar(t, database, `SELECT COUNT(*)::text FROM execution_account_events
		WHERE execution_account_id=$1 AND event_type='KALSHI_NETTED'`, account); got != "1" {
		t.Fatalf("netting events = %s, want 1", got)
	}
}

func TestKalshiSettlementReviewRefusesIncompleteEvidencePostgresIntegration(t *testing.T) {
	ctx := context.Background()
	const account = "kalshi:review-refuse"
	database, record, fills := lateFillReview(t, account)
	store := newKalshiReviewStore(t, database)
	before := reviewState(t, database)
	refuse := func(name, want string, mutate func(*KalshiSettlementReviewRequest)) {
		t.Helper()
		request := reviewRequest(account, "KXREV-1", record, "110", fills...)
		mutate(&request)
		if _, err := store.Run(ctx, request); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err = %v, want %q", name, err, want)
		}
		if reviewState(t, database) != before {
			t.Fatalf("%s: a refused review wrote to the ledger", name)
		}
	}
	refuse("missing exchange fill", "missing locally [], not on the exchange [fill-b-review-refuse]", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.Fills = fills[:1]
	})
	refuse("unknown exchange fill", "missing locally [fill-x]", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.Fills = append(append([]KalshiReviewFill{}, fills...), KalshiReviewFill{FillID: "fill-x", Count: "1"})
	})
	refuse("different count", "different counts [fill-b-review-refuse", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.Fills = []KalshiReviewFill{fills[0], {FillID: fills[1].FillID, Count: "3"}}
	})
	refuse("exchange still holds", "must be flat", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.PositionFP = "4"
	})
	refuse("cash disagrees", "after the review", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.Cash = "111"
	})
	refuse("non-binary result", "non-binary", func(request *KalshiSettlementReviewRequest) {
		scalarRecord := kalshiRecord("KXREV-1", "scalar", "10", "0", "4.2", "0.42")
		request.Evidence.Settlements = []domain.KalshiSettlement{scalarRecord}
	})
	refuse("void result", "non-binary", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.Settlements = []domain.KalshiSettlement{kalshiRecord("KXREV-1", "void", "10", "0", "5", "")}
	})
	refuse("contradicting value", "contradicts market result", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.Settlements = []domain.KalshiSettlement{kalshiRecord("KXREV-1", "yes", "10", "0", "10", "0")}
	})
	refuse("two records", "want exactly one", func(request *KalshiSettlementReviewRequest) {
		request.Evidence.Settlements = []domain.KalshiSettlement{record, record}
	})
	refuse("no actor", "actor", func(request *KalshiSettlementReviewRequest) { request.Actor = " " })
	refuse("wide tolerance", "must not exceed", func(request *KalshiSettlementReviewRequest) { request.CashTolerance = "0.02" })

	if _, err := database.Exec(`
		INSERT INTO asset_reservations (
			order_id, client_order_id, intent_fingerprint, execution_account_id, strategy_id, market_id, token_id,
			side, requested_shares, reserve_unit_price, initial_reserved_balance, remaining_reserved_balance, status
		) VALUES ('review-hold','review-hold','fp',$1,'strategy','KXREV-1','kalshi:KXREV-1:NO','BUY',1,0.5,0.5,0.5,'ACTIVE')`, account); err != nil {
		t.Fatal(err)
	}
	before = reviewState(t, database)
	refuse("active reservation", "active reservation", func(*KalshiSettlementReviewRequest) {})

	// A market without a settlement row has nothing to review.
	if _, err := store.Run(ctx, reviewRequest(account, "KXNONE-1", kalshiRecord("KXNONE-1", "yes", "1", "0", "1", "1"), "110")); err == nil ||
		!strings.Contains(err.Error(), "no settlement row") {
		t.Fatalf("missing row = %v", err)
	}
}

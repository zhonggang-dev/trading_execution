package kalshirepair

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestRunDryRunRequiresTerminalOrderAndEmptyFills(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	local := repairLocalState()
	evidence := repairEvidence(now)
	store := &fakeStore{local: local}
	source := &fakeEvidenceSource{evidence: evidence}
	service := newTestService(t, store, source, now)

	result, err := service.Run(context.Background(), Request{
		ExecutionAccountID: "wallet-7", OrderID: local.Order.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.DryRun || !result.Eligible || result.Applied || store.applyCalls != 0 {
		t.Fatalf("dry-run result=%#v apply calls=%d", result, store.applyCalls)
	}
	if result.AuthoritativeID != evidence.OrderID || result.ReservedBalance != "10.09" || len(source.orders) != 1 {
		t.Fatalf("dry-run evidence/result=%#v source orders=%#v", result, source.orders)
	}
}

func TestNewRejectsFinalityGraceBelowHardMinimum(t *testing.T) {
	_, err := New(Params{
		Store: &fakeStore{}, Evidence: &fakeEvidenceSource{}, FinalityGrace: 29 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "30s") {
		t.Fatalf("New() error=%v, want 30s hard minimum", err)
	}
}

func TestRunAcceptsTheSameCanonicalKalshiIdentityUsedByLiveSerialization(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		side        domain.Side
		outcomeID   string
		worstPrice  domain.Decimal
		outcomeSide string
		bookSide    string
		orderPrice  domain.Decimal
	}{
		{name: "buy yes", side: domain.SideBuy, outcomeID: "YES", worstPrice: "0.27", outcomeSide: "yes", bookSide: "bid", orderPrice: "0.27"},
		{name: "buy no", side: domain.SideBuy, outcomeID: "NO", worstPrice: "0.27", outcomeSide: "no", bookSide: "ask", orderPrice: "0.7300"},
		{name: "sell yes", side: domain.SideSell, outcomeID: "YES", worstPrice: "0.62", outcomeSide: "no", bookSide: "ask", orderPrice: "0.62"},
		{name: "sell no", side: domain.SideSell, outcomeID: "NO", worstPrice: "0.62", outcomeSide: "yes", bookSide: "bid", orderPrice: "0.3800"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			local := repairLocalState()
			local.Order.Intent.Side = testCase.side
			local.Order.Intent.OutcomeID = testCase.outcomeID
			local.Order.Intent.WorstPrice = testCase.worstPrice
			if testCase.side == domain.SideSell {
				local.RemainingReservedBalance = "0"
				local.RemainingReservedShares = local.Order.Intent.Size
			}
			evidence := repairEvidence(now)
			evidence.OutcomeSide = testCase.outcomeSide
			evidence.Action = strings.ToLower(string(testCase.side))
			evidence.BookSide = testCase.bookSide
			evidence.OrderPrice = testCase.orderPrice
			service := newTestService(t, &fakeStore{local: local}, &fakeEvidenceSource{evidence: evidence}, now)
			result, err := service.Run(context.Background(), Request{ExecutionAccountID: "wallet-7", OrderID: local.Order.ID})
			if err != nil || !result.Eligible || !result.DryRun {
				t.Fatalf("result=%#v error=%v", result, err)
			}
		})
	}
}

func TestRunDoesNotDependOnDeprecatedKalshiAction(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	for _, action := range []string{"", "sell"} {
		local := repairLocalState()
		evidence := repairEvidence(now)
		evidence.Action = action
		evidence.CancelOnPause = nil
		evidence.SelfTradePolicy = ""
		service := newTestService(t, &fakeStore{local: local}, &fakeEvidenceSource{evidence: evidence}, now)
		result, err := service.Run(context.Background(), Request{ExecutionAccountID: "wallet-7", OrderID: local.Order.ID})
		if err != nil || !result.Eligible {
			t.Fatalf("deprecated action %q changed canonical validation: result=%#v error=%v", action, result, err)
		}
	}
}

func TestEvidenceFingerprintIgnoresDeprecatedActionAndOptionalTimeInForce(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	first := repairEvidence(now)
	second := first
	second.Action = ""
	second.TimeInForce = "fill_or_kill"
	second.CancelOnPause = nil
	second.SelfTradePolicy = ""
	firstFingerprint, err := first.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, err := second.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint != secondFingerprint {
		t.Fatalf("optional/deprecated response fields changed repair fingerprint: %s != %s", firstFingerprint, secondFingerprint)
	}
}

func TestRunApplyRequiresExactConfirmationAndIsIdempotent(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	local := repairLocalState()
	evidence := repairEvidence(now)
	store := &fakeStore{local: local, applyResult: true}
	service := newTestService(t, store, &fakeEvidenceSource{evidence: evidence}, now)

	_, err := service.Run(context.Background(), Request{
		ExecutionAccountID: "wallet-7", OrderID: local.Order.ID, Apply: true,
	})
	if err == nil || !strings.Contains(err.Error(), "confirmation") || store.applyCalls != 0 {
		t.Fatalf("missing confirmation error=%v apply calls=%d", err, store.applyCalls)
	}
	result, err := service.Run(context.Background(), Request{
		ExecutionAccountID: "wallet-7", OrderID: local.Order.ID, Apply: true,
		Confirmation: "wallet-7/" + local.Order.ID,
	})
	if err != nil || !result.Applied || result.DryRun || store.applyCalls != 1 {
		t.Fatalf("apply result=%#v error=%v calls=%d", result, err, store.applyCalls)
	}

	fingerprint, err := evidence.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	store.local.Order.Status = domain.OrderStatusCancelled
	store.local.ReservationStatus = domain.ReservationStatusReleased
	store.local.RemainingReservedBalance = "0"
	store.local.RepairFingerprint = fingerprint
	result, err = service.Run(context.Background(), Request{
		ExecutionAccountID: "wallet-7", OrderID: local.Order.ID, Apply: true,
		Confirmation: "wallet-7/" + local.Order.ID,
	})
	if err != nil || !result.AlreadyApplied || result.Applied || store.applyCalls != 1 {
		t.Fatalf("idempotent result=%#v error=%v calls=%d", result, err, store.applyCalls)
	}
}

func TestRunFailsClosedWithoutCompleteAuthoritativeEvidence(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*Evidence)
		want   string
	}{
		{name: "live order", mutate: func(value *Evidence) { value.Status = "resting" }, want: "confirmed cancellation"},
		{name: "venue fill count", mutate: func(value *Evidence) { value.FillCount = "1" }, want: "fill_count must be zero"},
		{name: "fills endpoint", mutate: func(value *Evidence) { value.FillIDs = []string{"fill-1"} }, want: "no-fill repair is forbidden"},
		{name: "wrong client", mutate: func(value *Evidence) { value.ClientOrderID = "other" }, want: "identity"},
		{name: "wrong market", mutate: func(value *Evidence) { value.MarketID = "other" }, want: "identity"},
		{name: "wrong size", mutate: func(value *Evidence) { value.InitialCount = "19" }, want: "size"},
		{name: "missing fill count", mutate: func(value *Evidence) { value.FillCount = "" }, want: "fill_count is missing"},
		{name: "missing remaining count", mutate: func(value *Evidence) { value.RemainingCount = "" }, want: "remaining_count is missing"},
		{name: "missing fill result", mutate: func(value *Evidence) { value.FillIDs = nil }, want: "fills query result is missing"},
		{name: "wrong outcome", mutate: func(value *Evidence) { value.OutcomeSide = "NO" }, want: "canonical outcome"},
		{name: "wrong book side", mutate: func(value *Evidence) { value.BookSide = "ask" }, want: "canonical outcome"},
		{name: "wrong type", mutate: func(value *Evidence) { value.OrderType = "market" }, want: "canonical outcome"},
		{name: "wrong tif", mutate: func(value *Evidence) { value.TimeInForce = "immediate_or_cancel" }, want: "time_in_force"},
		{name: "wrong price", mutate: func(value *Evidence) { value.OrderPrice = "0.51" }, want: "order price"},
		{name: "wrong subaccount", mutate: func(value *Evidence) { value.SubaccountNumber = intPointer(1) }, want: "subaccount"},
		{name: "missing order provenance", mutate: func(value *Evidence) { value.OrderQuerySource = "" }, want: "provenance"},
		{name: "within finality grace", mutate: func(value *Evidence) { value.LastUpdatedAt = now.Add(-10 * time.Second) }, want: "finality grace"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			local := repairLocalState()
			evidence := repairEvidence(now)
			testCase.mutate(&evidence)
			store := &fakeStore{local: local, applyResult: true}
			service := newTestService(t, store, &fakeEvidenceSource{evidence: evidence}, now)
			_, err := service.Run(context.Background(), Request{
				ExecutionAccountID: "wallet-7", OrderID: local.Order.ID, Apply: true,
				Confirmation: "wallet-7/" + local.Order.ID,
			})
			if err == nil || !strings.Contains(err.Error(), testCase.want) || store.applyCalls != 0 {
				t.Fatalf("error=%v, want %q; apply calls=%d", err, testCase.want, store.applyCalls)
			}
		})
	}
}

func TestRunRejectsUnsafeLocalScopeAndReservation(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*LocalState)
		want   string
	}{
		{name: "wrong account", mutate: func(value *LocalState) { value.Order.Intent.ExecutionAccountID = "other" }, want: "scope"},
		{name: "not manual", mutate: func(value *LocalState) { value.Order.Status = domain.OrderStatusUnknown }, want: "not MANUAL_REVIEW"},
		{name: "active reservation", mutate: func(value *LocalState) { value.ReservationStatus = domain.ReservationStatusActive }, want: "not RECONCILIATION_REQUIRED"},
		{name: "no held balance", mutate: func(value *LocalState) { value.RemainingReservedBalance = "0" }, want: "unsettled collateral"},
		{name: "local fill", mutate: func(value *LocalState) { value.Order.FilledSize = "1" }, want: "local fills"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			local := repairLocalState()
			testCase.mutate(&local)
			store := &fakeStore{local: local}
			source := &fakeEvidenceSource{evidence: repairEvidence(now)}
			service := newTestService(t, store, source, now)
			_, err := service.Run(context.Background(), Request{ExecutionAccountID: "wallet-7", OrderID: local.Order.ID})
			if err == nil || !strings.Contains(err.Error(), testCase.want) || len(source.orders) != 0 {
				t.Fatalf("error=%v, want %q; evidence calls=%d", err, testCase.want, len(source.orders))
			}
		})
	}
}

func newTestService(t *testing.T, store Store, source EvidenceSource, now time.Time) *Service {
	t.Helper()
	service, err := New(Params{Store: store, Evidence: source, FinalityGrace: 30 * time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func repairLocalState() LocalState {
	return LocalState{
		Order: domain.Order{
			ID: "order-legacy", VenueOrderID: "client-legacy", Status: domain.OrderStatusManualReview,
			FilledSize: "0", FilledNotional: "0", TotalFees: "0",
			Intent: domain.OrderIntent{
				ModelID: "gemini_masked", StrategyID: "multfactor_v2", ExecutionAccountID: "wallet-7",
				SignalID: "signal-legacy", ClientOrderID: "client-legacy", Venue: "kalshi",
				MarketSource: domain.MarketSourceKalshi, MarketID: "KXTEST-YES", ConditionID: "kalshi:KXTEST-YES",
				OutcomeID: "YES", TokenID: "kalshi:KXTEST-YES:YES", Side: domain.SideBuy,
				Type: domain.OrderTypeLimit, Price: "0.5", WorstPrice: "0.5", Size: "20", TimeInForce: domain.TimeInForceFOK,
			},
		},
		ReservationStatus:        domain.ReservationStatusReconciliationRequired,
		RemainingReservedBalance: "10.09", RemainingReservedShares: "0",
	}
}

func repairEvidence(now time.Time) Evidence {
	cancelOnPause := true
	subaccount := 0
	return Evidence{
		OrderID: "01-authoritative", ClientOrderID: "client-legacy", MarketID: "KXTEST-YES",
		OutcomeSide: "YES", Action: "buy", BookSide: "bid", OrderType: "limit",
		OrderPrice: "0.5", SelfTradePolicy: "taker_at_cross",
		CancelOnPause: &cancelOnPause, SubaccountNumber: &subaccount,
		Status: "canceled", FillCount: "0", RemainingCount: "0", InitialCount: "20", FillIDs: []string{},
		LastUpdatedAt: now.Add(-time.Hour), ObservedAt: now,
		OrderQuerySource: "KALSHI_ORDER_BY_CLIENT_THEN_ORDER_ID", FillQuerySource: "KALSHI_FILLS_BY_ORDER_ID",
	}
}

func intPointer(value int) *int { return &value }

type fakeStore struct {
	local       LocalState
	loadErr     error
	applyErr    error
	applyResult bool
	applyCalls  int
	params      []ApplyParams
}

func (store *fakeStore) Load(context.Context, string, string) (LocalState, error) {
	return store.local, store.loadErr
}

func (store *fakeStore) ApplyCancelled(_ context.Context, params ApplyParams) (bool, error) {
	store.applyCalls++
	store.params = append(store.params, params)
	return store.applyResult, store.applyErr
}

type fakeEvidenceSource struct {
	evidence Evidence
	err      error
	orders   []domain.Order
}

func (source *fakeEvidenceSource) Inspect(_ context.Context, order domain.Order) (Evidence, error) {
	source.orders = append(source.orders, order)
	return source.evidence, source.err
}

package strategyreport

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func order(id, status string, revision int64, mutate func(*domain.Order)) domain.Order {
	value := domain.Order{
		ID: id, Status: domain.OrderStatus(status), Revision: revision, FilledSize: "0",
		UpdatedAt: time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC),
		Intent: domain.OrderIntent{
			ModelID: "deepseek_masked", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main",
			SignalID: "entry-" + id, ConditionID: "0xcond", TokenID: "tok", Side: domain.SideBuy,
		},
	}
	if mutate != nil {
		mutate(&value)
	}
	return value
}

func TestReportFromOrderMapsStatuses(t *testing.T) {
	filled := func(o *domain.Order) {
		o.FilledSize, o.AverageFillPrice, o.TotalFees = "18", "0.53", "0.01"
	}
	cases := []struct {
		name   string
		order  domain.Order
		want   domain.StrategyReportStatus
		wantOK bool
	}{
		{"pre-exchange states are not reported", order("a", "RESERVED", 3, nil), "", false},
		{"submitting is not reported", order("a", "SUBMITTING", 3, nil), "", false},
		{"acknowledged", order("a", "ACKNOWLEDGED", 3, nil), domain.StrategyReportSubmitted, true},
		{"live", order("a", "LIVE", 3, nil), domain.StrategyReportOpen, true},
		{"live with fill", order("a", "LIVE", 3, filled), domain.StrategyReportPartiallyFilled, true},
		{"partial without fill", order("a", "PARTIALLY_FILLED", 3, nil), domain.StrategyReportOpen, true},
		{"filled", order("a", "FILLED", 4, filled), domain.StrategyReportFilled, true},
		{"filled without fill is contradictory", order("a", "FILLED", 4, nil), domain.StrategyReportUnknown, true},
		{"cancelled", order("a", "CANCELLED", 4, nil), domain.StrategyReportCancelled, true},
		{"cancelled after partial fill", order("a", "CANCELLED", 5, filled), domain.StrategyReportCancelled, true},
		{"rejected before the exchange", order("a", "REJECTED", 2, func(o *domain.Order) {
			o.FailureCode = "INSUFFICIENT_WALLET_BALANCE"
		}), domain.StrategyReportNotSubmitted, true},
		{"rejected by the exchange", order("a", "REJECTED", 2, func(o *domain.Order) { o.VenueOrderID = "0xabc" }), domain.StrategyReportRejected, true},
		{"unknown", order("a", "UNKNOWN", 3, nil), domain.StrategyReportUnknown, true},
		{"manual review", order("a", "MANUAL_REVIEW", 3, nil), domain.StrategyReportUnknown, true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			report, ok := ReportFromOrder(test.order)
			if ok != test.wantOK || (ok && report.Status != test.want) {
				t.Fatalf("ReportFromOrder() = %#v, %v; want %s, %v", report, ok, test.want, test.wantOK)
			}
			if ok && (report.ReportSeq != test.order.Revision || report.DecisionID != "entry-a" ||
				report.Context.ExecutionAccountID != "main" || report.CumulativeFilledShares.IsEmpty()) {
				t.Fatalf("report identity = %#v", report)
			}
		})
	}
}

func TestReportFromOrderCarriesFillDetailsAndFailure(t *testing.T) {
	report, ok := ReportFromOrder(order("a", "FILLED", 4, func(o *domain.Order) {
		o.FilledSize, o.AverageFillPrice, o.TotalFees, o.VenueOrderID = "18", "0.53", "0.01", "0xabc"
	}))
	if !ok || report.CumulativeFilledShares != "18" || report.AverageFillPrice != "0.53" || report.FeesUSD != "0.01" || report.VenueOrderID != "0xabc" {
		t.Fatalf("filled report = %#v, %v", report, ok)
	}
	// The average price is required once something filled; derive it when missing.
	report, ok = ReportFromOrder(order("a", "FILLED", 4, func(o *domain.Order) {
		o.FilledSize, o.FilledNotional = "10", "5"
	}))
	if !ok || report.AverageFillPrice == "" {
		t.Fatalf("derived price report = %#v, %v", report, ok)
	}
	if _, ok := ReportFromOrder(order("a", "FILLED", 4, func(o *domain.Order) { o.FilledSize = "10" })); ok {
		t.Fatal("a fill without any price evidence must not be reported")
	}
	if _, ok := ReportFromOrder(order("a", "FILLED", 4, func(o *domain.Order) { o.FilledSize = "not-a-number" })); ok {
		t.Fatal("an undecodable fill must not be reported")
	}
	report, _ = ReportFromOrder(order("a", "REJECTED", 2, func(o *domain.Order) {
		o.FailureCode, o.FailureReason = "PRICE_DRIFT", strings.Repeat("x", 900)
	}))
	if report.FailureCode != "PRICE_DRIFT" || len(report.FailureReason) != 500 {
		t.Fatalf("failure fields = %q (%d)", report.FailureCode, len(report.FailureReason))
	}
}

type fakeOrders struct {
	orders []domain.Order
	err    error
}

func (source *fakeOrders) ListUpdatedSince(context.Context, []string, time.Time, int) ([]domain.Order, error) {
	return source.orders, source.err
}

type fakeSink struct {
	batches [][]domain.StrategyOrderReport
	err     error
	errCode map[string]string // decision_id -> per-report error code
}

func (sink *fakeSink) ReportOrders(_ context.Context, reports []domain.StrategyOrderReport) ([]domain.StrategyOrderReportResult, error) {
	if sink.err != nil {
		return nil, sink.err
	}
	sink.batches = append(sink.batches, reports)
	results := make([]domain.StrategyOrderReportResult, len(reports))
	for index, report := range reports {
		results[index] = domain.StrategyOrderReportResult{DecisionID: report.DecisionID, ReportSeq: report.ReportSeq, Outcome: "applied", ErrorCode: sink.errCode[report.DecisionID]}
	}
	return results, nil
}

func newReporter(t *testing.T, source OrderSource, sink Sink) *Reporter {
	t.Helper()
	reporter, err := New(Params{
		Orders: source, Sink: sink, StrategyIDs: []string{"brier_mid_edge_once"}, AccountIDs: []string{"main"},
		Now: func() time.Time { return time.Date(2026, 10, 23, 2, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return reporter
}

func TestSyncReportsOnlyChangedOrdersOfConfiguredStrategies(t *testing.T) {
	other := order("other", "REJECTED", 2, func(o *domain.Order) { o.Intent.StrategyID = "multfactor_v1" })
	sell := order("sell", "FILLED", 2, func(o *domain.Order) { o.Intent.Side = domain.SideSell })
	source := &fakeOrders{orders: []domain.Order{
		order("one", "REJECTED", 2, nil), order("two", "LIVE", 3, nil), other, sell,
	}}
	sink := &fakeSink{}
	reporter := newReporter(t, source, sink)

	if err := reporter.Sync(context.Background()); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 2 {
		t.Fatalf("first pass batches = %#v, want one batch of the two BUY orders of the strategy", sink.batches)
	}
	// Nothing changed: nothing is sent.
	if err := reporter.Sync(context.Background()); err != nil || len(sink.batches) != 1 {
		t.Fatalf("second pass error = %v, batches = %d", err, len(sink.batches))
	}
	// "two" moves on (new revision): only it is sent again.
	source.orders[1] = order("two", "FILLED", 5, func(o *domain.Order) { o.FilledSize, o.AverageFillPrice = "3", "0.4" })
	if err := reporter.Sync(context.Background()); err != nil || len(sink.batches) != 2 {
		t.Fatalf("third pass error = %v, batches = %d", err, len(sink.batches))
	}
	if got := sink.batches[1]; len(got) != 1 || got[0].DecisionID != "entry-two" || got[0].ReportSeq != 5 || got[0].Status != domain.StrategyReportFilled {
		t.Fatalf("third pass batch = %#v", got)
	}
}

func TestSyncRetriesWholeRequestFailureButNotRejectedReports(t *testing.T) {
	source := &fakeOrders{orders: []domain.Order{order("one", "REJECTED", 2, nil), order("two", "REJECTED", 2, nil)}}
	sink := &fakeSink{err: errors.New("strategy HTTP 503")}
	reporter := newReporter(t, source, sink)
	if err := reporter.Sync(context.Background()); err == nil {
		t.Fatal("Sync() with a failing sink returned nil")
	}
	sink.err = nil
	sink.errCode = map[string]string{"entry-two": "REPORT_CONFLICT"}
	if err := reporter.Sync(context.Background()); err != nil || len(sink.batches) != 1 || len(sink.batches[0]) != 2 {
		t.Fatalf("retry error = %v, batches = %#v", err, sink.batches)
	}
	// The rejected report is not resent forever.
	if err := reporter.Sync(context.Background()); err != nil || len(sink.batches) != 1 {
		t.Fatalf("pass after a per-report rejection error = %v, batches = %d", err, len(sink.batches))
	}
}

func TestSyncSplitsLargeBatches(t *testing.T) {
	source := &fakeOrders{}
	for index := range 1201 {
		source.orders = append(source.orders, order(string(rune('a'+index%26))+strings.Repeat("z", index/26), "REJECTED", 2, nil))
	}
	sink := &fakeSink{}
	if err := newReporter(t, source, sink).Sync(context.Background()); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if len(sink.batches) != 3 || len(sink.batches[0]) != 500 || len(sink.batches[2]) != 201 {
		t.Fatalf("batch sizes = %d", len(sink.batches))
	}
}

func TestReportNotSubmittedBuildsTerminalZeroFillReports(t *testing.T) {
	sink := &fakeSink{}
	reporter := newReporter(t, &fakeOrders{}, sink)
	execution := domain.StrategyExecutionContext{ModelID: "deepseek_masked", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main"}
	reporter.ReportNotSubmitted(context.Background(), execution, []domain.StrategyUndeliveredDecision{
		{DecisionID: "entry-1", ConditionID: "0xc1", TokenID: "t1"},
		{DecisionID: "entry-2", ConditionID: "0xc2", TokenID: "t2"},
	}, "STRATEGY_RESPONSE_REJECTED", "bad response")
	if len(sink.batches) != 1 || len(sink.batches[0]) != 2 {
		t.Fatalf("batches = %#v", sink.batches)
	}
	got := sink.batches[0][0]
	if got.Status != domain.StrategyReportNotSubmitted || got.ReportSeq != 1 || got.CumulativeFilledShares != "0" ||
		got.FailureCode != "STRATEGY_RESPONSE_REJECTED" || got.Context != execution || got.DecisionID != "entry-1" {
		t.Fatalf("report = %#v", got)
	}
	// Other strategies are never reported to the service.
	other := execution
	other.StrategyID = "multfactor_v1"
	reporter.ReportNotSubmitted(context.Background(), other, []domain.StrategyUndeliveredDecision{{DecisionID: "x"}}, "C", "r")
	if len(sink.batches) != 1 {
		t.Fatalf("a non-configured strategy was reported: %#v", sink.batches)
	}
}

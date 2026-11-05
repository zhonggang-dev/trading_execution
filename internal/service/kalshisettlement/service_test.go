package kalshisettlement

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type fakeSource struct {
	records      map[string][]domain.KalshiSettlement
	listErr      map[string]error
	balance      domain.Decimal
	balanceErr   error
	positions    []domain.KalshiMarketPosition
	positionsErr error
	listed       []string
	calls        []string
}

func (source *fakeSource) ListSettlements(_ context.Context, ticker string) ([]domain.KalshiSettlement, error) {
	source.listed = append(source.listed, ticker)
	source.calls = append(source.calls, "settlements:"+ticker)
	return source.records[ticker], source.listErr[ticker]
}

func (source *fakeSource) AvailableBalance(context.Context) (domain.Decimal, error) {
	source.calls = append(source.calls, "balance")
	return source.balance, source.balanceErr
}

func (source *fakeSource) ListPositions(context.Context) ([]domain.KalshiMarketPosition, error) {
	source.calls = append(source.calls, "positions")
	return source.positions, source.positionsErr
}

type fakeStore struct {
	tickers  []string
	outcomes map[string]domain.KalshiSettlementOutcome
	applyErr error
	applied  []string
	checks   []domain.Decimal
	check    domain.KalshiBalanceCheck
	snapshot domain.KalshiLedgerSnapshot
	syncs    []domain.KalshiLedgerObservation
	result   domain.KalshiLedgerSyncResult
}

func (store *fakeStore) LedgerSnapshot(context.Context, string) (domain.KalshiLedgerSnapshot, error) {
	return store.snapshot, nil
}

func (store *fakeStore) SyncKalshiLedger(_ context.Context, _ string, observation domain.KalshiLedgerObservation) (domain.KalshiLedgerSyncResult, error) {
	store.syncs = append(store.syncs, observation)
	return store.result, nil
}

func (store *fakeStore) OpenKalshiSettlementTickers(context.Context, string) ([]string, error) {
	return store.tickers, nil
}

func (store *fakeStore) ApplyKalshiSettlement(_ context.Context, _ string, settlement domain.KalshiSettlement, _ time.Time) (domain.KalshiSettlementOutcome, error) {
	store.applied = append(store.applied, settlement.Ticker)
	return store.outcomes[settlement.Ticker], store.applyErr
}

func (store *fakeStore) CheckKalshiBalance(_ context.Context, _ string, exchange, _ domain.Decimal, _ time.Time) (domain.KalshiBalanceCheck, error) {
	store.checks = append(store.checks, exchange)
	return store.check, nil
}

func newService(t *testing.T, source *fakeSource, store *fakeStore) *Service {
	t.Helper()
	service, err := New(Params{ExecutionAccountID: "kalshi:main", Source: source, Store: store,
		Interval: time.Minute, BalanceDriftThreshold: "0.01"})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestSweepAppliesOnlySettledMarketsThenChecksBalance(t *testing.T) {
	source := &fakeSource{
		records: map[string][]domain.KalshiSettlement{"KX-DONE": {{Ticker: "KX-DONE", MarketResult: "yes"}}},
		balance: "98.02",
	}
	store := &fakeStore{
		tickers:  []string{"KX-OPEN", "KX-DONE"},
		outcomes: map[string]domain.KalshiSettlementOutcome{"KX-DONE": {Result: domain.KalshiSettlementApplied, Payout: "10"}},
	}
	if err := newService(t, source, store).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(source.listed, ",") != "KX-OPEN,KX-DONE" {
		t.Fatalf("listed = %v, want every held market asked", source.listed)
	}
	if strings.Join(store.applied, ",") != "KX-DONE" {
		t.Fatalf("applied = %v, want only the settled market", store.applied)
	}
	if len(store.checks) != 1 || store.checks[0] != "98.02" {
		t.Fatalf("balance checks = %v, want one against 98.02 after settlement", store.checks)
	}
}

func TestSweepNeverAppliesAmbiguousSettlementRecords(t *testing.T) {
	source := &fakeSource{
		records: map[string][]domain.KalshiSettlement{"KX-TWO": {{Ticker: "KX-TWO"}, {Ticker: "KX-TWO"}}},
		balance: "10",
	}
	store := &fakeStore{tickers: []string{"KX-TWO"}}
	err := newService(t, source, store).Sweep(context.Background())
	if err == nil || !strings.Contains(err.Error(), "2 settlement records") {
		t.Fatalf("Sweep() error = %v, want ambiguity reported", err)
	}
	if len(store.applied) != 0 {
		t.Fatalf("applied = %v, want nothing applied for an ambiguous record", store.applied)
	}
	if len(store.checks) != 1 {
		t.Fatal("a failing market must not stop the balance check")
	}
}

func TestSweepKeepsGoingAfterOneMarketFails(t *testing.T) {
	source := &fakeSource{
		records: map[string][]domain.KalshiSettlement{"KX-OK": {{Ticker: "KX-OK"}}},
		listErr: map[string]error{"KX-BAD": errors.New("rate limited")},
		balance: "1",
	}
	store := &fakeStore{
		tickers:  []string{"KX-BAD", "KX-OK"},
		outcomes: map[string]domain.KalshiSettlementOutcome{"KX-OK": {Result: domain.KalshiSettlementApplied}},
	}
	err := newService(t, source, store).Sweep(context.Background())
	if err == nil || !strings.Contains(err.Error(), "KX-BAD") {
		t.Fatalf("Sweep() error = %v, want the failed market named", err)
	}
	if strings.Join(store.applied, ",") != "KX-OK" {
		t.Fatalf("applied = %v, want the healthy market still applied", store.applied)
	}
}

func TestSweepReportsExchangeBalanceFailure(t *testing.T) {
	source := &fakeSource{balanceErr: errors.New("unavailable")}
	if err := newService(t, source, &fakeStore{}).Sweep(context.Background()); err == nil || !strings.Contains(err.Error(), "balance check") {
		t.Fatalf("Sweep() error = %v, want balance failure reported", err)
	}
}

func TestNewRejectsUnsafeParameters(t *testing.T) {
	good := Params{ExecutionAccountID: "kalshi:main", Source: &fakeSource{}, Store: &fakeStore{},
		Interval: time.Minute, BalanceDriftThreshold: "0.01"}
	for name, mutate := range map[string]func(*Params){
		"polymarket account": func(p *Params) { p.ExecutionAccountID = "main" },
		"no source":          func(p *Params) { p.Source = nil },
		"no store":           func(p *Params) { p.Store = nil },
		"interval too short": func(p *Params) { p.Interval = time.Second },
		"interval too long":  func(p *Params) { p.Interval = 2 * time.Hour },
		"zero threshold":     func(p *Params) { p.BalanceDriftThreshold = "0" },
	} {
		params := good
		mutate(&params)
		if _, err := New(params); err == nil {
			t.Errorf("%s: New() accepted unsafe parameters", name)
		}
	}
	if _, err := New(good); err != nil {
		t.Fatalf("New() rejected good parameters: %v", err)
	}
}

func newSyncService(t *testing.T, source *fakeSource, store *fakeStore, positionCheck bool, logger *slog.Logger) *Service {
	t.Helper()
	service, err := New(Params{ExecutionAccountID: "kalshi:main", Source: source, Store: store,
		Interval: time.Minute, BalanceDriftThreshold: "0.01", BalanceSyncEnabled: true,
		PositionCheckEnabled: positionCheck, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestSyncSweepReadsBalanceAndPositionsBeforeAnySettlementRecord(t *testing.T) {
	source := &fakeSource{
		records:   map[string][]domain.KalshiSettlement{"KX-DONE": {{Ticker: "KX-DONE", MarketResult: "yes"}}},
		balance:   "1010",
		positions: []domain.KalshiMarketPosition{{Ticker: "KX-OPEN", PositionFP: "3"}},
	}
	store := &fakeStore{
		tickers:  []string{"KX-OPEN", "KX-DONE"},
		outcomes: map[string]domain.KalshiSettlementOutcome{"KX-DONE": {Result: domain.KalshiSettlementApplied}},
		snapshot: domain.KalshiLedgerSnapshot{AccountVersion: 7, Quiet: true},
	}
	if err := newSyncService(t, source, store, true, nil).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(source.calls, ","); got != "balance,positions,settlements:KX-OPEN,settlements:KX-DONE" {
		t.Fatalf("exchange reads = %s, want balance and positions before settlements", got)
	}
	if len(store.checks) != 0 || len(store.syncs) != 1 {
		t.Fatalf("checks %d syncs %d, want only the running sync", len(store.checks), len(store.syncs))
	}
	observation := store.syncs[0]
	if observation.AccountVersionBefore != 7 || observation.ExchangeBalance != "1010" || !observation.PositionCheckEnabled ||
		len(observation.Positions) != 1 || !observation.SettlementActivity || observation.SettlementReadFailed ||
		observation.Threshold != "0.01" || observation.ObservedAt.IsZero() {
		t.Fatalf("observation = %#v", observation)
	}
}

func TestSyncSweepReadsNoPositionsOutsideAQuietWindow(t *testing.T) {
	source := &fakeSource{balance: "10"}
	store := &fakeStore{snapshot: domain.KalshiLedgerSnapshot{AccountVersion: 3, Quiet: false}}
	if err := newSyncService(t, source, store, true, nil).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(source.calls, ","), "positions") {
		t.Fatalf("exchange reads = %v, positions must not be read while the account is busy", source.calls)
	}
	if len(store.syncs) != 1 || store.syncs[0].Positions != nil || store.syncs[0].SettlementActivity {
		t.Fatalf("syncs = %#v, want one without positions", store.syncs)
	}
}

func TestSyncSweepFlagsSettlementReadFailuresAndPositionErrors(t *testing.T) {
	source := &fakeSource{
		listErr:      map[string]error{"KX-BAD": errors.New("rate limited")},
		balance:      "10",
		positionsErr: errors.New("positions unavailable"),
	}
	store := &fakeStore{tickers: []string{"KX-BAD"}, snapshot: domain.KalshiLedgerSnapshot{Quiet: true}}
	err := newSyncService(t, source, store, true, nil).Sweep(context.Background())
	if err == nil || !strings.Contains(err.Error(), "KX-BAD") || strings.Contains(err.Error(), "positions unavailable") {
		t.Fatalf("Sweep() error = %v, want only the settlement read failure", err)
	}
	if len(store.syncs) != 1 {
		t.Fatalf("syncs = %d, want the sync to still record the observation", len(store.syncs))
	}
	observation := store.syncs[0]
	if !observation.SettlementReadFailed || observation.Positions != nil || observation.PositionsError != "positions unavailable" {
		t.Fatalf("observation = %#v", observation)
	}
}

func TestSyncSweepWithoutPositionCheckNeverReadsPositions(t *testing.T) {
	source := &fakeSource{balance: "10", positions: []domain.KalshiMarketPosition{}}
	store := &fakeStore{snapshot: domain.KalshiLedgerSnapshot{Quiet: true}}
	if err := newSyncService(t, source, store, false, nil).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(source.calls, ",") != "balance" || store.syncs[0].PositionCheckEnabled {
		t.Fatalf("reads = %v observation = %#v", source.calls, store.syncs[0])
	}
}

func TestDisabledSyncKeepsTheDriftOnlyCheck(t *testing.T) {
	source := &fakeSource{balance: "10"}
	store := &fakeStore{snapshot: domain.KalshiLedgerSnapshot{Quiet: true}}
	service, err := New(Params{ExecutionAccountID: "kalshi:main", Source: source, Store: store,
		Interval: time.Minute, BalanceDriftThreshold: "0.01", PositionCheckEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.checks) != 1 || len(store.syncs) != 0 || strings.Contains(strings.Join(source.calls, ","), "positions") {
		t.Fatalf("checks %d syncs %d reads %v, want the drift-only path", len(store.checks), len(store.syncs), source.calls)
	}
}

type capturedLogs struct {
	mu       sync.Mutex
	messages []string
}

func (logs *capturedLogs) Enabled(context.Context, slog.Level) bool { return true }
func (logs *capturedLogs) Handle(_ context.Context, record slog.Record) error {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	logs.messages = append(logs.messages, record.Message)
	return nil
}
func (logs *capturedLogs) WithAttrs([]slog.Attr) slog.Handler { return logs }
func (logs *capturedLogs) WithGroup(string) slog.Handler      { return logs }

func (logs *capturedLogs) count(message string) int {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	count := 0
	for _, logged := range logs.messages {
		if logged == message {
			count++
		}
	}
	return count
}

func TestSyncSkipLogsOnlyOnReasonChangeAndMilestones(t *testing.T) {
	logs := &capturedLogs{}
	service := newSyncService(t, &fakeSource{}, &fakeStore{}, true, slog.New(logs))
	previous := ""
	for consecutive := int64(1); consecutive <= 300; consecutive++ {
		service.logSync(domain.KalshiLedgerSyncResult{BalanceOutcome: domain.KalshiBalanceSkipped,
			SkipReason: domain.KalshiSyncSkipOrderInFlight, PreviousSkipReason: previous,
			ConsecutiveSkips: consecutive, TotalSkips: consecutive})
		previous = domain.KalshiSyncSkipOrderInFlight
	}
	if got := logs.count("Kalshi cash sync skipped"); got != 3 {
		t.Fatalf("skip logs = %d, want 3 (1st, 12th, 288th)", got)
	}
	service.logSync(domain.KalshiLedgerSyncResult{BalanceOutcome: domain.KalshiBalanceSkipped,
		SkipReason: domain.KalshiSyncSkipReservationActive, PreviousSkipReason: previous, ConsecutiveSkips: 301})
	if got := logs.count("Kalshi cash sync skipped"); got != 4 {
		t.Fatalf("skip logs after a reason change = %d, want 4", got)
	}
	service.logSync(domain.KalshiLedgerSyncResult{BalanceOutcome: domain.KalshiBalanceSynced, Difference: "910"})
	if logs.count("Kalshi cash synced to the exchange balance") != 1 {
		t.Fatal("a sync must be logged")
	}
}

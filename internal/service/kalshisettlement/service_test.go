package kalshisettlement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type fakeSource struct {
	records    map[string][]domain.KalshiSettlement
	listErr    map[string]error
	balance    domain.Decimal
	balanceErr error
	listed     []string
}

func (source *fakeSource) ListSettlements(_ context.Context, ticker string) ([]domain.KalshiSettlement, error) {
	source.listed = append(source.listed, ticker)
	return source.records[ticker], source.listErr[ticker]
}

func (source *fakeSource) AvailableBalance(context.Context) (domain.Decimal, error) {
	return source.balance, source.balanceErr
}

type fakeStore struct {
	tickers  []string
	outcomes map[string]domain.KalshiSettlementOutcome
	applyErr error
	applied  []string
	checks   []domain.Decimal
	check    domain.KalshiBalanceCheck
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

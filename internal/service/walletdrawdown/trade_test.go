package walletdrawdown

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/thinmarket"
)

type fixedCandidates []thinmarket.Candidate

func (c fixedCandidates) Find(context.Context, thinmarket.Params) (thinmarket.Result, error) {
	return thinmarket.Result{Candidates: c}, nil
}

func tradeCandidate(token string) thinmarket.Candidate {
	return thinmarket.Candidate{
		Market:  domain.MarketListing{MarketID: "market", ConditionID: "condition", NegRisk: true},
		Outcome: domain.MarketOutcome{Index: 1, Name: "No", TokenID: token},
		BestAsk: domain.PriceLevel{Price: "0.031", Size: "100.2"}, ObservedAt: base.Add(time.Second),
	}
}

func TestDrawdownTradeStopsAfterFirstFillOrUnknown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		second  domain.Order
		err     error
		outcome string
	}{
		{"price improves", domain.Order{Status: domain.OrderStatusFilled, FilledSize: "105"}, nil, "filled"},
		{"unknown", domain.Order{Status: domain.OrderStatusUnknown}, nil, "awaiting_order_confirmation"},
		{"cancel unconfirmed", domain.Order{Status: domain.OrderStatusCancelled, FilledSize: "0"}, nil, "awaiting_order_confirmation"},
		{"submit timeout", domain.Order{}, context.DeadlineExceeded, "submit_unconfirmed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tokens []string
			store := &memStore{}
			trader := Trader{
				Finder: fixedCandidates{tradeCandidate("101"), tradeCandidate("102"), tradeCandidate("103")},
				Submit: func(_ context.Context, i domain.OrderIntent) (domain.Order, error) {
					if store.operations[i.ClientOrderID] != "prepared" {
						t.Fatal("called execution before recording the operation")
					}
					if err := i.Validate(); err != nil {
						t.Fatalf("invalid execution intent: %v", err)
					}
					if i.ExecutionAccountID != "main" || i.TimeInForce != domain.TimeInForceFOK ||
						i.Side != domain.SideBuy || i.Type != domain.OrderTypeLimit || i.Price != "0.031" ||
						i.WorstPrice != i.Price || i.Size != "100.2" || !*i.ExpectedNegRisk ||
						!i.MarketSnapshotAt.Equal(base.Add(time.Second)) || !i.SignalAt.Equal(base) {
						t.Fatalf("unexpected order intent: %+v", i)
					}
					tokens = append(tokens, i.TokenID)
					if i.TokenID == "101" {
						return domain.Order{ID: "first", Status: domain.OrderStatusRejected, FilledSize: "0", FailureCode: "CLOB_FOK_NOT_FILLED"}, nil
					}
					tc.second.ID = "second"
					return tc.second, tc.err
				},
			}
			report, err := trader.buy(context.Background(), store, Sample{SlotAt: base, SampledAt: base})
			if !errors.Is(err, tc.err) || report.Outcome != tc.outcome || !slices.Equal(tokens, []string{"101", "102"}) {
				t.Fatalf("trade = %+v, %v, tokens %v", report, err, tokens)
			}
			if report.Attempts[0].ClientOrderID == report.Attempts[1].ClientOrderID {
				t.Fatal("different candidate tokens reused a client order ID")
			}
			wantSecondState := "recorded"
			if tc.err != nil {
				wantSecondState = "unconfirmed"
			}
			if store.operations[report.Attempts[0].ClientOrderID] != "recorded" ||
				store.operations[report.Attempts[1].ClientOrderID] != wantSecondState || len(store.operations) != 2 {
				t.Fatalf("operation audit: %v", store.operations)
			}
		})
	}
}

func TestDrawdownTradeStopsIfOperationCannotBeRecorded(t *testing.T) {
	auditErr := errors.New("audit unavailable")
	store := &memStore{auditErr: auditErr}
	trader := Trader{
		Finder: fixedCandidates{tradeCandidate("101")},
		Submit: func(context.Context, domain.OrderIntent) (domain.Order, error) {
			t.Fatal("submitted an unaudited order")
			return domain.Order{}, nil
		},
	}
	report, err := trader.buy(context.Background(), store, Sample{SlotAt: base, SampledAt: base})
	if !errors.Is(err, auditErr) || report.Outcome != "audit_failed" {
		t.Fatalf("trade = %+v, %v", report, err)
	}
}

func TestDrawdownTradeRunsOncePerEpisode(t *testing.T) {
	store := &memStore{}
	src := &fakeSources{}
	notifier := &fakeNotifier{err: errors.New("Feishu unavailable")}
	cfg := testConfig()
	calls := 0
	trader := &Trader{
		Finder: fixedCandidates{tradeCandidate("101")},
		Submit: func(context.Context, domain.OrderIntent) (domain.Order, error) {
			calls++
			return domain.Order{Status: domain.OrderStatusFilled, FilledSize: "100.2"}, nil
		},
	}
	for _, tc := range []struct {
		minutes   int
		pnl       string
		dryRun    bool
		wantTrade bool
	}{
		{0, "200", false, false},
		{10, "150", false, false},
		{20, "149", true, false},
		{20, "149", false, true},
		{20, "149", false, false},
		{30, "120", false, false},
		{40, "150", false, false},
		{50, "149", false, true},
	} {
		src.realized = tc.pnl
		report, err := Run(context.Background(), cfg, store, src, notifier, trader, base.Add(time.Duration(tc.minutes)*time.Minute), tc.dryRun)
		if (report.Trade != nil) != tc.wantTrade || (err != nil && report.Notify != "failed") {
			t.Fatalf("minute %d: trade %+v, notify %q, error %v", tc.minutes, report.Trade, report.Notify, err)
		}
	}
	if calls != 2 {
		t.Fatalf("submitted %d orders, want 2", calls)
	}
}

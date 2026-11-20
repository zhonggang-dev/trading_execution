package walletdrawdown

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestDrawdownSellPrice(t *testing.T) {
	for _, test := range []struct {
		name                 string
		cost, tick, bid, ask domain.Decimal
		want                 domain.Decimal
	}{
		{"exact", "5", "0.01", "0.5", "0.70", "0.65"},
		{"next tick", "5.25", "0.01", "0.5", "0.70", "0.66"},
		{"can immediately match best bid", "5", "0.01", "0.66", "0.70", "0.65"},
		{"must undercut", "5", "0.01", "0.5", "0.65", ""},
		{"can submit with no maker tick", "5", "0.01", "0.66", "0.67", "0.65"},
		{"outside market", "40.01", "0.01", "0.5", "0.99", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			book := domain.OrderBookSnapshot{
				TickSize: test.tick, Bids: []domain.PriceLevel{{Price: test.bid}}, Asks: []domain.PriceLevel{{Price: test.ask}},
			}
			if got := drawdownSellPrice(test.cost, "100", book); got != test.want {
				t.Fatalf("price = %q, want %q", got, test.want)
			}
		})
	}
}

type sellTestStore struct {
	t        *testing.T
	due      []DueSell
	claimed  map[string]bool
	finished int
}

func (s *sellTestStore) ListDueSells(_ context.Context, boughtBefore, retryBefore time.Time) ([]DueSell, error) {
	if !boughtBefore.Equal(base.Add(-5*time.Minute)) || !retryBefore.Equal(base.Add(-time.Minute)) {
		s.t.Fatalf("unexpected cutoffs %s, %s", boughtBefore, retryBefore)
	}
	return s.due, nil
}

func (s *sellTestStore) ClaimSell(_ context.Context, _ DueSell, intent domain.OrderIntent) (bool, error) {
	if s.claimed[intent.ClientOrderID] {
		return false, nil
	}
	s.claimed[intent.ClientOrderID] = true
	return true, nil
}

func (s *sellTestStore) FinishOrderOperation(_ context.Context, id string, recorded bool) error {
	if !recorded || !s.claimed[id] {
		s.t.Fatal("sale was not recorded and then confirmed")
	}
	s.finished++
	return nil
}

type sellTestBooks struct{ calls int }

func (b *sellTestBooks) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	b.calls++
	return []domain.OrderBookSnapshot{{
		TokenID: targets[0].TokenID, Status: domain.OrderBookStatusEmpty, ObservedAt: base,
		TickSize: "0.01", Asks: []domain.PriceLevel{{Price: "0.7"}},
	}}, nil
}

func TestSellDueSubmitsAllLotsAsAuditedGTCWithoutBids(t *testing.T) {
	outcomeIndex, negRisk := 1, true
	lot := domain.PositionLot{
		LotID: "lot-1", MarketID: "market", ConditionID: "condition", TokenID: "100",
		OutcomeIndex: &outcomeIndex, OutcomeName: "No", NegRisk: &negRisk, RemainingShares: "45",
	}
	buy := DueSell{SlotAt: base.Add(-time.Hour), OpeningClientOrderID: "buy", Lot: lot, BoughtCost: "5", BoughtShares: "100"}
	other := buy
	other.Lot.LotID, other.Lot.RemainingShares = "lot-2", "55"
	store := &sellTestStore{t: t, due: []DueSell{buy, other}, claimed: make(map[string]bool)}
	books := &sellTestBooks{}
	trader := &Trader{
		Books: books,
		Submit: func(_ context.Context, intent domain.OrderIntent) (domain.Order, error) {
			if err := intent.Validate(); err != nil {
				t.Fatal(err)
			}
			if !store.claimed[intent.ClientOrderID] || intent.Price != "0.65" ||
				intent.Side != domain.SideSell || intent.TimeInForce != domain.TimeInForceGTC || intent.PostOnly {
				t.Fatalf("unexpected sell %+v", intent)
			}
			return domain.Order{ID: "order-" + intent.TargetLotID, Status: domain.OrderStatusLive}, nil
		},
	}
	attempts, err := trader.SellDue(context.Background(), store, base, true)
	if err != nil || len(attempts) != 2 || len(store.claimed) != 0 {
		t.Fatalf("dry run: %+v, %v", attempts, err)
	}
	books.calls = 0
	attempts, err = trader.SellDue(context.Background(), store, base, false)
	if err != nil || len(attempts) != 2 || attempts[0].Size != "45" || attempts[1].Size != "55" ||
		store.finished != 2 || books.calls != 1 {
		t.Fatalf("submit: %+v, %v, finishes %d, quotes %d", attempts, err, store.finished, books.calls)
	}
}

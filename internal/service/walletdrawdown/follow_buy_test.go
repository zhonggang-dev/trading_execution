package walletdrawdown

import (
	"context"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type followBuyTestStore struct {
	source  OpenSell
	claimed string
	*memStore
}

func (s *followBuyTestStore) ListOpenSellsForBuy(context.Context) ([]OpenSell, error) {
	return []OpenSell{s.source}, nil
}

func (s *followBuyTestStore) ClaimFollowBuy(ctx context.Context, sourceSellID string, intent domain.OrderIntent) (bool, error) {
	if s.claimed != "" || sourceSellID != s.source.Intent.ClientOrderID {
		return false, nil
	}
	if err := s.RecordOrderOperation(ctx, base, 1, intent); err != nil {
		return false, err
	}
	s.claimed = intent.ClientOrderID
	return true, nil
}

func TestFollowBuyUsesOpenSellPriceAndRemainingSize(t *testing.T) {
	index, negRisk := 1, true
	store := &followBuyTestStore{
		source: OpenSell{
			Intent: domain.OrderIntent{
				ClientOrderID: "main-sell", Venue: "polymarket", MarketSource: domain.MarketSourcePolymarket,
				MarketID: "market", ConditionID: "condition", TokenID: "100", OutcomeIndex: &index,
				OutcomeName: "No", ExpectedNegRisk: &negRisk, MarketSnapshotAt: &base,
				Side: domain.SideSell, TimeInForce: domain.TimeInForceGTC, Price: "0.65", Size: "100",
			},
			Remaining: "80.25",
		}, memStore: &memStore{},
	}
	calls := 0
	trader := Trader{Submit: func(_ context.Context, intent domain.OrderIntent) (domain.Order, error) {
		calls++
		if intent.Side != domain.SideBuy || intent.TimeInForce != domain.TimeInForceIOC ||
			intent.ExecutionAccountID != "wallet-10" || intent.Price != "0.65" || intent.WorstPrice != "0.65" ||
			intent.Size != "80.25" || intent.TokenID != "100" || intent.TargetLotID != "" ||
			store.operations[intent.ClientOrderID] != "prepared" {
			t.Fatalf("unexpected public buy %+v", intent)
		}
		return domain.Order{ID: "buy", Status: domain.OrderStatusFilled, FilledSize: "80.25"}, nil
	}}
	ctx := context.Background()
	if attempts, err := trader.BuyForOpenSells(ctx, store, "wallet-10", base, true); err != nil || len(attempts) != 1 || calls != 0 {
		t.Fatalf("dry run = %+v, %v, submits %d", attempts, err, calls)
	}
	if attempts, err := trader.BuyForOpenSells(ctx, store, "wallet-10", base, false); err != nil || len(attempts) != 1 || calls != 1 {
		t.Fatalf("submit = %+v, %v, submits %d", attempts, err, calls)
	}
	if attempts, err := trader.BuyForOpenSells(ctx, store, "wallet-10", base, false); err != nil || len(attempts) != 0 || calls != 1 {
		t.Fatalf("repeat = %+v, %v, submits %d", attempts, err, calls)
	}
	if store.operations[store.claimed] != "recorded" {
		t.Fatalf("follow buy audit = %v", store.operations)
	}
}

package walletdrawdown

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const sellDelay = 5 * time.Minute

// DueSell describes an open lot and the accounting totals of its opening BUY.
type DueSell struct {
	SlotAt               time.Time
	OpeningClientOrderID string
	PreviousAttempt      int
	Lot                  domain.PositionLot
	BoughtShares         domain.Decimal
	BoughtCost           domain.Decimal
}

type SellStore interface {
	ListDueSells(ctx context.Context, boughtBefore, retryBefore time.Time) ([]DueSell, error)
	ClaimSell(ctx context.Context, due DueSell, intent domain.OrderIntent) (bool, error)
	orderOperationFinisher
}

// SellDue scans confirmed buys after the delay and submits each remaining lot
// through the normal audited limit sell flow.
func (t *Trader) SellDue(ctx context.Context, store SellStore, now time.Time, dryRun bool) ([]TradeAttempt, error) {
	due, err := store.ListDueSells(ctx, now.Add(-sellDelay), now.Add(-time.Minute))
	if err != nil {
		return nil, err
	}
	attempts := make([]TradeAttempt, 0)
	books := make(map[string]domain.OrderBookSnapshot)
	var runErr error
	for _, sell := range due {
		if err := ctx.Err(); err != nil {
			return attempts, errors.Join(runErr, err)
		}
		book, found := books[sell.OpeningClientOrderID]
		if !found {
			book, err = t.sellBook(ctx, sell.Lot, now)
			if err != nil {
				runErr = errors.Join(runErr, err)
				continue
			}
			// Use the same quote for sibling lots since the first sell can
			// become the best ask before the next one is submitted.
			books[sell.OpeningClientOrderID] = book
		}
		attempt, err := t.sellLot(ctx, store, sell, book, dryRun)
		runErr = errors.Join(runErr, err)
		if attempt != nil {
			attempts = append(attempts, *attempt)
		}
	}
	return attempts, runErr
}

func (t *Trader) sellBook(ctx context.Context, lot domain.PositionLot, now time.Time) (domain.OrderBookSnapshot, error) {
	if lot.OutcomeIndex == nil {
		return domain.OrderBookSnapshot{}, fmt.Errorf("drawdown lot %s has no outcome index", lot.LotID)
	}
	target, err := (domain.BookTargetParams{
		MarketID: lot.MarketID, ConditionID: lot.ConditionID, TokenID: lot.TokenID,
		OutcomeIndex: *lot.OutcomeIndex, MarketSource: domain.MarketSourcePolymarket,
	}).Build()
	if err != nil {
		return domain.OrderBookSnapshot{}, err
	}
	snapshots, err := t.Books.Capture(ctx, now.UTC(), []domain.BookTarget{target})
	if err != nil {
		return domain.OrderBookSnapshot{}, fmt.Errorf("read drawdown sell book for %s: %w", lot.LotID, err)
	}
	if len(snapshots) != 1 {
		return domain.OrderBookSnapshot{}, fmt.Errorf("expected one order book for drawdown lot %s", lot.LotID)
	}
	return snapshots[0], nil
}

func (t *Trader) sellLot(ctx context.Context, store SellStore, sell DueSell, book domain.OrderBookSnapshot, dryRun bool) (*TradeAttempt, error) {
	lot := sell.Lot
	if lot.OutcomeIndex == nil || lot.NegRisk == nil {
		return nil, fmt.Errorf("drawdown lot %s has incomplete market identity", lot.LotID)
	}
	if (book.Status != domain.OrderBookStatusOK && book.Status != domain.OrderBookStatusEmpty) || book.ObservedAt.IsZero() ||
		book.TokenID != lot.TokenID || len(book.Asks) == 0 {
		return nil, nil
	}
	if lot.RemainingShares.Scale() > 2 {
		return nil, fmt.Errorf("drawdown lot %s has %s shares, beyond the Polymarket sell size precision", lot.LotID, lot.RemainingShares)
	}
	price := drawdownSellPrice(sell.BoughtCost, sell.BoughtShares, book)
	if price.IsEmpty() {
		return nil, nil
	}
	intent := domain.OrderIntent{
		ModelID: tradeStrategyID, StrategyID: tradeStrategyID, ExecutionAccountID: tradeAccountID,
		SignalID:      sell.OpeningClientOrderID,
		ClientOrderID: fmt.Sprintf("%s:sell:%s:%d", tradeStrategyID, lot.LotID, sell.PreviousAttempt),
		Venue:         "polymarket", MarketSource: domain.MarketSourcePolymarket, MarketID: lot.MarketID, ConditionID: lot.ConditionID,
		OutcomeIndex: lot.OutcomeIndex, OutcomeName: lot.OutcomeName, TokenID: lot.TokenID, TargetLotID: lot.LotID,
		ExpectedNegRisk: lot.NegRisk, MarketSnapshotAt: &book.ObservedAt, SignalAt: &book.ObservedAt,
		Side: domain.SideSell, Type: domain.OrderTypeLimit, TimeInForce: domain.TimeInForceGTC,
		Price: price, WorstPrice: price, Size: lot.RemainingShares,
	}
	attempt := TradeAttempt{ClientOrderID: intent.ClientOrderID, TokenID: intent.TokenID, Price: price, Size: intent.Size}
	if dryRun {
		return &attempt, nil
	}
	claimed, err := store.ClaimSell(ctx, sell, intent)
	if err != nil || !claimed {
		return nil, err
	}
	order, submitErr, auditErr := t.submitAudited(ctx, store, intent)
	attempt.OrderID, attempt.Status, attempt.FilledSize, attempt.FailureCode = order.ID, order.Status, order.FilledSize, order.FailureCode
	return &attempt, errors.Join(submitErr, auditErr)
}

func drawdownSellPrice(cost, boughtShares domain.Decimal, book domain.OrderBookSnapshot) domain.Decimal {
	if len(book.Asks) == 0 {
		return ""
	}
	actualCost, costErr := ratOf(cost.String())
	quantity, sizeErr := ratOf(boughtShares.String())
	tick, tickErr := ratOf(book.TickSize.String())
	ask, askErr := ratOf(book.Asks[0].Price.String())
	if costErr != nil || sizeErr != nil || tickErr != nil || askErr != nil || quantity.Sign() <= 0 || tick.Sign() <= 0 {
		return ""
	}

	// Round the minimum acceptable price up to the next valid tick.
	target := new(big.Rat).Quo(new(big.Rat).Add(actualCost, big.NewRat(60, 1)), quantity)
	units := new(big.Rat).Quo(target, tick)
	steps := new(big.Int)
	remainder := new(big.Int)
	steps.QuoRem(units.Num(), units.Denom(), remainder)
	if remainder.Sign() != 0 {
		steps.Add(steps, big.NewInt(1))
	}
	price := new(big.Rat).Mul(new(big.Rat).SetInt(steps), tick)
	maxPrice := new(big.Rat).Sub(big.NewRat(1, 1), tick)
	if price.Cmp(tick) < 0 || price.Cmp(ask) >= 0 || price.Cmp(maxPrice) > 0 {
		return ""
	}
	return domain.Decimal(price.FloatString(book.TickSize.Scale())).Canonical()
}

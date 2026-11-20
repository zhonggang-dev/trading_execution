package walletdrawdown

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// OpenSell holds the source order and remaining shares visible in the ledger.
type OpenSell struct {
	Intent    domain.OrderIntent
	Remaining domain.Decimal
}

type FollowBuyStore interface {
	ListOpenSellsForBuy(context.Context) ([]OpenSell, error)
	ClaimFollowBuy(ctx context.Context, sourceSellID string, intent domain.OrderIntent) (bool, error)
	orderOperationFinisher
}

// BuyForOpenSells submits one ordinary public order per source sell. The
// execution service handles order validation, authorization and settlement.
func (t *Trader) BuyForOpenSells(ctx context.Context, store FollowBuyStore, account string, now time.Time, dryRun bool) ([]TradeAttempt, error) {
	if account == "" || account == tradeAccountID {
		return nil, fmt.Errorf("drawdown follow buy requires a separate execution account")
	}
	sells, err := store.ListOpenSellsForBuy(ctx)
	if err != nil {
		return nil, err
	}
	attempts := make([]TradeAttempt, 0, len(sells))
	var runErr error
	for _, sell := range sells {
		if err := ctx.Err(); err != nil {
			return attempts, errors.Join(runErr, err)
		}
		source := sell.Intent
		intent := domain.OrderIntent{
			ModelID: tradeStrategyID, StrategyID: tradeStrategyID, ExecutionAccountID: account,
			SignalID: source.ClientOrderID, ClientOrderID: tradeStrategyID + ":follow_buy:" + source.ClientOrderID,
			Venue: source.Venue, MarketSource: source.MarketSource, MarketID: source.MarketID,
			ConditionID: source.ConditionID, TokenID: source.TokenID, OutcomeIndex: source.OutcomeIndex,
			OutcomeName: source.OutcomeName, OutcomeID: source.OutcomeID, ExpectedNegRisk: source.ExpectedNegRisk,
			MarketSnapshotAt: source.MarketSnapshotAt, SignalAt: &now,
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, TimeInForce: domain.TimeInForceIOC,
			Price: source.Price, WorstPrice: source.Price, Size: sell.Remaining,
		}
		if err := intent.Validate(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("build drawdown buy from sell %s: %w", source.ClientOrderID, err))
			continue
		}
		attempt := TradeAttempt{ClientOrderID: intent.ClientOrderID, TokenID: intent.TokenID, Price: intent.Price, Size: intent.Size}
		if dryRun {
			attempts = append(attempts, attempt)
			continue
		}
		claimed, err := store.ClaimFollowBuy(ctx, source.ClientOrderID, intent)
		if err != nil || !claimed {
			runErr = errors.Join(runErr, err)
			continue
		}
		order, submitErr, auditErr := t.submitAudited(ctx, store, intent)
		attempt.OrderID, attempt.Status, attempt.FilledSize, attempt.FailureCode = order.ID, order.Status, order.FilledSize, order.FailureCode
		attempts = append(attempts, attempt)
		runErr = errors.Join(runErr, submitErr, auditErr)
	}
	return attempts, runErr
}

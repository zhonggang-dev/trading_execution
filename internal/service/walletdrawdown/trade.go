package walletdrawdown

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/thinmarket"
)

const (
	tradeAccountID      = "main"
	tradeStrategyID     = "wallet_drawdown"
	minTradeDrawdownUSD = 50
)

// Trader routes the ordered thin-market candidates through the execution API.
type Trader struct {
	Finder interface {
		Find(context.Context, thinmarket.Params) (thinmarket.Result, error)
	}
	Books  port.OrderBookSource
	Submit func(context.Context, domain.OrderIntent) (domain.Order, error)
}

type orderOperationStore interface {
	RecordOrderOperation(ctx context.Context, slot time.Time, attempt int, intent domain.OrderIntent) error
	orderOperationFinisher
}

type orderOperationFinisher interface {
	FinishOrderOperation(ctx context.Context, clientOrderID string, orderRecorded bool) error
}

type TradeReport struct {
	Outcome  string         `json:"outcome"`
	Attempts []TradeAttempt `json:"attempts,omitempty"`
}

type TradeAttempt struct {
	ClientOrderID string             `json:"client_order_id"`
	OrderID       string             `json:"order_id,omitempty"`
	TokenID       string             `json:"token_id"`
	Price         domain.Decimal     `json:"price"`
	Size          domain.Decimal     `json:"size"`
	Status        domain.OrderStatus `json:"status,omitempty"`
	FilledSize    domain.Decimal     `json:"filled_size,omitempty"`
	FailureCode   string             `json:"failure_code,omitempty"`
}

func startsTradeEpisode(sample Sample, history []Sample) bool {
	if sample.AccountID != tradeAccountID || !exceedsTradeThreshold(sample) {
		return false
	}
	var previous Sample
	for _, candidate := range history {
		if candidate.SlotAt.After(previous.SlotAt) {
			previous = candidate
		}
	}
	return !exceedsTradeThreshold(previous)
}

func exceedsTradeThreshold(sample Sample) bool {
	if !sample.Exceeded || sample.Drawdown == nil {
		return false
	}
	drawdown, err := ratOf(*sample.Drawdown)
	return err == nil && drawdown.Cmp(big.NewRat(minTradeDrawdownUSD, 1)) > 0
}

func (t *Trader) buy(ctx context.Context, store orderOperationStore, sample Sample) (TradeReport, error) {
	result, err := t.Finder.Find(ctx, thinmarket.Params{})
	if err != nil {
		return TradeReport{Outcome: "scan_failed"}, fmt.Errorf("find drawdown trade markets: %w", err)
	}
	if len(result.Candidates) == 0 {
		return TradeReport{Outcome: "no_candidates"}, nil
	}

	report := TradeReport{Outcome: "all_rejected"}
	signalID := fmt.Sprintf("%s:%s:%d", tradeStrategyID, tradeAccountID, sample.SlotAt.Unix())
	for index, candidate := range result.Candidates {
		if err := ctx.Err(); err != nil {
			report.Outcome = "interrupted"
			return report, err
		}
		intent := domain.OrderIntent{
			ModelID: tradeStrategyID, StrategyID: tradeStrategyID, ExecutionAccountID: tradeAccountID,
			SignalID: signalID, ClientOrderID: signalID + ":" + candidate.Outcome.TokenID,
			Venue: "polymarket", MarketSource: domain.MarketSourcePolymarket,
			MarketID: candidate.Market.MarketID, ConditionID: candidate.Market.ConditionID,
			OutcomeIndex: &candidate.Outcome.Index, OutcomeName: candidate.Outcome.Name, TokenID: candidate.Outcome.TokenID,
			ExpectedNegRisk: &candidate.Market.NegRisk, MarketSnapshotAt: &candidate.ObservedAt, SignalAt: &sample.SampledAt,
			Side: domain.SideBuy, Type: domain.OrderTypeLimit, TimeInForce: domain.TimeInForceFOK,
			Price: candidate.BestAsk.Price, WorstPrice: candidate.BestAsk.Price, Size: candidate.BestAsk.Size,
		}
		if err := store.RecordOrderOperation(ctx, sample.SlotAt, index+1, intent); err != nil {
			report.Outcome = "audit_failed"
			return report, err
		}
		order, submitErr, auditErr := t.submitAudited(ctx, store, intent)
		report.Attempts = append(report.Attempts, TradeAttempt{
			ClientOrderID: intent.ClientOrderID, OrderID: order.ID, TokenID: intent.TokenID,
			Price: intent.Price, Size: intent.Size, Status: order.Status, FilledSize: order.FilledSize, FailureCode: order.FailureCode,
		})
		switch {
		case auditErr != nil:
			report.Outcome = "audit_failed"
			return report, errors.Join(auditErr, submitErr)
		case submitErr != nil:
			report.Outcome = "submit_unconfirmed"
			return report, fmt.Errorf("submit drawdown trade %s: %w", intent.ClientOrderID, submitErr)
		case order.Status == domain.OrderStatusFilled:
			report.Outcome = "filled"
			return report, nil
		case order.Status == domain.OrderStatusRejected && order.FilledSize.Equal("0"):
			// A definitive rejection is the only submit response that proves
			// zero fills. Cancellation can still be awaiting fill evidence.
			continue
		default:
			report.Outcome = "awaiting_order_confirmation"
			return report, nil
		}
	}
	return report, nil
}

func (t *Trader) submitAudited(ctx context.Context, store orderOperationFinisher, intent domain.OrderIntent) (domain.Order, error, error) {
	order, submitErr := t.Submit(ctx, intent)
	// Persist the outcome even if the submission exhausted the caller's deadline.
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	auditErr := store.FinishOrderOperation(auditCtx, intent.ClientOrderID, order.ID != "" && submitErr == nil)
	return order, submitErr, auditErr
}

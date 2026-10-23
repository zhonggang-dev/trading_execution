// Package strategyreport tells the strategy service what became of the orders
// it asked for. The strategy occupies a market when it answers and only
// releases it on a terminal report, so every SUBMIT decision must eventually be
// reported: orders are read back from the order table (stateless, idempotent on
// the strategy side), and decisions that never became an order are reported
// directly with NOT_SUBMITTED.
package strategyreport

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/strategyhttp"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	defaultLookback  = 24 * time.Hour
	scanLimit        = 5000
	notSubmittedSeq  = 1
	directAttempts   = 3
	directRetryDelay = time.Second
	directTimeout    = 20 * time.Second
)

// OrderSource reads the orders of some accounts that changed since a time.
type OrderSource interface {
	ListUpdatedSince(ctx context.Context, executionAccountIDs []string, since time.Time, limit int) ([]domain.Order, error)
}

// Sink posts reports to the strategy service.
type Sink interface {
	ReportOrders(ctx context.Context, reports []domain.StrategyOrderReport) ([]domain.StrategyOrderReportResult, error)
}

// Params configures a Reporter.
type Params struct {
	Orders OrderSource
	Sink   Sink
	// StrategyIDs are the reported strategies; AccountIDs are the execution
	// accounts bound to them.
	StrategyIDs []string
	AccountIDs  []string
	Lookback    time.Duration
	Now         func() time.Time
	Logger      *slog.Logger
}

// Reporter is safe for concurrent use.
type Reporter struct {
	orders      OrderSource
	sink        Sink
	strategyIDs map[string]struct{}
	accountIDs  []string
	lookback    time.Duration
	now         func() time.Time
	logger      *slog.Logger

	mu   sync.Mutex
	sent map[string]int64 // order_id -> last revision the strategy has acknowledged
}

// New validates the configuration.
func New(params Params) (*Reporter, error) {
	if params.Orders == nil || params.Sink == nil {
		return nil, fmt.Errorf("strategy order reporter requires an order source and a sink")
	}
	if len(params.StrategyIDs) == 0 || len(params.AccountIDs) == 0 {
		return nil, fmt.Errorf("strategy order reporter requires strategy IDs and execution accounts")
	}
	if params.Lookback <= 0 {
		params.Lookback = defaultLookback
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	strategyIDs := make(map[string]struct{}, len(params.StrategyIDs))
	for _, id := range params.StrategyIDs {
		strategyIDs[strings.TrimSpace(id)] = struct{}{}
	}
	return &Reporter{
		orders: params.Orders, sink: params.Sink, strategyIDs: strategyIDs,
		accountIDs: append([]string(nil), params.AccountIDs...), lookback: params.Lookback,
		now: params.Now, logger: params.Logger, sent: make(map[string]int64),
	}, nil
}

// Sync reports every order of the configured strategies that changed within the
// lookback window and has not been acknowledged at its current revision. A
// failed request is simply retried by the next call.
func (reporter *Reporter) Sync(ctx context.Context) error {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	now := reporter.now().UTC()
	orders, err := reporter.orders.ListUpdatedSince(ctx, reporter.accountIDs, now.Add(-reporter.lookback), scanLimit)
	if err != nil {
		return fmt.Errorf("list orders to report: %w", err)
	}
	if len(orders) == scanLimit {
		reporter.logger.Warn("strategy order report scan hit its limit; older changes wait for the next pass", "limit", scanLimit)
	}
	seen := make(map[string]struct{}, len(orders))
	type pending struct {
		orderID  string
		revision int64
		report   domain.StrategyOrderReport
	}
	todo := make([]pending, 0)
	for _, order := range orders {
		if !reporter.reports(order.Intent) {
			continue
		}
		seen[order.ID] = struct{}{}
		if reporter.sent[order.ID] >= order.Revision {
			continue
		}
		report, ok := ReportFromOrder(order)
		if !ok {
			continue
		}
		todo = append(todo, pending{order.ID, order.Revision, report})
	}
	for orderID := range reporter.sent {
		if _, inWindow := seen[orderID]; !inWindow {
			delete(reporter.sent, orderID)
		}
	}
	var firstErr error
	for start := 0; start < len(todo); start += strategyhttp.MaxOrderReportsPerRequest {
		end := min(start+strategyhttp.MaxOrderReportsPerRequest, len(todo))
		batch := make([]domain.StrategyOrderReport, 0, end-start)
		for _, item := range todo[start:end] {
			batch = append(batch, item.report)
		}
		results, err := reporter.sink.ReportOrders(ctx, batch)
		if err != nil {
			firstErr = fmt.Errorf("report %d orders: %w", len(batch), err)
			break
		}
		for index, result := range results {
			item := todo[start+index]
			if result.ErrorCode != "" {
				// Per-report rejections do not heal by resending; keep them
				// visible once and stop retrying this revision.
				reporter.logger.Warn("strategy rejected an order report",
					"decision_id", result.DecisionID, "report_seq", result.ReportSeq,
					"code", result.ErrorCode, "message", result.ErrorText)
			}
			reporter.sent[item.orderID] = item.revision
		}
	}
	if len(todo) > 0 {
		reporter.logger.Info("strategy order reports sent", "candidates", len(todo), "error", firstErr)
	}
	return firstErr
}

// ReportNotSubmitted tells the strategy that decisions of one binding never
// reached an order (response rejected, submission gate closed, ...). It retries
// briefly and then gives up with a log line; the caller must not depend on it.
func (reporter *Reporter) ReportNotSubmitted(
	ctx context.Context,
	execution domain.StrategyExecutionContext,
	decisions []domain.StrategyUndeliveredDecision,
	failureCode, failureReason string,
) {
	if _, reported := reporter.strategyIDs[execution.StrategyID]; !reported || len(decisions) == 0 {
		return
	}
	// The caller's context is the whole decision cycle's; keep this bounded.
	ctx, cancel := context.WithTimeout(ctx, directTimeout)
	defer cancel()
	eventAt := reporter.now().UTC()
	reports := make([]domain.StrategyOrderReport, 0, len(decisions))
	for _, decision := range decisions {
		reports = append(reports, domain.StrategyOrderReport{
			ReportSeq: notSubmittedSeq, Context: execution, DecisionID: decision.DecisionID,
			ConditionID: decision.ConditionID, TokenID: decision.TokenID,
			Status: domain.StrategyReportNotSubmitted, CumulativeFilledShares: "0",
			FailureCode: failureCode, FailureReason: truncate(failureReason, 500), EventAt: eventAt,
		})
	}
	for start := 0; start < len(reports); start += strategyhttp.MaxOrderReportsPerRequest {
		end := min(start+strategyhttp.MaxOrderReportsPerRequest, len(reports))
		var err error
		for attempt := 1; attempt <= directAttempts; attempt++ {
			var results []domain.StrategyOrderReportResult
			if results, err = reporter.sink.ReportOrders(ctx, reports[start:end]); err == nil {
				for _, result := range results {
					if result.ErrorCode != "" {
						reporter.logger.Warn("strategy rejected a NOT_SUBMITTED report",
							"decision_id", result.DecisionID, "code", result.ErrorCode, "message", result.ErrorText)
					}
				}
				break
			}
			if attempt < directAttempts {
				select {
				case <-ctx.Done():
					attempt = directAttempts
				case <-time.After(directRetryDelay * time.Duration(attempt)):
				}
			}
		}
		if err != nil {
			reporter.logger.Error("could not report NOT_SUBMITTED decisions; their markets stay occupied until reset",
				"execution_account_id", execution.ExecutionAccountID, "count", end-start, "failure_code", failureCode, "error", err)
		}
	}
}

func (reporter *Reporter) reports(intent domain.OrderIntent) bool {
	_, ok := reporter.strategyIDs[intent.StrategyID]
	return ok && intent.Side == domain.SideBuy && strings.TrimSpace(intent.SignalID) != ""
}

// ReportFromOrder maps an order to its cumulative report. ok is false for
// orders that have not reached the exchange yet (they are reported once they
// move) and for fills whose price cannot be derived.
func ReportFromOrder(order domain.Order) (domain.StrategyOrderReport, bool) {
	filled := order.FilledSize
	if filled.IsEmpty() {
		filled = "0"
	}
	hasFill := false
	if sign, err := filled.Sign(); err != nil {
		return domain.StrategyOrderReport{}, false
	} else if sign > 0 {
		hasFill = true
	}
	var status domain.StrategyReportStatus
	switch order.Status {
	case domain.OrderStatusReceived, domain.OrderStatusValidating, domain.OrderStatusReserved, domain.OrderStatusSubmitting:
		return domain.StrategyOrderReport{}, false
	case domain.OrderStatusAcknowledged:
		status = domain.StrategyReportSubmitted
	case domain.OrderStatusLive, domain.OrderStatusCancelPending:
		status = domain.StrategyReportOpen
	case domain.OrderStatusPartiallyFilled:
		status = domain.StrategyReportPartiallyFilled
	case domain.OrderStatusFilled:
		status = domain.StrategyReportFilled
	case domain.OrderStatusCancelled:
		status = domain.StrategyReportCancelled
	case domain.OrderStatusRejected:
		status = domain.StrategyReportRejected
		if strings.TrimSpace(order.VenueOrderID) == "" {
			status = domain.StrategyReportNotSubmitted
		}
	default: // UNKNOWN, RECONCILING, MANUAL_REVIEW
		status = domain.StrategyReportUnknown
	}
	switch {
	case hasFill && (status == domain.StrategyReportSubmitted || status == domain.StrategyReportOpen):
		status = domain.StrategyReportPartiallyFilled
	case !hasFill && status == domain.StrategyReportPartiallyFilled:
		status = domain.StrategyReportOpen
	case !hasFill && status == domain.StrategyReportFilled:
		status = domain.StrategyReportUnknown // contradictory row: never release the market on it
	}
	report := domain.StrategyOrderReport{
		ReportSeq: order.Revision,
		Context: domain.StrategyExecutionContext{
			ModelID: order.Intent.ModelID, StrategyID: order.Intent.StrategyID,
			ExecutionAccountID: order.Intent.ExecutionAccountID,
		},
		DecisionID: strings.TrimSpace(order.Intent.SignalID), ConditionID: order.Intent.ConditionID,
		TokenID: order.Intent.TokenID, VenueOrderID: strings.TrimSpace(order.VenueOrderID),
		Status: status, CumulativeFilledShares: filled,
		FailureCode: order.FailureCode, FailureReason: truncate(order.FailureReason, 500),
		EventAt: order.UpdatedAt.UTC(),
	}
	if hasFill {
		price := order.AverageFillPrice
		if price.IsEmpty() {
			derived, ok := derivePrice(order.FilledNotional, filled)
			if !ok {
				return domain.StrategyOrderReport{}, false
			}
			price = derived
		}
		report.AverageFillPrice = price
		if !order.TotalFees.IsEmpty() {
			report.FeesUSD = order.TotalFees
		}
	}
	if order.Revision < 1 {
		return domain.StrategyOrderReport{}, false
	}
	// One undecodable number would fail the whole batch on every retry.
	for _, value := range []domain.Decimal{report.CumulativeFilledShares, report.AverageFillPrice, report.FeesUSD} {
		if value.IsEmpty() {
			continue
		}
		if _, err := domain.ParseDecimal(value.String()); err != nil {
			return domain.StrategyOrderReport{}, false
		}
	}
	return report, true
}

func derivePrice(notional, shares domain.Decimal) (domain.Decimal, bool) {
	n, ok := new(big.Rat).SetString(notional.String())
	if !ok || notional.IsEmpty() {
		return "", false
	}
	s, ok := new(big.Rat).SetString(shares.String())
	if !ok || s.Sign() <= 0 {
		return "", false
	}
	price := new(big.Rat).Quo(n, s)
	if price.Sign() <= 0 {
		return "", false
	}
	return domain.Decimal(price.FloatString(8)), true
}

func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

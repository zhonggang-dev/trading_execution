package port

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var (
	ErrOrderNotFound           = errors.New("order not found")
	ErrOrderRevisionConflict   = errors.New("order revision conflict")
	ErrReservationNotFound     = errors.New("asset reservation not found")
	ErrReservationConflict     = errors.New("asset reservation idempotency conflict")
	ErrFillNotFound            = errors.New("fill not found")
	ErrFillConflict            = errors.New("fill identity conflict")
	ErrPositionNotFound        = errors.New("position not found")
	ErrAccountNotFound         = errors.New("execution account not found")
	ErrPositionExitRunNotFound = errors.New("position exit run not found")
	ErrPositionExitConflict    = errors.New("position exit idempotency conflict")
	ErrCancelFinalityPending   = errors.New("cancel fill finality window has not elapsed")
)

// OrderRepository 表示后端使用的 OrderRepository 类型。
type OrderRepository interface {
	// Create is atomic. created=false returns the order already associated with
	// the same client_order_id.
	Create(ctx context.Context, order domain.Order) (stored domain.Order, created bool, err error)
	Get(ctx context.Context, orderID string) (domain.Order, error)
	GetByClientOrderID(ctx context.Context, clientOrderID string) (domain.Order, error)
	// Transition atomically writes the next order revision and its immutable
	// event. Implementations must reject partial writes and stale revisions.
	Transition(ctx context.Context, order domain.Order, event domain.OrderEvent) error
	// StartAttempt atomically persists SUBMITTING/CANCEL_PENDING/RECONCILING
	// and the STARTED attempt before any network call can leave the process.
	StartAttempt(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error
	// FinishAttempt atomically persists the completed attempt and its resulting
	// order transition. A crash can therefore leave only STARTED (ambiguous),
	// never a successful venue call without its state/event.
	FinishAttempt(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error
	Events(ctx context.Context, orderID string) ([]domain.OrderEvent, error)
	Attempts(ctx context.Context, orderID string) ([]domain.OrderAttempt, error)
	// ListPending supports the reconciliation/timeout coordinator. It returns
	// non-terminal orders whose updated_at is no later than before.
	ListPending(ctx context.Context, before time.Time, limit int) ([]domain.Order, error)
}

// VenueOrderState 表示后端使用的 VenueOrderState 类型。
type VenueOrderState string

const (
	VenueOrderAcknowledged    VenueOrderState = "ACKNOWLEDGED"
	VenueOrderLive            VenueOrderState = "LIVE"
	VenueOrderPartiallyFilled VenueOrderState = "PARTIALLY_FILLED"
	VenueOrderFilled          VenueOrderState = "FILLED"
	VenueOrderCancelled       VenueOrderState = "CANCELLED"
	VenueOrderRejected        VenueOrderState = "REJECTED"
	VenueOrderUnknown         VenueOrderState = "UNKNOWN"
)

// VenueOrder 表示后端使用的 VenueOrder 类型。
type VenueOrder struct {
	ID               string
	State            VenueOrderState
	RawStatus        string
	FilledSize       domain.Decimal
	AverageFillPrice domain.Decimal
	ObservedAt       time.Time
	TradeIDs         []string
}

// Venue 表示后端使用的 Venue 类型。
type Venue interface {
	Name() string
	// Place must use order.Intent.ExecutionAccountID to select the isolated
	// wallet/signer and reject an unknown or mismatched account in live mode.
	Place(ctx context.Context, order domain.Order) (VenueOrder, error)
	Cancel(ctx context.Context, order domain.Order) (VenueOrder, error)
	Get(ctx context.Context, order domain.Order) (VenueOrder, error)
}

// FillSource reads real trade records. Order placement status is deliberately
// not part of this interface and can never manufacture a Fill.
type FillSource interface {
	ListOrderFills(ctx context.Context, order domain.Order) ([]domain.Fill, error)
}

// FillLedger is the production accounting boundary. Record must atomically
// deduplicate the venue fill, update a CONFIRMED fill's order/reservation,
// cash, position snapshot, lots and audit events, and write its outbox event.
// Non-final MATCHED/MINED/RETRYING observations are stored but not booked.
type FillLedger interface {
	Record(ctx context.Context, order domain.Order, fill domain.Fill) (domain.FillApplication, error)
	GetFill(ctx context.Context, fillKey string) (domain.Fill, error)
	ListOrderFills(ctx context.Context, orderID string) ([]domain.Fill, error)
}

// TradeHistoryRepository exposes a read-only view over confirmed, booked
// fills. Implementations must apply one filter consistently to rows, totals,
// and aggregates so the operations console cannot show contradictory numbers.
type TradeHistoryRepository interface {
	ListTradeHistory(ctx context.Context, filter domain.TradeHistoryFilter) (domain.TradeHistoryPage, error)
}

// PositionLedger 表示后端使用的 PositionLedger 类型。
type PositionLedger interface {
	GetPosition(ctx context.Context, executionAccountID, tokenID string) (domain.Position, error)
	ListLots(ctx context.Context, executionAccountID, tokenID string) ([]domain.PositionLot, error)
	ListOpenLots(ctx context.Context, executionAccountID string) ([]domain.PositionLot, error)
	ListPositionEvents(ctx context.Context, executionAccountID, tokenID string) ([]domain.PositionEvent, error)
	MarkPosition(ctx context.Context, mark domain.PositionMark) (domain.Position, error)
}

// StrategyPositionSource provides the exact open lots for one isolated
// execution account. Strategy exits are lot-addressed rather than net-position
// addressed, so implementations must not aggregate rows by token.
type StrategyPositionSource interface {
	ListOpenLots(ctx context.Context, executionAccountID string) ([]domain.PositionLot, error)
}

// PositionExitTradeSource returns open lots together with the active
// reservations for those exact lots. It must never derive availability from
// a token-level net position alone.
type PositionExitTradeSource interface {
	ListOpenPositionExitTrades(ctx context.Context, executionAccountID string) ([]domain.PositionExitTrade, error)
}

// FundsLedger 表示后端使用的 FundsLedger 类型。
type FundsLedger interface {
	GetBalance(ctx context.Context, executionAccountID string) (domain.AccountBalance, error)
	ListAccountEvents(ctx context.Context, executionAccountID string) ([]domain.AccountEvent, error)
}

// ReconciliationOrderRepository exposes only orders created by this service.
// External CLOB orders are never inserted here merely to make totals match.
type ReconciliationOrderRepository interface {
	ListForReconciliation(ctx context.Context, executionAccountID string, updatedAfter time.Time) ([]domain.Order, error)
}

// VenueReconciliationSource 表示后端使用的 VenueReconciliationSource 类型。
type VenueReconciliationSource interface {
	ListReconciliationOpenOrders(ctx context.Context, executionAccountID string) ([]domain.VenueOrderSnapshot, error)
	ListReconciliationTrades(ctx context.Context, executionAccountID string, matchedAfter time.Time) ([]domain.VenueTradeSnapshot, error)
}

// ExternalPositionSource 表示后端使用的 ExternalPositionSource 类型。
type ExternalPositionSource interface {
	ListExternalPositions(ctx context.Context, walletAddress string) ([]domain.ExternalPosition, error)
}

// ExternalBalanceSource 表示后端使用的 ExternalBalanceSource 类型。
type ExternalBalanceSource interface {
	GetExternalBalance(ctx context.Context, walletAddress, asset string) (domain.ExternalBalance, error)
}

// ReconciliationLedger 表示后端使用的 ReconciliationLedger 类型。
type ReconciliationLedger interface {
	GetBalance(ctx context.Context, executionAccountID string) (domain.AccountBalance, error)
	ListPositions(ctx context.Context, executionAccountID string) ([]domain.Position, error)
	MarkPositionSettled(ctx context.Context, executionAccountID, tokenID, sourceReference string, observedAt time.Time) (domain.Position, error)
}

// ReconciliationRecorder 表示后端使用的 ReconciliationRecorder 类型。
type ReconciliationRecorder interface {
	Start(ctx context.Context, run domain.ReconciliationRun) error
	RecordIssue(ctx context.Context, issue domain.ReconciliationIssue) error
	Complete(ctx context.Context, run domain.ReconciliationRun) error
}

// ReconciliationTriggerer is intentionally fire-and-forget at the order path.
// A process crash before delivery is covered by the startup sweep.
type ReconciliationTriggerer interface {
	Trigger(executionAccountID string, trigger domain.ReconciliationTrigger, focusOrderID string)
}

// OutboxEvent 表示后端使用的 OutboxEvent 类型。
type OutboxEvent struct {
	ID          string
	Topic       string
	EventKey    string
	AggregateID string
	Payload     []byte
	Attempts    int
	CreatedAt   time.Time
}

// OutboxRepository 表示后端使用的 OutboxRepository 类型。
type OutboxRepository interface {
	Claim(ctx context.Context, limit int, now time.Time) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID string, publishedAt time.Time) error
	MarkFailed(ctx context.Context, eventID string, nextAttemptAt time.Time) error
}

// EventPublisher 表示后端使用的 EventPublisher 类型。
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// VenueErrorKind 表示后端使用的 VenueErrorKind 类型。
type VenueErrorKind string

const (
	// VenueErrorRejected proves the venue did not accept the operation.
	VenueErrorRejected VenueErrorKind = "REJECTED"
	// VenueErrorAmbiguous means bytes may have reached the venue. A submit or
	// cancel with this outcome must be reconciled and must not be retried.
	VenueErrorAmbiguous VenueErrorKind = "AMBIGUOUS"
	// VenueErrorUnavailable is safe for read-only retry but never for a
	// state-changing operation whose outcome cannot be proven.
	VenueErrorUnavailable VenueErrorKind = "UNAVAILABLE"
	VenueErrorInvalid     VenueErrorKind = "INVALID"
)

// VenueError 表示后端使用的 VenueError 类型。
type VenueError struct {
	Kind         VenueErrorKind
	Code         string
	Message      string
	VenueOrderID string
	HTTPStatus   int
	RetryAfter   time.Duration
	Cause        error
}

// Error 返回当前错误模型的可读错误文本。
func (venueError *VenueError) Error() string {
	if venueError == nil {
		return "<nil>"
	}
	if venueError.Message != "" {
		return fmt.Sprintf("%s: %s", venueError.Code, venueError.Message)
	}
	if venueError.Cause != nil {
		return fmt.Sprintf("%s: %v", venueError.Code, venueError.Cause)
	}
	return venueError.Code
}

// Unwrap 返回当前错误包装的底层原因。
func (venueError *VenueError) Unwrap() error {
	if venueError == nil {
		return nil
	}
	return venueError.Cause
}

// Guard 表示后端使用的 Guard 类型。
type Guard interface {
	Check(ctx context.Context, intent domain.OrderIntent) error
}

// OrderSubmitResult 表示一次订单提交的领域结果以及是否首次创建。
type OrderSubmitResult struct {
	Order   domain.Order
	Created bool
}

// OrderExecutor 定义策略编排层提交订单意图时依赖的执行端口。
type OrderExecutor interface {
	Submit(ctx context.Context, intent domain.OrderIntent) (OrderSubmitResult, error)
}

// FillSyncResult 汇总一次真实成交同步产生的账本应用结果。
type FillSyncResult struct {
	OrderID      string                   `json:"order_id"`
	Observed     int                      `json:"observed"`
	Applied      int                      `json:"applied"`
	Duplicates   int                      `json:"duplicates"`
	Applications []domain.FillApplication `json:"applications"`
}

// FillSynchronizer 定义对账服务同步单个订单真实成交时依赖的端口。
type FillSynchronizer interface {
	SyncOrder(ctx context.Context, orderID string) (FillSyncResult, error)
}

// AssetReservationManager is the authoritative balance/position concurrency
// boundary. A live implementation must perform every method in PostgreSQL
// transactions using execution-account row locks; Redis locks are not safe for
// this responsibility.
type AssetReservationManager interface {
	// Reserve atomically checks and locks BUY collateral or SELL shares. The
	// operation is idempotent by client_order_id and rejects a changed order.
	Reserve(ctx context.Context, order domain.Order) (domain.AssetReservation, error)
	// Reconcile consumes cumulative fills and releases only the amount that is
	// no longer needed. Terminal CANCELLED/REJECTED orders release the unfilled
	// remainder; FILLED orders settle the reservation.
	Reconcile(ctx context.Context, order domain.Order) (domain.AssetReservation, error)
	// MarkUncertain retains collateral after an ambiguous venue outcome.
	MarkUncertain(ctx context.Context, order domain.Order, reason string) error
}

// HardRiskSource returns one consistent account-level view. A live adapter
// must include positions plus pending/accepted/open order reservations. A
// snapshot read alone is not a concurrency boundary: the live integration
// must serialize check-and-reserve by execution account in durable storage.
type HardRiskSource interface {
	Snapshot(ctx context.Context, intent domain.OrderIntent, observedAt time.Time) (domain.HardRiskSnapshot, error)
}

// MarketUniverse resolves authoritative execution metadata by condition_id.
type MarketUniverse interface {
	FindByCondition(ctx context.Context, conditionID string) (market domain.MarketSnapshot, found bool, err error)
}

// MarketValidator performs the final market identity, state, tick, snapshot,
// and live-price checks immediately before a new order is claimed.
type MarketValidator interface {
	Validate(ctx context.Context, intent domain.OrderIntent) (domain.MarketValidation, error)
}

// PredictionSource reads persisted probabilities through prediction_infra's
// point-in-time HTTP API.
type PredictionSource interface {
	Snapshot(ctx context.Context, decisionAt time.Time, lookback time.Duration) (domain.PredictionSnapshot, error)
}

// OrderBookSource captures one normalized top-N book for every requested
// outcome token. A partial response is allowed and is represented explicitly in
// the strategy request as MISSING.
type OrderBookSource interface {
	Capture(ctx context.Context, decisionAt time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error)
}

// MidPriceHistorySource returns one frozen Polymarket midpoint series for each
// requested outcome token. Implementations may read a rolling cache or call
// /batch-prices-history; per-token failures are represented in-band.
type MidPriceHistorySource interface {
	Capture(ctx context.Context, decisionAt time.Time, lookback time.Duration, targets []domain.BookTarget) ([]domain.MidPriceHistory, error)
}

// StrategyClient sends one model/strategy/execution-account-scoped frozen
// input to the external strategy service and receives audited evaluations.
type StrategyClient interface {
	Decide(ctx context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error)
}

// PositionExitStrategyClient 表示后端使用的 PositionExitStrategyClient 类型。
type PositionExitStrategyClient interface {
	EvaluatePositionExits(ctx context.Context, request domain.PositionExitRequest) (domain.PositionExitResponse, error)
}

// PositionExitRecorder freezes the exact per-lot input before Python is
// called, and freezes its output before any SELL reaches execution.
type PositionExitRecorder interface {
	GetInput(ctx context.Context, cycleID string) (domain.PositionExitRequest, error)
	ClaimInput(ctx context.Context, request domain.PositionExitRequest) (stored domain.PositionExitRequest, created bool, err error)
	GetOutput(ctx context.Context, cycleID string) (domain.PositionExitResponse, error)
	ClaimOutput(ctx context.Context, response domain.PositionExitResponse) (stored domain.PositionExitResponse, created bool, err error)
}

// DecisionRecorder durably records the exact input before strategy evaluation
// and the exact strategy output before any order reaches a venue.
type DecisionRecorder interface {
	// ClaimInput atomically creates the cycle input or returns the exact input
	// already stored for that cycle. A different input for the same cycle must
	// return an idempotency conflict.
	ClaimInput(ctx context.Context, request domain.StrategyDecisionRequest) (stored domain.StrategyDecisionRequest, created bool, err error)
	// ClaimOutput provides the same guarantee before any returned SUBMIT action
	// is converted into an OrderIntent.
	ClaimOutput(ctx context.Context, response domain.StrategyDecisionResponse) (stored domain.StrategyDecisionResponse, created bool, err error)
}

// Rejection 表示后端使用的 Rejection 类型。
type Rejection struct {
	Code   string
	Reason string
}

// Error 返回当前错误模型的可读错误文本。
func (rejection *Rejection) Error() string {
	return fmt.Sprintf("%s: %s", rejection.Code, rejection.Reason)
}

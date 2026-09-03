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
	ErrDecisionRunNotFound     = errors.New("strategy decision run not found")
	ErrDecisionConflict        = errors.New("strategy decision idempotency conflict")
	ErrDecisionIntentNotFound  = errors.New("strategy decision intent not found")
	ErrDecisionIntentConflict  = errors.New("strategy decision intent claim conflict")
	ErrCancelFinalityPending   = errors.New("cancel fill finality window has not elapsed")
)

// ExecutionAccountScope is the process-owned account boundary for live
// mutation paths. Active accounts may create/resume orders; managed accounts
// may additionally receive risk-reducing maintenance such as cancel/refresh
// and explicit reconciliation. Read-only history does not require this gate.
type ExecutionAccountScope interface {
	IsActive(executionAccountID string) bool
	IsManaged(executionAccountID string) bool
}

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
	// ListPendingForAccounts is the production recovery boundary. It must return
	// pending rows only for the explicit active account allowlist; retired or
	// quarantined evidence is left untouched for manual reconciliation.
	ListPendingForAccounts(ctx context.Context, executionAccountIDs []string, before time.Time, limit int) ([]domain.Order, error)
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

// TimeInForceSupport is an optional Venue capability. A venue that reports
// false for IOC has no native immediate-or-cancel order type; execution then
// emulates IOC by placing a share-denominated resting limit order at the
// protection price and cancelling any unfilled remainder immediately after the
// placement response. Venues that do not implement this interface are treated
// as executing every accepted time_in_force natively.
type TimeInForceSupport interface {
	SupportsTimeInForce(timeInForce domain.TimeInForce) bool
}

// PreparedPlacement is an opaque, in-memory-only signed placement. Only its
// expected venue order identity is exposed to the execution service; signed
// bytes and credentials must never be persisted in the order ledger.
type PreparedPlacement interface {
	ExpectedVenueOrderID() string
}

// PreparedVenue separates signing from the state-changing POST. Live venues
// implement this contract so execution can durably persist SUBMITTING and the
// expected signed-order hash before any placement POST bytes leave the process.
type PreparedVenue interface {
	Venue
	PreparePlace(ctx context.Context, order domain.Order) (PreparedPlacement, error)
	PlacePrepared(ctx context.Context, order domain.Order, placement PreparedPlacement) (VenueOrder, error)
}

// FillSource 读取真实成交记录；订单提交状态不属于此接口，也不能据此构造成交记录。
type FillSource interface {
	ListOrderFills(ctx context.Context, order domain.Order) ([]domain.Fill, error)
}

// FillLedger 定义生产环境的成交记账边界，负责原子去重并更新订单、预占、资金、仓位批次、审计事件和 Outbox。
// MATCHED、MINED 和 RETRYING 等非最终状态只保存观察结果，不进入资金与仓位账本。
type FillLedger interface {
	Record(ctx context.Context, order domain.Order, fill domain.Fill) (domain.FillApplication, error)
	GetFill(ctx context.Context, fillKey string) (domain.Fill, error)
	ListOrderFills(ctx context.Context, orderID string) ([]domain.Fill, error)
}

// TradeHistoryRepository 提供已确认且已入账成交与赎回结算的只读视图，并保证明细、总数和汇总使用同一筛选条件。
type TradeHistoryRepository interface {
	ListTradeHistory(ctx context.Context, filter domain.TradeHistoryFilter) (domain.TradeHistoryPage, error)
	ListLedgerActivities(ctx context.Context, filter domain.LedgerActivityFilter) (domain.LedgerActivityPage, error)
	DailyPnL(ctx context.Context, filter domain.DailyPnLFilter) (domain.DailyPnLReport, error)
}

// EdgeDistributionRepository 定义最新全局决策边界冻结输入的只读访问能力。
type EdgeDistributionRepository interface {
	ListLatestDecisionInputs(ctx context.Context, modelID string) ([]domain.StrategyDecisionRequest, error)
}

// LiveOperationsRepository 在同一个 PostgreSQL 可重复读事务中构建实盘监控所需的本地权威视图。
type LiveOperationsRepository interface {
	LoadLiveOperations(ctx context.Context, query domain.LiveOperationsQuery) (domain.LiveOperationsLocalState, error)
}

// LiveOperationsStatusWriter 持久化业务线程 heartbeat 和同一轮完整漏斗。
type LiveOperationsStatusWriter interface {
	ReportLiveWorker(ctx context.Context, state domain.LiveWorkerState) error
	ReportLiveFunnel(ctx context.Context, report domain.LiveFunnelReport) error
}

// PositionLedger 表示后端使用的 PositionLedger 类型。
type PositionLedger interface {
	GetPosition(ctx context.Context, executionAccountID, tokenID string) (domain.Position, error)
	ListLots(ctx context.Context, executionAccountID, tokenID string) ([]domain.PositionLot, error)
	ListOpenLots(ctx context.Context, executionAccountID string) ([]domain.PositionLot, error)
	ListPositionEvents(ctx context.Context, executionAccountID, tokenID string) ([]domain.PositionEvent, error)
	MarkPosition(ctx context.Context, mark domain.PositionMark) (domain.Position, error)
}

// StrategyPositionSource 提供单个隔离执行账户的精确开放批次，退出策略按批次寻址，禁止按 Token 合并。
type StrategyPositionSource interface {
	ListOpenLots(ctx context.Context, executionAccountID string) ([]domain.PositionLot, error)
}

// PositionExitTradeSource 返回开放批次及其有效预占，不能仅根据 Token 净仓位推导可卖数量。
type PositionExitTradeSource interface {
	ListOpenPositionExitTrades(ctx context.Context, executionAccountID string) ([]domain.PositionExitTrade, error)
}

// FundsLedger 表示后端使用的 FundsLedger 类型。
type FundsLedger interface {
	GetBalance(ctx context.Context, executionAccountID string) (domain.AccountBalance, error)
	ListAccountEvents(ctx context.Context, executionAccountID string) ([]domain.AccountEvent, error)
}

// ReconciliationOrderRepository 只暴露本服务创建的订单，不能为了对齐数量而写入外部 CLOB 订单。
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

// ExternalPositionBaselineSource reads immutable cutover evidence for shares
// that are present in a wallet but are outside trading_execution ownership.
type ExternalPositionBaselineSource interface {
	ListExternalPositionBaselines(ctx context.Context, executionAccountID string) ([]domain.ExternalPositionBaseline, error)
}

// ExternalPositionDispositionTradeSource reads exact venue-trade identities
// whose effects on unmanaged baseline shares have already been accounted for.
// The evidence is read-only and does not imply ownership of the venue order.
type ExternalPositionDispositionTradeSource interface {
	ListExternalPositionDispositionTrades(ctx context.Context, executionAccountID string) ([]domain.ExternalPositionDispositionTrade, error)
}

// ExternalBalanceSource 表示后端使用的 ExternalBalanceSource 类型。
type ExternalBalanceSource interface {
	GetExternalBalance(ctx context.Context, walletAddress, asset string) (domain.ExternalBalance, error)
}

// ReconciliationLedger 表示后端使用的 ReconciliationLedger 类型。
type ReconciliationLedger interface {
	GetBalance(ctx context.Context, executionAccountID string) (domain.AccountBalance, error)
	ListPositions(ctx context.Context, executionAccountID string) ([]domain.Position, error)
	// ListFinalityPendingFills returns fills whose on-chain settlement evidence
	// is canonical but still below the configured confirmation depth, so they
	// have not been applied to balances or positions yet. The chain already
	// reflects them; reconciliation adds their exact deltas to the local view.
	ListFinalityPendingFills(ctx context.Context, executionAccountID string) ([]domain.Fill, error)
	MarkPositionSettled(ctx context.Context, executionAccountID, tokenID, sourceReference string, settlementPrice domain.Decimal, observedAt time.Time) (domain.Position, error)
}

// RedemptionStore persists discovery, submit intent, venue identity, receipt
// confirmation, and accounting application as separate idempotent transitions.
type RedemptionStore interface {
	SyncPendingRedemptions(ctx context.Context) error
	ListDueRedemptions(ctx context.Context, limit int, now time.Time) ([]domain.Redemption, error)
	BeginRedemptionSubmission(ctx context.Context, redemption domain.Redemption, kind domain.RedemptionSubmissionKind, startedAt time.Time) error
	RecordRedemptionSubmission(ctx context.Context, redemption domain.Redemption, submission domain.RedemptionSubmission, submittedAt, nextAttemptAt time.Time) error
	ResetRedemptionReady(ctx context.Context, redemption domain.Redemption, nextAttemptAt time.Time) error
	RecordRedemptionTransaction(ctx context.Context, redemption domain.Redemption, transactionHash string, nextAttemptAt time.Time) error
	RecordRedemptionConfirmed(ctx context.Context, redemption domain.Redemption, receipt domain.RedemptionReceipt, confirmedAt time.Time) error
	ApplyRedemption(ctx context.Context, redemption domain.Redemption, appliedAt time.Time) error
	RetryRedemption(ctx context.Context, redemption domain.Redemption, reason string, nextAttemptAt time.Time) error
	ReviewRedemption(ctx context.Context, redemption domain.Redemption, reason string, reviewedAt time.Time) error
}

// RedemptionProgressSource exposes redemptions that may already have burned
// outcome shares and paid collateral on-chain before the ledger applied them.
// Reconciliation consults it so the redeem-to-apply window is not recorded as
// permanent manual drift.
type RedemptionProgressSource interface {
	ListInFlightRedemptions(ctx context.Context, executionAccountID string) ([]domain.InFlightRedemption, error)
	// ListAppliedRedemptions returns redemptions applied to the ledger at or
	// after since, with the exact shares burned per token, so a stale external
	// snapshot that still lists them is not recorded as manual drift.
	ListAppliedRedemptions(ctx context.Context, executionAccountID string, since time.Time) ([]domain.AppliedRedemption, error)
}

type RedeemActivitySource interface {
	ListRedeemActivities(ctx context.Context, walletAddress, conditionID string, start time.Time) ([]domain.RedeemActivity, error)
}

type RedemptionReceiptSource interface {
	ResolveRedemptionReceipt(ctx context.Context, transactionHash, walletAddress, conditionID string, negRisk bool) (domain.RedemptionReceipt, error)
}

type RedemptionVenue interface {
	RedemptionApproved(ctx context.Context, walletAddress string, negRisk bool) (bool, error)
	SubmitRedemptionApproval(ctx context.Context, executionAccountID string, negRisk bool) (domain.RedemptionSubmission, error)
	SubmitRedemption(ctx context.Context, executionAccountID, conditionID string, negRisk bool) (domain.RedemptionSubmission, error)
	ResolveRedemptionSubmission(ctx context.Context, executionAccountID string, submission domain.RedemptionSubmission) (domain.RedemptionSubmission, error)
}

// ReconciliationRecorder 表示后端使用的 ReconciliationRecorder 类型。
type ReconciliationRecorder interface {
	Start(ctx context.Context, run domain.ReconciliationRun) error
	RecordIssue(ctx context.Context, issue domain.ReconciliationIssue) error
	Complete(ctx context.Context, run domain.ReconciliationRun) error
}

// ReconciliationTriggerer 在订单路径中以异步方式触发对账，进程崩溃造成的遗漏由启动扫描补偿。
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

// AssetReservationManager 定义资金与仓位并发预占的权威边界，实盘实现必须在 PostgreSQL 事务中锁定执行账户行。
type AssetReservationManager interface {
	// Reserve 原子检查并锁定 BUY 资金或 SELL 份额，按 client_order_id 保证幂等并拒绝语义被修改的订单。
	Reserve(ctx context.Context, order domain.Order) (domain.AssetReservation, error)
	// Reconcile 消费累计成交并只释放不再需要的预占；取消或拒绝释放未成交部分，完全成交则完成结算。
	Reconcile(ctx context.Context, order domain.Order) (domain.AssetReservation, error)
	// MarkUncertain 在交易所结果不明确时继续保留预占资产。
	MarkUncertain(ctx context.Context, order domain.Order, reason string) error
}

// HardRiskSource 返回一致的账户级风险视图，实盘实现必须包含仓位和活动订单预占，并在持久层串行执行检查与预占。
type HardRiskSource interface {
	Snapshot(ctx context.Context, intent domain.OrderIntent, observedAt time.Time) (domain.HardRiskSnapshot, error)
}

// MarketUniverse 根据 condition_id 解析权威 Market 执行元数据。
type MarketUniverse interface {
	FindByCondition(ctx context.Context, conditionID string) (market domain.MarketSnapshot, found bool, err error)
}

// MarketValidator 在订单被接收前完成 Market 身份、状态、tick、快照和实时价格的最终校验。
type MarketValidator interface {
	Validate(ctx context.Context, intent domain.OrderIntent) (domain.MarketValidation, error)
}

// PredictionSource 通过 prediction_infra 的时点 HTTP 接口读取已持久化的概率数据。
type PredictionSource interface {
	Snapshot(ctx context.Context, decisionAt time.Time, lookback time.Duration) (domain.PredictionSnapshot, error)
}

// OrderBookSource 为每个 Outcome Token 获取标准化的前 N 档订单簿，部分失败在策略请求中显式标记为 MISSING。
type OrderBookSource interface {
	Capture(ctx context.Context, decisionAt time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error)
}

// MidPriceHistorySource 返回每个 Outcome Token 的冻结 Polymarket 中间价序列，单 Token 失败通过响应字段表达。
type MidPriceHistorySource interface {
	Capture(ctx context.Context, decisionAt time.Time, lookback time.Duration, targets []domain.BookTarget) ([]domain.MidPriceHistory, error)
}

// StrategyClient 向外部策略服务发送按模型、策略和执行账户隔离的冻结输入，并接收可审计的评估结果。
type StrategyClient interface {
	Decide(ctx context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error)
}

// PositionExitStrategyClient 表示后端使用的 PositionExitStrategyClient 类型。
type PositionExitStrategyClient interface {
	EvaluatePositionExits(ctx context.Context, request domain.PositionExitRequest) (domain.PositionExitResponse, error)
}

// PositionExitRecorder 在调用 Python 前冻结逐批次输入，并在任何 SELL 进入执行前冻结策略输出。
type PositionExitRecorder interface {
	GetInput(ctx context.Context, cycleID string) (domain.PositionExitRequest, error)
	ClaimInput(ctx context.Context, request domain.PositionExitRequest) (stored domain.PositionExitRequest, created bool, err error)
	GetOutput(ctx context.Context, cycleID string) (domain.PositionExitResponse, error)
	ClaimOutput(ctx context.Context, response domain.PositionExitResponse) (stored domain.PositionExitResponse, created bool, err error)
}

// DecisionRecorder 在策略评估前持久化精确输入，并在订单进入交易所前持久化完整策略输出。
type DecisionRecorder interface {
	// ClaimInput 原子创建周期输入或返回已保存的相同输入；同一周期出现不同输入时返回幂等冲突。
	ClaimInput(ctx context.Context, request domain.StrategyDecisionRequest) (stored domain.StrategyDecisionRequest, created bool, err error)
	// ClaimOutput atomically freezes the validated output and, only when
	// submissionEnabled was true for this first claim, its complete SUBMIT
	// intent set. Replays must use the same mode and exact intent set.
	ClaimOutput(ctx context.Context, response domain.StrategyDecisionResponse, intents []domain.OrderIntent, submissionEnabled bool) (stored domain.StrategyDecisionResponse, created bool, err error)
	// CountUnresolvedIntentsForAccounts is the fail-closed recovery guard for
	// accounts whose automatic submission has been quarantined. PENDING and
	// SUBMITTING rows are unresolved; terminal rows are not recoverable work.
	CountUnresolvedIntentsForAccounts(ctx context.Context, executionAccountIDs []string) (int, error)
	// ClaimPendingIntents exclusively moves PENDING rows for the explicit active
	// account allowlist to SUBMITTING. An empty cycleID claims across cycles for
	// crash recovery; an empty side permits both BUY and SELL. Retired and
	// quarantined account rows remain durable operator-visible evidence.
	ClaimPendingIntents(ctx context.Context, activeExecutionAccountIDs []string, cycleID string, side domain.Side, limit int) ([]domain.DecisionIntentDelivery, error)
	// RequeueStaleSubmitting makes abandoned claims retryable. The stable
	// client_order_id and execution service's durable lookup make this safe;
	// an execution result already recorded as UNKNOWN is never requeued.
	RequeueStaleSubmitting(ctx context.Context, activeExecutionAccountIDs []string, before time.Time, side domain.Side, limit int) (int, error)
	// CompleteIntent uses Attempt as a fencing token and accepts only terminal
	// SUBMITTED, FAILED, or UNKNOWN states.
	CompleteIntent(ctx context.Context, clientOrderID string, attempt int, completion domain.DecisionIntentCompletion) error
	ListIntents(ctx context.Context, cycleID string) ([]domain.DecisionIntentDelivery, error)
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

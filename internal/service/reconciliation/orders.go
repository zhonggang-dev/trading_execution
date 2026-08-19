package reconciliation

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// reconcileOrdersParams 收拢订单对账阶段使用的本地订单、关注订单和外部证据。
type reconcileOrdersParams struct {
	orders       []domain.Order
	focusOrderID string
	evidence     venueEvidence
}

// reconcileOrderParams 收拢单张订单对账所需的上下文证据。
type reconcileOrderParams struct {
	order        domain.Order
	focusOrderID string
	evidence     venueEvidence
}

// orderSourceIssueParams 收拢单张订单外部数据源异常的记录信息。
type orderSourceIssueParams struct {
	order     domain.Order
	source    string
	operation string
	err       error
}

// reconcileOrders 按关注订单优先的顺序逐张恢复成交和订单状态。
func (state *runState) reconcileOrders(ctx context.Context, params reconcileOrdersParams) {
	for _, order := range prioritizeOrder(params.orders, params.focusOrderID) {
		if err := ctx.Err(); err != nil {
			state.errors = append(state.errors, err)
			return
		}
		state.reconcileOrder(ctx, reconcileOrderParams{order: order, focusOrderID: params.focusOrderID, evidence: params.evidence})
	}
}

// reconcileOrder 对单张订单同步成交、处理撤单终局并按需刷新状态。
func (state *runState) reconcileOrder(ctx context.Context, params reconcileOrderParams) {
	if strings.TrimSpace(params.order.VenueOrderID) == "" {
		state.recordUnconfirmedSubmission(ctx, params.order)
		return
	}

	_, tradeReferenced := params.evidence.ordersWithTrades[normalizedID(params.order.VenueOrderID)]
	fillEvidenceComplete := params.evidence.tradesAvailable && !tradeReferenced
	if shouldSyncOrderFills(params.order, params.focusOrderID, tradeReferenced) {
		fillEvidenceComplete = state.syncOrderFills(ctx, params.order)
	}
	if params.order.Status == domain.OrderStatusCancelled {
		state.finalizeCancellationWhenSafe(ctx, params.order, fillEvidenceComplete)
		return
	}
	state.refreshOrder(ctx, params.order, fillEvidenceComplete)
}

// finalizeCancellationWhenSafe 仅在成交证据完整时释放已撤订单的剩余预占。
func (state *runState) finalizeCancellationWhenSafe(ctx context.Context, order domain.Order, fillEvidenceComplete bool) {
	if !fillEvidenceComplete {
		return
	}
	state.finalizeCancellation(ctx, order)
}

// recordUnconfirmedSubmission 记录没有外部订单标识且结果不确定的提交。
func (state *runState) recordUnconfirmedSubmission(ctx context.Context, order domain.Order) {
	if !hasAmbiguousSubmission(order.Status) {
		return
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueSubmitUnconfirmed, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, OrderID: order.ID, MarketID: order.Intent.MarketID,
		ConditionID: order.Intent.ConditionID, TokenID: order.Intent.TokenID, Source: "LOCAL_ORDER_STATE",
		Details: "mutating request outcome is ambiguous and no venue order id is known; automatic resubmission is forbidden",
	})
}

// hasAmbiguousSubmission 判断订单是否可能已经发送但缺少可查询的外部标识。
func hasAmbiguousSubmission(status domain.OrderStatus) bool {
	switch status {
	case domain.OrderStatusSubmitting, domain.OrderStatusUnknown,
		domain.OrderStatusReconciling, domain.OrderStatusCancelPending:
		return true
	default:
		return false
	}
}

// shouldSyncOrderFills 判断本次是否必须执行订单级成交读取。
func shouldSyncOrderFills(order domain.Order, focusOrderID string, tradeReferenced bool) bool {
	return !order.Terminal() || tradeReferenced || order.ID == focusOrderID
}

// syncOrderFills 同步一张订单的真实成交并记录自动补录结果。
func (state *runState) syncOrderFills(ctx context.Context, order domain.Order) bool {
	result, err := state.service.fills.SyncOrder(ctx, order.ID)
	if err != nil {
		state.addOrderSourceIssue(ctx, orderSourceIssueParams{order: order, source: "CLOB_ORDER_TRADES", operation: "read/apply order fills", err: err})
		return false
	}
	state.run.Summary["fill_observations"] += result.Observed
	state.run.Summary["fills_applied"] += result.Applied
	for _, application := range result.Applications {
		state.recordRecoveredFill(ctx, order, application)
	}
	return true
}

// recordRecoveredFill 记录一条通过交易所证据自动补录的真实成交。
func (state *runState) recordRecoveredFill(ctx context.Context, order domain.Order, application domain.FillApplication) {
	if !application.Applied {
		return
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: recoveredFillIssueType(application.Fill.Side), Resolution: domain.ReconciliationResolutionAutomatic,
		Status: domain.ReconciliationIssueResolved, OrderID: order.ID,
		VenueOrderID: order.VenueOrderID, VenueTradeID: application.Fill.VenueFillID,
		MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID, TokenID: order.Intent.TokenID,
		RemoteValue: application.Fill.Shares, Source: "POLYMARKET_TRADES",
		Details: "confirmed venue fill was missing locally and was applied through the idempotent fill ledger",
	})
}

// recoveredFillIssueType 根据成交方向选择漏记成交的问题类型。
func recoveredFillIssueType(side domain.Side) domain.ReconciliationIssueType {
	if side == domain.SideSell {
		return domain.ReconciliationIssueMissedSellFill
	}
	return domain.ReconciliationIssueMissedBuyFill
}

// refreshOrder 刷新一张需要对账的订单，并处理新确认的撤单。
func (state *runState) refreshOrder(ctx context.Context, order domain.Order, fillEvidenceComplete bool) {
	if !orderNeedsRefresh(order) {
		return
	}
	refreshed, err := state.service.orderRefresher.Refresh(ctx, order.ID)
	if err != nil {
		state.addOrderSourceIssue(ctx, orderSourceIssueParams{order: order, source: "CLOB_ORDER", operation: "refresh order state", err: err})
		return
	}
	state.run.Summary["orders_refreshed"]++
	if order.Status == domain.OrderStatusCancelled || refreshed.Status != domain.OrderStatusCancelled {
		return
	}
	state.recordConfirmedCancellation(ctx, order, refreshed)
	state.finalizeCancellationWhenSafe(ctx, refreshed, fillEvidenceComplete)
}

// recordConfirmedCancellation 记录交易所已经证明撤销的本地订单。
func (state *runState) recordConfirmedCancellation(ctx context.Context, order, refreshed domain.Order) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueLocalOrderCancelled, Resolution: domain.ReconciliationResolutionAutomatic,
		Status: domain.ReconciliationIssueResolved, OrderID: order.ID, VenueOrderID: refreshed.VenueOrderID,
		MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID,
		TokenID: order.Intent.TokenID, Source: "POLYMARKET_CLOB",
		Details: "venue proved the order cancelled; unfilled reservation remains protected until the fill propagation grace completes",
	})
}

// addOrderSourceIssue 记录单张订单的外部数据源错误并保留预占。
func (state *runState) addOrderSourceIssue(ctx context.Context, params orderSourceIssueParams) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueSourceUnavailable, Resolution: domain.ReconciliationResolutionRetry,
		Status: domain.ReconciliationIssueOpen, OrderID: params.order.ID, VenueOrderID: params.order.VenueOrderID,
		MarketID: params.order.Intent.MarketID, ConditionID: params.order.Intent.ConditionID,
		TokenID: params.order.Intent.TokenID, Source: params.source,
		Details: params.operation + " failed; preserve state and reservations: " + params.err.Error(),
	})
	state.errors = append(state.errors, params.err)
}

// finalizeCancellation 在宽限期满足时完成已撤订单的最终预占释放。
func (state *runState) finalizeCancellation(ctx context.Context, order domain.Order) {
	_, err := state.service.orderRefresher.FinalizeCancellation(ctx, order.ID)
	if errors.Is(err, port.ErrCancelFinalityPending) {
		state.run.Summary["cancel_finality_pending"]++
		return
	}
	if err != nil {
		state.addOrderSourceIssue(ctx, orderSourceIssueParams{order: order, source: "CANCEL_FINALITY", operation: "release cancelled order reservation", err: err})
		return
	}
	state.run.Summary["cancellations_finalized"]++
}

// orderNeedsRefresh 判断订单当前状态是否需要从交易所刷新。
func orderNeedsRefresh(order domain.Order) bool {
	switch order.Status {
	case domain.OrderStatusSubmitting, domain.OrderStatusAcknowledged, domain.OrderStatusLive,
		domain.OrderStatusPartiallyFilled, domain.OrderStatusUnknown,
		domain.OrderStatusCancelPending, domain.OrderStatusReconciling:
		return strings.TrimSpace(order.VenueOrderID) != ""
	default:
		return false
	}
}

// prioritizeOrder 把即时触发对账的关注订单稳定排列在最前面。
func prioritizeOrder(orders []domain.Order, focus string) []domain.Order {
	result := append([]domain.Order(nil), orders...)
	if focus == "" {
		return result
	}
	sort.SliceStable(result, func(left, right int) bool {
		return result[left].ID == focus && result[right].ID != focus
	})
	return result
}

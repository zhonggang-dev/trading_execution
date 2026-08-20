package reconciliation

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// venueEvidence 保存本次账户级交易扫描对单订单处理有用的最小证据。
type venueEvidence struct {
	tradesAvailable  bool
	ordersWithTrades map[string]struct{}
}

// accountRunScope 保存一次账户对账中各阶段共享且已规范化的查询范围。
type accountRunScope struct {
	executionAccountID string
	focusOrderID       string
	scanAfter          time.Time
}

// reconciliationScanStart 根据触发类型选择全量或有界对账窗口。
func reconciliationScanStart(now time.Time, lookback time.Duration, trigger domain.ReconciliationTrigger) time.Time {
	if trigger == domain.ReconciliationTriggerStartup || trigger == domain.ReconciliationTriggerAssetDrift {
		return time.Time{}
	}
	return now.Add(-lookback)
}

// applyAccountOwnershipBaseline treats a deliberately reconciled current
// ledger snapshot as the permanent ownership boundary for an onboarded wallet.
// Historical venue activity before that timestamp belongs to the retired
// execution system and must not be reported as an unexplained trade by either
// startup or periodic reconciliation. A wallet without a baseline keeps the
// trigger's fail-closed scan range.
func applyAccountOwnershipBaseline(scanAfter time.Time, balance domain.AccountBalance) time.Time {
	if balance.ReconciledAt == nil || balance.ReconciledAt.IsZero() {
		return scanAfter
	}
	baseline := balance.ReconciledAt.UTC()
	if scanAfter.IsZero() || baseline.After(scanAfter) {
		return baseline
	}
	return scanAfter
}

// loadLocalAuthority 读取本地权威账户和订单，任一失败都停止后续外部对账。
func (state *runState) loadLocalAuthority(ctx context.Context, scope accountRunScope) (domain.AccountBalance, []domain.Order, error) {
	balance, err := state.service.ledger.GetBalance(ctx, scope.executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_LEDGER", "read local execution account", err)
		return domain.AccountBalance{}, nil, err
	}
	orders, err := state.service.orders.ListForReconciliation(ctx, scope.executionAccountID, scope.scanAfter)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_ORDERS", "read local orders", err)
		return domain.AccountBalance{}, nil, err
	}
	state.run.Summary["local_orders"] = len(orders)
	return balance, orders, nil
}

// collectVenueEvidence 读取账户级 CLOB 证据并记录不属于本系统的订单和成交。
func (state *runState) collectVenueEvidence(ctx context.Context, scope accountRunScope, orders []domain.Order) (venueEvidence, error) {
	openOrders, openErr := state.service.venue.ListReconciliationOpenOrders(ctx, scope.executionAccountID)
	trades, tradesErr := state.service.venue.ListReconciliationTrades(ctx, scope.executionAccountID, scope.scanAfter)
	state.recordVenueReadError(ctx, "CLOB_OPEN_ORDERS", "read CLOB open orders", openErr)
	state.recordVenueReadError(ctx, "CLOB_TRADES", "read Polymarket trades", tradesErr)
	state.run.Summary["venue_open_orders"] = len(openOrders)
	state.run.Summary["venue_trades"] = len(trades)

	localOrders := indexLocalOrdersByVenueID(orders)
	if openErr == nil {
		state.recordExternalOrders(ctx, openOrders, localOrders)
	}
	ordersWithTrades := make(map[string]struct{})
	if tradesErr == nil {
		ordersWithTrades = state.recordExternalTrades(ctx, trades, localOrders)
	}
	return venueEvidence{
		tradesAvailable:  tradesErr == nil,
		ordersWithTrades: ordersWithTrades,
	}, errors.Join(openErr, tradesErr)
}

// recordVenueReadError 把非空的外部读取错误转换为可追踪的基础设施问题。
func (state *runState) recordVenueReadError(ctx context.Context, source, operation string, err error) {
	if err == nil {
		return
	}
	state.addInfrastructureIssue(ctx, source, operation, err)
}

// indexLocalOrdersByVenueID 构建本系统订单的外部标识集合。
func indexLocalOrdersByVenueID(orders []domain.Order) map[string]struct{} {
	result := make(map[string]struct{}, len(orders))
	for _, order := range orders {
		venueOrderID := normalizedID(order.VenueOrderID)
		if venueOrderID != "" {
			result[venueOrderID] = struct{}{}
		}
	}
	return result
}

// recordExternalOrders 记录只属于钱包而不属于本系统的活动订单。
func (state *runState) recordExternalOrders(ctx context.Context, openOrders []domain.VenueOrderSnapshot, localOrders map[string]struct{}) {
	for _, order := range openOrders {
		if _, owned := localOrders[normalizedID(order.VenueOrderID)]; owned {
			continue
		}
		state.issue(ctx, domain.ReconciliationIssueParams{
			Type: domain.ReconciliationIssueExternalOrder, Resolution: domain.ReconciliationResolutionObserved,
			Status: domain.ReconciliationIssueOpen, VenueOrderID: order.VenueOrderID,
			ConditionID: order.ConditionID, TokenID: order.TokenID, Source: "POLYMARKET_CLOB",
			Details: "wallet open order is not owned by this service; it is visible but will never be cancelled or modified",
		})
	}
}

// recordExternalTrades 建立有成交的订单索引，并记录无法归属到本系统的成交。
func (state *runState) recordExternalTrades(ctx context.Context, trades []domain.VenueTradeSnapshot, localOrders map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{})
	for _, trade := range trades {
		state.recordExternalTradeOrders(ctx, externalTradeOrdersParams{trade: trade, localOrders: localOrders, ordersWithTrades: result})
	}
	return result
}

// externalTradeOrdersParams 收拢处理一笔外部成交时需要共享的索引。
type externalTradeOrdersParams struct {
	trade            domain.VenueTradeSnapshot
	localOrders      map[string]struct{}
	ordersWithTrades map[string]struct{}
}

// recordExternalTradeOrders 处理一笔交易所成交关联的全部订单标识。
func (state *runState) recordExternalTradeOrders(ctx context.Context, params externalTradeOrdersParams) {
	for _, venueOrderID := range params.trade.OrderIDs {
		normalizedOrderID := normalizedID(venueOrderID)
		params.ordersWithTrades[normalizedOrderID] = struct{}{}
		if _, owned := params.localOrders[normalizedOrderID]; owned {
			continue
		}
		state.recordExternalTrade(ctx, params.trade, venueOrderID)
	}
}

// recordExternalTrade 记录一笔无法归属到本系统策略订单的交易所成交。
func (state *runState) recordExternalTrade(ctx context.Context, trade domain.VenueTradeSnapshot, venueOrderID string) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueExternalTrade, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, VenueOrderID: venueOrderID,
		VenueTradeID: trade.VenueTradeID, ConditionID: trade.ConditionID, TokenID: trade.TokenID,
		Source:  "POLYMARKET_CLOB",
		Details: "wallet trade cannot be attributed to a local strategy order; do not manufacture a local fill",
	})
}

// normalizedID 统一外部标识大小写和首尾空白。
func normalizedID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

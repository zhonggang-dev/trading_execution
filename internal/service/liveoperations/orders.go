package liveoperations

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// buildOrders 以本地订单生命周期为主线，用 CLOB Open Orders 验证当前开放状态。
func buildOrders(observedAt time.Time, local []domain.LiveLedgerOrder, observations []accountObservation, quality *qualityCollector) ([]domain.LiveOrder, error) {
	openByKey := indexVenueOpenOrders(observations)
	localVenueKeys := make(map[string]struct{}, len(local))
	result := make([]domain.LiveOrder, 0, len(local))
	for _, item := range local {
		view, err := buildLiveOrder(observedAt, item, openByKey, observations, quality)
		if err != nil {
			return nil, err
		}
		result = append(result, view)
		if item.Order.VenueOrderID != "" {
			localVenueKeys[venueOrderKey(item.Order.Intent.ExecutionAccountID, item.Order.VenueOrderID)] = struct{}{}
		}
	}
	for key := range openByKey {
		if _, found := localVenueKeys[key]; !found {
			quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, "发现 CLOB 开放订单在本系统 Ledger 中没有对应订单")
			break
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].AgeSeconds < result[right].AgeSeconds })
	if result == nil {
		result = []domain.LiveOrder{}
	}
	return result, nil
}

// buildLiveOrder 把单个本地订单和外部开放状态转换成接口订单。
func buildLiveOrder(observedAt time.Time, item domain.LiveLedgerOrder, openByKey map[string]domain.VenueOrderSnapshot, observations []accountObservation, quality *qualityCollector) (domain.LiveOrder, error) {
	order := item.Order
	venueKey := venueOrderKey(order.Intent.ExecutionAccountID, order.VenueOrderID)
	openOrder, venueOpen := openByKey[venueKey]
	status := displayOrderStatus(order, venueOpen, openOrder)
	if venueOpen && venueOrderIdentityMismatch(order, openOrder) {
		quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, "CLOB 开放订单与 Ledger 的 token、condition 或 side 不一致")
	}
	if shouldExpectVenueOpen(order.Status) && accountOpenOrdersAvailable(order.Intent.ExecutionAccountID, observations) && !venueOpen {
		quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, "Ledger 开放订单在 CLOB Open Orders 中缺失，等待成交或撤单对账")
	}
	price := order.Intent.Price
	if price.IsEmpty() {
		price = order.Intent.WorstPrice
	}
	if price.IsEmpty() {
		price = order.AverageFillPrice
	}
	if price.IsEmpty() {
		price = "0"
	}
	priceNumber, err := numberFromDecimal(price)
	if err != nil {
		return domain.LiveOrder{}, fmt.Errorf("order %s price: %w", order.ID, err)
	}
	shares, err := numberFromDecimal(defaultDecimal(order.Intent.Size))
	if err != nil {
		return domain.LiveOrder{}, fmt.Errorf("order %s shares: %w", order.ID, err)
	}
	filled, err := numberFromDecimal(defaultDecimal(order.FilledSize))
	if err != nil {
		return domain.LiveOrder{}, fmt.Errorf("order %s filled shares: %w", order.ID, err)
	}
	age := observedAt.Sub(order.CreatedAt)
	if age < 0 {
		age = 0
	}
	return domain.LiveOrder{
		OrderID: order.ID, ExecutionAccountID: order.Intent.ExecutionAccountID,
		VenueOrderID: order.VenueOrderID, MarketID: order.Intent.MarketID,
		ConditionID: order.Intent.ConditionID, TokenID: order.Intent.TokenID,
		MarketLabel: item.MarketLabel, OutcomeName: order.Intent.OutcomeName,
		Side: order.Intent.Side, Status: status, Price: priceNumber, Shares: shares,
		FilledShares: filled, AgeSeconds: int64(age / time.Second),
		ModelID: order.Intent.ModelID, StrategyID: order.Intent.StrategyID,
		TriggeredBy: triggeredBy(order.Intent), PredictedProbability: orderProbability(order.Intent.Metadata),
		Edge:      orderEdge(order.Intent.Metadata),
		Lifecycle: buildLifecycle(item),
	}, nil
}

// venueOrderIdentityMismatch 核对 CLOB 开放订单与本地冻结订单身份。
func venueOrderIdentityMismatch(order domain.Order, venue domain.VenueOrderSnapshot) bool {
	if order.Intent.TokenID != venue.TokenID || order.Intent.Side != venue.Side {
		return true
	}
	return order.Intent.ConditionID == "" || venue.ConditionID == "" || order.Intent.ConditionID != venue.ConditionID
}

// orderProbability 从冻结订单元数据读取策略使用的预测概率。
func orderProbability(metadata map[string]string) *float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(metadata["predicted_probability"]), 64)
	if err != nil || value < 0 || value > 1 {
		return nil
	}
	return &value
}

// displayOrderStatus 生成经过外部开放订单与真实 Fill 口径约束的展示状态。
func displayOrderStatus(order domain.Order, venueOpen bool, openOrder domain.VenueOrderSnapshot) string {
	if order.Status == domain.OrderStatusCancelPending {
		return "CANCEL_PENDING"
	}
	if order.Status == domain.OrderStatusFilled {
		return "MATCHED"
	}
	if venueOpen {
		filled := order.FilledSize
		if !openOrder.FilledSize.IsEmpty() {
			filled = openOrder.FilledSize
		}
		if sign, err := defaultDecimal(filled).Sign(); err == nil && sign > 0 {
			return "PARTIAL"
		}
		return "LIVE"
	}
	return string(order.Status)
}

// shouldExpectVenueOpen 判断本地状态是否通常应在 CLOB Open Orders 中出现。
func shouldExpectVenueOpen(status domain.OrderStatus) bool {
	return status == domain.OrderStatusAcknowledged || status == domain.OrderStatusLive || status == domain.OrderStatusPartiallyFilled
}

// accountOpenOrdersAvailable 判断指定账户本轮是否成功读取过开放订单。
func accountOpenOrdersAvailable(accountID string, observations []accountObservation) bool {
	for _, observation := range observations {
		if observation.account.ExecutionAccountID == accountID {
			return observation.openOrdersErr == nil
		}
	}
	return false
}

// indexVenueOpenOrders 按账户和 venue_order_id 建立开放订单索引。
func indexVenueOpenOrders(observations []accountObservation) map[string]domain.VenueOrderSnapshot {
	result := make(map[string]domain.VenueOrderSnapshot)
	for _, observation := range observations {
		if observation.openOrdersErr != nil {
			continue
		}
		for _, order := range observation.openOrders {
			result[venueOrderKey(observation.account.ExecutionAccountID, order.VenueOrderID)] = order
		}
	}
	return result
}

// venueOrderKey 生成跨钱包隔离的交易所订单键。
func venueOrderKey(accountID string, venueOrderID string) string {
	return strings.TrimSpace(accountID) + "\x00" + strings.ToLower(strings.TrimSpace(venueOrderID))
}

// triggeredBy 根据退出批次和元数据标识订单触发线程。
func triggeredBy(intent domain.OrderIntent) string {
	if intent.TargetLotID != "" || strings.EqualFold(intent.Metadata["triggered_by"], "monitor") {
		return "monitor"
	}
	if value := strings.TrimSpace(intent.Metadata["triggered_by"]); value != "" {
		return value
	}
	return "cycle"
}

// orderEdge 从冻结订单元数据解析策略 Edge，解析失败时不伪造数值。
func orderEdge(metadata map[string]string) *domain.LiveNumber {
	value := strings.TrimSpace(metadata["strategy_edge"])
	if value == "" {
		return nil
	}
	number, err := domain.NewLiveNumber(domain.Decimal(value))
	if err != nil {
		return nil
	}
	return &number
}

// buildLifecycle 将不可变 order_events 映射成可追溯的订单生命周期。
func buildLifecycle(item domain.LiveLedgerOrder) []domain.LiveLifecycleStep {
	result := make([]domain.LiveLifecycleStep, 0, len(item.Events))
	for _, event := range item.Events {
		state := domain.LiveFlowDone
		if event.ToStatus == domain.OrderStatusPartiallyFilled || event.ToStatus == domain.OrderStatusCancelPending || event.ToStatus == domain.OrderStatusReconciling || event.ToStatus == domain.OrderStatusUnknown {
			state = domain.LiveFlowActive
		}
		if event.ToStatus == domain.OrderStatusRejected || event.ToStatus == domain.OrderStatusManualReview {
			state = domain.LiveFlowWarning
		}
		result = append(result, domain.LiveLifecycleStep{
			Name: lifecycleName(event.ToStatus), Status: state,
			Timestamp: event.OccurredAt, Detail: lifecycleDetail(event),
		})
	}
	if result == nil {
		result = []domain.LiveLifecycleStep{}
	}
	return result
}

// lifecycleName 把内部状态转换成前端易读的生命周期节点名。
func lifecycleName(status domain.OrderStatus) string {
	names := map[domain.OrderStatus]string{
		domain.OrderStatusReceived: "订单已接收", domain.OrderStatusValidating: "执行校验",
		domain.OrderStatusReserved: "资金仓位已预占", domain.OrderStatusSubmitting: "正在提交 CLOB",
		domain.OrderStatusAcknowledged: "CLOB 已接受", domain.OrderStatusLive: "订单挂单中",
		domain.OrderStatusPartiallyFilled: "部分成交", domain.OrderStatusFilled: "成交已验真入账",
		domain.OrderStatusRejected: "订单已拒绝", domain.OrderStatusUnknown: "提交结果未知",
		domain.OrderStatusCancelPending: "撤单竞态检查", domain.OrderStatusCancelled: "订单已取消",
		domain.OrderStatusReconciling: "订单对账中", domain.OrderStatusManualReview: "等待人工核查",
	}
	if value := names[status]; value != "" {
		return value
	}
	return string(status)
}

// lifecycleDetail 生成不包含凭证和签名信息的生命周期说明。
func lifecycleDetail(event domain.OrderEvent) string {
	parts := make([]string, 0, 4)
	if event.ReasonCode != "" {
		parts = append(parts, event.ReasonCode)
	}
	if event.VenueStatus != "" {
		parts = append(parts, "CLOB="+event.VenueStatus)
	}
	if !event.FilledSize.IsEmpty() {
		if sign, err := event.FilledSize.Sign(); err == nil && sign > 0 {
			parts = append(parts, "累计成交 "+event.FilledSize.String()+" shares")
		}
	}
	if len(parts) == 0 {
		return "revision " + strconv.FormatInt(event.Revision, 10)
	}
	return strings.Join(parts, "；")
}

// checkVenueTrades 检查 CLOB /trades 返回的成交是否已经被本地 Ledger 验真入账。
func checkVenueTrades(confirmed map[string]struct{}, observations []accountObservation, quality *qualityCollector) {
	missing := 0
	for _, observation := range observations {
		if observation.tradesErr != nil {
			continue
		}
		for _, trade := range observation.trades {
			if trade.Status != domain.FillStatusConfirmed {
				continue
			}
			key := observation.account.ExecutionAccountID + "\x00" + strings.ToLower(strings.TrimSpace(trade.VenueTradeID))
			if _, found := confirmed[key]; !found {
				missing++
			}
		}
	}
	if missing > 0 {
		quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, fmt.Sprintf("发现 %d 笔 CLOB 成交尚未验真写入 Ledger", missing))
	}
}

// defaultDecimal 把空十进制字段转换成合法零值。
func defaultDecimal(value domain.Decimal) domain.Decimal {
	if value.IsEmpty() {
		return "0"
	}
	return value
}

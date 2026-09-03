package domain

import (
	"fmt"
	"strings"
	"time"
)

const (
	DefaultTradeHistoryLimit = 20
	MaxTradeHistoryLimit     = 100
	DefaultDailyPnLDays      = 14
	MaxDailyPnLDays          = 90
)

// LedgerActivityType 区分统一账本活动的来源：CLOB 买入、CLOB 卖出与链上赎回结算。
type LedgerActivityType string

const (
	LedgerActivityBuy    LedgerActivityType = "BUY"
	LedgerActivitySell   LedgerActivityType = "SELL"
	LedgerActivityRedeem LedgerActivityType = "REDEEM"
)

// LedgerActivity 是交易与结算记录页的统一行模型。BUY/SELL 来自已确认并入账的真实 Fill，
// REDEEM 来自 auto redeem 完成后按原始批次拆分的 position_lot_redemptions；REDEEM 不是 CLOB 成交，
// 因此没有成交价、订单 ID 和流动性角色，这些字段以空值返回并由前端显示为 —。
type LedgerActivity struct {
	ActivityKey        string             `json:"activity_key"`
	ActivityType       LedgerActivityType `json:"activity_type"`
	Venue              string             `json:"venue"`
	ExecutionAccountID string             `json:"execution_account_id"`
	ModelID            string             `json:"model_id"`
	StrategyID         string             `json:"strategy_id"`
	MarketID           string             `json:"market_id"`
	MarketLabel        string             `json:"market_label,omitempty"`
	ConditionID        string             `json:"condition_id,omitempty"`
	TokenID            string             `json:"token_id"`
	OutcomeName        string             `json:"outcome_name,omitempty"`
	LotID              string             `json:"lot_id,omitempty"`
	OrderID            string             `json:"order_id,omitempty"`
	VenueOrderID       string             `json:"venue_order_id,omitempty"`
	VenueTradeID       string             `json:"venue_trade_id,omitempty"`
	OrderStatus        OrderStatus        `json:"order_status,omitempty"`
	LiquidityRole      LiquidityRole      `json:"liquidity_role,omitempty"`
	Shares             Decimal            `json:"shares"`
	Price              Decimal            `json:"price,omitempty"`
	GrossNotional      Decimal            `json:"gross_notional,omitempty"`
	TotalFee           Decimal            `json:"total_fee"`
	NetCashDelta       Decimal            `json:"net_cash_delta"`
	CostBasis          Decimal            `json:"cost_basis,omitempty"`
	SettlementPayout   Decimal            `json:"settlement_payout,omitempty"`
	RealizedPnL        Decimal            `json:"realized_pnl"`
	TransactionHash    string             `json:"transaction_hash,omitempty"`
	OccurredAt         time.Time          `json:"occurred_at"`
	ConfirmedAt        time.Time          `json:"confirmed_at"`
	AppliedAt          time.Time          `json:"applied_at"`
}

// LedgerActivitySummary 汇总当前筛选范围。RealizedPnL = SELL 平仓 PnL + REDEEM PnL；
// 赎回到账单独记入 RedeemPayout，不混入 SellNotional。
type LedgerActivitySummary struct {
	ActivityCount     int64   `json:"activity_count"`
	TradeCount        int64   `json:"trade_count"`
	RedemptionCount   int64   `json:"redemption_count"`
	BuyNotional       Decimal `json:"buy_notional"`
	SellNotional      Decimal `json:"sell_notional"`
	RedeemPayout      Decimal `json:"redeem_payout"`
	NetCashFlow       Decimal `json:"net_cash_flow"`
	TotalFee          Decimal `json:"total_fee"`
	RealizedPnL       Decimal `json:"realized_pnl"`
	SellRealizedPnL   Decimal `json:"sell_realized_pnl"`
	RedeemRealizedPnL Decimal `json:"redeem_realized_pnl"`
}

// LedgerActivityPage 表示后端使用的 LedgerActivityPage 类型。
type LedgerActivityPage struct {
	Items   []LedgerActivity      `json:"items"`
	Summary LedgerActivitySummary `json:"summary"`
	Total   int64                 `json:"total"`
	Limit   int                   `json:"limit"`
	Offset  int                   `json:"offset"`
}

// LedgerActivityFilter 表示后端使用的 LedgerActivityFilter 类型。时间范围作用于 occurred_at：
// Fill 为撮合时间，REDEEM 为入账时间（与 daily-pnl 的归属日期一致）。
type LedgerActivityFilter struct {
	Limit              int
	Offset             int
	From               *time.Time
	To                 *time.Time
	ActivityType       LedgerActivityType
	ModelID            string
	StrategyID         string
	ExecutionAccountID string
	Search             string
}

// TradeRecord 表示一笔已确认且已入账的 Polymarket 成交，并包含运维页面需要的策略身份和批次账本信息。
// 该模型不会暴露钱包地址、凭证、签名或交易所原始响应。
type TradeRecord struct {
	FillKey            string        `json:"fill_key"`
	Venue              string        `json:"venue"`
	VenueTradeID       string        `json:"venue_trade_id"`
	OrderID            string        `json:"order_id"`
	VenueOrderID       string        `json:"venue_order_id"`
	OrderStatus        OrderStatus   `json:"order_status"`
	ExecutionAccountID string        `json:"execution_account_id"`
	ModelID            string        `json:"model_id"`
	StrategyID         string        `json:"strategy_id"`
	MarketID           string        `json:"market_id"`
	MarketLabel        string        `json:"market_label,omitempty"`
	ConditionID        string        `json:"condition_id,omitempty"`
	TokenID            string        `json:"token_id"`
	OutcomeName        string        `json:"outcome_name,omitempty"`
	LotID              string        `json:"lot_id,omitempty"`
	Side               Side          `json:"side"`
	LiquidityRole      LiquidityRole `json:"liquidity_role"`
	Shares             Decimal       `json:"shares"`
	Price              Decimal       `json:"price"`
	GrossNotional      Decimal       `json:"gross_notional"`
	TotalFee           Decimal       `json:"total_fee"`
	NetCashDelta       Decimal       `json:"net_cash_delta"`
	RealizedPnL        Decimal       `json:"realized_pnl"`
	TransactionHash    string        `json:"transaction_hash,omitempty"`
	MatchedAt          time.Time     `json:"matched_at"`
	ConfirmedAt        time.Time     `json:"confirmed_at"`
}

// TradeHistorySummary 表示后端使用的 TradeHistorySummary 类型。
type TradeHistorySummary struct {
	TradeCount   int64   `json:"trade_count"`
	BuyNotional  Decimal `json:"buy_notional"`
	SellNotional Decimal `json:"sell_notional"`
	NetCashFlow  Decimal `json:"net_cash_flow"`
	TotalFee     Decimal `json:"total_fee"`
	RealizedPnL  Decimal `json:"realized_pnl"`
}

// TradeHistoryPage 表示后端使用的 TradeHistoryPage 类型。
type TradeHistoryPage struct {
	Items   []TradeRecord       `json:"items"`
	Summary TradeHistorySummary `json:"summary"`
	Total   int64               `json:"total"`
	Limit   int                 `json:"limit"`
	Offset  int                 `json:"offset"`
}

// TradeHistoryFilter 表示后端使用的 TradeHistoryFilter 类型。
type TradeHistoryFilter struct {
	Limit              int
	Offset             int
	From               *time.Time
	To                 *time.Time
	Side               Side
	ModelID            string
	StrategyID         string
	ExecutionAccountID string
	Search             string
}

// DailyPnLPoint 是一个 UTC 自然日内、按执行账户与开仓策略归因的已实现盈亏。
// RealizedPnL = SELL 平仓 PnL + REDEEM 赎回 PnL，均使用批次账本的净收入和成本口径，
// 不把当前未实现浮盈混入历史收益。ClosedShares 包含赎回份额；ClosedTradeCount 只统计 SELL 平仓，
// 赎回事件单独记入 RedemptionCount / RedemptionPnL，便于前端区分展示。
type DailyPnLPoint struct {
	Day                string  `json:"day"`
	ExecutionAccountID string  `json:"execution_account_id"`
	ModelID            string  `json:"model_id"`
	StrategyID         string  `json:"strategy_id"`
	RealizedPnL        Decimal `json:"realized_pnl"`
	ClosedTradeCount   int64   `json:"closed_trade_count"`
	ClosedShares       Decimal `json:"closed_shares"`
	RedemptionCount    int64   `json:"redemption_count"`
	RedemptionPnL      Decimal `json:"redemption_pnl"`
}

// DailyPnLReport 包含连续自然日。当前启用的绑定即使当天没有平仓也会返回零值点。
type DailyPnLReport struct {
	Items       []DailyPnLPoint `json:"items"`
	Days        int             `json:"days"`
	FromDay     string          `json:"from_day"`
	ToDay       string          `json:"to_day"`
	Timezone    string          `json:"timezone"`
	GeneratedAt time.Time       `json:"generated_at"`
}

// DailyPnLFilter 限制查询窗口；AsOf 由服务端设置，不接受浏览器任意覆盖。
type DailyPnLFilter struct {
	Days int
	AsOf time.Time
}

// Normalize 补齐默认天数与服务端当前时间，并统一为 UTC。
func (filter DailyPnLFilter) Normalize() DailyPnLFilter {
	if filter.Days == 0 {
		filter.Days = DefaultDailyPnLDays
	}
	if filter.AsOf.IsZero() {
		filter.AsOf = time.Now().UTC()
	} else {
		filter.AsOf = filter.AsOf.UTC()
	}
	return filter
}

// Validate 限制可视化窗口，避免绑定数量与日期做无上限笛卡尔积。
func (filter DailyPnLFilter) Validate() error {
	filter = filter.Normalize()
	if filter.Days < 1 || filter.Days > MaxDailyPnLDays {
		return fmt.Errorf("days must be between 1 and %d", MaxDailyPnLDays)
	}
	return nil
}

// Normalize 规范化交易历史筛选条件并补充分页默认值。
func (filter TradeHistoryFilter) Normalize() TradeHistoryFilter {
	if filter.Limit == 0 {
		filter.Limit = DefaultTradeHistoryLimit
	}
	filter.Side = Side(strings.ToUpper(strings.TrimSpace(string(filter.Side))))
	filter.ModelID = strings.TrimSpace(filter.ModelID)
	filter.StrategyID = strings.TrimSpace(filter.StrategyID)
	filter.ExecutionAccountID = strings.TrimSpace(filter.ExecutionAccountID)
	filter.Search = strings.TrimSpace(filter.Search)
	if filter.From != nil {
		value := filter.From.UTC()
		filter.From = &value
	}
	if filter.To != nil {
		value := filter.To.UTC()
		filter.To = &value
	}
	return filter
}

// Validate 校验交易历史筛选条件的分页、方向和时间范围。
func (filter TradeHistoryFilter) Validate() error {
	filter = filter.Normalize()
	if filter.Limit < 1 || filter.Limit > MaxTradeHistoryLimit {
		return fmt.Errorf("limit must be between 1 and %d", MaxTradeHistoryLimit)
	}
	if filter.Offset < 0 {
		return fmt.Errorf("offset must not be negative")
	}
	if filter.Side != "" && filter.Side != SideBuy && filter.Side != SideSell {
		return fmt.Errorf("side must be BUY or SELL")
	}
	if filter.From != nil && filter.To != nil && filter.From.After(*filter.To) {
		return fmt.Errorf("from must not be after to")
	}
	if len(filter.Search) > 200 {
		return fmt.Errorf("search must contain at most 200 characters")
	}
	return nil
}

// Normalize 规范化账本活动筛选条件并补充分页默认值。
func (filter LedgerActivityFilter) Normalize() LedgerActivityFilter {
	if filter.Limit == 0 {
		filter.Limit = DefaultTradeHistoryLimit
	}
	filter.ActivityType = LedgerActivityType(strings.ToUpper(strings.TrimSpace(string(filter.ActivityType))))
	filter.ModelID = strings.TrimSpace(filter.ModelID)
	filter.StrategyID = strings.TrimSpace(filter.StrategyID)
	filter.ExecutionAccountID = strings.TrimSpace(filter.ExecutionAccountID)
	filter.Search = strings.TrimSpace(filter.Search)
	if filter.From != nil {
		value := filter.From.UTC()
		filter.From = &value
	}
	if filter.To != nil {
		value := filter.To.UTC()
		filter.To = &value
	}
	return filter
}

// Validate 校验账本活动筛选条件的分页、活动类型和时间范围。
func (filter LedgerActivityFilter) Validate() error {
	filter = filter.Normalize()
	if filter.Limit < 1 || filter.Limit > MaxTradeHistoryLimit {
		return fmt.Errorf("limit must be between 1 and %d", MaxTradeHistoryLimit)
	}
	if filter.Offset < 0 {
		return fmt.Errorf("offset must not be negative")
	}
	switch filter.ActivityType {
	case "", LedgerActivityBuy, LedgerActivitySell, LedgerActivityRedeem:
	default:
		return fmt.Errorf("activity_type must be BUY, SELL or REDEEM")
	}
	if filter.From != nil && filter.To != nil && filter.From.After(*filter.To) {
		return fmt.Errorf("from must not be after to")
	}
	if len(filter.Search) > 200 {
		return fmt.Errorf("search must contain at most 200 characters")
	}
	return nil
}

package domain

import "time"

// HardRiskLimits 表示执行模块持有的资金与数据新鲜度上限，独立于 Edge、longshot 和止盈止损等策略规则。
type HardRiskLimits struct {
	PolicyID               string        `json:"policy_id"`
	MaxOrderNotional       Decimal       `json:"max_order_notional"`
	MaxMarketExposure      Decimal       `json:"max_market_exposure"`
	MaxStrategyExposure    Decimal       `json:"max_strategy_exposure"`
	MaxWalletExposure      Decimal       `json:"max_wallet_exposure"`
	MaxDailyTradedNotional Decimal       `json:"max_daily_traded_notional"`
	MaxPriceAge            time.Duration `json:"max_price_age"`
	MaxSignalAge           time.Duration `json:"max_signal_age"`
	MaxStateAge            time.Duration `json:"max_state_age"`
}

// TradingControls 表示当前订单上下文对应的 Kill Switch 和暂停控制状态。
type TradingControls struct {
	GlobalKillSwitch       bool   `json:"global_kill_switch"`
	ExecutionAccountPaused bool   `json:"execution_account_paused"`
	StrategyPaused         bool   `json:"strategy_paused"`
	MarketPaused           bool   `json:"market_paused"`
	Reason                 string `json:"reason,omitempty"`
}

// RiskPosition 表示按来源策略归属的钱包当前仓位，RiskValue 是执行系统采用的保守风险金额。
type RiskPosition struct {
	StrategyID      string  `json:"strategy_id"`
	MarketID        string  `json:"market_id"`
	TokenID         string  `json:"token_id"`
	TotalShares     Decimal `json:"total_shares"`
	AvailableShares Decimal `json:"available_shares"`
	ReservedShares  Decimal `json:"reserved_shares"`
	RiskValue       Decimal `json:"risk_value"`
}

// RiskOpenOrder 表示活动且未终结的订单，BUY 订单使用 WorstPrice 计算预占风险敞口。
type RiskOpenOrder struct {
	OrderID       string  `json:"order_id"`
	ClientOrderID string  `json:"client_order_id"`
	StrategyID    string  `json:"strategy_id"`
	MarketID      string  `json:"market_id"`
	TokenID       string  `json:"token_id"`
	Side          Side    `json:"side"`
	RemainingSize Decimal `json:"remaining_size"`
	WorstPrice    Decimal `json:"worst_price"`
}

// HardRiskSnapshot 表示钱包余额、仓位、活动订单、每日交易额、控制状态和限额的一致性快照。
type HardRiskSnapshot struct {
	SnapshotID          string          `json:"snapshot_id"`
	ExecutionAccountID  string          `json:"execution_account_id"`
	ObservedAt          time.Time       `json:"observed_at"`
	TotalBalance        Decimal         `json:"total_balance"`
	AvailableBalance    Decimal         `json:"available_balance"`
	ReservedBalance     Decimal         `json:"reserved_balance"`
	DailyTradedNotional Decimal         `json:"daily_traded_notional"`
	Limits              HardRiskLimits  `json:"limits"`
	Controls            TradingControls `json:"controls"`
	Positions           []RiskPosition  `json:"positions"`
	OpenOrders          []RiskOpenOrder `json:"open_orders"`
}

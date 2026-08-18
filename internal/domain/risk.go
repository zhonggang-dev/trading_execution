package domain

import "time"

// HardRiskLimits are execution-owned monetary and freshness ceilings. They are
// independent of alpha rules such as edge, longshot, take-profit, or stop-loss.
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

// TradingControls is the current kill-switch/pause view for the exact order
// context queried from the risk source.
type TradingControls struct {
	GlobalKillSwitch       bool   `json:"global_kill_switch"`
	ExecutionAccountPaused bool   `json:"execution_account_paused"`
	StrategyPaused         bool   `json:"strategy_paused"`
	MarketPaused           bool   `json:"market_paused"`
	Reason                 string `json:"reason,omitempty"`
}

// RiskPosition is a current wallet position attributed to its originating
// strategy. RiskValue is the execution system's conservative monetary value.
type RiskPosition struct {
	StrategyID      string  `json:"strategy_id"`
	MarketID        string  `json:"market_id"`
	TokenID         string  `json:"token_id"`
	TotalShares     Decimal `json:"total_shares"`
	AvailableShares Decimal `json:"available_shares"`
	ReservedShares  Decimal `json:"reserved_shares"`
	RiskValue       Decimal `json:"risk_value"`
}

// RiskOpenOrder contains only active, non-terminal orders. WorstPrice is used
// to reserve exposure for BUY orders.
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

// HardRiskSnapshot must represent one consistent view of wallet balance,
// positions, active orders, daily turnover, controls, and limits.
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

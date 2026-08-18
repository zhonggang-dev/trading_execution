package domain

import (
	"fmt"
	"strings"
	"time"
)

const (
	DefaultTradeHistoryLimit = 20
	MaxTradeHistoryLimit     = 100
)

// TradeRecord is one confirmed, booked Polymarket fill enriched with the
// strategy identity and lot-level accounting needed by the operations UI.
// It intentionally contains no wallet address, credential, signature, or raw
// venue payload.
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

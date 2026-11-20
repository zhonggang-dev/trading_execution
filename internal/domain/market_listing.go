package domain

import "time"

// MarketListing 表示 Market Universe 列表接口返回的一个可交易二元市场，
// 在执行元数据之外带上成交量与流动性，用于只读的市场筛选，不参与下单校验。
type MarketListing struct {
	MarketID    string          `json:"market_id"`
	ConditionID string          `json:"condition_id"`
	Question    string          `json:"question"`
	Slug        string          `json:"slug,omitempty"`
	EndDate     *time.Time      `json:"end_date,omitempty"`
	Volume      Decimal         `json:"volume"`
	Liquidity   Decimal         `json:"liquidity"`
	Outcomes    []MarketOutcome `json:"outcomes"`
}

// MarketListFilter 描述一页市场列表请求。结果按流动性升序，
// 因此调用方可以从最小的市场开始分页，并在凑够结果后提前停止。
type MarketListFilter struct {
	// MaxVolume、MaxLiquidity 为包含上界，MinLiquidity 为包含下界；空值表示不限制。
	MaxVolume    Decimal
	MaxLiquidity Decimal
	MinLiquidity Decimal
	Offset       int
	Limit        int
}

// MarketListingPage 是一页市场列表。Fetched 为上游返回的原始条数（含被跳过的市场），
// 调用方只能用它判断是否已翻到末页：Fetched 小于请求的 Limit 才表示没有更多。
type MarketListingPage struct {
	Markets []MarketListing
	Fetched int
	// LastLiquidity 是上游返回的最后一条（含被跳过的）的流动性，用于以它为下界开启下一个流动性窗口。
	LastLiquidity Decimal
}

package domain

import "time"

const (
	EdgePriceBasisMidpoint   = "MIDPOINT"
	EdgeOutcomeScopeOutcome0 = "OUTCOME_0"
)

// EdgeDistribution 表示一个冻结十分钟决策边界的跨市场 Edge 分布。
type EdgeDistribution struct {
	DecisionAt   time.Time                `json:"decision_at"`
	GeneratedAt  time.Time                `json:"generated_at"`
	PriceBasis   string                   `json:"price_basis"`
	OutcomeScope string                   `json:"outcome_scope"`
	BinWidth     float64                  `json:"bin_width"`
	RangeMin     float64                  `json:"range_min"`
	RangeMax     float64                  `json:"range_max"`
	Series       []EdgeDistributionSeries `json:"series"`
}

// EdgeDistributionSeries 表示一个逻辑预测模型去重后的样本和统计量。
type EdgeDistributionSeries struct {
	ModelID       string                `json:"model_id"`
	SampleCount   int                   `json:"sample_count"`
	ExcludedCount int                   `json:"excluded_count"`
	Mean          float64               `json:"mean"`
	Median        float64               `json:"median"`
	StandardDev   float64               `json:"standard_deviation"`
	Minimum       float64               `json:"minimum"`
	Maximum       float64               `json:"maximum"`
	PositiveRatio float64               `json:"positive_ratio"`
	Bins          []EdgeDistributionBin `json:"bins"`
}

// EdgeDistributionBin 表示左闭右开分箱，最后一个分箱额外包含最大边界。
type EdgeDistributionBin struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
	Count int     `json:"count"`
	Ratio float64 `json:"ratio"`
}

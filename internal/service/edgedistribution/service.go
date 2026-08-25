package edgedistribution

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	binWidth        = 0.05
	minimumAbsRange = 0.10
)

var ErrNotFound = errors.New("edge distribution decision snapshot not found")

// Service 提供最新 Edge 分布的查询和计算能力。
type Service struct {
	repository port.EdgeDistributionRepository
	now        func() time.Time
}

// New 创建 Edge 分布服务并校验依赖。
func New(repository port.EdgeDistributionRepository, now func() time.Time) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("edge distribution repository is required")
	}
	if now == nil {
		now = time.Now
	}
	return &Service{repository: repository, now: now}, nil
}

// Latest 基于最新十分钟决策快照计算 outcome 0 的 Edge 分布。
func (service *Service) Latest(ctx context.Context, modelID string) (domain.EdgeDistribution, error) {
	inputs, err := service.repository.ListLatestDecisionInputs(ctx, strings.TrimSpace(modelID))
	if err != nil {
		return domain.EdgeDistribution{}, err
	}
	decisionAt, err := commonDecisionAt(inputs)
	if err != nil {
		return domain.EdgeDistribution{}, err
	}
	seriesSamples, err := collectModelSamples(inputs)
	if err != nil {
		return domain.EdgeDistribution{}, err
	}
	modelIDs, absRange := distributionShape(seriesSamples)
	return service.buildDistribution(decisionAt, modelIDs, seriesSamples, absRange), nil
}

// edgeSample 表示一个去重后的有效或排除样本。
type edgeSample struct {
	value float64
	valid bool
}

// modelSamples 保存单个模型按预测和 token 去重后的样本。
type modelSamples struct {
	byKey map[string]edgeSample
}

// distributionStats 保存一组有效 Edge 样本的统计量。
type distributionStats struct {
	mean          float64
	median        float64
	standardDev   float64
	minimum       float64
	maximum       float64
	positiveRatio float64
}

// newModelSamples 创建单个模型的样本集合。
func newModelSamples() *modelSamples {
	return &modelSamples{byKey: make(map[string]edgeSample)}
}

// record 记录去重样本，并在重复数据中优先保留有效样本。
func (samples *modelSamples) record(key string, sample edgeSample) {
	current, exists := samples.byKey[key]
	if !exists || (!current.valid && sample.valid) {
		samples.byKey[key] = sample
	}
}

// summary 返回有效样本值和去重后的排除数量。
func (samples *modelSamples) summary() ([]float64, int) {
	values := make([]float64, 0, len(samples.byKey))
	for _, sample := range samples.byKey {
		if sample.valid {
			values = append(values, sample.value)
		}
	}
	return values, len(samples.byKey) - len(values)
}

// commonDecisionAt 校验所有输入属于同一个十分钟决策边界。
func commonDecisionAt(inputs []domain.StrategyDecisionRequest) (time.Time, error) {
	if len(inputs) == 0 {
		return time.Time{}, ErrNotFound
	}
	decisionAt := inputs[0].DecisionAt.UTC()
	if decisionAt.IsZero() {
		return time.Time{}, fmt.Errorf("latest strategy input has an empty decision_at")
	}
	for _, input := range inputs[1:] {
		if input.DecisionAt.IsZero() || !input.DecisionAt.Equal(decisionAt) {
			return time.Time{}, fmt.Errorf("latest strategy inputs span multiple decision boundaries")
		}
	}
	return decisionAt, nil
}

// collectModelSamples 按逻辑模型汇总并去重最新快照中的 Edge 样本。
func collectModelSamples(inputs []domain.StrategyDecisionRequest) (map[string]*modelSamples, error) {
	result := make(map[string]*modelSamples)
	for _, input := range inputs {
		if err := collectInputSamples(input, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// collectInputSamples 采集一个策略输入中的 outcome 0 Edge 样本。
func collectInputSamples(input domain.StrategyDecisionRequest, result map[string]*modelSamples) error {
	modelID := strings.TrimSpace(input.Context.ModelID)
	if modelID == "" {
		return fmt.Errorf("latest strategy input has an empty model_id")
	}
	samples := result[modelID]
	if samples == nil {
		samples = newModelSamples()
		result[modelID] = samples
	}
	books := outcomeZeroBooks(input.OrderBooks)
	for _, prediction := range input.Predictions {
		key, sample, err := predictionEdgeSample(prediction, books)
		if err != nil {
			return err
		}
		samples.record(key, sample)
	}
	return nil
}

// predictionEdgeSample 将一个预测转换为可去重的 Edge 样本。
func predictionEdgeSample(prediction domain.Prediction, books map[string]domain.OrderBookSnapshot) (string, edgeSample, error) {
	predictionID := strings.TrimSpace(prediction.PredictionID)
	if predictionID == "" {
		return "", edgeSample{}, fmt.Errorf("latest strategy input has an empty prediction_id")
	}
	outcome, found := outcomeZero(prediction.Outcomes)
	if !found {
		return predictionID + "\x00outcome-0-missing", edgeSample{}, nil
	}
	key := predictionID + "\x00" + outcome.TokenID
	book, found := books[outcome.TokenID]
	if !found {
		return key, edgeSample{}, nil
	}
	value, valid := calculateEdge(outcome.Probability, book)
	return key, edgeSample{value: value, valid: valid}, nil
}

// calculateEdge 使用买一卖一中间价计算模型概率减市场概率。
func calculateEdge(modelProbability float64, book domain.OrderBookSnapshot) (float64, bool) {
	if book.Status != domain.OrderBookStatusOK || book.BestBid.IsEmpty() || book.BestAsk.IsEmpty() || !validProbability(modelProbability) {
		return 0, false
	}
	bid, bidErr := decimalFloat(book.BestBid)
	ask, askErr := decimalFloat(book.BestAsk)
	if bidErr != nil || askErr != nil || bid <= 0 || ask <= 0 || bid > ask || ask > 1 {
		return 0, false
	}
	return round(modelProbability - (bid+ask)/2), true
}

// validProbability 判断数值是否为合法概率。
func validProbability(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 1
}

// outcomeZero 查找并规范化 outcome 0。
func outcomeZero(outcomes []domain.PredictionOutcome) (domain.PredictionOutcome, bool) {
	for _, outcome := range outcomes {
		if outcome.Index == 0 && strings.TrimSpace(outcome.TokenID) != "" {
			outcome.TokenID = strings.TrimSpace(outcome.TokenID)
			return outcome, true
		}
	}
	return domain.PredictionOutcome{}, false
}

// outcomeZeroBooks 按 token 建立有效 outcome 0 盘口快照索引。
func outcomeZeroBooks(books []domain.OrderBookSnapshot) map[string]domain.OrderBookSnapshot {
	result := make(map[string]domain.OrderBookSnapshot)
	for _, book := range books {
		tokenID := strings.TrimSpace(book.TokenID)
		if book.OutcomeIndex == 0 && tokenID != "" {
			result[tokenID] = book
		}
	}
	return result
}

// decimalFloat 将领域 Decimal 安全转换为有限浮点数。
func decimalFloat(value domain.Decimal) (float64, error) {
	parsed, err := strconv.ParseFloat(value.String(), 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("invalid decimal %q", value)
	}
	return parsed, nil
}

// distributionShape 返回排序后的模型列表和对称图表范围。
func distributionShape(seriesSamples map[string]*modelSamples) ([]string, float64) {
	modelIDs := make([]string, 0, len(seriesSamples))
	maximumAbs := 0.0
	for modelID, samples := range seriesSamples {
		modelIDs = append(modelIDs, modelID)
		values, _ := samples.summary()
		for _, value := range values {
			maximumAbs = math.Max(maximumAbs, math.Abs(value))
		}
	}
	sort.Strings(modelIDs)
	absRange := math.Max(minimumAbsRange, math.Ceil(maximumAbs/binWidth)*binWidth)
	return modelIDs, round(math.Min(1, absRange))
}

// buildDistribution 组装 API 返回的完整 Edge 分布快照。
func (service *Service) buildDistribution(decisionAt time.Time, modelIDs []string, samples map[string]*modelSamples, absRange float64) domain.EdgeDistribution {
	result := domain.EdgeDistribution{
		DecisionAt: decisionAt, GeneratedAt: service.now().UTC(),
		PriceBasis: domain.EdgePriceBasisMidpoint, OutcomeScope: domain.EdgeOutcomeScopeOutcome0,
		BinWidth: binWidth, RangeMin: -absRange, RangeMax: absRange,
		Series: make([]domain.EdgeDistributionSeries, 0, len(modelIDs)),
	}
	for _, modelID := range modelIDs {
		result.Series = append(result.Series, buildSeries(modelID, samples[modelID], absRange))
	}
	return result
}

// buildSeries 计算单个模型的分箱和汇总统计。
func buildSeries(modelID string, samples *modelSamples, absRange float64) domain.EdgeDistributionSeries {
	values, excluded := samples.summary()
	series := domain.EdgeDistributionSeries{
		ModelID: modelID, SampleCount: len(values), ExcludedCount: excluded,
		Bins: buildBins(values, absRange),
	}
	if len(values) == 0 {
		return series
	}
	stats := calculateStats(values)
	series.Mean = stats.mean
	series.Median = stats.median
	series.StandardDev = stats.standardDev
	series.Minimum = stats.minimum
	series.Maximum = stats.maximum
	series.PositiveRatio = stats.positiveRatio
	return series
}

// calculateStats 计算有效样本的均值、中位数、标准差和正 Edge 比例。
func calculateStats(values []float64) distributionStats {
	sortedValues := append([]float64(nil), values...)
	sort.Float64s(sortedValues)
	sum, positive := 0.0, 0
	for _, value := range sortedValues {
		sum += value
		if value > 0 {
			positive++
		}
	}
	mean := sum / float64(len(sortedValues))
	variance := populationVariance(sortedValues, mean)
	return distributionStats{
		mean: round(mean), median: round(median(sortedValues)), standardDev: round(math.Sqrt(variance)),
		minimum: sortedValues[0], maximum: sortedValues[len(sortedValues)-1],
		positiveRatio: round(float64(positive) / float64(len(sortedValues))),
	}
}

// populationVariance 计算总体方差。
func populationVariance(values []float64, mean float64) float64 {
	variance := 0.0
	for _, value := range values {
		delta := value - mean
		variance += delta * delta
	}
	return variance / float64(len(values))
}

// median 计算已排序样本的中位数。
func median(sortedValues []float64) float64 {
	middle := len(sortedValues) / 2
	if len(sortedValues)%2 == 0 {
		return (sortedValues[middle-1] + sortedValues[middle]) / 2
	}
	return sortedValues[middle]
}

// buildBins 按固定 5 个百分点宽度生成对称直方图分箱。
func buildBins(values []float64, absRange float64) []domain.EdgeDistributionBin {
	bins := emptyBins(absRange)
	for _, value := range values {
		bins[binIndex(value, absRange, len(bins))].Count++
	}
	if len(values) > 0 {
		applyBinRatios(bins, len(values))
	}
	return bins
}

// emptyBins 创建指定对称范围内的空分箱。
func emptyBins(absRange float64) []domain.EdgeDistributionBin {
	count := int(math.Round((2 * absRange) / binWidth))
	if count < 1 {
		count = 1
	}
	bins := make([]domain.EdgeDistributionBin, count)
	for index := range bins {
		bins[index].Lower = round(-absRange + float64(index)*binWidth)
		bins[index].Upper = round(bins[index].Lower + binWidth)
	}
	return bins
}

// binIndex 将 Edge 值限制到对应的分箱下标。
func binIndex(value, absRange float64, count int) int {
	index := int(math.Floor((value + absRange) / binWidth))
	if index < 0 {
		return 0
	}
	if index >= count {
		return count - 1
	}
	return index
}

// applyBinRatios 根据样本总数填充分箱比例。
func applyBinRatios(bins []domain.EdgeDistributionBin, sampleCount int) {
	for index := range bins {
		bins[index].Ratio = round(float64(bins[index].Count) / float64(sampleCount))
	}
}

// round 统一 Edge 计算的浮点精度。
func round(value float64) float64 {
	return math.Round(value*1e10) / 1e10
}

package edgedistribution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type fakeRepository struct {
	inputs  []domain.StrategyDecisionRequest
	err     error
	modelID string
}

// ListLatestDecisionInputs 返回测试预置的最新决策输入。
func (repository *fakeRepository) ListLatestDecisionInputs(_ context.Context, modelID string) ([]domain.StrategyDecisionRequest, error) {
	repository.modelID = modelID
	return repository.inputs, repository.err
}

// TestLatestBuildsDeduplicatedModelDistributions 验证多模型去重、统计和分箱结果。
func TestLatestBuildsDeduplicatedModelDistributions(t *testing.T) {
	decisionAt := time.Date(2026, 8, 25, 4, 20, 0, 0, time.UTC)
	generatedAt := decisionAt.Add(time.Minute)
	modelA := decisionInput(decisionAt, "model-a", "strategy-1", []predictionFixture{
		{id: "prediction-1", tokenID: "token-1", probability: 0.70, bid: "0.58", ask: "0.62"},
		{id: "prediction-2", tokenID: "token-2", probability: 0.40, bid: "0.48", ask: "0.52"},
		{id: "prediction-3", tokenID: "token-3", probability: 0.60},
	})
	duplicate := decisionInput(decisionAt, "model-a", "strategy-2", []predictionFixture{
		{id: "prediction-1", tokenID: "token-1", probability: 0.70, bid: "0.58", ask: "0.62"},
		{id: "prediction-2", tokenID: "token-2", probability: 0.40, bid: "0.48", ask: "0.52"},
		{id: "prediction-3", tokenID: "token-3", probability: 0.60},
	})
	modelB := decisionInput(decisionAt, "model-b", "strategy-1", []predictionFixture{
		{id: "prediction-4", tokenID: "token-4", probability: 0.80, bid: "0.68", ask: "0.72"},
	})
	repository := &fakeRepository{inputs: []domain.StrategyDecisionRequest{duplicate, modelB, modelA}}
	service, err := New(repository, func() time.Time { return generatedAt })
	if err != nil {
		t.Fatal(err)
	}

	distribution, err := service.Latest(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if repository.modelID != "" || !distribution.DecisionAt.Equal(decisionAt) || !distribution.GeneratedAt.Equal(generatedAt) {
		t.Fatalf("distribution identity = %#v, repository model = %q", distribution, repository.modelID)
	}
	if distribution.PriceBasis != domain.EdgePriceBasisMidpoint || distribution.OutcomeScope != domain.EdgeOutcomeScopeOutcome0 ||
		distribution.RangeMin != -0.1 || distribution.RangeMax != 0.1 || distribution.BinWidth != 0.05 {
		t.Fatalf("distribution metadata = %#v", distribution)
	}
	if len(distribution.Series) != 2 || distribution.Series[0].ModelID != "model-a" || distribution.Series[1].ModelID != "model-b" {
		t.Fatalf("series ordering = %#v", distribution.Series)
	}
	series := distribution.Series[0]
	if series.SampleCount != 2 || series.ExcludedCount != 1 || series.Mean != 0 || series.Median != 0 ||
		series.StandardDev != 0.1 || series.Minimum != -0.1 || series.Maximum != 0.1 || series.PositiveRatio != 0.5 {
		t.Fatalf("model-a series = %#v", series)
	}
	if len(series.Bins) != 4 || series.Bins[0].Count != 1 || series.Bins[3].Count != 1 {
		t.Fatalf("model-a bins = %#v", series.Bins)
	}
}

// TestLatestPrefersValidDuplicate 验证重复预测中有效盘口不会被先出现的无效副本覆盖。
func TestLatestPrefersValidDuplicate(t *testing.T) {
	decisionAt := time.Date(2026, 8, 25, 4, 20, 0, 0, time.UTC)
	invalid := decisionInput(decisionAt, "model-a", "strategy-1", []predictionFixture{
		{id: "prediction-1", tokenID: "token-1", probability: 0.70},
	})
	valid := decisionInput(decisionAt, "model-a", "strategy-2", []predictionFixture{
		{id: "prediction-1", tokenID: "token-1", probability: 0.70, bid: "0.58", ask: "0.62"},
	})
	service, err := New(&fakeRepository{inputs: []domain.StrategyDecisionRequest{invalid, valid}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	distribution, err := service.Latest(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	series := distribution.Series[0]
	if series.SampleCount != 1 || series.ExcludedCount != 0 || series.Mean != 0.1 {
		t.Fatalf("valid duplicate series = %#v", series)
	}
}

// TestLatestExcludesMalformedSamples 验证缺失 outcome、非法概率和非法盘口被计入排除数量。
func TestLatestExcludesMalformedSamples(t *testing.T) {
	decisionAt := time.Date(2026, 8, 25, 4, 20, 0, 0, time.UTC)
	input := decisionInput(decisionAt, "model-a", "strategy-1", []predictionFixture{
		{id: "missing-outcome", tokenID: "token-1", probability: 0.60, bid: "0.48", ask: "0.52"},
		{id: "bad-probability", tokenID: "token-2", probability: 1.20, bid: "0.48", ask: "0.52"},
		{id: "bad-book", tokenID: "token-3", probability: 0.60, bid: "0.70", ask: "0.60"},
	})
	input.Predictions[0].Outcomes = nil
	service, err := New(&fakeRepository{inputs: []domain.StrategyDecisionRequest{input}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	distribution, err := service.Latest(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	series := distribution.Series[0]
	if series.SampleCount != 0 || series.ExcludedCount != 3 {
		t.Fatalf("malformed sample series = %#v", series)
	}
}

// TestBuildBinsHandlesBoundaries 验证左右边界和零值都只落入一个分箱。
func TestBuildBinsHandlesBoundaries(t *testing.T) {
	bins := buildBins([]float64{-0.10, -0.05, 0, 0.10}, 0.10)
	if len(bins) != 4 {
		t.Fatalf("bin count = %d", len(bins))
	}
	for index, bin := range bins {
		if bin.Count != 1 || bin.Ratio != 0.25 {
			t.Fatalf("bin %d = %#v", index, bin)
		}
	}
}

// TestLatestTrimsModelFilterAndPropagatesRepositoryError 验证模型筛选被规范化且仓储错误原样返回。
func TestLatestTrimsModelFilterAndPropagatesRepositoryError(t *testing.T) {
	repositoryErr := errors.New("repository unavailable")
	repository := &fakeRepository{err: repositoryErr}
	service, err := New(repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Latest(context.Background(), " model-a "); !errors.Is(err, repositoryErr) {
		t.Fatalf("repository error = %v", err)
	}
	if repository.modelID != "model-a" {
		t.Fatalf("repository model = %q", repository.modelID)
	}
}

// TestLatestReturnsNotFoundAndRejectsMixedBoundaries 验证空快照和跨边界快照会失败。
func TestLatestReturnsNotFoundAndRejectsMixedBoundaries(t *testing.T) {
	service, err := New(&fakeRepository{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Latest(context.Background(), ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty latest error = %v", err)
	}

	decisionAt := time.Date(2026, 8, 25, 4, 20, 0, 0, time.UTC)
	mixed, err := New(&fakeRepository{inputs: []domain.StrategyDecisionRequest{
		decisionInput(decisionAt, "model-a", "strategy-1", nil),
		decisionInput(decisionAt.Add(10*time.Minute), "model-b", "strategy-1", nil),
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mixed.Latest(context.Background(), ""); err == nil {
		t.Fatal("mixed decision boundaries must fail")
	}
}

type predictionFixture struct {
	id          string
	tokenID     string
	probability float64
	bid         domain.Decimal
	ask         domain.Decimal
}

// decisionInput 构造包含预测和盘口的测试决策输入。
func decisionInput(decisionAt time.Time, modelID, strategyID string, fixtures []predictionFixture) domain.StrategyDecisionRequest {
	request := domain.StrategyDecisionRequest{
		DecisionAt: decisionAt,
		Context: domain.StrategyExecutionContext{
			ModelID: modelID, StrategyID: strategyID, ExecutionAccountID: "wallet-1",
		},
		Predictions: make([]domain.Prediction, 0, len(fixtures)),
		OrderBooks:  make([]domain.OrderBookSnapshot, 0, len(fixtures)),
	}
	for index, fixture := range fixtures {
		request.Predictions = append(request.Predictions, domain.Prediction{
			PredictionID: fixture.id,
			Outcomes: []domain.PredictionOutcome{
				{Index: 0, TokenID: fixture.tokenID, Probability: fixture.probability},
				{Index: 1, TokenID: fixture.tokenID + "-no", Probability: 1 - fixture.probability},
			},
		})
		if fixture.bid.IsEmpty() || fixture.ask.IsEmpty() {
			continue
		}
		request.OrderBooks = append(request.OrderBooks, domain.OrderBookSnapshot{
			MarketID: "market", ConditionID: "condition", OutcomeIndex: 0, TokenID: fixture.tokenID,
			Status: domain.OrderBookStatusOK, BestBid: fixture.bid, BestAsk: fixture.ask,
			ObservedAt: decisionAt.Add(time.Duration(index) * time.Second),
		})
	}
	return request
}

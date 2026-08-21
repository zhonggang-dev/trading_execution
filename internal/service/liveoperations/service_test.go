package liveoperations

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// fakeRepository 返回测试冻结的 PostgreSQL 本地状态。
type fakeRepository struct {
	state domain.LiveOperationsLocalState
	err   error
}

// LoadLiveOperations 返回预设本地状态。
func (repository *fakeRepository) LoadLiveOperations(context.Context, domain.LiveOperationsQuery) (domain.LiveOperationsLocalState, error) {
	return repository.state, repository.err
}

// fakeVenue 返回测试冻结的 CLOB 开放订单和成交。
type fakeVenue struct {
	orders    []domain.VenueOrderSnapshot
	trades    []domain.VenueTradeSnapshot
	ordersErr error
	tradesErr error
}

// ListReconciliationOpenOrders 返回预设开放订单。
func (venue *fakeVenue) ListReconciliationOpenOrders(context.Context, string) ([]domain.VenueOrderSnapshot, error) {
	return venue.orders, venue.ordersErr
}

// ListReconciliationTrades 返回预设真实成交。
func (venue *fakeVenue) ListReconciliationTrades(context.Context, string, time.Time) ([]domain.VenueTradeSnapshot, error) {
	return venue.trades, venue.tradesErr
}

// fakePositionSource 返回测试冻结的外部持仓。
type fakePositionSource struct {
	positions []domain.ExternalPosition
	err       error
}

// fakePositionBaselineSource returns sealed unmanaged position evidence.
type fakePositionBaselineSource struct {
	baselines []domain.ExternalPositionBaseline
	err       error
}

// ListExternalPositionBaselines returns the configured account cutover items.
func (source *fakePositionBaselineSource) ListExternalPositionBaselines(context.Context, string) ([]domain.ExternalPositionBaseline, error) {
	return source.baselines, source.err
}

// ListExternalPositions 返回预设外部持仓。
func (source *fakePositionSource) ListExternalPositions(context.Context, string) ([]domain.ExternalPosition, error) {
	return source.positions, source.err
}

// fakeBalanceSource 返回测试冻结的钱包余额。
type fakeBalanceSource struct {
	balance domain.ExternalBalance
	err     error
}

// GetExternalBalance 返回预设钱包余额。
func (source *fakeBalanceSource) GetExternalBalance(context.Context, string, string) (domain.ExternalBalance, error) {
	return source.balance, source.err
}

// fakeBookSource 返回测试冻结的盘口。
type fakeBookSource struct {
	books []domain.OrderBookSnapshot
	err   error
}

// Capture 返回预设盘口快照。
func (source *fakeBookSource) Capture(context.Context, time.Time, []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	return source.books, source.err
}

// TestRefreshBuildsAuthoritativeSnapshot 验证资金和仓位按真实外部事实与精确公式生成。
func TestRefreshBuildsAuthoritativeSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 19, 8, 0, 10, 0, time.UTC)
	service, _, _ := newTestService(t, &now)
	if err := service.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	snapshot, err := service.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.Capital.AvailableCash != "100" || snapshot.Capital.GrossExposure != "4" || snapshot.Capital.Equity != "105.5" {
		t.Fatalf("capital = %#v", snapshot.Capital)
	}
	if len(snapshot.Positions) != 1 || snapshot.Positions[0].MarkPrice != "0.55" || snapshot.Positions[0].UnrealizedPnL != "1.5" {
		t.Fatalf("positions = %#v", snapshot.Positions)
	}
	if snapshot.DataFreshnessSeconds != 2 || snapshot.Engine.Health != domain.LiveHealthHealthy {
		t.Fatalf("freshness=%d engine=%#v", snapshot.DataFreshnessSeconds, snapshot.Engine)
	}
	if snapshot.Orders == nil || snapshot.Events == nil || len(snapshot.Workers) != 3 || len(snapshot.Funnel) != 6 {
		t.Fatalf("array contract is not complete: %#v", snapshot)
	}
}

// TestRefreshKeepsLastGoodSnapshotAndEventuallyFailsClosed 验证核心余额失败不会覆盖缓存且过期后返回 503 语义错误。
func TestRefreshKeepsLastGoodSnapshotAndEventuallyFailsClosed(t *testing.T) {
	now := time.Date(2026, 8, 19, 8, 0, 10, 0, time.UTC)
	service, balance, _ := newTestService(t, &now)
	if err := service.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	balance.err = errors.New("rpc unavailable")
	if err := service.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh() error = nil, want core source failure")
	}
	snapshot, err := service.Snapshot(context.Background())
	if err != nil || snapshot.Engine.Health != domain.LiveHealthDegraded {
		t.Fatalf("stale-but-valid snapshot health=%s error=%v", snapshot.Engine.Health, err)
	}
	now = now.Add(20 * time.Second)
	if _, err := service.Snapshot(context.Background()); !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("Snapshot() error = %v, want ErrSnapshotUnavailable", err)
	}
}

// TestPositionDriftAndMissingWorkerCannotAppearHealthy 验证数量漂移和缺失 heartbeat 会传播到引擎健康状态。
func TestPositionDriftAndMissingWorkerCannotAppearHealthy(t *testing.T) {
	now := time.Date(2026, 8, 19, 8, 0, 10, 0, time.UTC)
	service, _, repository := newTestService(t, &now)
	repository.state.Positions[0].Position.TotalShares = "9"
	repository.state.Workers = nil
	if err := service.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Engine.Health != domain.LiveHealthStopped || dataQualityStatus(snapshot.DataQuality, "positions") != domain.LiveHealthDegraded {
		t.Fatalf("engine=%s dataQuality=%#v", snapshot.Engine.Health, snapshot.DataQuality)
	}
}

func TestExternalPositionBaselineOnlyIsExcludedFromManagedOperations(t *testing.T) {
	now := time.Date(2026, 8, 19, 8, 0, 10, 0, time.UTC)
	service, _, repository := newTestService(t, &now)
	repository.state.Positions = nil
	service.positionSource.(*fakePositionSource).positions = []domain.ExternalPosition{
		testLiveExternalPosition(&now, "5"),
	}
	service.positionBaselines.(*fakePositionBaselineSource).baselines = []domain.ExternalPositionBaseline{
		testLivePositionBaseline(&now, "5"),
	}

	if err := service.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	snapshot, err := service.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Positions) != 0 || snapshot.Capital.GrossExposure != "0" || snapshot.Capital.Equity != "100" {
		t.Fatalf("unmanaged baseline leaked into operations snapshot: positions=%#v capital=%#v", snapshot.Positions, snapshot.Capital)
	}
	if snapshot.Engine.Health != domain.LiveHealthHealthy || dataQualityStatus(snapshot.DataQuality, "positions") != domain.LiveHealthHealthy {
		t.Fatalf("baseline-only position degraded operations health: engine=%s quality=%#v", snapshot.Engine.Health, snapshot.DataQuality)
	}
}

func TestExternalPositionBaselineProjectsRemoteTotalToManagedShares(t *testing.T) {
	now := time.Date(2026, 8, 19, 8, 0, 10, 0, time.UTC)
	service, _, _ := newTestService(t, &now)
	service.positionSource.(*fakePositionSource).positions[0].Shares = "15"
	service.positionBaselines.(*fakePositionBaselineSource).baselines = []domain.ExternalPositionBaseline{
		testLivePositionBaseline(&now, "5"),
	}

	if err := service.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	snapshot, err := service.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Positions) != 1 || snapshot.Positions[0].Shares != "10" ||
		snapshot.Capital.GrossExposure != "4" || snapshot.Capital.Equity != "105.5" {
		t.Fatalf("managed baseline projection = positions=%#v capital=%#v", snapshot.Positions, snapshot.Capital)
	}
	if dataQualityStatus(snapshot.DataQuality, "positions") != domain.LiveHealthHealthy {
		t.Fatalf("matching managed+baseline total degraded quality: %#v", snapshot.DataQuality)
	}
}

func TestExternalPositionBaselineDriftFailsClosed(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*Service)
	}{
		{
			name: "quantity",
			mutate: func(service *Service) {
				service.positionSource.(*fakePositionSource).positions[0].Shares = "14.999999"
			},
		},
		{
			name: "identity",
			mutate: func(service *Service) {
				service.positionSource.(*fakePositionSource).positions[0].ConditionID = "condition-other"
			},
		},
		{
			name: "missing remote token",
			mutate: func(service *Service) {
				service.positionSource.(*fakePositionSource).positions = nil
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			now := time.Date(2026, 8, 19, 8, 0, 10, 0, time.UTC)
			service, _, _ := newTestService(t, &now)
			service.positionSource.(*fakePositionSource).positions[0].Shares = "15"
			service.positionBaselines.(*fakePositionBaselineSource).baselines = []domain.ExternalPositionBaseline{
				testLivePositionBaseline(&now, "5"),
			}
			testCase.mutate(service)
			if err := service.Refresh(context.Background()); err == nil {
				t.Fatal("Refresh() error = nil, want immutable baseline drift failure")
			}
			if _, err := service.Snapshot(context.Background()); !errors.Is(err, ErrSnapshotUnavailable) {
				t.Fatalf("Snapshot() error = %v, want ErrSnapshotUnavailable", err)
			}
		})
	}
}

// newTestService 创建包含一个账户、一个仓位和完整健康状态的聚合服务。
func newTestService(t *testing.T, now *time.Time) (*Service, *fakeBalanceSource, *fakeRepository) {
	t.Helper()
	bookAt := now.Add(-2 * time.Second)
	reconciledAt := now.Add(-time.Minute)
	signalAt := now.Add(-time.Minute)
	local := domain.LiveOperationsLocalState{
		Accounts: []domain.LiveAccountState{{ExecutionAccountID: "account-1", WalletAddress: "0xwallet", CollateralAsset: "pUSD"}},
		Positions: []domain.LiveLedgerPosition{{Position: domain.Position{
			ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
			TokenID: "token-yes", OutcomeIndex: intPointer(0), OutcomeName: "YES",
			TotalShares: "10", CostBasis: "4", AverageCostPrice: "0.4",
		}, StrategyID: "multfactor_v2", ModelID: "model-v2", MarketLabel: "测试市场", LatestSignalAt: &signalAt}},
		RiskPolicies: []domain.LiveRiskPolicyState{{
			ExecutionAccountID: "account-1", PolicyID: "conservative", Enabled: true,
			MaxMarketExposure: "20", MaxWalletExposure: "100", MaxDailyTradedNotional: "50",
			MaxSignalAge: 5 * time.Minute, MaxStateAge: 5 * time.Minute,
		}},
		Reconciliations: []domain.LiveReconciliationState{{ExecutionAccountID: "account-1", Run: domain.ReconciliationRun{
			RunID: "reconcile-1", ExecutionAccountID: "account-1", Status: domain.ReconciliationRunCompleted,
			CompletedAt: &reconciledAt,
		}}},
		Workers: []domain.LiveWorkerState{
			{ThreadID: "cycle", LastHeartbeatAt: now.Add(-time.Minute), MaxHeartbeatAge: 5 * time.Minute},
			{ThreadID: "monitor", LastHeartbeatAt: now.Add(-time.Minute), MaxHeartbeatAge: 5 * time.Minute},
			{ThreadID: "prediction", LastHeartbeatAt: now.Add(-10 * time.Second), MaxHeartbeatAge: time.Minute},
		},
		ConfirmedTradeIDs: map[string]struct{}{}, RealizedPnLToday: "2", FeeToday: "0.1",
		DailyTradedNotional: "5", DatabaseObservedAt: *now,
	}
	repository := &fakeRepository{state: local}
	balance := &fakeBalanceSource{balance: domain.ExternalBalance{Asset: "pUSD", Amount: "100", ObservedAt: now.Add(-time.Second)}}
	positions := &fakePositionSource{positions: []domain.ExternalPosition{{
		ConditionID: "condition-1", TokenID: "token-yes", OutcomeIndex: intPointer(0), OutcomeName: "YES",
		Shares: "10", AveragePrice: "0.4", CurrentPrice: "0.54", ObservedAt: now.Add(-time.Second),
	}}}
	baselines := &fakePositionBaselineSource{}
	books := &fakeBookSource{books: []domain.OrderBookSnapshot{{
		MarketID: "market-1", ConditionID: "condition-1", TokenID: "token-yes", OutcomeIndex: 0,
		Status: domain.OrderBookStatusOK, BestBid: "0.5", BestAsk: "0.6", ObservedAt: bookAt,
	}}}
	service, err := New(Params{
		Repository: repository, Venue: &fakeVenue{}, PositionSource: positions,
		PositionBaselines: baselines, BalanceSource: balance, OrderBooks: books,
		Accounts:  []string{"account-1"},
		VenueName: "Polymarket CLOB", StartedAt: now.Add(-time.Hour), RunID: "run-1",
		Interval: 5 * time.Second, RefreshTimeout: time.Second, MaxSnapshotAge: 15 * time.Second,
		Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service, balance, repository
}

func testLivePositionBaseline(now *time.Time, shares domain.Decimal) domain.ExternalPositionBaseline {
	return domain.ExternalPositionBaseline{
		BaselineID: "baseline-1", ExecutionAccountID: "account-1",
		ConditionID: "condition-1", TokenID: "token-yes", OutcomeIndex: intPointer(0),
		OutcomeName: "YES", Shares: shares, Source: "POLYMARKET_DATA_API",
		ObservedAt: now.Add(-2 * time.Second), Evidence: json.RawMessage(`{"snapshot_sha256":"abc123"}`),
		Actor: "integration-test", Reason: "initial account cutover",
	}
}

func testLiveExternalPosition(now *time.Time, shares domain.Decimal) domain.ExternalPosition {
	return domain.ExternalPosition{
		ConditionID: "condition-1", TokenID: "token-yes", OutcomeIndex: intPointer(0), OutcomeName: "YES",
		Shares: shares, AveragePrice: "0.4", CurrentPrice: "0.54", ObservedAt: now.Add(-time.Second),
	}
}

// intPointer 返回测试需要的 outcome index 指针。
func intPointer(value int) *int {
	return &value
}

// dataQualityStatus 返回测试指定数据源的健康状态。
func dataQualityStatus(values []domain.LiveDataQuality, id string) domain.LiveHealth {
	for _, value := range values {
		if value.ID == id {
			return value.Status
		}
	}
	return ""
}

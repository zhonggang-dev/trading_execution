package fillprocessor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// staticFillSource 表示后端使用的 staticFillSource 类型。
type staticFillSource struct {
	fills []domain.Fill
	err   error
}

// ListOrderFills 返回当前测试配置的真实成交观察结果。
func (source *staticFillSource) ListOrderFills(context.Context, domain.Order) ([]domain.Fill, error) {
	return append([]domain.Fill(nil), source.fills...), source.err
}

// recordingFillLedger 表示后端使用的 recordingFillLedger 类型。
type recordingFillLedger struct {
	records             []domain.Fill
	revisionConflicts   int
	duplicateVenueFills map[string]bool
}

// Record 模拟权威账本的乐观锁冲突、幂等去重和成交应用。
func (ledger *recordingFillLedger) Record(_ context.Context, order domain.Order, fill domain.Fill) (domain.FillApplication, error) {
	if ledger.revisionConflicts > 0 {
		ledger.revisionConflicts--
		return domain.FillApplication{}, port.ErrOrderRevisionConflict
	}
	duplicate := ledger.duplicateVenueFills[fill.VenueFillID]
	ledger.duplicateVenueFills[fill.VenueFillID] = true
	ledger.records = append(ledger.records, fill)
	return domain.FillApplication{
		Fill: fill, Order: order, Applied: !duplicate, Duplicate: duplicate,
	}, nil
}

// GetFill 返回当前测试不支持的单笔成交查询结果。
func (ledger *recordingFillLedger) GetFill(context.Context, string) (domain.Fill, error) {
	return domain.Fill{}, port.ErrFillNotFound
}

// ListOrderFills 返回模拟账本已经记录的成交列表。
func (ledger *recordingFillLedger) ListOrderFills(context.Context, string) ([]domain.Fill, error) {
	return append([]domain.Fill(nil), ledger.records...), nil
}

// testFillOrder 构造真实成交同步所需的测试订单。
func testFillOrder(t *testing.T, repository *memory.OrderRepository, now time.Time) domain.Order {
	t.Helper()
	outcomeIndex := 0
	negRisk := false
	order, err := (domain.OrderParams{
		ID: "order-fill-sync-1",
		Intent: domain.OrderIntent{
			ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV2,
			ExecutionAccountID: "account-a", SignalID: "signal-a",
			ClientOrderID: "client-fill-sync-1", Venue: "polymarket",
			MarketID: "market-1", ConditionID: "condition-1",
			OutcomeIndex: &outcomeIndex, OutcomeName: "YES", TokenID: "token-yes",
			ExpectedNegRisk: &negRisk, Side: domain.SideBuy, Type: domain.OrderTypeLimit,
			Price: "0.50", WorstPrice: "0.50", Size: "20", TimeInForce: domain.TimeInForceFOK,
		},
		CreatedAt: now,
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	order.VenueOrderID = "venue-order-1"
	stored, created, err := repository.Create(context.Background(), order)
	if err != nil || !created {
		t.Fatalf("Create() = %#v, %v, %v", stored, created, err)
	}
	return stored
}

// newRecordingFillLedger 创建可记录账本调用的测试对象。
func newRecordingFillLedger() *recordingFillLedger {
	return &recordingFillLedger{duplicateVenueFills: make(map[string]bool)}
}

// TestSyncOrderEnrichesSortsAndBooksConfirmedFills 验证真实成交被排序、补全、精确计费并逐笔提交权威账本。
func TestSyncOrderEnrichesSortsAndBooksConfirmedFills(t *testing.T) {
	now := time.Date(2026, 8, 18, 5, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	order := testFillOrder(t, repository, now)
	updatedAt := now.Add(-time.Minute)
	source := &staticFillSource{fills: []domain.Fill{
		{
			VenueFillID: "fill-later", LiquidityRole: domain.LiquidityRoleMaker,
			Status: domain.FillStatusConfirmed, Shares: "2", Price: "0.4",
			GrossNotional: "0.8", FeeRateBPS: "10", PlatformFeeRate: "0.001", FeeExponent: "1", PlatformFee: "0",
			BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
			FeeSource: domain.FeeSourcePolygonV2OrderFilled,
			MatchedAt: now.Add(-time.Minute), VenueUpdatedAt: updatedAt,
		},
		{
			VenueFillID: "fill-earlier", LiquidityRole: domain.LiquidityRoleTaker,
			Status: domain.FillStatusConfirmed, Shares: "10", Price: "0.5", FeeRateBPS: "10",
			GrossNotional: "5", PlatformFeeRate: "0.001", FeeExponent: "1", PlatformFee: "0.0025",
			BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0.0025",
			FeeSource: domain.FeeSourcePolygonV2OrderFilled,
			MatchedAt: now.Add(-2 * time.Minute), VenueUpdatedAt: updatedAt,
		},
	}}
	ledger := newRecordingFillLedger()
	service, err := New(Params{Orders: repository, Source: source, Ledger: ledger, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.SyncOrder(context.Background(), order.ID)
	if err != nil {
		t.Fatalf("SyncOrder() error = %v", err)
	}
	if result.Observed != 2 || result.Applied != 2 || result.Duplicates != 0 || len(result.Applications) != 2 {
		t.Fatalf("sync result = %#v", result)
	}
	if len(ledger.records) != 2 || ledger.records[0].VenueFillID != "fill-earlier" || ledger.records[1].VenueFillID != "fill-later" {
		t.Fatalf("ledger record order = %#v", ledger.records)
	}
	fill := ledger.records[0]
	if fill.OrderID != order.ID || fill.VenueOrderID != order.VenueOrderID ||
		fill.ExecutionAccountID != order.Intent.ExecutionAccountID || fill.Key == "" || fill.ConfirmedAt == nil {
		t.Fatalf("enriched fill = %#v", fill)
	}
	if !fill.GrossNotional.Equal("5") || !fill.PlatformFee.Equal("0.0025") ||
		!fill.TotalFee.Equal("0.0025") || !fill.NetCashDelta.Equal("-5.0025") {
		t.Fatalf("accounting fill = %#v", fill)
	}
}

// TestSyncOrderRetriesTransientRevisionConflict 验证账本乐观锁冲突会刷新订单并在限定次数内重试。
func TestSyncOrderRetriesTransientRevisionConflict(t *testing.T) {
	now := time.Date(2026, 8, 18, 5, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	order := testFillOrder(t, repository, now)
	source := &staticFillSource{fills: []domain.Fill{{
		VenueFillID: "fill-retry", LiquidityRole: domain.LiquidityRoleMaker,
		Status: domain.FillStatusConfirmed, Shares: "1", Price: "0.5", GrossNotional: "0.5",
		FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0", PlatformFee: "0", BuilderFeeRateBPS: "0",
		BuilderFee: "0", TotalFee: "0", FeeSource: domain.FeeSourcePolygonV2OrderFilled, MatchedAt: now,
	}}}
	ledger := newRecordingFillLedger()
	ledger.revisionConflicts = 2
	service, err := New(Params{Orders: repository, Source: source, Ledger: ledger, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.SyncOrder(context.Background(), order.ID)
	if err != nil || result.Applied != 1 || len(ledger.records) != 1 {
		t.Fatalf("SyncOrder() = %#v, %v, records = %#v", result, err, ledger.records)
	}
}

// TestSyncOrderRejectsConflictingFillIdentityBeforeLedger 验证交易所成交冒用其他订单身份时在账本边界前失败关闭。
func TestSyncOrderRejectsConflictingFillIdentityBeforeLedger(t *testing.T) {
	now := time.Date(2026, 8, 18, 5, 0, 0, 0, time.UTC)
	repository := memory.NewOrderRepository()
	order := testFillOrder(t, repository, now)
	source := &staticFillSource{fills: []domain.Fill{{
		VenueFillID: "fill-conflict", OrderID: "another-order",
		LiquidityRole: domain.LiquidityRoleMaker, Status: domain.FillStatusConfirmed,
		Shares: "1", Price: "0.5", MatchedAt: now,
	}}}
	ledger := newRecordingFillLedger()
	service, err := New(Params{Orders: repository, Source: source, Ledger: ledger, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.SyncOrder(context.Background(), order.ID)
	if !errors.Is(err, port.ErrFillConflict) || result.Observed != 1 || result.Applied != 0 {
		t.Fatalf("SyncOrder() = %#v, %v", result, err)
	}
	if len(ledger.records) != 0 {
		t.Fatalf("conflicting fill reached ledger: %#v", ledger.records)
	}
}

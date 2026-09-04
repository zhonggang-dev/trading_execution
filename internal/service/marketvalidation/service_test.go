package marketvalidation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// fakeUniverse 表示后端使用的 fakeUniverse 类型。
type fakeUniverse struct {
	market domain.MarketSnapshot
	found  bool
	err    error
}

// FindByCondition 实现当前测试场景所需的辅助行为。
func (universe fakeUniverse) FindByCondition(context.Context, string) (domain.MarketSnapshot, bool, error) {
	return universe.market, universe.found, universe.err
}

// fakeBooks 表示后端使用的 fakeBooks 类型。
type fakeBooks struct {
	books   []domain.OrderBookSnapshot
	err     error
	targets []domain.BookTarget
}

// Capture 返回模拟行情快照。
func (source *fakeBooks) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	source.targets = targets
	return source.books, source.err
}

// TestValidateAcceptsCurrentTradableMarket 验证 Validate Accepts Current Tradable Market 场景下的行为。
func TestValidateAcceptsCurrentTradableMarket(t *testing.T) {
	now, intent, market, book := validFixtures()
	books := &fakeBooks{books: []domain.OrderBookSnapshot{book}}
	service := newValidator(t, now, market, books)

	validation, err := service.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.Mode != "LIVE_CHECK" || validation.TokenID != "token-yes" ||
		validation.OutcomeIndex != 0 || validation.BestAsk != "0.51" || validation.TickSize != "0.01" {
		t.Fatalf("Validate() = %#v", validation)
	}
	if len(books.targets) != 1 || books.targets[0].TokenID != "token-yes" {
		t.Fatalf("Capture() targets = %#v", books.targets)
	}
}

// TestValidateAcceptsOldStrategySnapshotWithFreshOfficialBook verifies that
// the immutable strategy snapshot remains audit evidence while execution uses
// the newly captured official order book for freshness.
func TestValidateAcceptsOldStrategySnapshotWithFreshOfficialBook(t *testing.T) {
	now, intent, market, book := validFixtures()
	strategySnapshotAt := now.Add(-4 * time.Hour)
	intent.MarketSnapshotAt = &strategySnapshotAt
	service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})

	validation, err := service.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !validation.StrategySnapshotAt.Equal(strategySnapshotAt) {
		t.Fatalf("StrategySnapshotAt = %s, want %s", validation.StrategySnapshotAt, strategySnapshotAt)
	}
	if !validation.LatestBookSourceAt.Equal(book.SourceAt) {
		t.Fatalf("LatestBookSourceAt = %s, want %s", validation.LatestBookSourceAt, book.SourceAt)
	}
}

// TestValidateFailsClosed 验证 Validate Fails Closed 场景下的行为。
func TestValidateFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		mutate func(*domain.OrderIntent, *domain.MarketSnapshot, *domain.OrderBookSnapshot, time.Time)
	}{
		{
			name: "future strategy snapshot", code: "MARKET_SNAPSHOT_FUTURE",
			mutate: func(intent *domain.OrderIntent, _ *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, now time.Time) {
				future := now.Add(3 * time.Second)
				intent.MarketSnapshotAt = &future
			},
		},
		{
			name: "resolved", code: "MARKET_RESOLVED",
			mutate: func(_ *domain.OrderIntent, market *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				market.Resolved = true
			},
		},
		{
			name: "closed", code: "MARKET_CLOSED",
			mutate: func(_ *domain.OrderIntent, market *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				market.Closed = true
			},
		},
		{
			name: "paused", code: "MARKET_PAUSED",
			mutate: func(_ *domain.OrderIntent, market *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				market.Paused = true
			},
		},
		{
			name: "not accepting orders", code: "MARKET_NOT_ACCEPTING_ORDERS",
			mutate: func(_ *domain.OrderIntent, market *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				market.AcceptingOrders = false
			},
		},
		{
			name: "wrong token", code: "OUTCOME_TOKEN_MISMATCH",
			mutate: func(intent *domain.OrderIntent, _ *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				intent.TokenID = "token-no"
			},
		},
		{
			name: "neg risk changed", code: "NEG_RISK_MISMATCH",
			mutate: func(_ *domain.OrderIntent, market *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				market.NegRisk = true
			},
		},
		{
			name: "duplicate outcome mapping", code: "MARKET_METADATA_INVALID",
			mutate: func(_ *domain.OrderIntent, market *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				market.Outcomes[1].Index = 0
			},
		},
		{
			name: "price violates current tick", code: "PRICE_TICK_MISMATCH",
			mutate: func(intent *domain.OrderIntent, _ *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				intent.Price = "0.525"
				intent.WorstPrice = "0.525"
			},
		},
		{
			name: "limit price differs from protection", code: "LIMIT_PRICE_MISMATCH",
			mutate: func(intent *domain.OrderIntent, _ *domain.MarketSnapshot, _ *domain.OrderBookSnapshot, _ time.Time) {
				intent.Price = "0.51"
			},
		},
		{
			name: "latest price moved", code: "PRICE_DRIFT",
			mutate: func(_ *domain.OrderIntent, _ *domain.MarketSnapshot, book *domain.OrderBookSnapshot, _ time.Time) {
				book.Asks[0].Price = "0.53"
			},
		},
		{
			name: "stale latest book capture", code: "LATEST_BOOK_OBSERVATION_STALE",
			mutate: func(_ *domain.OrderIntent, _ *domain.MarketSnapshot, book *domain.OrderBookSnapshot, now time.Time) {
				book.ObservedAt = now.Add(-11 * time.Second)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, market, book := validFixtures()
			test.mutate(&intent, &market, &book, now)
			service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})
			_, err := service.Validate(context.Background(), intent)
			var rejection *port.Rejection
			if !errors.As(err, &rejection) || rejection.Code != test.code {
				t.Fatalf("Validate() error = %v, want rejection %s", err, test.code)
			}
		})
	}
}

// TestValidateAcceptsQuietBookWithOldVenueTimestamp 验证盘口最后变动时间很久以前的慢市场
// 只要执行层刚抓到盘口就可以下单，venue 时间戳仍被记录为证据。
func TestValidateAcceptsQuietBookWithOldVenueTimestamp(t *testing.T) {
	now, intent, market, book := validFixtures()
	book.SourceAt = now.Add(-7 * time.Minute)
	book.ObservedAt = now.Add(-time.Second)
	service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})

	validation, err := service.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v, want quiet market accepted", err)
	}
	if !validation.LatestBookSourceAt.Equal(book.SourceAt) || !validation.LatestBookObservedAt.Equal(book.ObservedAt) {
		t.Fatalf("Validate() timestamps = %s / %s", validation.LatestBookSourceAt, validation.LatestBookObservedAt)
	}
	if validation.BuyFeeReserve != nil {
		t.Fatalf("BuyFeeReserve = %#v, want nil without a fee schedule source", validation.BuyFeeReserve)
	}
}

// fakeFeeSchedules 模拟 venue 官方费率读取。
type fakeFeeSchedules struct {
	schedule domain.MarketFeeSchedule
	err      error
	calls    int
}

// MarketFeeSchedule 返回模拟费率。
func (source *fakeFeeSchedules) MarketFeeSchedule(context.Context, string, string) (domain.MarketFeeSchedule, error) {
	source.calls++
	return source.schedule, source.err
}

func newValidatorWithFees(t *testing.T, now time.Time, market domain.MarketSnapshot, books port.OrderBookSource, fees port.FeeScheduleSource) *Service {
	t.Helper()
	service, err := New(Params{
		Universe:     fakeUniverse{market: market, found: true},
		OrderBooks:   books,
		FeeSchedules: fees,
		Now:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

// TestValidateRecordsVenueFeeReserveForBuy 验证 BUY 校验按官方费率曲线在保护价内的最大值记录
// 每股最坏手续费。
func TestValidateRecordsVenueFeeReserveForBuy(t *testing.T) {
	now, intent, market, book := validFixtures()
	fetchedAt := now.Add(-time.Minute)
	fees := &fakeFeeSchedules{schedule: domain.MarketFeeSchedule{
		PlatformFeeRate: "0.2", FeeExponent: "1", TakerOnly: true, FetchedAt: fetchedAt,
	}}
	service := newValidatorWithFees(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}}, fees)

	validation, err := service.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	reserve := validation.BuyFeeReserve
	if reserve == nil || reserve.Source != domain.BuyFeeReserveSourceVenueFeeSchedule {
		t.Fatalf("BuyFeeReserve = %#v, want venue fee schedule", reserve)
	}
	// worst_price 0.52 允许成交到 0.5，曲线最大值 0.25：0.2 * 0.25 = 0.05
	if !reserve.MaxFeePerShare.Equal("0.05") || !reserve.PlatformFeeRate.Equal("0.2") || !reserve.FeeExponent.Equal("1") {
		t.Fatalf("BuyFeeReserve = %#v, want max fee per share 0.05", reserve)
	}
	if reserve.ScheduleFetchedAt == nil || !reserve.ScheduleFetchedAt.Equal(fetchedAt) {
		t.Fatalf("ScheduleFetchedAt = %v, want %s", reserve.ScheduleFetchedAt, fetchedAt)
	}
	if fee, ok := reserve.VenueMaxFeePerShare(); !ok || !fee.Equal("0.05") {
		t.Fatalf("VenueMaxFeePerShare() = %s, %v", fee, ok)
	}
}

// TestValidateFallsBackToConfigCapWhenFeeScheduleUnavailable 验证费率无法确认时不拒单，
// 证据记录退回配置上限的原因。
func TestValidateFallsBackToConfigCapWhenFeeScheduleUnavailable(t *testing.T) {
	now, intent, market, book := validFixtures()
	fees := &fakeFeeSchedules{err: errors.New("clob-markets unavailable")}
	service := newValidatorWithFees(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}}, fees)

	validation, err := service.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v, want fee schedule failure to fall back instead of rejecting", err)
	}
	reserve := validation.BuyFeeReserve
	if reserve == nil || reserve.Source != domain.BuyFeeReserveSourceConfigCap || reserve.Reason != "clob-markets unavailable" {
		t.Fatalf("BuyFeeReserve = %#v, want config cap fallback with reason", reserve)
	}
	if _, ok := reserve.VenueMaxFeePerShare(); ok {
		t.Fatal("config cap fallback must not expose a venue fee per share")
	}
}

// TestValidateSkipsFeeReserveForSell 验证 SELL 不读取费率，也不记录 BUY 手续费预占依据。
func TestValidateSkipsFeeReserveForSell(t *testing.T) {
	now, intent, market, book := validFixtures()
	intent.Side = domain.SideSell
	intent.Price = "0.50"
	intent.WorstPrice = "0.50"
	fees := &fakeFeeSchedules{schedule: domain.MarketFeeSchedule{PlatformFeeRate: "0.2", FeeExponent: "1"}}
	service := newValidatorWithFees(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}}, fees)

	validation, err := service.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.BuyFeeReserve != nil || fees.calls != 0 {
		t.Fatalf("SELL validation = %#v, fee calls = %d, want no fee reserve", validation.BuyFeeReserve, fees.calls)
	}
}

// TestValidateSellUsesBestBidAsFloor 验证 Validate Sell Uses Best Bid As Floor 场景下的行为。
func TestValidateSellUsesBestBidAsFloor(t *testing.T) {
	now, intent, market, book := validFixtures()
	index := 1
	intent.OutcomeIndex = &index
	intent.OutcomeName = "No"
	intent.TokenID = "token-no"
	intent.Side = domain.SideSell
	intent.Price = "0.48"
	intent.WorstPrice = "0.48"
	book.OutcomeIndex = 1
	book.TokenID = "token-no"
	book.Bids[0].Price = "0.47"
	service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})

	_, err := service.Validate(context.Background(), intent)
	var rejection *port.Rejection
	if !errors.As(err, &rejection) || rejection.Code != "PRICE_DRIFT" {
		t.Fatalf("Validate() error = %v, want PRICE_DRIFT", err)
	}
}

// newValidator 创建测试所需的模拟对象。
func newValidator(t *testing.T, now time.Time, market domain.MarketSnapshot, books port.OrderBookSource) *Service {
	t.Helper()
	service, err := New(Params{
		Universe:   fakeUniverse{market: market, found: true},
		OrderBooks: books,
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

// validFixtures 构建测试使用的合法输入。
func validFixtures() (time.Time, domain.OrderIntent, domain.MarketSnapshot, domain.OrderBookSnapshot) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	snapshotAt := now.Add(-5 * time.Second)
	negRisk := false
	index := 0
	intent := domain.OrderIntent{
		ModelID:            "model-1",
		StrategyID:         "strategy-1",
		ExecutionAccountID: "account-model-1-strategy-1",
		SignalID:           "decision-1",
		ClientOrderID:      "order-1",
		Venue:              "polymarket",
		MarketID:           "market-1",
		ConditionID:        "condition-1",
		OutcomeIndex:       &index,
		OutcomeName:        "Yes",
		TokenID:            "token-yes",
		ExpectedNegRisk:    &negRisk,
		MarketSnapshotAt:   &snapshotAt,
		Side:               domain.SideBuy,
		Type:               domain.OrderTypeLimit,
		Price:              "0.52",
		WorstPrice:         "0.52",
		Size:               "10",
		TimeInForce:        domain.TimeInForceGTC,
	}
	market := domain.MarketSnapshot{
		MarketID:        "market-1",
		ConditionID:     "condition-1",
		Active:          true,
		AcceptingOrders: true,
		TickSize:        "0.01",
		ObservedAt:      now.Add(-2 * time.Second),
		Outcomes: []domain.MarketOutcome{
			{Index: 0, Name: "Yes", TokenID: "token-yes"},
			{Index: 1, Name: "No", TokenID: "token-no"},
		},
	}
	book := domain.OrderBookSnapshot{
		MarketID:     "market-1",
		ConditionID:  "condition-1",
		OutcomeIndex: 0,
		TokenID:      "token-yes",
		Status:       domain.OrderBookStatusOK,
		SourceAt:     now.Add(-time.Second),
		ObservedAt:   now,
		Bids:         []domain.PriceLevel{{Price: "0.50", Size: "100"}},
		Asks:         []domain.PriceLevel{{Price: "0.51", Size: "100"}},
	}
	return now, intent, market, book
}

// TestValidateMeasuresIOCExecutableSizeInsideWorstPrice 验证 IOC 的可成交量是保护价内可见深度与 size 的较小值，
// worst_price 只是价格上限而不是预算。
func TestValidateMeasuresIOCExecutableSizeInsideWorstPrice(t *testing.T) {
	now, intent, market, book := validFixtures()
	intent.TimeInForce = domain.TimeInForceIOC
	intent.Size = "42"
	book.Asks = []domain.PriceLevel{
		{Price: "0.51", Size: "12.5"},
		{Price: "0.52", Size: "5"},
		{Price: "0.53", Size: "100"},
	}
	service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})

	validation, err := service.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.ExecutableSize != "17.5" || validation.WorstPrice != "0.52" || validation.BestAsk != "0.51" {
		t.Fatalf("Validate() = %#v, want 17.5 executable shares inside worst_price 0.52", validation)
	}

	intent.Size = "10"
	validation, err = service.Validate(context.Background(), intent)
	if err != nil || validation.ExecutableSize != "10" {
		t.Fatalf("Validate() with full depth = %#v, err = %v, want executable_size capped at size", validation, err)
	}
}

// TestValidateRejectsIOCBelowLatestMinOrderSize 验证保护价内可成交量低于 min_order_size 时 fail closed。
func TestValidateRejectsIOCBelowLatestMinOrderSize(t *testing.T) {
	now, intent, market, book := validFixtures()
	intent.TimeInForce = domain.TimeInForceIOC
	intent.Size = "42"
	book.MinOrderSize = "20"
	book.Asks = []domain.PriceLevel{{Price: "0.51", Size: "12.5"}, {Price: "0.53", Size: "100"}}
	service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})

	_, err := service.Validate(context.Background(), intent)
	var rejection *port.Rejection
	if !errors.As(err, &rejection) || rejection.Code != "PROTECTED_LIQUIDITY_BELOW_MIN_ORDER_SIZE" {
		t.Fatalf("Validate() error = %v, want PROTECTED_LIQUIDITY_BELOW_MIN_ORDER_SIZE", err)
	}
}

// TestValidateIOCSellMeasuresBidDepthAndFOKLeavesExecutableSizeEmpty 验证 SELL IOC 使用买盘深度，
// 而 FOK/GTC 不记录 executable_size。
func TestValidateIOCSellMeasuresBidDepthAndFOKLeavesExecutableSizeEmpty(t *testing.T) {
	now, intent, market, book := validFixtures()
	service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})
	validation, err := service.Validate(context.Background(), intent)
	if err != nil || !validation.ExecutableSize.IsEmpty() {
		t.Fatalf("GTC Validate() = %#v, err = %v, want no executable_size", validation, err)
	}

	sell := intent
	sell.Side = domain.SideSell
	sell.Price = "0.49"
	sell.WorstPrice = "0.49"
	sell.TimeInForce = domain.TimeInForceIOC
	sellBook := book
	sellBook.Bids = []domain.PriceLevel{{Price: "0.50", Size: "3"}, {Price: "0.49", Size: "4"}, {Price: "0.48", Size: "50"}}
	service = newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{sellBook}})
	validation, err = service.Validate(context.Background(), sell)
	if err != nil || validation.ExecutableSize != "7" {
		t.Fatalf("SELL IOC Validate() = %#v, err = %v, want 7 executable shares at or above worst_price", validation, err)
	}
}

// TestValidateFloorsIOCExecutableSizeToVenueSharePrecision 验证可成交量向下取整到 2 位小数，
// 提交数量不会超过所见深度，也不会因精度被交易所拒绝。
func TestValidateFloorsIOCExecutableSizeToVenueSharePrecision(t *testing.T) {
	now, intent, market, book := validFixtures()
	intent.TimeInForce = domain.TimeInForceIOC
	intent.Size = "42"
	book.Asks = []domain.PriceLevel{{Price: "0.51", Size: "12.505"}, {Price: "0.52", Size: "0.009"}, {Price: "0.53", Size: "100"}}
	service := newValidator(t, now, market, &fakeBooks{books: []domain.OrderBookSnapshot{book}})

	validation, err := service.Validate(context.Background(), intent)
	if err != nil || validation.ExecutableSize != "12.51" {
		t.Fatalf("Validate() = %#v, err = %v, want 12.514 floored to 12.51", validation, err)
	}
}

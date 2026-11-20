package thinmarket

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// fakeLister 模拟 Gamma：按流动性升序、支持 liquidity 下/上界过滤、offset 超过 2000 报错。
type fakeLister struct {
	mu      sync.Mutex
	markets []domain.MarketListing
	filters []domain.MarketListFilter
	err     error
}

func (lister *fakeLister) ListMarkets(_ context.Context, filter domain.MarketListFilter) (domain.MarketListingPage, error) {
	lister.mu.Lock()
	lister.filters = append(lister.filters, filter)
	lister.mu.Unlock()
	if lister.err != nil {
		return domain.MarketListingPage{}, lister.err
	}
	if filter.Offset > 2000 {
		return domain.MarketListingPage{}, errors.New("offset too large")
	}
	value := func(d domain.Decimal) float64 { parsed, _ := strconv.ParseFloat(d.String(), 64); return parsed }
	var matched []domain.MarketListing
	for _, market := range lister.markets {
		liquidity := value(market.Liquidity)
		if !filter.MinLiquidity.IsEmpty() && liquidity < value(filter.MinLiquidity) {
			continue
		}
		if !filter.MaxLiquidity.IsEmpty() && liquidity > value(filter.MaxLiquidity) {
			continue
		}
		matched = append(matched, market)
	}
	sort.SliceStable(matched, func(i, j int) bool { return value(matched[i].Liquidity) < value(matched[j].Liquidity) })
	if filter.Offset >= len(matched) {
		return domain.MarketListingPage{}, nil
	}
	page := matched[filter.Offset:min(filter.Offset+filter.Limit, len(matched))]
	return domain.MarketListingPage{Markets: page, Fetched: len(page), LastLiquidity: page[len(page)-1].Liquidity}, nil
}

// fakeBooks 按 token 返回预置盘口，未预置的 token 返回空卖盘。
type fakeBooks struct {
	asks     map[string][]domain.PriceLevel
	status   map[string]domain.OrderBookStatus
	captures int
}

func (books *fakeBooks) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	books.captures++
	snapshots := make([]domain.OrderBookSnapshot, len(targets))
	for index, target := range targets {
		status := domain.OrderBookStatusOK
		if value, ok := books.status[target.TokenID]; ok {
			status = value
		}
		snapshots[index] = domain.OrderBookSnapshot{TokenID: target.TokenID, Status: status, Asks: books.asks[target.TokenID]}
	}
	return snapshots, nil
}

func level(price, size string) domain.PriceLevel {
	return domain.PriceLevel{Price: domain.Decimal(price), Size: domain.Decimal(size)}
}

func listing(id, liquidity, volume string) domain.MarketListing {
	return domain.MarketListing{
		MarketID: id, ConditionID: "0x" + id, Question: "q" + id,
		Volume: domain.Decimal(volume), Liquidity: domain.Decimal(liquidity),
		Outcomes: []domain.MarketOutcome{
			{Index: 0, Name: "Yes", TokenID: id + "1"},
			{Index: 1, Name: "No", TokenID: id + "2"},
		},
	}
}

// TestEvaluateBoundaries 验证两个金额条件的边界：成本含 10、价差价值必须严格大于 60。
func TestEvaluateBoundaries(t *testing.T) {
	limits := thresholds{minGap: mustRat(t, "60"), maxCost: mustRat(t, "10")}
	market := listing("1", "1", "1")
	cases := []struct {
		name string
		asks []domain.PriceLevel
		want bool
	}{
		{"命中", []domain.PriceLevel{level("0.01", "1000"), level("0.5", "5")}, true},
		{"只有一档", []domain.PriceLevel{level("0.01", "1000")}, false},
		{"成本恰好 10 含边界", []domain.PriceLevel{level("0.01", "1000"), level("0.08", "1")}, true},
		{"成本略超 10", []domain.PriceLevel{level("0.01", "1001"), level("0.5", "1")}, false},
		{"差价价值恰好 60 不算", []domain.PriceLevel{level("0.01", "100"), level("0.61", "1")}, false},
		{"差价价值略超 60", []domain.PriceLevel{level("0.01", "100"), level("0.611", "1")}, true},
		{"卖二不高于卖一", []domain.PriceLevel{level("0.01", "1000"), level("0.01", "5")}, false},
		{"成本合格但差价太小", []domain.PriceLevel{level("0.5", "20"), level("0.6", "5")}, false},
	}
	for _, testCase := range cases {
		_, ok, err := evaluate(market, market.Outcomes[0], testCase.asks, limits)
		if err != nil || ok != testCase.want {
			t.Errorf("%s: ok=%v err=%v, want %v", testCase.name, ok, err, testCase.want)
		}
	}
}

// TestEvaluateReportsExactValues 验证返回的价差、价值与成本按有理数精确计算。
func TestEvaluateReportsExactValues(t *testing.T) {
	limits := thresholds{minGap: mustRat(t, "60"), maxCost: mustRat(t, "10")}
	market := listing("1", "1", "1")
	candidate, ok, err := evaluate(market, market.Outcomes[0], []domain.PriceLevel{level("0.03", "176.96"), level("0.799", "10")}, limits)
	if err != nil || !ok {
		t.Fatalf("evaluate() ok=%v err=%v", ok, err)
	}
	if candidate.AskSpread != "0.769" || candidate.GapValue != "136.08224" || candidate.BestAskCost != "5.3088" {
		t.Fatalf("values = spread %s gap %s cost %s", candidate.AskSpread, candidate.GapValue, candidate.BestAskCost)
	}
}

// TestFindSortsByLiquidityThenVolumeAndCapsLimit 验证排序、截断，并且两个 Outcome 分别判断。
func TestFindSortsByLiquidityThenVolumeAndCapsLimit(t *testing.T) {
	hit := []domain.PriceLevel{level("0.01", "1000"), level("0.5", "5")}
	lister := &fakeLister{markets: []domain.MarketListing{
		listing("a", "5", "9"), listing("b", "5", "3"), listing("c", "1", "100"), listing("d", "7", "1"),
	}}
	books := &fakeBooks{asks: map[string][]domain.PriceLevel{
		"a1": hit, "b1": hit, "b2": hit, "c2": hit, "d1": hit,
	}}
	service, err := New(lister, books)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Find(t.Context(), Params{Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, candidate := range result.Candidates {
		got = append(got, fmt.Sprintf("%s/%d", candidate.Market.MarketID, candidate.Outcome.Index))
	}
	want := []string{"c/1", "b/0", "b/1", "a/0"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	if result.ScannedMarkets != 4 || result.ScannedTokens != 8 || !result.Exhausted {
		t.Fatalf("stats = %+v", result)
	}
}

// rangedMarkets 生成 count 个流动性为 1..count 的市场，hitAt 中的序号（从 0 起）两个 Outcome 之一命中。
func rangedMarkets(count int, hitAt ...int) ([]domain.MarketListing, map[string][]domain.PriceLevel) {
	hit := []domain.PriceLevel{level("0.01", "1000"), level("0.5", "5")}
	asks := map[string][]domain.PriceLevel{}
	markets := make([]domain.MarketListing, count)
	for index := range markets {
		markets[index] = listing(fmt.Sprintf("m%05d", index), strconv.Itoa(index+1), "1")
	}
	for _, index := range hitAt {
		asks[markets[index].Outcomes[0].TokenID] = hit
	}
	return markets, asks
}

// TestFindWindowsPastOffsetCap 验证越过 2000 的 offset 上限：命中位于第 4300 个市场也能找到，
// 市场只被计数一次（窗口衔接处的重复项已去重），且从不请求超过 1900 的 offset。
func TestFindWindowsPastOffsetCap(t *testing.T) {
	markets, asks := rangedMarkets(4500, 4300)
	lister := &fakeLister{markets: markets}
	service, _ := New(lister, &fakeBooks{asks: asks})
	result, err := service.Find(t.Context(), Params{MaxLiquidity: "100000"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].Market.MarketID != "m04300" {
		t.Fatalf("candidates = %+v", result.Candidates)
	}
	if result.ScannedMarkets != 4500 || result.Windows != 3 || !result.Exhausted {
		t.Fatalf("stats = %+v", result)
	}
	for _, filter := range lister.filters {
		if filter.Offset > 1900 {
			t.Fatalf("offset %d exceeds the safe window", filter.Offset)
		}
	}
}

// TestFindStopsAfterWindowWithEnoughHits 验证窗口内凑够 Limit 后不再请求后续窗口。
func TestFindStopsAfterWindowWithEnoughHits(t *testing.T) {
	markets, asks := rangedMarkets(4500, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	lister := &fakeLister{markets: markets}
	service, _ := New(lister, &fakeBooks{asks: asks})
	result, err := service.Find(t.Context(), Params{MaxLiquidity: "100000", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 10 || result.Exhausted || result.Windows != 1 {
		t.Fatalf("result = %+v", result)
	}
	for _, filter := range lister.filters {
		if filter.MinLiquidity != "0.01" {
			t.Fatalf("second window requested: %+v", filter)
		}
	}
}

// TestFindStopsWhenWindowCannotAdvance 验证一个窗口以上的市场流动性完全相同时不会死循环，且如实标为未翻完。
func TestFindStopsWhenWindowCannotAdvance(t *testing.T) {
	markets := make([]domain.MarketListing, 2500)
	for index := range markets {
		markets[index] = listing(fmt.Sprintf("m%05d", index), "7", "1")
	}
	service, _ := New(&fakeLister{markets: markets}, &fakeBooks{})
	result, err := service.Find(t.Context(), Params{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Exhausted || result.Windows != 2 {
		t.Fatalf("result = %+v", result)
	}
}

// TestFindRespectsScanCapAndPassesFilter 验证扫描上限、默认阈值和翻页参数传给列表接口。
func TestFindRespectsScanCapAndPassesFilter(t *testing.T) {
	markets, _ := rangedMarkets(300)
	lister := &fakeLister{markets: markets}
	service, _ := New(lister, &fakeBooks{})
	result, err := service.Find(t.Context(), Params{MaxScanMarkets: 150, PageSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if result.ScannedMarkets != 150 || result.Exhausted || len(result.Candidates) != 0 {
		t.Fatalf("result = %+v", result)
	}
	offsets := map[int]int{}
	for _, filter := range lister.filters {
		offsets[filter.Offset] = filter.Limit
		if filter.MaxVolume != "500" || filter.MaxLiquidity != "30000" || filter.MinLiquidity != "0.01" {
			t.Fatalf("default thresholds = %+v", filter)
		}
	}
	if len(offsets) != 2 || offsets[0] != 100 || offsets[100] != 50 {
		t.Fatalf("pages = %v", offsets)
	}
}

// TestFindCountsBookErrorsWithoutFailing 验证单个盘口读取失败只计数，不影响其他 Token。
func TestFindCountsBookErrorsWithoutFailing(t *testing.T) {
	hit := []domain.PriceLevel{level("0.01", "1000"), level("0.5", "5")}
	lister := &fakeLister{markets: []domain.MarketListing{listing("a", "1", "1")}}
	books := &fakeBooks{
		asks:   map[string][]domain.PriceLevel{"a2": hit},
		status: map[string]domain.OrderBookStatus{"a1": domain.OrderBookStatusError},
	}
	service, _ := New(lister, books)
	result, err := service.Find(t.Context(), Params{})
	if err != nil {
		t.Fatal(err)
	}
	if result.BookErrors != 1 || len(result.Candidates) != 1 || result.Candidates[0].Outcome.Index != 1 {
		t.Fatalf("result = %+v", result)
	}
}

// TestFindPropagatesListError 验证市场列表失败时整体失败，不返回部分结果冒充完整。
func TestFindPropagatesListError(t *testing.T) {
	service, _ := New(&fakeLister{err: errors.New("boom")}, &fakeBooks{})
	if _, err := service.Find(t.Context(), Params{}); err == nil {
		t.Fatal("Find() error = nil")
	}
}

// TestNormalizeRejectsInvalidParams 验证越界与非法数值被拒绝。
func TestNormalizeRejectsInvalidParams(t *testing.T) {
	for name, params := range map[string]Params{
		"limit 过大": {Limit: 101},
		"扫描上限过大":   {MaxScanMarkets: 100001},
		"并发过大":     {ListConcurrency: 17},
		"页大小过大":    {PageSize: 101},
		"负阈值":      {MinGapValue: "-1"},
		"非法数字":     {MaxVolume: "1e5"},
	} {
		if _, _, err := normalize(params); err == nil {
			t.Errorf("%s: normalize() error = nil", name)
		}
	}
	if _, _, err := normalize(Params{}); err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
}

func mustRat(t *testing.T, value string) *big.Rat {
	t.Helper()
	parsed, ok := new(big.Rat).SetString(value)
	if !ok {
		t.Fatalf("invalid rat %q", value)
	}
	return parsed
}

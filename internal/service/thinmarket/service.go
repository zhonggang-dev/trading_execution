// Package thinmarket 在成交量与流动性都很小的 Polymarket 市场里，找出卖盘结构特殊的 Outcome：
// 卖一很便宜（全部吃掉不超过一定金额），卖二却离得很远（价差乘卖一份额超过一定金额）。
// 整个包只读，不创建订单也不占用任何账户资源。
package thinmarket

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	defaultMaxVolume      = "500"
	defaultMaxLiquidity   = "30000"
	defaultMinLiquidity   = "0.01"
	defaultMinGapValue    = "60"
	defaultMaxBestAskCost = "10"
	defaultLimit          = 10
	defaultPageSize       = 100
	// maxWindowMarkets 是单个流动性窗口的市场数，受 Gamma /markets 的 offset 上限约束（2000）。
	maxWindowMarkets = 2000
	// maxScanMarkets 是所有窗口合计检查的市场数上限。
	maxScanMarkets         = 100000
	defaultScanMarkets     = 30000
	defaultListConcurrency = 4
)

// Params 是筛选条件，零值字段取默认值。
type Params struct {
	// MaxVolume、MaxLiquidity 是 Gamma 口径的成交量、流动性上限（美元，含）。
	MaxVolume    domain.Decimal
	MaxLiquidity domain.Decimal
	// MinLiquidity 排除流动性为 0 的休眠盘：Gamma 里它们没有有效挂单，却占了流动性升序翻页的绝大部分
	//（线上前约 1800 条），会耗尽 2000 的翻页上限。
	MinLiquidity domain.Decimal
	// MinGapValue 要求 (卖二价 − 卖一价) × 卖一份额 严格大于该值（美元）。
	MinGapValue domain.Decimal
	// MaxBestAskCost 要求 卖一价 × 卖一份额（吃掉全部卖一的成本）不超过该值（美元，含）。
	MaxBestAskCost domain.Decimal
	// Limit 是返回的最大条数。
	Limit int
	// MaxScanMarkets 是最多检查的市场数，每个市场取两个 Outcome 的盘口。
	MaxScanMarkets int
	// PageSize 是 Gamma 单页条数。
	PageSize int
	// ListConcurrency 是同一窗口内并发请求 Gamma 页数。
	ListConcurrency int
}

// Candidate 是一个满足条件的 Outcome Token。
type Candidate struct {
	Market  domain.MarketListing `json:"market"`
	Outcome domain.MarketOutcome `json:"outcome"`
	// BestAsk、SecondAsk 是该 Token 卖盘最低的两档。
	BestAsk   domain.PriceLevel `json:"best_ask"`
	SecondAsk domain.PriceLevel `json:"second_ask"`
	// AskSpread 是卖二价减卖一价。
	AskSpread domain.Decimal `json:"ask_spread"`
	// GapValue 是 AskSpread × 卖一份额。
	GapValue domain.Decimal `json:"gap_value"`
	// BestAskCost 是卖一价 × 卖一份额，即吃掉全部卖一的成本。
	BestAskCost domain.Decimal `json:"best_ask_cost"`
}

// Result 是一次筛选的结果与扫描统计。
type Result struct {
	// Candidates 按流动性、成交量升序，最多 Limit 个。
	Candidates     []Candidate `json:"candidates"`
	ScannedMarkets int         `json:"scanned_markets"`
	ScannedTokens  int         `json:"scanned_tokens"`
	// Windows 是翻过的流动性窗口数。
	Windows int `json:"windows"`
	// BookErrors 是盘口读取失败（被跳过）的 Token 数，非零说明结果可能不完整。
	BookErrors int `json:"book_errors"`
	// Exhausted 为 true 表示在扫描上限内已翻完所有符合量化上限的市场。
	Exhausted bool `json:"exhausted"`
}

// Service 组合市场列表与订单簿读取完成筛选。
type Service struct {
	markets port.MarketLister
	books   port.OrderBookSource
	now     func() time.Time
}

// New 创建筛选服务。
func New(markets port.MarketLister, books port.OrderBookSource) (*Service, error) {
	if markets == nil || books == nil {
		return nil, fmt.Errorf("thin market finder requires a market lister and an orderbook source")
	}
	return &Service{markets: markets, books: books, now: time.Now}, nil
}

// thresholds 是已校验的数值条件。
type thresholds struct {
	minGap  *big.Rat
	maxCost *big.Rat
}

// Find 从流动性最小的市场开始，按流动性窗口翻页：每个窗口最多 2000 个市场（Gamma offset 上限），
// 窗口之间以上一窗口末位的流动性为下界衔接，因此能越过 offset 上限覆盖全部符合量化上限的市场。
// 凑够 Limit 个结果、翻完所有市场或达到扫描上限即停止。
func (service *Service) Find(ctx context.Context, params Params) (Result, error) {
	params, limits, err := normalize(params)
	if err != nil {
		return Result{}, err
	}
	result := Result{Candidates: []Candidate{}}
	seen := make(map[string]struct{})
	windowMin := params.MinLiquidity
	fetched := 0
	for fetched < params.MaxScanMarkets {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		window, err := service.fetchWindow(ctx, params, windowMin, params.MaxScanMarkets-fetched)
		if err != nil {
			return Result{}, err
		}
		result.Windows++
		fetched += window.fetched
		fresh := make([]domain.MarketListing, 0, len(window.markets))
		for _, market := range window.markets {
			if _, duplicate := seen[market.MarketID]; duplicate {
				continue
			}
			seen[market.MarketID] = struct{}{}
			fresh = append(fresh, market)
		}
		if err := service.scanPage(ctx, fresh, limits, &result); err != nil {
			return Result{}, err
		}
		if !window.saturated {
			result.Exhausted = true
			break
		}
		if len(result.Candidates) >= params.Limit {
			break
		}
		if compare(window.lastLiquidity, windowMin) == 0 {
			// 超过一个窗口的市场流动性完全相同，无法再前进；如实返回未翻完。
			break
		}
		windowMin = window.lastLiquidity
	}
	sortCandidates(result.Candidates)
	if len(result.Candidates) > params.Limit {
		result.Candidates = result.Candidates[:params.Limit]
	}
	return result, nil
}

// window 是一个流动性窗口内取到的全部市场。
type window struct {
	markets       []domain.MarketListing
	fetched       int
	lastLiquidity domain.Decimal
	// saturated 为 true 表示窗口翻满了 offset 上限仍未到末页，之后还有更多市场。
	saturated bool
}

// fetchWindow 以 windowMin 为流动性下界，分批并发取最多 maxWindowMarkets 个市场。
func (service *Service) fetchWindow(ctx context.Context, params Params, windowMin domain.Decimal, remaining int) (window, error) {
	budget := min(maxWindowMarkets, remaining)
	pageCount := (budget + params.PageSize - 1) / params.PageSize
	limitOf := func(page int) int { return min(params.PageSize, budget-page*params.PageSize) }
	pages := make([]domain.MarketListingPage, pageCount)
	errs := make([]error, pageCount)
	var result window
	result.saturated = true
	for wave := 0; wave < pageCount && result.saturated; wave += params.ListConcurrency {
		upper := min(wave+params.ListConcurrency, pageCount)
		var group sync.WaitGroup
		for page := wave; page < upper; page++ {
			group.Add(1)
			go func() {
				defer group.Done()
				pages[page], errs[page] = service.markets.ListMarkets(ctx, domain.MarketListFilter{
					MaxVolume: params.MaxVolume, MinLiquidity: windowMin, MaxLiquidity: params.MaxLiquidity,
					Offset: page * params.PageSize, Limit: limitOf(page),
				})
			}()
		}
		group.Wait()
		for page := wave; page < upper; page++ {
			if errs[page] != nil {
				return window{}, errs[page]
			}
			result.markets = append(result.markets, pages[page].Markets...)
			result.fetched += pages[page].Fetched
			if pages[page].Fetched > 0 {
				result.lastLiquidity = pages[page].LastLiquidity
			}
			if pages[page].Fetched < limitOf(page) {
				// 短页表示数据已到末尾，其后同一波内更晚的页必然为空。
				result.saturated = false
			}
		}
	}
	// 被扫描上限截断时窗口并未取满 offset 上限，但也没有到末页；
	// 此时 saturated 仍为 true，调用方会因 fetched 达到上限而停止。
	return result, nil
}

// scanPage 读取一页市场全部 Outcome 的盘口并追加命中项。
func (service *Service) scanPage(ctx context.Context, markets []domain.MarketListing, limits thresholds, result *Result) error {
	type ref struct {
		market  domain.MarketListing
		outcome domain.MarketOutcome
	}
	targets := make([]domain.BookTarget, 0, len(markets)*2)
	refs := make([]ref, 0, len(markets)*2)
	for _, market := range markets {
		built := true
		marketTargets := make([]domain.BookTarget, 0, len(market.Outcomes))
		for _, outcome := range market.Outcomes {
			target, err := domain.BookTargetParams{
				MarketSource: domain.MarketSourcePolymarket, MarketID: market.MarketID, ConditionID: market.ConditionID,
				OutcomeIndex: outcome.Index, TokenID: outcome.TokenID, OutcomeID: strings.ToUpper(outcome.Name),
			}.Build()
			if err != nil {
				built = false
				break
			}
			marketTargets = append(marketTargets, target)
		}
		if !built {
			continue
		}
		result.ScannedMarkets++
		for index, target := range marketTargets {
			targets = append(targets, target)
			refs = append(refs, ref{market: market, outcome: market.Outcomes[index]})
		}
	}
	if len(targets) == 0 {
		return nil
	}
	books, err := service.books.Capture(ctx, service.now().UTC(), targets)
	if err != nil {
		return fmt.Errorf("capture orderbooks: %w", err)
	}
	if len(books) != len(targets) {
		return fmt.Errorf("orderbook source returned %d books for %d targets", len(books), len(targets))
	}
	for index, book := range books {
		result.ScannedTokens++
		if book.Status == domain.OrderBookStatusError || book.Status == domain.OrderBookStatusMissing {
			result.BookErrors++
			continue
		}
		candidate, ok, err := evaluate(refs[index].market, refs[index].outcome, book.Asks, limits)
		if err != nil {
			result.BookErrors++
			continue
		}
		if ok {
			result.Candidates = append(result.Candidates, candidate)
		}
	}
	return nil
}

// evaluate 判断一个 Token 的卖盘是否满足条件。asks 必须按价格升序，OrderBookSource 已保证。
func evaluate(market domain.MarketListing, outcome domain.MarketOutcome, asks []domain.PriceLevel, limits thresholds) (Candidate, bool, error) {
	if len(asks) < 2 {
		return Candidate{}, false, nil
	}
	bestPrice, err := rat(asks[0].Price)
	if err != nil {
		return Candidate{}, false, err
	}
	bestSize, err := rat(asks[0].Size)
	if err != nil {
		return Candidate{}, false, err
	}
	secondPrice, err := rat(asks[1].Price)
	if err != nil {
		return Candidate{}, false, err
	}
	if secondPrice.Cmp(bestPrice) <= 0 {
		return Candidate{}, false, nil
	}
	cost := new(big.Rat).Mul(bestPrice, bestSize)
	if cost.Cmp(limits.maxCost) > 0 {
		return Candidate{}, false, nil
	}
	spread := new(big.Rat).Sub(secondPrice, bestPrice)
	gap := new(big.Rat).Mul(spread, bestSize)
	if gap.Cmp(limits.minGap) <= 0 {
		return Candidate{}, false, nil
	}
	return Candidate{
		Market: market, Outcome: outcome, BestAsk: asks[0], SecondAsk: asks[1],
		AskSpread: format(spread), GapValue: format(gap), BestAskCost: format(cost),
	}, true, nil
}

// sortCandidates 按流动性、成交量升序，再以市场与 Outcome 稳定排序。
func sortCandidates(candidates []Candidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if c := compare(left.Market.Liquidity, right.Market.Liquidity); c != 0 {
			return c < 0
		}
		if c := compare(left.Market.Volume, right.Market.Volume); c != 0 {
			return c < 0
		}
		if left.Market.MarketID != right.Market.MarketID {
			return left.Market.MarketID < right.Market.MarketID
		}
		return left.Outcome.Index < right.Outcome.Index
	})
}

// normalize 填充默认值并校验条件。
func normalize(params Params) (Params, thresholds, error) {
	fill := func(value *domain.Decimal, fallback string) {
		if value.IsEmpty() {
			*value = domain.Decimal(fallback)
		}
	}
	fill(&params.MaxVolume, defaultMaxVolume)
	fill(&params.MaxLiquidity, defaultMaxLiquidity)
	fill(&params.MinLiquidity, defaultMinLiquidity)
	fill(&params.MinGapValue, defaultMinGapValue)
	fill(&params.MaxBestAskCost, defaultMaxBestAskCost)
	if params.Limit == 0 {
		params.Limit = defaultLimit
	}
	if params.PageSize == 0 {
		params.PageSize = defaultPageSize
	}
	if params.MaxScanMarkets == 0 {
		params.MaxScanMarkets = defaultScanMarkets
	}
	if params.ListConcurrency == 0 {
		params.ListConcurrency = defaultListConcurrency
	}
	if params.ListConcurrency < 1 || params.ListConcurrency > 16 {
		return Params{}, thresholds{}, fmt.Errorf("list concurrency must be between 1 and 16")
	}
	if params.Limit < 1 || params.Limit > 100 {
		return Params{}, thresholds{}, fmt.Errorf("limit must be between 1 and 100")
	}
	if params.PageSize < 1 || params.PageSize > 100 {
		return Params{}, thresholds{}, fmt.Errorf("page size must be between 1 and 100")
	}
	if params.MaxScanMarkets < 1 || params.MaxScanMarkets > maxScanMarkets {
		return Params{}, thresholds{}, fmt.Errorf("max scan markets must be between 1 and %d", maxScanMarkets)
	}
	for name, value := range map[string]domain.Decimal{
		"max volume": params.MaxVolume, "max liquidity": params.MaxLiquidity, "min liquidity": params.MinLiquidity,
		"min gap value": params.MinGapValue, "max best ask cost": params.MaxBestAskCost,
	} {
		sign, err := value.Sign()
		if err != nil || sign < 0 {
			return Params{}, thresholds{}, fmt.Errorf("%s must be a non-negative decimal", name)
		}
	}
	minGap, _ := rat(params.MinGapValue)
	maxCost, _ := rat(params.MaxBestAskCost)
	return params, thresholds{minGap: minGap, maxCost: maxCost}, nil
}

// rat 把 Decimal 精确转成有理数。
func rat(value domain.Decimal) (*big.Rat, error) {
	parsed, ok := new(big.Rat).SetString(strings.TrimSpace(value.String()))
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", value)
	}
	return parsed, nil
}

// compare 比较两个 Decimal；无法解析的值排在最后。
func compare(left, right domain.Decimal) int {
	result, err := left.Compare(right)
	if err != nil {
		return 0
	}
	return result
}

// format 以最多 8 位小数输出，去掉尾随零。
func format(value *big.Rat) domain.Decimal {
	return domain.Decimal(value.FloatString(8)).Canonical()
}

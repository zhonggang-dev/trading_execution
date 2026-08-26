package kalshi

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type OrderBookParams struct {
	Client  *Client
	Depth   int
	Workers int
	Now     func() time.Time
}

// OrderBookSource converts Kalshi's YES-bid/NO-bid representation into a
// conventional bid/ask book for each outcome. Requests are authenticated but
// read-only; this source never invokes a state-changing endpoint.
type OrderBookSource struct {
	client  *Client
	depth   int
	workers int
	now     func() time.Time
}

type marketEnvelope struct {
	Market struct {
		Ticker                   string `json:"ticker"`
		MarketType               string `json:"market_type"`
		Status                   string `json:"status"`
		Result                   string `json:"result"`
		FractionalTradingEnabled bool   `json:"fractional_trading_enabled"`
		PriceRanges              []struct {
			Step domain.Decimal `json:"step"`
		} `json:"price_ranges"`
	} `json:"market"`
}

type orderBookEnvelope struct {
	OrderBook struct {
		Yes [][]string `json:"yes_dollars"`
		No  [][]string `json:"no_dollars"`
	} `json:"orderbook_fp"`
}

func NewOrderBookSource(params OrderBookParams) (*OrderBookSource, error) {
	if params.Client == nil {
		return nil, fmt.Errorf("authenticated Kalshi client is required")
	}
	if params.Depth == 0 {
		params.Depth = domain.StrategyOrderBookDepth
	}
	if params.Depth < 1 || params.Depth > domain.StrategyOrderBookDepth {
		return nil, fmt.Errorf("Kalshi orderbook depth must be between 1 and %d", domain.StrategyOrderBookDepth)
	}
	if params.Workers == 0 {
		params.Workers = 16
	}
	if params.Workers < 1 || params.Workers > 64 {
		return nil, fmt.Errorf("Kalshi orderbook workers must be between 1 and 64")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &OrderBookSource{client: params.Client, depth: params.Depth, workers: params.Workers, now: params.Now}, nil
}

func (source *OrderBookSource) Capture(ctx context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	books := make([]domain.OrderBookSnapshot, len(targets))
	indicesByTicker := make(map[string][]int)
	for index, target := range targets {
		if target.MarketSource.Normalize() != domain.MarketSourceKalshi {
			return nil, fmt.Errorf("Kalshi orderbook source received non-Kalshi target %q", target.TokenID)
		}
		indicesByTicker[target.MarketID] = append(indicesByTicker[target.MarketID], index)
	}
	jobs := make(chan string)
	var workers sync.WaitGroup
	workerCount := min(source.workers, len(indicesByTicker))
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ticker := range jobs {
				indices := indicesByTicker[ticker]
				marketBooks := source.captureMarket(ctx, ticker, targets, indices)
				for offset, index := range indices {
					books[index] = marketBooks[offset]
				}
			}
		}()
	}
	for ticker := range indicesByTicker {
		select {
		case jobs <- ticker:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return books, nil
}

func (source *OrderBookSource) captureMarket(ctx context.Context, ticker string, targets []domain.BookTarget, indices []int) []domain.OrderBookSnapshot {
	observedAt := source.now().UTC()
	books := make([]domain.OrderBookSnapshot, len(indices))
	for offset, index := range indices {
		target := targets[index]
		books[offset] = domain.OrderBookSnapshot{
			MarketSource: domain.MarketSourceKalshi, MarketID: target.MarketID, ConditionID: target.ConditionID,
			OutcomeIndex: target.OutcomeIndex, OutcomeID: target.OutcomeID, TokenID: target.TokenID,
			Status: domain.OrderBookStatusError, DepthLimit: source.depth, ObservedAt: observedAt,
			Bids: []domain.PriceLevel{}, Asks: []domain.PriceLevel{},
		}
	}
	var market marketEnvelope
	if err := source.getJSON(ctx, "/trade-api/v2/markets/"+url.PathEscape(ticker), &market); err != nil {
		return withError(books, "KALSHI_MARKET_REQUEST_FAILED")
	}
	if market.Market.Ticker != ticker || market.Market.MarketType != "binary" || market.Market.Status != "active" || strings.TrimSpace(market.Market.Result) != "" {
		return withError(books, "KALSHI_MARKET_NOT_TRADABLE")
	}
	tickSize, err := marketTickSize(market)
	if err != nil {
		return withError(books, "KALSHI_TICK_SIZE_INVALID")
	}
	var payload orderBookEnvelope
	bookPath := "/trade-api/v2/markets/" + url.PathEscape(ticker) + "/orderbook?depth=" + strconv.Itoa(source.depth)
	if err := source.getJSON(ctx, bookPath, &payload); err != nil {
		return withError(books, "KALSHI_ORDERBOOK_REQUEST_FAILED")
	}
	yesBids, err := parseBidLevels(payload.OrderBook.Yes)
	if err != nil {
		return withError(books, "KALSHI_YES_BIDS_INVALID")
	}
	noBids, err := parseBidLevels(payload.OrderBook.No)
	if err != nil {
		return withError(books, "KALSHI_NO_BIDS_INVALID")
	}
	yesAsks, err := complementAsks(noBids)
	if err != nil {
		return withError(books, "KALSHI_YES_ASK_INVALID")
	}
	noAsks, err := complementAsks(yesBids)
	if err != nil {
		return withError(books, "KALSHI_NO_ASK_INVALID")
	}
	for index := range books {
		books[index].SourceAt = observedAt
		books[index].TickSize = tickSize
		books[index].MinOrderSize = "1"
		switch strings.ToUpper(strings.TrimSpace(books[index].OutcomeID)) {
		case "YES":
			books[index].Bids = trimLevels(yesBids, source.depth)
			books[index].Asks = trimLevels(yesAsks, source.depth)
		case "NO":
			books[index].Bids = trimLevels(noBids, source.depth)
			books[index].Asks = trimLevels(noAsks, source.depth)
		default:
			books[index].ErrorCode = "KALSHI_OUTCOME_INVALID"
			continue
		}
		if len(books[index].Bids) == 0 || len(books[index].Asks) == 0 {
			books[index].Status = domain.OrderBookStatusEmpty
			continue
		}
		books[index].Status = domain.OrderBookStatusOK
		books[index].BestBid = books[index].Bids[0].Price
		books[index].BestAsk = books[index].Asks[0].Price
	}
	return books
}

func (source *OrderBookSource) getJSON(ctx context.Context, requestPath string, target any) error {
	return source.client.doAuthenticated(ctx, http.MethodGet, requestPath, nil, target)
}

func marketTickSize(market marketEnvelope) (domain.Decimal, error) {
	var result domain.Decimal
	for _, priceRange := range market.Market.PriceRanges {
		if sign, err := priceRange.Step.Sign(); err != nil || sign <= 0 {
			continue
		}
		if result.IsEmpty() {
			result = priceRange.Step
			continue
		}
		if comparison, _ := priceRange.Step.Compare(result); comparison < 0 {
			result = priceRange.Step
		}
	}
	if result.IsEmpty() && !market.Market.FractionalTradingEnabled {
		return "0.01", nil
	}
	if result.IsEmpty() {
		return "", fmt.Errorf("fractional market omitted price_ranges")
	}
	return result, nil
}

func parseBidLevels(raw [][]string) ([]domain.PriceLevel, error) {
	levels := make([]domain.PriceLevel, 0, len(raw))
	for _, value := range raw {
		if len(value) != 2 {
			return nil, fmt.Errorf("Kalshi orderbook level must have price and size")
		}
		price, err := domain.ParseDecimal(value[0])
		if err != nil {
			return nil, err
		}
		size, err := domain.ParseDecimal(value[1])
		if err != nil {
			return nil, err
		}
		if sign, _ := price.Sign(); sign <= 0 {
			return nil, fmt.Errorf("Kalshi price must be positive")
		}
		if comparison, _ := price.Compare("1"); comparison >= 0 {
			return nil, fmt.Errorf("Kalshi bid price must be below one")
		}
		if sign, _ := size.Sign(); sign <= 0 {
			return nil, fmt.Errorf("Kalshi size must be positive")
		}
		levels = append(levels, domain.PriceLevel{Price: price, Size: size})
	}
	sort.Slice(levels, func(left, right int) bool {
		comparison, _ := levels[left].Price.Compare(levels[right].Price)
		return comparison > 0
	})
	return levels, nil
}

func complementAsks(bids []domain.PriceLevel) ([]domain.PriceLevel, error) {
	asks := make([]domain.PriceLevel, 0, len(bids))
	for _, bid := range bids {
		priceRat, ok := new(big.Rat).SetString(bid.Price.String())
		if !ok {
			return nil, fmt.Errorf("invalid Kalshi bid price")
		}
		complement := new(big.Rat).Sub(big.NewRat(1, 1), priceRat)
		price, err := domain.ParseDecimal(strings.TrimRight(strings.TrimRight(complement.FloatString(4), "0"), "."))
		if err != nil {
			return nil, err
		}
		asks = append(asks, domain.PriceLevel{Price: price, Size: bid.Size})
	}
	sort.Slice(asks, func(left, right int) bool {
		comparison, _ := asks[left].Price.Compare(asks[right].Price)
		return comparison < 0
	})
	return asks, nil
}

func trimLevels(levels []domain.PriceLevel, depth int) []domain.PriceLevel {
	if len(levels) > depth {
		levels = levels[:depth]
	}
	return append([]domain.PriceLevel(nil), levels...)
}

func withError(books []domain.OrderBookSnapshot, code string) []domain.OrderBookSnapshot {
	for index := range books {
		books[index].ErrorCode = code
	}
	return books
}

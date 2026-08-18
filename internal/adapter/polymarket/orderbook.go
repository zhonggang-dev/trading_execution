package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// OrderBookParams 表示后端使用的 OrderBookParams 类型。
type OrderBookParams struct {
	BaseURL    string
	HTTPClient *http.Client
	Depth      int
	Workers    int
	Now        func() time.Time
}

// OrderBookSource reads public CLOB books, normalizes ordering, and keeps the
// same top-15 depth used by the old live service.
type OrderBookSource struct {
	baseURL    *url.URL
	httpClient *http.Client
	depth      int
	workers    int
	now        func() time.Time
}

// clobLevel 表示后端使用的 clobLevel 类型。
type clobLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// NewOrderBookSource 创建并初始化 Order Book Source。
func NewOrderBookSource(params OrderBookParams) (*OrderBookSource, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("Polymarket CLOB base URL is invalid")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if params.Depth == 0 {
		params.Depth = 15
	}
	if params.Depth < 1 || params.Depth > 15 {
		return nil, fmt.Errorf("orderbook depth must be between 1 and 15")
	}
	if params.Workers == 0 {
		params.Workers = 16
	}
	if params.Workers < 1 || params.Workers > 128 {
		return nil, fmt.Errorf("orderbook workers must be between 1 and 128")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &OrderBookSource{
		baseURL:    baseURL,
		httpClient: params.HTTPClient,
		depth:      params.Depth,
		workers:    params.Workers,
		now:        params.Now,
	}, nil
}

// Capture 为目标集合采集并返回冻结行情快照。
func (source *OrderBookSource) Capture(ctx context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	books := make([]domain.OrderBookSnapshot, len(targets))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workerCount := min(source.workers, len(targets))
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				books[index] = source.captureOne(ctx, targets[index])
			}
		}()
	}
	for index := range targets {
		select {
		case jobs <- index:
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

// captureOne 采集 One。
func (source *OrderBookSource) captureOne(ctx context.Context, target domain.BookTarget) domain.OrderBookSnapshot {
	observedAt := source.now().UTC()
	book := domain.OrderBookSnapshot{
		MarketID:     target.MarketID,
		ConditionID:  target.ConditionID,
		OutcomeIndex: target.OutcomeIndex,
		TokenID:      target.TokenID,
		Status:       domain.OrderBookStatusError,
		DepthLimit:   source.depth,
		SourceAt:     observedAt,
		ObservedAt:   observedAt,
		Bids:         []domain.PriceLevel{},
		Asks:         []domain.PriceLevel{},
	}
	endpoint := source.baseURL.ResolveReference(&url.URL{Path: "/book"})
	query := endpoint.Query()
	query.Set("token_id", target.TokenID)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		book.ErrorCode = "REQUEST_BUILD_FAILED"
		return book
	}
	response, err := source.httpClient.Do(request)
	if err != nil {
		book.ErrorCode = "CLOB_REQUEST_FAILED"
		return book
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		book.ErrorCode = "CLOB_HTTP_" + strconv.Itoa(response.StatusCode)
		return book
	}
	var payload struct {
		Timestamp    string         `json:"timestamp"`
		TickSize     domain.Decimal `json:"tick_size"`
		MinOrderSize domain.Decimal `json:"min_order_size"`
		Bids         []clobLevel    `json:"bids"`
		Asks         []clobLevel    `json:"asks"`
	}
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&payload); err != nil {
		book.ErrorCode = "CLOB_INVALID_JSON"
		return book
	}
	if sourceAt, ok := parseCLOBTime(payload.Timestamp); ok {
		book.SourceAt = sourceAt
	}
	book.ObservedAt = source.now().UTC()
	book.TickSize = payload.TickSize
	book.MinOrderSize = payload.MinOrderSize
	book.Bids, err = normalizeLevels(payload.Bids, true, source.depth)
	if err != nil {
		book.ErrorCode = "CLOB_INVALID_BID"
		return book
	}
	book.Asks, err = normalizeLevels(payload.Asks, false, source.depth)
	if err != nil {
		book.ErrorCode = "CLOB_INVALID_ASK"
		return book
	}
	book.ErrorCode = ""
	if len(book.Bids) > 0 {
		book.BestBid = book.Bids[0].Price
	}
	if len(book.Asks) > 0 {
		book.BestAsk = book.Asks[0].Price
	}
	if len(book.Bids) == 0 || len(book.Asks) == 0 {
		book.Status = domain.OrderBookStatusEmpty
	} else {
		book.Status = domain.OrderBookStatusOK
	}
	return book
}

// normalizeLevels 规范化 Levels 的字段和表示。
func normalizeLevels(raw []clobLevel, bids bool, depth int) ([]domain.PriceLevel, error) {
	levels := make([]domain.PriceLevel, 0, len(raw))
	for _, item := range raw {
		price, err := domain.ParseDecimal(item.Price)
		if err != nil {
			return nil, err
		}
		size, err := domain.ParseDecimal(item.Size)
		if err != nil {
			return nil, err
		}
		priceSign, _ := price.Sign()
		sizeSign, _ := size.Sign()
		if priceSign <= 0 || sizeSign <= 0 {
			return nil, fmt.Errorf("price and size must be positive")
		}
		levels = append(levels, domain.PriceLevel{Price: price, Size: size})
	}
	sort.Slice(levels, func(i, j int) bool {
		comparison, _ := levels[i].Price.Compare(levels[j].Price)
		if bids {
			return comparison > 0
		}
		return comparison < 0
	})
	if len(levels) > depth {
		levels = levels[:depth]
	}
	return levels, nil
}

// parseCLOBTime 解析 CLOB 数据 Time。
func parseCLOBTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	if timestamp, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return timestamp.UTC(), true
	}
	milliseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(milliseconds).UTC(), true
}

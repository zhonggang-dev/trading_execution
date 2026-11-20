package polymarket

import (
	"bytes"
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
	// BatchSize 大于 0 时改用 POST /books 批量读取，每次请求最多 BatchSize 个 token（上限 500）。
	// 零值保持逐 token GET /book，决策链路依赖该行为。
	BatchSize int
	Now       func() time.Time
}

// OrderBookSource 读取公开 CLOB 订单簿、规范排序，并保持与旧实盘服务一致的前 15 档深度。
type OrderBookSource struct {
	baseURL    *url.URL
	httpClient *http.Client
	depth      int
	workers    int
	batchSize  int
	now        func() time.Time
}

// maxBatchSize 是 Polymarket POST /books 单次请求的 token 数上限。
const maxBatchSize = 500

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
	if params.BatchSize < 0 || params.BatchSize > maxBatchSize {
		return nil, fmt.Errorf("orderbook batch size must be between 0 and %d", maxBatchSize)
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &OrderBookSource{
		baseURL:    baseURL,
		httpClient: params.HTTPClient,
		depth:      params.Depth,
		workers:    params.Workers,
		batchSize:  params.BatchSize,
		now:        params.Now,
	}, nil
}

// Capture 为目标集合采集并返回冻结行情快照。
func (source *OrderBookSource) Capture(ctx context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	if source.batchSize > 0 {
		return source.captureBatches(ctx, targets)
	}
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
	source.fillBook(&book, payload.Timestamp, payload.TickSize, payload.MinOrderSize, payload.Bids, payload.Asks)
	return book
}

// fillBook 把一个 CLOB 订单簿负载规范化后写入快照，并按买卖盘是否齐全设置状态。
func (source *OrderBookSource) fillBook(
	book *domain.OrderBookSnapshot, timestamp string, tickSize, minOrderSize domain.Decimal, rawBids, rawAsks []clobLevel,
) {
	if sourceAt, ok := parseCLOBTime(timestamp); ok {
		book.SourceAt = sourceAt
	}
	book.ObservedAt = source.now().UTC()
	book.TickSize = tickSize
	book.MinOrderSize = minOrderSize
	var err error
	book.Bids, err = normalizeLevels(rawBids, true, source.depth)
	if err != nil {
		book.Status, book.ErrorCode = domain.OrderBookStatusError, "CLOB_INVALID_BID"
		book.Bids, book.Asks = []domain.PriceLevel{}, []domain.PriceLevel{}
		return
	}
	book.Asks, err = normalizeLevels(rawAsks, false, source.depth)
	if err != nil {
		book.Status, book.ErrorCode = domain.OrderBookStatusError, "CLOB_INVALID_ASK"
		book.Bids, book.Asks = []domain.PriceLevel{}, []domain.PriceLevel{}
		return
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
}

// captureBatches 把目标切成每批 batchSize 个 token，用 POST /books 并发读取。
// 单批失败只把该批 token 标为 ERROR，与逐 token 模式的带内失败语义一致。
func (source *OrderBookSource) captureBatches(ctx context.Context, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	books := make([]domain.OrderBookSnapshot, len(targets))
	type span struct{ from, to int }
	var spans []span
	for from := 0; from < len(targets); from += source.batchSize {
		spans = append(spans, span{from: from, to: min(from+source.batchSize, len(targets))})
	}
	jobs := make(chan span)
	var workers sync.WaitGroup
	for worker := 0; worker < min(source.workers, len(spans)); worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				source.captureBatch(ctx, targets[job.from:job.to], books[job.from:job.to])
			}
		}()
	}
	for _, job := range spans {
		select {
		case jobs <- job:
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

// captureBatch 读取一批 token 的订单簿并写入 out（与 targets 等长、同序）。
func (source *OrderBookSource) captureBatch(ctx context.Context, targets []domain.BookTarget, out []domain.OrderBookSnapshot) {
	observedAt := source.now().UTC()
	for index, target := range targets {
		out[index] = domain.OrderBookSnapshot{
			MarketID: target.MarketID, ConditionID: target.ConditionID, OutcomeIndex: target.OutcomeIndex,
			TokenID: target.TokenID, Status: domain.OrderBookStatusError, DepthLimit: source.depth,
			SourceAt: observedAt, ObservedAt: observedAt, Bids: []domain.PriceLevel{}, Asks: []domain.PriceLevel{},
		}
	}
	failAll := func(code string) {
		for index := range out {
			out[index].ErrorCode = code
		}
	}
	type tokenRequest struct {
		TokenID string `json:"token_id"`
	}
	requests := make([]tokenRequest, len(targets))
	for index, target := range targets {
		requests[index] = tokenRequest{TokenID: target.TokenID}
	}
	body, err := json.Marshal(requests)
	if err != nil {
		failAll("REQUEST_BUILD_FAILED")
		return
	}
	endpoint := source.baseURL.ResolveReference(&url.URL{Path: "/books"})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		failAll("REQUEST_BUILD_FAILED")
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := source.httpClient.Do(request)
	if err != nil {
		failAll("CLOB_REQUEST_FAILED")
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		failAll("CLOB_HTTP_" + strconv.Itoa(response.StatusCode))
		return
	}
	var payload []struct {
		AssetID      string         `json:"asset_id"`
		Timestamp    string         `json:"timestamp"`
		TickSize     domain.Decimal `json:"tick_size"`
		MinOrderSize domain.Decimal `json:"min_order_size"`
		Bids         []clobLevel    `json:"bids"`
		Asks         []clobLevel    `json:"asks"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		failAll("CLOB_INVALID_JSON")
		return
	}
	byToken := make(map[string]int, len(payload))
	for index := range payload {
		byToken[payload[index].AssetID] = index
	}
	for index, target := range targets {
		position, found := byToken[target.TokenID]
		if !found {
			// 批量接口不会为没有订单簿的 token 返回条目，按缺失处理而不是当作空盘。
			out[index].Status, out[index].ErrorCode = domain.OrderBookStatusMissing, "CLOB_BOOK_NOT_RETURNED"
			continue
		}
		item := payload[position]
		source.fillBook(&out[index], item.Timestamp, item.TickSize, item.MinOrderSize, item.Bids, item.Asks)
	}
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

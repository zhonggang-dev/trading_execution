package polymarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const maxMidPriceHistoryResponseBytes = 32 << 20

// MidPriceHistoryParams 表示后端使用的 MidPriceHistoryParams 类型。
type MidPriceHistoryParams struct {
	BaseURL           string
	HTTPClient        *http.Client
	Fidelity          time.Duration
	BatchSize         int
	Workers           int
	RequestsPerSecond int
	MaxAttempts       int
	Now               func() time.Time
}

// MidPriceHistorySource reads Polymarket's historical midpoint series. The
// upstream `p` value is already the midpoint and is mapped directly to
// MidPricePoint.P without another bid/ask calculation.
type MidPriceHistorySource struct {
	baseURL         *url.URL
	httpClient      *http.Client
	fidelity        time.Duration
	fidelityMinutes int
	batchSize       int
	workers         int
	requestInterval time.Duration
	maxAttempts     int
	now             func() time.Time
	rateMu          sync.Mutex
	nextRequest     time.Time
}

var _ port.MidPriceHistorySource = (*MidPriceHistorySource)(nil)

// NewMidPriceHistorySource 创建并初始化 Mid Price History Source。
func NewMidPriceHistorySource(params MidPriceHistoryParams) (*MidPriceHistorySource, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("Polymarket CLOB base URL is invalid")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if params.Fidelity == 0 {
		params.Fidelity = time.Minute
	}
	if params.Fidelity < time.Minute || params.Fidelity%time.Minute != 0 {
		return nil, fmt.Errorf("mid-price fidelity must be a positive whole number of minutes")
	}
	if params.BatchSize == 0 {
		params.BatchSize = 20
	}
	if params.BatchSize < 1 || params.BatchSize > 20 {
		return nil, fmt.Errorf("mid-price batch size must be between 1 and 20")
	}
	if params.Workers == 0 {
		params.Workers = 4
	}
	if params.Workers < 1 || params.Workers > 16 {
		return nil, fmt.Errorf("mid-price workers must be between 1 and 16")
	}
	if params.RequestsPerSecond == 0 {
		params.RequestsPerSecond = 10
	}
	if params.RequestsPerSecond < 1 || params.RequestsPerSecond > 100 {
		return nil, fmt.Errorf("mid-price requests per second must be between 1 and 100")
	}
	if params.MaxAttempts == 0 {
		params.MaxAttempts = 3
	}
	if params.MaxAttempts < 1 || params.MaxAttempts > 6 {
		return nil, fmt.Errorf("mid-price maximum attempts must be between 1 and 6")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &MidPriceHistorySource{
		baseURL:         baseURL,
		httpClient:      params.HTTPClient,
		fidelity:        params.Fidelity,
		fidelityMinutes: int(params.Fidelity / time.Minute),
		batchSize:       params.BatchSize,
		workers:         params.Workers,
		requestInterval: time.Second / time.Duration(params.RequestsPerSecond),
		maxAttempts:     params.MaxAttempts,
		now:             params.Now,
	}, nil
}

// Capture 为目标集合采集并返回冻结行情快照。
func (source *MidPriceHistorySource) Capture(
	ctx context.Context,
	decisionAt time.Time,
	lookback time.Duration,
	targets []domain.BookTarget,
) ([]domain.MidPriceHistory, error) {
	decisionAt = decisionAt.UTC()
	if decisionAt.IsZero() || lookback <= 0 {
		return nil, fmt.Errorf("mid-price decision time and lookback are required")
	}
	if len(targets) == 0 {
		return []domain.MidPriceHistory{}, nil
	}
	windowStart := decisionAt.Add(-lookback)
	histories := make([]domain.MidPriceHistory, len(targets))
	type batch struct{ start, end int }
	jobs := make(chan batch)
	var workers sync.WaitGroup
	workerCount := min(source.workers, (len(targets)+source.batchSize-1)/source.batchSize)
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				batchHistories := source.captureBatch(ctx, decisionAt, windowStart, targets[job.start:job.end])
				copy(histories[job.start:job.end], batchHistories)
			}
		}()
	}
	for start := 0; start < len(targets); start += source.batchSize {
		end := min(start+source.batchSize, len(targets))
		select {
		case jobs <- batch{start: start, end: end}:
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
	return histories, nil
}

// clobMidPricePoint 表示后端使用的 clobMidPricePoint 类型。
type clobMidPricePoint struct {
	Timestamp int64       `json:"t"`
	Price     json.Number `json:"p"`
}

// captureBatch 采集 Batch。
func (source *MidPriceHistorySource) captureBatch(
	ctx context.Context,
	windowEnd time.Time,
	windowStart time.Time,
	targets []domain.BookTarget,
) []domain.MidPriceHistory {
	payload, errorCode, err := source.fetchBatch(ctx, targets, windowStart, windowEnd)
	fetchedAt := source.now().UTC()
	histories := make([]domain.MidPriceHistory, len(targets))
	for index, target := range targets {
		histories[index] = newMidPriceHistory(target, windowStart, windowEnd, fetchedAt, source.fidelity)
	}
	if err != nil {
		for index := range histories {
			histories[index].Status = domain.MidPriceHistoryStatusError
			histories[index].ErrorCode = errorCode
		}
		return histories
	}
	for index, target := range targets {
		raw, found := payload.History[target.TokenID]
		if !found {
			histories[index].Status = domain.MidPriceHistoryStatusMissing
			histories[index].ErrorCode = "SOURCE_DID_NOT_RETURN_TOKEN"
			continue
		}
		points, normalizeErr := normalizeMidPricePoints(raw, windowStart, windowEnd)
		if normalizeErr != nil {
			histories[index].Status = domain.MidPriceHistoryStatusError
			histories[index].ErrorCode = "CLOB_INVALID_MID_PRICE_HISTORY"
			continue
		}
		if len(points) == 0 {
			histories[index].Status = domain.MidPriceHistoryStatusEmpty
			histories[index].ErrorCode = "CLOB_NO_MID_PRICE_HISTORY"
			continue
		}
		histories[index].MidPrices = points
		histories[index].CoverageStart = points[0].IntervalEndAt
		histories[index].CoverageEnd = points[len(points)-1].IntervalEndAt
		tolerance := 2 * source.fidelity
		coverage := histories[index].CoverageEnd.Sub(histories[index].CoverageStart)
		if coverage < time.Duration(domain.StrategyMinimumHistorySeconds)*time.Second ||
			histories[index].CoverageEnd.Before(windowEnd.Add(-tolerance)) {
			histories[index].Status = domain.MidPriceHistoryStatusPartial
		} else {
			histories[index].Status = domain.MidPriceHistoryStatusOK
		}
	}
	return histories
}

// newMidPriceHistory 创建并初始化 Mid Price History。
func newMidPriceHistory(
	target domain.BookTarget,
	windowStart time.Time,
	windowEnd time.Time,
	fetchedAt time.Time,
	fidelity time.Duration,
) domain.MidPriceHistory {
	return domain.MidPriceHistory{
		MarketID:           target.MarketID,
		ConditionID:        target.ConditionID,
		OutcomeIndex:       target.OutcomeIndex,
		TokenID:            target.TokenID,
		Status:             domain.MidPriceHistoryStatusError,
		WindowStart:        windowStart,
		WindowEnd:          windowEnd,
		FidelitySeconds:    int(fidelity / time.Second),
		Sampling:           domain.MidPriceSamplingUpstreamRaw,
		MissingValues:      domain.MidPriceMissingValuePolicyNoFill,
		TimestampSemantics: domain.MidPriceTimestampSemanticsIntervalEndUTC,
		FetchedAt:          fetchedAt,
		MidPrices:          []domain.MidPricePoint{},
	}
}

// fetchBatch 从外部数据源批量获取 Batch。
func (source *MidPriceHistorySource) fetchBatch(
	ctx context.Context,
	targets []domain.BookTarget,
	windowStart time.Time,
	windowEnd time.Time,
) (struct {
	History map[string][]clobMidPricePoint `json:"history"`
}, string, error) {
	var decoded struct {
		History map[string][]clobMidPricePoint `json:"history"`
	}
	markets := make([]string, len(targets))
	for index, target := range targets {
		markets[index] = target.TokenID
	}
	requestBody := struct {
		Markets  []string `json:"markets"`
		StartTS  int64    `json:"start_ts"`
		EndTS    int64    `json:"end_ts"`
		Fidelity int      `json:"fidelity"`
	}{
		Markets: markets, StartTS: windowStart.Unix(), EndTS: windowEnd.Unix(), Fidelity: source.fidelityMinutes,
	}
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return decoded, "REQUEST_ENCODE_FAILED", err
	}
	endpoint := source.baseURL.ResolveReference(&url.URL{Path: "/batch-prices-history"})
	lastCode := "CLOB_REQUEST_FAILED"
	var lastErr error
	for attempt := 0; attempt < source.maxAttempts; attempt++ {
		if err := source.waitForRateLimit(ctx); err != nil {
			return decoded, "CLOB_REQUEST_CANCELED", err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
		if err != nil {
			return decoded, "REQUEST_BUILD_FAILED", err
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		response, err := source.httpClient.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return decoded, "CLOB_REQUEST_CANCELED", ctx.Err()
			}
			lastErr = err
			lastCode = "CLOB_REQUEST_FAILED"
		} else {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, maxMidPriceHistoryResponseBytes+1))
			response.Body.Close()
			if readErr != nil {
				lastErr = readErr
				lastCode = "CLOB_RESPONSE_READ_FAILED"
			} else if len(body) > maxMidPriceHistoryResponseBytes {
				lastErr = fmt.Errorf("response exceeds %d bytes", maxMidPriceHistoryResponseBytes)
				lastCode = "CLOB_RESPONSE_TOO_LARGE"
			} else if response.StatusCode == http.StatusOK {
				decoder := json.NewDecoder(bytes.NewReader(body))
				decoder.UseNumber()
				if err := decoder.Decode(&decoded); err != nil {
					return decoded, "CLOB_INVALID_JSON", fmt.Errorf("decode midpoint history: %w", err)
				}
				if decoded.History == nil {
					return decoded, "CLOB_INVALID_JSON", fmt.Errorf("decode midpoint history: history map is required")
				}
				return decoded, "", nil
			} else {
				lastCode = "CLOB_HTTP_" + strconv.Itoa(response.StatusCode)
				lastErr = fmt.Errorf("Polymarket midpoint history HTTP %d", response.StatusCode)
				if response.StatusCode != http.StatusTooManyRequests && response.StatusCode < http.StatusInternalServerError {
					return decoded, lastCode, lastErr
				}
			}
		}
		if attempt+1 < source.maxAttempts {
			if err := waitContext(ctx, time.Duration(attempt+1)*200*time.Millisecond); err != nil {
				return decoded, "CLOB_REQUEST_CANCELED", err
			}
		}
	}
	return decoded, lastCode, lastErr
}

// normalizeMidPricePoints 规范化 Mid Price Points 的字段和表示。
func normalizeMidPricePoints(raw []clobMidPricePoint, windowStart, windowEnd time.Time) ([]domain.MidPricePoint, error) {
	byIntervalEnd := make(map[int64]domain.MidPricePoint, len(raw))
	for _, item := range raw {
		rawAt := time.Unix(item.Timestamp, 0).UTC()
		if item.Timestamp <= 0 || rawAt.Before(windowStart) || rawAt.After(windowEnd) {
			continue
		}
		intervalEndAt := rawAt.Truncate(time.Minute)
		if !intervalEndAt.Equal(rawAt) {
			intervalEndAt = intervalEndAt.Add(time.Minute)
		}
		if intervalEndAt.Before(windowStart) || intervalEndAt.After(windowEnd) {
			continue
		}
		midPrice, err := domain.ParseDecimal(item.Price.String())
		if err != nil {
			return nil, err
		}
		if sign, _ := midPrice.Sign(); sign < 0 {
			return nil, fmt.Errorf("midpoint must be in [0,1]")
		}
		if comparison, _ := midPrice.Compare(domain.Decimal("1")); comparison > 0 {
			return nil, fmt.Errorf("midpoint must be in [0,1]")
		}
		byIntervalEnd[intervalEndAt.Unix()] = domain.MidPricePoint{IntervalEndAt: intervalEndAt, P: midPrice}
	}
	timestamps := make([]int64, 0, len(byIntervalEnd))
	for timestamp := range byIntervalEnd {
		timestamps = append(timestamps, timestamp)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
	points := make([]domain.MidPricePoint, 0, len(timestamps))
	for _, timestamp := range timestamps {
		points = append(points, byIntervalEnd[timestamp])
	}
	return points, nil
}

// waitForRateLimit 等待 For Rate Limit 或上下文结束。
func (source *MidPriceHistorySource) waitForRateLimit(ctx context.Context) error {
	source.rateMu.Lock()
	now := time.Now()
	reservedAt := now
	if source.nextRequest.After(now) {
		reservedAt = source.nextRequest
	}
	source.nextRequest = reservedAt.Add(source.requestInterval)
	source.rateMu.Unlock()
	return waitContext(ctx, time.Until(reservedAt))
}

// waitContext 等待 Context 或上下文结束。
func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

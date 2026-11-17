package polymarketdata

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

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// MaxPriceHistorySpanSeconds 是 /v2/prices-history 单次查询允许的最大跨度（15 天）。
const MaxPriceHistorySpanSeconds = 15 * 24 * 3600

const maxPriceHistoryPoints = 10_000

// PricePoint 是某个 outcome token 在一个时间桶起点的价格。
type PricePoint struct {
	Timestamp int64
	Price     domain.Decimal
}

// PriceHistory 只读访问公开的 /v2/prices-history，返回按时间升序的价格桶。
// 跨度超过 15 天或上游提示还有下一页时直接报错，不返回不完整的数据。
func (client *PnLClient) PriceHistory(ctx context.Context, tokenID string, start, end int64, bucketSeconds int) ([]PricePoint, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" || len(tokenID) > 100 || strings.Trim(tokenID, "0123456789") != "" {
		return nil, fmt.Errorf("price history token id is invalid")
	}
	if start <= 0 || end <= start || end-start > MaxPriceHistorySpanSeconds {
		return nil, fmt.Errorf("price history range is invalid")
	}
	if bucketSeconds < 60 || bucketSeconds > 86400 {
		return nil, fmt.Errorf("price history bucket is invalid")
	}
	endpoint := client.baseURL.ResolveReference(&url.URL{Path: "/v2/prices-history"})
	query := endpoint.Query()
	query.Set("token_id", tokenID)
	query.Set("start", strconv.FormatInt(start, 10))
	query.Set("end", strconv.FormatInt(end, 10))
	query.Set("bucket_seconds", strconv.Itoa(bucketSeconds))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Data API price history request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("query Data API price history: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxPnLResponseBytes+1))
	response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read Data API price history: %w", readErr)
	}
	if len(body) > maxPnLResponseBytes {
		return nil, fmt.Errorf("Data API price history response is too large")
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Data API price history returned HTTP %d", response.StatusCode)
	}
	var envelope struct {
		Data *[]struct {
			Timestamp int64           `json:"timestamp"`
			Price     json.RawMessage `json:"price"`
		} `json:"data"`
		Pagination struct {
			HasMore bool `json:"has_more"`
		} `json:"pagination"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode Data API price history: %w", err)
	}
	if envelope.Data == nil {
		return nil, fmt.Errorf("decode Data API price history: data is required")
	}
	if envelope.Pagination.HasMore || len(*envelope.Data) > maxPriceHistoryPoints {
		return nil, fmt.Errorf("Data API price history is truncated")
	}
	points := make([]PricePoint, 0, len(*envelope.Data))
	for _, item := range *envelope.Data {
		if item.Timestamp <= 0 {
			return nil, fmt.Errorf("invalid Data API price history timestamp")
		}
		raw := strings.Trim(strings.TrimSpace(string(item.Price)), "\"")
		price, err := normalizePnLDecimal(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid Data API price history price")
		}
		if sign, _ := price.Sign(); sign < 0 {
			return nil, fmt.Errorf("invalid Data API price history price")
		}
		if over, _ := price.Compare(domain.Decimal("1")); over > 0 {
			return nil, fmt.Errorf("invalid Data API price history price")
		}
		points = append(points, PricePoint{Timestamp: item.Timestamp, Price: price})
	}
	sort.Slice(points, func(left, right int) bool { return points[left].Timestamp < points[right].Timestamp })
	return points, nil
}

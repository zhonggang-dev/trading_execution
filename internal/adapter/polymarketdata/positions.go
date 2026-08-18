package polymarketdata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const maxPositionsResponseBytes = 16 << 20

// PositionClientParams 表示后端使用的 PositionClientParams 类型。
type PositionClientParams struct {
	BaseURL           string
	HTTPClient        *http.Client
	RequestsPerSecond int
	Now               func() time.Time
}

// PositionClient 表示后端使用的 PositionClient 类型。
type PositionClient struct {
	baseURL       *url.URL
	httpClient    *http.Client
	now           func() time.Time
	interval      time.Duration
	requestMu     sync.Mutex
	nextRequestAt time.Time
}

var _ port.ExternalPositionSource = (*PositionClient)(nil)

// NewPositionClient 创建并初始化 Position Client。
func NewPositionClient(params PositionClientParams) (*PositionClient, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("Polymarket Data API base URL is invalid")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	if params.RequestsPerSecond == 0 {
		params.RequestsPerSecond = 10
	}
	if params.RequestsPerSecond < 1 || params.RequestsPerSecond > 15 {
		return nil, fmt.Errorf("Data API position requests per second must be between 1 and 15")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &PositionClient{
		baseURL: baseURL, httpClient: params.HTTPClient, now: params.Now,
		interval: time.Second / time.Duration(params.RequestsPerSecond),
	}, nil
}

// wirePosition 表示后端使用的 wirePosition 类型。
type wirePosition struct {
	ConditionID  string      `json:"conditionId"`
	TokenID      string      `json:"asset"`
	Outcome      string      `json:"outcome"`
	OutcomeIndex *int        `json:"outcomeIndex"`
	Size         wireDecimal `json:"size"`
	Average      wireDecimal `json:"avgPrice"`
	Current      wireDecimal `json:"curPrice"`
	NegRisk      bool        `json:"negativeRisk"`
	Redeemable   bool        `json:"redeemable"`
}

// ListExternalPositions 分页查询指定钱包的外部 Polymarket 仓位。
func (client *PositionClient) ListExternalPositions(ctx context.Context, walletAddress string) ([]domain.ExternalPosition, error) {
	walletAddress = strings.ToLower(strings.TrimSpace(walletAddress))
	if walletAddress == "" {
		return nil, fmt.Errorf("wallet address is required")
	}
	const pageSize = 500
	positions := make([]domain.ExternalPosition, 0)
	seen := make(map[string]struct{})
	for page := 0; page < 20; page++ {
		endpoint := client.baseURL.ResolveReference(&url.URL{Path: "/positions"})
		query := endpoint.Query()
		query.Set("user", walletAddress)
		query.Set("sizeThreshold", "0")
		query.Set("includeArchived", "true")
		query.Set("limit", strconv.Itoa(pageSize))
		query.Set("offset", strconv.Itoa(page*pageSize))
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		if err := client.waitForRequest(ctx); err != nil {
			return nil, err
		}
		response, err := client.httpClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("query Data API positions: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxPositionsResponseBytes+1))
		response.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read Data API positions: %w", readErr)
		}
		if len(body) > maxPositionsResponseBytes {
			return nil, fmt.Errorf("Data API positions response is too large")
		}
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Data API positions HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
		}
		var values []wirePosition
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&values); err != nil {
			// The Data API adds fields over time. Decode the same known fields
			// permissively, while still validating every accounting field below.
			if err := json.Unmarshal(body, &values); err != nil {
				return nil, fmt.Errorf("decode Data API positions: %w", err)
			}
		}
		observedAt := client.now().UTC()
		for index, value := range values {
			tokenID := strings.TrimSpace(value.TokenID)
			conditionID := strings.TrimSpace(value.ConditionID)
			outcome := strings.TrimSpace(value.Outcome)
			shares := domain.Decimal(value.Size)
			if tokenID == "" || conditionID == "" || outcome == "" {
				return nil, fmt.Errorf("Data API position %d has incomplete identity", index+page*pageSize)
			}
			if value.OutcomeIndex != nil && *value.OutcomeIndex < 0 {
				return nil, fmt.Errorf("Data API position %s has invalid outcome index", tokenID)
			}
			if sign, err := shares.Sign(); err != nil || sign < 0 {
				return nil, fmt.Errorf("Data API position %s has invalid shares", tokenID)
			} else if sign == 0 {
				continue
			}
			if _, duplicate := seen[tokenID]; duplicate {
				return nil, fmt.Errorf("Data API returned duplicate token %s", tokenID)
			}
			seen[tokenID] = struct{}{}
			positions = append(positions, domain.ExternalPosition{
				ConditionID: conditionID, TokenID: tokenID, OutcomeIndex: cloneInt(value.OutcomeIndex), OutcomeName: outcome,
				Shares: shares, AveragePrice: domain.Decimal(value.Average), CurrentPrice: domain.Decimal(value.Current),
				NegRisk: value.NegRisk, Redeemable: value.Redeemable,
				Source: "POLYMARKET_DATA_API", ObservedAt: observedAt,
			})
		}
		if len(values) < pageSize {
			return positions, nil
		}
	}
	return nil, fmt.Errorf("Data API positions pagination exceeded 20 pages")
}

// cloneInt 复制 Int。
func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// waitForRequest 等待 For Request 或上下文结束。
func (client *PositionClient) waitForRequest(ctx context.Context) error {
	client.requestMu.Lock()
	now := time.Now()
	reservedAt := now
	if client.nextRequestAt.After(now) {
		reservedAt = client.nextRequestAt
	}
	client.nextRequestAt = reservedAt.Add(client.interval)
	client.requestMu.Unlock()
	if !reservedAt.After(now) {
		return nil
	}
	timer := time.NewTimer(reservedAt.Sub(now))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// wireDecimal 表示后端使用的 wireDecimal 类型。
type wireDecimal string

// UnmarshalJSON 从 JSON 字符串解码并校验十进制值。
func (value *wireDecimal) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*value = ""
		return nil
	}
	var text string
	if len(data) > 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
	} else {
		text = string(data)
	}
	decimal, err := domain.ParseDecimal(text)
	if err != nil {
		return err
	}
	*value = wireDecimal(decimal)
	return nil
}

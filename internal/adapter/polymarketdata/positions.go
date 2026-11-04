package polymarketdata

import (
	"context"
	"encoding/json"
	"fmt"
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
	sleep         func(context.Context, time.Duration) error
	jitter        func(time.Duration) time.Duration
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
		sleep:    sleepContext, jitter: defaultJitter,
	}, nil
}

// unlabeledOutcomeIndex is the Data API v2 sentinel for an outcome it could
// not label; it is not a real index.
const unlabeledOutcomeIndex = 999

// wirePosition is one Data API v2 /v2/positions row.
type wirePosition struct {
	ConditionID  string      `json:"condition_id"`
	TokenID      string      `json:"token_id"`
	Outcome      string      `json:"outcome"`
	OutcomeIndex *int        `json:"outcome_index"`
	Size         wireDecimal `json:"current_size"`
	Average      wireDecimal `json:"avg_price"`
	Current      wireDecimal `json:"current_price"`
	NegRisk      bool        `json:"negative_risk"`
	Redeemable   bool        `json:"redeemable"`
}

// ListExternalPositions 分页查询指定钱包的外部 Polymarket 仓位。
//
// Data API v1 was retired on 2026-11-02. The v2 default status OPEN is the
// superset that still includes settled-but-unredeemed rows (winning and
// losing), matching the v1 view; filter_amount=0 lifts the v2 0.1-share
// default dust floor that v1 did not have.
func (client *PositionClient) ListExternalPositions(ctx context.Context, walletAddress string) ([]domain.ExternalPosition, error) {
	return client.listPositions(ctx, walletAddress, false)
}

// ListValuationPrices retains zero external quantities: the caller values its
// local managed shares and must not let external size filter a price source.
// Reconciliation's ListExternalPositions keeps its existing quantity semantics.
func (client *PositionClient) ListValuationPrices(ctx context.Context, walletAddress string) ([]domain.ExternalPosition, error) {
	return client.listPositions(ctx, walletAddress, true)
}
func (client *PositionClient) listPositions(ctx context.Context, walletAddress string, includeZero bool) ([]domain.ExternalPosition, error) {
	walletAddress = strings.ToLower(strings.TrimSpace(walletAddress))
	if walletAddress == "" {
		return nil, fmt.Errorf("wallet address is required")
	}
	const pageSize = 500
	positions := make([]domain.ExternalPosition, 0)
	seen := make(map[string]struct{})
	cursor := ""
	for page := 0; page < maxDataAPIPages; page++ {
		endpoint := client.baseURL.ResolveReference(&url.URL{Path: "/v2/positions"})
		query := endpoint.Query()
		// v2 cursors do not carry these filters, so every page restates them.
		query.Set("user", walletAddress)
		query.Set("filter_type", "TOKENS")
		query.Set("filter_amount", "0")
		query.Set("include_archived", "true")
		query.Set("limit", strconv.Itoa(pageSize))
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		body, err := client.doWithRetry(ctx, request, "positions")
		if err != nil {
			return nil, err
		}
		values, next, err := decodeDataAPIPage[wirePosition](body, "positions")
		if err != nil {
			return nil, err
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
			outcomeIndex := value.OutcomeIndex
			if outcomeIndex != nil && *outcomeIndex == unlabeledOutcomeIndex {
				outcomeIndex = nil
			}
			if outcomeIndex != nil && *outcomeIndex < 0 {
				return nil, fmt.Errorf("Data API position %s has invalid outcome index", tokenID)
			}
			if sign, err := shares.Sign(); err != nil || sign < 0 {
				return nil, fmt.Errorf("Data API position %s has invalid shares", tokenID)
			} else if sign == 0 && !includeZero {
				continue
			}
			if _, duplicate := seen[tokenID]; duplicate {
				return nil, fmt.Errorf("Data API returned duplicate token %s", tokenID)
			}
			seen[tokenID] = struct{}{}
			positions = append(positions, domain.ExternalPosition{
				ConditionID: conditionID, TokenID: tokenID, OutcomeIndex: cloneInt(outcomeIndex), OutcomeName: outcome,
				Shares: shares, AveragePrice: domain.Decimal(value.Average), CurrentPrice: domain.Decimal(value.Current),
				NegRisk: value.NegRisk, Redeemable: value.Redeemable,
				Source: "POLYMARKET_DATA_API", ObservedAt: observedAt,
			})
		}
		if next == "" {
			return positions, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("Data API positions pagination exceeded %d pages", maxDataAPIPages)
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

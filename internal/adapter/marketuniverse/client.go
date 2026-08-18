package marketuniverse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const maxResponseBytes = 1 << 20

// Params 表示后端使用的 Params 类型。
type Params struct {
	BaseURL     string
	BearerToken string
	HTTPClient  *http.Client
}

// Client implements the execution-facing Market Universe Service contract:
// GET /api/v1/markets/by-condition/{condition_id}.
type Client struct {
	baseURL     *url.URL
	bearerToken string
	httpClient  *http.Client
}

// marketPayload 表示后端使用的 marketPayload 类型。
type marketPayload struct {
	MarketID        string                 `json:"market_id"`
	ConditionID     string                 `json:"condition_id"`
	Active          *bool                  `json:"active"`
	Closed          *bool                  `json:"closed"`
	Resolved        *bool                  `json:"resolved"`
	ClosedAt        *time.Time             `json:"closed_at"`
	Paused          *bool                  `json:"paused"`
	AcceptingOrders *bool                  `json:"accepting_orders"`
	NegRisk         *bool                  `json:"neg_risk"`
	TickSize        domain.Decimal         `json:"tick_size"`
	Outcomes        []domain.MarketOutcome `json:"outcomes"`
	ObservedAt      time.Time              `json:"observed_at"`
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("Market Universe Service base URL is invalid")
	}
	if strings.TrimSpace(params.BearerToken) == "" {
		return nil, fmt.Errorf("Market Universe Service bearer token is required")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{
		baseURL:     baseURL,
		bearerToken: strings.TrimSpace(params.BearerToken),
		httpClient:  params.HTTPClient,
	}, nil
}

// FindByCondition 查找 By Condition。
func (client *Client) FindByCondition(ctx context.Context, conditionID string) (domain.MarketSnapshot, bool, error) {
	conditionID = strings.TrimSpace(conditionID)
	if conditionID == "" {
		return domain.MarketSnapshot{}, false, fmt.Errorf("condition_id is required")
	}
	endpoint, err := url.Parse(strings.TrimRight(client.baseURL.String(), "/") +
		"/api/v1/markets/by-condition/" + url.PathEscape(conditionID))
	if err != nil {
		return domain.MarketSnapshot{}, false, fmt.Errorf("build Market Universe Service URL: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return domain.MarketSnapshot{}, false, fmt.Errorf("build market lookup request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+client.bearerToken)
	request.Header.Set("Accept", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return domain.MarketSnapshot{}, false, fmt.Errorf("request market lookup: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return domain.MarketSnapshot{}, false, fmt.Errorf("read market lookup response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return domain.MarketSnapshot{}, false, fmt.Errorf("market lookup response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode == http.StatusNotFound {
		return domain.MarketSnapshot{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return domain.MarketSnapshot{}, false, fmt.Errorf("market lookup HTTP %d: %s", response.StatusCode, responseError(body))
	}
	var envelope struct {
		Data marketPayload `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return domain.MarketSnapshot{}, false, fmt.Errorf("decode market lookup response: %w", err)
	}
	if strings.TrimSpace(envelope.Data.ConditionID) != conditionID {
		return domain.MarketSnapshot{}, false, fmt.Errorf("market lookup returned condition_id %q for %q", envelope.Data.ConditionID, conditionID)
	}
	if envelope.Data.Active == nil || envelope.Data.Closed == nil || envelope.Data.Resolved == nil ||
		envelope.Data.Paused == nil || envelope.Data.AcceptingOrders == nil || envelope.Data.NegRisk == nil {
		return domain.MarketSnapshot{}, false, fmt.Errorf("market lookup response is missing required state fields")
	}
	return domain.MarketSnapshot{
		MarketID:        strings.TrimSpace(envelope.Data.MarketID),
		ConditionID:     strings.TrimSpace(envelope.Data.ConditionID),
		Active:          *envelope.Data.Active,
		Closed:          *envelope.Data.Closed,
		Resolved:        *envelope.Data.Resolved,
		ClosedAt:        utcTimePointer(envelope.Data.ClosedAt),
		Paused:          *envelope.Data.Paused,
		AcceptingOrders: *envelope.Data.AcceptingOrders,
		NegRisk:         *envelope.Data.NegRisk,
		TickSize:        envelope.Data.TickSize,
		Outcomes:        envelope.Data.Outcomes,
		ObservedAt:      envelope.Data.ObservedAt.UTC(),
	}, true, nil
}

// utcTimePointer 复制时间指针并规范化为 UTC。
func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

// responseError 解析并规范化 Error。
func responseError(body []byte) string {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return "invalid error response"
	}
	if envelope.Error.Code != "" || envelope.Error.Message != "" {
		return strings.TrimSpace(envelope.Error.Code + ": " + envelope.Error.Message)
	}
	return strings.TrimSpace(envelope.Code + ": " + envelope.Message)
}

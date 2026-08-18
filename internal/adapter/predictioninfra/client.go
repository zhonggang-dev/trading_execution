package predictioninfra

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const maxResponseBytes = 32 << 20

// Params 表示后端使用的 Params 类型。
type Params struct {
	BaseURL     string
	BearerToken string
	HTTPClient  *http.Client
}

// Client 表示后端使用的 Client 类型。
type Client struct {
	baseURL     *url.URL
	bearerToken string
	httpClient  *http.Client
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("prediction_infra base URL is invalid")
	}
	if strings.TrimSpace(params.BearerToken) == "" {
		return nil, fmt.Errorf("prediction_infra bearer token is required")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		baseURL:     baseURL,
		bearerToken: strings.TrimSpace(params.BearerToken),
		httpClient:  params.HTTPClient,
	}, nil
}

// Snapshot 读取指定决策边界可见的预测快照。
func (client *Client) Snapshot(ctx context.Context, decisionAt time.Time, lookback time.Duration) (domain.PredictionSnapshot, error) {
	endpoint := client.baseURL.ResolveReference(&url.URL{Path: "/api/v1/live-predictions/snapshot"})
	query := endpoint.Query()
	query.Set("decision_at", decisionAt.UTC().Format(time.RFC3339Nano))
	query.Set("lookback_seconds", strconv.FormatInt(int64(lookback/time.Second), 10))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return domain.PredictionSnapshot{}, fmt.Errorf("build prediction snapshot request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+client.bearerToken)
	request.Header.Set("Accept", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return domain.PredictionSnapshot{}, fmt.Errorf("request prediction snapshot: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return domain.PredictionSnapshot{}, fmt.Errorf("read prediction snapshot response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return domain.PredictionSnapshot{}, fmt.Errorf("prediction snapshot response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		var failure struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(body, &failure)
		return domain.PredictionSnapshot{}, fmt.Errorf("prediction snapshot HTTP %d: %s: %s", response.StatusCode, failure.Code, failure.Message)
	}
	var envelope struct {
		Code string                    `json:"code"`
		Data domain.PredictionSnapshot `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return domain.PredictionSnapshot{}, fmt.Errorf("decode prediction snapshot response: %w", err)
	}
	if envelope.Code != "LIVE_PREDICTION_SNAPSHOT_FOUND" {
		return domain.PredictionSnapshot{}, fmt.Errorf("unexpected prediction snapshot response code %q", envelope.Code)
	}
	if err := envelope.Data.Validate(decisionAt); err != nil {
		return domain.PredictionSnapshot{}, fmt.Errorf("validate prediction snapshot: %w", err)
	}
	return envelope.Data, nil
}

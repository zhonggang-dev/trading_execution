package strategyhttp

import (
	"bytes"
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

const maxResponseBytes = 8 << 20

// Params 表示后端使用的 Params 类型。
type Params struct {
	BaseURL     string
	BearerToken string
	HTTPClient  *http.Client
}

// Client 表示后端使用的 Client 类型。
type Client struct {
	endpoint    *url.URL
	bearerToken string
	httpClient  *http.Client
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("strategy base URL is invalid")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		endpoint:    baseURL.ResolveReference(&url.URL{Path: "/api/v4/decisions"}),
		bearerToken: strings.TrimSpace(params.BearerToken),
		httpClient:  params.HTTPClient,
	}, nil
}

// Decide 向外部策略服务提交冻结输入并获取决策。
func (client *Client) Decide(ctx context.Context, input domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("encode strategy decision input: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("build strategy request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Idempotency-Key", input.CycleID)
	request.Header.Set("X-Strategy-Input-ID", input.InputID)
	request.Header.Set("X-Model-ID", input.Context.ModelID)
	request.Header.Set("X-Strategy-ID", input.Context.StrategyID)
	request.Header.Set("X-Execution-Account-ID", input.Context.ExecutionAccountID)
	if client.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+client.bearerToken)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("request strategy decision: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("read strategy response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("strategy response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("strategy HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		Data domain.StrategyDecisionResponse `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("decode strategy response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return domain.StrategyDecisionResponse{}, fmt.Errorf("strategy response must contain exactly one JSON object")
	}
	return envelope.Data, nil
}

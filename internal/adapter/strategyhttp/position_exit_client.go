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
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// PositionExitClient deliberately has a separate endpoint from entry
// decisions. This keeps a scheduled exit retry from re-running entry logic.
type PositionExitClient struct {
	endpoint    *url.URL
	bearerToken string
	httpClient  *http.Client
}

var _ port.PositionExitStrategyClient = (*PositionExitClient)(nil)

// NewPositionExitClient 创建并初始化 Position Exit Client。
func NewPositionExitClient(params Params) (*PositionExitClient, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("strategy base URL is invalid")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &PositionExitClient{
		endpoint:    baseURL.ResolveReference(&url.URL{Path: "/api/v2/position-exits/evaluate"}),
		bearerToken: strings.TrimSpace(params.BearerToken),
		httpClient:  params.HTTPClient,
	}, nil
}

// EvaluatePositionExits 向外部策略服务提交持仓快照并获取退出决策。
func (client *PositionExitClient) EvaluatePositionExits(
	ctx context.Context,
	input domain.PositionExitRequest,
) (domain.PositionExitResponse, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return domain.PositionExitResponse{}, fmt.Errorf("encode position exit input: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return domain.PositionExitResponse{}, fmt.Errorf("build position exit request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Idempotency-Key", input.CycleID)
	request.Header.Set("X-Position-Exit-Input-ID", input.InputID)
	request.Header.Set("X-Model-ID", input.Context.ModelID)
	request.Header.Set("X-Strategy-ID", input.Context.StrategyID)
	request.Header.Set("X-Execution-Account-ID", input.Context.ExecutionAccountID)
	if client.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+client.bearerToken)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return domain.PositionExitResponse{}, fmt.Errorf("request position exits: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return domain.PositionExitResponse{}, fmt.Errorf("read position exit response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return domain.PositionExitResponse{}, fmt.Errorf("position exit response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		return domain.PositionExitResponse{}, fmt.Errorf("position exit HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		Data domain.PositionExitResponse `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return domain.PositionExitResponse{}, fmt.Errorf("decode position exit response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return domain.PositionExitResponse{}, fmt.Errorf("position exit response must contain exactly one JSON object")
	}
	return envelope.Data, nil
}

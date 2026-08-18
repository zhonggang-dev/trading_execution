package strategyhttp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// roundTripFunc 表示后端使用的 roundTripFunc 类型。
type roundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip 实现当前测试场景所需的辅助行为。
func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// TestPositionExitClientUsesDedicatedEndpointAndIdentityHeaders 验证 Position Exit Client Uses Dedicated Endpoint And Identity Headers 场景下的行为。
func TestPositionExitClientUsesDedicatedEndpointAndIdentityHeaders(t *testing.T) {
	client, err := NewPositionExitClient(Params{
		BaseURL: "https://strategy.example/base", BearerToken: "secret",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path != "/api/v2/position-exits/evaluate" || request.Method != http.MethodPost {
				t.Fatalf("request endpoint = %s %s", request.Method, request.URL.Path)
			}
			for header, expected := range map[string]string{
				"Idempotency-Key": "cycle-1", "X-Position-Exit-Input-ID": "input-1",
				"X-Model-ID": "model-1", "X-Strategy-ID": "multfactor_v1",
				"X-Execution-Account-ID": "account-1", "Authorization": "Bearer secret",
			} {
				if value := request.Header.Get(header); value != expected {
					t.Fatalf("%s = %q, want %q", header, value, expected)
				}
			}
			return &http.Response{
				StatusCode: http.StatusOK, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(`{"data":{"schema_version":"trading.position_exit_output.v2","cycle_id":"cycle-1","input_id":"input-1","context":{"model_id":"model-1","strategy_id":"multfactor_v1","execution_account_id":"account-1"},"decided_at":"2026-08-18T12:00:01Z","evaluations":[]}}`)),
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.EvaluatePositionExits(context.Background(), domain.PositionExitRequest{
		SchemaVersion: domain.PositionExitInputSchemaVersion,
		CycleID:       "cycle-1", InputID: "input-1", DecisionAt: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC),
		Context: domain.StrategyExecutionContext{ModelID: "model-1", StrategyID: "multfactor_v1", ExecutionAccountID: "account-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.CycleID != "cycle-1" || response.InputID != "input-1" {
		t.Fatalf("response = %#v", response)
	}
}

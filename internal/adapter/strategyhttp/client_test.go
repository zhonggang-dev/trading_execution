package strategyhttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestDecideSendsCycleAsIdempotencyKey 验证 Decide Sends Cycle As Idempotency Key 场景下的行为。
func TestDecideSendsCycleAsIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v4/decisions" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Idempotency-Key") != "account-1:20260818T042000Z" {
			t.Fatalf("Idempotency-Key = %q", request.Header.Get("Idempotency-Key"))
		}
		if request.Header.Get("X-Strategy-Input-ID") != "strategy-input-test" {
			t.Fatalf("X-Strategy-Input-ID = %q", request.Header.Get("X-Strategy-Input-ID"))
		}
		if request.Header.Get("X-Model-ID") != "model-a" || request.Header.Get("X-Strategy-ID") != "strategy-v1" ||
			request.Header.Get("X-Execution-Account-ID") != "account-1" {
			t.Fatalf("execution context headers are invalid")
		}
		var body bytes.Buffer
		if _, err := body.ReadFrom(request.Body); err != nil {
			t.Fatalf("read request: %v", err)
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(body.Bytes(), &payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if _, exists := payload["mid_price_histories"]; exists {
			t.Fatalf("request unexpectedly contains mid_price_histories: %s", body.String())
		}
		for _, key := range []string{"predictions", "positions", "orderbooks"} {
			if _, exists := payload[key]; !exists {
				t.Fatalf("request is missing %s: %s", key, body.String())
			}
		}
		var schemaVersion string
		if err := json.Unmarshal(payload["schema_version"], &schemaVersion); err != nil || schemaVersion != domain.StrategyInputSchemaVersion {
			t.Fatalf("schema_version = %q, error = %v", schemaVersion, err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"data":{"schema_version":"trading.strategy_output.v4","cycle_id":"account-1:20260818T042000Z","input_id":"strategy-input-test","context":{"model_id":"model-a","strategy_id":"strategy-v1","execution_account_id":"account-1"},"decided_at":"2026-08-18T04:20:04Z","evaluations":[],"exits":[]}}`))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	executionContext := domain.StrategyExecutionContext{ModelID: "model-a", StrategyID: "strategy-v1", ExecutionAccountID: "account-1"}
	response, err := client.Decide(t.Context(), domain.StrategyDecisionRequest{
		SchemaVersion: domain.StrategyInputSchemaVersion,
		CycleID:       "account-1:20260818T042000Z", InputID: "strategy-input-test", Context: executionContext,
		Predictions: []domain.Prediction{}, Positions: []domain.StrategyPositionLot{}, OrderBooks: []domain.OrderBookSnapshot{},
	})
	if err != nil {
		t.Fatalf("Decide() error = %v", err)
	}
	if response.CycleID != "account-1:20260818T042000Z" || !response.Context.Equal(executionContext) {
		t.Fatalf("response = %#v", response)
	}
}

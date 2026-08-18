package strategyhttp

import (
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
		var input domain.StrategyDecisionRequest
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(input.MidPriceHistories) != 1 || len(input.MidPriceHistories[0].MidPrices) != 1 ||
			input.MidPriceHistories[0].MidPrices[0].P != "0.135" {
			t.Fatalf("mid-price histories = %#v", input.MidPriceHistories)
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
		CycleID: "account-1:20260818T042000Z", InputID: "strategy-input-test", Context: executionContext,
		MidPriceHistories: []domain.MidPriceHistory{{
			TokenID: "token-1", MidPrices: []domain.MidPricePoint{{P: "0.135"}},
		}},
	})
	if err != nil {
		t.Fatalf("Decide() error = %v", err)
	}
	if response.CycleID != "account-1:20260818T042000Z" || !response.Context.Equal(executionContext) {
		t.Fatalf("response = %#v", response)
	}
}

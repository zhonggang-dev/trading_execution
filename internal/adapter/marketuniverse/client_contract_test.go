package marketuniverse

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestFindByConditionRejectsMissingStateFields 验证 Find By Condition Rejects Missing State Fields 场景下的行为。
func TestFindByConditionRejectsMissingStateFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"data":{"market_id":"market-1","condition_id":"0xabc","active":true,"closed":false,"resolved":false,"accepting_orders":true,"neg_risk":false,"tick_size":"0.01","outcomes":[],"observed_at":"2026-08-18T08:00:00Z"}}`))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "universe-secret"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, _, err = client.FindByCondition(t.Context(), "0xabc")
	if err == nil || !strings.Contains(err.Error(), "missing required state fields") {
		t.Fatalf("FindByCondition() error = %v", err)
	}
}

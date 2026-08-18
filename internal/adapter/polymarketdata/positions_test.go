package polymarketdata

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// roundTripFunc 表示后端使用的 roundTripFunc 类型。
type roundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip 实现当前测试场景所需的辅助行为。
func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// TestPositionClientPreservesExactSharesAndSettlementFlag 验证 Position Client Preserves Exact Shares And Settlement Flag 场景下的行为。
func TestPositionClientPreservesExactSharesAndSettlementFlag(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/positions" || request.URL.Query().Get("user") != "0xabc" ||
			request.URL.Query().Get("sizeThreshold") != "0" || request.URL.Query().Get("includeArchived") != "true" ||
			request.URL.Query().Get("limit") != "500" {
			t.Fatalf("request URL = %s", request.URL)
		}
		body := `[{"conditionId":"condition-1","asset":"token-1","outcome":"Up","outcomeIndex":0,"size":5.125,"avgPrice":"0.4","curPrice":1,"negativeRisk":false,"redeemable":true,"futureField":"ignored"}]`
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	})}
	client, err := NewPositionClient(PositionClientParams{
		BaseURL: "https://data-api.example", HTTPClient: httpClient, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	positions, err := client.ListExternalPositions(context.Background(), "0xAbC")
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 1 || positions[0].Shares != "5.125" || positions[0].AveragePrice != "0.4" ||
		positions[0].CurrentPrice != "1" || positions[0].OutcomeName != "Up" || positions[0].OutcomeIndex == nil ||
		*positions[0].OutcomeIndex != 0 || !positions[0].Redeemable || positions[0].ObservedAt != now {
		t.Fatalf("positions = %#v", positions)
	}
}

// TestPositionClientDoesNotReturnPartialSnapshotOnInvalidAccounting 验证 Position Client Does Not Return Partial Snapshot On Invalid Accounting 场景下的行为。
func TestPositionClientDoesNotReturnPartialSnapshotOnInvalidAccounting(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `[{"conditionId":"condition-1","asset":"token-1","outcome":"YES","size":-1,"avgPrice":0,"curPrice":0}]`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}
	client, err := NewPositionClient(PositionClientParams{BaseURL: "https://data-api.example", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	if positions, err := client.ListExternalPositions(context.Background(), "0xabc"); err == nil || positions != nil {
		t.Fatalf("ListExternalPositions() = %#v, %v; want fail closed", positions, err)
	}
}

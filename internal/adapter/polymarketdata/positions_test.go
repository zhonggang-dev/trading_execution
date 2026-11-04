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
		query := request.URL.Query()
		if request.URL.Path != "/v2/positions" || query.Get("user") != "0xabc" ||
			query.Get("filter_type") != "TOKENS" || query.Get("filter_amount") != "0" ||
			query.Get("include_archived") != "true" || query.Get("limit") != "500" || query.Has("status") {
			t.Fatalf("request URL = %s", request.URL)
		}
		body := `{"data":[{"condition_id":"condition-1","token_id":"token-1","outcome":"Up","outcome_index":0,"current_size":5.125,"avg_price":0.4,"current_price":1,"negative_risk":false,"redeemable":true,"status":"REDEEMABLE","future_field":"ignored"}],"pagination":{"limit":500,"offset":0,"has_more":false,"next_cursor":null}}`
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
		body := `{"data":[{"condition_id":"condition-1","token_id":"token-1","outcome":"YES","current_size":-1,"avg_price":0,"current_price":0}],"pagination":{"has_more":false,"next_cursor":null}}`
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

// TestPositionClientWalksV2CursorRestatingFilters 验证 v2 cursor 分页每页都重申过滤条件，并以 next_cursor 为终止依据。
func TestPositionClientWalksV2CursorRestatingFilters(t *testing.T) {
	calls := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		query := request.URL.Query()
		if query.Get("user") != "0xabc" || query.Get("filter_amount") != "0" || query.Get("include_archived") != "true" {
			t.Fatalf("page %d dropped a filter: %s", calls, request.URL)
		}
		var body string
		switch calls {
		case 1:
			if query.Has("cursor") {
				t.Fatalf("first page sent a cursor: %s", request.URL)
			}
			// A short page with has_more=true must not end the walk.
			body = `{"data":[{"condition_id":"c-1","token_id":"t-1","outcome":"Yes","outcome_index":999,"current_size":0.02,"avg_price":0.5,"current_price":0.5}],"pagination":{"has_more":true,"next_cursor":"next-1"}}`
		case 2:
			if query.Get("cursor") != "next-1" {
				t.Fatalf("second page cursor = %q", query.Get("cursor"))
			}
			body = `{"data":null,"pagination":{"has_more":false,"next_cursor":null}}`
		default:
			t.Fatalf("unexpected page %d", calls)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}
	client, err := NewPositionClient(PositionClientParams{BaseURL: "https://data-api.example", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	positions, err := client.ListExternalPositions(context.Background(), "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(positions) != 1 || positions[0].Shares != "0.02" || positions[0].OutcomeIndex != nil {
		t.Fatalf("calls = %d, positions = %#v; want one dust row with unlabeled outcome index", calls, positions)
	}
}

// TestPositionClientFailsClosedOnMalformedV2Envelope 验证缺少或自相矛盾的分页信息不会被当作完整快照。
func TestPositionClientFailsClosedOnMalformedV2Envelope(t *testing.T) {
	for name, body := range map[string]string{
		"v1 bare array":         `[]`,
		"missing pagination":    `{"data":[]}`,
		"has_more without next": `{"data":[],"pagination":{"has_more":true,"next_cursor":null}}`,
		"next without has_more": `{"data":[],"pagination":{"has_more":false,"next_cursor":"x"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
			})}
			client, err := NewPositionClient(PositionClientParams{BaseURL: "https://data-api.example", HTTPClient: httpClient})
			if err != nil {
				t.Fatal(err)
			}
			if positions, err := client.ListExternalPositions(context.Background(), "0xabc"); err == nil || positions != nil {
				t.Fatalf("ListExternalPositions() = %#v, %v; want fail closed", positions, err)
			}
		})
	}
}

func TestValuationPricesPreserveZeroExternalQuantityWithoutChangingReconciliation(t *testing.T) {
	client, _ := NewPositionClient(PositionClientParams{BaseURL: "https://data-api.example", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":[{"condition_id":"c","token_id":"t","outcome":"No","outcome_index":1,"current_size":0,"avg_price":null,"current_price":0,"redeemable":true}],"pagination":{"has_more":false,"next_cursor":null}}`)), Request: r}, nil
	})}})
	old, e := client.ListExternalPositions(context.Background(), "0xabc")
	if e != nil || len(old) != 0 {
		t.Fatal("old reconciliation quantity semantics changed")
	}
	prices, e := client.ListValuationPrices(context.Background(), "0xabc")
	if e != nil || len(prices) != 1 || prices[0].CurrentPrice != "0" || prices[0].Shares != "0" {
		t.Fatal("zero quantity filtered official price", prices, e)
	}
}

package kalshi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestRepairEvidenceUsesClientLookupThenAuthoritativeFillScope(t *testing.T) {
	privateKey := testPrivateKey(t)
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/trade-api/v2/portfolio/orders":
			_, _ = fmt.Fprint(writer, `{"orders":[{"order_id":"venue-authoritative","client_order_id":"client-legacy","ticker":"KXTEST-YES","status":"canceled"}],"cursor":""}`)
		case "/trade-api/v2/portfolio/orders/venue-authoritative":
			_, _ = fmt.Fprint(writer, `{"order":{"order_id":"venue-authoritative","client_order_id":"client-legacy","ticker":"KXTEST-YES","outcome_side":"yes","side":"yes","action":"buy","book_side":"bid","type":"limit","yes_price_dollars":"0.5000","no_price_dollars":"0.5000","self_trade_prevention_type":"taker_at_cross","cancel_order_on_pause":true,"subaccount_number":0,"status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"20","last_update_time":"2026-09-01T07:00:00Z"}}`)
		case "/trade-api/v2/portfolio/fills":
			if got := request.URL.Query().Get("order_id"); got != "venue-authoritative" {
				t.Errorf("fills order_id=%q", got)
			}
			_, _ = fmt.Fprint(writer, `{"fills":[],"cursor":""}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(ClientParams{
		BaseURL: server.URL, APIKeyID: "key", PrivateKey: privateKey,
		HTTPClient: server.Client(), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewRepairEvidenceSource(client)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := source.Inspect(context.Background(), domain.Order{Intent: domain.OrderIntent{ClientOrderID: "client-legacy"}})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.OrderID != "venue-authoritative" || evidence.ClientOrderID != "client-legacy" || evidence.MarketID != "KXTEST-YES" ||
		evidence.OutcomeSide != "yes" || evidence.Action != "buy" || evidence.BookSide != "bid" || evidence.OrderType != "limit" ||
		evidence.OrderPrice != "0.5000" || evidence.SelfTradePolicy != "taker_at_cross" || evidence.CancelOnPause == nil || !*evidence.CancelOnPause ||
		evidence.SubaccountNumber == nil || *evidence.SubaccountNumber != 0 || evidence.Status != "canceled" ||
		evidence.FillCount != "0" || evidence.RemainingCount != "0" || evidence.InitialCount != "20" ||
		len(evidence.FillIDs) != 0 || !evidence.ObservedAt.Equal(now) ||
		evidence.OrderQuerySource != "KALSHI_ORDER_BY_CLIENT_THEN_ORDER_ID" || evidence.FillQuerySource != "KALSHI_FILLS_BY_ORDER_ID" {
		t.Fatalf("evidence=%#v", evidence)
	}
}

func TestRepairEvidenceRejectsFillForAnotherOrder(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/trade-api/v2/portfolio/orders":
			_, _ = fmt.Fprint(writer, `{"orders":[{"order_id":"venue-authoritative","client_order_id":"client-legacy","ticker":"KXTEST-YES","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"20","last_update_time":"2026-09-01T07:00:00Z"}]}`)
		case "/trade-api/v2/portfolio/orders/venue-authoritative":
			_, _ = fmt.Fprint(writer, `{"order":{"order_id":"venue-authoritative","client_order_id":"client-legacy","ticker":"KXTEST-YES","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"20","last_update_time":"2026-09-01T07:00:00Z"}}`)
		case "/trade-api/v2/portfolio/fills":
			_, _ = fmt.Fprint(writer, `{"fills":[{"fill_id":"fill-1","order_id":"other-order"}],"cursor":""}`)
		}
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, false)
	source, err := NewRepairEvidenceSource(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Inspect(context.Background(), domain.Order{Intent: domain.OrderIntent{ClientOrderID: "client-legacy"}}); err == nil {
		t.Fatal("mismatched fill identity must fail closed")
	}
}

func TestRepairEvidenceRejectsIncompleteFillsEnvelope(t *testing.T) {
	for _, testCase := range []struct {
		name, response, want string
	}{
		{name: "missing fills", response: `{}`, want: "fills collection"},
		{name: "null fills", response: `{"fills":null,"cursor":""}`, want: "fills collection"},
		{name: "missing cursor", response: `{"fills":[]}`, want: "pagination cursor"},
		{name: "null cursor", response: `{"fills":[],"cursor":null}`, want: "pagination cursor"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			privateKey := testPrivateKey(t)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				switch request.URL.Path {
				case "/trade-api/v2/portfolio/orders":
					_, _ = fmt.Fprint(writer, `{"orders":[{"order_id":"venue-authoritative","client_order_id":"client-legacy","ticker":"KXTEST-YES"}],"cursor":""}`)
				case "/trade-api/v2/portfolio/orders/venue-authoritative":
					_, _ = fmt.Fprint(writer, `{"order":{"order_id":"venue-authoritative","client_order_id":"client-legacy","ticker":"KXTEST-YES","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"20","last_update_time":"2026-09-01T07:00:00Z"}}`)
				case "/trade-api/v2/portfolio/fills":
					_, _ = fmt.Fprint(writer, testCase.response)
				}
			}))
			t.Cleanup(server.Close)
			client := testClient(t, server.URL, "key", privateKey, false)
			source, err := NewRepairEvidenceSource(client)
			if err != nil {
				t.Fatal(err)
			}
			_, err = source.Inspect(context.Background(), domain.Order{Intent: domain.OrderIntent{ClientOrderID: "client-legacy"}})
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("incomplete fills envelope error=%v, want %q", err, testCase.want)
			}
		})
	}
}

package polymarket

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestTradingClientSignsAndPostsExactV2Order 验证 Trading Client Signs And Posts Exact V 2 Order 场景下的行为。
func TestTradingClientSignsAndPostsExactV2Order(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	var posted postOrderPayload
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case "/tick-size":
			writeTestJSON(writer, map[string]any{"minimum_tick_size": 0.01})
		case "/neg-risk":
			writeTestJSON(writer, map[string]any{"neg_risk": false})
		case "/order":
			if request.Method != http.MethodPost {
				t.Fatalf("method = %s", request.Method)
			}
			if request.Header.Get("POLY_ADDRESS") == "" || request.Header.Get("POLY_SIGNATURE") == "" ||
				request.Header.Get("POLY_API_KEY") != "api-key" || request.Header.Get("POLY_PASSPHRASE") != "passphrase" {
				t.Fatalf("missing L2 headers: %#v", request.Header)
			}
			if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
				t.Fatal(err)
			}
			writeTestJSON(writer, map[string]any{"success": true, "orderID": "0xvenue", "status": "live"})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	venueOrder, err := client.Place(context.Background(), adapterOrder())
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if venueOrder.State != port.VenueOrderLive || venueOrder.ID != "0xvenue" {
		t.Fatalf("venue order = %#v", venueOrder)
	}
	if posted.Order.MakerAmount != "5000000" || posted.Order.TakerAmount != "10000000" ||
		posted.Order.TokenID != adapterOrder().Intent.TokenID || posted.Order.Side != "BUY" ||
		posted.Order.SignatureType != 0 || len(posted.Order.Signature) != 132 || posted.Order.Timestamp != "1787040000000" {
		t.Fatalf("posted order = %#v", posted.Order)
	}
}

// TestCancelRaceReturnsFillObservedAfterCancel 验证 Cancel Race Returns Fill Observed After Cancel 场景下的行为。
func TestCancelRaceReturnsFillObservedAfterCancel(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodDelete && request.URL.Path == "/order":
			writeTestJSON(writer, map[string]any{"canceled": []string{"0xvenue"}, "not_canceled": map[string]string{}})
		case request.Method == http.MethodGet && request.URL.Path == "/data/order/0xvenue":
			writeTestJSON(writer, map[string]any{
				"id": "0xvenue", "status": "MATCHED", "original_size": "10", "size_matched": "10",
				"associate_trades": []string{"trade-filled"},
			})
		case request.Method == http.MethodGet && request.URL.Path == "/data/trades":
			writeTestJSON(writer, []map[string]any{{
				"id": "trade-filled", "taker_order_id": "0xvenue", "size": "10", "price": "0.5",
				"status": "CONFIRMED",
			}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	order := adapterOrder()
	order.VenueOrderID = "0xvenue"
	venueOrder, err := client.Cancel(context.Background(), order)
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if venueOrder.State != port.VenueOrderFilled || venueOrder.FilledSize != "10" {
		t.Fatalf("Cancel() = %#v, want fill to win cancel race", venueOrder)
	}
}

// TestMatchedPlacementReadsAuthoritativePartialFill 验证 Matched Placement Reads Authoritative Partial Fill 场景下的行为。
func TestMatchedPlacementReadsAuthoritativePartialFill(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case "/tick-size":
			writeTestJSON(writer, map[string]any{"minimum_tick_size": "0.01"})
		case "/neg-risk":
			writeTestJSON(writer, map[string]any{"neg_risk": false})
		case "/order":
			writeTestJSON(writer, map[string]any{
				"success": true, "orderID": "0xpartial", "status": "matched", "tradeIDs": []string{"trade-1"},
			})
		case "/data/order/0xpartial":
			writeTestJSON(writer, map[string]any{
				"id": "0xpartial", "status": "MATCHED", "original_size": "10", "size_matched": "4",
				"associate_trades": []string{"trade-1", "trade-2"},
			})
		case "/data/trades":
			tradeID := request.URL.Query().Get("id")
			price := "0.49"
			if tradeID == "trade-2" {
				price = "0.51"
			}
			writeTestJSON(writer, map[string]any{
				"data": []map[string]any{{
					"id": tradeID, "taker_order_id": "0xpartial", "size": "2", "price": price,
					"status": "CONFIRMED",
				}},
				"next_cursor": "LTE=",
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	venueOrder, err := client.Place(context.Background(), adapterOrder())
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if venueOrder.State != port.VenueOrderPartiallyFilled || venueOrder.FilledSize != "4" || venueOrder.AverageFillPrice != "0.5" {
		t.Fatalf("Place() = %#v, want authoritative partial fill", venueOrder)
	}
	if len(venueOrder.TradeIDs) != 2 {
		t.Fatalf("trade ids = %#v, want merged and deduplicated", venueOrder.TradeIDs)
	}
}

// TestAcceptedPlacementWithoutOrderIDIsAmbiguous 验证 Accepted Placement Without Order ID Is Ambiguous 场景下的行为。
func TestAcceptedPlacementWithoutOrderIDIsAmbiguous(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case "/tick-size":
			writeTestJSON(writer, map[string]any{"minimum_tick_size": "0.01"})
		case "/neg-risk":
			writeTestJSON(writer, map[string]any{"neg_risk": false})
		case "/order":
			writeTestJSON(writer, map[string]any{"ok": true, "status": "live"})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	_, err := client.Place(context.Background(), adapterOrder())
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Kind != port.VenueErrorAmbiguous || venueError.VenueOrderID == "" {
		t.Fatalf("Place() error = %#v, want ambiguous error with expected order id", err)
	}
}

// TestListTradesAcceptsBareWireArray 验证 List Trades Accepts Bare Wire Array 场景下的行为。
func TestListTradesAcceptsBareWireArray(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/data/trades" {
			http.NotFound(writer, request)
			return
		}
		writeTestJSON(writer, []map[string]any{{
			"id": "trade-1", "taker_order_id": "0xvenue", "market": "condition-1",
			"asset_id": "token-1", "side": "BUY", "size": "4", "price": "0.5",
			"status": "MATCHED", "transaction_hash": "0xtx",
		}})
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	trades, err := client.ListTrades(context.Background(), "account-1", TradeFilter{TokenID: "token-1"})
	if err != nil {
		t.Fatalf("ListTrades() error = %v", err)
	}
	if len(trades) != 1 || trades[0].ID != "trade-1" || trades[0].TransactionHash != "0xtx" {
		t.Fatalf("trades = %#v", trades)
	}
}

// newTestTradingClient 创建测试所需的模拟对象。
func newTestTradingClient(t *testing.T, baseURL string, now time.Time) *TradingClient {
	t.Helper()
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewStaticCredentialProvider([]TradingAccount{{
		ExecutionAccountID: "account-1",
		FunderAddress:      signer.Address(),
		SignatureType:      SignatureTypeEOA,
		API: APICredentials{
			Key:        "api-key",
			Secret:     base64.URLEncoding.EncodeToString([]byte("test-secret")),
			Passphrase: "passphrase",
		},
		Signer: signer,
	}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewTradingClient(TradingClientParams{
		BaseURL:           baseURL,
		Credentials:       provider,
		RequestTimeout:    2 * time.Second,
		RequestsPerSecond: 100,
		Burst:             20,
		Now:               func() time.Time { return now },
		Random:            strings.NewReader(strings.Repeat("x", 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// adapterOrder 实现当前测试场景所需的辅助行为。
func adapterOrder() domain.Order {
	return domain.Order{
		ID:     "order-1",
		Intent: adapterIntent(),
		MarketValidation: &domain.MarketValidation{
			Mode:         "LIVE_CHECK",
			TickSize:     "0.01",
			MinOrderSize: "5",
			NegRisk:      false,
		},
		Status:     domain.OrderStatusSubmitting,
		FilledSize: "0",
	}
}

// writeTestJSON 实现当前测试场景所需的辅助行为。
func writeTestJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}

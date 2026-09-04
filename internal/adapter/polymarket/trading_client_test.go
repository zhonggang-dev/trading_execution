package polymarket

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
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
			writeTestJSON(writer, map[string]any{
				"success": true, "orderID": signedOrderIDForTest(t, posted.Order, false), "status": "live",
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
	if venueOrder.State != port.VenueOrderLive || venueOrder.ID != signedOrderIDForTest(t, posted.Order, false) {
		t.Fatalf("venue order = %#v", venueOrder)
	}
	if posted.Order.MakerAmount != "5000000" || posted.Order.TakerAmount != "10000000" ||
		posted.Order.TokenID != adapterOrder().Intent.TokenID || posted.Order.Side != "BUY" ||
		posted.Order.SignatureType != 0 || len(posted.Order.Signature) != 132 || posted.Order.Timestamp != "1787040000000" {
		t.Fatalf("posted order = %#v", posted.Order)
	}
}

func TestPreparePlaceDoesNotPostAndPlacePreparedRequiresPersistedExpectedHash(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	postCalls := 0
	var posted postOrderPayload
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case "/tick-size":
			writeTestJSON(writer, map[string]any{"minimum_tick_size": "0.01"})
		case "/neg-risk":
			writeTestJSON(writer, map[string]any{"neg_risk": false})
		case "/order":
			postCalls++
			if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
				t.Fatal(err)
			}
			writeTestJSON(writer, map[string]any{
				"success": true, "orderID": signedOrderIDForTest(t, posted.Order, false), "status": "live",
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	order := adapterOrder()
	prepared, err := client.PreparePlace(context.Background(), order)
	if err != nil {
		t.Fatalf("PreparePlace() error = %v", err)
	}
	if postCalls != 0 || prepared.ExpectedVenueOrderID() == "" {
		t.Fatalf("prepare post calls/hash = %d/%q", postCalls, prepared.ExpectedVenueOrderID())
	}
	if _, err := client.PlacePrepared(context.Background(), order, prepared); err == nil {
		t.Fatal("PlacePrepared() without persisted expected hash error = nil")
	}
	if postCalls != 0 {
		t.Fatalf("invalid prepared placement emitted %d POST(s)", postCalls)
	}
	order.VenueOrderID = prepared.ExpectedVenueOrderID()
	venueOrder, err := client.PlacePrepared(context.Background(), order, prepared)
	if err != nil {
		t.Fatalf("PlacePrepared() error = %v", err)
	}
	if postCalls != 1 || venueOrder.ID != prepared.ExpectedVenueOrderID() ||
		venueOrder.ID != signedOrderIDForTest(t, posted.Order, false) {
		t.Fatalf("post calls/order = %d/%#v", postCalls, venueOrder)
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
				"id": "0xvenue", "status": "MATCHED", "original_size": "10000000", "size_matched": "10000000",
				"associate_trades": []string{"trade-filled"},
			})
		case request.Method == http.MethodGet && request.URL.Path == "/data/trades":
			writeTestJSON(writer, []map[string]any{{
				"id": "trade-filled", "taker_order_id": "0xvenue", "size": "10", "price": "0.5",
				"status": "CONFIRMED", "trader_side": "TAKER",
				"maker_address": request.URL.Query().Get("maker_address"),
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
	var placedOrderID string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case request.URL.Path == "/tick-size":
			writeTestJSON(writer, map[string]any{"minimum_tick_size": "0.01"})
		case request.URL.Path == "/neg-risk":
			writeTestJSON(writer, map[string]any{"neg_risk": false})
		case request.URL.Path == "/order":
			var posted postOrderPayload
			if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
				t.Fatal(err)
			}
			placedOrderID = signedOrderIDForTest(t, posted.Order, false)
			writeTestJSON(writer, map[string]any{
				"success": true, "orderID": placedOrderID, "status": "matched", "tradeIDs": []string{"trade-1"},
			})
		case placedOrderID != "" && request.URL.Path == "/data/order/"+placedOrderID:
			writeTestJSON(writer, map[string]any{
				"id": placedOrderID, "status": "MATCHED", "original_size": "10000000", "size_matched": "4000000",
				"associate_trades": []string{"trade-1", "trade-2"},
			})
		case request.URL.Path == "/data/trades":
			tradeID := request.URL.Query().Get("id")
			price := "0.49"
			if tradeID == "trade-2" {
				price = "0.51"
			}
			writeTestJSON(writer, map[string]any{
				"data": []map[string]any{{
					"id": tradeID, "taker_order_id": placedOrderID, "size": "2", "price": price,
					"status": "CONFIRMED", "trader_side": "TAKER",
					"maker_address": request.URL.Query().Get("maker_address"),
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

func TestMatchedPlacementPreservesObservedFillWhileTradeDetailsArePending(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	var placedOrderID string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case request.URL.Path == "/tick-size":
			writeTestJSON(writer, map[string]any{"minimum_tick_size": "0.01"})
		case request.URL.Path == "/neg-risk":
			writeTestJSON(writer, map[string]any{"neg_risk": false})
		case request.URL.Path == "/order":
			var posted postOrderPayload
			if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
				t.Fatal(err)
			}
			placedOrderID = signedOrderIDForTest(t, posted.Order, false)
			writeTestJSON(writer, map[string]any{
				"success": true, "orderID": placedOrderID, "status": "matched",
				"tradeIDs": []string{"trade-post", "trade-shared"},
				"tradeIds": []string{"trade-alt", "trade-shared"},
			})
		case placedOrderID != "" && request.URL.Path == "/data/order/"+placedOrderID:
			writeTestJSON(writer, map[string]any{
				"id": placedOrderID, "status": "MATCHED", "original_size": "10000000", "size_matched": "10000000",
				"associate_trades": []string{"trade-shared", "trade-pending"},
			})
		case request.URL.Path == "/data/trades":
			tradeID := request.URL.Query().Get("id")
			writeTestJSON(writer, map[string]any{
				"data": []map[string]any{{
					"id": tradeID, "taker_order_id": placedOrderID, "size": "10", "price": "0.5",
					"status": "MATCHED", "trader_side": "TAKER",
					"maker_address": request.URL.Query().Get("maker_address"),
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
	if venueOrder.State != port.VenueOrderFilled || venueOrder.FilledSize != "10" || !venueOrder.AverageFillPrice.IsEmpty() {
		t.Fatalf("Place() = %#v, want observed fill without a provisional average price", venueOrder)
	}
	wantTradeIDs := []string{"trade-shared", "trade-pending", "trade-post", "trade-alt"}
	if strings.Join(venueOrder.TradeIDs, ",") != strings.Join(wantTradeIDs, ",") {
		t.Fatalf("trade ids = %#v, want %#v", venueOrder.TradeIDs, wantTradeIDs)
	}
}

func TestAcceptedPlacementWithMismatchedOrderIDIsAmbiguous(t *testing.T) {
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
				"success": true,
				"orderID": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
				"status":  "live",
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	_, err := client.Place(context.Background(), adapterOrder())
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Kind != port.VenueErrorAmbiguous ||
		venueError.Code != "CLOB_ORDER_ID_MISMATCH" || venueError.VenueOrderID == "" {
		t.Fatalf("Place() error = %#v, want ambiguous id mismatch with expected order id", err)
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
			"asset_id": "token-1", "side": "BUY", "size": "100", "price": "0.5",
			"status": "MATCHED", "transaction_hash": "0xtx", "trader_side": "TAKER",
			"maker_address": request.URL.Query().Get("maker_address"),
			"maker_orders":  []map[string]any{{"order_id": "0xmaker", "matched_amount": "2.5", "price": "0.5"}},
		}})
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	trades, err := client.ListTrades(context.Background(), "account-1", TradeFilter{TokenID: "token-1"})
	if err != nil {
		t.Fatalf("ListTrades() error = %v", err)
	}
	if len(trades) != 1 || trades[0].ID != "trade-1" || !trades[0].Size.Equal("100") ||
		len(trades[0].MakerOrders) != 1 || !trades[0].MakerOrders[0].MatchedAmount.Equal("2.5") ||
		trades[0].TransactionHash != "0xtx" || trades[0].MakerAddress == "" {
		t.Fatalf("trades = %#v", trades)
	}
}

func TestListTradesBindsProxyFunderAndRejectsSignerFilter(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	const funderAddress = "0x1111111111111111111111111111111111111111"
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		if got := request.URL.Query().Get("maker_address"); got != funderAddress {
			t.Fatalf("maker_address = %q, want proxy funder %q", got, funderAddress)
		}
		writeTestJSON(writer, map[string]any{"data": []map[string]any{{
			"id": "trade-1", "taker_order_id": "order-1", "size": "1", "price": "0.5",
			"trader_side": "TAKER", "maker_address": funderAddress,
		}}, "next_cursor": "LTE="})
	}))
	defer server.Close()

	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewStaticCredentialProvider([]TradingAccount{{
		ExecutionAccountID: "proxy-account", FunderAddress: funderAddress,
		SignatureType: SignatureTypePolyProxy, Signer: signer,
		API: APICredentials{Key: "api-key", Secret: base64.URLEncoding.EncodeToString([]byte("test-secret")), Passphrase: "passphrase"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewTradingClient(TradingClientParams{
		BaseURL: server.URL, Credentials: provider, RequestTimeout: 2 * time.Second,
		RequestsPerSecond: 100, Burst: 20, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTrades(context.Background(), "proxy-account", TradeFilter{}); err != nil {
		t.Fatalf("ListTrades() error = %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("request count = %d, want 1", requestCount)
	}

	_, err = client.ListTrades(context.Background(), "proxy-account", TradeFilter{MakerAddress: signer.Address()})
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Kind != port.VenueErrorInvalid ||
		venueError.Code != "CLOB_TRADE_ACCOUNT_FILTER_MISMATCH" {
		t.Fatalf("cross-account ListTrades() error = %#v", err)
	}
	if requestCount != 1 {
		t.Fatalf("rejected filter emitted %d request(s), want 1 total", requestCount)
	}
}

func TestListTradesRejectsForeignTradeComponent(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writeTestJSON(writer, map[string]any{"data": []map[string]any{{
			"id": "foreign-trade", "taker_order_id": "foreign-order", "size": "1", "price": "0.5",
			"trader_side": "TAKER", "maker_address": "0x2222222222222222222222222222222222222222",
		}}, "next_cursor": "LTE="})
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	_, err := client.ListTrades(context.Background(), "account-1", TradeFilter{})
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Kind != port.VenueErrorUnavailable ||
		venueError.Code != "CLOB_TRADE_OWNERSHIP_MISMATCH" {
		t.Fatalf("ListTrades() error = %#v, want ownership mismatch", err)
	}
}

func TestAccountTradeOrderIDsHonorsTakerMakerAndLegacyOwnership(t *testing.T) {
	const funderAddress = "0x1111111111111111111111111111111111111111"
	tests := []struct {
		name    string
		trade   Trade
		wantIDs []string
		wantErr bool
	}{
		{name: "taker", trade: Trade{ID: "t1", TraderSide: "TAKER", MakerAddress: funderAddress, TakerOrderID: "take-1"}, wantIDs: []string{"take-1"}},
		{name: "maker", trade: Trade{ID: "t2", TraderSide: "MAKER", MakerOrders: []MakerOrder{{OrderID: "make-1", TokenID: "maker-token", MakerAddress: funderAddress}}}, wantIDs: []string{"make-1"}},
		{name: "legacy taker", trade: Trade{ID: "t3", MakerAddress: funderAddress, TakerOrderID: "take-legacy"}, wantIDs: []string{"take-legacy"}},
		{name: "legacy maker", trade: Trade{ID: "t4", MakerOrders: []MakerOrder{{OrderID: "make-legacy", TokenID: "maker-token", MakerAddress: funderAddress}}}, wantIDs: []string{"make-legacy"}},
		{name: "taker remains top level", trade: Trade{ID: "t5", TraderSide: "TAKER", MakerAddress: funderAddress, TakerOrderID: "take-self", MakerOrders: []MakerOrder{{OrderID: "make-self", TokenID: "maker-token", MakerAddress: funderAddress}}}, wantIDs: []string{"take-self"}},
		{name: "foreign taker", trade: Trade{ID: "t6", TraderSide: "TAKER", MakerAddress: "0x2222222222222222222222222222222222222222", TakerOrderID: "foreign"}, wantErr: true},
		{name: "foreign maker", trade: Trade{ID: "t7", TraderSide: "MAKER", MakerOrders: []MakerOrder{{OrderID: "foreign", MakerAddress: "0x2222222222222222222222222222222222222222"}}}, wantErr: true},
		{name: "unknown side", trade: Trade{ID: "t8", TraderSide: "BROKER", MakerAddress: funderAddress, TakerOrderID: "unknown"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := accountTradeOrderIDs(test.trade, funderAddress)
			if test.wantErr {
				if err == nil {
					t.Fatalf("accountTradeOrderIDs() = %#v, want error", got)
				}
				return
			}
			if err != nil || strings.Join(got, ",") != strings.Join(test.wantIDs, ",") {
				t.Fatalf("accountTradeOrderIDs() = %#v, %v; want %#v", got, err, test.wantIDs)
			}
		})
	}
}

func TestListReconciliationTradesPreservesOwnedMakerComponentTokens(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	const funderAddress = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/data/trades" {
			http.NotFound(writer, request)
			return
		}
		if got := request.URL.Query().Get("maker_address"); got != funderAddress {
			t.Fatalf("maker_address = %q, want %q", got, funderAddress)
		}
		writeTestJSON(writer, map[string]any{
			"data": []map[string]any{{
				"id": "trade-maker-components", "taker_order_id": "foreign-taker",
				"market": "condition-1", "asset_id": "top-level-taker-token",
				"size": "4", "price": "0.5", "status": "CONFIRMED",
				"last_update": "2026-08-18T07:59:59Z", "trader_side": "MAKER",
				"maker_address": "0x2222222222222222222222222222222222222222",
				"maker_orders": []map[string]any{
					{"order_id": "owned-maker-a", "asset_id": "owned-token-a", "maker_address": funderAddress, "matched_amount": "1", "price": "0.4"},
					{"order_id": "foreign-maker", "asset_id": "foreign-token", "maker_address": "0x3333333333333333333333333333333333333333", "matched_amount": "1", "price": "0.5"},
					{"order_id": "owned-maker-b", "asset_id": "owned-token-b", "maker_address": funderAddress, "matched_amount": "2", "price": "0.6"},
				},
			}},
			"next_cursor": "LTE=",
		})
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	trades, err := client.ListReconciliationTrades(context.Background(), "account-1", time.Time{})
	if err != nil {
		t.Fatalf("ListReconciliationTrades() error = %v", err)
	}
	if len(trades) != 2 {
		t.Fatalf("reconciliation trades = %#v, want two owned maker components", trades)
	}
	for index, expected := range []struct {
		orderID string
		tokenID string
	}{{"owned-maker-a", "owned-token-a"}, {"owned-maker-b", "owned-token-b"}} {
		trade := trades[index]
		if trade.VenueTradeID != "trade-maker-components" || trade.ConditionID != "condition-1" ||
			trade.TokenID != expected.tokenID || len(trade.OrderIDs) != 1 || trade.OrderIDs[0] != expected.orderID {
			t.Fatalf("component %d = %#v, want order/token %s/%s", index, trade, expected.orderID, expected.tokenID)
		}
		if trade.TokenID == "top-level-taker-token" {
			t.Fatalf("maker component %d inherited the top-level taker token", index)
		}
	}
}

func TestAccountTradeComponentsUsesTopLevelIdentityForTaker(t *testing.T) {
	const funderAddress = "0x1111111111111111111111111111111111111111"
	components, err := accountTradeComponents(Trade{
		ID: "taker-trade", TraderSide: "TAKER", MakerAddress: funderAddress,
		TakerOrderID: "owned-taker", TokenID: "top-level-taker-token",
		MakerOrders: []MakerOrder{{
			OrderID: "owned-self-maker", TokenID: "nested-maker-token", MakerAddress: funderAddress,
		}},
	}, funderAddress)
	if err != nil {
		t.Fatalf("accountTradeComponents() error = %v", err)
	}
	if len(components) != 1 || components[0].OrderID != "owned-taker" || components[0].TokenID != "top-level-taker-token" {
		t.Fatalf("taker components = %#v, want only exact top-level taker identity", components)
	}
}

func TestTradeWireDefaultsEmptyDeprecatedFeeRateMetadataToZero(t *testing.T) {
	var trade Trade
	err := json.Unmarshal([]byte(`{
		"id":"trade-1","size":"1.25","price":"0.5","fee_rate_bps":"",
		"maker_orders":[{"order_id":"order-1","matched_amount":"0.75","price":"0.5","fee_rate_bps":""}]
	}`), &trade)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !trade.Size.Equal("1.25") || !trade.FeeRateBPS.Equal("0") || len(trade.MakerOrders) != 1 ||
		!trade.MakerOrders[0].MatchedAmount.Equal("0.75") || !trade.MakerOrders[0].FeeRateBPS.Equal("0") {
		t.Fatalf("trade = %#v", trade)
	}
}

func TestTradeOwnershipFieldKeepsLegacyFillDigestShape(t *testing.T) {
	var trade Trade
	if err := json.Unmarshal([]byte(`{
		"id":"trade-1","taker_order_id":"order-1","size":"1","price":"0.5",
		"maker_address":"0x1111111111111111111111111111111111111111","trader_side":"TAKER"
	}`), &trade); err != nil {
		t.Fatal(err)
	}
	if trade.MakerAddress == "" {
		t.Fatal("maker_address was not decoded for ownership validation")
	}
	raw, err := json.Marshal(trade)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "maker_address") {
		t.Fatalf("digest payload unexpectedly changed shape: %s", raw)
	}
}

func TestTradeWireQuantityRequiresDecimalString(t *testing.T) {
	for _, size := range []string{`1000000`, `null`, `""`} {
		var trade Trade
		if err := json.Unmarshal([]byte(`{"size":`+size+`}`), &trade); err == nil {
			t.Fatalf("invalid trade size %s was accepted", size)
		}
	}
}

func TestRawOrderAcceptsHumanDecimalMatchedSharesForPriceImprovedBuy(t *testing.T) {
	var raw rawOrder
	if err := json.Unmarshal([]byte(`{
		"id":"0xvenue","status":"MATCHED","original_size":"10200000",
		"size_matched":"30.147057","price":"0.34"
	}`), &raw); err != nil {
		t.Fatal(err)
	}
	if !raw.OriginalSize.Equal("10.2") || !raw.SizeMatched.Equal("30.147057") {
		t.Fatalf("order quantities = %s/%s", raw.OriginalSize, raw.SizeMatched)
	}
	order := adapterOrder()
	order.Intent.Venue = "polymarket"
	order.Intent.Side = domain.SideBuy
	order.Intent.Size = "30"
	observed, err := normalizeRawOrder(raw, order, time.Date(2026, 9, 1, 11, 1, 6, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if observed.State != port.VenueOrderFilled || !observed.FilledSize.Equal("30.147057") {
		t.Fatalf("price-improved order = %#v", observed)
	}
}

func TestNormalizeRawOrderRejectsOverfillOutsidePolymarketBuy(t *testing.T) {
	order := adapterOrder()
	order.Intent.Venue = "kalshi"
	raw := rawOrder{ID: "venue-order", Status: "MATCHED", OriginalSize: "30", SizeMatched: "30.147057"}
	if _, err := normalizeRawOrder(raw, order, time.Now().UTC()); err == nil {
		t.Fatal("non-Polymarket BUY overfill was accepted")
	}
}

func TestListOrderFillsFailsClosedWithoutFinalizedFeeEvidence(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	order := adapterOrder()
	order.VenueOrderID = "0xvenue"
	order.Intent.ConditionID = "condition-1"
	tokenID := order.Intent.TokenID
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/data/trades":
			writeTestJSON(writer, map[string]any{"data": []map[string]any{{
				"id": "trade-1", "taker_order_id": "0xvenue", "market": "condition-1",
				"asset_id": tokenID, "side": "BUY", "size": "10", "price": "0.5",
				"status": "CONFIRMED", "fee_rate_bps": "0", "transaction_hash": "0xtx",
				"match_time": "2026-08-18T07:59:58Z", "last_update": "2026-08-18T07:59:59Z",
				"trader_side": "TAKER", "maker_address": request.URL.Query().Get("maker_address"),
			}}, "next_cursor": "LTE="})
		case "/clob-markets/condition-1":
			writeTestJSON(writer, map[string]any{
				"c": "condition-1", "t": []map[string]any{{"t": tokenID}},
				"fd": map[string]any{"r": 0.25, "e": 2, "to": true},
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	if _, err := client.ListOrderFills(context.Background(), order); err == nil ||
		!strings.Contains(err.Error(), "fee evidence source is not configured") ||
		!strings.Contains(err.Error(), "CLOB trade trade-1 fee evidence") {
		t.Fatalf("ListOrderFills() error = %v, want fail-closed evidence error", err)
	}
	schedule := client.feeSchedules[order.Intent.ConditionID+"\x00"+order.Intent.TokenID].schedule
	if !schedule.Rate.Equal("0.25") || !schedule.Exponent.Equal("2") {
		t.Fatalf("official fee schedule = %#v", schedule)
	}
}

func TestListOrderFillsFailsClosedWhileOwnedTradeIsPending(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	for _, status := range []string{"MATCHED", "MINED", "RETRYING"} {
		t.Run(status, func(t *testing.T) {
			order := adapterOrder()
			order.VenueOrderID = "0xvenue"
			order.Intent.ConditionID = "condition-1"
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/data/trades" {
					t.Fatalf("unexpected path %s", request.URL.Path)
				}
				writeTestJSON(writer, map[string]any{"data": []map[string]any{{
					"id": "trade-pending", "taker_order_id": order.VenueOrderID, "market": order.Intent.ConditionID,
					"asset_id": order.Intent.TokenID, "side": "BUY", "size": "10", "price": "0.5",
					"status": status, "fee_rate_bps": "0", "match_time": "2026-08-18T07:59:58Z",
					"last_update": "2026-08-18T07:59:59Z", "trader_side": "TAKER",
					"maker_address": request.URL.Query().Get("maker_address"),
				}}, "next_cursor": "LTE="})
			}))
			defer server.Close()

			client := newTestTradingClient(t, server.URL, now)
			fills, err := client.ListOrderFills(context.Background(), order)
			var venueErr *port.VenueError
			if !errors.As(err, &venueErr) || venueErr.Code != "CLOB_FILL_DETAILS_UNAVAILABLE" {
				t.Fatalf("ListOrderFills() fills/error = %#v/%v, want explicit pending evidence error", fills, err)
			}
			if len(fills) != 0 {
				t.Fatalf("ListOrderFills() fills = %#v, want no provisional ledger fills", fills)
			}
		})
	}
}

func TestTradingClientRejectsAuthenticatedRedirect(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	redirectReached := false
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		redirectReached = true
		if request.Header.Get("POLY_SIGNATURE") != "" || request.Header.Get("POLY_API_KEY") != "" {
			t.Error("authenticated CLOB headers leaked to redirect target")
		}
		writeTestJSON(writer, map[string]any{"data": []any{}, "next_cursor": "LTE="})
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+"/capture", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	client := newTestTradingClient(t, origin.URL, now)
	if _, err := client.ListTrades(context.Background(), "account-1", TradeFilter{}); err == nil {
		t.Fatal("authenticated redirect was accepted")
	}
	if redirectReached {
		t.Fatal("redirect target was reached")
	}
}

func TestGetBalanceAllowanceSignsPathAndSendsRequiredQuery(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/balance-allowance" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Query().Get("asset_type") != "COLLATERAL" || request.URL.Query().Get("signature_type") != "0" {
			t.Fatalf("query = %s", request.URL.RawQuery)
		}
		expected, err := hmacSignature(base64.URLEncoding.EncodeToString([]byte("test-secret")), now.Unix(), http.MethodGet, "/balance-allowance", nil)
		if err != nil {
			t.Fatal(err)
		}
		if request.Header.Get("POLY_SIGNATURE") != expected {
			t.Fatalf("POLY_SIGNATURE = %q, want %q", request.Header.Get("POLY_SIGNATURE"), expected)
		}
		writeTestJSON(writer, map[string]any{
			"balance": "1000000",
			"allowances": map[string]string{
				StandardExchangeV2Address: "1000000",
				NegRiskExchangeV2Address:  "1000000",
			},
		})
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	result, err := client.GetBalanceAllowance(context.Background(), "account-1", BalanceAssetCollateral, "")
	if err != nil {
		t.Fatalf("GetBalanceAllowance() error = %v", err)
	}
	if !result.Positive() || !result.AllAllowancesPositive() ||
		!result.RequiredAllowancesPositive(StandardExchangeV2Address, NegRiskExchangeV2Address) || len(result.Allowances) != 2 {
		t.Fatalf("GetBalanceAllowance() = %#v", result)
	}
}

func TestUpdateBalanceAllowanceSignsRefreshPathAndSendsRequiredQuery(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/balance-allowance/update" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Query().Get("asset_type") != "COLLATERAL" || request.URL.Query().Get("signature_type") != "0" ||
			request.URL.Query().Has("token_id") {
			t.Fatalf("query = %s", request.URL.RawQuery)
		}
		expected, err := hmacSignature(base64.URLEncoding.EncodeToString([]byte("test-secret")), now.Unix(), http.MethodGet, "/balance-allowance/update", nil)
		if err != nil {
			t.Fatal(err)
		}
		if request.Header.Get("POLY_SIGNATURE") != expected {
			t.Fatalf("POLY_SIGNATURE = %q, want %q", request.Header.Get("POLY_SIGNATURE"), expected)
		}
		writeTestJSON(writer, map[string]any{"updated": true})
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, now)
	if err := client.UpdateBalanceAllowance(context.Background(), "account-1", BalanceAssetCollateral, ""); err != nil {
		t.Fatalf("UpdateBalanceAllowance() error = %v", err)
	}
}

func TestUpdateBalanceAllowanceRequiresConditionalTokenIDBeforeRequest(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()

	client := newTestTradingClient(t, server.URL, time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC))
	if err := client.UpdateBalanceAllowance(context.Background(), "account-1", BalanceAssetConditional, " "); err == nil {
		t.Fatal("UpdateBalanceAllowance() error = nil, want missing token id rejection")
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want 0", requests)
	}
}

func TestRequiredAllowancesRejectsIncompleteContractSet(t *testing.T) {
	allowance := BalanceAllowance{Allowances: map[string]string{
		strings.ToLower(StandardExchangeV2Address): "1",
	}}
	if allowance.RequiredAllowancesPositive(StandardExchangeV2Address, NegRiskExchangeV2Address) {
		t.Fatal("RequiredAllowancesPositive() = true for a missing neg-risk approval")
	}
}

func TestGetBalanceAllowanceRejectsMalformedQuantities(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTestJSON(writer, map[string]any{"balance": "1.5", "allowances": map[string]string{}})
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	if _, err := client.GetBalanceAllowance(context.Background(), "account-1", BalanceAssetCollateral, ""); err == nil {
		t.Fatal("GetBalanceAllowance() error = nil, want malformed quantity rejection")
	}
}

func TestClosedOnlyUsesAuthenticatedV2Gate(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/auth/ban-status/closed-only" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("POLY_API_KEY") != "api-key" || request.Header.Get("POLY_SIGNATURE") == "" {
			t.Fatalf("missing L2 authentication headers")
		}
		writeTestJSON(writer, map[string]any{"closed_only": false})
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	closedOnly, err := client.ClosedOnly(context.Background(), "account-1")
	if err != nil {
		t.Fatalf("ClosedOnly() error = %v", err)
	}
	if closedOnly {
		t.Fatal("ClosedOnly() = true, want false")
	}
}

func TestHeartbeatUsesV2EndpointAndReusesServerID(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	var received string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/heartbeats" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		var body struct {
			HeartbeatID string `json:"heartbeat_id"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		received = body.HeartbeatID
		writeTestJSON(writer, map[string]any{"heartbeat_id": "heartbeat-next"})
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	next, err := client.Heartbeat(context.Background(), "account-1", "heartbeat-current")
	if err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	if received != "heartbeat-current" || next != "heartbeat-next" {
		t.Fatalf("received/next = %q/%q", received, next)
	}
}

func TestProbeProtocolSynchronizesAuthenticatedTimestamp(t *testing.T) {
	localNow := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	serverNow := localNow.Add(2 * time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/ok":
			_, _ = writer.Write([]byte("OK"))
		case "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case "/time":
			_, _ = writer.Write([]byte(strconv.FormatInt(serverNow.Unix(), 10)))
		case "/data/orders":
			if request.Header.Get("POLY_TIMESTAMP") != strconv.FormatInt(serverNow.Unix(), 10) {
				t.Fatalf("POLY_TIMESTAMP = %q, want server-synchronized time", request.Header.Get("POLY_TIMESTAMP"))
			}
			writeTestJSON(writer, []any{})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, localNow)
	probe, err := client.ProbeProtocol(context.Background(), 3*time.Second)
	if err != nil {
		t.Fatalf("ProbeProtocol() error = %v", err)
	}
	if probe.Version != 2 || !probe.ServerTime.Equal(serverNow) || probe.ClockSkew != 2*time.Second {
		t.Fatalf("ProbeProtocol() = %#v", probe)
	}
	if _, err := client.ListOpenOrders(context.Background(), "account-1", OpenOrderFilter{}); err != nil {
		t.Fatalf("ListOpenOrders() error = %v", err)
	}
}

func TestProbeProtocolRejectsUnsafeClockSkew(t *testing.T) {
	localNow := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/ok":
			_, _ = writer.Write([]byte("OK"))
		case "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case "/time":
			_, _ = writer.Write([]byte(strconv.FormatInt(localNow.Add(10*time.Second).Unix(), 10)))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, localNow)
	if _, err := client.ProbeProtocol(context.Background(), 3*time.Second); err == nil {
		t.Fatal("ProbeProtocol() error = nil, want unsafe clock skew rejection")
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

func signedOrderIDForTest(t *testing.T, order signedOrderV2, negRisk bool) string {
	t.Helper()
	salt, ok := new(big.Int).SetString(order.Salt.String(), 10)
	if !ok {
		t.Fatalf("invalid signed order salt %q", order.Salt)
	}
	timestamp, err := strconv.ParseInt(order.Timestamp, 10, 64)
	if err != nil {
		t.Fatalf("invalid signed order timestamp %q: %v", order.Timestamp, err)
	}
	side := uint8(0)
	if order.Side == "SELL" {
		side = 1
	}
	exchange := polygonExchangeV2
	if negRisk {
		exchange = polygonNegRiskExchangeV2
	}
	digest, err := orderDigest(orderDigestInput{
		ChainID: polygonChainID, Exchange: exchange, Salt: salt,
		Maker: order.Maker, Signer: order.Signer, TokenID: order.TokenID,
		MakerAmount: order.MakerAmount, TakerAmount: order.TakerAmount,
		Side: side, SignatureType: order.SignatureType, Timestamp: timestamp,
		Metadata: order.Metadata, Builder: order.Builder,
	})
	if err != nil {
		t.Fatalf("derive signed order id: %v", err)
	}
	return "0x" + hex.EncodeToString(digest)
}

// TestTradingClientPostsEmulatedIOCAsShareDenominatedGTC 验证 IOC BUY 以 GTC 提交、数量为可成交量、
// maker notional 为 size×worst_price 但只作为按股数成交的限价，不是预算。
func TestTradingClientPostsEmulatedIOCAsShareDenominatedGTC(t *testing.T) {
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
			if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
				t.Fatal(err)
			}
			writeTestJSON(writer, map[string]any{
				"success": true, "orderID": signedOrderIDForTest(t, posted.Order, false), "status": "live",
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	order := adapterOrder()
	order.Intent.TimeInForce = domain.TimeInForceIOC
	order.Intent.Size = "42"
	order.MarketValidation.WorstPrice = "0.50"
	order.MarketValidation.ExecutableSize = "7"
	venueOrder, err := client.Place(context.Background(), order)
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if venueOrder.State != port.VenueOrderLive {
		t.Fatalf("venue order = %#v, want resting GTC for execution to cancel", venueOrder)
	}
	if posted.OrderType != "GTC" || posted.Order.MakerAmount != "3500000" || posted.Order.TakerAmount != "7000000" || posted.Order.Side != "BUY" {
		t.Fatalf("posted order = %#v, want GTC 7 shares at 0.50", posted)
	}
}

// TestCancelKeepsConfirmedCancelWhilePartialFillDetailsPropagate 验证撤单已确认、部分成交明细尚未可见时，
// 返回 CANCELLED 观察结果而不是把确认的撤单降级为未知结果；模拟 IOC 的残单撤销依赖这一点。
func TestCancelKeepsConfirmedCancelWhilePartialFillDetailsPropagate(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodDelete && request.URL.Path == "/order":
			writeTestJSON(writer, map[string]any{"canceled": []string{"0xvenue"}, "not_canceled": map[string]string{}})
		case request.Method == http.MethodGet && request.URL.Path == "/data/order/0xvenue":
			writeTestJSON(writer, map[string]any{
				"id": "0xvenue", "status": "CANCELED", "original_size": "10000000", "size_matched": "4000000",
			})
		case request.Method == http.MethodGet && request.URL.Path == "/data/trades":
			writeTestJSON(writer, []map[string]any{})
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
	if venueOrder.State != port.VenueOrderCancelled || venueOrder.FilledSize != "4" {
		t.Fatalf("Cancel() = %#v, want CANCELLED with the observed partial fill", venueOrder)
	}
}

type feeEvidenceSourceFunc func(context.Context, FillFeeEvidenceRequest) (FillFeeEvidence, error)

func (source feeEvidenceSourceFunc) ResolveFillFeeEvidence(ctx context.Context, request FillFeeEvidenceRequest) (FillFeeEvidence, error) {
	return source(ctx, request)
}

// TestListOrderFillsReturnsShallowReceiptAsFinalityPendingMinedFill verifies a
// CLOB CONFIRMED trade whose Polygon receipt is below the configured depth is
// returned as a MINED finality-pending fill carrying the exact settlement
// evidence, and that the later finalized read is the same fill identity.
func TestListOrderFillsReturnsShallowReceiptAsFinalityPendingMinedFill(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	order := adapterOrder()
	order.VenueOrderID = "0x" + strings.Repeat("ab", 32)
	order.Intent.ConditionID = "condition-1"
	tokenID := order.Intent.TokenID
	transactionHash := "0x" + strings.Repeat("cd", 32)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/data/trades":
			writeTestJSON(writer, map[string]any{"data": []map[string]any{{
				"id": "trade-shallow", "taker_order_id": order.VenueOrderID, "market": "condition-1",
				"asset_id": tokenID, "side": "BUY", "size": "10", "price": "0.5",
				"status": "CONFIRMED", "fee_rate_bps": "0", "transaction_hash": transactionHash,
				"match_time": "2026-08-18T07:59:58Z", "last_update": "2026-08-18T07:59:59Z",
				"trader_side": "TAKER", "maker_address": request.URL.Query().Get("maker_address"),
			}}, "next_cursor": "LTE="})
		case "/clob-markets/condition-1":
			writeTestJSON(writer, map[string]any{
				"c": "condition-1", "t": []map[string]any{{"t": tokenID}},
				"fd": map[string]any{"r": 0.25, "e": 2, "to": true},
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	confirmations, finalized := uint64(17), false
	client := newTestTradingClient(t, server.URL, now)
	client.feeEvidence = feeEvidenceSourceFunc(func(_ context.Context, request FillFeeEvidenceRequest) (FillFeeEvidence, error) {
		return FillFeeEvidence{
			Source: v2OrderFilledFeeSource, ExchangeAddress: request.ExpectedExchangeAddress,
			TransactionHash: request.TransactionHash, OrderHash: request.VenueOrderID,
			MakerAddress: request.ExpectedMakerAddress, TokenID: request.TokenID,
			BuilderCode: request.ExpectedBuilderCode, Side: request.Side,
			MakerAmountBaseUnits: "5000000", TakerAmountBaseUnits: "10000000", TotalFeeBaseUnits: "156250",
			BuilderFeeBaseUnits: "0", BuilderFeeKnown: true, CollateralDecimals: 6, OutcomeTokenDecimals: 6,
			BlockNumber: 100, BlockHash: "0x" + strings.Repeat("ef", 32), LogIndex: 3,
			Confirmations: confirmations, Finalized: finalized,
		}, nil
	})

	pending, err := client.ListOrderFills(context.Background(), order)
	if err != nil || len(pending) != 1 {
		t.Fatalf("ListOrderFills() = %#v, %v; want one finality-pending fill", pending, err)
	}
	if pending[0].Status != domain.FillStatusMined || pending[0].ConfirmedAt != nil {
		t.Fatalf("finality-pending fill = status %s confirmed_at %v", pending[0].Status, pending[0].ConfirmedAt)
	}
	if pending[0].SettlementEvidence == nil || pending[0].SettlementEvidence.Confirmations != 17 ||
		pending[0].FeeSource != domain.FeeSourcePolygonV2OrderFilled ||
		!pending[0].TotalFee.Equal("0.15625") || !pending[0].GrossNotional.Equal("5") {
		t.Fatalf("finality-pending fill lost evidence or money: %#v", pending[0])
	}

	confirmations, finalized = 64, true
	final, err := client.ListOrderFills(context.Background(), order)
	if err != nil || len(final) != 1 {
		t.Fatalf("ListOrderFills() = %#v, %v; want one finalized fill", final, err)
	}
	if final[0].Status != domain.FillStatusConfirmed || final[0].ConfirmedAt == nil {
		t.Fatalf("finalized fill = status %s confirmed_at %v", final[0].Status, final[0].ConfirmedAt)
	}
	if final[0].RawPayloadSHA256 != pending[0].RawPayloadSHA256 || final[0].Key != pending[0].Key ||
		final[0].VenueFillID != pending[0].VenueFillID {
		t.Fatalf("finalized fill must keep the pending identity: %#v vs %#v", final[0], pending[0])
	}
}

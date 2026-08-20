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
				"id": "trade-filled", "taker_order_id": "0xvenue", "size": "10000000", "price": "0.5",
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
					"id": tradeID, "taker_order_id": placedOrderID, "size": "2000000", "price": price,
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
			"asset_id": "token-1", "side": "BUY", "size": "100000000", "price": "0.5",
			"status": "MATCHED", "transaction_hash": "0xtx",
			"maker_orders": []map[string]any{{"order_id": "0xmaker", "matched_amount": "2500000", "price": "0.5"}},
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
		trades[0].TransactionHash != "0xtx" {
		t.Fatalf("trades = %#v", trades)
	}
}

func TestTradeWireQuantityRejectsNonCanonicalOrHumanDecimal(t *testing.T) {
	for _, size := range []string{`"01"`, `"1.5"`, `1000000`} {
		var trade Trade
		if err := json.Unmarshal([]byte(`{"size":`+size+`}`), &trade); err == nil {
			t.Fatalf("non-canonical V2 size %s was accepted", size)
		}
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
				"asset_id": tokenID, "side": "BUY", "size": "10000000", "price": "0.5",
				"status": "CONFIRMED", "fee_rate_bps": "0", "transaction_hash": "0xtx",
				"match_time": "2026-08-18T07:59:58Z", "last_update": "2026-08-18T07:59:59Z",
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
		!strings.Contains(err.Error(), "fee evidence source is not configured") {
		t.Fatalf("ListOrderFills() error = %v, want fail-closed evidence error", err)
	}
	schedule := client.feeSchedules[order.Intent.ConditionID+"\x00"+order.Intent.TokenID]
	if !schedule.Rate.Equal("0.25") || !schedule.Exponent.Equal("2") {
		t.Fatalf("official fee schedule = %#v", schedule)
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

package kalshi

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestClientSignsReadRequestsAndProbesScopes(t *testing.T) {
	privateKey := testPrivateKey(t)
	const keyID = "test-key-id"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, keyID, &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/trade-api/v2/portfolio/balance":
			_, _ = writer.Write([]byte(`{"balance_dollars":"12.34","portfolio_value":1234}`))
		case "/trade-api/v2/api_keys":
			_, _ = writer.Write([]byte(`{"api_keys":[{"api_key_id":"test-key-id","name":"trading","scopes":["read","write"]}]}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, keyID, privateKey, false)
	capabilities, err := client.ProbeCapabilities(context.Background())
	if err != nil {
		t.Fatalf("ProbeCapabilities() error = %v", err)
	}
	if !capabilities.Authenticated || !capabilities.Read || !capabilities.Write {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func TestPrepareOrderMapsOutcomeToSingleYesBook(t *testing.T) {
	client := testClient(t, "https://example.com", "key", testPrivateKey(t), false)
	tests := []struct {
		name      string
		side      domain.Side
		outcomeID string
		bookSide  string
		price     string
	}{
		{name: "buy yes", side: domain.SideBuy, outcomeID: "YES", bookSide: "bid", price: "0.6000"},
		{name: "buy no", side: domain.SideBuy, outcomeID: "NO", bookSide: "ask", price: "0.4000"},
		{name: "sell yes", side: domain.SideSell, outcomeID: "YES", bookSide: "ask", price: "0.6000"},
		{name: "sell no", side: domain.SideSell, outcomeID: "NO", bookSide: "bid", price: "0.4000"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := validKalshiIntent(test.side, test.outcomeID)
			prepared, err := client.PrepareOrder(intent)
			if err != nil {
				t.Fatalf("PrepareOrder() error = %v", err)
			}
			if prepared.Request.Side != test.bookSide || prepared.Request.Price != test.price || prepared.Request.Ticker != "TEST-MARKET" {
				t.Fatalf("request = %#v", prepared.Request)
			}
			if prepared.Request.ReduceOnly != (test.side == domain.SideSell) || prepared.Fingerprint() == "" {
				t.Fatalf("prepared order safety fields = %#v", prepared)
			}
		})
	}
}

func TestSubmitPreparedIsIndependentAndFailClosed(t *testing.T) {
	privateKey := testPrivateKey(t)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte(`{"order_id":"venue-order","client_order_id":"client-order","fill_count":"1.00","remaining_count":"0.00","ts_ms":1787673600000}`))
	}))
	t.Cleanup(server.Close)
	disabled := testClient(t, server.URL, "key", privateKey, false)
	prepared, err := disabled.PrepareOrder(validKalshiIntent(domain.SideBuy, "YES"))
	if err != nil {
		t.Fatalf("PrepareOrder() error = %v", err)
	}
	if _, err := disabled.SubmitPrepared(context.Background(), prepared); err == nil {
		t.Fatal("SubmitPrepared() error = nil while live trading disabled")
	}
	if requests != 0 {
		t.Fatalf("disabled client sent %d requests", requests)
	}
	enabled := testClient(t, server.URL, "key", privateKey, true)
	response, err := enabled.SubmitPrepared(context.Background(), prepared)
	if err != nil {
		t.Fatalf("SubmitPrepared() error = %v", err)
	}
	if response.OrderID != "venue-order" || response.ClientOrderID != "client-order" || requests != 1 {
		t.Fatalf("response = %#v, requests = %d", response, requests)
	}
}

func TestBalanceSupportsCurrentCentResponse(t *testing.T) {
	if got := (Balance{Balance: 12345}).AvailableDollars(); got != "123.45" {
		t.Fatalf("AvailableDollars()=%s", got)
	}
}

func TestOrderLifecycleAndFillEvidenceUseExactVenueIdentity(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/orders/venue-order":
			_, _ = writer.Write([]byte(`{"order":{"order_id":"venue-order","client_order_id":"client-order","ticker":"TEST-MARKET","status":"executed","fill_count_fp":"2.00","remaining_count_fp":"0.00","initial_count_fp":"2.00","taker_fill_cost_dollars":"1.20","maker_fill_cost_dollars":"0","last_update_time":"2026-08-26T00:00:01Z"}}`))
		case request.Method == http.MethodDelete && request.URL.Path == "/trade-api/v2/portfolio/events/orders/venue-order":
			_, _ = writer.Write([]byte(`{"order":{"order_id":"venue-order","client_order_id":"client-order","ticker":"TEST-MARKET","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"2.00","last_update_time":"2026-08-26T00:00:02Z"}}`))
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/fills":
			if request.URL.Query().Get("order_id") != "venue-order" {
				t.Errorf("order_id query=%q", request.URL.Query().Get("order_id"))
			}
			_, _ = writer.Write([]byte(`{"fills":[{"fill_id":"fill-1","order_id":"venue-order","market_ticker":"TEST-MARKET","outcome_side":"yes","count_fp":"2.00","yes_price_dollars":"0.6000","no_price_dollars":"0.4000","is_taker":true,"fee_cost":"0.03","action":"buy","created_time":"2026-08-26T00:00:01Z"}],"cursor":""}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, true)
	remote, err := client.GetOrder(context.Background(), "venue-order")
	if err != nil || remote.Status != "executed" {
		t.Fatalf("GetOrder=%#v err=%v", remote, err)
	}
	order := domain.Order{VenueOrderID: "venue-order", Intent: validKalshiIntent(domain.SideBuy, "YES")}
	fills, err := client.ListOrderFills(context.Background(), order)
	if err != nil {
		t.Fatal(err)
	}
	if len(fills) != 1 || fills[0].VenueFillID != "fill-1" || fills[0].Price != "0.6000" || fills[0].TotalFee != "0.03000000" || fills[0].FeeSource != "KALSHI_API" {
		t.Fatalf("fills=%#v", fills)
	}
	cancelled, err := client.CancelOrder(context.Background(), "venue-order")
	if err != nil || cancelled.Status != "canceled" {
		t.Fatalf("CancelOrder=%#v err=%v", cancelled, err)
	}
}

func TestFillEvidenceRejectsWrongOutcomeOrAction(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"fills":[{"fill_id":"fill-1","order_id":"venue-order","ticker":"TEST-MARKET","outcome_side":"no","count_fp":"1","yes_price_dollars":"0.6","no_price_dollars":"0.4","is_taker":true,"fee_cost":"0","action":"sell","created_time":"2026-08-26T00:00:01Z"}]}`))
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, false)
	order := domain.Order{VenueOrderID: "venue-order", Intent: validKalshiIntent(domain.SideBuy, "YES")}
	if _, err := client.ListOrderFills(context.Background(), order); err == nil {
		t.Fatal("mismatched fill identity must fail closed")
	}
}

func validKalshiIntent(side domain.Side, outcomeID string) domain.OrderIntent {
	intent := domain.OrderIntent{
		ModelID: "model", StrategyID: "multfactor_v1", ExecutionAccountID: "account",
		SignalID: "signal", ClientOrderID: "client-order", Venue: "kalshi",
		MarketSource: domain.MarketSourceKalshi, MarketID: "TEST-MARKET", ConditionID: "kalshi:TEST-MARKET",
		OutcomeID: outcomeID, TokenID: "kalshi:TEST-MARKET:" + outcomeID,
		Side: side, Type: domain.OrderTypeLimit, Price: "0.6", WorstPrice: "0.6", Size: "2",
		TimeInForce: domain.TimeInForceFOK,
	}
	if side == domain.SideSell {
		intent.TargetLotID = "lot"
	}
	return intent
}

func testClient(t *testing.T, baseURL, keyID string, privateKey *rsa.PrivateKey, live bool) *Client {
	t.Helper()
	client, err := NewClient(ClientParams{
		BaseURL: baseURL, APIKeyID: keyID, PrivateKey: privateKey,
		HTTPClient:         &http.Client{Timeout: time.Second},
		Now:                func() time.Time { return time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC) },
		LiveTradingEnabled: live,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func testPrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	return privateKey
}

func verifySignature(t *testing.T, request *http.Request, keyID string, publicKey *rsa.PublicKey) {
	t.Helper()
	if request.Header.Get("KALSHI-ACCESS-KEY") != keyID {
		t.Errorf("access key = %q", request.Header.Get("KALSHI-ACCESS-KEY"))
	}
	timestamp := request.Header.Get("KALSHI-ACCESS-TIMESTAMP")
	signature, err := base64.StdEncoding.DecodeString(request.Header.Get("KALSHI-ACCESS-SIGNATURE"))
	if err != nil {
		t.Errorf("decode signature: %v", err)
		return
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s%s%s", timestamp, request.Method, request.URL.EscapedPath())))
	if err := rsa.VerifyPSS(publicKey, crypto.SHA256, digest[:], signature, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}); err != nil {
		t.Errorf("VerifyPSS() error = %v", err)
	}
}

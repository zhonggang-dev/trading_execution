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

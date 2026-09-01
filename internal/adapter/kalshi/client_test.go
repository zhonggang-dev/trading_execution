package kalshi

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
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

func TestKalshiReadOnlyNotFoundAndRateLimitRemainRetryable(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		status   int
		body     string
		call     func(*Client) error
		wantCode string
		wantKind port.VenueErrorKind
	}{
		{
			name: "order detail not yet visible", status: http.StatusNotFound,
			body:     `{"error":{"code":"not_found","message":"order not found"}}`,
			call:     func(client *Client) error { _, err := client.GetOrder(context.Background(), "venue-order"); return err },
			wantCode: "KALSHI_ORDER_VISIBILITY_PENDING", wantKind: port.VenueErrorUnavailable,
		},
		{
			name: "read rate limited", status: http.StatusTooManyRequests,
			body:     `{"error":{"code":"rate_limited","message":"slow down"}}`,
			call:     func(client *Client) error { _, err := client.GetBalance(context.Background()); return err },
			wantCode: "KALSHI_RATE_LIMITED", wantKind: port.VenueErrorUnavailable,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			privateKey := testPrivateKey(t)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				verifySignature(t, request, "key", &privateKey.PublicKey)
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(testCase.status)
				_, _ = writer.Write([]byte(testCase.body))
			}))
			t.Cleanup(server.Close)
			err := testCase.call(testClient(t, server.URL, "key", privateKey, false))
			var venueError *port.VenueError
			if !errors.As(err, &venueError) || venueError.Kind != testCase.wantKind || venueError.Code != testCase.wantCode {
				t.Fatalf("read error = %#v, venue error = %#v", err, venueError)
			}
		})
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

func TestPrepareOrderMapsIOCAndRejectsExpiration(t *testing.T) {
	client := testClient(t, "https://example.com", "key", testPrivateKey(t), false)
	intent := validKalshiIntent(domain.SideBuy, "YES")
	intent.TimeInForce = domain.TimeInForceIOC
	prepared, err := client.PrepareOrder(intent)
	if err != nil {
		t.Fatalf("PrepareOrder() error = %v", err)
	}
	if prepared.Request.TimeInForce != "immediate_or_cancel" || prepared.Request.ExpirationTime != nil {
		t.Fatalf("prepared IOC = %#v", prepared.Request)
	}
	expiresAt := time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC)
	intent.ExpiresAt = &expiresAt
	if _, err := client.PrepareOrder(intent); err == nil || !strings.Contains(err.Error(), "must not contain expires_at") {
		t.Fatalf("PrepareOrder(IOC+expiry) error = %v", err)
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
		_, _ = writer.Write([]byte(`{"order_id":"venue-order","fill_count":"2.00","remaining_count":"0.00","ts_ms":1787673600000}`))
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
	if response.OrderID != "venue-order" || response.ClientOrderID != "" || requests != 1 {
		t.Fatalf("response = %#v, requests = %d", response, requests)
	}
}

func TestSubmitPreparedClassifiesFOKHTTP409AsDefinitiveRejection(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"invalid_order","message":"fill_or_kill order would not be filled immediately due to insufficient resting volume"}}`))
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, true)
	prepared, err := client.PrepareOrder(validKalshiIntent(domain.SideBuy, "YES"))
	if err != nil {
		t.Fatal(err)
	}
	_, submitErr := client.SubmitPrepared(context.Background(), prepared)
	var venueError *port.VenueError
	if !errors.As(submitErr, &venueError) || venueError.Kind != port.VenueErrorRejected ||
		venueError.Code != "KALSHI_FOK_NOT_FILLED" || venueError.HTTPStatus != http.StatusConflict {
		t.Fatalf("SubmitPrepared() error = %#v, venue error = %#v", submitErr, venueError)
	}
}

func TestSubmitPreparedKeepsUnknownHTTP409Ambiguous(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"conflict","message":"fill_or_kill client order id conflict"}}`))
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, true)
	prepared, err := client.PrepareOrder(validKalshiIntent(domain.SideBuy, "YES"))
	if err != nil {
		t.Fatal(err)
	}
	_, submitErr := client.SubmitPrepared(context.Background(), prepared)
	var venueError *port.VenueError
	if !errors.As(submitErr, &venueError) || venueError.Kind != port.VenueErrorAmbiguous ||
		venueError.Code != "KALSHI_ORDER_CONFLICT" || venueError.HTTPStatus != http.StatusConflict {
		t.Fatalf("SubmitPrepared() error = %#v, venue error = %#v", submitErr, venueError)
	}
}

func TestSubmitPreparedKeepsFOKSpecific409AmbiguousForIOC(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"invalid_order","message":"fill_or_kill order would not be filled immediately due to insufficient resting volume"}}`))
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, true)
	intent := validKalshiIntent(domain.SideBuy, "YES")
	intent.TimeInForce = domain.TimeInForceIOC
	prepared, err := client.PrepareOrder(intent)
	if err != nil {
		t.Fatal(err)
	}
	_, submitErr := client.SubmitPrepared(context.Background(), prepared)
	var venueError *port.VenueError
	if !errors.As(submitErr, &venueError) || venueError.Kind != port.VenueErrorAmbiguous ||
		venueError.Code != "KALSHI_ORDER_CONFLICT" {
		t.Fatalf("SubmitPrepared(IOC) error = %#v, venue error = %#v", submitErr, venueError)
	}
}

func TestVenueKeepsMutatingServerFailureAmbiguousWithClientOrderID(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"error":{"code":"service_unavailable","message":"matching engine unavailable"}}`))
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, true)
	venue, err := NewVenue(client)
	if err != nil {
		t.Fatal(err)
	}
	intent := validKalshiIntent(domain.SideBuy, "YES")
	_, placeErr := venue.Place(context.Background(), domain.Order{ID: "order-1", Intent: intent})
	var venueError *port.VenueError
	if !errors.As(placeErr, &venueError) || venueError.Kind != port.VenueErrorAmbiguous ||
		venueError.Code != "KALSHI_SERVER_ERROR" || venueError.VenueOrderID != intent.ClientOrderID {
		t.Fatalf("Place() error = %#v, venue error = %#v", placeErr, venueError)
	}
}

func TestKalshiFOKRejectionReleasesReservationAndAllowsNextSubmission(t *testing.T) {
	privateKey := testPrivateKey(t)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		verifySignature(t, request, "key", &privateKey.PublicKey)
		var submitted OrderRequestV2
		if err := json.NewDecoder(request.Body).Decode(&submitted); err != nil {
			t.Errorf("decode submitted order: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"invalid_order","message":"fill_or_kill order would not be filled immediately due to insufficient resting volume"}}`))
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, true)
	venue, err := NewVenue(client)
	if err != nil {
		t.Fatal(err)
	}
	reservations := paper.NewReservationManager()
	sequence := 0
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue,
		Guard: kalshiAllowGuard{}, MarketValidator: kalshiAllowMarketValidator{}, Reservations: reservations,
		RequirePreparedPlacement: true, AccountScope: kalshiAllowAccountScope{},
		Now: func() time.Time { return time.Date(2026, 8, 31, 0, 0, sequence, 0, time.UTC) },
		NewID: func() string {
			sequence++
			return fmt.Sprintf("kalshi-order-%d", sequence)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	firstIntent := validKalshiIntent(domain.SideBuy, "YES")
	first, firstErr := service.Submit(context.Background(), firstIntent)
	if firstErr == nil || first.Order.Status != domain.OrderStatusRejected || first.Order.FailureCode != "KALSHI_FOK_NOT_FILLED" {
		t.Fatalf("first Submit() = %#v, %v", first, firstErr)
	}
	reservation, found := reservations.Get(first.Order.ID)
	if !found || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("first reservation = %#v, found=%v; want RELEASED", reservation, found)
	}

	secondIntent := validKalshiIntent(domain.SideBuy, "YES")
	secondIntent.ClientOrderID = "client-order-2"
	secondIntent.SignalID = "signal-2"
	second, secondErr := service.Submit(context.Background(), secondIntent)
	if secondErr == nil || second.Order.Status != domain.OrderStatusRejected || requests != 2 {
		t.Fatalf("second Submit() = %#v, %v, requests=%d; want a fresh venue submission", second, secondErr, requests)
	}
}

func TestKalshiIOCZeroFillFlowsThroughExecutionAndReleasesAfterOfficialFinality(t *testing.T) {
	privateKey := testPrivateKey(t)
	currentTime := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	const authoritativeID = "kalshi-ioc-zero-fill"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/trade-api/v2/portfolio/events/orders":
			var submitted OrderRequestV2
			if err := json.NewDecoder(request.Body).Decode(&submitted); err != nil {
				t.Errorf("decode submitted order: %v", err)
			}
			if submitted.TimeInForce != "immediate_or_cancel" || submitted.ExpirationTime != nil {
				t.Errorf("submitted IOC request = %#v", submitted)
			}
			_, _ = fmt.Fprintf(writer,
				`{"order_id":%q,"client_order_id":%q,"fill_count":"0","remaining_count":"0","ts_ms":%d}`,
				authoritativeID, submitted.ClientOrderID, currentTime.UnixMilli())
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/orders/"+authoritativeID:
			_, _ = fmt.Fprintf(writer,
				`{"order":{"order_id":%q,"client_order_id":"client-order","ticker":"TEST-MARKET","outcome_side":"yes","book_side":"bid","type":"limit","time_in_force":"immediate_or_cancel","yes_price_dollars":"0.6000","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"2.00","cancel_order_on_pause":true,"subaccount_number":0,"last_update_time":"2026-09-01T00:00:00Z"}}`,
				authoritativeID)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(ClientParams{
		BaseURL: server.URL, APIKeyID: "key", PrivateKey: privateKey,
		HTTPClient: &http.Client{Timeout: time.Second}, Now: func() time.Time { return currentTime }, LiveTradingEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	venue, err := NewVenue(client)
	if err != nil {
		t.Fatal(err)
	}
	reservations := paper.NewReservationManager()
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue,
		Guard: kalshiAllowGuard{}, MarketValidator: kalshiAllowMarketValidator{}, Reservations: reservations,
		FillSynchronizer: kalshiNoopFillSynchronizer{}, AuthoritativeFills: true,
		RequirePreparedPlacement: true, AccountScope: kalshiAllowAccountScope{},
		CancelFillFinalityGrace: 30 * time.Second,
		Now:                     func() time.Time { return currentTime }, NewID: func() string { return "kalshi-ioc-order" },
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validKalshiIntent(domain.SideBuy, "YES")
	intent.TimeInForce = domain.TimeInForceIOC
	result, err := service.Submit(context.Background(), intent)
	if err != nil || result.Order.Status != domain.OrderStatusCancelled || result.Order.VenueOrderID != authoritativeID {
		t.Fatalf("Submit() = %#v, %v", result, err)
	}
	reservation, found := reservations.Get(result.Order.ID)
	if !found || reservation.Status != domain.ReservationStatusReconciliationRequired {
		t.Fatalf("reservation after IOC zero fill = %#v, found=%v", reservation, found)
	}

	currentTime = currentTime.Add(31 * time.Second)
	finalized, err := service.FinalizeCancellation(context.Background(), result.Order.ID)
	if err != nil || finalized.Status != domain.OrderStatusCancelled {
		t.Fatalf("FinalizeCancellation() = %#v, %v", finalized, err)
	}
	reservation, found = reservations.Get(result.Order.ID)
	if !found || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("final IOC reservation = %#v, found=%v; want RELEASED", reservation, found)
	}
}

func TestKalshiSuccessfulSubmissionAdoptsAuthoritativeOrderID(t *testing.T) {
	privateKey := testPrivateKey(t)
	const authoritativeID = "01a056df-authoritative"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/trade-api/v2/portfolio/events/orders":
			var submitted OrderRequestV2
			if err := json.NewDecoder(request.Body).Decode(&submitted); err != nil {
				t.Errorf("decode submitted order: %v", err)
			}
			_, _ = fmt.Fprintf(writer, `{"order_id":%q,"client_order_id":%q,"fill_count":"0","remaining_count":"2","ts_ms":1787673600000}`, authoritativeID, submitted.ClientOrderID)
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/fills":
			if request.URL.Query().Get("order_id") != authoritativeID {
				t.Errorf("fill lookup order_id = %q", request.URL.Query().Get("order_id"))
			}
			_, _ = writer.Write([]byte(`{"fills":[],"cursor":""}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, true)
	venue, err := NewVenue(client)
	if err != nil {
		t.Fatal(err)
	}
	repository := memory.NewOrderRepository()
	service, err := execution.New(execution.Params{
		Repository: repository, Venue: venue,
		Guard: kalshiAllowGuard{}, MarketValidator: kalshiAllowMarketValidator{}, Reservations: paper.NewReservationManager(),
		RequirePreparedPlacement: true, AccountScope: kalshiAllowAccountScope{},
		Now:   func() time.Time { return time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC) },
		NewID: func() string { return "kalshi-order-success" },
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validKalshiIntent(domain.SideBuy, "YES")
	intent.TimeInForce = domain.TimeInForceGTC
	intent.ClientOrderID = "strategy-order-success"
	result, err := service.Submit(context.Background(), intent)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if result.Order.Status != domain.OrderStatusAcknowledged || result.Order.VenueOrderID != authoritativeID {
		t.Fatalf("submitted order = %#v", result.Order)
	}
	attempts, err := repository.Attempts(context.Background(), result.Order.ID)
	if err != nil || len(attempts) != 1 || attempts[0].VenueOrderID != authoritativeID || attempts[0].Outcome != domain.AttemptOutcomeSucceeded {
		t.Fatalf("attempts = %#v, err = %v", attempts, err)
	}
}

func TestKalshiUnknownSubmissionRecoversByClientOrderIDAndReleasesReservation(t *testing.T) {
	privateKey := testPrivateKey(t)
	const authoritativeID = "01a056df-recovered"
	now := time.Date(2026, 8, 31, 0, 10, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/trade-api/v2/portfolio/events/orders":
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(`{"error":{"code":"service_unavailable","message":"matching engine unavailable"}}`))
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/orders":
			_, _ = fmt.Fprintf(writer, `{"orders":[{"order_id":%q,"client_order_id":"strategy-order-recovery","ticker":"TEST-MARKET","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"2","last_update_time":"2026-08-31T00:00:01Z"}],"cursor":""}`, authoritativeID)
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/orders/"+authoritativeID:
			_, _ = fmt.Fprintf(writer, `{"order":{"order_id":%q,"client_order_id":"strategy-order-recovery","ticker":"TEST-MARKET","outcome_side":"yes","book_side":"bid","type":"limit","time_in_force":"fill_or_kill","yes_price_dollars":"0.6000","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"2","cancel_order_on_pause":true,"subaccount_number":0,"last_update_time":"2026-08-31T00:00:01Z"}}`, authoritativeID)
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/fills":
			if request.URL.Query().Get("order_id") != authoritativeID {
				t.Errorf("fill lookup order_id = %q", request.URL.Query().Get("order_id"))
			}
			_, _ = writer.Write([]byte(`{"fills":[],"cursor":""}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(ClientParams{
		BaseURL: server.URL, APIKeyID: "key", PrivateKey: privateKey,
		HTTPClient: &http.Client{Timeout: time.Second}, Now: func() time.Time { return now }, LiveTradingEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	venue, err := NewVenue(client)
	if err != nil {
		t.Fatal(err)
	}
	reservations := paper.NewReservationManager()
	service, err := execution.New(execution.Params{
		Repository: memory.NewOrderRepository(), Venue: venue,
		Guard: kalshiAllowGuard{}, MarketValidator: kalshiAllowMarketValidator{}, Reservations: reservations,
		RequirePreparedPlacement: true, AccountScope: kalshiAllowAccountScope{},
		CancelFillFinalityGrace: 30 * time.Second,
		Now:                     func() time.Time { return now },
		NewID:                   func() string { return "kalshi-recovery-order" },
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := validKalshiIntent(domain.SideBuy, "YES")
	intent.ClientOrderID = "strategy-order-recovery"
	result, submitErr := service.Submit(context.Background(), intent)
	if submitErr == nil || result.Order.Status != domain.OrderStatusUnknown || result.Order.VenueOrderID != intent.ClientOrderID {
		t.Fatalf("ambiguous Submit() = %#v, %v", result, submitErr)
	}
	recovered, err := service.Refresh(context.Background(), result.Order.ID)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if recovered.Status != domain.OrderStatusCancelled || recovered.VenueOrderID != authoritativeID {
		t.Fatalf("recovered order = %#v", recovered)
	}
	reservation, found := reservations.Get(result.Order.ID)
	if !found || reservation.Status != domain.ReservationStatusReconciliationRequired {
		t.Fatalf("recovered reservation = %#v, found=%v; want finality hold", reservation, found)
	}
	now = now.Add(31 * time.Second)
	if _, err := service.FinalizeCancellation(context.Background(), result.Order.ID); err != nil {
		t.Fatalf("FinalizeCancellation() error = %v", err)
	}
	reservation, found = reservations.Get(result.Order.ID)
	if !found || reservation.Status != domain.ReservationStatusReleased {
		t.Fatalf("final reservation = %#v, found=%v; want RELEASED", reservation, found)
	}
}

type kalshiAllowGuard struct{}

func (kalshiAllowGuard) Check(context.Context, domain.OrderIntent) error { return nil }

type kalshiAllowMarketValidator struct{}

func (kalshiAllowMarketValidator) Validate(context.Context, domain.OrderIntent) (domain.MarketValidation, error) {
	return domain.MarketValidation{Mode: "TEST"}, nil
}

type kalshiAllowAccountScope struct{}

func (kalshiAllowAccountScope) IsActive(string) bool  { return true }
func (kalshiAllowAccountScope) IsManaged(string) bool { return true }

type kalshiNoopFillSynchronizer struct{}

func (kalshiNoopFillSynchronizer) SyncOrder(context.Context, string) (port.FillSyncResult, error) {
	return port.FillSyncResult{}, nil
}

func TestBalanceSupportsCurrentCentResponse(t *testing.T) {
	if got := (Balance{Balance: 12345}).AvailableDollars(); got != "123.45" {
		t.Fatalf("AvailableDollars()=%s", got)
	}
}

func TestOrderLifecycleAndFillEvidenceUseExactVenueIdentity(t *testing.T) {
	privateKey := testPrivateKey(t)
	var cancellationAcknowledged atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/orders/venue-order":
			if cancellationAcknowledged.Load() {
				_, _ = writer.Write([]byte(`{"order":{"order_id":"venue-order","client_order_id":"client-order","ticker":"TEST-MARKET","status":"canceled","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"2.00"}}`))
			} else {
				_, _ = writer.Write([]byte(`{"order":{"order_id":"venue-order","client_order_id":"client-order","ticker":"TEST-MARKET","status":"executed","fill_count_fp":"2.00","remaining_count_fp":"0.00","initial_count_fp":"2.00","taker_fill_cost_dollars":"1.20","maker_fill_cost_dollars":"0","last_update_time":"2026-08-26T00:00:01Z"}}`))
			}
		case request.Method == http.MethodDelete && request.URL.Path == "/trade-api/v2/portfolio/events/orders/venue-order":
			if request.URL.Query().Get("market_ticker") != "TEST-MARKET" || request.URL.Query().Get("subaccount") != "0" {
				t.Errorf("cancel query = %s", request.URL.RawQuery)
			}
			cancellationAcknowledged.Store(true)
			_, _ = writer.Write([]byte(`{"order_id":"venue-order","client_order_id":"client-order","reduced_by":"2.00","ts_ms":1787702402000}`))
		case request.Method == http.MethodGet && request.URL.Path == "/trade-api/v2/portfolio/fills":
			if request.URL.Query().Get("order_id") != "venue-order" {
				t.Errorf("order_id query=%q", request.URL.Query().Get("order_id"))
			}
			_, _ = writer.Write([]byte(`{"fills":[{"fill_id":"fill-1","order_id":"venue-order","market_ticker":"TEST-MARKET","outcome_side":"yes","book_side":"bid","count_fp":"2.00","yes_price_dollars":"0.6000","no_price_dollars":"0.4000","is_taker":true,"fee_cost":"0.03","action":"buy","created_time":"2026-08-26T00:00:01Z"}],"cursor":""}`))
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
	cancelled, err := client.CancelOrder(context.Background(), "venue-order", "TEST-MARKET", 0)
	if err != nil || cancelled.Status != "canceled" || cancelled.LastUpdateTime.UnixMilli() != 1787702402000 {
		t.Fatalf("CancelOrder=%#v err=%v", cancelled, err)
	}
}

func TestCancelOrderV2KeepsMalformedOrUnconfirmedResultAmbiguous(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		delete   string
		get      string
		wantCode string
	}{
		{
			name:     "legacy nested response is not authoritative",
			delete:   `{"order":{"order_id":"venue-order","client_order_id":"client-order","status":"canceled"}}`,
			wantCode: "KALSHI_INVALID_CANCEL_RESPONSE",
		},
		{
			name:     "read after acknowledgement is still resting",
			delete:   `{"order_id":"venue-order","client_order_id":"client-order","reduced_by":"2.00","ts_ms":1787702402000}`,
			get:      `{"order":{"order_id":"venue-order","client_order_id":"client-order","ticker":"TEST-MARKET","status":"resting","fill_count_fp":"0","remaining_count_fp":"2","initial_count_fp":"2","last_update_time":"2026-08-26T00:00:01Z"}}`,
			wantCode: "KALSHI_CANCEL_CONFIRMATION_PENDING",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			privateKey := testPrivateKey(t)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				verifySignature(t, request, "key", &privateKey.PublicKey)
				writer.Header().Set("Content-Type", "application/json")
				switch request.Method {
				case http.MethodDelete:
					_, _ = writer.Write([]byte(testCase.delete))
				case http.MethodGet:
					if testCase.get == "" {
						t.Error("unexpected canonical order read after malformed cancellation acknowledgement")
						http.NotFound(writer, request)
						return
					}
					_, _ = writer.Write([]byte(testCase.get))
				default:
					http.NotFound(writer, request)
				}
			}))
			t.Cleanup(server.Close)
			client := testClient(t, server.URL, "key", privateKey, true)
			_, err := client.CancelOrder(context.Background(), "venue-order", "TEST-MARKET", 0)
			var venueError *port.VenueError
			if !errors.As(err, &venueError) || venueError.Kind != port.VenueErrorAmbiguous ||
				venueError.Code != testCase.wantCode || venueError.VenueOrderID != "venue-order" {
				t.Fatalf("CancelOrder() error = %#v, venue error = %#v", err, venueError)
			}
		})
	}
}

func TestVenueCancelDoesNotDeleteAnAlreadyTerminalOrder(t *testing.T) {
	for _, testCase := range []struct {
		status string
		filled string
		want   port.VenueOrderState
	}{
		{status: "canceled", filled: "0", want: port.VenueOrderCancelled},
		{status: "executed", filled: "2", want: port.VenueOrderFilled},
	} {
		t.Run(testCase.status, func(t *testing.T) {
			privateKey := testPrivateKey(t)
			var deletes atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				verifySignature(t, request, "key", &privateKey.PublicKey)
				writer.Header().Set("Content-Type", "application/json")
				if request.Method == http.MethodDelete {
					deletes.Add(1)
					http.Error(writer, "unexpected delete", http.StatusConflict)
					return
				}
				_, _ = fmt.Fprintf(writer,
					`{"order":{"order_id":"venue-order","client_order_id":"client-order","ticker":"TEST-MARKET","outcome_side":"yes","book_side":"bid","type":"limit","time_in_force":"immediate_or_cancel","yes_price_dollars":"0.6000","status":%q,"fill_count_fp":%q,"remaining_count_fp":"0","initial_count_fp":"2","cancel_order_on_pause":true,"subaccount_number":0,"last_update_time":"2026-09-01T00:00:00Z"}}`,
					testCase.status, testCase.filled)
			}))
			t.Cleanup(server.Close)
			venue, err := NewVenue(testClient(t, server.URL, "key", privateKey, true))
			if err != nil {
				t.Fatal(err)
			}
			intent := validKalshiIntent(domain.SideBuy, "YES")
			intent.TimeInForce = domain.TimeInForceIOC
			observed, err := venue.Cancel(context.Background(), domain.Order{VenueOrderID: "venue-order", Intent: intent})
			if err != nil || observed.State != testCase.want || deletes.Load() != 0 {
				t.Fatalf("Cancel() = %#v, %v, deletes=%d", observed, err, deletes.Load())
			}
		})
	}
}

func TestFillEvidenceRejectsWrongOutcomeOrAction(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"fills":[{"fill_id":"fill-1","order_id":"venue-order","ticker":"TEST-MARKET","outcome_side":"no","book_side":"ask","count_fp":"1","yes_price_dollars":"0.6","no_price_dollars":"0.4","is_taker":true,"fee_cost":"0","action":"sell","created_time":"2026-08-26T00:00:01Z"}],"cursor":""}`))
	}))
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", privateKey, false)
	order := domain.Order{VenueOrderID: "venue-order", Intent: validKalshiIntent(domain.SideBuy, "YES")}
	if _, err := client.ListOrderFills(context.Background(), order); err == nil {
		t.Fatal("mismatched fill identity must fail closed")
	}
}

func TestFillEvidenceUsesCanonicalDirectionAndOutcomePriceForEveryIntent(t *testing.T) {
	tests := []struct {
		name        string
		side        domain.Side
		outcomeID   string
		outcomeSide string
		bookSide    string
		yesPrice    string
		wantPrice   domain.Decimal
	}{
		{name: "buy yes", side: domain.SideBuy, outcomeID: "YES", outcomeSide: "yes", bookSide: "bid", yesPrice: "0.6000", wantPrice: "0.6000"},
		{name: "buy no", side: domain.SideBuy, outcomeID: "NO", outcomeSide: "no", bookSide: "ask", yesPrice: "0.4000", wantPrice: "0.6000"},
		{name: "sell yes", side: domain.SideSell, outcomeID: "YES", outcomeSide: "no", bookSide: "ask", yesPrice: "0.6000", wantPrice: "0.6000"},
		{name: "sell no", side: domain.SideSell, outcomeID: "NO", outcomeSide: "yes", bookSide: "bid", yesPrice: "0.4000", wantPrice: "0.6000"},
		{name: "buy no six-decimal price", side: domain.SideBuy, outcomeID: "NO", outcomeSide: "no", bookSide: "ask", yesPrice: "0.123456", wantPrice: "0.876544"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			privateKey := testPrivateKey(t)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(writer,
					`{"fills":[{"fill_id":"fill-1","order_id":"venue-order","ticker":"TEST-MARKET","outcome_side":%q,"book_side":%q,"count_fp":"1","yes_price_dollars":%q,"no_price_dollars":"0.4000","is_taker":true,"fee_cost":"0","action":%q,"created_time":"2026-08-26T00:00:01Z"}],"cursor":""}`,
					testCase.outcomeSide, testCase.bookSide, testCase.yesPrice, strings.ToLower(string(testCase.side)))
			}))
			t.Cleanup(server.Close)
			client := testClient(t, server.URL, "key", privateKey, false)
			order := domain.Order{VenueOrderID: "venue-order", Intent: validKalshiIntent(testCase.side, testCase.outcomeID)}
			fills, err := client.ListOrderFills(context.Background(), order)
			if err != nil {
				t.Fatal(err)
			}
			if len(fills) != 1 || !fills[0].Price.Equal(testCase.wantPrice) {
				t.Fatalf("fills=%#v, want outcome price %s", fills, testCase.wantPrice)
			}
		})
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

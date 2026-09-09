package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestSellRawAmountsDeferMinimumToVenueButKeepPrecision(t *testing.T) {
	for _, tif := range []domain.TimeInForce{domain.TimeInForceGTC, domain.TimeInForceIOC, domain.TimeInForceFOK} {
		t.Run(string(tif), func(t *testing.T) {
			intent := adapterIntent()
			intent.Side, intent.TimeInForce, intent.Size = domain.SideSell, tif, "0.25"
			amounts, err := buildRawAmounts(intent, "0.01", "5", "1")
			if err != nil || amounts.MakerAmount != "250000" || amounts.TakerAmount != "125000" || amounts.Side != 1 {
				t.Fatalf("SELL raw amounts = %#v, err = %v", amounts, err)
			}
			intent.Side = domain.SideBuy
			_, err = buildRawAmounts(intent, "0.01", "5", "1")
			var venueErr *port.VenueError
			if !errors.As(err, &venueErr) || venueErr.Code != "MIN_ORDER_SIZE" {
				t.Fatalf("BUY error = %v, want MIN_ORDER_SIZE", err)
			}
		})
	}
	for _, test := range []struct{ size, price, tick, code string }{
		{"0", "0.50", "0.01", "INVALID_SIZE"},
		{"-1", "0.50", "0.01", "INVALID_SIZE"},
		{"bad", "0.50", "0.01", "INVALID_SIZE"},
		{"0.007", "0.50", "0.01", "INVALID_SIZE_PRECISION"},
		{"1", "0.505", "0.01", "PRICE_TICK_MISMATCH"},
		{"0.01", "0.501", "0.001", "INVALID_FAK_FOK_PRECISION"},
	} {
		t.Run(test.code+test.size, func(t *testing.T) {
			intent := adapterIntent()
			intent.Side, intent.TimeInForce = domain.SideSell, domain.TimeInForceFOK
			intent.Size, intent.Price = domain.Decimal(test.size), domain.Decimal(test.price)
			_, err := buildRawAmounts(intent, domain.Decimal(test.tick), "5", "1")
			var venueErr *port.VenueError
			if !errors.As(err, &venueErr) || venueErr.Code != test.code {
				t.Fatalf("SELL error = %v, want %s", err, test.code)
			}
		})
	}
}

// Requests stay in memory; this exercises signing and the real HTTP payload
// without contacting a venue or opening a localhost listener.
type sellTestTransport func(*http.Request) (*http.Response, error)

func (transport sellTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestSellBelowMinimumSignsAndPostsExactStrategySize(t *testing.T) {
	for _, tif := range []domain.TimeInForce{domain.TimeInForceFOK, domain.TimeInForceIOC} {
		t.Run(string(tif), func(t *testing.T) {
			now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
			client := newTestTradingClient(t, "https://clob.example.invalid", now)
			var posted postOrderPayload
			posts := 0
			client.httpClient.Transport = sellTestTransport(func(request *http.Request) (*http.Response, error) {
				response := httptest.NewRecorder()
				switch request.URL.Path {
				case "/version":
					writeTestJSON(response, map[string]any{"version": 2})
				case "/tick-size":
					writeTestJSON(response, map[string]any{"minimum_tick_size": "0.01"})
				case "/neg-risk":
					writeTestJSON(response, map[string]any{"neg_risk": false})
				case "/order":
					posts++
					if request.Method != http.MethodPost || request.Header.Get("POLY_SIGNATURE") == "" {
						t.Fatalf("invalid order method or authentication: %s", request.Method)
					}
					if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
						t.Fatal(err)
					}
					writeTestJSON(response, map[string]any{"success": true, "orderID": signedOrderIDForTest(t, posted.Order, false), "status": "live"})
				default:
					t.Fatalf("unexpected request: %s", request.URL.Path)
				}
				return response.Result(), nil
			})
			order := adapterOrder()
			order.Intent.Side, order.Intent.TimeInForce = domain.SideSell, tif
			order.Intent.TargetLotID, order.Intent.Size = "lot-1", "0.25"
			order.MarketValidation.BookStatus = domain.OrderBookStatusEmpty
			prepared, err := client.PreparePlace(context.Background(), order)
			if err != nil || posts != 0 {
				t.Fatalf("prepare = %v, posts = %d", err, posts)
			}
			order.VenueOrderID = prepared.ExpectedVenueOrderID()
			venueOrder, err := client.PlacePrepared(context.Background(), order, prepared)
			if err != nil || posts != 1 || venueOrder.ID != order.VenueOrderID {
				t.Fatalf("place = %#v, err = %v, posts = %d", venueOrder, err, posts)
			}
			wantType := "FOK"
			if tif == domain.TimeInForceIOC {
				wantType = "GTC"
			}
			if posted.OrderType != wantType || posted.Order.Side != "SELL" ||
				posted.Order.MakerAmount != "250000" || posted.Order.TakerAmount != "125000" {
				t.Fatalf("incorrect SELL payload: %#v", posted)
			}
		})
	}
}

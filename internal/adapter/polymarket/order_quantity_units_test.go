package polymarket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestKnownOrderDisambiguatesHumanShareIntegers(t *testing.T) {
	for _, test := range []struct{ name, original, matched, want string }{
		{"human partial", "48", "19.01", "19.01"},
		{"human full integer", "48", "48", "48"},
		{"base units partial", "48000000", "19010000", "19.01"},
		{"base units full", "48000000", "48000000", "48"},
		{"documented original decimal match", "48000000", "19.01", "19.01"},
	} {
		t.Run(test.name, func(t *testing.T) {
			order := pairedMakerOrder()
			wire := map[string]any{"id": order.VenueOrderID, "market": order.Intent.ConditionID, "asset_id": order.Intent.TokenID, "side": "SELL", "status": "LIVE", "original_size": test.original, "size_matched": test.matched, "price": "0.042"}
			payload, _ := json.Marshal(wire)
			var raw rawOrder
			if err := json.Unmarshal(payload, &raw); err != nil {
				t.Fatal(err)
			}
			observed, err := normalizeRawOrder(raw, order, time.Now().UTC())
			if err != nil || !observed.FilledSize.Equal(domain.Decimal(test.want)) {
				t.Fatalf("observation=%#v err=%v", observed, err)
			}
			if test.name == "human partial" {
				for _, mutate := range []func(*domain.Order){
					func(o *domain.Order) { o.VenueOrderID = "another-order" }, func(o *domain.Order) { o.Intent.ConditionID = "another-market" },
					func(o *domain.Order) { o.Intent.TokenID = "another-token" }, func(o *domain.Order) { o.Intent.Side = domain.SideBuy },
					func(o *domain.Order) { o.Intent.Size = "49" },
				} {
					wrong := order
					mutate(&wrong)
					normalized := raw.withKnownOrderQuantityUnits(wrong)
					if !normalized.OriginalSize.Equal("0.000048") {
						t.Fatal("untrusted identity changed quantity units")
					}
				}
			}
		})
	}
}

func TestGetHumanIntegerSizeKeepsPartialIOCObservable(t *testing.T) {
	order := pairedMakerOrder()
	var cancelled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/data/order/owned-maker":
			status := "LIVE"
			if cancelled.Load() {
				status = "CANCELED"
			}
			writeTestJSON(w, map[string]any{"id": order.VenueOrderID, "market": order.Intent.ConditionID, "asset_id": order.Intent.TokenID, "side": "SELL", "status": status, "original_size": "48", "size_matched": "19.01", "price": "0.042", "associate_trades": []string{"paired-trade"}})
		case "/order":
			if r.Method != http.MethodDelete {
				t.Error("unexpected mutation")
			}
			cancelled.Store(true)
			writeTestJSON(w, map[string]any{"canceled": []string{order.VenueOrderID}})
		case "/data/trades":
			tr := complementaryMakerFixture(r.URL.Query().Get("maker_address"))
			tr.MakerOrders[0].MatchedAmount = "19.01"
			writeTestJSON(w, map[string]any{"data": []Trade{tr}, "next_cursor": "LTE="})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, time.Now().UTC())
	observed, err := client.Get(context.Background(), order)
	if err != nil || observed.State != port.VenueOrderPartiallyFilled || !observed.FilledSize.Equal("19.01") || !observed.AverageFillPrice.Equal("0.042") {
		t.Fatalf("partial IOC observation=%#v err=%v", observed, err)
	}
	cancelledOrder, err := client.Cancel(context.Background(), order)
	if err != nil || !cancelled.Load() || cancelledOrder.State != port.VenueOrderCancelled || !cancelledOrder.FilledSize.Equal("19.01") {
		t.Fatalf("partial IOC cancellation=%#v err=%v", cancelledOrder, err)
	}
}

func TestHumanShareInterpretationStillRejectsSellOverfill(t *testing.T) {
	order := pairedMakerOrder()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(w, map[string]any{"id": order.VenueOrderID, "market": order.Intent.ConditionID, "asset_id": order.Intent.TokenID, "side": "SELL", "status": "MATCHED", "original_size": "48", "size_matched": "48.01", "price": "0.042"})
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, time.Now().UTC())
	_, err := client.Get(context.Background(), order)
	venueError, ok := err.(*port.VenueError)
	if !ok || venueError.Code != "CLOB_INVALID_ORDER_RESPONSE" || venueError.Kind != port.VenueErrorUnavailable {
		t.Fatalf("invalid SELL observation must retain state as retryable evidence error: %v", err)
	}
}

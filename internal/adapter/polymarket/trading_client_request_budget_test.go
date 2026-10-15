package polymarket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestPlacementRequestBudget pins how many CLOB requests one matched order costs
// the shared token bucket (POLYMARKET_REQUESTS_PER_SECOND). Parallel intent
// delivery multiplies this number by its concurrency, so a change here changes
// the safe concurrency and must be a conscious one.
//
// Place alone: tick-size, neg-risk, POST /order, GET /data/order, and one
// GET /data/trades per trade id the venue reports. The execution service then
// lists the wallet's trades once more to book the fill, and an emulated IOC
// that left a resting remainder adds DELETE /order plus a GET /data/order.
func TestPlacementRequestBudget(t *testing.T) {
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		tradeIDs   []string
		wantPaths  map[string]int
		wantTotals int
	}{
		{
			name:       "one matched trade",
			tradeIDs:   []string{"trade-1"},
			wantPaths:  map[string]int{"/tick-size": 1, "/neg-risk": 1, "/order": 1, "/data/order": 1, "/data/trades": 1},
			wantTotals: 5,
		},
		{
			name:       "three matched trades",
			tradeIDs:   []string{"trade-1", "trade-2", "trade-3"},
			wantPaths:  map[string]int{"/tick-size": 1, "/neg-risk": 1, "/order": 1, "/data/order": 1, "/data/trades": 3},
			wantTotals: 7,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				mu            sync.Mutex
				placedOrderID string
				counts        = map[string]int{}
			)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				path := request.URL.Path
				mu.Lock()
				defer mu.Unlock()
				switch {
				case path == "/version":
					writeTestJSON(writer, map[string]any{"version": 2})
					return
				case path == "/tick-size":
					writeTestJSON(writer, map[string]any{"minimum_tick_size": "0.01"})
				case path == "/neg-risk":
					writeTestJSON(writer, map[string]any{"neg_risk": false})
				case path == "/order":
					var posted postOrderPayload
					if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
						t.Error(err)
					}
					placedOrderID = signedOrderIDForTest(t, posted.Order, false)
					writeTestJSON(writer, map[string]any{
						"success": true, "orderID": placedOrderID, "status": "matched", "tradeIDs": test.tradeIDs,
					})
				case placedOrderID != "" && path == "/data/order/"+placedOrderID:
					path = "/data/order"
					writeTestJSON(writer, map[string]any{
						"id": placedOrderID, "status": "MATCHED", "original_size": "10000000", "size_matched": "10000000",
						"associate_trades": test.tradeIDs,
					})
				case path == "/data/trades":
					writeTestJSON(writer, map[string]any{
						"data": []map[string]any{{
							"id": request.URL.Query().Get("id"), "taker_order_id": placedOrderID, "size": "10", "price": "0.5",
							"status": "MATCHED", "trader_side": "TAKER",
							"maker_address": request.URL.Query().Get("maker_address"),
						}},
						"next_cursor": "LTE=",
					})
				default:
					http.NotFound(writer, request)
					return
				}
				counts[path]++
			}))
			defer server.Close()

			client := newTestTradingClient(t, server.URL, now)
			// The protocol probe happens once per process, not per order.
			if err := client.ensureV2(context.Background()); err != nil {
				t.Fatalf("version probe: %v", err)
			}
			if _, err := client.Place(context.Background(), adapterOrder()); err != nil {
				t.Fatalf("Place() error = %v", err)
			}
			total := 0
			for path, want := range test.wantPaths {
				if counts[path] != want {
					t.Errorf("%s requests = %d, want %d (all: %v)", path, counts[path], want, counts)
				}
				total += counts[path]
			}
			if total != test.wantTotals {
				t.Errorf("total requests = %d, want %d (all: %v)", total, test.wantTotals, counts)
			}
		})
	}
}

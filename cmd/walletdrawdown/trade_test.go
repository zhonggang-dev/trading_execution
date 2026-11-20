package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestDrawdownExecutionAPI(t *testing.T) {
	intent := domain.OrderIntent{ExecutionAccountID: "main", ClientOrderID: "drawdown:1", TimeInForce: domain.TimeInForceFOK}
	for _, tc := range []struct {
		name   string
		status int
		order  domain.Order
		ok     bool
	}{
		{"definitive rejection", http.StatusUnprocessableEntity, domain.Order{ID: "order", Intent: intent, Status: domain.OrderStatusRejected}, true},
		{"unknown accepted", http.StatusAccepted, domain.Order{ID: "order", Intent: intent, Status: domain.OrderStatusUnknown}, true},
		{"gateway failure", http.StatusBadGateway, domain.Order{}, false},
		{"unattributed response", http.StatusCreated, domain.Order{ID: "order"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var submitted domain.OrderIntent
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/orders" ||
					r.Header.Get("Authorization") != "Bearer example-token" ||
					json.NewDecoder(r.Body).Decode(&submitted) != nil ||
					submitted.ClientOrderID != intent.ClientOrderID || submitted.TimeInForce != domain.TimeInForceFOK {
					t.Errorf("unexpected submission: %s %s %+v", r.Method, r.URL.Path, submitted)
				}
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(map[string]any{"data": tc.order})
			}))
			defer server.Close()
			env := map[string]string{"WALLET_DRAWDOWN_EXECUTION_URL": server.URL, "EXECUTION_API_TOKEN": "example-token"}
			trader, err := loadTrader(func(key string) string { return env[key] }, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			order, err := trader.Submit(context.Background(), intent)
			if (err == nil) != tc.ok || (tc.ok && order.Status != tc.order.Status) {
				t.Fatalf("submit = %+v, %v", order, err)
			}
		})
	}
}

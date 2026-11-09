package kalshi

import (
	"errors"
	"net/http"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestMapKalshiHTTPErrorClassifiesShardBalance(t *testing.T) {
	cases := []struct {
		name   string
		method string
		status int
		body   string
		code   string
		kind   port.VenueErrorKind
	}{
		{"unfunded shard on submit", http.MethodPost, http.StatusNotFound,
			`{"error":{"code":"not_found","message":"insufficient shard balance"}}`,
			"KALSHI_INSUFFICIENT_SHARD_BALANCE", port.VenueErrorRejected},
		{"account balance", http.MethodPost, http.StatusBadRequest,
			`{"error":{"code":"insufficient_balance","message":"insufficient balance"}}`,
			"KALSHI_INSUFFICIENT_BALANCE", port.VenueErrorRejected},
		{"missing order", http.MethodGet, http.StatusNotFound,
			`{"error":{"code":"not_found","message":"order not found"}}`,
			"KALSHI_ORDER_NOT_FOUND", port.VenueErrorRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var venueErr *port.VenueError
			if !errors.As(mapKalshiHTTPError(tc.method, tc.status, http.Header{}, []byte(tc.body)), &venueErr) {
				t.Fatal("expected VenueError")
			}
			if venueErr.Code != tc.code || venueErr.Kind != tc.kind {
				t.Fatalf("got code=%s kind=%v, want code=%s kind=%v", venueErr.Code, venueErr.Kind, tc.code, tc.kind)
			}
		})
	}
}

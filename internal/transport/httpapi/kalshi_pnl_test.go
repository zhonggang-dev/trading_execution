package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshipnl"
)

type kalshiPnLTestSource struct{ err error }

func (s kalshiPnLTestSource) Series(context.Context, string) (kalshi.PnLSeries, error) {
	return kalshi.PnLSeries{PeriodIntervalMinutes: 60, ProcessedThrough: time.Unix(100, 0), Points: []kalshi.PnLPoint{{Timestamp: 100, CumulativePnL: "-1.25", PortfolioValue: "9.5"}}}, s.err
}
func newKalshiPnLServer(t *testing.T, upstream error) *Server {
	t.Helper()
	svc, e := kalshipnl.New(kalshipnl.Params{Sources: map[string]kalshipnl.Source{"kalshi:main": kalshiPnLTestSource{upstream}}})
	if e != nil {
		t.Fatal(e)
	}
	s, e := New(Params{Service: baseExecutionService(t), KalshiPnL: svc, APIToken: "execution", ReadOnlyToken: "readonly", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if e != nil {
		t.Fatal(e)
	}
	return s
}

func TestKalshiPnLAuthorizationMethodsAndParameters(t *testing.T) {
	s := newKalshiPnLServer(t, nil)
	path := "/api/v1/kalshi-pnl?execution_account_id=kalshi%3Amain&range=1w"
	for _, tc := range []struct {
		method, path, token string
		status              int
	}{
		{"GET", path, "", 401}, {"GET", path, "execution", 403}, {"POST", path, "readonly", 405}, {"GET", path, "readonly", 200},
		{"GET", "/api/v1/kalshi-pnl/accounts", "readonly", 200},
		{"GET", "/api/v1/kalshi-pnl/accounts?x=1", "readonly", 400},
		{"GET", "/api/v1/kalshi-pnl?execution_account_id=main&range=1w", "readonly", 404},
		{"GET", path + "&range=1d", "readonly", 400},
		{"GET", path + "&start_ts=1", "readonly", 400},
		{"GET", "/api/v1/kalshi-pnl?execution_account_id=kalshi:main&range=custom", "readonly", 400},
		{"GET", "/api/v1/kalshi-pnl?execution_account_id=kalshi:main&range=1w&bad=%zz", "readonly", 400},
		{"GET", "/api/v1/kalshi-pnl?execution_account_id=kalshi:main", "readonly", 400},
	} {
		r := performRequest(t, s, tc.method, tc.path, "", tc.token)
		if r.Code != tc.status {
			t.Fatalf("%s %s got %d body %s", tc.method, tc.path, r.Code, r.Body)
		}
		if tc.status == 200 {
			if r.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing no-store")
			}
			for _, secret := range []string{"official_user_id", "api_key", "private_key", "wallet_address", "/v1/users"} {
				if strings.Contains(r.Body.String(), secret) {
					t.Fatalf("leaked %s", secret)
				}
			}
		}
	}
	r := performRequest(t, s, "GET", path, "", "readonly")
	for _, field := range []string{`"metric":"cumulative_pnl"`, `"period_interval_minutes":60`, `"cumulative_pnl":"-1.25"`, `"market_source":"KALSHI"`, `"processed_through"`} {
		if !strings.Contains(r.Body.String(), field) {
			t.Fatalf("missing %s in %s", field, r.Body)
		}
	}
}

func TestKalshiPnLErrorsStaySafe(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{{context.DeadlineExceeded, 504, "KALSHI_PNL_TIMEOUT"}, {context.Canceled, 503, "KALSHI_PNL_CANCELED"}, {errors.New("secret raw body token credentials"), 502, "KALSHI_PNL_UNAVAILABLE"}} {
		s := newKalshiPnLServer(t, tc.err)
		r := performRequest(t, s, http.MethodGet, "/api/v1/kalshi-pnl?execution_account_id=kalshi:main&range=1w", "", "readonly")
		if r.Code != tc.status || !strings.Contains(r.Body.String(), tc.code) || strings.Contains(r.Body.String(), "secret") {
			t.Fatalf("status=%d body=%s", r.Code, r.Body)
		}
	}
	s, e := New(Params{Service: baseExecutionService(t)})
	if e != nil {
		t.Fatal(e)
	}
	r := performRequest(t, s, "GET", "/api/v1/kalshi-pnl/accounts", "", "")
	if r.Code != 503 {
		t.Fatalf("unconfigured=%d", r.Code)
	}
}

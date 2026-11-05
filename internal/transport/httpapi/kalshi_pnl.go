package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshipnl"
)

func (s *Server) kalshiPnLHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	q, err := parseKalshiPnLQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, 400, "INVALID_KALSHI_PNL_FILTER", "execution_account_id and a supported range must be provided once")
		return
	}
	if s.kalshiPnL == nil {
		writeError(w, 503, "KALSHI_PNL_UNAVAILABLE", "Kalshi PnL is not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	data, err := s.kalshiPnL.Query(ctx, q)
	if err != nil {
		writeKalshiPnLError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"data": data})
}

func (s *Server) kalshiPnLAccountsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.RawQuery != "" {
		writeError(w, 400, "INVALID_KALSHI_PNL_FILTER", "Kalshi PnL account discovery does not accept query parameters")
		return
	}
	if s.kalshiPnL == nil {
		writeError(w, 503, "KALSHI_PNL_UNAVAILABLE", "Kalshi PnL is not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	data, err := s.kalshiPnL.ListAccounts(ctx)
	if err != nil {
		writeKalshiPnLError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"data": data})
}

func parseKalshiPnLQuery(raw string) (kalshipnl.Query, error) {
	q, err := url.ParseQuery(raw)
	if err != nil {
		return kalshipnl.Query{}, err
	}
	if len(q) != 2 {
		return kalshipnl.Query{}, errors.New("invalid query")
	}
	for k, v := range q {
		if (k != "execution_account_id" && k != "range") || len(v) != 1 {
			return kalshipnl.Query{}, errors.New("invalid query")
		}
	}
	id := q.Get("execution_account_id")
	if id == "" || len(id) > 128 || strings.TrimSpace(id) != id || strings.ContainsAny(id, "\r\n\t\x00") || !kalshi.IsValidPnLRange(q.Get("range")) {
		return kalshipnl.Query{}, errors.New("invalid query")
	}
	return kalshipnl.Query{ExecutionAccountID: id, Range: q.Get("range")}, nil
}

func writeKalshiPnLError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, 504, "KALSHI_PNL_TIMEOUT", "Kalshi PnL request timed out")
	case errors.Is(err, context.Canceled):
		writeError(w, 503, "KALSHI_PNL_CANCELED", "Kalshi PnL request was canceled")
	case errors.Is(err, kalshipnl.ErrUnknownAccount):
		writeError(w, 404, "KALSHI_PNL_ACCOUNT_NOT_FOUND", "Kalshi PnL account is not available")
	default:
		writeError(w, 502, "KALSHI_PNL_UNAVAILABLE", "Kalshi PnL is temporarily unavailable")
	}
}

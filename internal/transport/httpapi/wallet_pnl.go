package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/walletpnl"
)

func (server *Server) walletPnLHandler(writer http.ResponseWriter, request *http.Request) {
	if server.walletPnL == nil {
		writeError(writer, http.StatusServiceUnavailable, "WALLET_PNL_UNAVAILABLE", "wallet PnL is not configured")
		return
	}
	query, err := parseWalletPnLQuery(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_WALLET_PNL_FILTER", err.Error())
		return
	}
	// The BFF waits at most ten seconds for upstream reads. Keep this service
	// budget lower so a slow public Data API cannot outlive that proxy request.
	ctx, cancel := context.WithTimeout(request.Context(), 8*time.Second)
	defer cancel()
	result, err := server.walletPnL.Query(ctx, query)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			writeError(writer, http.StatusGatewayTimeout, "WALLET_PNL_TIMEOUT", "wallet PnL request timed out")
		case errors.Is(err, context.Canceled):
			writeError(writer, http.StatusServiceUnavailable, "WALLET_PNL_CANCELED", "wallet PnL request was canceled")
		case errors.Is(err, walletpnl.ErrUnknownAccount):
			writeError(writer, http.StatusNotFound, "WALLET_PNL_ACCOUNT_NOT_FOUND", "wallet PnL account is not available")
		case errors.Is(err, walletpnl.ErrUnavailable):
			server.logger.Error("wallet pnl query failed", "error", err)
			writeError(writer, http.StatusBadGateway, "WALLET_PNL_UNAVAILABLE", "wallet PnL is temporarily unavailable")
		default:
			writeError(writer, http.StatusBadRequest, "INVALID_WALLET_PNL_FILTER", err.Error())
		}
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": result})
}

func (server *Server) walletPnLAccountsHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	if request.URL.RawQuery != "" {
		writeError(writer, http.StatusBadRequest, "INVALID_WALLET_PNL_FILTER", "wallet PnL account discovery does not accept query parameters")
		return
	}
	if server.walletPnL == nil {
		writeError(writer, http.StatusServiceUnavailable, "WALLET_PNL_UNAVAILABLE", "wallet PnL is not configured")
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 8*time.Second)
	defer cancel()
	result, err := server.walletPnL.ListAccounts(ctx)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			writeError(writer, http.StatusGatewayTimeout, "WALLET_PNL_TIMEOUT", "wallet PnL account discovery timed out")
		case errors.Is(err, context.Canceled):
			writeError(writer, http.StatusServiceUnavailable, "WALLET_PNL_CANCELED", "wallet PnL account discovery was canceled")
		default:
			server.logger.Error("wallet pnl account discovery failed", "error", err)
			writeError(writer, http.StatusServiceUnavailable, "WALLET_PNL_UNAVAILABLE", "wallet PnL account discovery is temporarily unavailable")
		}
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": result})
}

func parseWalletPnLQuery(request *http.Request) (walletpnl.Query, error) {
	query := request.URL.Query()
	allowed := map[string]bool{"execution_account_id": true, "interval": true, "fidelity": true}
	for key, values := range query {
		if !allowed[key] {
			return walletpnl.Query{}, errors.New("unsupported query parameter")
		}
		if len(values) != 1 {
			return walletpnl.Query{}, errors.New("query parameter must be provided once")
		}
	}
	result := walletpnl.Query{
		ExecutionAccountID: strings.TrimSpace(query.Get("execution_account_id")),
		Interval:           strings.TrimSpace(query.Get("interval")), Fidelity: strings.TrimSpace(query.Get("fidelity")),
	}
	if result.ExecutionAccountID == "" || result.Interval == "" || result.Fidelity == "" {
		return walletpnl.Query{}, errors.New("execution_account_id, interval, and fidelity are required")
	}
	if !walletpnl.IsValidInterval(result.Interval) || !walletpnl.IsValidFidelity(result.Fidelity) {
		return walletpnl.Query{}, errors.New("interval or fidelity is invalid")
	}
	return result, nil
}

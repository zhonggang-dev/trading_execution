package httpapi

import (
	"errors"
	"fmt"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
	"net/http"
	"net/url"
)

func parseCoreMetricQuery(r *http.Request) (url.Values, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	for key, values := range query {
		if key != "execution_account_id" || len(values) != 1 || values[0] == "" {
			return nil, fmt.Errorf("invalid core metric filter")
		}
	}
	if err = consolev2.ValidatePerformanceAccountID(query.Get("execution_account_id")); err != nil {
		return nil, err
	}
	return query, nil
}
func (server *Server) consoleV2AccountCoreMetrics(w http.ResponseWriter, r *http.Request) {
	query, err := parseCoreMetricQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", "execution_account_id is invalid")
		return
	}
	if server.accountCoreMetrics == nil {
		server.writeConsoleV2Result(w, nil, consolev2.ErrUnavailable)
		return
	}
	result, err := server.accountCoreMetrics.Query(r.Context(), query.Get("execution_account_id"))
	if errors.Is(err, consolev2.ErrInvalidPerformanceAccount) {
		writeError(w, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", "execution_account_id is invalid")
		return
	}
	server.writeConsoleV2Result(w, result, err)
}

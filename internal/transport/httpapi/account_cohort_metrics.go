package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

func parseCohortQuery(r *http.Request) (consolev2.CohortFilter, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return consolev2.CohortFilter{}, err
	}
	for k, v := range q {
		if (k != "execution_account_id" && k != "account_scope" && k != "opening_from" && k != "opening_to") || len(v) != 1 || v[0] == "" {
			return consolev2.CohortFilter{}, consolev2.ErrInvalidPerformanceAccount
		}
	}
	filter := consolev2.CohortFilter{ExecutionAccountID: q.Get("execution_account_id"), AccountScope: q.Get("account_scope")}
	for k, target := range map[string]**time.Time{"opening_from": &filter.OpeningFrom, "opening_to": &filter.OpeningTo} {
		if value := q.Get(k); value != "" {
			at, e := time.Parse(time.RFC3339Nano, value)
			if e != nil {
				return filter, consolev2.ErrInvalidPerformanceAccount
			}
			at = at.UTC()
			*target = &at
		}
	}
	return filter, consolev2.ValidateCohortFilter(filter)
}

func (server *Server) consoleV2AccountCohortMetrics(w http.ResponseWriter, r *http.Request) {
	filter, err := parseCohortQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", "opening cohort filter is invalid")
		return
	}
	if server.accountCoreMetrics == nil {
		server.writeConsoleV2Result(w, nil, consolev2.ErrUnavailable)
		return
	}
	service, ok := server.accountCoreMetrics.(interface {
		QueryCohort(context.Context, consolev2.CohortFilter) (accountcoremetrics.CohortResponse, error)
	})
	if !ok {
		server.writeConsoleV2Result(w, nil, consolev2.ErrUnavailable)
		return
	}
	result, err := service.QueryCohort(r.Context(), filter)
	if errors.Is(err, consolev2.ErrInvalidPerformanceAccount) {
		writeError(w, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", "opening cohort filter is invalid")
		return
	}
	server.writeConsoleV2Result(w, result, err)
}

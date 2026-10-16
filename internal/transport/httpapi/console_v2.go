package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type consoleV2FilterOptions struct {
	Model        bool
	Strategy     bool
	Account      bool
	MarketSource map[string]struct{}
	Status       map[string]struct{}
	Side         bool
}

const consoleV2ReadTimeout = 5 * time.Second

func (server *Server) registerConsoleV2Routes(mux *http.ServeMux) {
	readOnly := func(handler http.HandlerFunc) http.Handler {
		return server.authenticateReadOnly(server.withConsoleV2ReadDeadline(handler))
	}
	mux.Handle("GET /api/v1/console/v2/runtime", readOnly(server.consoleV2Runtime))
	mux.Handle("GET /api/v1/console/v2/decision-cycles", readOnly(server.consoleV2DecisionCycles))
	mux.Handle("GET /api/v1/console/v2/decision-cycles/{cycle_id}", readOnly(server.consoleV2DecisionCycle))
	mux.Handle("GET /api/v1/console/v2/orders", readOnly(server.consoleV2Orders))
	mux.Handle("GET /api/v1/console/v2/orders/{order_id}/trace", readOnly(server.consoleV2OrderTrace))
	mux.Handle("GET /api/v1/console/v2/position-lots", readOnly(server.consoleV2PositionLots))
	mux.Handle("GET /api/v1/console/v2/reconciliation-runs", readOnly(server.consoleV2ReconciliationRuns))
	mux.Handle("GET /api/v1/console/v2/reconciliation-issues", readOnly(server.consoleV2ReconciliationIssues))
	mux.Handle("GET /api/v1/console/v2/redemptions", readOnly(server.consoleV2Redemptions))
	mux.Handle("GET /api/v1/console/v2/service-metrics", readOnly(server.consoleV2ServiceMetrics))
}

// withConsoleV2ReadDeadline bounds only the new v2 read namespace. A caller
// that already has a shorter deadline keeps it because context.WithTimeout
// preserves the earliest parent deadline; the derived context is the one
// passed through the handlers into the reader and database calls.
func (server *Server) withConsoleV2ReadDeadline(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), consoleV2ReadTimeout)
		defer cancel()
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func (server *Server) consoleV2Runtime(writer http.ResponseWriter, request *http.Request) {
	if err := rejectConsoleV2Query(request); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.Runtime(request.Context())
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2DecisionCycles(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseConsoleV2Filter(request, consoleV2FilterOptions{
		Model: true, Strategy: true, Account: true,
		Status: map[string]struct{}{"INPUT_RECORDED": {}, "OUTPUT_RECORDED": {}, "DELIVERY_PENDING": {}, "DELIVERY_ATTENTION": {}},
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.DecisionCycles(request.Context(), filter)
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2DecisionCycle(writer http.ResponseWriter, request *http.Request) {
	if err := rejectConsoleV2Query(request); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.DecisionCycle(request.Context(), request.PathValue("cycle_id"))
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2Orders(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseConsoleV2Filter(request, consoleV2FilterOptions{
		Model: true, Strategy: true, Account: true, MarketSource: map[string]struct{}{"POLYMARKET": {}, "KALSHI": {}}, Side: true,
		Status: map[string]struct{}{
			"RECEIVED": {}, "VALIDATING": {}, "RESERVED": {}, "SUBMITTING": {},
			"ACKNOWLEDGED": {}, "LIVE": {}, "PARTIALLY_FILLED": {}, "FILLED": {},
			"REJECTED": {}, "UNKNOWN": {}, "CANCEL_PENDING": {}, "CANCELLED": {},
			"RECONCILING": {}, "MANUAL_REVIEW": {},
		},
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.Orders(request.Context(), filter)
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2OrderTrace(writer http.ResponseWriter, request *http.Request) {
	if err := rejectConsoleV2Query(request); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.OrderTrace(request.Context(), request.PathValue("order_id"))
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2PositionLots(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseConsoleV2Filter(request, consoleV2FilterOptions{
		Model: true, Strategy: true, Account: true, MarketSource: map[string]struct{}{"POLYMARKET": {}, "KALSHI": {}},
		Status: map[string]struct{}{"OPEN": {}, "SETTLED_PENDING_REDEEM": {}, "CLOSED": {}},
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.PositionLots(request.Context(), filter)
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2ReconciliationRuns(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseConsoleV2Filter(request, consoleV2FilterOptions{
		Account: true,
		Status:  map[string]struct{}{"RUNNING": {}, "COMPLETED": {}, "ATTENTION_REQUIRED": {}, "FAILED": {}},
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.ReconciliationRuns(request.Context(), filter)
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2ReconciliationIssues(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseConsoleV2Filter(request, consoleV2FilterOptions{
		Account: true,
		Status:  map[string]struct{}{"OPEN": {}, "RESOLVED": {}},
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.ReconciliationIssues(request.Context(), filter)
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2Redemptions(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseConsoleV2Filter(request, consoleV2FilterOptions{
		Account: true,
		Status: map[string]struct{}{
			"READY": {}, "APPROVAL_SUBMITTING": {}, "APPROVAL_SUBMITTED": {},
			"REDEEM_SUBMITTING": {}, "REDEEM_SUBMITTED": {}, "CONFIRMED": {},
			"APPLIED": {}, "MANUAL_REVIEW": {},
		},
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.Redemptions(request.Context(), filter)
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) consoleV2ServiceMetrics(writer http.ResponseWriter, request *http.Request) {
	if err := rejectConsoleV2Query(request); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_CONSOLE_V2_FILTER", err.Error())
		return
	}
	value, err := server.consoleV2.ServiceMetrics(request.Context())
	server.writeConsoleV2Result(writer, value, err)
}

func (server *Server) writeConsoleV2Result(writer http.ResponseWriter, value any, err error) {
	writer.Header().Set("Cache-Control", "no-store")
	if err == nil {
		writeJSON(writer, http.StatusOK, map[string]any{"data": value})
		return
	}
	if errors.Is(err, consolev2.ErrNotFound) {
		writeError(writer, http.StatusNotFound, "CONSOLE_V2_NOT_FOUND", "requested trading identity was not found")
		return
	}
	if errors.Is(err, consolev2.ErrUnavailable) {
		writeError(writer, http.StatusServiceUnavailable, "CONSOLE_V2_UNAVAILABLE", "trading console v2 data is temporarily unavailable")
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		writeError(writer, http.StatusServiceUnavailable, "CONSOLE_V2_UNAVAILABLE", "trading console v2 data is temporarily unavailable")
		return
	}
	server.logger.Error("console v2 read failed", "error", err)
	writeError(writer, http.StatusServiceUnavailable, "CONSOLE_V2_UNAVAILABLE", "trading console v2 data is temporarily unavailable")
}

func rejectConsoleV2Query(request *http.Request) error {
	if len(request.URL.Query()) != 0 {
		return fmt.Errorf("query parameters are not supported for this resource")
	}
	return nil
}

func parseConsoleV2Filter(request *http.Request, options consoleV2FilterOptions) (consolev2.ListFilter, error) {
	query := request.URL.Query()
	allowed := map[string]bool{
		"limit": true, "offset": true, "q": true, "from": true, "to": true,
	}
	if options.Model {
		allowed["model_id"] = true
		allowed["model"] = true
	}
	if options.Strategy {
		allowed["strategy_id"] = true
		allowed["strategy"] = true
	}
	if options.Account {
		allowed["execution_account_id"] = true
		allowed["account"] = true
	}
	if options.MarketSource != nil {
		allowed["market_source"] = true
		allowed["source"] = true
	}
	if options.Status != nil {
		allowed["status"] = true
	}
	if options.Side {
		allowed["side"] = true
	}
	for key, values := range query {
		if !allowed[key] {
			return consolev2.ListFilter{}, fmt.Errorf("unsupported query parameter %q", key)
		}
		if len(values) != 1 {
			return consolev2.ListFilter{}, fmt.Errorf("query parameter %q must be provided once", key)
		}
	}
	modelID, err := consoleV2FilterAlias(query, "model_id", "model")
	if err != nil {
		return consolev2.ListFilter{}, err
	}
	strategyID, err := consoleV2FilterAlias(query, "strategy_id", "strategy")
	if err != nil {
		return consolev2.ListFilter{}, err
	}
	accountID, err := consoleV2FilterAlias(query, "execution_account_id", "account")
	if err != nil {
		return consolev2.ListFilter{}, err
	}
	marketSource, err := consoleV2FilterAlias(query, "market_source", "source")
	if err != nil {
		return consolev2.ListFilter{}, err
	}
	filter := consolev2.ListFilter{Limit: 20, Query: strings.TrimSpace(query.Get("q")),
		ModelID: strings.TrimSpace(modelID), StrategyID: strings.TrimSpace(strategyID),
		ExecutionAccountID: strings.TrimSpace(accountID), MarketSource: strings.TrimSpace(marketSource),
		Status: strings.TrimSpace(query.Get("status")), Side: strings.TrimSpace(query.Get("side"))}
	if raw := query.Get("limit"); raw != "" {
		filter.Limit, err = strconv.Atoi(raw)
		if err != nil {
			return consolev2.ListFilter{}, fmt.Errorf("limit must be an integer")
		}
	}
	if raw := query.Get("offset"); raw != "" {
		filter.Offset, err = strconv.Atoi(raw)
		if err != nil {
			return consolev2.ListFilter{}, fmt.Errorf("offset must be an integer")
		}
	}
	if filter.From, err = parseOptionalRFC3339(query.Get("from")); err != nil {
		return consolev2.ListFilter{}, fmt.Errorf("from must be RFC3339: %w", err)
	}
	if filter.To, err = parseOptionalRFC3339(query.Get("to")); err != nil {
		return consolev2.ListFilter{}, fmt.Errorf("to must be RFC3339: %w", err)
	}
	if filter.Limit < 1 || filter.Limit > 100 {
		return consolev2.ListFilter{}, fmt.Errorf("limit must be between 1 and 100")
	}
	if filter.Offset < 0 || filter.Offset > 10000 {
		return consolev2.ListFilter{}, fmt.Errorf("offset must be between 0 and 10000")
	}
	for key, value := range map[string]string{
		"q": filter.Query, "model_id": filter.ModelID, "strategy_id": filter.StrategyID,
		"execution_account_id": filter.ExecutionAccountID, "market_source": filter.MarketSource,
	} {
		if len(value) > 128 || strings.ContainsAny(value, "\x00\r\n") {
			return consolev2.ListFilter{}, fmt.Errorf("%s is invalid", key)
		}
	}
	if options.Status != nil && filter.Status != "" {
		if _, ok := options.Status[filter.Status]; !ok {
			return consolev2.ListFilter{}, fmt.Errorf("status %q is not supported", filter.Status)
		}
	}
	if options.MarketSource != nil && filter.MarketSource != "" {
		if _, ok := options.MarketSource[filter.MarketSource]; !ok {
			return consolev2.ListFilter{}, fmt.Errorf("market_source %q is not supported", filter.MarketSource)
		}
	}
	if options.Side && filter.Side != "" && filter.Side != "BUY" && filter.Side != "SELL" {
		return consolev2.ListFilter{}, fmt.Errorf("side must be BUY or SELL")
	}
	if filter.From != nil && filter.To != nil && filter.To.Before(*filter.From) {
		return consolev2.ListFilter{}, fmt.Errorf("to must not be before from")
	}
	if filter.From != nil && filter.From.Before(time.Now().UTC().Add(-366*24*time.Hour)) {
		return consolev2.ListFilter{}, fmt.Errorf("from must be within the bounded one-year query window")
	}
	return filter, nil
}

func consoleV2FilterAlias(query map[string][]string, longName, shortName string) (string, error) {
	longValues, hasLong := query[longName]
	shortValues, hasShort := query[shortName]
	if hasLong && hasShort {
		return "", fmt.Errorf("use only one of %q and %q", longName, shortName)
	}
	if hasShort {
		return shortValues[0], nil
	}
	if hasLong {
		return longValues[0], nil
	}
	return "", nil
}

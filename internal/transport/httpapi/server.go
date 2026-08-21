package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/buildinfo"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/liveoperations"
	"github.com/UniPat-AI/trading_execution/internal/service/positionexit"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
)

const maxRequestBodyBytes = 1 << 20

// executionService 表示后端使用的 executionService 类型。
type executionService interface {
	Submit(context.Context, domain.OrderIntent) (port.OrderSubmitResult, error)
	Get(context.Context, string) (domain.Order, error)
	Refresh(context.Context, string) (domain.Order, error)
	Cancel(context.Context, string) (domain.Order, error)
	Events(context.Context, string) ([]domain.OrderEvent, error)
	Attempts(context.Context, string) ([]domain.OrderAttempt, error)
}

// positionExitJob 表示后端使用的 positionExitJob 类型。
type positionExitJob interface {
	Run(context.Context, time.Time) (positionexit.RunResult, error)
}

// reconciliationJob 表示后端使用的 reconciliationJob 类型。
type reconciliationJob interface {
	RunAccount(context.Context, reconciliation.RunAccountParams) (reconciliation.Result, error)
}

// tradeHistoryService 表示后端使用的 tradeHistoryService 类型。
type tradeHistoryService interface {
	List(context.Context, domain.TradeHistoryFilter) (domain.TradeHistoryPage, error)
	DailyPnL(context.Context, domain.DailyPnLFilter) (domain.DailyPnLReport, error)
}

// liveOperationsService 提供已经在后台完成聚合的实盘只读快照。
type liveOperationsService interface {
	Snapshot(context.Context) (domain.LiveOperationsSnapshot, error)
}

// readinessChecker 以只读方式检查安全提供请求所需的依赖。
type readinessChecker interface {
	Check(context.Context) error
}

// Params 表示后端使用的 Params 类型。
type Params struct {
	Service          executionService
	PositionExitJob  positionExitJob
	Reconciliation   reconciliationJob
	TradeHistory     tradeHistoryService
	LiveOperations   liveOperationsService
	Readiness        readinessChecker
	ReadinessTimeout time.Duration
	Logger           *slog.Logger
	APIToken         string
	JobToken         string
	ReadOnlyToken    string
}

// Server 表示后端使用的 Server 类型。
type Server struct {
	service          executionService
	positionExitJob  positionExitJob
	reconciliation   reconciliationJob
	tradeHistory     tradeHistoryService
	liveOperations   liveOperationsService
	readinessChecker readinessChecker
	readinessTimeout time.Duration
	logger           *slog.Logger
	apiToken         string
	jobToken         string
	readOnlyToken    string
	handler          http.Handler
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Server, error) {
	if params.Service == nil {
		return nil, fmt.Errorf("HTTP execution service is required")
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	if params.ReadinessTimeout == 0 {
		params.ReadinessTimeout = 2 * time.Second
	}
	if params.ReadinessTimeout < 100*time.Millisecond || params.ReadinessTimeout > 30*time.Second {
		return nil, fmt.Errorf("HTTP readiness timeout must be between 100ms and 30s")
	}
	server := &Server{
		service:          params.Service,
		positionExitJob:  params.PositionExitJob,
		reconciliation:   params.Reconciliation,
		tradeHistory:     params.TradeHistory,
		liveOperations:   params.LiveOperations,
		readinessChecker: params.Readiness,
		readinessTimeout: params.ReadinessTimeout,
		logger:           params.Logger,
		apiToken:         strings.TrimSpace(params.APIToken),
		jobToken:         strings.TrimSpace(params.JobToken),
		readOnlyToken:    strings.TrimSpace(params.ReadOnlyToken),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", server.liveness)
	mux.HandleFunc("GET /health/ready", server.readiness)
	mux.Handle("POST /api/v1/orders", server.authenticate(http.HandlerFunc(server.submit)))
	mux.Handle("GET /api/v1/orders/{order_id}", server.authenticate(http.HandlerFunc(server.get)))
	mux.Handle("POST /api/v1/orders/{order_id}/refresh", server.authenticate(http.HandlerFunc(server.refresh)))
	mux.Handle("POST /api/v1/orders/{order_id}/cancel", server.authenticate(http.HandlerFunc(server.cancel)))
	mux.Handle("GET /api/v1/orders/{order_id}/events", server.authenticate(http.HandlerFunc(server.events)))
	mux.Handle("GET /api/v1/orders/{order_id}/attempts", server.authenticate(http.HandlerFunc(server.attempts)))
	if server.tradeHistory != nil {
		mux.Handle("GET /api/v1/trades", server.authenticate(http.HandlerFunc(server.trades)))
		mux.Handle("GET /api/v1/daily-pnl", server.authenticate(http.HandlerFunc(server.dailyPnL)))
	}
	if server.liveOperations != nil {
		mux.Handle("GET /api/v1/live-operations", server.authenticateReadOnly(http.HandlerFunc(server.getLiveOperations)))
	}
	if server.positionExitJob != nil {
		token := server.jobToken
		if token == "" {
			token = server.apiToken
		}
		mux.Handle("POST /internal/jobs/position-exit-evaluation/run", server.authenticateToken(token, http.HandlerFunc(server.runPositionExitJob)))
	}
	if server.reconciliation != nil {
		token := server.jobToken
		if token == "" {
			token = server.apiToken
		}
		mux.Handle("POST /internal/jobs/reconciliation/run", server.authenticateToken(token, http.HandlerFunc(server.runReconciliation)))
	}
	server.handler = server.recover(server.logRequests(mux))
	return server, nil
}

// Handler 返回完成路由和中间件装配的 HTTP 处理器。
func (server *Server) Handler() http.Handler {
	return server.handler
}

// liveness 返回进程存活检查结果。
func (server *Server) liveness(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{
		"status": "ok", "version": buildinfo.Version, "commit": buildinfo.Commit,
	})
}

// readiness 检查服务依赖，不把依赖健康与进程存活混为一谈。
func (server *Server) readiness(writer http.ResponseWriter, request *http.Request) {
	if server.readinessChecker != nil {
		ctx, cancel := context.WithTimeout(request.Context(), server.readinessTimeout)
		defer cancel()
		if err := server.readinessChecker.Check(ctx); err != nil {
			server.logger.Error("readiness dependency check failed", "error", err)
			writeJSON(writer, http.StatusServiceUnavailable, map[string]string{
				"status": "not_ready", "version": buildinfo.Version, "commit": buildinfo.Commit,
			})
			return
		}
	}
	writeJSON(writer, http.StatusOK, map[string]string{
		"status": "ready", "version": buildinfo.Version, "commit": buildinfo.Commit,
	})
}

// submit 解码订单意图并调用执行服务返回幂等提交结果。
func (server *Server) submit(writer http.ResponseWriter, request *http.Request) {
	var intent domain.OrderIntent
	if err := decodeJSON(writer, request, &intent); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	result, err := server.service.Submit(request.Context(), intent)
	if err != nil {
		if result.Order.ID != "" {
			status := http.StatusUnprocessableEntity
			if result.Order.Status == domain.OrderStatusUnknown || result.Order.Status == domain.OrderStatusManualReview {
				status = http.StatusAccepted
			}
			writeJSON(writer, status, map[string]any{
				"data": result.Order,
				"error": map[string]string{
					"code":    errorCode(err),
					"message": err.Error(),
				},
			})
			return
		}
		server.handleExecutionError(writer, err)
		return
	}
	status := http.StatusCreated
	if !result.Created {
		status = http.StatusOK
	}
	writeJSON(writer, status, map[string]any{
		"data": result.Order,
		"meta": map[string]bool{"idempotent_replay": !result.Created},
	})
}

// get 查询指定订单并写入 HTTP 响应。
func (server *Server) get(writer http.ResponseWriter, request *http.Request) {
	order, err := server.service.Get(request.Context(), request.PathValue("order_id"))
	if err != nil {
		server.handleExecutionError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": order})
}

// refresh 刷新并处理 对应数据。
func (server *Server) refresh(writer http.ResponseWriter, request *http.Request) {
	order, err := server.service.Refresh(request.Context(), request.PathValue("order_id"))
	if err != nil {
		server.handleExecutionError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": order})
}

// cancel 撤销并处理 对应数据。
func (server *Server) cancel(writer http.ResponseWriter, request *http.Request) {
	order, err := server.service.Cancel(request.Context(), request.PathValue("order_id"))
	if err != nil {
		server.handleExecutionError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": order})
}

// events 处理对应的内部或 HTTP 业务请求。
func (server *Server) events(writer http.ResponseWriter, request *http.Request) {
	events, err := server.service.Events(request.Context(), request.PathValue("order_id"))
	if err != nil {
		server.handleExecutionError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": events})
}

// attempts 处理对应的内部或 HTTP 业务请求。
func (server *Server) attempts(writer http.ResponseWriter, request *http.Request) {
	attempts, err := server.service.Attempts(request.Context(), request.PathValue("order_id"))
	if err != nil {
		server.handleExecutionError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": attempts})
}

// trades 查询并返回仅包含已确认且已入账成交的交易历史。
func (server *Server) trades(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseTradeHistoryFilter(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_TRADE_FILTER", err.Error())
		return
	}
	page, err := server.tradeHistory.List(request.Context(), filter)
	if err != nil {
		server.logger.Error("trade history query failed", "error", err)
		writeError(writer, http.StatusBadGateway, "TRADE_HISTORY_UNAVAILABLE", "trade history is temporarily unavailable")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": page})
}

// dailyPnL 返回连续 UTC 日内按执行账户和来源策略归因的净已实现盈亏。
func (server *Server) dailyPnL(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseDailyPnLFilter(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_DAILY_PNL_FILTER", err.Error())
		return
	}
	report, err := server.tradeHistory.DailyPnL(request.Context(), filter)
	if err != nil {
		server.logger.Error("daily pnl query failed", "error", err)
		writeError(writer, http.StatusBadGateway, "DAILY_PNL_UNAVAILABLE", "daily pnl is temporarily unavailable")
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writeJSON(writer, http.StatusOK, map[string]any{"data": report})
}

// getLiveOperations 直接返回后台原子快照，不在请求路径串行调用外部交易接口。
func (server *Server) getLiveOperations(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	snapshot, err := server.liveOperations.Snapshot(request.Context())
	if err == nil {
		writeJSON(writer, http.StatusOK, map[string]any{"data": snapshot})
		return
	}
	requestID := liveOperationsRequestID(request)
	if errors.Is(err, liveoperations.ErrSnapshotUnavailable) {
		server.logger.Warn("live operations snapshot unavailable", "request_id", requestID, "error", err)
		writeLiveOperationsError(writer, http.StatusServiceUnavailable, "LIVE_SNAPSHOT_UNAVAILABLE", "无法获取完整实盘快照", requestID)
		return
	}
	server.logger.Error("live operations snapshot failed", "request_id", requestID, "error", err)
	writeLiveOperationsError(writer, http.StatusInternalServerError, "INTERNAL_ERROR", "实盘快照读取失败", requestID)
}

// parseTradeHistoryFilter 解析并校验交易历史 HTTP 查询参数。
func parseTradeHistoryFilter(request *http.Request) (domain.TradeHistoryFilter, error) {
	query := request.URL.Query()
	allowed := map[string]bool{
		"limit": true, "offset": true, "from": true, "to": true, "side": true,
		"model_id": true, "strategy_id": true, "execution_account_id": true, "q": true,
	}
	for key, values := range query {
		if !allowed[key] {
			return domain.TradeHistoryFilter{}, fmt.Errorf("unsupported query parameter %q", key)
		}
		if len(values) != 1 {
			return domain.TradeHistoryFilter{}, fmt.Errorf("query parameter %q must be provided once", key)
		}
	}
	filter := domain.TradeHistoryFilter{
		Limit: domain.DefaultTradeHistoryLimit,
		Side:  domain.Side(query.Get("side")), ModelID: query.Get("model_id"),
		StrategyID: query.Get("strategy_id"), ExecutionAccountID: query.Get("execution_account_id"),
		Search: query.Get("q"),
	}
	var err error
	if raw := query.Get("limit"); raw != "" {
		filter.Limit, err = strconv.Atoi(raw)
		if err != nil {
			return domain.TradeHistoryFilter{}, fmt.Errorf("limit must be an integer")
		}
	}
	if raw := query.Get("offset"); raw != "" {
		filter.Offset, err = strconv.Atoi(raw)
		if err != nil {
			return domain.TradeHistoryFilter{}, fmt.Errorf("offset must be an integer")
		}
	}
	if filter.From, err = parseOptionalRFC3339(query.Get("from")); err != nil {
		return domain.TradeHistoryFilter{}, fmt.Errorf("from must be RFC3339: %w", err)
	}
	if filter.To, err = parseOptionalRFC3339(query.Get("to")); err != nil {
		return domain.TradeHistoryFilter{}, fmt.Errorf("to must be RFC3339: %w", err)
	}
	filter = filter.Normalize()
	if err := filter.Validate(); err != nil {
		return domain.TradeHistoryFilter{}, err
	}
	return filter, nil
}

// parseDailyPnLFilter 只接受有限天数，日期边界始终由服务端按 UTC 当前日计算。
func parseDailyPnLFilter(request *http.Request) (domain.DailyPnLFilter, error) {
	query := request.URL.Query()
	for key, values := range query {
		if key != "days" {
			return domain.DailyPnLFilter{}, fmt.Errorf("unsupported query parameter %q", key)
		}
		if len(values) != 1 {
			return domain.DailyPnLFilter{}, fmt.Errorf("query parameter %q must be provided once", key)
		}
	}
	filter := domain.DailyPnLFilter{Days: domain.DefaultDailyPnLDays}
	if raw := query.Get("days"); raw != "" {
		var err error
		filter.Days, err = strconv.Atoi(raw)
		if err != nil {
			return domain.DailyPnLFilter{}, fmt.Errorf("days must be an integer")
		}
	}
	filter = filter.Normalize()
	if err := filter.Validate(); err != nil {
		return domain.DailyPnLFilter{}, err
	}
	return filter, nil
}

// parseOptionalRFC3339 将可选 RFC3339 文本解析为 UTC 时间。
func parseOptionalRFC3339(value string) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	parsed = parsed.UTC()
	return &parsed, nil
}

// positionExitJobRequest 表示后端使用的 positionExitJobRequest 类型。
type positionExitJobRequest struct {
	XXLLogID    int64     `json:"xxl_log_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// runPositionExitJob 校验任务触发参数并运行持仓退出任务。
func (server *Server) runPositionExitJob(writer http.ResponseWriter, request *http.Request) {
	var input positionExitJobRequest
	if err := decodeJSON(writer, request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	if input.XXLLogID <= 0 || input.ScheduledAt.IsZero() {
		writeError(writer, http.StatusBadRequest, "INVALID_JOB_TRIGGER", "xxl_log_id and scheduled_at are required")
		return
	}
	result, err := server.positionExitJob.Run(request.Context(), input.ScheduledAt.UTC())
	if err != nil {
		status := http.StatusBadGateway
		code := "POSITION_EXIT_JOB_FAILED"
		if errors.Is(err, positionexit.ErrInvalidBoundary) {
			status = http.StatusBadRequest
			code = "INVALID_SCHEDULE_BOUNDARY"
		}
		writeJSON(writer, status, map[string]any{
			"data":  result,
			"meta":  map[string]any{"xxl_log_id": input.XXLLogID},
			"error": map[string]string{"code": code, "message": err.Error()},
		})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"data": result,
		"meta": map[string]any{"xxl_log_id": input.XXLLogID},
	})
}

// reconciliationJobRequest 表示后端使用的 reconciliationJobRequest 类型。
type reconciliationJobRequest struct {
	ExecutionAccountID string                       `json:"execution_account_id"`
	Trigger            domain.ReconciliationTrigger `json:"trigger"`
	FocusOrderID       string                       `json:"focus_order_id,omitempty"`
}

// runReconciliation 校验任务触发参数并运行账户对账任务。
func (server *Server) runReconciliation(writer http.ResponseWriter, request *http.Request) {
	var input reconciliationJobRequest
	if err := decodeJSON(writer, request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	input.ExecutionAccountID = strings.TrimSpace(input.ExecutionAccountID)
	input.FocusOrderID = strings.TrimSpace(input.FocusOrderID)
	if input.ExecutionAccountID == "" || input.Trigger == "" {
		writeError(writer, http.StatusBadRequest, "INVALID_RECONCILIATION_TRIGGER", "execution_account_id and trigger are required")
		return
	}
	switch input.Trigger {
	case domain.ReconciliationTriggerStartup, domain.ReconciliationTriggerScheduled,
		domain.ReconciliationTriggerOrderUnknown, domain.ReconciliationTriggerCancelUnknown,
		domain.ReconciliationTriggerAssetDrift:
	default:
		writeError(writer, http.StatusBadRequest, "INVALID_RECONCILIATION_TRIGGER", "unsupported reconciliation trigger")
		return
	}
	params := reconciliation.RunAccountParams{ExecutionAccountID: input.ExecutionAccountID, Trigger: input.Trigger, FocusOrderID: input.FocusOrderID}
	result, err := server.reconciliation.RunAccount(request.Context(), params)
	if err != nil {
		writeJSON(writer, http.StatusBadGateway, map[string]any{
			"data": result,
			"error": map[string]string{
				"code": "RECONCILIATION_INCOMPLETE", "message": err.Error(),
			},
		})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"data": result})
}

// handleExecutionError 将执行领域错误映射为稳定的 HTTP 状态和错误码。
func (server *Server) handleExecutionError(writer http.ResponseWriter, err error) {
	var rejection *port.Rejection
	switch {
	case errors.Is(err, execution.ErrInvalidIntent):
		writeError(writer, http.StatusBadRequest, "INVALID_ORDER_INTENT", err.Error())
	case errors.Is(err, execution.ErrIntentExpired):
		writeError(writer, http.StatusUnprocessableEntity, "ORDER_INTENT_EXPIRED", err.Error())
	case errors.Is(err, execution.ErrIdempotencyConflict):
		writeError(writer, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
	case errors.Is(err, port.ErrOrderNotFound):
		writeError(writer, http.StatusNotFound, "ORDER_NOT_FOUND", err.Error())
	case errors.As(err, &rejection):
		writeError(writer, http.StatusUnprocessableEntity, rejection.Code, rejection.Reason)
	default:
		server.logger.Error("execution request failed", "error", err)
		writeError(writer, http.StatusBadGateway, "EXECUTION_FAILED", "execution operation failed")
	}
}

// authenticate 为业务路由添加 Bearer Token 鉴权。
func (server *Server) authenticate(next http.Handler) http.Handler {
	return server.authenticateToken(server.apiToken, next)
}

// authenticateReadOnly 只接受独立只读令牌，执行令牌访问该路由时明确返回无读取权限。
func (server *Server) authenticateReadOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if server.readOnlyToken == "" {
			next.ServeHTTP(writer, request)
			return
		}
		supplied, valid := bearerToken(request)
		if !valid {
			writeLiveOperationsError(writer, http.StatusUnauthorized, "UNAUTHORIZED", "只读 Token 缺失或无效", liveOperationsRequestID(request))
			return
		}
		if secureTokenEqual(supplied, server.readOnlyToken) {
			next.ServeHTTP(writer, request)
			return
		}
		if server.hasNonReadToken(supplied) {
			writeLiveOperationsError(writer, http.StatusForbidden, "READ_PERMISSION_REQUIRED", "Token 没有实盘监控读取权限", liveOperationsRequestID(request))
			return
		}
		writeLiveOperationsError(writer, http.StatusUnauthorized, "UNAUTHORIZED", "只读 Token 缺失或无效", liveOperationsRequestID(request))
	})
}

// hasNonReadToken 判断调用方是否拿着有效但不具备监控读取权限的令牌。
func (server *Server) hasNonReadToken(supplied string) bool {
	return (server.apiToken != "" && secureTokenEqual(supplied, server.apiToken)) ||
		(server.jobToken != "" && secureTokenEqual(supplied, server.jobToken))
}

// authenticateToken 严格校验 Bearer 鉴权方案和令牌后再放行请求。
func (server *Server) authenticateToken(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if token == "" {
			next.ServeHTTP(writer, request)
			return
		}
		scheme, supplied, found := strings.Cut(strings.TrimSpace(request.Header.Get("Authorization")), " ")
		if !found || !strings.EqualFold(scheme, "Bearer") {
			writeError(writer, http.StatusUnauthorized, "UNAUTHORIZED", "valid bearer token is required")
			return
		}
		supplied = strings.TrimSpace(supplied)
		if len(supplied) != len(token) || subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) != 1 {
			writeError(writer, http.StatusUnauthorized, "UNAUTHORIZED", "valid bearer token is required")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

// bearerToken 严格解析 Authorization Bearer 令牌。
func bearerToken(request *http.Request) (string, bool) {
	scheme, supplied, found := strings.Cut(strings.TrimSpace(request.Header.Get("Authorization")), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	supplied = strings.TrimSpace(supplied)
	return supplied, supplied != ""
}

// secureTokenEqual 使用常量时间比较两个令牌。
func secureTokenEqual(left string, right string) bool {
	return len(left) == len(right) && subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

// logRequests 记录 HTTP 请求状态和耗时。
func (server *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(recorder, request)
		server.logger.Info("HTTP request completed",
			"method", request.Method,
			"path", request.URL.Path,
			"status", recorder.status,
			"duration", time.Since(startedAt),
		)
	})
}

// recover 捕获 HTTP 处理 panic 并返回统一错误。
func (server *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				server.logger.Error("HTTP handler panic", "panic", recovered)
				writeError(writer, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
			}
		}()
		next.ServeHTTP(writer, request)
	})
}

// statusRecorder 表示后端使用的 statusRecorder 类型。
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader 记录响应状态码并写入底层响应头。
func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

// decodeJSON 解码并校验 JSON 数据。
func decodeJSON(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("request must contain exactly one JSON object")
	}
	return nil
}

// writeError 写入 Error。
func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

// writeLiveOperationsError 写入需求约定的实盘监控顶层错误结构。
func writeLiveOperationsError(writer http.ResponseWriter, status int, code string, message string, requestID string) {
	writeJSON(writer, status, map[string]string{"code": code, "message": message, "requestId": requestID})
}

// liveOperationsRequestID 复用可信格式的请求标识，缺失时生成不含业务信息的随机标识。
func liveOperationsRequestID(request *http.Request) string {
	if value := strings.TrimSpace(request.Header.Get("X-Request-ID")); value != "" && len(value) <= 128 {
		return value
	}
	random := make([]byte, 12)
	if _, err := rand.Read(random); err == nil {
		return "req-" + hex.EncodeToString(random)
	}
	return "req-unavailable"
}

// errorCode 从领域拒绝或交易所错误中提取稳定错误码。
func errorCode(err error) string {
	var rejection *port.Rejection
	if errors.As(err, &rejection) && rejection.Code != "" {
		return rejection.Code
	}
	var venueError *port.VenueError
	if errors.As(err, &venueError) && venueError.Code != "" {
		return venueError.Code
	}
	if errors.Is(err, execution.ErrIntentExpired) {
		return "ORDER_INTENT_EXPIRED"
	}
	return "EXECUTION_FAILED"
}

// writeJSON 写入 JSON 数据。
func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

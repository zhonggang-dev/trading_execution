package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	"github.com/UniPat-AI/trading_execution/internal/adapter/risk"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/liveoperations"
	"github.com/UniPat-AI/trading_execution/internal/service/positionexit"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
)

type fakeReadiness struct{ err error }

func (checker fakeReadiness) Check(context.Context) error { return checker.err }

// fakeLiveOperationsService 返回预设的只读实盘快照。
type fakeLiveOperationsService struct {
	snapshot domain.LiveOperationsSnapshot
	err      error
}

// Snapshot 返回预设快照或错误。
func (service *fakeLiveOperationsService) Snapshot(context.Context) (domain.LiveOperationsSnapshot, error) {
	return service.snapshot, service.err
}

// TestLiveOperationsEndpointUsesDedicatedReadOnlyPermission 验证只读接口的独立权限和 JSON 数字契约。
func TestLiveOperationsEndpointUsesDedicatedReadOnlyPermission(t *testing.T) {
	operations := &fakeLiveOperationsService{snapshot: testLiveOperationsSnapshot()}
	server, err := New(Params{
		Service: baseExecutionService(t), LiveOperations: operations,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIToken: "execution-secret", ReadOnlyToken: "readonly-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	unauthorized := performRequest(t, server, http.MethodGet, "/api/v1/live-operations", "", "")
	if unauthorized.Code != http.StatusUnauthorized || !strings.Contains(unauthorized.Body.String(), `"requestId"`) {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	forbidden := performRequest(t, server, http.MethodGet, "/api/v1/live-operations", "", "execution-secret")
	if forbidden.Code != http.StatusForbidden || !strings.Contains(forbidden.Body.String(), `"code":"READ_PERMISSION_REQUIRED"`) {
		t.Fatalf("forbidden status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
	response := performRequest(t, server, http.MethodGet, "/api/v1/live-operations", "", "readonly-secret")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"equity":1.25`) ||
		!strings.Contains(response.Body.String(), `"executionAccountId":"wallet-1"`) ||
		!strings.Contains(response.Body.String(), `"return":null`) ||
		!strings.Contains(response.Body.String(), `"warningThreshold":8`) ||
		!strings.Contains(response.Body.String(), `"hardLimit":10`) ||
		!strings.Contains(response.Body.String(), `"limit":10`) ||
		!strings.Contains(response.Body.String(), `"hardLimitEnforced":false`) ||
		!strings.Contains(response.Body.String(), `"thresholdType":"target"`) ||
		!strings.Contains(response.Body.String(), `"positions":[]`) {
		t.Fatalf("response status=%d body=%s", response.Code, response.Body.String())
	}
}

// TestLiveOperationsEndpointReturnsTopLevelUnavailableError 验证可信快照缺失时返回需求约定的 503 结构。
func TestLiveOperationsEndpointReturnsTopLevelUnavailableError(t *testing.T) {
	operations := &fakeLiveOperationsService{err: liveoperations.ErrSnapshotUnavailable}
	server, err := New(Params{
		Service: baseExecutionService(t), LiveOperations: operations,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ReadOnlyToken: "readonly-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/live-operations", nil)
	request.Header.Set("Authorization", "Bearer readonly-secret")
	request.Header.Set("X-Request-ID", "req-test")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	want := `{"code":"LIVE_SNAPSHOT_UNAVAILABLE","message":"无法获取完整实盘快照","requestId":"req-test"}`
	if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != want {
		t.Fatalf("response status=%d body=%s", response.Code, response.Body.String())
	}
}

// testLiveOperationsSnapshot 创建字段可完整 JSON 编码的最小只读快照。
func testLiveOperationsSnapshot() domain.LiveOperationsSnapshot {
	now := time.Date(2026, 8, 19, 8, 0, 0, 0, time.UTC)
	return domain.LiveOperationsSnapshot{
		ObservedAt: now, Engine: domain.LiveEngine{Health: domain.LiveHealthHealthy},
		Capital: domain.LiveCapital{
			Equity: "1.25", AvailableCash: "1.25", GrossExposure: "0", ExposureLimit: "10",
			RealizedPnLToday: "0", UnrealizedPnL: "0", FeeToday: "0",
		},
		Wallets: []domain.LiveWallet{{
			ExecutionAccountID: "wallet-1", PositionCount: 0,
			PeakCashUsed: "0", CumulativeInvestedCost: "0",
			RealizedPnL: "0", UnrealizedPnL: "0", TotalPnL: "0", ReturnRate: nil,
		}},
		Workers: []domain.LiveWorker{}, Funnel: []domain.LiveFunnelStage{}, Risks: []domain.LiveRisk{{
			ID: "exposure", Name: "总敞口", Current: "9", WarningThreshold: "8", HardLimit: "10", Limit: "10",
			HardLimitEnforced: false, ThresholdType: domain.LiveRiskTarget, Unit: "$", Hint: "只读运营目标", State: domain.LiveFlowWarning,
		}},
		Orders: []domain.LiveOrder{}, Positions: []domain.LivePosition{}, Events: []domain.LiveEvent{},
		DataQuality: []domain.LiveDataQuality{},
	}
}

// TestHTTPOrderLifecycleAndAuthentication 验证 HTTP Order Lifecycle And Authentication 场景下的行为。
func TestHTTPOrderLifecycleAndAuthentication(t *testing.T) {
	server := testServer(t, "test-secret")

	health := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	healthResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(healthResponse, health)
	if healthResponse.Code != http.StatusOK {
		t.Fatalf("health status = %d", healthResponse.Code)
	}

	unauthorized := httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(validRequestJSON))
	unauthorizedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(unauthorizedResponse, unauthorized)
	if unauthorizedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want 401", unauthorizedResponse.Code)
	}

	created := performRequest(t, server, http.MethodPost, "/api/v1/orders", validRequestJSON, "test-secret")
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", created.Code, created.Body.String())
	}
	var createBody struct {
		Data domain.Order `json:"data"`
		Meta struct {
			Replay bool `json:"idempotent_replay"`
		} `json:"meta"`
	}
	decodeResponse(t, created, &createBody)
	if createBody.Data.Status != domain.OrderStatusOpen || createBody.Meta.Replay {
		t.Fatalf("create response = %#v", createBody)
	}

	replayed := performRequest(t, server, http.MethodPost, "/api/v1/orders", validRequestJSON, "test-secret")
	if replayed.Code != http.StatusOK || !strings.Contains(replayed.Body.String(), `"idempotent_replay":true`) {
		t.Fatalf("replay status = %d, body = %s", replayed.Code, replayed.Body.String())
	}

	canceled := performRequest(t, server, http.MethodPost, "/api/v1/orders/"+createBody.Data.ID+"/cancel", "", "test-secret")
	if canceled.Code != http.StatusOK || !strings.Contains(canceled.Body.String(), `"status":"CANCELLED"`) {
		t.Fatalf("cancel status = %d, body = %s", canceled.Code, canceled.Body.String())
	}

	events := performRequest(t, server, http.MethodGet, "/api/v1/orders/"+createBody.Data.ID+"/events", "", "test-secret")
	if events.Code != http.StatusOK || !strings.Contains(events.Body.String(), `"to_status":"CANCELLED"`) {
		t.Fatalf("events status = %d, body = %s", events.Code, events.Body.String())
	}
	attempts := performRequest(t, server, http.MethodGet, "/api/v1/orders/"+createBody.Data.ID+"/attempts", "", "test-secret")
	if attempts.Code != http.StatusOK || !strings.Contains(attempts.Body.String(), `"kind":"SUBMIT"`) || !strings.Contains(attempts.Body.String(), `"kind":"CANCEL"`) {
		t.Fatalf("attempts status = %d, body = %s", attempts.Code, attempts.Body.String())
	}
}

// TestReadinessFailsClosed verifies that a dependency failure cannot be
// reported as ready while liveness remains independent.
func TestReadinessFailsClosed(t *testing.T) {
	server, err := New(Params{
		Service: baseExecutionService(t), Readiness: fakeReadiness{err: errors.New("database unavailable")},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	readyRequest := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	readyResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(readyResponse, readyRequest)
	if readyResponse.Code != http.StatusServiceUnavailable || !strings.Contains(readyResponse.Body.String(), `"status":"not_ready"`) {
		t.Fatalf("readiness status = %d, body = %s", readyResponse.Code, readyResponse.Body.String())
	}
	liveRequest := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	liveResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(liveResponse, liveRequest)
	if liveResponse.Code != http.StatusOK {
		t.Fatalf("liveness status = %d", liveResponse.Code)
	}
}

// TestHTTPAuthenticationRequiresBearerScheme 验证鉴权不会把裸令牌或其他认证方案误当成 Bearer Token。
func TestHTTPAuthenticationRequiresBearerScheme(t *testing.T) {
	server := testServer(t, "test-secret")
	for _, authorization := range []string{"test-secret", "Basic test-secret"} {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/orders/missing", nil)
		request.Header.Set("Authorization", authorization)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("authorization %q status = %d, want 401", authorization, response.Code)
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/orders/missing", nil)
	request.Header.Set("Authorization", "bearer test-secret")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("case-insensitive bearer status = %d, want 404", response.Code)
	}
}

// TestHTTPRejectsUnknownFieldsAndNumericDecimals 验证 HTTP Rejects Unknown Fields And Numeric Decimals 场景下的行为。
func TestHTTPRejectsUnknownFieldsAndNumericDecimals(t *testing.T) {
	server := testServer(t, "")
	tests := []string{
		strings.Replace(validRequestJSON, `"size":"10"`, `"size":10`, 1),
		strings.Replace(validRequestJSON, `"strategy_id":"strategy-1"`, `"strategy_id":"strategy-1","probability":0.7`, 1),
	}
	for _, body := range tests {
		response := performRequest(t, server, http.MethodPost, "/api/v1/orders", body, "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
	}
}

// fakePositionExitJob 表示后端使用的 fakePositionExitJob 类型。
type fakePositionExitJob struct {
	decisionAt time.Time
}

// Run 执行测试模拟流程。
func (job *fakePositionExitJob) Run(_ context.Context, decisionAt time.Time) (positionexit.RunResult, error) {
	job.decisionAt = decisionAt
	return positionexit.RunResult{DecisionAt: decisionAt, Runs: []positionexit.BindingRunResult{}}, nil
}

// TestPositionExitJobEndpointUsesDedicatedTokenAndBoundary 验证 Position Exit Job Endpoint Uses Dedicated Token And Boundary 场景下的行为。
func TestPositionExitJobEndpointUsesDedicatedTokenAndBoundary(t *testing.T) {
	job := &fakePositionExitJob{}
	server, err := New(Params{
		Service: baseExecutionService(t), PositionExitJob: job,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), JobToken: "job-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"xxl_log_id":321,"scheduled_at":"2026-08-18T12:00:00Z"}`
	unauthorized := performRequest(t, server, http.MethodPost, "/internal/jobs/position-exit-evaluation/run", body, "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	response := performRequest(t, server, http.MethodPost, "/internal/jobs/position-exit-evaluation/run", body, "job-secret")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"xxl_log_id":321`) {
		t.Fatalf("job response status=%d body=%s", response.Code, response.Body.String())
	}
	if !job.decisionAt.Equal(time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("job decision_at = %s", job.decisionAt)
	}
}

func TestPositionExitJobEndpointRejectsExecutionTokenFallback(t *testing.T) {
	_, err := New(Params{
		Service: baseExecutionService(t), PositionExitJob: &fakePositionExitJob{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), APIToken: "api-secret",
	})
	if err == nil || !strings.Contains(err.Error(), "dedicated job token") {
		t.Fatalf("New() error = %v, want execution-token fallback rejection", err)
	}
}

// fakeReconciliationJob 表示后端使用的 fakeReconciliationJob 类型。
type fakeReconciliationJob struct {
	accountID string
	trigger   domain.ReconciliationTrigger
	orderID   string
}

// fakeTradeHistoryService 表示后端使用的 fakeTradeHistoryService 类型。
type fakeTradeHistoryService struct {
	filter         domain.TradeHistoryFilter
	ledgerFilter   domain.LedgerActivityFilter
	dailyPnLFilter domain.DailyPnLFilter
	calls          int
	ledgerCalls    int
	dailyPnLCalls  int
}

// List 记录交易历史筛选条件并返回模拟成交页面。
func (service *fakeTradeHistoryService) List(_ context.Context, filter domain.TradeHistoryFilter) (domain.TradeHistoryPage, error) {
	service.filter = filter
	service.calls++
	return domain.TradeHistoryPage{
		Items: []domain.TradeRecord{{
			FillKey: "fill-key-1", VenueTradeID: "venue-trade-1", OrderID: "order-1",
			ExecutionAccountID: "account-1", ModelID: "model-v2", StrategyID: "multfactor_v2",
			MarketID: "market-1", TokenID: "token-1", Side: domain.SideSell,
			Shares: "10", Price: "0.62", GrossNotional: "6.2", TotalFee: "0.01",
			NetCashDelta: "6.19", RealizedPnL: "1.19",
			MatchedAt:   time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC),
			ConfirmedAt: time.Date(2026, 8, 18, 12, 1, 0, 0, time.UTC),
		}},
		Summary: domain.TradeHistorySummary{
			TradeCount: 1, BuyNotional: "0", SellNotional: "6.2", NetCashFlow: "6.19", TotalFee: "0.01", RealizedPnL: "1.19",
		},
		Total: 1, Limit: filter.Limit, Offset: filter.Offset,
	}, nil
}

// ListLedgerActivities 记录账本活动筛选条件并返回一条 SELL 成交与一条 REDEEM 赎回。
func (service *fakeTradeHistoryService) ListLedgerActivities(_ context.Context, filter domain.LedgerActivityFilter) (domain.LedgerActivityPage, error) {
	service.ledgerFilter = filter
	service.ledgerCalls++
	return domain.LedgerActivityPage{
		Items: []domain.LedgerActivity{{
			ActivityKey: "redemption:lot-redemption:0xabc:lot-1", ActivityType: domain.LedgerActivityRedeem,
			Venue: "polymarket", ExecutionAccountID: "wallet-6", ModelID: "model-v2", StrategyID: "multfactor_v2",
			MarketID: "market-1", ConditionID: "0xcond", TokenID: "token-1", LotID: "lot-1",
			Shares: "40", TotalFee: "0", NetCashDelta: "40", CostBasis: "2.08", SettlementPayout: "40",
			RealizedPnL: "37.92", TransactionHash: "0xabc",
			OccurredAt:  time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC),
			ConfirmedAt: time.Date(2026, 9, 2, 9, 59, 0, 0, time.UTC),
			AppliedAt:   time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC),
		}, {
			ActivityKey: "fill:fill-key-1", ActivityType: domain.LedgerActivitySell, Venue: "polymarket",
			ExecutionAccountID: "wallet-6", ModelID: "model-v2", StrategyID: "multfactor_v2",
			MarketID: "market-1", TokenID: "token-1", OrderID: "order-1", VenueTradeID: "venue-trade-1",
			LiquidityRole: domain.LiquidityRoleTaker, Shares: "10", Price: "0.62", GrossNotional: "6.2",
			TotalFee: "0.01", NetCashDelta: "6.19", CostBasis: "5", RealizedPnL: "1.19",
			OccurredAt:  time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC),
			ConfirmedAt: time.Date(2026, 8, 18, 12, 1, 0, 0, time.UTC),
			AppliedAt:   time.Date(2026, 8, 18, 12, 1, 0, 0, time.UTC),
		}},
		Summary: domain.LedgerActivitySummary{
			ActivityCount: 2, TradeCount: 1, RedemptionCount: 1, BuyNotional: "0", SellNotional: "6.2",
			RedeemPayout: "40", NetCashFlow: "46.19", TotalFee: "0.01", RealizedPnL: "39.11",
			SellRealizedPnL: "1.19", RedeemRealizedPnL: "37.92",
		},
		Total: 2, Limit: filter.Limit, Offset: filter.Offset,
	}, nil
}

// DailyPnL 记录每日盈亏窗口并返回包含零值与正收益的模拟账户策略序列。
func (service *fakeTradeHistoryService) DailyPnL(_ context.Context, filter domain.DailyPnLFilter) (domain.DailyPnLReport, error) {
	service.dailyPnLFilter = filter
	service.dailyPnLCalls++
	return domain.DailyPnLReport{
		Items: []domain.DailyPnLPoint{{
			Day: "2026-08-21", ExecutionAccountID: "account-1", ModelID: "model-v2",
			StrategyID: "multfactor_v2", RealizedPnL: "1.19", ClosedTradeCount: 1, ClosedShares: "10",
		}},
		Days: filter.Days, FromDay: "2026-08-08", ToDay: "2026-08-21",
		Timezone: "UTC", GeneratedAt: time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
	}, nil
}

// TestTradeHistoryEndpointAuthenticatesAndValidatesFilters 验证交易历史接口鉴权并校验筛选参数。
func TestTradeHistoryEndpointAuthenticatesAndValidatesFilters(t *testing.T) {
	history := &fakeTradeHistoryService{}
	server, err := New(Params{
		Service: baseExecutionService(t), TradeHistory: history,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), APIToken: "console-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	unauthorized := performRequest(t, server, http.MethodGet, "/api/v1/trades", "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	invalid := performRequest(t, server, http.MethodGet, "/api/v1/trades?limit=101", "", "console-secret")
	if invalid.Code != http.StatusBadRequest || history.calls != 0 {
		t.Fatalf("invalid status=%d calls=%d body=%s", invalid.Code, history.calls, invalid.Body.String())
	}
	response := performRequest(t, server, http.MethodGet,
		"/api/v1/trades?limit=25&offset=5&side=sell&model_id=model-v2&strategy_id=multfactor_v2&execution_account_id=account-1&from=2026-08-18T00:00:00Z&q=venue-trade",
		"", "console-secret")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"realized_pnl":"1.19"`) {
		t.Fatalf("trade response status=%d body=%s", response.Code, response.Body.String())
	}
	if history.calls != 1 || history.filter.Side != domain.SideSell || history.filter.Limit != 25 || history.filter.Offset != 5 || history.filter.From == nil {
		t.Fatalf("captured filter = %#v, calls=%d", history.filter, history.calls)
	}
}

// TestLedgerActivitiesEndpointReturnsRedeemRowsWithoutTradeFields 验证统一账本接口鉴权、校验活动类型，
// 且 REDEEM 行不携带成交价、订单 ID 与流动性角色。
func TestLedgerActivitiesEndpointReturnsRedeemRowsWithoutTradeFields(t *testing.T) {
	history := &fakeTradeHistoryService{}
	server, err := New(Params{
		Service: baseExecutionService(t), TradeHistory: history,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), APIToken: "console-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	unauthorized := performRequest(t, server, http.MethodGet, "/api/v1/ledger-activities", "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	for _, path := range []string{"/api/v1/ledger-activities?activity_type=HOLD", "/api/v1/ledger-activities?side=SELL", "/api/v1/ledger-activities?limit=101"} {
		invalid := performRequest(t, server, http.MethodGet, path, "", "console-secret")
		if invalid.Code != http.StatusBadRequest || history.ledgerCalls != 0 {
			t.Fatalf("%s status=%d calls=%d body=%s", path, invalid.Code, history.ledgerCalls, invalid.Body.String())
		}
	}
	response := performRequest(t, server, http.MethodGet,
		"/api/v1/ledger-activities?limit=25&offset=5&activity_type=redeem&model_id=model-v2&strategy_id=multfactor_v2&execution_account_id=wallet-6&from=2026-09-01T00:00:00Z&q=0xabc",
		"", "console-secret")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Data struct {
			Items   []map[string]any `json:"items"`
			Summary map[string]any   `json:"summary"`
			Total   int64            `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Data.Total != 2 || len(payload.Data.Items) != 2 {
		t.Fatalf("payload = %s", response.Body.String())
	}
	redeem := payload.Data.Items[0]
	if redeem["activity_type"] != "REDEEM" || redeem["realized_pnl"] != "37.92" || redeem["settlement_payout"] != "40" || redeem["cost_basis"] != "2.08" || redeem["transaction_hash"] != "0xabc" {
		t.Fatalf("redeem row = %#v", redeem)
	}
	for _, absent := range []string{"price", "order_id", "venue_order_id", "venue_trade_id", "liquidity_role", "order_status", "gross_notional"} {
		if _, present := redeem[absent]; present {
			t.Fatalf("redeem row unexpectedly carries %q: %#v", absent, redeem)
		}
	}
	if payload.Data.Items[1]["price"] != "0.62" || payload.Data.Items[1]["liquidity_role"] != "TAKER" {
		t.Fatalf("sell row = %#v", payload.Data.Items[1])
	}
	if payload.Data.Summary["realized_pnl"] != "39.11" || payload.Data.Summary["redeem_payout"] != "40" || payload.Data.Summary["sell_notional"] != "6.2" {
		t.Fatalf("summary = %#v", payload.Data.Summary)
	}
	if history.ledgerCalls != 1 || history.ledgerFilter.ActivityType != domain.LedgerActivityRedeem || history.ledgerFilter.Limit != 25 ||
		history.ledgerFilter.Offset != 5 || history.ledgerFilter.From == nil || history.ledgerFilter.Search != "0xabc" {
		t.Fatalf("captured filter = %#v, calls=%d", history.ledgerFilter, history.ledgerCalls)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("cache-control = %q", response.Header().Get("Cache-Control"))
	}
}

// TestDailyPnLEndpointAuthenticatesAndValidatesWindow 验证每日盈亏只接受安全天数并复用账本读取权限。
func TestDailyPnLEndpointAuthenticatesAndValidatesWindow(t *testing.T) {
	history := &fakeTradeHistoryService{}
	server, err := New(Params{
		Service: baseExecutionService(t), TradeHistory: history,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), APIToken: "console-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	unauthorized := performRequest(t, server, http.MethodGet, "/api/v1/daily-pnl?days=14", "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	invalid := performRequest(t, server, http.MethodGet, "/api/v1/daily-pnl?days=91", "", "console-secret")
	if invalid.Code != http.StatusBadRequest || history.dailyPnLCalls != 0 {
		t.Fatalf("invalid status=%d calls=%d body=%s", invalid.Code, history.dailyPnLCalls, invalid.Body.String())
	}
	response := performRequest(t, server, http.MethodGet, "/api/v1/daily-pnl?days=14", "", "console-secret")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"realized_pnl":"1.19"`) {
		t.Fatalf("daily pnl response status=%d body=%s", response.Code, response.Body.String())
	}
	if history.dailyPnLCalls != 1 || history.dailyPnLFilter.Days != 14 || history.dailyPnLFilter.AsOf.IsZero() {
		t.Fatalf("captured filter=%#v calls=%d", history.dailyPnLFilter, history.dailyPnLCalls)
	}
}

// RunAccount 执行测试模拟流程。
func (job *fakeReconciliationJob) RunAccount(_ context.Context, params reconciliation.RunAccountParams) (reconciliation.Result, error) {
	job.accountID, job.trigger, job.orderID = params.ExecutionAccountID, params.Trigger, params.FocusOrderID
	return reconciliation.Result{Run: domain.ReconciliationRun{
		RunID: "recon-1", ExecutionAccountID: params.ExecutionAccountID, Trigger: params.Trigger,
		Status: domain.ReconciliationRunCompleted, Summary: map[string]int{},
	}}, nil
}

// TestReconciliationEndpointUsesJobToken 验证 Reconciliation Endpoint Uses Job Token 场景下的行为。
func TestReconciliationEndpointUsesJobToken(t *testing.T) {
	job := &fakeReconciliationJob{}
	server, err := New(Params{
		Service: baseExecutionService(t), Reconciliation: job,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIToken: "api-secret", JobToken: "job-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"execution_account_id":"account-1","trigger":"ASSET_DRIFT","focus_order_id":"order-1"}`
	unauthorized := performRequest(t, server, http.MethodPost, "/internal/jobs/reconciliation/run", body, "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	apiTokenResponse := performRequest(t, server, http.MethodPost, "/internal/jobs/reconciliation/run", body, "api-secret")
	if apiTokenResponse.Code != http.StatusUnauthorized || job.accountID != "" {
		t.Fatalf("API-token response status=%d job=%#v", apiTokenResponse.Code, job)
	}
	response := performRequest(t, server, http.MethodPost, "/internal/jobs/reconciliation/run", body, "job-secret")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"COMPLETED"`) {
		t.Fatalf("reconciliation response status=%d body=%s", response.Code, response.Body.String())
	}
	if job.accountID != "account-1" || job.trigger != domain.ReconciliationTriggerAssetDrift || job.orderID != "order-1" {
		t.Fatalf("reconciliation input = %#v", job)
	}
}

func TestReconciliationEndpointRejectsMissingDedicatedJobToken(t *testing.T) {
	_, err := New(Params{
		Service: baseExecutionService(t), Reconciliation: &fakeReconciliationJob{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), APIToken: "api-secret",
	})
	if err == nil || !strings.Contains(err.Error(), "dedicated job token") {
		t.Fatalf("New() error = %v, want dedicated job-token rejection", err)
	}
}

func TestReconciliationEndpointRejectsSharedJobToken(t *testing.T) {
	for _, test := range []struct {
		name          string
		apiToken      string
		readOnlyToken string
		jobToken      string
	}{
		{name: "execution", apiToken: "shared-secret", jobToken: "shared-secret"},
		{name: "read only", readOnlyToken: "shared-secret", jobToken: "shared-secret"},
		{name: "position exit execution", apiToken: "shared-secret", jobToken: "shared-secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reconciliationJob reconciliationJob = &fakeReconciliationJob{}
			var positionJob positionExitJob
			if test.name == "position exit execution" {
				reconciliationJob = nil
				positionJob = &fakePositionExitJob{}
			}
			_, err := New(Params{
				Service: baseExecutionService(t), Reconciliation: reconciliationJob, PositionExitJob: positionJob,
				Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
				APIToken: test.apiToken, ReadOnlyToken: test.readOnlyToken, JobToken: test.jobToken,
			})
			if err == nil || !strings.Contains(err.Error(), "must differ") {
				t.Fatalf("New() error = %v, want shared job-token rejection", err)
			}
		})
	}
}

// testServer 实现当前测试场景所需的辅助行为。
func testServer(t *testing.T, token string) *Server {
	t.Helper()
	service := baseExecutionService(t)
	server, err := New(Params{
		Service:  service,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIToken: token,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return server
}

// baseExecutionService 构建测试使用的基础领域对象。
func baseExecutionService(t *testing.T) *execution.Service {
	t.Helper()
	guard, err := risk.NewStaticGuard(risk.StaticGuardParams{})
	if err != nil {
		t.Fatalf("NewStaticGuard() error = %v", err)
	}
	service, err := execution.New(execution.Params{
		Repository:      memory.NewOrderRepository(),
		Venue:           paper.NewVenue("polymarket-paper"),
		Guard:           guard,
		MarketValidator: paper.NewMarketValidator(),
		Reservations:    paper.NewReservationManager(),
	})
	if err != nil {
		t.Fatalf("execution.New() error = %v", err)
	}
	return service
}

// performRequest 发送测试请求并返回记录的响应。
func performRequest(t *testing.T, server *Server, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	return response
}

// decodeResponse 解码测试响应到目标结构。
func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

const validRequestJSON = `{
  "model_id":"model-1",
  "strategy_id":"strategy-1",
  "execution_account_id":"account-model-1-strategy-1",
  "signal_id":"signal-1",
  "client_order_id":"client-1",
  "venue":"polymarket-paper",
  "market_id":"market-1",
  "condition_id":"condition-1",
  "token_id":"token-1",
  "side":"BUY",
  "type":"LIMIT",
  "price":"0.50",
  "size":"10",
  "time_in_force":"GTC"
}`

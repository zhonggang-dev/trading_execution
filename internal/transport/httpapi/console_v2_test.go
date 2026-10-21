package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type fakeConsoleV2Service struct {
	runtimeErr error
	readErr    error
	resource   consolev2.Runtime
}

type deadlineConsoleV2Service struct {
	fakeConsoleV2Service
	deadline      chan time.Time
	waitForCancel bool
}

func (fake *deadlineConsoleV2Service) Orders(ctx context.Context, _ consolev2.ListFilter) (consolev2.Page[consolev2.Order], error) {
	if deadline, ok := ctx.Deadline(); ok {
		fake.deadline <- deadline
	}
	if fake.waitForCancel {
		<-ctx.Done()
	}
	err := ctx.Err()
	if err == nil {
		err = context.DeadlineExceeded
	}
	return consolev2.Page[consolev2.Order]{}, err
}

func (fake *fakeConsoleV2Service) Runtime(context.Context) (consolev2.Runtime, error) {
	return fake.resource, fake.runtimeErr
}

func (fake *fakeConsoleV2Service) DecisionCycles(context.Context, consolev2.ListFilter) (consolev2.Page[consolev2.DecisionCycle], error) {
	return consolev2.Page[consolev2.DecisionCycle]{}, fake.readErr
}

func (fake *fakeConsoleV2Service) DecisionCycle(context.Context, string) (consolev2.DecisionCycleDetail, error) {
	return consolev2.DecisionCycleDetail{}, fake.readErr
}

func (fake *fakeConsoleV2Service) Orders(context.Context, consolev2.ListFilter) (consolev2.Page[consolev2.Order], error) {
	return consolev2.Page[consolev2.Order]{}, fake.readErr
}

func (fake *fakeConsoleV2Service) OrderTrace(context.Context, string) (consolev2.OrderTrace, error) {
	return consolev2.OrderTrace{}, fake.readErr
}

func (fake *fakeConsoleV2Service) PositionLots(context.Context, consolev2.ListFilter) (consolev2.Page[consolev2.PositionLot], error) {
	return consolev2.Page[consolev2.PositionLot]{}, fake.readErr
}

func (fake *fakeConsoleV2Service) ReconciliationRuns(context.Context, consolev2.ListFilter) (consolev2.Page[consolev2.ReconciliationRun], error) {
	return consolev2.Page[consolev2.ReconciliationRun]{}, fake.readErr
}

func (fake *fakeConsoleV2Service) ReconciliationIssues(context.Context, consolev2.ListFilter) (consolev2.Page[consolev2.ReconciliationIssue], error) {
	return consolev2.Page[consolev2.ReconciliationIssue]{}, fake.readErr
}

func (fake *fakeConsoleV2Service) Redemptions(context.Context, consolev2.ListFilter) (consolev2.Page[consolev2.Redemption], error) {
	return consolev2.Page[consolev2.Redemption]{}, fake.readErr
}

func (fake *fakeConsoleV2Service) ServiceMetrics(context.Context) (consolev2.ServiceMetrics, error) {
	return consolev2.ServiceMetrics{}, fake.readErr
}

func testConsoleV2Server(t *testing.T, service consolev2.API) *Server {
	t.Helper()
	server, err := New(Params{
		Service: baseExecutionService(t), ConsoleV2: service,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIToken: "execution-secret", ReadOnlyToken: "readonly-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestConsoleV2RuntimeUsesReadOnlyTokenAndSafeRunnerProjection(t *testing.T) {
	fake := &fakeConsoleV2Service{resource: consolev2.Runtime{
		ObservedAt: time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC),
		Runner:     consolev2.RuntimeRunner{Enabled: true, Running: true, State: "running", IntervalSeconds: 600},
		Bindings:   []consolev2.Binding{{PredictionModelID: "masked-source", ModelID: "deepseek_masked", StrategyID: "multfactor_v2", ExecutionAccountID: "wallet-7", SourceMode: "SANDBOX"}},
		Submission: consolev2.RuntimeSubmission{OrderSubmissionEnabled: false, EntrySubmissionDisabled: true},
	}}
	server := testConsoleV2Server(t, fake)

	unauthorized := performRequest(t, server, http.MethodGet, "/api/v1/console/v2/runtime", "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	forbidden := performRequest(t, server, http.MethodGet, "/api/v1/console/v2/runtime", "", "execution-secret")
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("execution token status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
	response := performRequest(t, server, http.MethodGet, "/api/v1/console/v2/runtime", "", "readonly-secret")
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"state":"running"`) ||
		!strings.Contains(body, `"prediction_model_id":"masked-source"`) ||
		strings.Contains(body, "strategyToken") || strings.Contains(body, "secret") {
		t.Fatalf("runtime status=%d body=%s", response.Code, body)
	}
}

func TestConsoleV2RejectsWritesAndInvalidFilters(t *testing.T) {
	server := testConsoleV2Server(t, &fakeConsoleV2Service{})
	write := performRequest(t, server, http.MethodPost, "/api/v1/console/v2/orders", `{}`, "readonly-secret")
	if write.Code != http.StatusMethodNotAllowed {
		t.Fatalf("write status=%d body=%s", write.Code, write.Body.String())
	}
	for _, path := range []string{
		"/api/v1/console/v2/orders?limit=101",
		"/api/v1/console/v2/orders?offset=9007199254740892",
		"/api/v1/console/v2/orders?status=NOT_A_STATUS",
		"/api/v1/console/v2/orders?market_source=UNKNOWN",
		"/api/v1/console/v2/orders?model=echo&model_id=echo",
		"/api/v1/console/v2/runtime?limit=1",
		"/api/v1/console/v2/decision-cycles?from=not-a-time",
	} {
		response := performRequest(t, server, http.MethodGet, path, "", "readonly-secret")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	for _, path := range []string{
		"/api/v1/console/v2/orders?account=wallet-1&source=POLYMARKET&side=BUY",
		"/api/v1/console/v2/decision-cycles?account=wallet-1&model=echo",
	} {
		response := performRequest(t, server, http.MethodGet, path, "", "readonly-secret")
		if response.Code != http.StatusOK {
			t.Fatalf("short aliases path=%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	page := performRequest(t, server, http.MethodGet, "/api/v1/console/v2/orders", "", "readonly-secret")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), `"total":0`) {
		t.Fatalf("page total status=%d body=%s", page.Code, page.Body.String())
	}
}

func TestConsoleV2AcceptsBrowserSafeDeepOffset(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/console/v2/orders?offset=9007199254740891", nil)
	filter, err := parseConsoleV2Filter(request, consoleV2FilterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if filter.Offset != consolev2.MaxOffset {
		t.Fatalf("offset=%d, want %d", filter.Offset, consolev2.MaxOffset)
	}
}

func TestConsoleV2MapsUnavailableAndMissingIdentity(t *testing.T) {
	fake := &fakeConsoleV2Service{readErr: consolev2.ErrUnavailable}
	server := testConsoleV2Server(t, fake)
	response := performRequest(t, server, http.MethodGet, "/api/v1/console/v2/orders", "", "readonly-secret")
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"CONSOLE_V2_UNAVAILABLE"`) {
		t.Fatalf("unavailable status=%d body=%s", response.Code, response.Body.String())
	}
	fake.readErr = consolev2.ErrNotFound
	missing := performRequest(t, server, http.MethodGet, "/api/v1/console/v2/orders/unknown/trace", "", "readonly-secret")
	if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), `"code":"CONSOLE_V2_NOT_FOUND"`) {
		t.Fatalf("missing status=%d body=%s", missing.Code, missing.Body.String())
	}
}

func TestConsoleV2ReadUsesFiveSecondDeadline(t *testing.T) {
	fake := &deadlineConsoleV2Service{deadline: make(chan time.Time, 1)}
	server := testConsoleV2Server(t, fake)
	started := time.Now()
	response := performRequest(t, server, http.MethodGet, "/api/v1/console/v2/orders", "", "readonly-secret")
	deadline := <-fake.deadline
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if deadline.Before(started.Add(4*time.Second)) || deadline.After(started.Add(consoleV2ReadTimeout+250*time.Millisecond)) {
		t.Fatalf("deadline=%s, want about %s after request start", deadline, consoleV2ReadTimeout)
	}
}

func TestConsoleV2ReadPreservesShorterParentDeadline(t *testing.T) {
	fake := &deadlineConsoleV2Service{deadline: make(chan time.Time, 1), waitForCancel: true}
	server := testConsoleV2Server(t, fake)
	parent, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	parentDeadline, ok := parent.Deadline()
	if !ok {
		t.Fatal("parent context has no deadline")
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/console/v2/orders", nil).WithContext(parent)
	request.Header.Set("Authorization", "Bearer readonly-secret")
	response := httptest.NewRecorder()
	started := time.Now()
	server.Handler().ServeHTTP(response, request)
	deadline := <-fake.deadline
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if time.Since(started) > time.Second {
		t.Fatalf("short parent deadline was not preserved; elapsed=%s", time.Since(started))
	}
	if deadline.After(parentDeadline.Add(5 * time.Millisecond)) {
		t.Fatalf("derived deadline=%s exceeds parent deadline", deadline)
	}
}

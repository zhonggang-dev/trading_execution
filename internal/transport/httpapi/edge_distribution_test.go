package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/edgedistribution"
)

type fakeEdgeDistributionService struct {
	distribution domain.EdgeDistribution
	err          error
	modelID      string
}

// Latest 返回测试预置的 Edge 分布并记录模型筛选值。
func (service *fakeEdgeDistributionService) Latest(_ context.Context, modelID string) (domain.EdgeDistribution, error) {
	service.modelID = modelID
	return service.distribution, service.err
}

// TestEdgeDistributionEndpointUsesReadOnlyPermission 验证端点只接受只读令牌并返回约定口径。
func TestEdgeDistributionEndpointUsesReadOnlyPermission(t *testing.T) {
	decisionAt := time.Date(2026, 8, 25, 4, 20, 0, 0, time.UTC)
	edges := &fakeEdgeDistributionService{distribution: domain.EdgeDistribution{
		DecisionAt: decisionAt, PriceBasis: domain.EdgePriceBasisMidpoint,
		OutcomeScope: domain.EdgeOutcomeScopeOutcome0, BinWidth: 0.05, RangeMin: -0.1, RangeMax: 0.1,
		Series: []domain.EdgeDistributionSeries{{ModelID: "echo", SampleCount: 1}},
	}}
	server, err := New(Params{
		Service: baseExecutionService(t), EdgeDistribution: edges,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIToken: "execution-secret", ReadOnlyToken: "readonly-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	unauthorized := performRequest(t, server, http.MethodGet, "/api/v1/edge-distribution", "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
	forbidden := performRequest(t, server, http.MethodGet, "/api/v1/edge-distribution", "", "execution-secret")
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("forbidden status = %d", forbidden.Code)
	}
	response := performRequest(t, server, http.MethodGet, "/api/v1/edge-distribution?model_id=echo", "", "readonly-secret")
	if response.Code != http.StatusOK || edges.modelID != "echo" ||
		!strings.Contains(response.Body.String(), `"price_basis":"MIDPOINT"`) ||
		!strings.Contains(response.Body.String(), `"model_id":"echo"`) {
		t.Fatalf("response status=%d model=%q body=%s", response.Code, edges.modelID, response.Body.String())
	}
}

// TestEdgeDistributionEndpointHandlesFiltersAndMissingSnapshot 验证非法筛选和快照缺失的稳定错误响应。
func TestEdgeDistributionEndpointHandlesFiltersAndMissingSnapshot(t *testing.T) {
	edges := &fakeEdgeDistributionService{err: edgedistribution.ErrNotFound}
	server, err := New(Params{
		Service: baseExecutionService(t), EdgeDistribution: edges,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ReadOnlyToken: "readonly-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid := performRequest(t, server, http.MethodGet, "/api/v1/edge-distribution?window=10m", "", "readonly-secret")
	if invalid.Code != http.StatusBadRequest || !strings.Contains(invalid.Body.String(), "INVALID_EDGE_DISTRIBUTION_FILTER") {
		t.Fatalf("invalid filter status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	invalidPaths := []string{
		"/api/v1/edge-distribution?model_id=a&model_id=b",
		"/api/v1/edge-distribution?model_id=echo%0Aother",
		"/api/v1/edge-distribution?model_id=" + strings.Repeat("a", 129),
	}
	for _, path := range invalidPaths {
		response := performRequest(t, server, http.MethodGet, path, "", "readonly-secret")
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "INVALID_EDGE_DISTRIBUTION_FILTER") {
			t.Fatalf("invalid path=%q status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	missing := performRequest(t, server, http.MethodGet, "/api/v1/edge-distribution", "", "readonly-secret")
	if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), "EDGE_DISTRIBUTION_NOT_FOUND") {
		t.Fatalf("missing status=%d body=%s", missing.Code, missing.Body.String())
	}
}

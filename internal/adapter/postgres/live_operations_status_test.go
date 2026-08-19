package postgres

import (
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestValidateLiveWorkerState 验证 heartbeat 只接受固定线程和完整阈值。
func TestValidateLiveWorkerState(t *testing.T) {
	valid := domain.LiveWorkerState{
		ThreadID: "monitor", RunID: "run-1", Name: "持仓看护", Purpose: "检查仓位",
		Cadence: "每 3 分钟", MaxHeartbeatAge: 6 * time.Minute, LastHeartbeatAt: time.Now(),
	}
	if err := validateLiveWorkerState(valid); err != nil {
		t.Fatalf("validateLiveWorkerState() error = %v", err)
	}
	valid.ThreadID = "unknown"
	if err := validateLiveWorkerState(valid); err == nil {
		t.Fatal("validateLiveWorkerState() error = nil, want invalid thread")
	}
}

// TestValidateLiveFunnelReportRequiresSameCompleteCycle 验证漏斗必须一次上报同一轮完整六步。
func TestValidateLiveFunnelReportRequiresSameCompleteCycle(t *testing.T) {
	stages := []domain.LiveFunnelStage{
		{ID: "scan", Index: 1, Name: "扫描", Description: "扫描", State: domain.LiveFlowDone},
		{ID: "filter", Index: 2, Name: "过滤", Description: "过滤", State: domain.LiveFlowDone},
		{ID: "predict", Index: 3, Name: "预测", Description: "预测", State: domain.LiveFlowActive},
		{ID: "risk", Index: 4, Name: "风控", Description: "风控", State: domain.LiveFlowIdle},
		{ID: "route", Index: 5, Name: "执行", Description: "执行", State: domain.LiveFlowIdle},
		{ID: "ledger", Index: 6, Name: "入账", Description: "入账", State: domain.LiveFlowIdle},
	}
	report := domain.LiveFunnelReport{RunID: "run-1", CycleID: "cycle-1", ObservedAt: time.Now(), Stages: stages}
	if err := validateLiveFunnelReport(report); err != nil {
		t.Fatalf("validateLiveFunnelReport() error = %v", err)
	}
	report.Stages[5].ID = "route"
	if err := validateLiveFunnelReport(report); err == nil {
		t.Fatal("validateLiveFunnelReport() error = nil, want duplicated stage")
	}
}

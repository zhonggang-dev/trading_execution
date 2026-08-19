package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

var _ port.LiveOperationsStatusWriter = (*LiveOperationsRepository)(nil)

// ReportLiveWorker 原子新增或更新一个固定业务线程的 heartbeat。
func (repository *LiveOperationsRepository) ReportLiveWorker(ctx context.Context, state domain.LiveWorkerState) error {
	state.ThreadID = domain.NormalizeLiveWorkerID(state.ThreadID)
	if err := validateLiveWorkerState(state); err != nil {
		return err
	}
	_, err := repository.db.ExecContext(ctx, `
		INSERT INTO live_runtime_status (
			thread_id, run_id, name, purpose, cadence, max_heartbeat_age_ms,
			last_heartbeat_at, current_task, current_cycle_id, last_error,
			metric_label, metric_value, stopped, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,clock_timestamp())
		ON CONFLICT (run_id, thread_id) DO UPDATE SET
			name=EXCLUDED.name, purpose=EXCLUDED.purpose, cadence=EXCLUDED.cadence,
			max_heartbeat_age_ms=EXCLUDED.max_heartbeat_age_ms,
			last_heartbeat_at=EXCLUDED.last_heartbeat_at, current_task=EXCLUDED.current_task,
			current_cycle_id=EXCLUDED.current_cycle_id, last_error=EXCLUDED.last_error,
			metric_label=EXCLUDED.metric_label, metric_value=EXCLUDED.metric_value,
			stopped=EXCLUDED.stopped, updated_at=clock_timestamp()`,
		state.ThreadID, strings.TrimSpace(state.RunID), strings.TrimSpace(state.Name), strings.TrimSpace(state.Purpose),
		strings.TrimSpace(state.Cadence), state.MaxHeartbeatAge.Milliseconds(),
		state.LastHeartbeatAt.UTC(), strings.TrimSpace(state.CurrentTask),
		strings.TrimSpace(state.CurrentCycleID), strings.TrimSpace(state.LastError),
		strings.TrimSpace(state.MetricLabel), strings.TrimSpace(state.MetricValue), state.Stopped,
	)
	if err != nil {
		return fmt.Errorf("upsert live worker heartbeat: %w", err)
	}
	return nil
}

// ReportLiveFunnel 在一个事务中替换同一 run_id 和 cycle_id 的完整六步漏斗。
func (repository *LiveOperationsRepository) ReportLiveFunnel(ctx context.Context, report domain.LiveFunnelReport) error {
	if err := validateLiveFunnelReport(report); err != nil {
		return err
	}
	tx, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin live funnel report: %w", err)
	}
	defer tx.Rollback()
	runID, cycleID := strings.TrimSpace(report.RunID), strings.TrimSpace(report.CycleID)
	if _, err := tx.ExecContext(ctx, `DELETE FROM live_cycle_funnel WHERE run_id=$1 AND cycle_id=$2`, runID, cycleID); err != nil {
		return fmt.Errorf("clear live funnel cycle: %w", err)
	}
	for _, stage := range report.Stages {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO live_cycle_funnel (
				run_id, cycle_id, stage_id, stage_index, name, description,
				count, throughput_label, state, observed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			runID, cycleID, stage.ID, stage.Index, stage.Name, stage.Description,
			stage.Count, stage.ThroughputLabel, stage.State, report.ObservedAt.UTC(),
		); err != nil {
			return fmt.Errorf("insert live funnel stage %s: %w", stage.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit live funnel report: %w", err)
	}
	return nil
}

// validateLiveWorkerState 校验 heartbeat 身份、阈值和时间。
func validateLiveWorkerState(state domain.LiveWorkerState) error {
	if state.ThreadID != "cycle" && state.ThreadID != "monitor" && state.ThreadID != "prediction" {
		return fmt.Errorf("live worker thread_id must be cycle, monitor, or prediction")
	}
	if strings.TrimSpace(state.RunID) == "" {
		return fmt.Errorf("live worker run_id is required")
	}
	if strings.TrimSpace(state.Name) == "" || strings.TrimSpace(state.Purpose) == "" || strings.TrimSpace(state.Cadence) == "" {
		return fmt.Errorf("live worker name, purpose, and cadence are required")
	}
	if state.MaxHeartbeatAge <= 0 || state.LastHeartbeatAt.IsZero() {
		return fmt.Errorf("live worker max heartbeat age and last heartbeat time are required")
	}
	return nil
}

// validateLiveFunnelReport 校验漏斗必须包含固定且不重复的完整六步。
func validateLiveFunnelReport(report domain.LiveFunnelReport) error {
	if strings.TrimSpace(report.RunID) == "" || strings.TrimSpace(report.CycleID) == "" || report.ObservedAt.IsZero() {
		return fmt.Errorf("live funnel run_id, cycle_id, and observed_at are required")
	}
	expected := map[string]int{"scan": 1, "filter": 2, "predict": 3, "risk": 4, "route": 5, "ledger": 6}
	if len(report.Stages) != len(expected) {
		return fmt.Errorf("live funnel requires exactly six stages")
	}
	seen := make(map[string]struct{}, len(report.Stages))
	for _, stage := range report.Stages {
		if expected[stage.ID] != stage.Index {
			return fmt.Errorf("live funnel stage %q has invalid index", stage.ID)
		}
		if _, duplicate := seen[stage.ID]; duplicate {
			return fmt.Errorf("live funnel stage %q is duplicated", stage.ID)
		}
		seen[stage.ID] = struct{}{}
		if stage.Count < 0 || strings.TrimSpace(stage.Name) == "" || strings.TrimSpace(stage.Description) == "" {
			return fmt.Errorf("live funnel stage %q has invalid fields", stage.ID)
		}
		if stage.State != domain.LiveFlowDone && stage.State != domain.LiveFlowActive && stage.State != domain.LiveFlowWarning && stage.State != domain.LiveFlowIdle {
			return fmt.Errorf("live funnel stage %q has invalid state", stage.ID)
		}
	}
	return nil
}

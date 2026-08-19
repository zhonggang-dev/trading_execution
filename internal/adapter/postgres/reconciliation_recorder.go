package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// ReconciliationRecorder 表示后端使用的 ReconciliationRecorder 类型。
type ReconciliationRecorder struct {
	db *sql.DB
}

const reconciliationRunLease = 30 * time.Minute

var _ port.ReconciliationRecorder = (*ReconciliationRecorder)(nil)

// NewReconciliationRecorder 创建并初始化 Reconciliation Recorder。
func NewReconciliationRecorder(db *sql.DB) (*ReconciliationRecorder, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &ReconciliationRecorder{db: db}, nil
}

// Start 持久化一次对账运行的开始状态。
func (recorder *ReconciliationRecorder) Start(ctx context.Context, run domain.ReconciliationRun) error {
	summary, err := json.Marshal(run.Summary)
	if err != nil {
		return fmt.Errorf("encode reconciliation summary: %w", err)
	}
	tx, err := recorder.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin reconciliation run: %w", err)
	}
	defer tx.Rollback()
	// A process can die after creating RUNNING. Recover only rows older than a
	// bounded lease; a fresh RUNNING row is protected by the partial unique
	// index so two service instances cannot repair the same account at once.
	if _, err := tx.ExecContext(ctx, `
		UPDATE reconciliation_runs
		SET status='FAILED', error='reconciliation worker lease expired', completed_at=$2
		WHERE execution_account_id=$1 AND status='RUNNING' AND started_at < $3`,
		run.ExecutionAccountID, run.StartedAt.UTC(), run.StartedAt.Add(-reconciliationRunLease).UTC()); err != nil {
		return fmt.Errorf("expire abandoned reconciliation run: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO reconciliation_runs (
			run_id, execution_account_id, trigger, focus_order_id,
			status, summary, error, started_at, completed_at
		) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,NULL)`,
		run.RunID, run.ExecutionAccountID, string(run.Trigger), run.FocusOrderID,
		string(run.Status), summary, run.Error, run.StartedAt.UTC())
	if err != nil {
		return fmt.Errorf("start reconciliation run: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reconciliation run start: %w", err)
	}
	return nil
}

// RecordIssue 幂等持久化一次对账问题。
func (recorder *ReconciliationRecorder) RecordIssue(ctx context.Context, issue domain.ReconciliationIssue) error {
	if issue.Status == domain.ReconciliationIssueResolved {
		result, err := recorder.db.ExecContext(ctx, `
			UPDATE reconciliation_issues
			SET status='RESOLVED', resolution=$3, details=$4, source=$5,
			    resolved_at=$6, observed_at=GREATEST(observed_at, $7)
			WHERE execution_account_id=$1 AND fingerprint=$2 AND status='OPEN'`,
			issue.ExecutionAccountID, issue.Fingerprint, string(issue.Resolution),
			issue.Details, issue.Source, issue.ResolvedAt, issue.ObservedAt.UTC())
		if err != nil {
			return fmt.Errorf("resolve reconciliation issue: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("resolve reconciliation issue rows: %w", err)
		} else if affected > 0 {
			return nil
		}
	}
	conflictClause := "ON CONFLICT DO NOTHING"
	if issue.Status == domain.ReconciliationIssueOpen {
		// Re-observing the same open fingerprint moves its evidence to this
		// run. Complete can then distinguish an actively reproduced transient
		// issue from one that disappeared after a healthy sweep.
		conflictClause = `ON CONFLICT (execution_account_id, fingerprint)
			WHERE status = 'OPEN' DO UPDATE SET
				run_id = EXCLUDED.run_id,
				resolution = EXCLUDED.resolution,
				details = EXCLUDED.details,
				source = EXCLUDED.source,
				local_value = EXCLUDED.local_value,
				remote_value = EXCLUDED.remote_value,
				observed_at = EXCLUDED.observed_at`
	}
	_, err := recorder.db.ExecContext(ctx, `
		INSERT INTO reconciliation_issues (
			issue_id, run_id, fingerprint, execution_account_id, issue_type,
			resolution, status, order_id, venue_order_id, venue_trade_id,
			market_id, condition_id, token_id, local_value, remote_value, source,
			details, observed_at, resolved_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
			NULLIF($14,'')::numeric,NULLIF($15,'')::numeric,$16,$17,$18,$19
		)
		`+conflictClause,
		issue.IssueID, issue.RunID, issue.Fingerprint, issue.ExecutionAccountID,
		string(issue.Type), string(issue.Resolution), string(issue.Status),
		issue.OrderID, issue.VenueOrderID, issue.VenueTradeID, issue.MarketID,
		issue.ConditionID, issue.TokenID, issue.LocalValue.String(), issue.RemoteValue.String(),
		issue.Source, issue.Details, issue.ObservedAt.UTC(), issue.ResolvedAt)
	if err != nil {
		return fmt.Errorf("record reconciliation issue: %w", err)
	}
	return nil
}

// Complete 持久化一次对账运行的完成状态和摘要。
func (recorder *ReconciliationRecorder) Complete(ctx context.Context, run domain.ReconciliationRun) error {
	if run.CompletedAt == nil {
		return fmt.Errorf("completed reconciliation run requires completed_at")
	}
	summary, err := json.Marshal(run.Summary)
	if err != nil {
		return fmt.Errorf("encode reconciliation summary: %w", err)
	}
	tx, err := recorder.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin reconciliation run completion: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE reconciliation_runs
		SET status=$2, summary=$3::jsonb, error=$4, completed_at=$5
		WHERE run_id=$1 AND status='RUNNING'`, run.RunID, string(run.Status),
		summary, run.Error, run.CompletedAt.UTC())
	if err != nil {
		return fmt.Errorf("complete reconciliation run: %w", err)
	}
	if !oneRow(result) {
		return fmt.Errorf("reconciliation run is missing or already completed")
	}
	if run.Status == domain.ReconciliationRunCompleted {
		// RETRY_LATER means infrastructure uncertainty, not a permanent manual
		// discrepancy. If a fully completed later sweep did not reproduce the
		// fingerprint, close it; manual-review issues remain fail-closed until an
		// operator or attributable repair explicitly resolves them.
		if _, err := tx.ExecContext(ctx, `
			UPDATE reconciliation_issues
			SET status='RESOLVED', resolved_at=$3
			WHERE execution_account_id=$1 AND status='OPEN'
			  AND resolution='RETRY_LATER' AND run_id <> $2`,
			run.ExecutionAccountID, run.RunID, run.CompletedAt.UTC()); err != nil {
			return fmt.Errorf("resolve recovered reconciliation infrastructure issues: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reconciliation run completion: %w", err)
	}
	return nil
}

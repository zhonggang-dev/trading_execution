package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// PositionExitRecorder 表示后端使用的 PositionExitRecorder 类型。
type PositionExitRecorder struct {
	db  *sql.DB
	now func() time.Time
}

var _ port.PositionExitRecorder = (*PositionExitRecorder)(nil)

// NewPositionExitRecorder 创建并初始化 Position Exit Recorder。
func NewPositionExitRecorder(db *sql.DB, now func() time.Time) (*PositionExitRecorder, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if now == nil {
		now = time.Now
	}
	return &PositionExitRecorder{db: db, now: now}, nil
}

// GetInput 按周期查询已持久化的冻结策略输入。
func (recorder *PositionExitRecorder) GetInput(ctx context.Context, cycleID string) (domain.PositionExitRequest, error) {
	var payload []byte
	err := recorder.db.QueryRowContext(ctx, `
		SELECT input_payload FROM position_exit_runs WHERE cycle_id=$1`, cycleID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PositionExitRequest{}, port.ErrPositionExitRunNotFound
	}
	if err != nil {
		return domain.PositionExitRequest{}, fmt.Errorf("get position exit input: %w", err)
	}
	var request domain.PositionExitRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return domain.PositionExitRequest{}, fmt.Errorf("decode stored position exit input: %w", err)
	}
	return request, nil
}

// ClaimInput 按周期幂等认领并持久化冻结策略输入。
func (recorder *PositionExitRecorder) ClaimInput(
	ctx context.Context,
	request domain.PositionExitRequest,
) (domain.PositionExitRequest, bool, error) {
	computed, err := domain.ComputePositionExitInputID(request)
	if err != nil || computed != request.InputID {
		return domain.PositionExitRequest{}, false, fmt.Errorf("position exit input_id does not match payload")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return domain.PositionExitRequest{}, false, fmt.Errorf("encode position exit input: %w", err)
	}
	now := recorder.now().UTC()
	result, err := recorder.db.ExecContext(ctx, `
		INSERT INTO position_exit_runs (
			cycle_id, input_id, decision_at, model_id, strategy_id,
			execution_account_id, input_payload, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $8)
		ON CONFLICT (cycle_id) DO NOTHING`,
		request.CycleID, request.InputID, request.DecisionAt.UTC(), request.Context.ModelID,
		request.Context.StrategyID, request.Context.ExecutionAccountID, payload, now)
	if err != nil {
		return domain.PositionExitRequest{}, false, fmt.Errorf("claim position exit input: %w", err)
	}
	if oneRow(result) {
		return request, true, nil
	}
	stored, err := recorder.GetInput(ctx, request.CycleID)
	if err != nil {
		return domain.PositionExitRequest{}, false, err
	}
	storedID, storedErr := domain.ComputePositionExitInputID(stored)
	if storedErr != nil || stored.InputID != request.InputID || storedID != request.InputID || !stored.Context.Equal(request.Context) {
		return domain.PositionExitRequest{}, false, port.ErrPositionExitConflict
	}
	return stored, false, nil
}

// GetOutput 按输入标识查询已持久化的策略输出。
func (recorder *PositionExitRecorder) GetOutput(ctx context.Context, cycleID string) (domain.PositionExitResponse, error) {
	var payload []byte
	err := recorder.db.QueryRowContext(ctx, `
		SELECT output_payload FROM position_exit_runs
		WHERE cycle_id=$1 AND output_payload IS NOT NULL`, cycleID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PositionExitResponse{}, port.ErrPositionExitRunNotFound
	}
	if err != nil {
		return domain.PositionExitResponse{}, fmt.Errorf("get position exit output: %w", err)
	}
	var response domain.PositionExitResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		return domain.PositionExitResponse{}, fmt.Errorf("decode stored position exit output: %w", err)
	}
	return response, nil
}

// ClaimOutput 按输入幂等认领并持久化策略输出。
func (recorder *PositionExitRecorder) ClaimOutput(
	ctx context.Context,
	response domain.PositionExitResponse,
) (stored domain.PositionExitResponse, created bool, resultErr error) {
	payload, err := json.Marshal(response)
	if err != nil {
		return domain.PositionExitResponse{}, false, fmt.Errorf("encode position exit output: %w", err)
	}
	tx, err := recorder.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.PositionExitResponse{}, false, fmt.Errorf("begin position exit output claim: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()
	var inputPayload []byte
	var existingPayload []byte
	err = tx.QueryRowContext(ctx, `
		SELECT input_payload, COALESCE(output_payload::text, '')
		FROM position_exit_runs WHERE cycle_id=$1 FOR UPDATE`, response.CycleID).Scan(&inputPayload, &existingPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PositionExitResponse{}, false, port.ErrPositionExitRunNotFound
	}
	if err != nil {
		return domain.PositionExitResponse{}, false, fmt.Errorf("lock position exit run: %w", err)
	}
	var input domain.PositionExitRequest
	if err := json.Unmarshal(inputPayload, &input); err != nil {
		return domain.PositionExitResponse{}, false, fmt.Errorf("decode position exit run input: %w", err)
	}
	if response.InputID != input.InputID || !response.Context.Equal(input.Context) {
		return domain.PositionExitResponse{}, false, port.ErrPositionExitConflict
	}
	if len(existingPayload) != 0 {
		var existing domain.PositionExitResponse
		if err := json.Unmarshal(existingPayload, &existing); err != nil {
			return domain.PositionExitResponse{}, false, fmt.Errorf("decode existing position exit output: %w", err)
		}
		existingJSON, _ := json.Marshal(existing)
		if string(existingJSON) != string(payload) {
			return domain.PositionExitResponse{}, false, port.ErrPositionExitConflict
		}
		if err := tx.Commit(); err != nil {
			return domain.PositionExitResponse{}, false, fmt.Errorf("commit existing position exit output: %w", err)
		}
		return existing, false, nil
	}
	now := recorder.now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE position_exit_runs
		SET output_payload=$2::jsonb, decided_at=$3, updated_at=$4
		WHERE cycle_id=$1`, response.CycleID, payload, response.DecidedAt.UTC(), now); err != nil {
		return domain.PositionExitResponse{}, false, fmt.Errorf("store position exit output: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.PositionExitResponse{}, false, fmt.Errorf("commit position exit output: %w", err)
	}
	return response, true, nil
}

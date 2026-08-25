package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// EdgeDistributionRepository 读取最新决策边界冻结的不可变策略输入。
type EdgeDistributionRepository struct {
	db *sql.DB
}

// NewEdgeDistributionRepository 创建 PostgreSQL Edge 分布仓储并校验数据库连接。
func NewEdgeDistributionRepository(db *sql.DB) (*EdgeDistributionRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("edge distribution database is required")
	}
	return &EdgeDistributionRepository{db: db}, nil
}

// ListLatestDecisionInputs 读取最新全局边界并可按模型过滤，绝不回退到旧边界。
func (repository *EdgeDistributionRepository) ListLatestDecisionInputs(
	ctx context.Context,
	modelID string,
) ([]domain.StrategyDecisionRequest, error) {
	rows, err := repository.db.QueryContext(ctx, `
		WITH latest_boundary AS (
			SELECT MAX(decision_at) AS decision_at
			FROM strategy_decision_runs
		)
		SELECT run.input_payload
		FROM strategy_decision_runs AS run
		CROSS JOIN latest_boundary AS latest
		WHERE run.decision_at = latest.decision_at
		  AND ($1 = '' OR run.model_id = $1)
		ORDER BY run.model_id, run.strategy_id, run.execution_account_id, run.cycle_id`, modelID)
	if err != nil {
		return nil, fmt.Errorf("query latest strategy decision inputs: %w", err)
	}
	defer rows.Close()

	inputs := make([]domain.StrategyDecisionRequest, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan latest strategy decision input: %w", err)
		}
		var input domain.StrategyDecisionRequest
		if err := json.Unmarshal(payload, &input); err != nil {
			return nil, fmt.Errorf("decode latest strategy decision input: %w", err)
		}
		inputs = append(inputs, input)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate latest strategy decision inputs: %w", err)
	}
	return inputs, nil
}

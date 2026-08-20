package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// HealthChecker 同时校验数据库连通性和 HTTP 执行链路所需的完整 schema。
type HealthChecker struct {
	db *sql.DB
}

// NewHealthChecker 创建只读 PostgreSQL 就绪检查器。
func NewHealthChecker(db *sql.DB) (*HealthChecker, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &HealthChecker{db: db}, nil
}

// Check 仅在 PostgreSQL 可访问且执行、预占、成交、对账、风控与监控 schema 完整时返回成功。
func (checker *HealthChecker) Check(ctx context.Context) error {
	if err := checker.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	var missing int
	err := checker.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM (VALUES
			('execution_accounts'),
			('execution_positions'),
			('asset_reservations'),
			('asset_reservation_events'),
			('execution_orders'),
			('execution_order_events'),
			('execution_order_attempts'),
			('execution_fills'),
			('execution_fill_events'),
			('position_lots'),
			('position_lot_closures'),
			('position_events'),
			('execution_account_events'),
			('execution_outbox'),
			('position_exit_runs'),
			('strategy_decision_runs'),
			('strategy_order_intent_deliveries'),
			('reconciliation_runs'),
			('reconciliation_issues'),
			('execution_risk_global_control'),
			('execution_risk_policies'),
			('execution_risk_controls'),
			('execution_strategy_bindings'),
			('live_runtime_status'),
			('live_cycle_funnel')
		) AS required(name)
		WHERE to_regclass(required.name) IS NULL`).Scan(&missing)
	if err != nil {
		return fmt.Errorf("inspect postgres schema: %w", err)
	}
	if missing != 0 {
		return fmt.Errorf("postgres schema is incomplete: %d required relations are missing", missing)
	}

	var missingColumns int
	err = checker.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM (VALUES
			('asset_reservations', 'settled_fees'),
			('asset_reservations', 'risk_policy_id'),
			('asset_reservations', 'risk_policy_version'),
			('asset_reservations', 'risk_day'),
			('asset_reservations', 'daily_risk_notional'),
			('execution_fills', 'platform_fee_rate'),
			('execution_fills', 'fee_exponent'),
			('execution_fills', 'settlement_evidence'),
			('strategy_decision_runs', 'order_submission_enabled'),
			('strategy_order_intent_deliveries', 'intent_payload'),
			('strategy_order_intent_deliveries', 'status'),
			('strategy_order_intent_deliveries', 'attempt_count')
		) AS required(table_name, column_name)
		WHERE NOT EXISTS (
			SELECT 1
			FROM information_schema.columns actual
			WHERE actual.table_schema = current_schema()
			  AND actual.table_name = required.table_name
			  AND actual.column_name = required.column_name
		)`).Scan(&missingColumns)
	if err != nil {
		return fmt.Errorf("inspect postgres live schema columns: %w", err)
	}
	if missingColumns != 0 {
		return fmt.Errorf("postgres live schema is incomplete: %d required columns are missing", missingColumns)
	}

	var missingConstraints int
	err = checker.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM (VALUES
			('asset_reservations_target_lot_fk'),
			('asset_reservations_target_lot_shape'),
			('asset_reservations_buy_initial_reserve_identity'),
			('asset_reservations_buy_spend_within_reserve'),
			('asset_reservations_risk_metadata_shape'),
			('execution_strategy_bindings_canonical_strategy'),
			('execution_risk_controls_canonical_strategy'),
			('execution_fills_platform_fee_rate_nonnegative'),
			('execution_fills_fee_exponent_shape'),
			('execution_fills_settlement_evidence_object'),
			('execution_fills_polygon_settlement_evidence_shape'),
			('strategy_decision_runs_submission_mode_shape'),
			('strategy_order_intent_deliveries_cycle_sequence_unique'),
			('strategy_order_intent_deliveries_identity_nonempty'),
			('strategy_order_intent_deliveries_payload_object'),
			('strategy_order_intent_deliveries_status'),
			('strategy_order_intent_deliveries_attempt_nonnegative'),
			('strategy_order_intent_deliveries_state_shape'),
			('strategy_order_intent_deliveries_result_shape')
		) AS required(name)
		WHERE NOT EXISTS (
			SELECT 1
			FROM pg_constraint constraint_definition
			JOIN pg_namespace namespace_definition
			  ON namespace_definition.oid = constraint_definition.connamespace
			WHERE namespace_definition.nspname = current_schema()
			  AND constraint_definition.conname = required.name
			  AND constraint_definition.convalidated
		)`).Scan(&missingConstraints)
	if err != nil {
		return fmt.Errorf("inspect postgres live schema constraints: %w", err)
	}
	if missingConstraints != 0 {
		return fmt.Errorf("postgres live schema is incomplete: %d required constraints are missing or not validated", missingConstraints)
	}

	var missingIndexes int
	err = checker.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM (VALUES
			('execution_fills_polygon_settlement_event_uidx'),
			('strategy_decision_runs_account_time_idx'),
			('strategy_order_intent_deliveries_cycle_sequence_unique')
		) AS required(name)
		WHERE NOT EXISTS (
			SELECT 1
			FROM pg_class index_definition
			JOIN pg_namespace namespace_definition
			  ON namespace_definition.oid = index_definition.relnamespace
			JOIN pg_index index_state ON index_state.indexrelid = index_definition.oid
			WHERE namespace_definition.nspname = current_schema()
			  AND index_definition.relname = required.name
			  AND index_state.indisunique
			  AND index_state.indisvalid
			  AND index_state.indisready
		)`).Scan(&missingIndexes)
	if err != nil {
		return fmt.Errorf("inspect postgres live schema indexes: %w", err)
	}
	if missingIndexes != 0 {
		return fmt.Errorf("postgres live schema is incomplete: %d required indexes are missing or invalid", missingIndexes)
	}

	var missingDeliveryIndexes int
	err = checker.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM (VALUES
			('strategy_order_intent_deliveries_pending_idx'),
			('strategy_order_intent_deliveries_stale_idx')
		) AS required(name)
		WHERE NOT EXISTS (
			SELECT 1
			FROM pg_class index_definition
			JOIN pg_namespace namespace_definition
			  ON namespace_definition.oid = index_definition.relnamespace
			JOIN pg_index index_state ON index_state.indexrelid = index_definition.oid
			WHERE namespace_definition.nspname = current_schema()
			  AND index_definition.relname = required.name
			  AND index_state.indisvalid
			  AND index_state.indisready
		)`).Scan(&missingDeliveryIndexes)
	if err != nil {
		return fmt.Errorf("inspect postgres decision delivery indexes: %w", err)
	}
	if missingDeliveryIndexes != 0 {
		return fmt.Errorf("postgres live schema is incomplete: %d decision delivery indexes are missing or invalid", missingDeliveryIndexes)
	}

	var missingTriggers int
	err = checker.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM (VALUES
			('execution_orders_live_submit_risk_trigger'),
			('execution_risk_global_control_version_trigger'),
			('execution_risk_policies_version_trigger'),
			('execution_risk_controls_version_trigger'),
			('execution_strategy_bindings_version_trigger')
		) AS required(name)
		WHERE NOT EXISTS (
			SELECT 1
			FROM pg_trigger trigger_definition
			JOIN pg_class relation_definition ON relation_definition.oid = trigger_definition.tgrelid
			JOIN pg_namespace namespace_definition ON namespace_definition.oid = relation_definition.relnamespace
			WHERE namespace_definition.nspname = current_schema()
			  AND trigger_definition.tgname = required.name
			  AND NOT trigger_definition.tgisinternal
			  AND trigger_definition.tgenabled <> 'D'
		)`).Scan(&missingTriggers)
	if err != nil {
		return fmt.Errorf("inspect postgres live schema triggers: %w", err)
	}
	if missingTriggers != 0 {
		return fmt.Errorf("postgres live schema is incomplete: %d required triggers are missing or disabled", missingTriggers)
	}
	return nil
}

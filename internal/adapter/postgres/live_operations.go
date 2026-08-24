package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// LiveOperationsRepository 从 PostgreSQL 权威账本构建实盘监控所需的本地只读视图。
type LiveOperationsRepository struct {
	db *sql.DB
}

var _ port.LiveOperationsRepository = (*LiveOperationsRepository)(nil)

// NewLiveOperationsRepository 创建实盘运维聚合仓储。
func NewLiveOperationsRepository(db *sql.DB) (*LiveOperationsRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &LiveOperationsRepository{db: db}, nil
}

// LoadLiveOperations 在一个只读可重复读事务中加载账本、风险、运行状态和最近事件。
func (repository *LiveOperationsRepository) LoadLiveOperations(ctx context.Context, query domain.LiveOperationsQuery) (domain.LiveOperationsLocalState, error) {
	query, err := normalizeLiveOperationsQuery(query)
	if err != nil {
		return domain.LiveOperationsLocalState{}, err
	}
	tx, err := repository.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return domain.LiveOperationsLocalState{}, fmt.Errorf("begin live operations snapshot: %w", err)
	}
	defer tx.Rollback()
	state := domain.LiveOperationsLocalState{}
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&state.DatabaseObservedAt); err != nil {
		return state, fmt.Errorf("read database observation time: %w", err)
	}
	accountClause, accountArgs := liveOperationsAccountClause(query.ExecutionAccountIDs, 1)
	if state.Accounts, err = loadLiveAccounts(ctx, tx, accountClause, accountArgs); err != nil {
		return state, err
	}
	if len(state.Accounts) != len(query.ExecutionAccountIDs) {
		return state, fmt.Errorf("loaded %d execution accounts, want %d", len(state.Accounts), len(query.ExecutionAccountIDs))
	}
	if state.WalletAccounting, err = loadLiveWalletAccounting(ctx, tx, accountClause, accountArgs, query.ObservedAt); err != nil {
		return state, err
	}
	if len(state.WalletAccounting) != len(query.ExecutionAccountIDs) {
		return state, fmt.Errorf("loaded %d wallet accounting rows, want %d", len(state.WalletAccounting), len(query.ExecutionAccountIDs))
	}
	if state.Positions, err = loadLivePositions(ctx, tx, accountClause, accountArgs); err != nil {
		return state, err
	}
	if state.Orders, err = loadLiveOrders(ctx, tx, liveOrdersParams{accountClause: accountClause, accountArgs: accountArgs, since: query.RecentOrderSince}); err != nil {
		return state, err
	}
	if state.RiskPolicies, err = loadLiveRiskPolicies(ctx, tx, accountClause, accountArgs); err != nil {
		return state, err
	}
	if state.Reconciliations, err = loadLiveReconciliations(ctx, tx, accountClause, accountArgs); err != nil {
		return state, err
	}
	if state.Workers, err = loadLiveWorkers(ctx, tx, query.RunID); err != nil {
		return state, err
	}
	if state.Funnel, err = loadLiveFunnel(ctx, tx, query.RunID); err != nil {
		return state, err
	}
	if state.Events, err = loadLiveEvents(ctx, tx, liveEventsParams{accountArgs: accountArgs, since: query.RecentOrderSince, limit: query.EventLimit}); err != nil {
		return state, err
	}
	if state.ConfirmedTradeIDs, err = loadLiveConfirmedTradeIDs(ctx, tx, accountClause, accountArgs, query.RecentOrderSince); err != nil {
		return state, err
	}
	if state.RealizedPnLToday, state.FeeToday, state.DailyTradedNotional, err = loadLiveDailyAccounting(ctx, tx, accountClause, accountArgs, query.DayStart, query.ObservedAt); err != nil {
		return state, err
	}
	if err := tx.Commit(); err != nil {
		return state, fmt.Errorf("commit live operations snapshot: %w", err)
	}
	state.DatabaseObservedAt = state.DatabaseObservedAt.UTC()
	return state, nil
}

// loadLiveWalletAccounting 以不可变仓位事件重放每个账户的累计投入、成本占用峰值和已实现收益。
// 峰值是 managed portfolio 历史同时占用的最大成本，累计投入则包含每次开仓、接管和外部 BUY
// 导入的正向成本。两者均包含已经进入成本账本的买入手续费。
func loadLiveWalletAccounting(
	ctx context.Context,
	tx *sql.Tx,
	clause string,
	accountArgs []any,
	observedAt time.Time,
) ([]domain.LiveWalletAccountingState, error) {
	args := append(append([]any{}, accountArgs...), observedAt.UTC())
	observedAtIndex := len(args)
	statement := fmt.Sprintf(`
		WITH scoped_events AS (
			SELECT event.execution_account_id, event.position_event_id, event.occurred_at,
			       event.event_type, event.cost_basis_delta, event.realized_pnl_delta
			FROM position_events AS event
			WHERE event.execution_account_id IN %s AND event.occurred_at <= $%d
		), running AS (
			SELECT execution_account_id, position_event_id, occurred_at,
			       event_type, cost_basis_delta, realized_pnl_delta,
			       SUM(cost_basis_delta) OVER (
			           PARTITION BY execution_account_id
			           ORDER BY occurred_at, position_event_id
			           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			       ) AS deployed_cost
			FROM scoped_events
		), accounting AS (
			SELECT execution_account_id,
			       COALESCE(MAX(GREATEST(deployed_cost, 0)), 0) AS peak_cash_used,
			       COALESCE(SUM(CASE
			           WHEN event_type IN ('BOUGHT','ADOPTED','EXTERNAL_BUY_IMPORTED')
			                AND cost_basis_delta > 0 THEN cost_basis_delta ELSE 0 END), 0)
			           AS cumulative_invested_cost,
			       COALESCE(SUM(realized_pnl_delta), 0) AS realized_pnl
			FROM running GROUP BY execution_account_id
		)
		SELECT account_row.execution_account_id,
		       COALESCE(accounting.peak_cash_used, 0)::text,
		       COALESCE(accounting.cumulative_invested_cost, 0)::text,
		       COALESCE(accounting.realized_pnl, 0)::text
		FROM execution_accounts AS account_row
		LEFT JOIN accounting USING (execution_account_id)
		WHERE account_row.execution_account_id IN %s
		ORDER BY account_row.execution_account_id`, clause, observedAtIndex, clause)
	rows, err := tx.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("query live wallet accounting: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveWalletAccountingState, 0, len(accountArgs))
	for rows.Next() {
		var item domain.LiveWalletAccountingState
		if err := rows.Scan(
			&item.ExecutionAccountID, &item.PeakCashUsed,
			&item.CumulativeInvestedCost, &item.RealizedPnL,
		); err != nil {
			return nil, fmt.Errorf("scan live wallet accounting: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live wallet accounting: %w", err)
	}
	return result, nil
}

// normalizeLiveOperationsQuery 校验并规范化运维查询范围。
func normalizeLiveOperationsQuery(query domain.LiveOperationsQuery) (domain.LiveOperationsQuery, error) {
	seen := make(map[string]struct{}, len(query.ExecutionAccountIDs))
	accounts := make([]string, 0, len(query.ExecutionAccountIDs))
	for _, raw := range query.ExecutionAccountIDs {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return query, fmt.Errorf("execution account id is required")
		}
		if _, duplicate := seen[accountID]; duplicate {
			continue
		}
		seen[accountID] = struct{}{}
		accounts = append(accounts, accountID)
	}
	if len(accounts) == 0 {
		return query, fmt.Errorf("at least one execution account is required")
	}
	if query.ObservedAt.IsZero() || query.DayStart.IsZero() || query.RecentOrderSince.IsZero() {
		return query, fmt.Errorf("live operations time range is required")
	}
	if query.EventLimit == 0 {
		query.EventLimit = 50
	}
	if query.EventLimit < 1 || query.EventLimit > 200 {
		return query, fmt.Errorf("live operations event limit must be between 1 and 200")
	}
	query.ExecutionAccountIDs = accounts
	query.RunID = strings.TrimSpace(query.RunID)
	if query.RunID == "" {
		return query, fmt.Errorf("live operations run_id is required")
	}
	query.ObservedAt = query.ObservedAt.UTC()
	query.DayStart = query.DayStart.UTC()
	query.RecentOrderSince = query.RecentOrderSince.UTC()
	return query, nil
}

// liveOperationsAccountClause 构建仅包含绑定参数的账户过滤条件。
func liveOperationsAccountClause(accountIDs []string, firstIndex int) (string, []any) {
	placeholders := make([]string, len(accountIDs))
	args := make([]any, len(accountIDs))
	for index, accountID := range accountIDs {
		placeholders[index] = fmt.Sprintf("$%d", firstIndex+index)
		args[index] = accountID
	}
	return "(" + strings.Join(placeholders, ",") + ")", args
}

// loadLiveAccounts 加载配置账户的本地资金账本，不把钱包地址暴露给 HTTP 层。
func loadLiveAccounts(ctx context.Context, tx *sql.Tx, clause string, args []any) ([]domain.LiveAccountState, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT execution_account_id, wallet_address, collateral_asset,
		       total_balance::text, available_balance::text, reserved_balance::text, updated_at
		FROM execution_accounts
		WHERE execution_account_id IN `+clause+`
		ORDER BY execution_account_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("query live execution accounts: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveAccountState, 0, len(args))
	for rows.Next() {
		var item domain.LiveAccountState
		if err := rows.Scan(&item.ExecutionAccountID, &item.WalletAddress, &item.CollateralAsset, &item.TotalBalance, &item.AvailableBalance, &item.ReservedBalance, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan live execution account: %w", err)
		}
		item.UpdatedAt = item.UpdatedAt.UTC()
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live execution accounts: %w", err)
	}
	return result, nil
}

// loadLivePositions 加载非零本地仓位，并从开放批次和最近订单补充策略与市场展示信息。
func loadLivePositions(ctx context.Context, tx *sql.Tx, clause string, args []any) ([]domain.LiveLedgerPosition, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT position.execution_account_id, position.market_id, position.condition_id,
		       position.token_id, position.outcome_index, position.outcome_name,
		       position.total_shares::text, position.available_shares::text,
		       position.reserved_shares::text, position.cost_basis::text,
		       position.average_cost_price::text, position.realized_pnl::text,
		       COALESCE(position.mark_price::text, ''), position.market_value::text,
		       position.unrealized_pnl::text, position.is_dust, position.lifecycle_status,
		       position.last_marked_at, position.updated_at, position.version,
		       COALESCE(lot.model_id, ''), COALESCE(lot.strategy_id, ''),
		       COALESCE(label.market_label, ''), signal.signal_at
		FROM execution_positions AS position
		LEFT JOIN LATERAL (
			SELECT string_agg(
			           DISTINCT COALESCE(route.logical_model_id, lot_row.model_id), ','
			           ORDER BY COALESCE(route.logical_model_id, lot_row.model_id)
			       ) AS model_id,
			       string_agg(
			           DISTINCT execution_canonical_strategy_id(lot_row.strategy_id), ','
			           ORDER BY execution_canonical_strategy_id(lot_row.strategy_id)
			       ) AS strategy_id
			FROM position_lots AS lot_row
			LEFT JOIN position_lot_model_routes AS route ON route.lot_id=lot_row.lot_id
			WHERE lot_row.execution_account_id=position.execution_account_id AND lot_row.token_id=position.token_id
			  AND lot_row.status IN ('OPEN','SETTLED_PENDING_REDEEM')
		) AS lot ON TRUE
		LEFT JOIN LATERAL (
			SELECT intent->'metadata'->>'market_question' AS market_label
			FROM execution_orders
			WHERE execution_account_id=position.execution_account_id AND token_id=position.token_id
			  AND COALESCE(intent->'metadata'->>'market_question', '') <> ''
			ORDER BY created_at DESC, order_id DESC LIMIT 1
		) AS label ON TRUE
		LEFT JOIN LATERAL (
			SELECT NULLIF(intent->>'signal_at', '')::timestamptz AS signal_at
			FROM execution_orders
			WHERE execution_account_id=position.execution_account_id AND token_id=position.token_id
			  AND NULLIF(intent->>'signal_at', '') IS NOT NULL
			ORDER BY created_at DESC, order_id DESC LIMIT 1
		) AS signal ON TRUE
		WHERE position.execution_account_id IN `+clause+` AND position.total_shares > 0
		ORDER BY position.execution_account_id, position.market_id, position.token_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("query live positions: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveLedgerPosition, 0)
	for rows.Next() {
		var item domain.LiveLedgerPosition
		var outcomeIndex sql.NullInt64
		var markPrice string
		var lastMarked, signalAt sql.NullTime
		var lifecycle string
		position := &item.Position
		if err := rows.Scan(
			&position.ExecutionAccountID, &position.MarketID, &position.ConditionID,
			&position.TokenID, &outcomeIndex, &position.OutcomeName,
			&position.TotalShares, &position.AvailableShares, &position.ReservedShares,
			&position.CostBasis, &position.AverageCostPrice, &position.RealizedPnL,
			&markPrice, &position.MarketValue, &position.UnrealizedPnL,
			&position.IsDust, &lifecycle, &lastMarked, &position.UpdatedAt, &position.Revision,
			&item.ModelID, &item.StrategyID, &item.MarketLabel, &signalAt,
		); err != nil {
			return nil, fmt.Errorf("scan live position: %w", err)
		}
		if outcomeIndex.Valid {
			value := int(outcomeIndex.Int64)
			position.OutcomeIndex = &value
		}
		position.MarkPrice = domain.Decimal(markPrice)
		position.LifecycleStatus = domain.PositionLifecycleStatus(lifecycle)
		if lastMarked.Valid {
			value := lastMarked.Time.UTC()
			position.LastMarkedAt = &value
		}
		if signalAt.Valid {
			value := signalAt.Time.UTC()
			item.LatestSignalAt = &value
		}
		position.UpdatedAt = position.UpdatedAt.UTC()
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live positions: %w", err)
	}
	return result, nil
}

// liveOrdersParams 收拢当前订单查询范围。
type liveOrdersParams struct {
	accountClause string
	accountArgs   []any
	since         time.Time
}

// loadLiveOrders 加载活动订单及最近已成交订单，并批量读取完整生命周期。
func loadLiveOrders(ctx context.Context, tx *sql.Tx, params liveOrdersParams) ([]domain.LiveLedgerOrder, error) {
	args := append(append([]any{}, params.accountArgs...), params.since)
	sinceIndex := len(args)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT order_id, intent, market_validation, venue_order_id, status,
		       filled_size::text, filled_notional::text, total_fees::text,
		       COALESCE(average_fill_price::text, ''), failure_code, failure_reason,
		       created_at, updated_at, venue_last_observed_at, revision,
		       COALESCE(NULLIF(intent->'metadata'->>'market_question', ''), (
		           SELECT label_order.intent->'metadata'->>'market_question'
		           FROM execution_orders label_order
		           WHERE label_order.execution_account_id=execution_orders.execution_account_id
		             AND label_order.market_id=execution_orders.market_id
		             AND label_order.token_id=execution_orders.token_id
		             AND COALESCE(label_order.intent->'metadata'->>'market_question', '') <> ''
		           ORDER BY label_order.created_at DESC, label_order.order_id DESC LIMIT 1
		       ), '')
		FROM execution_orders
		WHERE execution_account_id IN %s
		  AND (status IN ('RECEIVED','VALIDATING','RESERVED','SUBMITTING','ACKNOWLEDGED','LIVE','PARTIALLY_FILLED','UNKNOWN','CANCEL_PENDING','RECONCILING')
		       OR (status='FILLED' AND updated_at >= $%d))
		ORDER BY updated_at DESC, order_id DESC
		LIMIT 200`, params.accountClause, sinceIndex), args...)
	if err != nil {
		return nil, fmt.Errorf("query live orders: %w", err)
	}
	result := make([]domain.LiveLedgerOrder, 0)
	for rows.Next() {
		var item domain.LiveLedgerOrder
		var intentJSON, validationJSON []byte
		var status string
		var venueObserved sql.NullTime
		order := &item.Order
		if err := rows.Scan(
			&order.ID, &intentJSON, &validationJSON, &order.VenueOrderID, &status,
			&order.FilledSize, &order.FilledNotional, &order.TotalFees, &order.AverageFillPrice,
			&order.FailureCode, &order.FailureReason, &order.CreatedAt, &order.UpdatedAt,
			&venueObserved, &order.Revision, &item.MarketLabel,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan live order: %w", err)
		}
		if err := json.Unmarshal(intentJSON, &order.Intent); err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode live order intent: %w", err)
		}
		if len(validationJSON) > 0 && string(validationJSON) != "null" {
			var validation domain.MarketValidation
			if err := json.Unmarshal(validationJSON, &validation); err != nil {
				rows.Close()
				return nil, fmt.Errorf("decode live order validation: %w", err)
			}
			order.MarketValidation = &validation
		}
		order.Status = domain.OrderStatus(status)
		order.CreatedAt, order.UpdatedAt = order.CreatedAt.UTC(), order.UpdatedAt.UTC()
		if venueObserved.Valid {
			value := venueObserved.Time.UTC()
			order.VenueLastObservedAt = &value
		}
		result = append(result, item)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close live order rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live orders: %w", err)
	}
	if len(result) == 0 {
		return result, nil
	}
	orderIDs := make([]string, len(result))
	indexByID := make(map[string]int, len(result))
	for index, item := range result {
		orderIDs[index], indexByID[item.Order.ID] = item.Order.ID, index
	}
	clause, eventArgs := liveOperationsAccountClause(orderIDs, 1)
	eventRows, err := tx.QueryContext(ctx, `
		SELECT event_id, order_id, revision, from_status, to_status, trigger,
		       attempt_id, fill_key, reason_code, reason, venue_status, venue_order_id,
		       filled_size::text, filled_notional::text, total_fees::text,
		       COALESCE(average_fill_price::text, ''), venue_observed_at, occurred_at
		FROM execution_order_events WHERE order_id IN `+clause+`
		ORDER BY order_id, revision`, eventArgs...)
	if err != nil {
		return nil, fmt.Errorf("query live order events: %w", err)
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var event domain.OrderEvent
		var from, to, trigger string
		var venueObserved sql.NullTime
		if err := eventRows.Scan(
			&event.ID, &event.OrderID, &event.Revision, &from, &to, &trigger,
			&event.AttemptID, &event.FillKey, &event.ReasonCode, &event.Reason,
			&event.VenueStatus, &event.VenueOrderID, &event.FilledSize,
			&event.FilledNotional, &event.TotalFees, &event.FillPrice,
			&venueObserved, &event.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan live order event: %w", err)
		}
		event.FromStatus, event.ToStatus = domain.OrderStatus(from), domain.OrderStatus(to)
		event.Trigger = domain.OrderTransitionTrigger(trigger)
		event.OccurredAt = event.OccurredAt.UTC()
		if venueObserved.Valid {
			value := venueObserved.Time.UTC()
			event.VenueObservedAt = &value
		}
		if index, found := indexByID[event.OrderID]; found {
			result[index].Events = append(result[index].Events, event)
		}
	}
	if err := eventRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live order events: %w", err)
	}
	for index := range result {
		if result[index].Events == nil {
			result[index].Events = []domain.OrderEvent{}
		}
	}
	return result, nil
}

// loadLiveRiskPolicies 加载每个账户当前的风控限额、Kill Switch 和暂停状态。
func loadLiveRiskPolicies(ctx context.Context, tx *sql.Tx, clause string, args []any) ([]domain.LiveRiskPolicyState, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT account_row.execution_account_id, COALESCE(policy.policy_id, ''),
		       COALESCE(policy.enabled, FALSE), COALESCE(policy.max_market_exposure, 0)::text,
		       COALESCE(policy.max_wallet_exposure, 0)::text,
		       COALESCE(policy.max_daily_traded_notional, 0)::text,
		       COALESCE(policy.max_signal_age_ms, 1), COALESCE(policy.max_state_age_ms, 1),
		       COALESCE(global_control.kill_switch, TRUE), COALESCE(account_control.paused, TRUE)
		FROM execution_accounts AS account_row
		LEFT JOIN execution_risk_policies AS policy USING (execution_account_id)
		LEFT JOIN execution_risk_controls AS account_control
		  ON account_control.execution_account_id=account_row.execution_account_id
		 AND account_control.control_scope='ACCOUNT' AND account_control.control_key=''
		LEFT JOIN execution_risk_global_control AS global_control ON global_control.singleton=TRUE
		WHERE account_row.execution_account_id IN `+clause+`
		ORDER BY account_row.execution_account_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("query live risk policies: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveRiskPolicyState, 0, len(args))
	for rows.Next() {
		var item domain.LiveRiskPolicyState
		var maxSignalAgeMS, maxStateAgeMS int64
		if err := rows.Scan(&item.ExecutionAccountID, &item.PolicyID, &item.Enabled, &item.MaxMarketExposure, &item.MaxWalletExposure, &item.MaxDailyTradedNotional, &maxSignalAgeMS, &maxStateAgeMS, &item.KillSwitch, &item.AccountPaused); err != nil {
			return nil, fmt.Errorf("scan live risk policy: %w", err)
		}
		item.MaxSignalAge = time.Duration(maxSignalAgeMS) * time.Millisecond
		item.MaxStateAge = time.Duration(maxStateAgeMS) * time.Millisecond
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live risk policies: %w", err)
	}
	return result, nil
}

// loadLiveReconciliations 加载每个账户最近一次对账及当前未关闭问题数量。
func loadLiveReconciliations(ctx context.Context, tx *sql.Tx, clause string, args []any) ([]domain.LiveReconciliationState, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT account_row.execution_account_id,
		       COALESCE(run.run_id, ''), COALESCE(run.trigger, ''), COALESCE(run.focus_order_id, ''),
		       COALESCE(run.status, ''), run.started_at, run.completed_at,
		       COALESCE(run.summary, '{}'::jsonb), COALESCE(run.error, ''),
		       (SELECT count(*) FROM reconciliation_issues issue
		        WHERE issue.execution_account_id=account_row.execution_account_id AND issue.status='OPEN')
		FROM execution_accounts AS account_row
		LEFT JOIN LATERAL (
			SELECT * FROM reconciliation_runs
			WHERE execution_account_id=account_row.execution_account_id
			ORDER BY started_at DESC, run_id DESC LIMIT 1
		) AS run ON TRUE
		WHERE account_row.execution_account_id IN `+clause+`
		ORDER BY account_row.execution_account_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("query live reconciliation state: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveReconciliationState, 0, len(args))
	for rows.Next() {
		var item domain.LiveReconciliationState
		var trigger, status string
		var started, completed sql.NullTime
		var summaryJSON []byte
		if err := rows.Scan(
			&item.ExecutionAccountID, &item.Run.RunID, &trigger, &item.Run.FocusOrderID,
			&status, &started, &completed, &summaryJSON, &item.Run.Error, &item.OpenIssues,
		); err != nil {
			return nil, fmt.Errorf("scan live reconciliation state: %w", err)
		}
		item.Run.ExecutionAccountID = item.ExecutionAccountID
		item.Run.Trigger = domain.ReconciliationTrigger(trigger)
		item.Run.Status = domain.ReconciliationRunStatus(status)
		item.Run.Summary = map[string]int{}
		if len(summaryJSON) > 0 {
			if err := json.Unmarshal(summaryJSON, &item.Run.Summary); err != nil {
				return nil, fmt.Errorf("decode live reconciliation summary: %w", err)
			}
		}
		if started.Valid {
			item.Run.StartedAt = started.Time.UTC()
		}
		if completed.Valid {
			value := completed.Time.UTC()
			item.Run.CompletedAt = &value
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live reconciliation state: %w", err)
	}
	return result, nil
}

// loadLiveWorkers 读取三个业务线程持久化的 heartbeat。
func loadLiveWorkers(ctx context.Context, tx *sql.Tx, runID string) ([]domain.LiveWorkerState, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT thread_id, run_id, name, purpose, cadence, max_heartbeat_age_ms,
		       last_heartbeat_at, current_task, current_cycle_id, last_error,
		       metric_label, metric_value, stopped
		FROM live_runtime_status WHERE run_id=$1 ORDER BY thread_id`, runID)
	if err != nil {
		return nil, fmt.Errorf("query live worker status: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveWorkerState, 0, 3)
	for rows.Next() {
		var item domain.LiveWorkerState
		var maxAgeMS int64
		if err := rows.Scan(&item.ThreadID, &item.RunID, &item.Name, &item.Purpose, &item.Cadence, &maxAgeMS, &item.LastHeartbeatAt, &item.CurrentTask, &item.CurrentCycleID, &item.LastError, &item.MetricLabel, &item.MetricValue, &item.Stopped); err != nil {
			return nil, fmt.Errorf("scan live worker status: %w", err)
		}
		item.MaxHeartbeatAge = time.Duration(maxAgeMS) * time.Millisecond
		item.LastHeartbeatAt = item.LastHeartbeatAt.UTC()
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live worker status: %w", err)
	}
	return result, nil
}

// loadLiveFunnel 只读取最近一个 run_id/cycle_id 的六步漏斗。
func loadLiveFunnel(ctx context.Context, tx *sql.Tx, runID string) ([]domain.LiveFunnelStage, error) {
	rows, err := tx.QueryContext(ctx, `
		WITH latest AS (
			SELECT run_id, cycle_id FROM live_cycle_funnel
			WHERE run_id=$1
			ORDER BY observed_at DESC, cycle_id DESC LIMIT 1
		)
		SELECT stage.stage_id, stage.stage_index, stage.name, stage.description,
		       stage.count, stage.throughput_label, stage.state
		FROM live_cycle_funnel AS stage JOIN latest USING (run_id, cycle_id)
		ORDER BY stage.stage_index`, runID)
	if err != nil {
		return nil, fmt.Errorf("query live cycle funnel: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveFunnelStage, 0, 6)
	for rows.Next() {
		var item domain.LiveFunnelStage
		if err := rows.Scan(&item.ID, &item.Index, &item.Name, &item.Description, &item.Count, &item.ThroughputLabel, &item.State); err != nil {
			return nil, fmt.Errorf("scan live cycle funnel: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live cycle funnel: %w", err)
	}
	return result, nil
}

// liveEventsParams 收拢最近事件查询参数。
type liveEventsParams struct {
	accountArgs []any
	since       time.Time
	limit       int
}

// loadLiveEvents 合并订单、真实 Fill 和对账问题，按发生时间返回最近事件。
func loadLiveEvents(ctx context.Context, tx *sql.Tx, params liveEventsParams) ([]domain.LiveEvent, error) {
	accountIDs := stringArgs(params.accountArgs)
	firstClause, firstArgs := liveOperationsAccountClause(accountIDs, 1)
	secondClause, secondArgs := liveOperationsAccountClause(accountIDs, len(firstArgs)+1)
	thirdClause, thirdArgs := liveOperationsAccountClause(accountIDs, len(firstArgs)+len(secondArgs)+1)
	sinceIndex := len(firstArgs) + len(secondArgs) + len(thirdArgs) + 1
	limitIndex := sinceIndex + 1
	statement := fmt.Sprintf(`
		SELECT id, occurred_at, severity, thread, section, title, detail, market_label, order_id
		FROM (
			SELECT event.event_id AS id, event.occurred_at, 'info' AS severity,
			       'monitor' AS thread, 'order' AS section,
			       '订单状态变更' AS title,
			       CONCAT_WS(' ', event.from_status, '→', event.to_status, NULLIF(event.reason_code,'')) AS detail,
			       COALESCE(order_row.intent->'metadata'->>'market_question','') AS market_label,
			       event.order_id
			FROM execution_order_events event
			JOIN execution_orders order_row USING (order_id)
			WHERE order_row.execution_account_id IN %s AND event.occurred_at >= $%d
			UNION ALL
			SELECT fill.fill_key, fill.confirmed_at, 'success', 'monitor', 'fill',
			       '真实成交已验真入账',
			       CONCAT(fill.side, ' ', fill.shares::text, ' @ ', fill.price::text),
			       COALESCE(order_row.intent->'metadata'->>'market_question',''), fill.order_id
			FROM execution_fills fill JOIN execution_orders order_row USING (order_id)
			WHERE fill.execution_account_id IN %s AND fill.status='CONFIRMED'
			  AND fill.applied_at IS NOT NULL AND fill.confirmed_at IS NOT NULL
			  AND fill.confirmed_at >= $%d
			UNION ALL
			SELECT issue.issue_id, issue.observed_at,
			       CASE WHEN issue.status='OPEN' THEN 'warning' ELSE 'success' END,
			       'monitor', 'reconciliation', '对账状态变化',
			       CONCAT_WS(' / ', issue.issue_type, issue.resolution, issue.status),
			       '', NULLIF(issue.order_id,'')
			FROM reconciliation_issues issue
			WHERE issue.execution_account_id IN %s AND issue.observed_at >= $%d
		) event_union
		ORDER BY occurred_at DESC, id DESC LIMIT $%d`,
		firstClause, sinceIndex, secondClause, sinceIndex, thirdClause, sinceIndex, limitIndex)
	queryArgs := make([]any, 0, limitIndex)
	queryArgs = append(queryArgs, firstArgs...)
	queryArgs = append(queryArgs, secondArgs...)
	queryArgs = append(queryArgs, thirdArgs...)
	queryArgs = append(queryArgs, params.since.UTC())
	queryArgs = append(queryArgs, params.limit)
	rows, err := tx.QueryContext(ctx, statement, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query live events: %w", err)
	}
	defer rows.Close()
	result := make([]domain.LiveEvent, 0, params.limit)
	for rows.Next() {
		var item domain.LiveEvent
		var orderID sql.NullString
		if err := rows.Scan(&item.ID, &item.Timestamp, &item.Severity, &item.Thread, &item.Section, &item.Title, &item.Detail, &item.MarketLabel, &orderID); err != nil {
			return nil, fmt.Errorf("scan live event: %w", err)
		}
		item.Timestamp = item.Timestamp.UTC()
		if orderID.Valid && strings.TrimSpace(orderID.String) != "" {
			value := orderID.String
			item.OrderID = &value
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live events: %w", err)
	}
	return result, nil
}

// stringArgs 将绑定参数恢复成账户标识，用于生成后续占位符组。
func stringArgs(values []any) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index], _ = value.(string)
	}
	return result
}

// loadLiveConfirmedTradeIDs 加载已经通过真实 Fill 验真并入账的交易所成交标识。
func loadLiveConfirmedTradeIDs(ctx context.Context, tx *sql.Tx, clause string, accountArgs []any, since time.Time) (map[string]struct{}, error) {
	args := append(append([]any{}, accountArgs...), since.UTC())
	sinceIndex := len(args)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT execution_account_id, venue_fill_id
		FROM execution_fills
		WHERE execution_account_id IN %s AND status='CONFIRMED' AND applied_at IS NOT NULL
		  AND matched_at >= $%d`, clause, sinceIndex), args...)
	if err != nil {
		return nil, fmt.Errorf("query confirmed live trade identities: %w", err)
	}
	defer rows.Close()
	result := make(map[string]struct{})
	for rows.Next() {
		var accountID, tradeID string
		if err := rows.Scan(&accountID, &tradeID); err != nil {
			return nil, fmt.Errorf("scan confirmed live trade identity: %w", err)
		}
		result[accountID+"\x00"+strings.ToLower(strings.TrimSpace(tradeID))] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate confirmed live trade identities: %w", err)
	}
	return result, nil
}

// loadLiveDailyAccounting 汇总 UTC 当日已确认并入账 Fill 的手续费和已实现盈亏。
func loadLiveDailyAccounting(ctx context.Context, tx *sql.Tx, clause string, accountArgs []any, dayStart time.Time, observedAt time.Time) (domain.Decimal, domain.Decimal, domain.Decimal, error) {
	args := append(append([]any{}, accountArgs...), dayStart.UTC(), observedAt.UTC())
	startIndex, endIndex := len(args)-1, len(args)
	statement := fmt.Sprintf(`
		WITH accounting AS (
			SELECT COALESCE(SUM(closure.realized_pnl), 0) AS realized_pnl,
			       COALESCE(SUM(fill.total_fee), 0) AS fee
			FROM execution_fills fill
			LEFT JOIN (
				SELECT closing_fill_key, SUM(realized_pnl) AS realized_pnl
				FROM position_lot_closures GROUP BY closing_fill_key
			) closure ON closure.closing_fill_key=fill.fill_key
			WHERE fill.execution_account_id IN %s
			  AND fill.status='CONFIRMED' AND fill.applied_at IS NOT NULL
			  AND fill.matched_at >= $%d AND fill.matched_at <= $%d
		), risk_confirmed AS (
			SELECT COALESCE(SUM(fill.gross_notional), 0) AS traded_notional
			FROM execution_fills fill
			JOIN execution_risk_policies policy USING (execution_account_id)
			WHERE fill.execution_account_id IN %s
			  AND fill.status='CONFIRMED' AND fill.applied_at IS NOT NULL
			  AND fill.matched_at >= (
			      date_trunc('day', $%d::timestamptz AT TIME ZONE policy.daily_timezone)
			      AT TIME ZONE policy.daily_timezone
			  ) AND fill.matched_at <= $%d
		), pending AS (
			SELECT COALESCE(SUM(GREATEST(daily_risk_notional - settled_notional, 0)), 0) AS traded_notional
			FROM asset_reservations
			WHERE execution_account_id IN %s
			  AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
		)
		SELECT accounting.realized_pnl::text, accounting.fee::text,
		       (risk_confirmed.traded_notional + pending.traded_notional)::text
		FROM accounting CROSS JOIN risk_confirmed CROSS JOIN pending`,
		clause, startIndex, endIndex, clause, endIndex, endIndex, clause)
	var realized, fees, traded domain.Decimal
	if err := tx.QueryRowContext(ctx, statement, args...).Scan(&realized, &fees, &traded); err != nil {
		return "", "", "", fmt.Errorf("query live daily accounting: %w", err)
	}
	return realized, fees, traded, nil
}

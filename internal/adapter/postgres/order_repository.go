package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// OrderRepository 表示后端使用的 OrderRepository 类型。
type OrderRepository struct {
	db *sql.DB
}

var (
	_ port.OrderRepository               = (*OrderRepository)(nil)
	_ port.ReconciliationOrderRepository = (*OrderRepository)(nil)
)

// NewOrderRepository 创建并初始化 Order Repository。
func NewOrderRepository(db *sql.DB) (*OrderRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &OrderRepository{db: db}, nil
}

// Create 幂等创建并持久化新的订单记录。
func (repository *OrderRepository) Create(ctx context.Context, order domain.Order) (domain.Order, bool, error) {
	intentJSON, validationJSON, err := marshalOrderJSON(order)
	if err != nil {
		return domain.Order{}, false, err
	}
	tx, err := repository.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.Order{}, false, fmt.Errorf("begin order create: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO execution_orders (
			order_id, client_order_id, execution_account_id, venue, market_id,
			token_id, intent, market_validation, venue_order_id, status,
			filled_size, filled_notional, total_fees, average_fill_price, failure_code, failure_reason,
			venue_last_observed_at, revision, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10,
			$11::numeric, $12::numeric, $13::numeric, NULLIF($14, '')::numeric,
			$15, $16, $17, $18, $19, $20
		)
		ON CONFLICT (client_order_id) DO NOTHING`,
		order.ID, order.Intent.ClientOrderID, order.Intent.ExecutionAccountID,
		order.Intent.Venue, order.Intent.MarketID, order.Intent.TokenID,
		intentJSON, validationJSON, order.VenueOrderID, string(order.Status),
		decimalOrZero(order.FilledSize), decimalOrZero(order.FilledNotional),
		decimalOrZero(order.TotalFees), order.AverageFillPrice.String(),
		order.FailureCode, order.FailureReason, order.VenueLastObservedAt,
		order.Revision, order.CreatedAt, order.UpdatedAt)
	if err != nil {
		return domain.Order{}, false, fmt.Errorf("insert execution order: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return domain.Order{}, false, fmt.Errorf("inspect order insert: %w", err)
	}
	if rows == 0 {
		existing, err := selectOrderByClientID(ctx, tx, order.Intent.ClientOrderID)
		if err != nil {
			return domain.Order{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return domain.Order{}, false, fmt.Errorf("commit existing order read: %w", err)
		}
		return existing, false, nil
	}
	initialEvent := domain.OrderEvent{
		ID:             "event:" + order.ID + ":1",
		OrderID:        order.ID,
		Revision:       order.Revision,
		ToStatus:       order.Status,
		Trigger:        domain.TransitionTriggerReceived,
		FilledSize:     order.FilledSize,
		FilledNotional: order.FilledNotional,
		TotalFees:      order.TotalFees,
		OccurredAt:     order.CreatedAt,
	}
	if err := insertOrderEvent(ctx, tx, initialEvent); err != nil {
		return domain.Order{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Order{}, false, fmt.Errorf("commit order create: %w", err)
	}
	return domain.CloneOrder(order), true, nil
}

// Get 按标识查询并返回当前组件管理的记录。
func (repository *OrderRepository) Get(ctx context.Context, orderID string) (domain.Order, error) {
	return selectOrder(ctx, repository.db, `WHERE order_id = $1`, strings.TrimSpace(orderID))
}

// GetByClientOrderID 按客户端订单幂等键查询订单。
func (repository *OrderRepository) GetByClientOrderID(ctx context.Context, clientOrderID string) (domain.Order, error) {
	return selectOrder(ctx, repository.db, `WHERE client_order_id = $1`, strings.TrimSpace(clientOrderID))
}

// Transition 以乐观锁原子持久化订单状态和审计事件。
func (repository *OrderRepository) Transition(ctx context.Context, order domain.Order, event domain.OrderEvent) error {
	return repository.writeTransition(ctx, order, event, nil, false)
}

// StartAttempt 原子持久化外部操作开始状态、订单迁移和尝试记录。
func (repository *OrderRepository) StartAttempt(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error {
	return repository.writeTransition(ctx, order, event, &attempt, true)
}

// FinishAttempt 原子持久化外部操作结果、订单迁移和尝试记录。
func (repository *OrderRepository) FinishAttempt(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt domain.OrderAttempt) error {
	return repository.writeTransition(ctx, order, event, &attempt, false)
}

// writeTransition 写入 Transition。
func (repository *OrderRepository) writeTransition(ctx context.Context, order domain.Order, event domain.OrderEvent, attempt *domain.OrderAttempt, start bool) error {
	intentJSON, validationJSON, err := marshalOrderJSON(order)
	if err != nil {
		return err
	}
	tx, err := repository.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin order transition: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_orders
		SET intent = $4::jsonb,
		    market_validation = $5::jsonb,
		    venue_order_id = $6,
		    status = $7,
		    filled_size = $8::numeric,
		    filled_notional = $9::numeric,
		    total_fees = $10::numeric,
		    average_fill_price = NULLIF($11, '')::numeric,
		    failure_code = $12,
		    failure_reason = $13,
		    venue_last_observed_at = $14,
		    revision = $3,
		    updated_at = $15
		WHERE order_id = $1 AND revision = $2 AND status = $16`,
		order.ID, order.Revision-1, order.Revision, intentJSON, validationJSON,
		order.VenueOrderID, string(order.Status), decimalOrZero(order.FilledSize),
		decimalOrZero(order.FilledNotional), decimalOrZero(order.TotalFees),
		order.AverageFillPrice.String(), order.FailureCode, order.FailureReason,
		order.VenueLastObservedAt, order.UpdatedAt, string(event.FromStatus))
	if err != nil {
		return fmt.Errorf("update execution order: %w", err)
	}
	if !oneRow(result) {
		return port.ErrOrderRevisionConflict
	}
	if err := insertOrderEvent(ctx, tx, event); err != nil {
		return err
	}
	if attempt != nil {
		if start {
			if err := insertOrderAttempt(ctx, tx, *attempt); err != nil {
				return err
			}
		} else if err := finishOrderAttempt(ctx, tx, *attempt); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit order transition: %w", err)
	}
	return nil
}

// Events 查询指定订单的不可变状态事件。
func (repository *OrderRepository) Events(ctx context.Context, orderID string) ([]domain.OrderEvent, error) {
	rows, err := repository.db.QueryContext(ctx, `
		SELECT event_id, order_id, revision, from_status, to_status, trigger,
		       attempt_id, fill_key, reason_code, reason, venue_status, venue_order_id,
		       filled_size::text, filled_notional::text, total_fees::text,
		       COALESCE(average_fill_price::text, ''),
		       venue_observed_at, occurred_at
		FROM execution_order_events
		WHERE order_id = $1
		ORDER BY revision`, strings.TrimSpace(orderID))
	if err != nil {
		return nil, fmt.Errorf("query order events: %w", err)
	}
	defer rows.Close()
	var events []domain.OrderEvent
	for rows.Next() {
		var event domain.OrderEvent
		var from, to, trigger string
		var venueObserved sql.NullTime
		if err := rows.Scan(
			&event.ID, &event.OrderID, &event.Revision, &from, &to, &trigger,
			&event.AttemptID, &event.FillKey, &event.ReasonCode, &event.Reason, &event.VenueStatus,
			&event.VenueOrderID, &event.FilledSize, &event.FilledNotional, &event.TotalFees, &event.FillPrice,
			&venueObserved, &event.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan order event: %w", err)
		}
		event.FromStatus = domain.OrderStatus(from)
		event.ToStatus = domain.OrderStatus(to)
		event.Trigger = domain.OrderTransitionTrigger(trigger)
		if venueObserved.Valid {
			value := venueObserved.Time.UTC()
			event.VenueObservedAt = &value
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order events: %w", err)
	}
	if len(events) == 0 {
		if _, err := repository.Get(ctx, orderID); err != nil {
			return nil, err
		}
	}
	return events, nil
}

// Attempts 查询指定订单的外部操作尝试。
func (repository *OrderRepository) Attempts(ctx context.Context, orderID string) ([]domain.OrderAttempt, error) {
	rows, err := repository.db.QueryContext(ctx, `
		SELECT attempt_id, order_id, sequence, kind, outcome,
		       request_fingerprint, venue_order_id, venue_status,
		       COALESCE(http_status, 0), error_code, error_message,
		       started_at, completed_at
		FROM execution_order_attempts
		WHERE order_id = $1
		ORDER BY sequence`, strings.TrimSpace(orderID))
	if err != nil {
		return nil, fmt.Errorf("query order attempts: %w", err)
	}
	defer rows.Close()
	var attempts []domain.OrderAttempt
	for rows.Next() {
		var attempt domain.OrderAttempt
		var kind, outcome string
		var completed sql.NullTime
		if err := rows.Scan(
			&attempt.ID, &attempt.OrderID, &attempt.Sequence, &kind, &outcome,
			&attempt.RequestFingerprint, &attempt.VenueOrderID, &attempt.VenueStatus,
			&attempt.HTTPStatus, &attempt.ErrorCode, &attempt.ErrorMessage,
			&attempt.StartedAt, &completed,
		); err != nil {
			return nil, fmt.Errorf("scan order attempt: %w", err)
		}
		attempt.Kind = domain.OrderAttemptKind(kind)
		attempt.Outcome = domain.OrderAttemptOutcome(outcome)
		if completed.Valid {
			value := completed.Time.UTC()
			attempt.CompletedAt = &value
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order attempts: %w", err)
	}
	if len(attempts) == 0 {
		if _, err := repository.Get(ctx, orderID); err != nil {
			return nil, err
		}
	}
	return attempts, nil
}

// ListPending 分页查询需要协调器继续处理的非终态订单。
func (repository *OrderRepository) ListPending(ctx context.Context, before time.Time, limit int) ([]domain.Order, error) {
	if limit <= 0 || limit > 1000 {
		return nil, fmt.Errorf("pending-order limit must be between 1 and 1000")
	}
	rows, err := repository.db.QueryContext(ctx, selectOrderColumns+`
		WHERE status IN ('RECEIVED', 'VALIDATING', 'RESERVED',
		                 'SUBMITTING', 'ACKNOWLEDGED', 'LIVE', 'PARTIALLY_FILLED',
		                 'UNKNOWN', 'CANCEL_PENDING', 'RECONCILING')
		  AND updated_at <= $1
		ORDER BY updated_at, order_id
		LIMIT $2`, before.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("query pending orders: %w", err)
	}
	defer rows.Close()
	orders := make([]domain.Order, 0)
	for rows.Next() {
		order, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending orders: %w", err)
	}
	return orders, nil
}

// ListForReconciliation 查询本服务创建且需要参与对账的订单。
func (repository *OrderRepository) ListForReconciliation(
	ctx context.Context,
	executionAccountID string,
	updatedAfter time.Time,
) ([]domain.Order, error) {
	rows, err := repository.db.QueryContext(ctx, selectOrderColumns+`
		WHERE execution_account_id=$1
		  AND (
			status IN ('RECEIVED', 'VALIDATING', 'RESERVED', 'SUBMITTING',
			           'ACKNOWLEDGED', 'LIVE', 'PARTIALLY_FILLED', 'UNKNOWN',
			           'CANCEL_PENDING', 'RECONCILING')
			OR updated_at >= $2
		  )
		ORDER BY created_at, order_id`, strings.TrimSpace(executionAccountID), updatedAfter.UTC())
	if err != nil {
		return nil, fmt.Errorf("query reconciliation orders: %w", err)
	}
	defer rows.Close()
	orders := make([]domain.Order, 0)
	for rows.Next() {
		order, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reconciliation orders: %w", err)
	}
	return orders, nil
}

// insertOrderEvent 在当前事务中插入 Order Event。
func insertOrderEvent(ctx context.Context, tx *sql.Tx, event domain.OrderEvent) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO execution_order_events (
			event_id, order_id, revision, from_status, to_status, trigger,
			attempt_id, fill_key, reason_code, reason, venue_status, venue_order_id,
			filled_size, filled_notional, total_fees, average_fill_price, venue_observed_at, occurred_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13::numeric, $14::numeric, $15::numeric, NULLIF($16, '')::numeric, $17, $18
		)`,
		event.ID, event.OrderID, event.Revision, string(event.FromStatus),
		string(event.ToStatus), string(event.Trigger), event.AttemptID,
		event.FillKey, event.ReasonCode, event.Reason, event.VenueStatus, event.VenueOrderID,
		decimalOrZero(event.FilledSize), decimalOrZero(event.FilledNotional),
		decimalOrZero(event.TotalFees), event.FillPrice.String(),
		event.VenueObservedAt, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert order event: %w", err)
	}
	return nil
}

// insertOrderAttempt 在当前事务中插入 Order Attempt。
func insertOrderAttempt(ctx context.Context, tx *sql.Tx, attempt domain.OrderAttempt) error {
	if attempt.Outcome != domain.AttemptOutcomeStarted || attempt.CompletedAt != nil {
		return fmt.Errorf("new order attempt must be STARTED")
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO execution_order_attempts (
			attempt_id, order_id, sequence, kind, outcome,
			request_fingerprint, venue_order_id, venue_status, http_status,
			error_code, error_message, started_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, 0), $10, $11, $12, NULL)`,
		attempt.ID, attempt.OrderID, attempt.Sequence, string(attempt.Kind),
		string(attempt.Outcome), attempt.RequestFingerprint, attempt.VenueOrderID,
		attempt.VenueStatus, attempt.HTTPStatus, attempt.ErrorCode,
		attempt.ErrorMessage, attempt.StartedAt)
	if err != nil {
		return fmt.Errorf("insert order attempt: %w", err)
	}
	return nil
}

// finishOrderAttempt 完成并持久化 Order Attempt。
func finishOrderAttempt(ctx context.Context, tx *sql.Tx, attempt domain.OrderAttempt) error {
	if attempt.Outcome == domain.AttemptOutcomeStarted || attempt.CompletedAt == nil {
		return fmt.Errorf("completed order attempt requires terminal outcome and completed_at")
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_order_attempts
		SET outcome = $3, venue_order_id = $4, venue_status = $5,
		    http_status = NULLIF($6, 0), error_code = $7,
		    error_message = $8, completed_at = $9
		WHERE attempt_id = $1 AND order_id = $2 AND outcome = 'STARTED'`,
		attempt.ID, attempt.OrderID, string(attempt.Outcome),
		attempt.VenueOrderID, attempt.VenueStatus, attempt.HTTPStatus,
		attempt.ErrorCode, attempt.ErrorMessage, attempt.CompletedAt)
	if err != nil {
		return fmt.Errorf("finish order attempt: %w", err)
	}
	if !oneRow(result) {
		return port.ErrOrderRevisionConflict
	}
	return nil
}

const selectOrderColumns = `
	SELECT order_id, intent, market_validation, venue_order_id, status,
	       filled_size::text, filled_notional::text, total_fees::text,
	       COALESCE(average_fill_price::text, ''),
	       failure_code, failure_reason, created_at, updated_at,
	       venue_last_observed_at, revision
	FROM execution_orders `

// orderQuery 表示后端使用的 orderQuery 类型。
type orderQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// selectOrder 从 PostgreSQL 查询 Order。
func selectOrder(ctx context.Context, query orderQuery, clause string, argument any) (domain.Order, error) {
	row := query.QueryRowContext(ctx, selectOrderColumns+clause, argument)
	return scanOrder(row)
}

// selectOrderByClientID 从 PostgreSQL 查询 Order By Client 标识。
func selectOrderByClientID(ctx context.Context, tx *sql.Tx, clientOrderID string) (domain.Order, error) {
	return selectOrder(ctx, tx, `WHERE client_order_id = $1`, clientOrderID)
}

// orderRowScanner 表示后端使用的 orderRowScanner 类型。
type orderRowScanner interface {
	Scan(...any) error
}

// scanOrder 将数据库行扫描为 Order。
func scanOrder(row orderRowScanner) (domain.Order, error) {
	var order domain.Order
	var intentJSON, validationJSON []byte
	var status string
	var venueObserved sql.NullTime
	if err := row.Scan(
		&order.ID, &intentJSON, &validationJSON, &order.VenueOrderID, &status,
		&order.FilledSize, &order.FilledNotional, &order.TotalFees,
		&order.AverageFillPrice, &order.FailureCode,
		&order.FailureReason, &order.CreatedAt, &order.UpdatedAt,
		&venueObserved, &order.Revision,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Order{}, port.ErrOrderNotFound
		}
		return domain.Order{}, fmt.Errorf("scan execution order: %w", err)
	}
	if err := json.Unmarshal(intentJSON, &order.Intent); err != nil {
		return domain.Order{}, fmt.Errorf("decode persisted order intent: %w", err)
	}
	if len(validationJSON) > 0 && string(validationJSON) != "null" {
		var validation domain.MarketValidation
		if err := json.Unmarshal(validationJSON, &validation); err != nil {
			return domain.Order{}, fmt.Errorf("decode persisted market validation: %w", err)
		}
		order.MarketValidation = &validation
	}
	order.Status = domain.OrderStatus(status)
	if venueObserved.Valid {
		value := venueObserved.Time.UTC()
		order.VenueLastObservedAt = &value
	}
	return domain.CloneOrder(order), nil
}

// marshalOrderJSON 序列化 Order JSON 数据。
func marshalOrderJSON(order domain.Order) ([]byte, any, error) {
	intentJSON, err := json.Marshal(order.Intent.Normalize())
	if err != nil {
		return nil, nil, fmt.Errorf("marshal order intent: %w", err)
	}
	if order.MarketValidation == nil {
		return intentJSON, nil, nil
	}
	validationJSON, err := json.Marshal(order.MarketValidation)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal market validation: %w", err)
	}
	return intentJSON, validationJSON, nil
}

// decimalOrZero 将空十进制值规范化为零。
func decimalOrZero(value domain.Decimal) string {
	if value.IsEmpty() {
		return "0"
	}
	return value.String()
}

// ensure time import remains pinned in the public adapter's SQL scanning
// contract when database drivers return time.Time through interface values.
var _ = time.Time{}

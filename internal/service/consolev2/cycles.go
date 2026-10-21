package consolev2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const cycleRowsProjection = `
WITH cycle_rows AS (
	SELECT r.cycle_id, r.input_id, r.decision_at, r.model_id, r.strategy_id,
	       r.execution_account_id, r.created_at, r.decided_at, r.order_submission_enabled,
	       r.input_payload,
	       -- strategy_decision_runs.input_payload stores the Python-facing
	       -- projection, where prediction.model.name has already been replaced
	       -- with the logical model_id. The durable run has no source-model
	       -- evidence, so do not present that logical value as a producer ID.
	       NULL::text AS prediction_model_id,
	       COALESCE(r.input_payload->>'prediction_snapshot_id', '') AS prediction_snapshot_id,
	       COALESCE(batch.snapshot_set_id, '') AS orderbook_snapshot_set_id,
	       CASE
	         WHEN r.output_payload IS NULL THEN NULL
	         WHEN r.output_payload->'entry_policy' ? 'enabled' THEN r.output_payload->'entry_policy'->>'enabled'
	         ELSE NULL
	       END AS entry_submission_enabled,
	       CASE
	         WHEN r.output_payload IS NULL THEN NULL
	         ELSE NULLIF(r.output_payload->'entry_policy'->>'block_reason', '')
	       END AS entry_block_reason,
	       COALESCE(jsonb_array_length(COALESCE(r.input_payload->'predictions','[]'::jsonb)), 0) AS prediction_count,
	       COALESCE(jsonb_array_length(COALESCE(r.input_payload->'positions','[]'::jsonb)), 0) AS position_lot_count,
	       -- The output is an audit proposal. Only a persisted delivery is a
	       -- durable intent; disabled/filtered proposals must not be counted.
	       COALESCE((SELECT count(*) FROM strategy_order_intent_deliveries d
	                 WHERE d.cycle_id=r.cycle_id), 0)::int AS intent_count,
	       COALESCE((SELECT count(*) FROM strategy_order_intent_deliveries d
	                 WHERE d.cycle_id=r.cycle_id AND d.status IN ('PENDING','SUBMITTING')), 0)::int AS delivery_pending_count,
	       COALESCE((SELECT count(*) FROM strategy_order_intent_deliveries d
	                 WHERE d.cycle_id=r.cycle_id AND d.status IN ('FAILED','UNKNOWN')), 0)::int AS delivery_failed_count,
	       CASE
	         WHEN r.output_payload IS NULL THEN 'INPUT_RECORDED'
	         WHEN EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
	                      WHERE d.cycle_id=r.cycle_id AND d.status IN ('FAILED','UNKNOWN')) THEN 'DELIVERY_ATTENTION'
	         WHEN EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
	                      WHERE d.cycle_id=r.cycle_id AND d.status IN ('PENDING','SUBMITTING')) THEN 'DELIVERY_PENDING'
	         ELSE 'OUTPUT_RECORDED'
	       END AS status
	FROM strategy_decision_runs r
	LEFT JOIN strategy_orderbook_snapshot_batches batch ON batch.decision_at=r.decision_at
)
`

const cycleProjection = cycleRowsProjection + `
SELECT cycle_id, input_id, decision_at, model_id, prediction_model_id, strategy_id,
       execution_account_id, prediction_snapshot_id, orderbook_snapshot_set_id,
       created_at, decided_at, order_submission_enabled, entry_submission_enabled, entry_block_reason,
       prediction_count, position_lot_count, intent_count,
       delivery_pending_count, delivery_failed_count, status
FROM cycle_rows
`

// Filtering a cycle only needs its durable identity and delivery-derived
// status. Keep COUNT away from the JSON array lengths, snapshot join and
// per-cycle delivery counts used by the display projection.
const cycleCountProjection = `
WITH cycle_rows AS (
    SELECT r.cycle_id, r.input_id, r.decision_at, r.model_id, r.strategy_id,
           r.execution_account_id,
           CASE
             WHEN r.output_payload IS NULL THEN 'INPUT_RECORDED'
             WHEN EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
                          WHERE d.cycle_id=r.cycle_id AND d.status IN ('FAILED','UNKNOWN')) THEN 'DELIVERY_ATTENTION'
             WHEN EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
                          WHERE d.cycle_id=r.cycle_id AND d.status IN ('PENDING','SUBMITTING')) THEN 'DELIVERY_PENDING'
             ELSE 'OUTPUT_RECORDED'
           END AS status
    FROM strategy_decision_runs r
)
SELECT cycle_id, input_id, decision_at, model_id, strategy_id, execution_account_id, status
FROM cycle_rows
`

const cycleDetailProjection = cycleRowsProjection + `
SELECT cycle_id, input_id, decision_at, model_id, prediction_model_id, strategy_id,
       execution_account_id, prediction_snapshot_id, orderbook_snapshot_set_id,
       created_at, decided_at, order_submission_enabled, entry_submission_enabled, entry_block_reason,
       prediction_count, position_lot_count, intent_count,
       delivery_pending_count, delivery_failed_count, status,
       COALESCE((SELECT jsonb_agg(jsonb_build_object(
           'prediction_id', prediction->>'prediction_id',
           'source_job_id', NULLIF(prediction->>'source_job_id',''),
           'sandbox_id', NULLIF(prediction->>'sandbox_id',''),
           'market_source', NULLIF(prediction->>'market_source',''),
           'market_id', prediction->>'market_id',
           'condition_id', prediction->>'condition_id',
           'token_id', NULLIF(prediction->>'token_id','')
       ) ORDER BY prediction->>'prediction_id')
       FROM jsonb_array_elements(COALESCE(input_payload->'predictions','[]'::jsonb)) prediction), '[]'::jsonb) AS prediction_projection
FROM cycle_rows
`

func (reader *Reader) DecisionCycles(ctx context.Context, filter ListFilter) (Page[DecisionCycle], error) {
	filter, err := normalizeFilter(filter)
	if err != nil {
		return Page[DecisionCycle]{}, err
	}
	if err := reader.requireDB(ctx); err != nil {
		return Page[DecisionCycle]{}, err
	}
	now := reader.now().UTC()
	args := make([]any, 0, 12)
	conditions := []string{"1=1"}
	add := func(expression string, value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if filter.ModelID != "" {
		conditions = append(conditions, "model_id = "+add("", filter.ModelID))
	}
	if filter.StrategyID != "" {
		conditions = append(conditions, "strategy_id = "+add("", filter.StrategyID))
	}
	if filter.ExecutionAccountID != "" {
		conditions = append(conditions, "execution_account_id = "+add("", filter.ExecutionAccountID))
	}
	if filter.Status != "" {
		conditions = append(conditions, "status = "+add("", filter.Status))
	}
	if filter.Query != "" {
		placeholder := add("", "%"+filter.Query+"%")
		conditions = append(conditions, "(cycle_id ILIKE "+placeholder+" OR input_id ILIKE "+placeholder+
			" OR model_id ILIKE "+placeholder+" OR strategy_id ILIKE "+placeholder+")")
	}
	if filter.From != nil {
		conditions = append(conditions, "decision_at >= "+add("", filter.From.UTC()))
	} else {
		conditions = append(conditions, "decision_at >= "+add("", boundedSince(filter, now)))
	}
	if filter.To != nil {
		conditions = append(conditions, "decision_at <= "+add("", filter.To.UTC()))
	}
	where := strings.Join(conditions, " AND ")
	tx, err := reader.beginReadTx(ctx, "decision cycles")
	if err != nil {
		return Page[DecisionCycle]{}, err
	}
	defer tx.Rollback()
	total, err := countProjection(ctx, tx, cycleCountProjection, where, args, "decision cycles")
	if err != nil {
		return Page[DecisionCycle]{}, err
	}
	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	query := cycleProjection + " WHERE " + where +
		" ORDER BY decision_at DESC, cycle_id DESC LIMIT " + limit + " OFFSET " + offset
	rows, err := tx.QueryContext(ctx, query, listArgs...)
	if err != nil {
		return Page[DecisionCycle]{}, dbUnavailable("list decision cycles", err)
	}
	defer rows.Close()
	items := make([]DecisionCycle, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanDecisionCycle(rows)
		if scanErr != nil {
			return Page[DecisionCycle]{}, dbUnavailable("scan decision cycle", scanErr)
		}
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate decision cycles"); err != nil {
		return Page[DecisionCycle]{}, dbUnavailable("iterate decision cycles", err)
	}
	page := finishPage(items, filter, total, now, map[string]any{"basis": "bounded_recent_cycles"})
	if err := tx.Commit(); err != nil {
		return Page[DecisionCycle]{}, dbUnavailable("commit decision cycles", err)
	}
	return page, nil
}

func (reader *Reader) DecisionCycle(ctx context.Context, cycleID string) (DecisionCycleDetail, error) {
	cycleID = strings.TrimSpace(cycleID)
	if cycleID == "" || len(cycleID) > maxQueryLen || strings.ContainsAny(cycleID, "\x00\r\n/") {
		return DecisionCycleDetail{}, ErrNotFound
	}
	if err := reader.requireDB(ctx); err != nil {
		return DecisionCycleDetail{}, err
	}
	row := reader.db.QueryRowContext(ctx, cycleDetailProjection+" WHERE cycle_id=$1", cycleID)
	item, predictionJSON, err := scanDecisionCycleWithPredictions(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return DecisionCycleDetail{}, ErrNotFound
		}
		return DecisionCycleDetail{}, dbUnavailable("read decision cycle", err)
	}
	detail := DecisionCycleDetail{DecisionCycle: item, Predictions: make([]PredictionReference, 0), OrderbookSnapshots: make([]OrderbookSnapshotReference, 0), Deliveries: make([]Delivery, 0)}
	if err := decodeJSONProjection(predictionJSON, &detail.Predictions); err != nil {
		return DecisionCycleDetail{}, dbUnavailable("decode decision cycle predictions", err)
	}
	detail.Deliveries, err = reader.loadDeliveries(ctx, cycleID)
	if err != nil {
		return DecisionCycleDetail{}, err
	}
	detail.OrderbookSnapshots, err = reader.loadOrderbookSnapshots(ctx, item.DecisionAt)
	if err != nil {
		return DecisionCycleDetail{}, err
	}
	return detail, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDecisionCycle(row rowScanner) (DecisionCycle, error) {
	var item DecisionCycle
	var predictionModelID, snapshotID, setID sql.NullString
	var outputAt sql.NullTime
	var submissionEnabled, entrySubmissionEnabled sql.NullBool
	var entryBlockReason sql.NullString
	err := row.Scan(
		&item.CycleID, &item.InputID, &item.DecisionAt, &item.ModelID, &predictionModelID,
		&item.StrategyID, &item.ExecutionAccountID, &snapshotID, &setID, &item.InputCreatedAt,
		&outputAt, &submissionEnabled, &entrySubmissionEnabled, &entryBlockReason,
		&item.PredictionCount, &item.PositionLotCount, &item.IntentCount,
		&item.DeliveryPendingCount, &item.DeliveryFailedCount, &item.Status,
	)
	if err != nil {
		return DecisionCycle{}, err
	}
	item.PredictionModelID = scanNullableString(predictionModelID)
	item.PredictionSnapshotID = snapshotID.String
	item.OrderbookSnapshotSetID = setID.String
	item.OutputRecordedAt = scanNullableTime(outputAt)
	item.OrderSubmissionEnabled = scanNullableBool(submissionEnabled)
	item.EntrySubmissionEnabled = scanNullableBool(entrySubmissionEnabled)
	item.EntryBlockReason = scanNullableString(entryBlockReason)
	item.DecisionAt = item.DecisionAt.UTC()
	item.InputCreatedAt = item.InputCreatedAt.UTC()
	return item, nil
}

func scanDecisionCycleWithPredictions(row rowScanner) (DecisionCycle, []byte, error) {
	var item DecisionCycle
	var predictionModelID, snapshotID, setID sql.NullString
	var outputAt sql.NullTime
	var submissionEnabled, entrySubmissionEnabled sql.NullBool
	var entryBlockReason sql.NullString
	var predictionJSON []byte
	err := row.Scan(
		&item.CycleID, &item.InputID, &item.DecisionAt, &item.ModelID, &predictionModelID,
		&item.StrategyID, &item.ExecutionAccountID, &snapshotID, &setID, &item.InputCreatedAt,
		&outputAt, &submissionEnabled, &entrySubmissionEnabled, &entryBlockReason,
		&item.PredictionCount, &item.PositionLotCount, &item.IntentCount,
		&item.DeliveryPendingCount, &item.DeliveryFailedCount, &item.Status, &predictionJSON,
	)
	if err != nil {
		return DecisionCycle{}, nil, err
	}
	item.PredictionModelID = scanNullableString(predictionModelID)
	item.PredictionSnapshotID = snapshotID.String
	item.OrderbookSnapshotSetID = setID.String
	item.OutputRecordedAt = scanNullableTime(outputAt)
	item.OrderSubmissionEnabled = scanNullableBool(submissionEnabled)
	item.EntrySubmissionEnabled = scanNullableBool(entrySubmissionEnabled)
	item.EntryBlockReason = scanNullableString(entryBlockReason)
	item.DecisionAt = item.DecisionAt.UTC()
	item.InputCreatedAt = item.InputCreatedAt.UTC()
	return item, predictionJSON, nil
}

func (reader *Reader) loadDeliveries(ctx context.Context, cycleID string) ([]Delivery, error) {
	rows, err := reader.db.QueryContext(ctx, `
		SELECT client_order_id, cycle_id, sequence_no, status, attempt_count,
		       order_id, order_status, created_at, completed_at
		FROM strategy_order_intent_deliveries
		WHERE cycle_id=$1
		ORDER BY sequence_no
		LIMIT 100`, cycleID)
	if err != nil {
		return nil, dbUnavailable("list decision deliveries", err)
	}
	defer rows.Close()
	items := make([]Delivery, 0)
	for rows.Next() {
		var item Delivery
		var orderID, orderStatus sql.NullString
		var completed sql.NullTime
		if err := rows.Scan(&item.ClientOrderID, &item.CycleID, &item.SequenceNo, &item.Status, &item.AttemptCount,
			&orderID, &orderStatus, &item.CreatedAt, &completed); err != nil {
			return nil, dbUnavailable("scan decision delivery", err)
		}
		item.OrderID, item.OrderStatus = orderID.String, orderStatus.String
		item.Failed = item.Status == "FAILED" || item.Status == "UNKNOWN"
		item.CreatedAt = item.CreatedAt.UTC()
		item.CompletedAt = scanNullableTime(completed)
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate decision deliveries"); err != nil {
		return nil, dbUnavailable("iterate decision deliveries", err)
	}
	return items, nil
}

func (reader *Reader) loadOrderbookSnapshots(ctx context.Context, decisionAt time.Time) ([]OrderbookSnapshotReference, error) {
	rows, err := reader.db.QueryContext(ctx, `
		SELECT market_source, market_id, condition_id, token_id, status, error_code, observed_at
		FROM strategy_orderbook_snapshots
		WHERE decision_at=$1
		ORDER BY market_source, market_id, token_id
		LIMIT 200`, decisionAt.UTC())
	if err != nil {
		return nil, dbUnavailable("list decision orderbook snapshots", err)
	}
	defer rows.Close()
	items := make([]OrderbookSnapshotReference, 0)
	for rows.Next() {
		var item OrderbookSnapshotReference
		if err := rows.Scan(&item.MarketSource, &item.MarketID, &item.ConditionID, &item.TokenID,
			&item.Status, &item.ErrorCode, &item.ObservedAt); err != nil {
			return nil, dbUnavailable("scan decision orderbook snapshot", err)
		}
		item.ObservedAt = item.ObservedAt.UTC()
		items = append(items, item)
	}
	if err := queryRowsError(rows, "iterate decision orderbook snapshots"); err != nil {
		return nil, dbUnavailable("iterate decision orderbook snapshots", err)
	}
	return items, nil
}

func dbUnavailable(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errorsIsContext(err) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", ErrUnavailable, operation, err)
}

func errorsIsContext(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

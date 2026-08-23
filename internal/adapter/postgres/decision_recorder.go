package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/jackc/pgx/v5/pgconn"
)

// DecisionRecorder stores the exact input before an algorithm call and the
// validated output before an OrderIntent can enter the execution service.
type DecisionRecorder struct {
	db  *sql.DB
	now func() time.Time
}

var _ port.DecisionRecorder = (*DecisionRecorder)(nil)

func NewDecisionRecorder(db *sql.DB, now func() time.Time) (*DecisionRecorder, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if now == nil {
		now = time.Now
	}
	return &DecisionRecorder{db: db, now: now}, nil
}

// ClaimInput atomically inserts a frozen input, or returns the exact input
// already associated with this cycle. A different content hash is a conflict.
func (recorder *DecisionRecorder) ClaimInput(
	ctx context.Context,
	request domain.StrategyDecisionRequest,
) (domain.StrategyDecisionRequest, bool, error) {
	computed, err := domain.ComputeStrategyInputID(request)
	if err != nil || computed != request.InputID || request.SchemaVersion != domain.StrategyInputSchemaVersion ||
		request.CycleID == "" || request.DecisionAt.IsZero() {
		return domain.StrategyDecisionRequest{}, false, fmt.Errorf("strategy decision input identity is invalid")
	}
	if err := request.Context.Validate(); err != nil {
		return domain.StrategyDecisionRequest{}, false, fmt.Errorf("strategy decision context: %w", err)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return domain.StrategyDecisionRequest{}, false, fmt.Errorf("encode strategy decision input: %w", err)
	}
	now := recorder.now().UTC()
	result, err := recorder.db.ExecContext(ctx, `
		INSERT INTO strategy_decision_runs (
			cycle_id, input_id, decision_at, model_id, strategy_id,
			execution_account_id, input_payload, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $8)
		ON CONFLICT (cycle_id) DO NOTHING`,
		request.CycleID, request.InputID, request.DecisionAt.UTC(), request.Context.ModelID,
		request.Context.StrategyID, request.Context.ExecutionAccountID, payload, now)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return domain.StrategyDecisionRequest{}, false, port.ErrDecisionConflict
		}
		return domain.StrategyDecisionRequest{}, false, fmt.Errorf("claim strategy decision input: %w", err)
	}
	if oneRow(result) {
		return request, true, nil
	}
	stored, err := recorder.loadInput(ctx, request.CycleID)
	if err != nil {
		return domain.StrategyDecisionRequest{}, false, err
	}
	storedID, storedErr := domain.ComputeStrategyInputID(stored)
	if storedErr != nil || storedID != stored.InputID || stored.InputID != request.InputID ||
		stored.CycleID != request.CycleID || !stored.Context.Equal(request.Context) ||
		!stored.DecisionAt.Equal(request.DecisionAt) {
		return domain.StrategyDecisionRequest{}, false, port.ErrDecisionConflict
	}
	return stored, false, nil
}

func (recorder *DecisionRecorder) loadInput(ctx context.Context, cycleID string) (domain.StrategyDecisionRequest, error) {
	var payload []byte
	err := recorder.db.QueryRowContext(ctx, `
		SELECT input_payload FROM strategy_decision_runs WHERE cycle_id=$1`, cycleID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.StrategyDecisionRequest{}, port.ErrDecisionRunNotFound
	}
	if err != nil {
		return domain.StrategyDecisionRequest{}, fmt.Errorf("get strategy decision input: %w", err)
	}
	var request domain.StrategyDecisionRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return domain.StrategyDecisionRequest{}, fmt.Errorf("decode stored strategy decision input: %w", err)
	}
	return request, nil
}

// ClaimOutput locks the input row and stores the first exact algorithm output.
// Later retries can replay only a byte-equivalent structured response.
func (recorder *DecisionRecorder) ClaimOutput(
	ctx context.Context,
	response domain.StrategyDecisionResponse,
	intents []domain.OrderIntent,
	submissionEnabled bool,
) (stored domain.StrategyDecisionResponse, created bool, resultErr error) {
	if response.SchemaVersion != domain.StrategyOutputSchemaVersion || response.CycleID == "" ||
		response.InputID == "" || response.DecidedAt.IsZero() {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("strategy decision output identity is invalid")
	}
	now := recorder.now().UTC()
	if response.DecidedAt.After(now) {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("strategy decision output decided_at must not be in the future")
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("encode strategy decision output: %w", err)
	}
	intentPayloads, err := encodeDecisionIntents(response, intents)
	if err != nil {
		return domain.StrategyDecisionResponse{}, false, err
	}
	tx, err := recorder.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("begin strategy decision output claim: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()

	var inputPayload []byte
	var existingPayload []byte
	var existingSubmissionEnabled sql.NullBool
	err = tx.QueryRowContext(ctx, `
		SELECT input_payload, COALESCE(output_payload::text, ''), order_submission_enabled
		FROM strategy_decision_runs WHERE cycle_id=$1 FOR UPDATE`, response.CycleID).
		Scan(&inputPayload, &existingPayload, &existingSubmissionEnabled)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.StrategyDecisionResponse{}, false, port.ErrDecisionRunNotFound
	}
	if err != nil {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("lock strategy decision run: %w", err)
	}
	var input domain.StrategyDecisionRequest
	if err := json.Unmarshal(inputPayload, &input); err != nil {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("decode strategy decision run input: %w", err)
	}
	if response.InputID != input.InputID || !response.Context.Equal(input.Context) ||
		response.DecidedAt.Before(input.DecisionAt) {
		return domain.StrategyDecisionResponse{}, false, port.ErrDecisionConflict
	}
	if len(existingPayload) != 0 {
		var existing domain.StrategyDecisionResponse
		if err := json.Unmarshal(existingPayload, &existing); err != nil {
			return domain.StrategyDecisionResponse{}, false, fmt.Errorf("decode existing strategy decision output: %w", err)
		}
		existingJSON, err := json.Marshal(existing)
		if err != nil || !bytes.Equal(existingJSON, payload) {
			return domain.StrategyDecisionResponse{}, false, port.ErrDecisionConflict
		}
		if !existingSubmissionEnabled.Valid || existingSubmissionEnabled.Bool != submissionEnabled {
			return domain.StrategyDecisionResponse{}, false, port.ErrDecisionConflict
		}
		if err := verifyStoredDecisionIntents(ctx, tx, response.CycleID, intentPayloads, submissionEnabled); err != nil {
			return domain.StrategyDecisionResponse{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return domain.StrategyDecisionResponse{}, false, fmt.Errorf("commit existing strategy decision output: %w", err)
		}
		return existing, false, nil
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE strategy_decision_runs
		SET output_payload=$2::jsonb, decided_at=$3,
			order_submission_enabled=$4, updated_at=$5
		WHERE cycle_id=$1`, response.CycleID, payload, response.DecidedAt.UTC(), submissionEnabled, now); err != nil {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("store strategy decision output: %w", err)
	}
	if submissionEnabled {
		for index, intentPayload := range intentPayloads {
			intent := intents[index].Normalize()
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO strategy_order_intent_deliveries (
					client_order_id, cycle_id, sequence_no, intent_payload,
					status, created_at, updated_at
				) VALUES ($1, $2, $3, $4::jsonb, 'PENDING', $5, $5)`,
				intent.ClientOrderID, response.CycleID, index, intentPayload, now); err != nil {
				var postgresError *pgconn.PgError
				if errors.As(err, &postgresError) && postgresError.Code == "23505" {
					return domain.StrategyDecisionResponse{}, false, port.ErrDecisionConflict
				}
				return domain.StrategyDecisionResponse{}, false, fmt.Errorf("store strategy decision intent %q: %w", intent.ClientOrderID, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return domain.StrategyDecisionResponse{}, false, fmt.Errorf("commit strategy decision output: %w", err)
	}
	return response, true, nil
}

// encodeDecisionIntents validates the immutable hand-off payloads before the
// output transaction starts. Disabled dry-runs pass the same intents for
// audit/result reporting, but ClaimOutput deliberately does not enqueue them.
func encodeDecisionIntents(response domain.StrategyDecisionResponse, intents []domain.OrderIntent) ([][]byte, error) {
	payloads := make([][]byte, len(intents))
	seen := make(map[string]struct{}, len(intents))
	for index, value := range intents {
		intent := value.Normalize()
		if err := intent.Validate(); err != nil {
			return nil, fmt.Errorf("strategy decision intent %d: %w", index, err)
		}
		if intent.Metadata["cycle_id"] != response.CycleID || intent.Metadata["input_id"] != response.InputID ||
			intent.ExecutionAccountID != response.Context.ExecutionAccountID || intent.ModelID != response.Context.ModelID ||
			domain.CanonicalStrategyID(intent.StrategyID) != response.Context.Normalize().StrategyID {
			return nil, fmt.Errorf("strategy decision intent %q does not belong to its output", intent.ClientOrderID)
		}
		if _, exists := seen[intent.ClientOrderID]; exists {
			return nil, fmt.Errorf("duplicate strategy decision client_order_id %q", intent.ClientOrderID)
		}
		seen[intent.ClientOrderID] = struct{}{}
		payload, err := json.Marshal(intent)
		if err != nil {
			return nil, fmt.Errorf("encode strategy decision intent %q: %w", intent.ClientOrderID, err)
		}
		payloads[index] = payload
	}
	return payloads, nil
}

func verifyStoredDecisionIntents(ctx context.Context, tx *sql.Tx, cycleID string, expected [][]byte, enabled bool) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT intent_payload FROM strategy_order_intent_deliveries
		WHERE cycle_id=$1 ORDER BY sequence_no`, cycleID)
	if err != nil {
		return fmt.Errorf("list stored strategy decision intents: %w", err)
	}
	defer rows.Close()
	actual := make([][]byte, 0, len(expected))
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return fmt.Errorf("scan stored strategy decision intent: %w", err)
		}
		actual = append(actual, payload)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate stored strategy decision intents: %w", err)
	}
	if !enabled {
		if len(actual) != 0 {
			return port.ErrDecisionConflict
		}
		return nil
	}
	if len(actual) != len(expected) {
		return port.ErrDecisionConflict
	}
	for index := range actual {
		var actualIntent, expectedIntent domain.OrderIntent
		if json.Unmarshal(actual[index], &actualIntent) != nil || json.Unmarshal(expected[index], &expectedIntent) != nil ||
			!actualIntent.Equivalent(expectedIntent) {
			return port.ErrDecisionConflict
		}
	}
	return nil
}

// CountUnresolvedIntentsForAccounts prevents startup or cadence recovery from
// reviving durable work for an operator-quarantined execution account.
func (recorder *DecisionRecorder) CountUnresolvedIntentsForAccounts(
	ctx context.Context,
	executionAccountIDs []string,
) (int, error) {
	if len(executionAccountIDs) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(executionAccountIDs))
	args := make([]any, len(executionAccountIDs))
	seen := make(map[string]struct{}, len(executionAccountIDs))
	for index, accountID := range executionAccountIDs {
		accountID = strings.TrimSpace(accountID)
		if accountID == "" {
			return 0, fmt.Errorf("quarantined execution account id is required")
		}
		if _, duplicate := seen[accountID]; duplicate {
			return 0, fmt.Errorf("duplicate quarantined execution account %q", accountID)
		}
		seen[accountID] = struct{}{}
		placeholders[index] = fmt.Sprintf("$%d", index+1)
		args[index] = accountID
	}
	query := fmt.Sprintf(`
		SELECT count(*)
		FROM strategy_order_intent_deliveries
		WHERE status IN ('PENDING','SUBMITTING')
			AND intent_payload->>'execution_account_id' IN (%s)`, strings.Join(placeholders, ","))
	var count int
	if err := recorder.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count unresolved quarantined strategy intents: %w", err)
	}
	return count, nil
}

// ClaimPendingIntents atomically claims a bounded batch with SKIP LOCKED so
// concurrent workers cannot submit the same row.
func (recorder *DecisionRecorder) ClaimPendingIntents(ctx context.Context, cycleID string, side domain.Side, limit int) ([]domain.DecisionIntentDelivery, error) {
	cycleID = strings.TrimSpace(cycleID)
	if side != "" && side != domain.SideBuy && side != domain.SideSell {
		return nil, fmt.Errorf("decision intent claim side must be BUY, SELL, or empty")
	}
	if limit <= 0 || limit > 1000 {
		return nil, fmt.Errorf("decision intent claim limit must be between 1 and 1000")
	}
	now := recorder.now().UTC()
	rows, err := recorder.db.QueryContext(ctx, `
		WITH candidates AS (
			SELECT client_order_id
			FROM strategy_order_intent_deliveries
			WHERE status='PENDING' AND ($1='' OR cycle_id=$1)
				AND ($2='' OR intent_payload->>'side'=$2)
			ORDER BY created_at, cycle_id, sequence_no
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		UPDATE strategy_order_intent_deliveries delivery
		SET status='SUBMITTING', attempt_count=delivery.attempt_count+1,
			claimed_at=$4, completed_at=NULL, last_error=NULL, updated_at=$4
		FROM candidates
		WHERE delivery.client_order_id=candidates.client_order_id
		RETURNING delivery.cycle_id, delivery.client_order_id,
			delivery.sequence_no, delivery.intent_payload, delivery.status,
			delivery.attempt_count, delivery.claimed_at, delivery.completed_at,
			delivery.order_id, delivery.order_status, delivery.last_error,
			delivery.created_at, delivery.updated_at`, cycleID, side, limit, now)
	if err != nil {
		return nil, fmt.Errorf("claim pending strategy decision intents: %w", err)
	}
	defer rows.Close()
	result := make([]domain.DecisionIntentDelivery, 0)
	for rows.Next() {
		delivery, err := scanDecisionIntentDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed strategy decision intents: %w", err)
	}
	// PostgreSQL does not guarantee UPDATE ... RETURNING row order even when
	// the candidate CTE is ordered. Preserve the strategy output order before
	// execution because the first intent may consume a scarce balance/risk cap.
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		if result[i].CycleID != result[j].CycleID {
			return result[i].CycleID < result[j].CycleID
		}
		if result[i].Sequence != result[j].Sequence {
			return result[i].Sequence < result[j].Sequence
		}
		return result[i].ClientOrderID < result[j].ClientOrderID
	})
	return result, nil
}

// RequeueStaleSubmitting recovers a crashed worker claim. Retrying through
// OrderExecutor is safe because it first resolves the stable client_order_id;
// it never sends a second venue Place for an already-created order.
func (recorder *DecisionRecorder) RequeueStaleSubmitting(ctx context.Context, before time.Time, side domain.Side, limit int) (int, error) {
	if side != "" && side != domain.SideBuy && side != domain.SideSell {
		return 0, fmt.Errorf("stale decision intent side must be BUY, SELL, or empty")
	}
	if before.IsZero() || limit <= 0 || limit > 1000 {
		return 0, fmt.Errorf("stale decision intent cutoff and limit are required")
	}
	result, err := recorder.db.ExecContext(ctx, `
		WITH candidates AS (
			SELECT client_order_id
			FROM strategy_order_intent_deliveries
			WHERE status='SUBMITTING' AND claimed_at <= $1
				AND ($2='' OR intent_payload->>'side'=$2)
			ORDER BY claimed_at, cycle_id, sequence_no
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		UPDATE strategy_order_intent_deliveries delivery
		SET status='PENDING', claimed_at=NULL, completed_at=NULL,
			last_error='worker claim expired before durable completion', updated_at=$4
		FROM candidates
		WHERE delivery.client_order_id=candidates.client_order_id`, before.UTC(), side, limit, recorder.now().UTC())
	if err != nil {
		return 0, fmt.Errorf("requeue stale strategy decision intents: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count requeued strategy decision intents: %w", err)
	}
	return int(count), nil
}

// CompleteIntent fences completion by attempt_count. UNKNOWN is terminal and
// is intentionally excluded from every automatic claim query.
func (recorder *DecisionRecorder) CompleteIntent(ctx context.Context, clientOrderID string, attempt int, completion domain.DecisionIntentCompletion) error {
	clientOrderID = strings.TrimSpace(clientOrderID)
	if clientOrderID == "" || attempt <= 0 {
		return fmt.Errorf("strategy decision intent identity and attempt are required")
	}
	switch completion.Status {
	case domain.DecisionIntentSubmitted:
		if strings.TrimSpace(completion.OrderID) == "" || completion.OrderStatus == "" ||
			completion.OrderStatus == domain.OrderStatusRejected || completion.OrderStatus == domain.OrderStatusUnknown ||
			completion.OrderStatus == domain.OrderStatusManualReview {
			return fmt.Errorf("submitted strategy decision intent requires an execution-owned non-error order state")
		}
	case domain.DecisionIntentFailed:
		if strings.TrimSpace(completion.OrderID) == "" || completion.OrderStatus != domain.OrderStatusRejected {
			return fmt.Errorf("failed strategy decision intent requires a rejected execution order")
		}
	case domain.DecisionIntentUnknown:
		if strings.TrimSpace(completion.OrderID) == "" ||
			(completion.OrderStatus != domain.OrderStatusUnknown && completion.OrderStatus != domain.OrderStatusManualReview) {
			return fmt.Errorf("unknown strategy decision intent requires an uncertain execution order")
		}
	default:
		return fmt.Errorf("unsupported strategy decision intent completion status %q", completion.Status)
	}
	result, err := recorder.db.ExecContext(ctx, `
		UPDATE strategy_order_intent_deliveries
		SET status=$3, completed_at=$4, order_id=NULLIF($5,''),
			order_status=NULLIF($6,''), last_error=NULLIF($7,''), updated_at=$4
		WHERE client_order_id=$1 AND status='SUBMITTING' AND attempt_count=$2`,
		clientOrderID, attempt, completion.Status, recorder.now().UTC(),
		strings.TrimSpace(completion.OrderID), completion.OrderStatus, strings.TrimSpace(completion.LastError))
	if err != nil {
		return fmt.Errorf("complete strategy decision intent: %w", err)
	}
	if !oneRow(result) {
		return port.ErrDecisionIntentConflict
	}
	return nil
}

// ListIntents returns a cycle's immutable intents and delivery states in
// deterministic strategy-output order.
func (recorder *DecisionRecorder) ListIntents(ctx context.Context, cycleID string) ([]domain.DecisionIntentDelivery, error) {
	cycleID = strings.TrimSpace(cycleID)
	if cycleID == "" {
		return nil, fmt.Errorf("strategy decision cycle id is required")
	}
	rows, err := recorder.db.QueryContext(ctx, `
		SELECT cycle_id, client_order_id, sequence_no, intent_payload, status,
			attempt_count, claimed_at, completed_at, order_id, order_status,
			last_error, created_at, updated_at
		FROM strategy_order_intent_deliveries
		WHERE cycle_id=$1 ORDER BY sequence_no`, cycleID)
	if err != nil {
		return nil, fmt.Errorf("list strategy decision intents: %w", err)
	}
	defer rows.Close()
	result := make([]domain.DecisionIntentDelivery, 0)
	for rows.Next() {
		delivery, err := scanDecisionIntentDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate strategy decision intents: %w", err)
	}
	return result, nil
}

type decisionIntentScanner interface {
	Scan(...any) error
}

func scanDecisionIntentDelivery(scanner decisionIntentScanner) (domain.DecisionIntentDelivery, error) {
	var delivery domain.DecisionIntentDelivery
	var payload []byte
	var status string
	var claimedAt, completedAt sql.NullTime
	var orderID, orderStatus, lastError sql.NullString
	if err := scanner.Scan(
		&delivery.CycleID, &delivery.ClientOrderID, &delivery.Sequence, &payload,
		&status, &delivery.Attempt, &claimedAt, &completedAt, &orderID,
		&orderStatus, &lastError, &delivery.CreatedAt, &delivery.UpdatedAt,
	); err != nil {
		return domain.DecisionIntentDelivery{}, fmt.Errorf("scan strategy decision intent: %w", err)
	}
	if err := json.Unmarshal(payload, &delivery.Intent); err != nil {
		return domain.DecisionIntentDelivery{}, fmt.Errorf("decode strategy decision intent %q: %w", delivery.ClientOrderID, err)
	}
	if !delivery.Intent.Equivalent(delivery.Intent.Normalize()) || delivery.Intent.ClientOrderID != delivery.ClientOrderID {
		return domain.DecisionIntentDelivery{}, fmt.Errorf("stored strategy decision intent %q is invalid", delivery.ClientOrderID)
	}
	delivery.Status = domain.DecisionIntentDeliveryStatus(status)
	if claimedAt.Valid {
		value := claimedAt.Time.UTC()
		delivery.ClaimedAt = &value
	}
	if completedAt.Valid {
		value := completedAt.Time.UTC()
		delivery.CompletedAt = &value
	}
	delivery.OrderID = orderID.String
	delivery.OrderStatus = domain.OrderStatus(orderStatus.String)
	delivery.LastError = lastError.String
	delivery.CreatedAt = delivery.CreatedAt.UTC()
	delivery.UpdatedAt = delivery.UpdatedAt.UTC()
	return delivery, nil
}

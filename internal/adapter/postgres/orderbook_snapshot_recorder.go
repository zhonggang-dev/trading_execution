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
	"github.com/jackc/pgx/v5/pgconn"
)

// OrderBookSnapshotRecorder stores one complete shared capture per decision boundary.
type OrderBookSnapshotRecorder struct {
	db  *sql.DB
	now func() time.Time
}

var _ port.OrderBookSnapshotRecorder = (*OrderBookSnapshotRecorder)(nil)

func NewOrderBookSnapshotRecorder(db *sql.DB, now func() time.Time) (*OrderBookSnapshotRecorder, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if now == nil {
		now = time.Now
	}
	return &OrderBookSnapshotRecorder{db: db, now: now}, nil
}

// ClaimBatch implements atomic first-write-wins persistence. An equal replay
// verifies the sealed child count and performs no writes.
func (recorder *OrderBookSnapshotRecorder) ClaimBatch(
	ctx context.Context,
	batch domain.OrderBookSnapshotBatch,
) (stored domain.OrderBookSnapshotBatch, created bool, resultErr error) {
	validated, err := domain.NewOrderBookSnapshotBatch(
		batch.DecisionAt, batch.PredictionSnapshotID, batch.TargetCount, batch.Snapshots,
	)
	if err != nil || validated.SnapshotSetID != batch.SnapshotSetID ||
		validated.SnapshotCount != batch.SnapshotCount || validated.OKCount != batch.OKCount ||
		validated.EmptyCount != batch.EmptyCount || validated.MissingCount != batch.MissingCount ||
		validated.ErrorCount != batch.ErrorCount {
		return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("orderbook snapshot batch identity is invalid")
	}

	tx, err := recorder.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("begin orderbook snapshot batch claim: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()

	createdAt := recorder.now().UTC()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO strategy_orderbook_snapshot_batches (
			decision_at, prediction_snapshot_id, snapshot_set_id,
			target_count, snapshot_count, ok_count, empty_count,
			missing_count, error_count, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (decision_at) DO NOTHING`,
		validated.DecisionAt, validated.PredictionSnapshotID, validated.SnapshotSetID,
		validated.TargetCount, validated.SnapshotCount, validated.OKCount, validated.EmptyCount,
		validated.MissingCount, validated.ErrorCount, createdAt)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.OrderBookSnapshotBatch{}, false, port.ErrOrderBookSnapshotConflict
		}
		return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("claim orderbook snapshot batch: %w", err)
	}
	if !oneRow(result) {
		existing, err := loadOrderBookSnapshotBatchHeader(ctx, tx, validated.DecisionAt)
		if err != nil {
			return domain.OrderBookSnapshotBatch{}, false, err
		}
		if !sameOrderBookSnapshotBatch(existing, validated) {
			return domain.OrderBookSnapshotBatch{}, false, port.ErrOrderBookSnapshotConflict
		}
		storedSnapshots, err := loadOrderBookSnapshots(ctx, tx, validated.DecisionAt)
		if err != nil {
			return domain.OrderBookSnapshotBatch{}, false, err
		}
		if !sameOrderBookSnapshots(storedSnapshots, validated.Snapshots) {
			return domain.OrderBookSnapshotBatch{}, false, port.ErrOrderBookSnapshotConflict
		}
		if err := tx.Commit(); err != nil {
			return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("commit existing orderbook snapshot batch: %w", err)
		}
		existing.Snapshots = storedSnapshots
		return existing, false, nil
	}

	for index, snapshot := range validated.Snapshots {
		bids, err := json.Marshal(snapshot.Bids)
		if err != nil {
			return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("encode orderbook snapshot %d bids: %w", index, err)
		}
		asks, err := json.Marshal(snapshot.Asks)
		if err != nil {
			return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("encode orderbook snapshot %d asks: %w", index, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO strategy_orderbook_snapshots (
				decision_at, market_source, market_id, condition_id, token_id,
				outcome_index, outcome_id, status, error_code, source_at, observed_at,
				tick_size, min_order_size, best_bid, best_ask, depth_limit,
				bids, asks, created_at
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
				$12::numeric,$13::numeric,$14::numeric,$15::numeric,$16,
				$17::jsonb,$18::jsonb,$19
			)`,
			validated.DecisionAt, snapshot.MarketSource, snapshot.MarketID, snapshot.ConditionID, snapshot.TokenID,
			snapshot.OutcomeIndex, snapshot.OutcomeID, snapshot.Status, snapshot.ErrorCode,
			nullTime(snapshot.SourceAt), snapshot.ObservedAt, nullableOrderBookDecimal(snapshot.TickSize),
			nullableOrderBookDecimal(snapshot.MinOrderSize), nullableOrderBookDecimal(snapshot.BestBid), nullableOrderBookDecimal(snapshot.BestAsk),
			snapshot.DepthLimit, bids, asks, createdAt); err != nil {
			if isUniqueViolation(err) {
				return domain.OrderBookSnapshotBatch{}, false, port.ErrOrderBookSnapshotConflict
			}
			return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("store orderbook snapshot %q: %w", snapshot.TokenID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return domain.OrderBookSnapshotBatch{}, false, fmt.Errorf("commit orderbook snapshot batch: %w", err)
	}
	validated.CreatedAt = createdAt
	return validated, true, nil
}

func loadOrderBookSnapshots(ctx context.Context, tx *sql.Tx, decisionAt time.Time) ([]domain.OrderBookSnapshot, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT market_source, market_id, condition_id, token_id, outcome_index,
			outcome_id, status, error_code, source_at, observed_at,
			tick_size::text, min_order_size::text, best_bid::text, best_ask::text,
			depth_limit, bids, asks
		FROM strategy_orderbook_snapshots
		WHERE decision_at=$1
		ORDER BY market_source, condition_id, outcome_index, token_id`, decisionAt.UTC())
	if err != nil {
		return nil, fmt.Errorf("list stored orderbook snapshots: %w", err)
	}
	defer rows.Close()
	snapshots := make([]domain.OrderBookSnapshot, 0)
	for rows.Next() {
		var snapshot domain.OrderBookSnapshot
		var marketSource, status string
		var sourceAt sql.NullTime
		var tickSize, minOrderSize, bestBid, bestAsk sql.NullString
		var bids, asks []byte
		if err := rows.Scan(
			&marketSource, &snapshot.MarketID, &snapshot.ConditionID, &snapshot.TokenID,
			&snapshot.OutcomeIndex, &snapshot.OutcomeID, &status, &snapshot.ErrorCode,
			&sourceAt, &snapshot.ObservedAt, &tickSize, &minOrderSize, &bestBid, &bestAsk,
			&snapshot.DepthLimit, &bids, &asks,
		); err != nil {
			return nil, fmt.Errorf("scan stored orderbook snapshot: %w", err)
		}
		snapshot.MarketSource = domain.MarketSource(marketSource)
		snapshot.Status = domain.OrderBookStatus(status)
		if sourceAt.Valid {
			snapshot.SourceAt = sourceAt.Time
		}
		snapshot.TickSize = domain.Decimal(tickSize.String)
		snapshot.MinOrderSize = domain.Decimal(minOrderSize.String)
		snapshot.BestBid = domain.Decimal(bestBid.String)
		snapshot.BestAsk = domain.Decimal(bestAsk.String)
		if err := json.Unmarshal(bids, &snapshot.Bids); err != nil {
			return nil, fmt.Errorf("decode stored orderbook snapshot bids: %w", err)
		}
		if err := json.Unmarshal(asks, &snapshot.Asks); err != nil {
			return nil, fmt.Errorf("decode stored orderbook snapshot asks: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored orderbook snapshots: %w", err)
	}
	return snapshots, nil
}

func sameOrderBookSnapshots(left, right []domain.OrderBookSnapshot) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameOrderBookSnapshot(left[index], right[index]) {
			return false
		}
	}
	return true
}

func sameOrderBookSnapshot(left, right domain.OrderBookSnapshot) bool {
	if left.MarketSource.Normalize() != right.MarketSource.Normalize() || left.MarketID != right.MarketID ||
		left.ConditionID != right.ConditionID || left.TokenID != right.TokenID || left.OutcomeIndex != right.OutcomeIndex ||
		left.OutcomeID != right.OutcomeID || left.Status != right.Status || left.ErrorCode != right.ErrorCode ||
		!sameStoredTime(left.SourceAt, right.SourceAt) || !sameStoredTime(left.ObservedAt, right.ObservedAt) ||
		!left.TickSize.Equal(right.TickSize) || !left.MinOrderSize.Equal(right.MinOrderSize) ||
		!left.BestBid.Equal(right.BestBid) || !left.BestAsk.Equal(right.BestAsk) || left.DepthLimit != right.DepthLimit ||
		len(left.Bids) != len(right.Bids) || len(left.Asks) != len(right.Asks) {
		return false
	}
	for index := range left.Bids {
		if !left.Bids[index].Price.Equal(right.Bids[index].Price) || !left.Bids[index].Size.Equal(right.Bids[index].Size) {
			return false
		}
	}
	for index := range left.Asks {
		if !left.Asks[index].Price.Equal(right.Asks[index].Price) || !left.Asks[index].Size.Equal(right.Asks[index].Size) {
			return false
		}
	}
	return true
}

func sameStoredTime(left, right time.Time) bool {
	if left.IsZero() || right.IsZero() {
		return left.IsZero() && right.IsZero()
	}
	return left.UTC().Truncate(time.Microsecond).Equal(right.UTC().Truncate(time.Microsecond))
}

func loadOrderBookSnapshotBatchHeader(ctx context.Context, tx *sql.Tx, decisionAt time.Time) (domain.OrderBookSnapshotBatch, error) {
	var batch domain.OrderBookSnapshotBatch
	err := tx.QueryRowContext(ctx, `
		SELECT decision_at, prediction_snapshot_id, snapshot_set_id,
			target_count, snapshot_count, ok_count, empty_count,
			missing_count, error_count, created_at
		FROM strategy_orderbook_snapshot_batches
		WHERE decision_at=$1 FOR UPDATE`, decisionAt.UTC()).Scan(
		&batch.DecisionAt, &batch.PredictionSnapshotID, &batch.SnapshotSetID,
		&batch.TargetCount, &batch.SnapshotCount, &batch.OKCount, &batch.EmptyCount,
		&batch.MissingCount, &batch.ErrorCount, &batch.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.OrderBookSnapshotBatch{}, port.ErrOrderBookSnapshotConflict
	}
	if err != nil {
		return domain.OrderBookSnapshotBatch{}, fmt.Errorf("load orderbook snapshot batch: %w", err)
	}
	return batch, nil
}

func sameOrderBookSnapshotBatch(left, right domain.OrderBookSnapshotBatch) bool {
	return left.DecisionAt.Equal(right.DecisionAt) &&
		left.PredictionSnapshotID == right.PredictionSnapshotID && left.SnapshotSetID == right.SnapshotSetID &&
		left.TargetCount == right.TargetCount && left.SnapshotCount == right.SnapshotCount &&
		left.OKCount == right.OKCount && left.EmptyCount == right.EmptyCount &&
		left.MissingCount == right.MissingCount && left.ErrorCount == right.ErrorCount
}

func nullableOrderBookDecimal(value domain.Decimal) any {
	if value.IsEmpty() {
		return nil
	}
	return value.String()
}

func isUniqueViolation(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "23505"
}

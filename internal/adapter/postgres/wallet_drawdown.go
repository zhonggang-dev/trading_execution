package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
)

// WalletDrawdownStore persists samples and their order operations.
type WalletDrawdownStore struct {
	db *sql.DB
}

var _ walletdrawdown.Store = (*WalletDrawdownStore)(nil)

// NewWalletDrawdownStore wraps a database handle.
func NewWalletDrawdownStore(db *sql.DB) (*WalletDrawdownStore, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	}
	return &WalletDrawdownStore{db: db}, nil
}

// RecordOrderOperation claims and records a candidate before the execution call.
func (s *WalletDrawdownStore) RecordOrderOperation(ctx context.Context, slot time.Time, attempt int, intent domain.OrderIntent) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO wallet_drawdown_order_operations
			(client_order_id, execution_account_id, slot_at, attempt, market_id, condition_id,
			 token_id, side, time_in_force, price, size)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::numeric, $11::numeric)`,
		intent.ClientOrderID, intent.ExecutionAccountID, slot, attempt, intent.MarketID, intent.ConditionID,
		intent.TokenID, intent.Side, intent.TimeInForce, intent.WorstPrice.String(), intent.Size.String())
	if err != nil {
		return fmt.Errorf("record drawdown order operation %s: %w", intent.ClientOrderID, err)
	}
	return nil
}

// FinishOrderOperation records whether the execution API identified a local order.
func (s *WalletDrawdownStore) FinishOrderOperation(ctx context.Context, clientOrderID string, orderRecorded bool) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE wallet_drawdown_order_operations
		SET submission_state = CASE WHEN $2::boolean THEN 'ORDER_RECORDED' ELSE 'SUBMIT_UNCONFIRMED' END,
		    submit_finished_at = clock_timestamp()
		WHERE client_order_id = $1 AND submission_state = 'PREPARED'`, clientOrderID, orderRecorded)
	if err != nil {
		return fmt.Errorf("finish drawdown order operation %s: %w", clientOrderID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return fmt.Errorf("no pending drawdown order operation %s", clientOrderID)
	}
	return nil
}

const drawdownColumns = `execution_account_id,slot_at,sampled_at,positions_value::text,realized_pnl::text,unrealized_pnl::text,total_pnl::text,
 window_minutes,window_peak_pnl::text,window_peak_at,drawdown::text,threshold_usd::text,exceeded,notified,positions`

func scanDrawdownSample(row interface{ Scan(...any) error }) (walletdrawdown.Sample, error) {
	var s walletdrawdown.Sample
	var peak, drawdown sql.NullString
	var peakAt sql.NullTime
	var positions []byte
	if err := row.Scan(&s.AccountID, &s.SlotAt, &s.SampledAt, &s.PositionsValue, &s.RealizedPnL, &s.UnrealizedPnL, &s.TotalPnL,
		&s.WindowMinutes, &peak, &peakAt, &drawdown, &s.Threshold, &s.Exceeded, &s.Notified, &positions); err != nil {
		return s, err
	}
	if peak.Valid {
		s.PeakPnL = &peak.String
	}
	if peakAt.Valid {
		t := peakAt.Time.UTC()
		s.PeakAt = &t
	}
	if drawdown.Valid {
		s.Drawdown = &drawdown.String
	}
	s.SlotAt, s.SampledAt = s.SlotAt.UTC(), s.SampledAt.UTC()
	if err := json.Unmarshal(positions, &s.Positions); err != nil {
		return s, fmt.Errorf("decode sample positions: %w", err)
	}
	return s, nil
}

// LoadSample reads one slot.
func (s *WalletDrawdownStore) LoadSample(ctx context.Context, accountID string, slot time.Time) (walletdrawdown.Sample, bool, error) {
	sample, err := scanDrawdownSample(s.db.QueryRowContext(ctx,
		`SELECT `+drawdownColumns+` FROM wallet_drawdown_samples
		  WHERE execution_account_id=$1 AND slot_at=$2 AND total_pnl IS NOT NULL`, accountID, slot))
	if errors.Is(err, sql.ErrNoRows) {
		return walletdrawdown.Sample{}, false, nil
	}
	if err != nil {
		return walletdrawdown.Sample{}, false, fmt.Errorf("load drawdown sample: %w", err)
	}
	return sample, true, nil
}

// WindowSamples reads samples in [from, ...) taken in slots before `before`.
func (s *WalletDrawdownStore) WindowSamples(ctx context.Context, accountID string, from, before time.Time) ([]walletdrawdown.Sample, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+drawdownColumns+` FROM wallet_drawdown_samples
		WHERE execution_account_id=$1 AND sampled_at>=$2 AND slot_at<$3 AND total_pnl IS NOT NULL ORDER BY slot_at`, accountID, from, before)
	if err != nil {
		return nil, fmt.Errorf("load drawdown window: %w", err)
	}
	defer rows.Close()
	out := []walletdrawdown.Sample{}
	for rows.Next() {
		sample, err := scanDrawdownSample(rows)
		if err != nil {
			return nil, fmt.Errorf("scan drawdown window: %w", err)
		}
		out = append(out, sample)
	}
	return out, rows.Err()
}

// LastNotifiedDrawdown returns the latest notified drawdown in the current
// episode: after the most recent sample that did not exceed the threshold.
func (s *WalletDrawdownStore) LastNotifiedDrawdown(ctx context.Context, accountID string, before time.Time) (*big.Rat, error) {
	var drawdown sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT drawdown::text FROM wallet_drawdown_samples
		WHERE execution_account_id=$1 AND slot_at<$2 AND notified AND total_pnl IS NOT NULL
		  AND slot_at>COALESCE((SELECT max(slot_at) FROM wallet_drawdown_samples
		                       WHERE execution_account_id=$1 AND slot_at<$2 AND NOT exceeded AND total_pnl IS NOT NULL),'-infinity'::timestamptz)
		ORDER BY slot_at DESC LIMIT 1`, accountID, before).Scan(&drawdown)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load last drawdown notification: %w", err)
	}
	if !drawdown.Valid {
		return nil, nil
	}
	value, ok := new(big.Rat).SetString(drawdown.String)
	if !ok {
		return nil, fmt.Errorf("stored drawdown is not decimal")
	}
	return value, nil
}

// InsertSample writes a sample; false means the slot already exists.
func (s *WalletDrawdownStore) InsertSample(ctx context.Context, sample walletdrawdown.Sample) (bool, error) {
	positions, err := json.Marshal(sample.Positions)
	if err != nil {
		return false, fmt.Errorf("encode sample positions: %w", err)
	}
	var peakAt any
	if sample.PeakAt != nil {
		peakAt = *sample.PeakAt
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO wallet_drawdown_samples
		(execution_account_id,slot_at,sampled_at,positions_value,realized_pnl,unrealized_pnl,total_pnl,window_minutes,
		 window_peak_pnl,window_peak_at,drawdown,threshold_usd,exceeded,notified,positions)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,FALSE,$14)
		ON CONFLICT(execution_account_id,slot_at) DO NOTHING`,
		sample.AccountID, sample.SlotAt, sample.SampledAt, sample.PositionsValue, sample.RealizedPnL, sample.UnrealizedPnL,
		sample.TotalPnL, sample.WindowMinutes, sample.PeakPnL, peakAt, sample.Drawdown, sample.Threshold, sample.Exceeded, positions)
	if err != nil {
		return false, fmt.Errorf("insert drawdown sample: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// MarkNotified records the delivery outcome.
func (s *WalletDrawdownStore) MarkNotified(ctx context.Context, accountID string, slot time.Time, notified bool, failure string) error {
	var failureValue any
	if failure != "" {
		if len(failure) > 500 {
			failure = failure[:500]
		}
		failureValue = failure
	}
	result, err := s.db.ExecContext(ctx, `UPDATE wallet_drawdown_samples SET notified=$3, notify_error=$4
		WHERE execution_account_id=$1 AND slot_at=$2`, accountID, slot, notified, failureValue)
	if err != nil {
		return fmt.Errorf("mark drawdown notification: %w", err)
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("mark drawdown notification: sample not found")
	}
	return nil
}

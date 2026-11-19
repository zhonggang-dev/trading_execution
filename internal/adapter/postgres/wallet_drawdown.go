package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
)

// WalletDrawdownStore persists read-only drawdown monitoring samples.
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

// WalletAddress returns the account's Polymarket wallet.
func (s *WalletDrawdownStore) WalletAddress(ctx context.Context, accountID string) (string, error) {
	var wallet sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT lower(wallet_address) FROM execution_accounts WHERE execution_account_id=$1`, accountID).Scan(&wallet)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("execution account %s not found", accountID)
	}
	if err != nil {
		return "", fmt.Errorf("load wallet address: %w", err)
	}
	if !wallet.Valid || strings.TrimSpace(wallet.String) == "" {
		return "", fmt.Errorf("execution account %s has no wallet address", accountID)
	}
	return wallet.String, nil
}

const drawdownColumns = `execution_account_id,slot_at,sampled_at,cash_pusd::text,cash_block,positions_value::text,equity::text,
 window_minutes,window_peak_equity::text,window_peak_at,drawdown::text,threshold_usd::text,exceeded,notified,positions`

func scanDrawdownSample(row interface{ Scan(...any) error }) (walletdrawdown.Sample, error) {
	var s walletdrawdown.Sample
	var peak, drawdown sql.NullString
	var peakAt sql.NullTime
	var positions []byte
	if err := row.Scan(&s.AccountID, &s.SlotAt, &s.SampledAt, &s.CashPUSD, &s.CashBlock, &s.PositionsValue, &s.Equity,
		&s.WindowMinutes, &peak, &peakAt, &drawdown, &s.Threshold, &s.Exceeded, &s.Notified, &positions); err != nil {
		return s, err
	}
	if peak.Valid {
		s.PeakEquity = &peak.String
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
		`SELECT `+drawdownColumns+` FROM wallet_drawdown_samples WHERE execution_account_id=$1 AND slot_at=$2`, accountID, slot))
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
		WHERE execution_account_id=$1 AND sampled_at>=$2 AND slot_at<$3 ORDER BY slot_at`, accountID, from, before)
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
		WHERE execution_account_id=$1 AND slot_at<$2 AND notified
		  AND slot_at>COALESCE((SELECT max(slot_at) FROM wallet_drawdown_samples
		                       WHERE execution_account_id=$1 AND slot_at<$2 AND NOT exceeded),'-infinity'::timestamptz)
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
		(execution_account_id,slot_at,sampled_at,cash_pusd,cash_block,positions_value,equity,window_minutes,
		 window_peak_equity,window_peak_at,drawdown,threshold_usd,exceeded,notified,positions)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,FALSE,$14)
		ON CONFLICT(execution_account_id,slot_at) DO NOTHING`,
		sample.AccountID, sample.SlotAt, sample.SampledAt, sample.CashPUSD, int64(sample.CashBlock), sample.PositionsValue,
		sample.Equity, sample.WindowMinutes, sample.PeakEquity, peakAt, sample.Drawdown, sample.Threshold, sample.Exceeded, positions)
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

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	liveMarketValidationMode = "LIVE_CHECK"
	maxRiskFreshnessWindow   = 365 * 24 * time.Hour
	maxRiskFutureSkew        = 2 * time.Second
)

type liveRiskPolicy struct {
	policyID      string
	version       int64
	enabled       bool
	maxPriceAge   time.Duration
	maxSignalAge  time.Duration
	maxStateAge   time.Duration
	dailyTimezone string
}

type liveRiskAuthorization struct {
	policyID          string
	policyVersion     int64
	riskDay           string
	dailyRiskNotional domain.Decimal
}

// requiresAtomicLiveRisk relies on evidence produced by the server-side live
// market validator, not on a caller-controlled execution-mode flag. Paper uses
// PAPER_BYPASS and existing non-live repository tests have no validation.
func requiresAtomicLiveRisk(order domain.Order) bool {
	return order.MarketValidation != nil &&
		strings.EqualFold(strings.TrimSpace(order.MarketValidation.Mode), liveMarketValidationMode)
}

// authorizeLiveRisk runs after execution_accounts has been locked. Every
// mutable account writer in this adapter follows that same lock order, so the
// snapshot and the subsequent reservation write are one serialization point.
func (manager *ReservationManager) authorizeLiveRisk(
	ctx context.Context,
	tx *sql.Tx,
	order domain.Order,
	observedAt time.Time,
	reserveUnitPrice domain.Decimal,
	excludeOrderID string,
) (liveRiskAuthorization, error) {
	if err := validateLiveRiskTimestamps(order, observedAt); err != nil {
		return liveRiskAuthorization{}, err
	}
	if err := checkGlobalRiskControl(ctx, tx); err != nil {
		return liveRiskAuthorization{}, err
	}
	policy, err := loadLiveRiskPolicy(ctx, tx, order.Intent.ExecutionAccountID)
	if err != nil {
		return liveRiskAuthorization{}, err
	}
	if !policy.enabled {
		return liveRiskAuthorization{}, reject("RISK_POLICY_DISABLED", "the execution account risk policy is not enabled")
	}
	if err := checkLiveRiskBindingAndControls(ctx, tx, order.Intent); err != nil {
		return liveRiskAuthorization{}, err
	}
	if err := checkRiskTimestamp("PRICE", &order.MarketValidation.LatestBookSourceAt, observedAt, policy.maxPriceAge); err != nil {
		return liveRiskAuthorization{}, err
	}
	if err := checkRiskTimestamp("SIGNAL", order.Intent.SignalAt, observedAt, policy.maxSignalAge); err != nil {
		return liveRiskAuthorization{}, err
	}
	if err := checkLiveRiskState(ctx, tx, order.Intent.ExecutionAccountID, observedAt, policy.maxStateAge); err != nil {
		return liveRiskAuthorization{}, err
	}

	dailyRiskNotional, err := numericProduct(ctx, tx, order.Intent.WorstPrice, order.Intent.Size)
	if err != nil {
		return liveRiskAuthorization{}, fmt.Errorf("calculate daily live risk notional: %w", err)
	}
	grossCandidate, err := riskRat(dailyRiskNotional)
	if err != nil || grossCandidate.Sign() <= 0 {
		return liveRiskAuthorization{}, reject("INVALID_ORDER_NOTIONAL", "worst_price multiplied by size must be positive")
	}
	riskDay, _, _, err := riskDayBoundary(ctx, tx, policy.dailyTimezone, observedAt)
	if err != nil {
		return liveRiskAuthorization{}, err
	}
	legacyActive, err := queryRiskCount(ctx, tx, `
		SELECT count(*)
		FROM asset_reservations
		WHERE execution_account_id = $1
		  AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
		  AND order_id <> $2 AND risk_policy_id = ''`,
		order.Intent.ExecutionAccountID, strings.TrimSpace(excludeOrderID))
	if err != nil {
		return liveRiskAuthorization{}, fmt.Errorf("check legacy active reservations: %w", err)
	}
	if legacyActive != 0 {
		return liveRiskAuthorization{}, reject("LEGACY_ACTIVE_RESERVATION", "an active reservation has no atomic live-risk authorization")
	}
	if order.Intent.Side == domain.SideBuy {
		protectedCandidate, productErr := riskProduct(reserveUnitPrice, order.Intent.Size)
		if productErr != nil || protectedCandidate.Sign() <= 0 {
			return liveRiskAuthorization{}, reject("INVALID_ORDER_NOTIONAL", "fee-protected BUY notional must be positive")
		}
		if err := checkBuyAvailableBalance(ctx, tx, order.Intent.ExecutionAccountID, protectedCandidate); err != nil {
			return liveRiskAuthorization{}, err
		}
	}

	return liveRiskAuthorization{
		policyID: policy.policyID, policyVersion: policy.version,
		riskDay: riskDay, dailyRiskNotional: dailyRiskNotional,
	}, nil
}

func checkBuyAvailableBalance(
	ctx context.Context,
	tx *sql.Tx,
	executionAccountID string,
	candidate *big.Rat,
) error {
	available, err := queryRiskRat(ctx, tx, `
		SELECT available_balance::text FROM execution_accounts
		WHERE execution_account_id = $1`, executionAccountID)
	if err != nil {
		return fmt.Errorf("load live available balance: %w", err)
	}
	if candidate.Cmp(available) > 0 {
		return reject("INSUFFICIENT_WALLET_BALANCE", "available balance is below the fee-protected BUY notional")
	}
	return nil
}

func checkGlobalRiskControl(ctx context.Context, tx *sql.Tx) error {
	var killSwitch bool
	var reason string
	err := tx.QueryRowContext(ctx, `
		SELECT kill_switch, reason FROM execution_risk_global_control
		WHERE singleton = TRUE FOR SHARE`).Scan(&killSwitch, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return reject("GLOBAL_KILL_SWITCH", "global live risk control is missing")
	}
	if err != nil {
		return fmt.Errorf("load global live risk control: %w", err)
	}
	if killSwitch {
		if strings.TrimSpace(reason) == "" {
			reason = "global live trading kill switch is enabled"
		}
		return reject("GLOBAL_KILL_SWITCH", reason)
	}
	return nil
}

func loadLiveRiskPolicy(ctx context.Context, tx *sql.Tx, accountID string) (liveRiskPolicy, error) {
	var policy liveRiskPolicy
	var priceAgeMS, signalAgeMS, stateAgeMS int64
	err := tx.QueryRowContext(ctx, `
		SELECT policy_id, version, enabled,
		       max_price_age_ms, max_signal_age_ms, max_state_age_ms, daily_timezone
		FROM execution_risk_policies
		WHERE execution_account_id = $1 FOR SHARE`, accountID).Scan(
		&policy.policyID, &policy.version, &policy.enabled,
		&priceAgeMS, &signalAgeMS, &stateAgeMS, &policy.dailyTimezone,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return liveRiskPolicy{}, reject("RISK_POLICY_MISSING", "the execution account has no live risk policy")
	}
	if err != nil {
		return liveRiskPolicy{}, fmt.Errorf("load live risk policy: %w", err)
	}
	if strings.TrimSpace(policy.policyID) == "" || policy.version < 1 || strings.TrimSpace(policy.dailyTimezone) == "" {
		return liveRiskPolicy{}, fmt.Errorf("live risk policy identity is invalid")
	}
	var durationErr error
	policy.maxPriceAge, durationErr = riskDuration(priceAgeMS)
	if durationErr != nil {
		return liveRiskPolicy{}, fmt.Errorf("live risk max_price_age: %w", durationErr)
	}
	policy.maxSignalAge, durationErr = riskDuration(signalAgeMS)
	if durationErr != nil {
		return liveRiskPolicy{}, fmt.Errorf("live risk max_signal_age: %w", durationErr)
	}
	policy.maxStateAge, durationErr = riskDuration(stateAgeMS)
	if durationErr != nil {
		return liveRiskPolicy{}, fmt.Errorf("live risk max_state_age: %w", durationErr)
	}
	return policy, nil
}

func checkLiveRiskBindingAndControls(ctx context.Context, tx *sql.Tx, intent domain.OrderIntent) error {
	canonicalStrategy := domain.CanonicalStrategyID(intent.StrategyID)
	var bindingEnabled bool
	err := tx.QueryRowContext(ctx, `
		SELECT enabled FROM execution_strategy_bindings
		WHERE model_id = $1 AND strategy_id = $2 AND execution_account_id = $3
		FOR SHARE`, intent.ModelID, canonicalStrategy, intent.ExecutionAccountID).Scan(&bindingEnabled)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !bindingEnabled) {
		return reject("STRATEGY_ACCOUNT_BINDING_DENIED", "model and strategy are not enabled for this execution account")
	}
	if err != nil {
		return fmt.Errorf("load strategy execution binding: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT control_scope, control_key, paused, reason
		FROM execution_risk_controls
		WHERE execution_account_id = $1
		  AND ((control_scope = 'ACCOUNT' AND control_key = '')
		    OR (control_scope = 'STRATEGY' AND control_key = $2)
		    OR (control_scope = 'MARKET' AND control_key = $3))
		FOR SHARE`, intent.ExecutionAccountID, canonicalStrategy, intent.MarketID)
	if err != nil {
		return fmt.Errorf("load scoped live risk controls: %w", err)
	}
	accountSeen := false
	for rows.Next() {
		var scope, key, reason string
		var paused bool
		if err := rows.Scan(&scope, &key, &paused, &reason); err != nil {
			rows.Close()
			return fmt.Errorf("scan scoped live risk control: %w", err)
		}
		if scope == "ACCOUNT" {
			accountSeen = true
		}
		if !paused {
			continue
		}
		if strings.TrimSpace(reason) == "" {
			reason = strings.ToLower(scope) + " live trading is paused"
		}
		switch scope {
		case "ACCOUNT":
			rows.Close()
			return reject("EXECUTION_ACCOUNT_PAUSED", reason)
		case "STRATEGY":
			rows.Close()
			return reject("STRATEGY_PAUSED", reason)
		case "MARKET":
			rows.Close()
			return reject("MARKET_RISK_PAUSED", reason)
		default:
			rows.Close()
			return fmt.Errorf("unsupported live risk control scope %q/%q", scope, key)
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close scoped live risk controls: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate scoped live risk controls: %w", err)
	}
	if !accountSeen {
		return reject("EXECUTION_ACCOUNT_PAUSED", "the mandatory execution account control is missing")
	}
	return nil
}

func checkLiveRiskState(ctx context.Context, tx *sql.Tx, accountID string, now time.Time, maxAge time.Duration) error {
	var status string
	var completedAt sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT status, completed_at
		FROM reconciliation_runs
		WHERE execution_account_id = $1
		ORDER BY started_at DESC, run_id DESC
		LIMIT 1 FOR SHARE`, accountID).Scan(&status, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return reject("RISK_STATE_STALE", "the execution account has never completed reconciliation")
	}
	if err != nil {
		return fmt.Errorf("load latest live reconciliation: %w", err)
	}
	if status != string(domain.ReconciliationRunCompleted) || !completedAt.Valid {
		return reject("RISK_STATE_STALE", "the latest execution account reconciliation is not completed")
	}
	completed := completedAt.Time.UTC()
	if completed.After(now.Add(maxRiskFutureSkew)) || now.Sub(completed) > maxAge {
		return reject("RISK_STATE_STALE", "the latest execution account reconciliation is outside the policy freshness window")
	}
	count, err := queryRiskCount(ctx, tx, `
		SELECT count(*) FROM reconciliation_issues
		WHERE execution_account_id = $1 AND status = 'OPEN'`, accountID)
	if err != nil {
		return fmt.Errorf("check open reconciliation issues: %w", err)
	}
	if count != 0 {
		return reject("RISK_STATE_HAS_OPEN_ISSUES", "the execution account has unresolved reconciliation issues")
	}
	return nil
}

func validateLiveRiskTimestamps(order domain.Order, now time.Time) error {
	intent := order.Intent
	if intent.MarketSnapshotAt == nil || intent.MarketSnapshotAt.IsZero() {
		return reject("PRICE_TIMESTAMP_REQUIRED", "live order requires market_snapshot_at")
	}
	if order.MarketValidation == nil || order.MarketValidation.LatestBookSourceAt.IsZero() {
		return reject("PRICE_TIMESTAMP_REQUIRED", "live order requires the latest validated orderbook timestamp")
	}
	if intent.SignalAt == nil || intent.SignalAt.IsZero() {
		return reject("SIGNAL_TIMESTAMP_REQUIRED", "live order requires signal_at")
	}
	if sign, err := intent.WorstPrice.Sign(); err != nil || sign <= 0 {
		return reject("INVALID_ORDER_NOTIONAL", "live order requires a positive worst_price")
	}
	if now.IsZero() {
		return fmt.Errorf("live risk observation time is required")
	}
	return nil
}

func checkRiskTimestamp(prefix string, value *time.Time, now time.Time, maxAge time.Duration) error {
	if value == nil || value.IsZero() {
		return reject(prefix+"_TIMESTAMP_REQUIRED", strings.ToLower(prefix)+" timestamp is required")
	}
	timestamp := value.UTC()
	if timestamp.After(now.Add(maxRiskFutureSkew)) {
		return reject(prefix+"_TIMESTAMP_FUTURE", strings.ToLower(prefix)+" timestamp exceeds the live risk clock skew")
	}
	if now.Sub(timestamp) > maxAge {
		return reject(prefix+"_STALE", strings.ToLower(prefix)+" timestamp is outside the live risk freshness window")
	}
	return nil
}

func riskDayBoundary(ctx context.Context, tx *sql.Tx, timezone string, now time.Time) (string, time.Time, time.Time, error) {
	var day string
	var start, next time.Time
	err := tx.QueryRowContext(ctx, `
		SELECT to_char(timezone($1, $2::timestamptz), 'YYYY-MM-DD'),
		       date_trunc('day', timezone($1, $2::timestamptz)) AT TIME ZONE $1,
		       (date_trunc('day', timezone($1, $2::timestamptz)) + INTERVAL '1 day') AT TIME ZONE $1`,
		timezone, now).Scan(&day, &start, &next)
	if err != nil {
		return "", time.Time{}, time.Time{}, fmt.Errorf("calculate live risk day in timezone %q: %w", timezone, err)
	}
	return day, start.UTC(), next.UTC(), nil
}

func riskDuration(milliseconds int64) (time.Duration, error) {
	if milliseconds <= 0 || milliseconds > int64(maxRiskFreshnessWindow/time.Millisecond) {
		return 0, fmt.Errorf("milliseconds must be between 1 and %d", maxRiskFreshnessWindow/time.Millisecond)
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

func queryRiskCount(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	var count int64
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func queryRiskRat(ctx context.Context, tx *sql.Tx, query string, args ...any) (*big.Rat, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return nil, err
	}
	return riskRat(domain.Decimal(raw))
}

func riskProduct(left, right domain.Decimal) (*big.Rat, error) {
	leftRat, err := riskRat(left)
	if err != nil {
		return nil, err
	}
	rightRat, err := riskRat(right)
	if err != nil {
		return nil, err
	}
	return new(big.Rat).Mul(leftRat, rightRat), nil
}

func riskRat(value domain.Decimal) (*big.Rat, error) {
	parsed, err := domain.ParseDecimal(value.String())
	if err != nil {
		return nil, err
	}
	result, ok := new(big.Rat).SetString(parsed.String())
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", value)
	}
	return result, nil
}

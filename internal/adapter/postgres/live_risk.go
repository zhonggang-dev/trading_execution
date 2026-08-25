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
	policyID               string
	version                int64
	enabled                bool
	maxOrderNotional       domain.Decimal
	maxMarketExposure      domain.Decimal
	maxStrategyExposure    domain.Decimal
	maxWalletExposure      domain.Decimal
	maxDailyTradedNotional domain.Decimal
	maxPriceAge            time.Duration
	maxSignalAge           time.Duration
	maxStateAge            time.Duration
	dailyTimezone          string
}

// liveRiskExposure 保存一个账户在钱包、市场和策略三个维度的精确敞口。
type liveRiskExposure struct {
	wallet   *big.Rat
	market   *big.Rat
	strategy *big.Rat
}

type liveRiskAuthorization struct {
	policyID          string
	policyVersion     int64
	riskDay           string
	dailyRiskNotional domain.Decimal
}

// requiresAtomicLiveRisk 只对通过服务端实盘市场校验的订单启用原子硬风控。
func requiresAtomicLiveRisk(order domain.Order) bool {
	return order.MarketValidation != nil &&
		strings.EqualFold(strings.TrimSpace(order.MarketValidation.Mode), liveMarketValidationMode)
}

// authorizeLiveRisk 在账户锁内完成实盘控制、数据新鲜度和金额硬上限授权。
func (manager *ReservationManager) authorizeLiveRisk(
	ctx context.Context,
	tx *sql.Tx,
	order domain.Order,
	observedAt time.Time,
	reserveUnitPrice domain.Decimal,
	excludeOrderID string,
) (liveRiskAuthorization, error) {
	if err := validateLiveRiskTimestamps(order.Intent, observedAt); err != nil {
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
	if err := checkRiskTimestamp("PRICE", order.Intent.MarketSnapshotAt, observedAt, policy.maxPriceAge); err != nil {
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
	riskDay, dayStart, nextDayStart, err := riskDayBoundary(ctx, tx, policy.dailyTimezone, observedAt)
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
		if err := authorizeBuyLiveRisk(ctx, tx, buyLiveRiskParams{
			order: order, policy: policy, observedAt: observedAt,
			dayStart: dayStart, nextDayStart: nextDayStart,
			grossCandidate: grossCandidate, reserveUnitPrice: reserveUnitPrice,
			excludeOrderID: excludeOrderID,
		}); err != nil {
			return liveRiskAuthorization{}, err
		}
	}

	return liveRiskAuthorization{
		policyID: policy.policyID, policyVersion: policy.version,
		riskDay: riskDay, dailyRiskNotional: dailyRiskNotional,
	}, nil
}

// buyLiveRiskParams 收拢新增买入风险授权所需的事务内参数。
type buyLiveRiskParams struct {
	order            domain.Order
	policy           liveRiskPolicy
	observedAt       time.Time
	dayStart         time.Time
	nextDayStart     time.Time
	grossCandidate   *big.Rat
	reserveUnitPrice domain.Decimal
	excludeOrderID   string
}

// authorizeBuyLiveRisk 校验新增买入的单笔、当日、余额和各维度敞口硬上限。
func authorizeBuyLiveRisk(ctx context.Context, tx *sql.Tx, params buyLiveRiskParams) error {
	if exceedsRisk(params.grossCandidate, params.policy.maxOrderNotional) {
		return reject("MAX_ORDER_NOTIONAL_EXCEEDED", "order notional exceeds the live account hard limit")
	}
	if err := checkBuyDailyRisk(ctx, tx, params); err != nil {
		return err
	}
	protectedCandidate, err := riskProduct(params.reserveUnitPrice, params.order.Intent.Size)
	if err != nil || protectedCandidate.Sign() <= 0 {
		return reject("INVALID_ORDER_NOTIONAL", "fee-protected BUY notional must be positive")
	}
	return checkBuyLiveRiskExposure(ctx, tx, params.order, protectedCandidate, params.policy, params.excludeOrderID)
}

// checkBuyDailyRisk 把已确认成交、活动预占和候选订单合并后校验当日硬上限。
func checkBuyDailyRisk(ctx context.Context, tx *sql.Tx, params buyLiveRiskParams) error {
	confirmed, err := queryRiskRat(ctx, tx, `
		SELECT COALESCE(SUM(gross_notional), 0)::text
		FROM execution_fills
		WHERE execution_account_id = $1
		  AND status = 'CONFIRMED' AND applied_at IS NOT NULL
		  AND matched_at >= $2 AND matched_at < $3
		  AND matched_at <= $4`,
		params.order.Intent.ExecutionAccountID, params.dayStart, params.nextDayStart, params.observedAt)
	if err != nil {
		return fmt.Errorf("load daily confirmed notional: %w", err)
	}
	pending, err := queryRiskRat(ctx, tx, `
		SELECT COALESCE(SUM(GREATEST(daily_risk_notional - settled_notional, 0)), 0)::text
		FROM asset_reservations
		WHERE execution_account_id = $1
		  AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
		  AND order_id <> $2`, params.order.Intent.ExecutionAccountID, strings.TrimSpace(params.excludeOrderID))
	if err != nil {
		return fmt.Errorf("load pending daily notional: %w", err)
	}
	total := new(big.Rat).Add(confirmed, pending)
	total.Add(total, params.grossCandidate)
	if exceedsRisk(total, params.policy.maxDailyTradedNotional) {
		return reject("DAILY_TRADED_NOTIONAL_EXCEEDED", "confirmed fills plus pending orders would exceed the daily hard limit")
	}
	return nil
}

// checkBuyLiveRiskExposure 校验余额，并把持仓、活动买单和候选订单合并后检查敞口。
func checkBuyLiveRiskExposure(
	ctx context.Context,
	tx *sql.Tx,
	order domain.Order,
	candidate *big.Rat,
	policy liveRiskPolicy,
	excludeOrderID string,
) error {
	available, err := queryRiskRat(ctx, tx, `
		SELECT available_balance::text FROM execution_accounts
		WHERE execution_account_id = $1`, order.Intent.ExecutionAccountID)
	if err != nil {
		return fmt.Errorf("load live available balance: %w", err)
	}
	if candidate.Cmp(available) > 0 {
		return reject("INSUFFICIENT_WALLET_BALANCE", "available balance is below the fee-protected BUY notional")
	}
	exposure, positionCost, err := loadPositionRiskExposure(ctx, tx, order.Intent)
	if err != nil {
		return err
	}
	if err := verifyAggregatePositionCost(ctx, tx, order.Intent.ExecutionAccountID, positionCost); err != nil {
		return err
	}
	if err := addReservedBuyRisk(ctx, tx, order.Intent, excludeOrderID, &exposure); err != nil {
		return err
	}
	exposure.add(candidate, order.Intent.MarketID, order.Intent.StrategyID, order.Intent)
	return checkExposureHardLimits(exposure, policy)
}

// newLiveRiskExposure 创建三个维度均为零的精确敞口集合。
func newLiveRiskExposure() liveRiskExposure {
	return liveRiskExposure{wallet: new(big.Rat), market: new(big.Rat), strategy: new(big.Rat)}
}

// add 按候选订单的市场和策略归属累加风险金额。
func (exposure *liveRiskExposure) add(value *big.Rat, marketID string, strategyID string, intent domain.OrderIntent) {
	exposure.wallet.Add(exposure.wallet, value)
	if strings.TrimSpace(marketID) == intent.MarketID {
		exposure.market.Add(exposure.market, value)
	}
	if domain.CanonicalStrategyID(strategyID) == domain.CanonicalStrategyID(intent.StrategyID) {
		exposure.strategy.Add(exposure.strategy, value)
	}
}

// loadPositionRiskExposure 从权威仓位批次读取钱包、市场和策略敞口。
func loadPositionRiskExposure(ctx context.Context, tx *sql.Tx, intent domain.OrderIntent) (liveRiskExposure, *big.Rat, error) {
	exposure := newLiveRiskExposure()
	positionCost := new(big.Rat)
	rows, err := tx.QueryContext(ctx, `
		SELECT market_id, strategy_id, remaining_cost::text
		FROM position_lots
		WHERE execution_account_id = $1
		  AND status IN ('OPEN', 'SETTLED_PENDING_REDEEM')`, intent.ExecutionAccountID)
	if err != nil {
		return exposure, positionCost, fmt.Errorf("load live position risk: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var marketID, strategyID, raw string
		if err := rows.Scan(&marketID, &strategyID, &raw); err != nil {
			return exposure, positionCost, fmt.Errorf("scan live position risk: %w", err)
		}
		value, err := riskRat(domain.Decimal(raw))
		if err != nil || value.Sign() < 0 {
			return exposure, positionCost, fmt.Errorf("invalid live position risk value %q", raw)
		}
		positionCost.Add(positionCost, value)
		exposure.add(value, marketID, strategyID, intent)
	}
	if err := rows.Err(); err != nil {
		return exposure, positionCost, fmt.Errorf("iterate live position risk: %w", err)
	}
	return exposure, positionCost, nil
}

// verifyAggregatePositionCost 拒绝仓位批次成本与聚合仓位成本不一致的账户。
func verifyAggregatePositionCost(ctx context.Context, tx *sql.Tx, accountID string, positionCost *big.Rat) error {
	aggregateCost, err := queryRiskRat(ctx, tx, `
		SELECT COALESCE(SUM(cost_basis), 0)::text
		FROM execution_positions
		WHERE execution_account_id = $1`, accountID)
	if err != nil {
		return fmt.Errorf("load aggregate position risk: %w", err)
	}
	if positionCost.Cmp(aggregateCost) != 0 {
		return reject("RISK_POSITION_LEDGER_INCONSISTENT", "position lot cost does not equal the authoritative aggregate cost basis")
	}
	return nil
}

// addReservedBuyRisk 把其他活动买单尚未释放的余额预占加入风险敞口。
func addReservedBuyRisk(
	ctx context.Context,
	tx *sql.Tx,
	intent domain.OrderIntent,
	excludeOrderID string,
	exposure *liveRiskExposure,
) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT market_id, strategy_id, remaining_reserved_balance::text
		FROM asset_reservations
		WHERE execution_account_id = $1 AND side = 'BUY'
		  AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
		  AND order_id <> $2`, intent.ExecutionAccountID, strings.TrimSpace(excludeOrderID))
	if err != nil {
		return fmt.Errorf("load open BUY risk: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var marketID, strategyID, raw string
		if err := rows.Scan(&marketID, &strategyID, &raw); err != nil {
			return fmt.Errorf("scan open BUY risk: %w", err)
		}
		value, err := riskRat(domain.Decimal(raw))
		if err != nil || value.Sign() < 0 {
			return fmt.Errorf("invalid open BUY risk value %q", raw)
		}
		exposure.add(value, marketID, strategyID, intent)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate open BUY risk: %w", err)
	}
	return nil
}

// checkExposureHardLimits 依次检查市场、策略和钱包三个执行硬上限。
func checkExposureHardLimits(exposure liveRiskExposure, policy liveRiskPolicy) error {
	if exceedsRisk(exposure.market, policy.maxMarketExposure) {
		return reject("MAX_MARKET_EXPOSURE_EXCEEDED", "BUY would exceed the market exposure hard limit")
	}
	if exceedsRisk(exposure.strategy, policy.maxStrategyExposure) {
		return reject("MAX_STRATEGY_EXPOSURE_EXCEEDED", "BUY would exceed the strategy exposure hard limit")
	}
	if exceedsRisk(exposure.wallet, policy.maxWalletExposure) {
		return reject("MAX_WALLET_EXPOSURE_EXCEEDED", "BUY would exceed the wallet exposure hard limit")
	}
	return nil
}

// checkGlobalRiskControl 校验全局 Kill Switch 是否允许实盘新增订单。
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

// loadLiveRiskPolicy 在账户锁内加载并校验当前生效的完整硬风控策略。
func loadLiveRiskPolicy(ctx context.Context, tx *sql.Tx, accountID string) (liveRiskPolicy, error) {
	var policy liveRiskPolicy
	var priceAgeMS, signalAgeMS, stateAgeMS int64
	err := tx.QueryRowContext(ctx, `
		SELECT policy_id, version, enabled,
		       max_order_notional::text, max_market_exposure::text,
		       max_strategy_exposure::text, max_wallet_exposure::text,
		       max_daily_traded_notional::text,
		       max_price_age_ms, max_signal_age_ms, max_state_age_ms, daily_timezone
		FROM execution_risk_policies
		WHERE execution_account_id = $1 FOR SHARE`, accountID).Scan(
		&policy.policyID, &policy.version, &policy.enabled,
		&policy.maxOrderNotional, &policy.maxMarketExposure,
		&policy.maxStrategyExposure, &policy.maxWalletExposure,
		&policy.maxDailyTradedNotional,
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
	for name, value := range map[string]domain.Decimal{
		"max_order_notional":        policy.maxOrderNotional,
		"max_market_exposure":       policy.maxMarketExposure,
		"max_strategy_exposure":     policy.maxStrategyExposure,
		"max_wallet_exposure":       policy.maxWalletExposure,
		"max_daily_traded_notional": policy.maxDailyTradedNotional,
	} {
		parsed, parseErr := riskRat(value)
		if parseErr != nil || parsed.Sign() <= 0 {
			return liveRiskPolicy{}, fmt.Errorf("live risk policy %s is invalid", name)
		}
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

// checkLiveRiskBindingAndControls 校验模型策略账户绑定及账户、策略、市场暂停控制。
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

// checkLiveRiskState 校验账户最近对账完成且没有未关闭问题。
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

// validateLiveRiskTimestamps 校验实盘订单具备价格、信号和最坏价格证据。
func validateLiveRiskTimestamps(intent domain.OrderIntent, now time.Time) error {
	if intent.MarketSnapshotAt == nil || intent.MarketSnapshotAt.IsZero() {
		return reject("PRICE_TIMESTAMP_REQUIRED", "live order requires market_snapshot_at")
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

// checkRiskTimestamp 按硬风控允许的年龄和时钟偏差校验时间戳。
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

// riskDayBoundary 根据账户时区计算风险自然日的起止时间。
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

// riskDuration 把数据库毫秒配置转换为受限的时间长度。
func riskDuration(milliseconds int64) (time.Duration, error) {
	if milliseconds <= 0 || milliseconds > int64(maxRiskFreshnessWindow/time.Millisecond) {
		return 0, fmt.Errorf("milliseconds must be between 1 and %d", maxRiskFreshnessWindow/time.Millisecond)
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

// queryRiskCount 执行只返回单个整数的风险查询。
func queryRiskCount(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	var count int64
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// queryRiskRat 执行单值查询并转换为精确有理数。
func queryRiskRat(ctx context.Context, tx *sql.Tx, query string, args ...any) (*big.Rat, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return nil, err
	}
	return riskRat(domain.Decimal(raw))
}

// riskProduct 精确计算两个十进制风险值的乘积。
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

// riskRat 把领域十进制值转换成精确有理数。
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

// exceedsRisk 判断风险金额是否严格超过配置硬上限。
func exceedsRisk(value *big.Rat, maximum domain.Decimal) bool {
	limit, err := riskRat(maximum)
	return err != nil || value.Cmp(limit) > 0
}

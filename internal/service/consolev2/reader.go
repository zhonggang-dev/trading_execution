package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"
)

const (
	defaultLimit = 20
	maxLimit     = 100
	// MaxOffset follows the browser's Number.MAX_SAFE_INTEGER boundary (leaving
	// room for the largest supported page). The old 10,000/1,000,000 caps could
	// make a real last page unreachable when an exact total exceeded the cap.
	// Queries remain protected by the HTTP five-second deadline and the value is
	// always passed to PostgreSQL as a parameter.
	MaxOffset   = 9_007_199_254_740_891
	defaultAge  = 30 * 24 * time.Hour
	maxQueryLen = 128
)

const maxOffset = MaxOffset

func maxInt() int {
	return int(^uint(0) >> 1)
}

// Reader reads only durable, explicitly projected trading facts. It never
// invokes a venue, starts a job, or writes a database row.
type Reader struct {
	db                *sql.DB
	config            RuntimeConfig
	startedAt         time.Time
	now               func() time.Time
	runnerSnapshotSet bool
}

var _ API = (*Reader)(nil)

// New creates the v2 read model. A nil database is accepted so the runtime
// endpoint can still report disabled/configured state while data endpoints
// return a stable 503 instead of silently returning an empty success.
func New(db *sql.DB, config RuntimeConfig) *Reader {
	runnerSnapshotSet := config.RunnerSnapshot != nil
	if config.StartedAt.IsZero() {
		config.StartedAt = time.Now().UTC()
	}
	if config.RunnerInterval == 0 {
		config.RunnerInterval = 10 * time.Minute
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.RunnerSnapshot == nil {
		config.RunnerSnapshot = func() RunnerSnapshot { return RunnerSnapshot{} }
	}
	config.StartedAt = config.StartedAt.UTC()
	config.Bindings = append([]Binding(nil), config.Bindings...)
	config.SubmissionDisabledAccounts = append([]string(nil), config.SubmissionDisabledAccounts...)
	config.EntryDisabledAccounts = append([]string(nil), config.EntryDisabledAccounts...)
	config.StrategyDisabledAccounts = append([]string(nil), config.StrategyDisabledAccounts...)
	return &Reader{db: db, config: config, startedAt: config.StartedAt, now: config.Now, runnerSnapshotSet: runnerSnapshotSet}
}

// Runtime returns process-owned runner/config state. Runner state comes from
// DecisionRunner.Snapshot; the legacy live_runtime_status worker rows are
// intentionally not used as a substitute.
func (reader *Reader) Runtime(ctx context.Context) (Runtime, error) {
	if err := ctx.Err(); err != nil {
		return Runtime{}, err
	}
	now := reader.now().UTC()
	snapshot := reader.config.RunnerSnapshot()
	runner := RuntimeRunner{
		Enabled:           reader.config.RunnerEnabled,
		Running:           snapshot.LoopRunning,
		LoopStarted:       snapshot.LoopStarted,
		InFlight:          snapshot.InFlight,
		State:             runnerState(reader.config.RunnerEnabled, snapshot, !reader.runnerSnapshotSet),
		IntervalSeconds:   int64(reader.config.RunnerInterval / time.Second),
		LoopStoppedAt:     optionalTime(snapshot.LoopStoppedAt),
		NextBoundary:      optionalTime(snapshot.NextBoundary),
		NextDueAt:         optionalTime(snapshot.NextDueAt),
		LastStartedAt:     optionalTime(snapshot.LastStartedAt),
		LastCompletedAt:   optionalTime(snapshot.LastCompletedAt),
		LastSucceededAt:   optionalTime(snapshot.LastSucceededAt),
		HasLastError:      strings.TrimSpace(snapshot.LastError) != "",
		LastErrorCode:     runnerErrorCode(snapshot.LastError),
		SkippedBoundaries: snapshot.SkippedBoundaries,
	}
	accounts, accountDataState, accountErr := reader.runtimeAccounts(ctx)
	if accountErr != nil {
		// An explicitly cancelled request keeps the existing cancellation
		// contract. A database deadline, including the HTTP five-second read
		// deadline, only makes the account projection unavailable; runner and
		// configured bindings remain useful and must not be erased.
		if errors.Is(accountErr, context.Canceled) {
			return Runtime{}, accountErr
		}
		// Runner/config state is still useful when the database projection is
		// unavailable. Keep the failure explicit instead of returning an empty
		// account list that looks like there are no configured accounts.
		accounts, accountDataState = nil, "unavailable"
	}
	return Runtime{
		ObservedAt: now,
		Runner:     runner,
		Bindings:   append([]Binding(nil), reader.config.Bindings...),
		Submission: RuntimeSubmission{
			OrderSubmissionEnabled:     reader.config.OrderSubmissionEnabled,
			EntrySubmissionDisabled:    reader.config.EntrySubmissionDisabled,
			SubmissionDisabledAccounts: append([]string(nil), reader.config.SubmissionDisabledAccounts...),
			EntryDisabledAccounts:      append([]string(nil), reader.config.EntryDisabledAccounts...),
			StrategyDisabledAccounts:   append([]string(nil), reader.config.StrategyDisabledAccounts...),
			KalshiStrategyInputEnabled: reader.config.KalshiStrategyInputEnabled,
		},
		AccountDataState: accountDataState,
		Accounts:         accounts,
	}, nil
}

// runtimeAccounts reads the configured bindings and their independent
// database facts in one bounded repeatable-read snapshot. It is a projection,
// not a new execution-authorization algorithm; nil database values remain
// unknown and the caller reports an unavailable projection on query failure.
func (reader *Reader) runtimeAccounts(ctx context.Context) ([]RuntimeAccount, string, error) {
	if len(reader.config.Bindings) == 0 {
		return []RuntimeAccount{}, "available", nil
	}
	if reader.db == nil {
		return nil, "unavailable", nil
	}
	tx, err := reader.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, "unavailable", dbUnavailable("begin runtime accounts", err)
	}
	defer tx.Rollback()

	values := make([]string, 0, len(reader.config.Bindings))
	args := make([]any, 0, len(reader.config.Bindings)*3)
	for index, binding := range reader.config.Bindings {
		base := index*3 + 1
		values = append(values, fmt.Sprintf("($%d::text,$%d::text,$%d::text)", base, base+1, base+2))
		args = append(args, binding.ModelID, binding.StrategyID, binding.ExecutionAccountID)
	}
	query := `
WITH configured(model_id, strategy_id, execution_account_id) AS (VALUES ` + strings.Join(values, ",") + `)
SELECT configured.model_id, configured.strategy_id, configured.execution_account_id,
       binding.enabled, policy.enabled, control.paused,
		cycle.decision_at, cycle.status, cycle.entry_enabled, cycle.entry_block_reason,
       reconciliation.started_at, reconciliation.status,
       COALESCE(issues.open_issue_count, 0), COALESCE(orders.open_order_count, 0)
FROM configured
LEFT JOIN execution_strategy_bindings binding
  ON binding.model_id=configured.model_id
 AND binding.strategy_id=configured.strategy_id
 AND binding.execution_account_id=configured.execution_account_id
LEFT JOIN execution_risk_policies policy
  ON policy.execution_account_id=configured.execution_account_id
LEFT JOIN execution_risk_controls control
  ON control.execution_account_id=configured.execution_account_id
 AND control.control_scope='ACCOUNT' AND control.control_key=''
LEFT JOIN LATERAL (
    SELECT r.decision_at,
	           CASE
	             WHEN r.output_payload IS NULL THEN 'INPUT_RECORDED'
	             WHEN EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
	                          WHERE d.cycle_id=r.cycle_id AND d.status IN ('FAILED','UNKNOWN')) THEN 'DELIVERY_ATTENTION'
	             WHEN EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
	                          WHERE d.cycle_id=r.cycle_id AND d.status IN ('PENDING','SUBMITTING')) THEN 'DELIVERY_PENDING'
	             ELSE 'OUTPUT_RECORDED'
	           END AS status,
	           CASE
	             WHEN jsonb_typeof(r.output_payload->'entry_policy'->'enabled') = 'boolean'
	               THEN (r.output_payload->'entry_policy'->>'enabled')::boolean
	             ELSE NULL
	           END AS entry_enabled,
           NULLIF(r.output_payload->'entry_policy'->>'block_reason', '') AS entry_block_reason
    FROM strategy_decision_runs r
    WHERE r.model_id=configured.model_id
      AND r.strategy_id=configured.strategy_id
      AND r.execution_account_id=configured.execution_account_id
    ORDER BY r.decision_at DESC, r.cycle_id DESC
    LIMIT 1
) cycle ON TRUE
LEFT JOIN LATERAL (
    SELECT r.started_at, r.status
    FROM reconciliation_runs r
    WHERE r.execution_account_id=configured.execution_account_id
    ORDER BY r.started_at DESC, r.run_id DESC
    LIMIT 1
) reconciliation ON TRUE
LEFT JOIN LATERAL (
    SELECT count(*)::bigint AS open_issue_count
    FROM reconciliation_issues i
    WHERE i.execution_account_id=configured.execution_account_id AND i.status='OPEN'
) issues ON TRUE
LEFT JOIN LATERAL (
    SELECT count(*)::bigint AS open_order_count
    FROM execution_orders o
    WHERE o.execution_account_id=configured.execution_account_id
      AND o.status NOT IN ('FILLED','REJECTED','CANCELLED')
) orders ON TRUE
ORDER BY configured.execution_account_id, configured.model_id, configured.strategy_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "unavailable", dbUnavailable("read runtime accounts", err)
	}
	defer rows.Close()
	accounts := make([]RuntimeAccount, 0, len(reader.config.Bindings))
	for rows.Next() {
		var account RuntimeAccount
		var bindingEnabled, policyEnabled, accountPaused sql.NullBool
		var cycleAt, reconciliationAt sql.NullTime
		var cycleStatus, entryBlockReason, reconciliationStatus sql.NullString
		var cycleEntryEnabled sql.NullBool
		var openIssues, openOrders int64
		if err := rows.Scan(&account.ModelID, &account.StrategyID, &account.ExecutionAccountID,
			&bindingEnabled, &policyEnabled, &accountPaused,
			&cycleAt, &cycleStatus, &cycleEntryEnabled, &entryBlockReason,
			&reconciliationAt, &reconciliationStatus, &openIssues, &openOrders); err != nil {
			return nil, "unavailable", dbUnavailable("scan runtime accounts", err)
		}
		account.ConfiguredBinding = true
		account.DatabaseBindingEnabled = scanNullableBool(bindingEnabled)
		account.RiskPolicyEnabled = scanNullableBool(policyEnabled)
		account.AccountPaused = scanNullableBool(accountPaused)
		account.LastCycleAt = scanNullableTime(cycleAt)
		account.LastCycleStatus = cycleStatus.String
		account.LastCycleEntryBlockReason = scanNullableString(entryBlockReason)
		account.LastReconciliationAt = scanNullableTime(reconciliationAt)
		account.LastReconciliationStatus = reconciliationStatus.String
		if openIssues < 0 || openOrders < 0 || openIssues > int64(^uint(0)>>1) || openOrders > int64(^uint(0)>>1) {
			return nil, "unavailable", dbUnavailable("scan runtime accounts", fmt.Errorf("count outside supported range"))
		}
		account.OpenIssueCount, account.OpenOrderCount = int(openIssues), int(openOrders)
		account.StrategyInputState = runtimeAccountState(account.ExecutionAccountID, reader.config.SubmissionDisabledAccounts, reader.config.StrategyDisabledAccounts, false)
		account.EntryGateState = runtimeEntryGateState(
			cycleAt,
			cycleEntryEnabled,
			entryBlockReason,
		)
		account.OrderSubmissionState = runtimeAccountState(account.ExecutionAccountID, reader.config.SubmissionDisabledAccounts, nil, !reader.config.OrderSubmissionEnabled)
		if mode := runtimeBindingMode(reader.config.Bindings, account.ModelID, account.StrategyID, account.ExecutionAccountID); mode != "" {
			account.SourceMode = mode
		}
		accounts = append(accounts, account)
	}
	if err := queryRowsError(rows, "iterate runtime accounts"); err != nil {
		return nil, "unavailable", dbUnavailable("iterate runtime accounts", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, "unavailable", dbUnavailable("commit runtime accounts", err)
	}
	return accounts, "available", nil
}

func runtimeBindingMode(bindings []Binding, modelID, strategyID, accountID string) string {
	for _, binding := range bindings {
		if binding.ModelID == modelID && binding.StrategyID == strategyID && binding.ExecutionAccountID == accountID {
			return binding.SourceMode
		}
	}
	return ""
}

func runtimeAccountState(accountID string, disabledAccounts, strategyDisabled []string, globallyDisabled bool) string {
	if globallyDisabled || containsString(disabledAccounts, accountID) || containsString(strategyDisabled, accountID) {
		return "disabled"
	}
	return "enabled"
}

// runtimeEntryGateState keeps the current config and the latest durable cycle
// evidence separate. A persisted cycle with no Go-owned entry policy remains
// unknown; a later config change must not rewrite that historical fact. When
// no cycle exists yet there is no durable gate observation, so the state also
// remains unknown; current config switches are exposed in RuntimeSubmission.
func runtimeEntryGateState(cycleAt sql.NullTime, entryEnabled sql.NullBool, blockReason sql.NullString) string {
	if cycleAt.Valid {
		if blockReason.Valid && strings.TrimSpace(blockReason.String) != "" {
			return "sell_only"
		}
		if entryEnabled.Valid {
			if entryEnabled.Bool {
				return "enabled"
			}
			return "sell_only"
		}
		return "unknown"
	}
	return "unknown"
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// runnerErrorCode deliberately exposes only a stable operational category.
// DecisionRunner.LastError may wrap database, HTTP, or strategy errors, whose
// text can contain internal URLs or other implementation details. The full
// error remains in server logs and is never part of the browser DTO.
func runnerErrorCode(lastError string) string {
	lastError = strings.TrimSpace(lastError)
	if lastError == "" {
		return ""
	}
	if strings.HasPrefix(lastError, "pending strategy intent recovery:") {
		return "STARTUP_RECOVERY_FAILED"
	}
	if strings.HasPrefix(lastError, "decision boundary ") && strings.Contains(lastError, "was skipped after") {
		return "BOUNDARY_SKIPPED"
	}
	return "CYCLE_FAILED"
}

func runnerState(enabled bool, snapshot RunnerSnapshot, unknown bool) string {
	if !enabled {
		return "disabled"
	}
	if unknown {
		return "unknown"
	}
	if !snapshot.LoopStarted {
		return "not_started"
	}
	if snapshot.LoopRunning {
		return "running"
	}
	return "stopped"
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	utc := value.UTC()
	return &utc
}

// ServiceMetrics reports measurements available from the Go process. CPU,
// request rate, and container limits are intentionally null/omitted because
// this service has no authoritative collector for them.
func (reader *Reader) ServiceMetrics(ctx context.Context) (ServiceMetrics, error) {
	if err := ctx.Err(); err != nil {
		return ServiceMetrics{}, err
	}
	now := reader.now().UTC()
	uptime := int64(now.Sub(reader.startedAt) / time.Second)
	if uptime < 0 {
		uptime = 0
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return ServiceMetrics{
		Service:       "trading_execution",
		ObservedAt:    now,
		UptimeSeconds: uptime,
		Runtime:       RuntimeMetrics{Goroutines: runtime.NumGoroutine(), GoVersion: runtime.Version()},
		Memory: MemoryMetrics{
			HeapAllocBytes: stats.HeapAlloc, HeapSysBytes: stats.HeapSys,
			HeapInuseBytes: stats.HeapInuse, StackInuseBytes: stats.StackInuse, NumGC: stats.NumGC,
		},
		Availability: map[string]string{
			"cpu_percent":      "unavailable",
			"request_rate":     "unavailable",
			"container_limits": "unavailable",
		},
	}, nil
}

func (reader *Reader) requireDB(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader.db == nil {
		return ErrUnavailable
	}
	return nil
}

// beginReadTx gives a list request one consistent snapshot for both its exact
// count and page rows. A read-only repeatable-read transaction avoids a row
// being inserted or deleted between the two queries and keeps an empty or
// out-of-range page countable.
func (reader *Reader) beginReadTx(ctx context.Context, operation string) (*sql.Tx, error) {
	if err := reader.requireDB(ctx); err != nil {
		return nil, err
	}
	tx, err := reader.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, dbUnavailable("begin "+operation, err)
	}
	return tx, nil
}

func countProjection(ctx context.Context, tx *sql.Tx, projection, where string, args []any, operation string) (int, error) {
	query := "SELECT COUNT(*) FROM (" + projection + " WHERE " + where + ") AS filtered_rows"
	return countQuery(ctx, tx, query, args, operation)
}

// countFrom counts the same base row identity as a list query without running
// expensive display-only lateral projections. Callers must pass a fixed,
// internal FROM clause and a predicate built from the same query arguments.
func countFrom(ctx context.Context, tx *sql.Tx, from, where string, args []any, operation string) (int, error) {
	return countQuery(ctx, tx, "SELECT COUNT(*) FROM "+from+" WHERE "+where, args, operation)
}

func countQuery(ctx context.Context, tx *sql.Tx, query string, args []any, operation string) (int, error) {
	var total int64
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&total); err != nil {
		return 0, dbUnavailable("count "+operation, err)
	}
	if total < 0 || total > int64(^uint(0)>>1) {
		return 0, dbUnavailable("count "+operation, fmt.Errorf("total is outside the supported integer range"))
	}
	return int(total), nil
}

func normalizeFilter(filter ListFilter) (ListFilter, error) {
	if filter.Limit == 0 {
		filter.Limit = defaultLimit
	}
	if filter.Limit < 1 || filter.Limit > maxLimit {
		return ListFilter{}, fmt.Errorf("limit must be between 1 and %d", maxLimit)
	}
	if filter.Offset < 0 || filter.Offset > maxOffset {
		return ListFilter{}, fmt.Errorf("offset must be between 0 and %d", maxOffset)
	}
	for name, value := range map[string]string{
		"q": filter.Query, "model_id": filter.ModelID, "strategy_id": filter.StrategyID,
		"execution_account_id": filter.ExecutionAccountID, "market_source": filter.MarketSource,
		"status": filter.Status, "side": filter.Side,
	} {
		if len(value) > maxQueryLen || strings.ContainsAny(value, "\x00\r\n") {
			return ListFilter{}, fmt.Errorf("%s is invalid", name)
		}
	}
	if filter.From != nil {
		value := filter.From.UTC()
		filter.From = &value
	}
	if filter.To != nil {
		value := filter.To.UTC()
		filter.To = &value
	}
	if filter.From != nil && filter.To != nil && filter.To.Before(*filter.From) {
		return ListFilter{}, fmt.Errorf("to must not be before from")
	}
	return filter, nil
}

func boundedSince(filter ListFilter, now time.Time) time.Time {
	if filter.From != nil {
		return filter.From.UTC()
	}
	return now.UTC().Add(-defaultAge)
}

func pageLimitOffset(filter ListFilter, args *[]any) (string, string) {
	*args = append(*args, filter.Limit+1)
	limit := fmt.Sprintf("$%d", len(*args))
	*args = append(*args, filter.Offset)
	offset := fmt.Sprintf("$%d", len(*args))
	return limit, offset
}

func finishPage[T any](items []T, filter ListFilter, total int, now time.Time, summary map[string]any) Page[T] {
	hasMore := len(items) > filter.Limit
	if hasMore {
		items = items[:filter.Limit]
	}
	if items == nil {
		items = make([]T, 0)
	}
	// The exact count is authoritative for navigation, including a short page
	// returned after a concurrent change (which cannot happen inside the
	// repeatable-read transaction, but is cheap to make explicit here).
	hasMore = filter.Offset+len(items) < total
	return Page[T]{Items: items, Limit: filter.Limit, Offset: filter.Offset, Total: total, HasMore: hasMore, ObservedAt: now.UTC(), Summary: summary}
}

func scanNullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	text := value.String
	return &text
}

func scanNullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func scanNullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func scanNullableBool(value sql.NullBool) *bool {
	if !value.Valid {
		return nil
	}
	result := value.Bool
	return &result
}

func decodeJSONProjection(raw []byte, target any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode safe console projection: %w", err)
	}
	return nil
}

func queryRowsError(rows *sql.Rows, operation string) error {
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

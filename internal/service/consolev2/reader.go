package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"
)

const (
	defaultLimit = 20
	maxLimit     = 100
	maxOffset    = 10000
	defaultAge   = 30 * 24 * time.Hour
	maxQueryLen  = 128
)

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
	}, nil
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

func finishPage[T any](items []T, filter ListFilter, now time.Time, summary map[string]any) Page[T] {
	hasMore := len(items) > filter.Limit
	if hasMore {
		items = items[:filter.Limit]
	}
	if items == nil {
		items = make([]T, 0)
	}
	return Page[T]{Items: items, Limit: filter.Limit, Offset: filter.Offset, HasMore: hasMore, ObservedAt: now.UTC(), Summary: summary}
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

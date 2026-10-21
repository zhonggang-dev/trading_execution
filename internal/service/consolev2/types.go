// Package consolev2 contains the bounded, read-only projection used by the
// second generation trading console. It deliberately owns its DTOs so raw
// strategy and venue payloads cannot leak through the HTTP layer.
package consolev2

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrUnavailable means the required local read model or database is not
	// configured. Callers should expose this as a stable 503 response.
	ErrUnavailable = errors.New("console v2 trading read model unavailable")
	// ErrNotFound means a requested durable identity does not exist.
	ErrNotFound = errors.New("console v2 trading identity not found")
)

// API is the narrow interface consumed by the HTTP transport. Keeping the
// interface here lets transport tests use safe fake projections without a
// database or a live execution runtime.
type API interface {
	Runtime(context.Context) (Runtime, error)
	DecisionCycles(context.Context, ListFilter) (Page[DecisionCycle], error)
	DecisionCycle(context.Context, string) (DecisionCycleDetail, error)
	Orders(context.Context, ListFilter) (Page[Order], error)
	OrderTrace(context.Context, string) (OrderTrace, error)
	PositionLots(context.Context, ListFilter) (Page[PositionLot], error)
	ReconciliationRuns(context.Context, ListFilter) (Page[ReconciliationRun], error)
	ReconciliationIssues(context.Context, ListFilter) (Page[ReconciliationIssue], error)
	Redemptions(context.Context, ListFilter) (Page[Redemption], error)
	ServiceMetrics(context.Context) (ServiceMetrics, error)
}

// ListFilter is the shared bounded filter accepted by the v2 resources.
// Resource-specific validation is performed by the HTTP transport before the
// value reaches SQL; the reader validates bounds again for direct callers.
type ListFilter struct {
	Limit              int
	Offset             int
	Query              string
	ModelID            string
	StrategyID         string
	ExecutionAccountID string
	MarketSource       string
	Status             string
	Side               string
	From               *time.Time
	To                 *time.Time
}

// Page is the common finite list envelope. Total is the exact number of rows
// matching the applied filter in the same read snapshot as Items. It is kept
// beside HasMore for clients that still use cursor-like next-page controls.
type Page[T any] struct {
	Items      []T            `json:"items"`
	Limit      int            `json:"limit"`
	Offset     int            `json:"offset"`
	Total      int            `json:"total"`
	HasMore    bool           `json:"has_more"`
	ObservedAt time.Time      `json:"observed_at"`
	Summary    map[string]any `json:"summary,omitempty"`
}

// RuntimeConfig is the safe process-owned projection of config and runner
// state. It contains no token, URL, wallet address, credential, or secret.
type RuntimeConfig struct {
	StartedAt                  time.Time
	RunnerEnabled              bool
	RunnerInterval             time.Duration
	Bindings                   []Binding
	OrderSubmissionEnabled     bool
	EntrySubmissionDisabled    bool
	SubmissionDisabledAccounts []string
	EntryDisabledAccounts      []string
	StrategyDisabledAccounts   []string
	KalshiStrategyInputEnabled bool
	RunnerSnapshot             func() RunnerSnapshot
	Now                        func() time.Time
}

// RunnerSnapshot mirrors only credential-free fields from decisionrunner.Status.
type RunnerSnapshot struct {
	LoopStarted       bool
	LoopRunning       bool
	InFlight          bool
	LoopStoppedAt     time.Time
	NextBoundary      time.Time
	NextDueAt         time.Time
	LastStartedAt     time.Time
	LastCompletedAt   time.Time
	LastSucceededAt   time.Time
	LastError         string
	SkippedBoundaries uint64
}

// Binding is the runtime binding identity exposed to the console.
type Binding struct {
	PredictionModelID  string `json:"prediction_model_id"`
	ModelID            string `json:"model_id"`
	StrategyID         string `json:"strategy_id"`
	ExecutionAccountID string `json:"execution_account_id"`
	SourceMode         string `json:"source_mode"`
}

type Runtime struct {
	ObservedAt       time.Time         `json:"observed_at"`
	Runner           RuntimeRunner     `json:"runner"`
	Bindings         []Binding         `json:"bindings"`
	Submission       RuntimeSubmission `json:"submission"`
	AccountDataState string            `json:"account_data_state"`
	Accounts         []RuntimeAccount  `json:"accounts,omitempty"`
}

// RuntimeAccount keeps current configuration, database authorization, and
// the latest durable observations side by side. Nullable database fields are
// deliberately pointers: an unavailable read is different from a disabled
// account and must not be rendered as a fabricated false/zero.
type RuntimeAccount struct {
	ExecutionAccountID        string     `json:"execution_account_id"`
	ModelID                   string     `json:"model_id"`
	StrategyID                string     `json:"strategy_id"`
	SourceMode                string     `json:"source_mode,omitempty"`
	ConfiguredBinding         bool       `json:"configured_binding"`
	DatabaseBindingEnabled    *bool      `json:"database_binding_enabled"`
	RiskPolicyEnabled         *bool      `json:"risk_policy_enabled"`
	AccountPaused             *bool      `json:"account_paused"`
	StrategyInputState        string     `json:"strategy_input_state"`
	EntryGateState            string     `json:"entry_gate_state"`
	OrderSubmissionState      string     `json:"order_submission_state"`
	LastCycleAt               *time.Time `json:"last_cycle_at,omitempty"`
	LastCycleStatus           string     `json:"last_cycle_status,omitempty"`
	LastCycleEntryBlockReason *string    `json:"last_cycle_entry_block_reason,omitempty"`
	LastReconciliationAt      *time.Time `json:"last_reconciliation_at,omitempty"`
	LastReconciliationStatus  string     `json:"last_reconciliation_status,omitempty"`
	OpenIssueCount            int        `json:"open_issue_count"`
	OpenOrderCount            int        `json:"open_order_count"`
}

type RuntimeRunner struct {
	Enabled           bool       `json:"enabled"`
	Running           bool       `json:"running"`
	LoopStarted       bool       `json:"loop_started"`
	InFlight          bool       `json:"in_flight"`
	State             string     `json:"state"`
	IntervalSeconds   int64      `json:"interval_seconds"`
	LoopStoppedAt     *time.Time `json:"loop_stopped_at,omitempty"`
	NextBoundary      *time.Time `json:"next_boundary,omitempty"`
	NextDueAt         *time.Time `json:"next_due_at,omitempty"`
	LastStartedAt     *time.Time `json:"last_started_at,omitempty"`
	LastCompletedAt   *time.Time `json:"last_completed_at,omitempty"`
	LastSucceededAt   *time.Time `json:"last_succeeded_at,omitempty"`
	HasLastError      bool       `json:"has_last_error"`
	LastErrorCode     string     `json:"last_error_code,omitempty"`
	SkippedBoundaries uint64     `json:"skipped_boundaries"`
}

type RuntimeSubmission struct {
	OrderSubmissionEnabled     bool     `json:"order_submission_enabled"`
	EntrySubmissionDisabled    bool     `json:"entry_submission_disabled"`
	SubmissionDisabledAccounts []string `json:"submission_disabled_accounts,omitempty"`
	EntryDisabledAccounts      []string `json:"entry_disabled_accounts,omitempty"`
	StrategyDisabledAccounts   []string `json:"strategy_disabled_accounts,omitempty"`
	KalshiStrategyInputEnabled bool     `json:"kalshi_strategy_input_enabled"`
}

type DecisionCycle struct {
	CycleID                string     `json:"cycle_id"`
	DecisionAt             time.Time  `json:"decision_at"`
	ModelID                string     `json:"model_id"`
	PredictionModelID      *string    `json:"prediction_model_id"`
	StrategyID             string     `json:"strategy_id"`
	ExecutionAccountID     string     `json:"execution_account_id"`
	InputID                string     `json:"input_id"`
	PredictionSnapshotID   string     `json:"prediction_snapshot_id,omitempty"`
	OrderbookSnapshotSetID string     `json:"orderbook_snapshot_set_id,omitempty"`
	InputCreatedAt         time.Time  `json:"input_created_at"`
	OutputRecordedAt       *time.Time `json:"output_recorded_at,omitempty"`
	OrderSubmissionEnabled *bool      `json:"order_submission_enabled"`
	EntrySubmissionEnabled *bool      `json:"entry_submission_enabled"`
	EntryBlockReason       *string    `json:"entry_block_reason"`
	PredictionCount        int        `json:"prediction_count"`
	PositionLotCount       int        `json:"position_lot_count"`
	IntentCount            int        `json:"intent_count"`
	DeliveryPendingCount   int        `json:"delivery_pending_count"`
	DeliveryFailedCount    int        `json:"delivery_failed_count"`
	Status                 string     `json:"status"`
}

type PredictionReference struct {
	PredictionID string `json:"prediction_id"`
	SourceJobID  string `json:"source_job_id,omitempty"`
	SandboxID    string `json:"sandbox_id,omitempty"`
	MarketSource string `json:"market_source,omitempty"`
	MarketID     string `json:"market_id"`
	ConditionID  string `json:"condition_id"`
	TokenID      string `json:"token_id,omitempty"`
}

type OrderbookSnapshotReference struct {
	MarketSource string    `json:"market_source"`
	MarketID     string    `json:"market_id"`
	ConditionID  string    `json:"condition_id"`
	TokenID      string    `json:"token_id"`
	Status       string    `json:"status"`
	ErrorCode    string    `json:"error_code,omitempty"`
	ObservedAt   time.Time `json:"observed_at"`
}

type Delivery struct {
	ClientOrderID string     `json:"client_order_id"`
	CycleID       string     `json:"cycle_id"`
	SequenceNo    int        `json:"sequence_no"`
	Status        string     `json:"status"`
	AttemptCount  int        `json:"attempt_count"`
	OrderID       string     `json:"order_id,omitempty"`
	OrderStatus   string     `json:"order_status,omitempty"`
	Failed        bool       `json:"failed"`
	CreatedAt     time.Time  `json:"created_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

type DecisionCycleDetail struct {
	DecisionCycle
	Predictions        []PredictionReference        `json:"predictions"`
	OrderbookSnapshots []OrderbookSnapshotReference `json:"orderbook_snapshots"`
	Deliveries         []Delivery                   `json:"deliveries"`
}

type Order struct {
	OrderID            string               `json:"order_id"`
	ClientOrderID      string               `json:"client_order_id"`
	VenueOrderID       string               `json:"venue_order_id,omitempty"`
	Status             string               `json:"status"`
	ExecutionAccountID string               `json:"execution_account_id"`
	ModelID            string               `json:"model_id,omitempty"`
	StrategyID         string               `json:"strategy_id,omitempty"`
	Venue              string               `json:"venue"`
	MarketSource       string               `json:"market_source,omitempty"`
	MarketID           string               `json:"market_id"`
	ConditionID        string               `json:"condition_id,omitempty"`
	TokenID            string               `json:"token_id"`
	Side               string               `json:"side,omitempty"`
	Price              string               `json:"price,omitempty"`
	Shares             string               `json:"shares,omitempty"`
	FilledShares       string               `json:"filled_shares"`
	AverageFillPrice   string               `json:"average_fill_price,omitempty"`
	CreatedAt          time.Time            `json:"created_at"`
	UpdatedAt          time.Time            `json:"updated_at"`
	EvidenceSummary    OrderEvidenceSummary `json:"evidence_summary"`
}

// OrderEvidenceSummary is computed from durable fill/reservation/issue rows
// for the order. It is never inferred from a truncated trace array.
type OrderEvidenceSummary struct {
	FillEvidenceState  string `json:"fill_evidence_state"`
	AccountingState    string `json:"accounting_state"`
	ConfirmedFillCount int    `json:"confirmed_fill_count"`
	AppliedFillCount   int    `json:"applied_fill_count"`
	PendingFillCount   int    `json:"pending_fill_count"`
	OpenIssueCount     int    `json:"open_issue_count"`
	ReservationState   string `json:"reservation_state,omitempty"`
}

type OrderEvent struct {
	EventID          string     `json:"event_id"`
	OrderID          string     `json:"order_id"`
	Revision         int64      `json:"revision"`
	FromStatus       string     `json:"from_status,omitempty"`
	ToStatus         string     `json:"to_status"`
	Trigger          string     `json:"trigger"`
	AttemptID        string     `json:"attempt_id,omitempty"`
	FillKey          string     `json:"fill_key,omitempty"`
	ReasonCode       string     `json:"reason_code,omitempty"`
	VenueStatus      string     `json:"venue_status,omitempty"`
	VenueOrderID     string     `json:"venue_order_id,omitempty"`
	FilledShares     string     `json:"filled_shares"`
	AverageFillPrice string     `json:"average_fill_price,omitempty"`
	VenueObservedAt  *time.Time `json:"venue_observed_at,omitempty"`
	OccurredAt       time.Time  `json:"occurred_at"`
}

type OrderAttempt struct {
	AttemptID    string     `json:"attempt_id"`
	OrderID      string     `json:"order_id"`
	Sequence     int        `json:"sequence"`
	Kind         string     `json:"kind"`
	Outcome      string     `json:"outcome"`
	VenueOrderID string     `json:"venue_order_id,omitempty"`
	VenueStatus  string     `json:"venue_status,omitempty"`
	HTTPStatus   *int       `json:"http_status,omitempty"`
	ErrorCode    string     `json:"error_code,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

type Fill struct {
	FillKey            string     `json:"fill_key"`
	Venue              string     `json:"venue"`
	VenueFillID        string     `json:"venue_fill_id"`
	OrderID            string     `json:"order_id"`
	VenueOrderID       string     `json:"venue_order_id"`
	ExecutionAccountID string     `json:"execution_account_id"`
	MarketID           string     `json:"market_id"`
	ConditionID        string     `json:"condition_id,omitempty"`
	TokenID            string     `json:"token_id"`
	Side               string     `json:"side"`
	Status             string     `json:"status"`
	Shares             string     `json:"shares"`
	Price              string     `json:"price"`
	TotalFee           string     `json:"total_fee"`
	NetCashDelta       string     `json:"net_cash_delta"`
	TransactionHash    string     `json:"transaction_hash,omitempty"`
	MatchedAt          time.Time  `json:"matched_at"`
	ConfirmedAt        *time.Time `json:"confirmed_at,omitempty"`
	AppliedAt          *time.Time `json:"applied_at,omitempty"`
}

type PositionLot struct {
	LotID              string     `json:"lot_id"`
	ExecutionAccountID string     `json:"execution_account_id"`
	ModelID            string     `json:"model_id"`
	StrategyID         string     `json:"strategy_id"`
	Venue              string     `json:"venue,omitempty"`
	MarketSource       string     `json:"market_source,omitempty"`
	MarketID           string     `json:"market_id"`
	ConditionID        string     `json:"condition_id,omitempty"`
	TokenID            string     `json:"token_id"`
	OpeningOrderID     string     `json:"opening_order_id"`
	OpeningFillKey     string     `json:"opening_fill_key"`
	Status             string     `json:"status"`
	OriginalShares     string     `json:"original_shares"`
	RemainingShares    string     `json:"remaining_shares"`
	OriginalCost       string     `json:"original_cost"`
	RemainingCost      string     `json:"remaining_cost"`
	AverageEntryPrice  string     `json:"average_entry_price"`
	OpenedAt           time.Time  `json:"opened_at"`
	ClosedAt           *time.Time `json:"closed_at,omitempty"`
}

type ReconciliationRun struct {
	RunID              string     `json:"run_id"`
	ExecutionAccountID string     `json:"execution_account_id"`
	Trigger            string     `json:"trigger"`
	FocusOrderID       string     `json:"focus_order_id,omitempty"`
	Status             string     `json:"status"`
	StartedAt          time.Time  `json:"started_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
}

type ReconciliationIssue struct {
	IssueID            string     `json:"issue_id"`
	RunID              string     `json:"run_id"`
	ExecutionAccountID string     `json:"execution_account_id"`
	IssueType          string     `json:"issue_type"`
	ReasonCode         *string    `json:"reason_code"`
	Resolution         string     `json:"resolution"`
	Status             string     `json:"status"`
	OrderID            string     `json:"order_id,omitempty"`
	VenueOrderID       string     `json:"venue_order_id,omitempty"`
	VenueTradeID       string     `json:"venue_trade_id,omitempty"`
	MarketID           string     `json:"market_id,omitempty"`
	ConditionID        string     `json:"condition_id,omitempty"`
	TokenID            string     `json:"token_id,omitempty"`
	LocalValue         *string    `json:"local_value,omitempty"`
	RemoteValue        *string    `json:"remote_value,omitempty"`
	Source             string     `json:"source,omitempty"`
	ObservedAt         time.Time  `json:"observed_at"`
	FirstObservedAt    time.Time  `json:"first_observed_at"`
	ImpactScope        string     `json:"impact_scope"`
	ResolvedAt         *time.Time `json:"resolved_at,omitempty"`
}

type Redemption struct {
	RedemptionID       string     `json:"redemption_id"`
	ExecutionAccountID string     `json:"execution_account_id"`
	ConditionID        string     `json:"condition_id"`
	Status             string     `json:"status"`
	TransactionHash    string     `json:"transaction_hash,omitempty"`
	PayoutBaseUnits    *string    `json:"payout_base_units,omitempty"`
	ReceiptBlockNumber *int64     `json:"receipt_block_number,omitempty"`
	Confirmations      int64      `json:"confirmations"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	ConfirmedAt        *time.Time `json:"confirmed_at,omitempty"`
	AppliedAt          *time.Time `json:"applied_at,omitempty"`
}

type OrderTrace struct {
	Order                Order                 `json:"order"`
	Deliveries           []Delivery            `json:"deliveries"`
	Events               []OrderEvent          `json:"events"`
	Attempts             []OrderAttempt        `json:"attempts"`
	Fills                []Fill                `json:"fills"`
	PositionLots         []PositionLot         `json:"position_lots"`
	ReconciliationIssues []ReconciliationIssue `json:"reconciliation_issues"`
	ObservedAt           time.Time             `json:"observed_at"`
}

type ServiceMetrics struct {
	Service       string            `json:"service"`
	ObservedAt    time.Time         `json:"observed_at"`
	UptimeSeconds int64             `json:"uptime_seconds"`
	CPUPercent    *float64          `json:"cpu_percent"`
	RequestRate   *float64          `json:"request_rate"`
	Runtime       RuntimeMetrics    `json:"runtime"`
	Memory        MemoryMetrics     `json:"memory"`
	Availability  map[string]string `json:"availability"`
}

type RuntimeMetrics struct {
	Goroutines int    `json:"goroutines"`
	GoVersion  string `json:"go_version"`
}

type MemoryMetrics struct {
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapSysBytes    uint64 `json:"heap_sys_bytes"`
	HeapInuseBytes  uint64 `json:"heap_inuse_bytes"`
	StackInuseBytes uint64 `json:"stack_inuse_bytes"`
	NumGC           uint32 `json:"num_gc"`
}

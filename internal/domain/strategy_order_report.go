package domain

import "time"

// StrategyOrderReportSchemaVersion is the envelope version of
// POST /api/v1/strategy-order-reports.
const StrategyOrderReportSchemaVersion = "trading.strategy_order_report.v1"

// StrategyReportStatus is the order status vocabulary the strategy service
// understands. Terminal statuses with zero fill release the market for a retry;
// UNKNOWN keeps it occupied until a later report resolves it.
type StrategyReportStatus string

const (
	StrategyReportSubmitted       StrategyReportStatus = "SUBMITTED"
	StrategyReportOpen            StrategyReportStatus = "OPEN"
	StrategyReportPartiallyFilled StrategyReportStatus = "PARTIALLY_FILLED"
	StrategyReportUnknown         StrategyReportStatus = "UNKNOWN"
	StrategyReportFilled          StrategyReportStatus = "FILLED"
	StrategyReportCancelled       StrategyReportStatus = "CANCELLED"
	StrategyReportRejected        StrategyReportStatus = "REJECTED"
	StrategyReportNotSubmitted    StrategyReportStatus = "NOT_SUBMITTED"
)

// StrategyOrderReport is one cumulative order-state report. ReportSeq must be
// strictly increasing per DecisionID; it need not be contiguous.
type StrategyOrderReport struct {
	ReportSeq              int64                    `json:"report_seq"`
	Context                StrategyExecutionContext `json:"context"`
	DecisionID             string                   `json:"decision_id"`
	ConditionID            string                   `json:"condition_id,omitempty"`
	TokenID                string                   `json:"token_id,omitempty"`
	VenueOrderID           string                   `json:"venue_order_id,omitempty"`
	Status                 StrategyReportStatus     `json:"status"`
	CumulativeFilledShares Decimal                  `json:"cumulative_filled_shares"`
	AverageFillPrice       Decimal                  `json:"average_fill_price,omitempty"`
	FeesUSD                Decimal                  `json:"fees_usd,omitempty"`
	FailureCode            string                   `json:"failure_code,omitempty"`
	FailureReason          string                   `json:"failure_reason,omitempty"`
	EventAt                time.Time                `json:"event_at"`
}

// StrategyOrderReportResult is the per-report acknowledgement. A non-empty
// ErrorCode means the report was rejected and resending it will not help.
type StrategyOrderReportResult struct {
	DecisionID string
	ReportSeq  int64
	Outcome    string
	ErrorCode  string
	ErrorText  string
}

// StrategyUndeliveredDecision identifies a SUBMIT decision that never became an
// order, so the strategy service can release the market it occupied.
type StrategyUndeliveredDecision struct {
	DecisionID  string
	ConditionID string
	TokenID     string
}

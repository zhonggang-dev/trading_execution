package domain

import "time"

// RedemptionStatus is the durable lifecycle of one exact wallet/condition
// redemption. A submit-intent is persisted before any network mutation so a
// crash can never silently cause a second redemption attempt.
type RedemptionStatus string

const (
	RedemptionReady              RedemptionStatus = "READY"
	RedemptionApprovalSubmitting RedemptionStatus = "APPROVAL_SUBMITTING"
	RedemptionApprovalSubmitted  RedemptionStatus = "APPROVAL_SUBMITTED"
	RedemptionRedeemSubmitting   RedemptionStatus = "REDEEM_SUBMITTING"
	RedemptionRedeemSubmitted    RedemptionStatus = "REDEEM_SUBMITTED"
	RedemptionConfirmed          RedemptionStatus = "CONFIRMED"
	RedemptionApplied            RedemptionStatus = "APPLIED"
	RedemptionManualReview       RedemptionStatus = "MANUAL_REVIEW"
)

type RedemptionSubmissionKind string

const (
	RedemptionSubmissionApproval RedemptionSubmissionKind = "APPROVAL"
	RedemptionSubmissionRedeem   RedemptionSubmissionKind = "REDEEM"
)

type RedemptionSubmissionState string

const (
	RedemptionSubmissionPending   RedemptionSubmissionState = "PENDING"
	RedemptionSubmissionConfirmed RedemptionSubmissionState = "CONFIRMED"
	RedemptionSubmissionFailed    RedemptionSubmissionState = "FAILED"
)

// Redemption groups all managed outcome tokens in one account/condition.
// Polymarket redeemPositions consumes the complete condition balance, not an
// individual lot, so unmanaged baseline shares for this condition must be zero.
type Redemption struct {
	ExecutionAccountID  string           `json:"execution_account_id"`
	ConditionID         string           `json:"condition_id"`
	WalletAddress       string           `json:"wallet_address"`
	NegRisk             bool             `json:"neg_risk"`
	Status              RedemptionStatus `json:"status"`
	SubmissionProvider  string           `json:"submission_provider,omitempty"`
	SubmissionReference string           `json:"submission_reference,omitempty"`
	TransactionHash     string           `json:"transaction_hash,omitempty"`
	EventType           string           `json:"event_type,omitempty"`
	PayoutBaseUnits     string           `json:"payout_base_units,omitempty"`
	ReceiptBlockNumber  uint64           `json:"receipt_block_number,omitempty"`
	ReceiptBlockHash    string           `json:"receipt_block_hash,omitempty"`
	Confirmations       uint64           `json:"confirmations"`
	Attempts            int              `json:"attempts"`
	LastError           string           `json:"last_error,omitempty"`
	NextAttemptAt       time.Time        `json:"next_attempt_at"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
	SubmittingAt        *time.Time       `json:"submitting_at,omitempty"`
	SubmittedAt         *time.Time       `json:"submitted_at,omitempty"`
	ConfirmedAt         *time.Time       `json:"confirmed_at,omitempty"`
	AppliedAt           *time.Time       `json:"applied_at,omitempty"`
}

type RedemptionSubmission struct {
	Provider        string                    `json:"provider"`
	Reference       string                    `json:"reference"`
	TransactionHash string                    `json:"transaction_hash,omitempty"`
	State           RedemptionSubmissionState `json:"state"`
	FailureReason   string                    `json:"failure_reason,omitempty"`
}

type RedeemActivity struct {
	WalletAddress   string    `json:"wallet_address"`
	ConditionID     string    `json:"condition_id"`
	TransactionHash string    `json:"transaction_hash"`
	OccurredAt      time.Time `json:"occurred_at"`
}

type RedemptionReceipt struct {
	TransactionHash string `json:"transaction_hash"`
	WalletAddress   string `json:"wallet_address"`
	ConditionID     string `json:"condition_id"`
	EventType       string `json:"event_type"`
	PayoutBaseUnits string `json:"payout_base_units"`
	BlockNumber     uint64 `json:"block_number"`
	BlockHash       string `json:"block_hash"`
	Confirmations   uint64 `json:"confirmations"`
}

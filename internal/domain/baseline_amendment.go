package domain

import (
	"encoding/json"
	"time"
)

// BaselineAmendmentKind says how an append-only amendment changes the
// unmanaged baseline remaining of one token.
type BaselineAmendmentKind string

const (
	// BaselineAmendmentAddResidual registers shares that were already in the
	// wallet before the ledger took over and were missed by the baseline.
	BaselineAmendmentAddResidual BaselineAmendmentKind = "ADD_RESIDUAL"
	// BaselineAmendmentRetireConsumed retires the whole remaining baseline of a
	// token whose shares were burned on-chain.
	BaselineAmendmentRetireConsumed BaselineAmendmentKind = "RETIRE_CONSUMED"
)

// BaselineAmendmentEvidenceSchema versions the evidence document.
const BaselineAmendmentEvidenceSchema = "trading.external-position-baseline-amendment.v1"

// BaselineAmendment is one audited, append-only change to the unmanaged
// ownership equation of a token. It never alters the baseline, the managed
// position ledger, lots, fills or cash.
type BaselineAmendment struct {
	AmendmentID        string                `json:"amendment_id"`
	ExecutionAccountID string                `json:"execution_account_id"`
	TokenID            string                `json:"token_id"`
	ConditionID        string                `json:"condition_id"`
	OutcomeIndex       *int                  `json:"outcome_index,omitempty"`
	OutcomeName        string                `json:"outcome_name"`
	NegRisk            bool                  `json:"neg_risk"`
	Kind               BaselineAmendmentKind `json:"kind"`
	Shares             Decimal               `json:"shares"`
	ObservedAt         time.Time             `json:"observed_at"`
	Evidence           json.RawMessage       `json:"evidence"`
	Actor              string                `json:"actor"`
	Reason             string                `json:"reason"`
}

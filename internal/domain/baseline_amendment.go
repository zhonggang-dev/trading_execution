package domain

import (
	"encoding/json"
	"strings"
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

// ExternalPositionBaselineSourceAmendment is the source of a baseline entry
// that exists only because of an ADD_RESIDUAL amendment.
const ExternalPositionBaselineSourceAmendment = "BASELINE_AMENDMENT"

// DataAPIShareQuantum is the display precision of the Polymarket Data API
// position read model (a millishare). Amendment residuals are exact chain
// amounts, so a Data API total can legitimately differ from the ownership
// equation by up to one quantum.
const DataAPIShareQuantum = Decimal("0.001")

// BaselineSharesTolerance returns how far an external total may differ from
// baseline + managed shares. Sealed baseline evidence stays exact; only an
// amendment-derived residual compared against the Data API is allowed the
// source's own display quantum.
func BaselineSharesTolerance(baseline ExternalPositionBaseline, externalSource string) Decimal {
	if baseline.Source == ExternalPositionBaselineSourceAmendment &&
		strings.EqualFold(strings.TrimSpace(externalSource), "POLYMARKET_DATA_API") {
		return DataAPIShareQuantum
	}
	return "0"
}

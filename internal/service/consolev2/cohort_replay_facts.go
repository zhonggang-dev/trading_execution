package consolev2

import "time"

// CohortReplayLot is an internal, fee-exclusive projection of the same opening
// and disposition evidence that owns the displayed cohort financial metrics.
// It includes closed lots and must never be serialized as an HTTP response.
type CohortReplayLot struct {
	LotID             string
	Asset             CoreAssetFact
	OriginalShares    string
	RemainingShares   string
	OriginalPrincipal string
	OpenedAt          time.Time
	Dispositions      []CohortReplayDisposition
}

type CohortReplayDisposition struct {
	ID            string
	At            time.Time
	Shares        string
	GrossProceeds string
}

type CohortReplayFact struct {
	Lots []CohortReplayLot
}

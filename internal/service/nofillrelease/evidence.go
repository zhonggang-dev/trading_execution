package nofillrelease

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// EvidenceVersion identifies the layout of the audit evidence document.
const EvidenceVersion = 1

// Evidence is the complete, self-contained audit document of one no-fill
// decision. It is stored as JSON next to its sha256; it never contains
// credentials, signed bytes or RPC query strings.
type Evidence struct {
	Version      int       `json:"version"`
	Mode         string    `json:"mode"`
	Actor        string    `json:"actor"`
	Reason       string    `json:"reason,omitempty"`
	ResolvedAt   time.Time `json:"resolved_at"`
	BinaryCommit string    `json:"binary_commit,omitempty"`

	Order    EvidenceOrder    `json:"order"`
	Submit   EvidenceSubmit   `json:"submit"`
	TimeGate EvidenceTimeGate `json:"time_gate"`

	VenueOrderReads EvidenceOrderReads         `json:"venue_order_reads"`
	VenueOrder      domain.NoFillOrderRead     `json:"venue_order"`
	VenueTrades     domain.NoFillTradeScan     `json:"venue_trades"`
	VenueOpenOrders domain.NoFillOpenOrderScan `json:"venue_open_orders"`
	Chain           domain.NoFillChainEvidence `json:"chain"`

	Local          EvidenceLocal   `json:"local"`
	IssuesResolved []string        `json:"issues_resolved"`
	Release        EvidenceRelease `json:"release"`
}

// EvidenceOrder identifies the order and the revisions the evidence covers.
type EvidenceOrder struct {
	OrderID             string         `json:"order_id"`
	ExecutionAccountID  string         `json:"execution_account_id"`
	VenueOrderID        string         `json:"venue_order_id"`
	Side                domain.Side    `json:"side"`
	TokenID             string         `json:"token_id"`
	ConditionID         string         `json:"condition_id"`
	Size                domain.Decimal `json:"size"`
	WorstPrice          domain.Decimal `json:"worst_price"`
	CreatedAt           time.Time      `json:"created_at"`
	Revision            int64          `json:"revision"`
	ReservationRevision int64          `json:"reservation_revision"`
}

// EvidenceSubmit is the single SUBMIT attempt of the order.
type EvidenceSubmit struct {
	AttemptID      string     `json:"attempt_id"`
	StartedAt      time.Time  `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Outcome        string     `json:"outcome"`
	HTTPStatus     int        `json:"http_status,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	VenueExpiresAt *time.Time `json:"venue_expires_at,omitempty"`
}

// EvidenceTimeGate records the time condition.
type EvidenceTimeGate struct {
	Now           time.Time     `json:"now"`
	Rule          string        `json:"rule"`
	ExpiresAt     *time.Time    `json:"expires_at,omitempty"`
	Grace         time.Duration `json:"grace_ns"`
	Deadline      time.Time     `json:"deadline"`
	ScanFrom      time.Time     `json:"scan_from_not_after"`
	ScanToAtLeast time.Time     `json:"scan_to_not_before"`
}

// EvidenceOrderReads summarizes the recorded reconciliation reads.
type EvidenceOrderReads struct {
	Reconciles  int            `json:"reconcile_attempts"`
	Interrupted int            `json:"interrupted_attempts"`
	ErrorCodes  map[string]int `json:"error_codes"`
}

// EvidenceLocal is the local ledger view.
type EvidenceLocal struct {
	FilledSize       domain.Decimal           `json:"filled_size"`
	Fills            int                      `json:"fills"`
	FillEvents       int                      `json:"fill_events"`
	EventStatuses    []domain.OrderStatus     `json:"event_statuses"`
	ReservationState domain.ReservationStatus `json:"reservation_status"`
	OpenIssues       []domain.NoFillIssueRef  `json:"open_issues"`
}

// EvidenceRelease is what the release frees.
type EvidenceRelease struct {
	Side    domain.Side    `json:"side"`
	Balance domain.Decimal `json:"balance"`
	Shares  domain.Decimal `json:"shares"`
}

// Encode returns the stored JSON document and its sha256.
func (evidence Evidence) Encode() ([]byte, string, error) {
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return nil, "", fmt.Errorf("encode no-fill evidence: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(digest[:]), nil
}

// Summary is the one-line form stored on the order event.
func (evidence Evidence) Summary(digest string) string {
	expires := "none"
	if evidence.TimeGate.ExpiresAt != nil {
		expires = evidence.TimeGate.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf(
		"expires_at=%s grace=%s trades=%d open=%d chain[%d..%d]=%d digest=%s",
		expires, evidence.TimeGate.Grace, len(evidence.VenueTrades.References),
		boolCount(evidence.VenueOpenOrders.Contains), evidence.Chain.FromBlock, evidence.Chain.ToBlock,
		len(evidence.Chain.Matches), digest,
	)
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

// ConfirmID is the operator confirmation identity: the stable description of
// what a release would change (the order, its venue identity, its submit
// attempt and the exact reservation remainder). Revisions are deliberately
// not part of it: recovery keeps polling a stuck order between the dry run and
// the apply, and the release transaction re-checks that every such change was
// a read that never saw the order.
func ConfirmID(facts domain.NoFillLocalFacts) string {
	released := releaseOf(facts)
	submitAttempt := ""
	for _, attempt := range facts.Attempts {
		if attempt.Kind == domain.OrderAttemptSubmit {
			submitAttempt = attempt.ID
		}
	}
	payload := strings.Join([]string{
		facts.Order.ID,
		facts.Order.Intent.ExecutionAccountID,
		strings.ToLower(strings.TrimSpace(facts.Order.VenueOrderID)),
		submitAttempt,
		string(released.Side), released.Balance.String(), released.Shares.String(),
	}, "|")
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func releaseOf(facts domain.NoFillLocalFacts) EvidenceRelease {
	release := EvidenceRelease{Side: facts.Order.Intent.Side, Balance: "0", Shares: "0"}
	if facts.Reservation == nil {
		return release
	}
	release.Side = facts.Reservation.Side
	if facts.Reservation.Side == domain.SideBuy {
		release.Balance = facts.Reservation.RemainingReservedBalance
	} else {
		release.Shares = facts.Reservation.RemainingReservedShares
	}
	return release
}

func binaryCommit() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return ""
}

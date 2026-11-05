package nofillrelease

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// DecisionKind is the outcome of one evaluation. Only Resolve permits a
// release; every other kind keeps the order UNKNOWN and its reservation frozen.
type DecisionKind string

const (
	// DecisionNotEligible: the order is outside the no-fill path for good
	// (wrong venue/state, any recorded fill, the order was ever seen at the
	// venue, an unrelated OPEN issue, no venue expiry in automatic mode).
	DecisionNotEligible DecisionKind = "NOT_ELIGIBLE"
	// DecisionNotYet: the time gate or the chain window is not reached.
	DecisionNotYet DecisionKind = "NOT_YET"
	// DecisionInconclusive: a source could not be read completely.
	DecisionInconclusive DecisionKind = "INCONCLUSIVE"
	// DecisionFillObserved: a venue trade or an OrderFilled log names the
	// order. The order must go through fill synchronization, never release.
	DecisionFillObserved DecisionKind = "FILL_OBSERVED"
	// DecisionResolve: every source agrees the venue never accepted the order.
	DecisionResolve DecisionKind = "RESOLVE"
)

// Decision is the evaluation outcome with its reason.
type Decision struct {
	Kind   DecisionKind `json:"kind"`
	Reason string       `json:"reason"`
	// routine marks an expected, high-frequency outcome (no venue expiry on
	// a historical GTC order) that is logged at debug level only.
	routine bool
}

func decision(kind DecisionKind, format string, args ...any) Decision {
	return Decision{Kind: kind, Reason: fmt.Sprintf(format, args...)}
}

// Policy bounds the time gate and the chain scan.
type Policy struct {
	// Grace is waited after the venue expiry before any release (default
	// 30m): it covers on-chain settlement of a late match, the confirmation
	// depth and a lagging RPC backend.
	Grace time.Duration
	// BeforeSubmitMargin moves the trade listing and the chain scan start
	// before the order was created (default 10m).
	BeforeSubmitMargin time.Duration
	// AfterExpiryChainMargin is how far past the venue expiry the newest
	// confirmed scanned block must reach (default 10m).
	AfterExpiryChainMargin time.Duration
	// MaxChunks caps the eth_getLogs ranges of one scan (automatic default 30).
	MaxChunks int
	// AllowLegacyNoExpiry admits historical GTC orders without a venue expiry
	// (operator tool only, with --legacy-no-expiration).
	AllowLegacyNoExpiry bool
	// LegacyMinAge is the minimum age of such an order's submit attempt.
	LegacyMinAge time.Duration
	// RequireLeaseFree refuses while another worker holds the recovery lease.
	RequireLeaseFree bool
}

const (
	defaultGrace                  = 30 * time.Minute
	defaultBeforeSubmitMargin     = 10 * time.Minute
	defaultAfterExpiryChainMargin = 10 * time.Minute
	defaultMaxChunks              = 30
	defaultLegacyMinAge           = time.Hour
)

// Normalize fills defaults and validates the policy.
func (policy Policy) Normalize() (Policy, error) {
	if policy.Grace == 0 {
		policy.Grace = defaultGrace
	}
	if policy.BeforeSubmitMargin == 0 {
		policy.BeforeSubmitMargin = defaultBeforeSubmitMargin
	}
	if policy.AfterExpiryChainMargin == 0 {
		policy.AfterExpiryChainMargin = defaultAfterExpiryChainMargin
	}
	if policy.MaxChunks == 0 {
		policy.MaxChunks = defaultMaxChunks
	}
	if policy.LegacyMinAge == 0 {
		policy.LegacyMinAge = defaultLegacyMinAge
	}
	if policy.Grace < 15*time.Minute || policy.Grace > 24*time.Hour {
		return Policy{}, fmt.Errorf("no-fill release grace must be between 15m and 24h")
	}
	if policy.AfterExpiryChainMargin >= policy.Grace {
		return Policy{}, fmt.Errorf("no-fill chain margin must be shorter than the grace")
	}
	if policy.BeforeSubmitMargin < time.Minute || policy.AfterExpiryChainMargin < time.Minute {
		return Policy{}, fmt.Errorf("no-fill scan margins must be at least one minute")
	}
	if policy.MaxChunks < 1 || policy.MaxChunks > 100_000 {
		return Policy{}, fmt.Errorf("no-fill chain chunk budget must be between 1 and 100000")
	}
	if policy.LegacyMinAge < 30*time.Minute {
		return Policy{}, fmt.Errorf("legacy no-expiry minimum age must be at least 30m")
	}
	return policy, nil
}

// localGate is what the local checks established for the remote stage.
type localGate struct {
	submit      domain.OrderAttempt
	reads       EvidenceOrderReads
	timeGate    EvidenceTimeGate
	scanRequest domain.NoFillChainScanRequest
	tradesAfter time.Time
	releasable  []string
	release     EvidenceRelease
	reservation domain.AssetReservation
}

// checkLocal applies every condition decidable from the local ledger and the
// clock (G0 scope, G1 time, G2 no local fill, G3 read history, G7 issues).
func checkLocal(facts domain.NoFillLocalFacts, policy Policy, now time.Time) (localGate, Decision) {
	order := facts.Order
	if !strings.EqualFold(strings.TrimSpace(order.Intent.Venue), "polymarket") {
		return localGate{}, decision(DecisionNotEligible, "venue %q is not Polymarket", order.Intent.Venue)
	}
	if order.Status != domain.OrderStatusUnknown {
		return localGate{}, decision(DecisionNotEligible, "order status is %s, not UNKNOWN", order.Status)
	}
	if !isOrderHash(order.VenueOrderID) {
		return localGate{}, decision(DecisionNotEligible, "venue order id is not a 32-byte order hash")
	}
	if !zero(order.FilledSize) || !zero(order.FilledNotional) || !zero(order.TotalFees) {
		return localGate{}, decision(DecisionNotEligible, "order has a recorded fill")
	}
	if facts.FillCount != 0 || facts.FillEventCount != 0 {
		return localGate{}, decision(DecisionNotEligible, "order has %d fill(s) and %d fill event(s)", facts.FillCount, facts.FillEventCount)
	}
	if err := domain.NoFillHistoryError(facts.Attempts, facts.EventStatuses); err != nil {
		return localGate{}, decision(DecisionNotEligible, "%v", err)
	}
	gate := localGate{reads: EvidenceOrderReads{ErrorCodes: map[string]int{}}}
	for _, attempt := range facts.Attempts {
		switch attempt.Kind {
		case domain.OrderAttemptSubmit:
			gate.submit = attempt
		case domain.OrderAttemptReconcile:
			gate.reads.Reconciles++
			if attempt.Outcome == domain.AttemptOutcomeStarted {
				gate.reads.Interrupted++
				continue
			}
			gate.reads.ErrorCodes[strings.ToUpper(strings.TrimSpace(attempt.ErrorCode))]++
		}
	}
	if !strings.EqualFold(strings.TrimSpace(gate.submit.VenueOrderID), strings.TrimSpace(order.VenueOrderID)) &&
		strings.TrimSpace(gate.submit.VenueOrderID) != "" {
		return localGate{}, decision(DecisionNotEligible, "submit attempt venue order id differs from the order")
	}
	if facts.Reservation == nil {
		return localGate{}, decision(DecisionNotEligible, "order has no reservation")
	}
	reservation := *facts.Reservation
	if reservation.Status != domain.ReservationStatusActive && reservation.Status != domain.ReservationStatusReconciliationRequired {
		return localGate{}, decision(DecisionNotEligible, "reservation is %s", reservation.Status)
	}
	if !zero(reservation.SettledShares) || !zero(reservation.SettledNotional) || !zero(reservation.SettledFees) {
		return localGate{}, decision(DecisionNotEligible, "reservation has settled amounts")
	}
	gate.reservation = reservation
	gate.release = releaseOf(facts)
	for _, issue := range facts.OpenIssues {
		if !domain.NoFillReleasableIssue(issue) {
			return localGate{}, decision(DecisionNotEligible, "order has an OPEN %s issue from %s", issue.Type, issue.Source)
		}
		gate.releasable = append(gate.releasable, issue.IssueID)
	}
	if policy.RequireLeaseFree && facts.LeaseHeld {
		return localGate{}, decision(DecisionNotYet, "another worker holds the order recovery lease")
	}

	now = now.UTC()
	submitStart := gate.submit.StartedAt.UTC()
	scanFrom := submitStart
	if created := order.CreatedAt.UTC(); !created.IsZero() && created.Before(scanFrom) {
		scanFrom = created
	}
	scanFrom = scanFrom.Add(-policy.BeforeSubmitMargin)
	gate.tradesAfter = scanFrom
	gate.timeGate = EvidenceTimeGate{Now: now, Grace: policy.Grace, ScanFrom: scanFrom}
	if gate.submit.VenueExpiresAt != nil {
		expiresAt := gate.submit.VenueExpiresAt.UTC()
		gate.timeGate.Rule = "VENUE_EXPIRY"
		gate.timeGate.ExpiresAt = &expiresAt
		gate.timeGate.Deadline = expiresAt.Add(policy.Grace)
		gate.timeGate.ScanToAtLeast = expiresAt.Add(policy.AfterExpiryChainMargin)
	} else {
		if !policy.AllowLegacyNoExpiry {
			noExpiry := decision(DecisionNotEligible, "order has no venue expiry; only the operator tool with --legacy-no-expiration may release it")
			noExpiry.routine = true
			return localGate{}, noExpiry
		}
		gate.timeGate.Rule = "LEGACY_NO_EXPIRY_MIN_AGE"
		gate.timeGate.Grace = policy.LegacyMinAge
		gate.timeGate.Deadline = submitStart.Add(policy.LegacyMinAge)
		reach := policy.LegacyMinAge - policy.AfterExpiryChainMargin
		if reach < policy.AfterExpiryChainMargin {
			reach = policy.AfterExpiryChainMargin
		}
		gate.timeGate.ScanToAtLeast = submitStart.Add(reach)
	}
	if now.Before(gate.timeGate.Deadline) {
		return gate, decision(DecisionNotYet, "time gate opens at %s", gate.timeGate.Deadline.Format(time.RFC3339))
	}
	gate.scanRequest = domain.NoFillChainScanRequest{
		OrderHash: order.VenueOrderID, FromNotAfter: scanFrom, ToNotBefore: gate.timeGate.ScanToAtLeast,
		MaxChunks: policy.MaxChunks,
	}
	return gate, Decision{Kind: DecisionResolve, Reason: "local evidence complete"}
}

// remoteResult is what the venue and chain reads returned.
type remoteResult struct {
	order     domain.NoFillOrderRead
	orderErr  error
	trades    domain.NoFillTradeScan
	tradesErr error
	open      domain.NoFillOpenOrderScan
	openErr   error
	chain     domain.NoFillChainEvidence
	chainErr  error
	// windowNotReached marks a chain error that only means "later".
	windowNotReached bool
}

// decideVenue applies G3 (the current read), G4 (trades) and G5 (open orders).
func decideVenue(result remoteResult) Decision {
	if result.orderErr != nil {
		return decision(DecisionInconclusive, "order read failed: %v", result.orderErr)
	}
	if !result.order.NotFound {
		return decision(DecisionNotEligible, "the venue returns the order")
	}
	if result.tradesErr != nil {
		return decision(DecisionInconclusive, "trade listing failed: %v", result.tradesErr)
	}
	if len(result.trades.References) > 0 {
		reference := result.trades.References[0]
		return decision(DecisionFillObserved, "venue trade %s (%s, %s) names the order", reference.TradeID, reference.Status, reference.Role)
	}
	if result.openErr != nil {
		return decision(DecisionInconclusive, "open-order listing failed: %v", result.openErr)
	}
	if result.open.Contains {
		return decision(DecisionNotEligible, "the order is listed as open at the venue")
	}
	return Decision{Kind: DecisionResolve, Reason: "venue evidence complete"}
}

// decideChain applies G6.
func decideChain(result remoteResult) Decision {
	if result.chainErr != nil {
		if result.windowNotReached {
			return decision(DecisionNotYet, "chain window not reached: %v", result.chainErr)
		}
		return decision(DecisionInconclusive, "OrderFilled scan failed: %v", result.chainErr)
	}
	if len(result.chain.Matches) > 0 {
		match := result.chain.Matches[0]
		return decision(DecisionFillObserved, "OrderFilled %s:%d in block %d names the order", match.TransactionHash, match.LogIndex, match.BlockNumber)
	}
	if len(result.chain.Controls) < 3 {
		return decision(DecisionInconclusive, "OrderFilled scan has %d control block(s); 3 are required", len(result.chain.Controls))
	}
	for _, control := range result.chain.Controls {
		if control.Logs < 1 {
			return decision(DecisionInconclusive, "control block %d returned no OrderFilled log", control.BlockNumber)
		}
	}
	if result.chain.ToBlockHashBefore == "" || result.chain.ToBlockHashBefore != result.chain.ToBlockHashAfter {
		return decision(DecisionInconclusive, "scanned range was not stable")
	}
	return Decision{Kind: DecisionResolve, Reason: "chain evidence complete"}
}

func isOrderHash(value string) bool {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "0x") || len(value) != 66 {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}

func zero(value domain.Decimal) bool {
	if value.IsEmpty() {
		return true
	}
	sign, err := value.Sign()
	return err == nil && sign == 0
}

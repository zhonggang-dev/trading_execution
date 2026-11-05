package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// No-fill release of an unaccepted Polymarket order (migration 0046).
//
// A placement whose outcome is unknown leaves the order UNKNOWN and its
// reservation frozen. The order may only be cancelled and its reservation
// released when every independent source proves the venue never accepted or
// matched it. Incomplete evidence always keeps the order frozen.
const (
	// NoFillReleaseAutoReasonCode marks a release decided by the service.
	NoFillReleaseAutoReasonCode = "VENUE_UNACCEPTED_NO_FILL_RELEASE"
	// NoFillReleaseOperatorReasonCode marks a release confirmed by an
	// operator with cmd/nofillrelease. It is the code used by the historical
	// manual SQL releases, so both paths stay comparable.
	NoFillReleaseOperatorReasonCode = "OPERATOR_NO_FILL_RELEASE"
	// NoFillReleaseVenueStatus is recorded on the release events.
	NoFillReleaseVenueStatus = "NOT_FOUND"
)

// ErrNoFillChainWindowNotReached reports that the newest confirmed block is
// still older than the no-fill scan must reach. It means "later", not failure.
var ErrNoFillChainWindowNotReached = errors.New("confirmed chain head has not reached the required scan window")

// NoFillResolutionMode is how a complete no-fill evaluation was recorded.
type NoFillResolutionMode string

const (
	// NoFillModeShadow records the evaluation only; nothing else changes.
	NoFillModeShadow NoFillResolutionMode = "SHADOW"
	// NoFillModeAuto cancels the order and releases its reservation.
	NoFillModeAuto NoFillResolutionMode = "AUTO"
	// NoFillModeOperator is an operator-confirmed release.
	NoFillModeOperator NoFillResolutionMode = "OPERATOR"
)

// NoFillIssueRef is one OPEN reconciliation issue of the order.
type NoFillIssueRef struct {
	IssueID    string                   `json:"issue_id"`
	Type       ReconciliationIssueType  `json:"issue_type"`
	Source     string                   `json:"source"`
	Resolution ReconciliationResolution `json:"resolution"`
}

// NoFillLocalFacts is the local ledger view of one order.
type NoFillLocalFacts struct {
	Order          Order
	Attempts       []OrderAttempt
	EventStatuses  []OrderStatus
	FillCount      int
	FillEventCount int
	// Reservation is nil when the order has no reservation row.
	Reservation *AssetReservation
	OpenIssues  []NoFillIssueRef
	// LeaseHeld reports an unexpired order recovery lease.
	LeaseHeld bool
}

// NoFillReleaseSourceOrder is the issue source of a failed single-order read.
const NoFillReleaseSourceOrder = "CLOB_ORDER"

// NoFillReleasableIssue reports whether an OPEN issue of the order is one the
// release explains and closes: the failed order read itself and the recovery
// bookkeeping issues. Any other OPEN issue (a missed fill, a drift) blocks the
// release.
func NoFillReleasableIssue(issue NoFillIssueRef) bool {
	switch issue.Type {
	case ReconciliationIssueSourceUnavailable:
		return issue.Source == NoFillReleaseSourceOrder
	case ReconciliationIssueOrderRecoveryPending, ReconciliationIssueOrderRecoveryStalled:
		return true
	default:
		return false
	}
}

// NoFillOrderRead is the result of the single-order read.
type NoFillOrderRead struct {
	NotFound bool      `json:"not_found"`
	Code     string    `json:"code"`
	ReadAt   time.Time `json:"read_at"`
}

// NoFillTradeReference is a venue trade that names the order hash.
type NoFillTradeReference struct {
	TradeID string `json:"trade_id"`
	Status  string `json:"status"`
	Role    string `json:"role"`
}

// NoFillTradeScan is the complete venue trade listing used as evidence.
type NoFillTradeScan struct {
	Market     string                 `json:"market"`
	After      time.Time              `json:"after"`
	Scanned    int                    `json:"trades_scanned"`
	References []NoFillTradeReference `json:"references"`
	ReadAt     time.Time              `json:"read_at"`
}

// NoFillOpenOrderScan is the venue open-order listing used as evidence.
type NoFillOpenOrderScan struct {
	Market   string    `json:"market"`
	TokenID  string    `json:"token_id"`
	Returned int       `json:"returned"`
	Contains bool      `json:"contains"`
	ReadAt   time.Time `json:"read_at"`
}

// NoFillChainScanRequest bounds the OrderFilled scan by order hash.
type NoFillChainScanRequest struct {
	OrderHash string
	// FromNotAfter: the first scanned block must not be newer than this.
	FromNotAfter time.Time
	// ToNotBefore: the last scanned block must not be older than this.
	ToNotBefore time.Time
	// MaxChunks caps the number of eth_getLogs ranges.
	MaxChunks int
}

// NoFillChainMatch is one OrderFilled log naming the order hash.
type NoFillChainMatch struct {
	TransactionHash string `json:"transaction_hash"`
	LogIndex        uint64 `json:"log_index"`
	BlockNumber     uint64 `json:"block_number"`
	Exchange        string `json:"exchange"`
}

// NoFillChainControl is one single-block control query that proves the RPC
// backend returns OrderFilled logs for the scanned range.
type NoFillChainControl struct {
	BlockNumber uint64 `json:"block_number"`
	Logs        int    `json:"logs"`
}

// NoFillChainEvidence is the confirmed-range OrderFilled scan.
type NoFillChainEvidence struct {
	ChainID           uint64               `json:"chain_id"`
	Exchanges         []string             `json:"exchanges"`
	Topic0            string               `json:"topic0"`
	Topic1            string               `json:"topic1"`
	FromBlock         uint64               `json:"from_block"`
	FromBlockTime     time.Time            `json:"from_block_time"`
	ToBlock           uint64               `json:"to_block"`
	ToBlockTime       time.Time            `json:"to_block_time"`
	ToBlockHashBefore string               `json:"to_block_hash_before"`
	ToBlockHashAfter  string               `json:"to_block_hash_after"`
	HeadBefore        uint64               `json:"head_before"`
	HeadAfter         uint64               `json:"head_after"`
	Confirmations     uint64               `json:"confirmations"`
	ChunkBlocks       uint64               `json:"chunk_blocks"`
	Chunks            int                  `json:"chunks"`
	Matches           []NoFillChainMatch   `json:"matches"`
	Controls          []NoFillChainControl `json:"controls"`
	RPCHost           string               `json:"rpc_host"`
	ReadAt            time.Time            `json:"read_at"`
}

// NoFillResolution is one recorded no-fill decision.
type NoFillResolution struct {
	Mode               NoFillResolutionMode
	OrderID            string
	ExecutionAccountID string
	Actor              string
	Reason             string
	ReasonCode         string
	// ExpectedOrderRevision is the revision the evidence was collected at;
	// the transaction refuses any other revision.
	ExpectedOrderRevision int64
	// ExpectedReservationRevision is checked the same way.
	ExpectedReservationRevision int64
	Evidence                    []byte
	EvidenceSHA256              string
	Summary                     string
	ResolvedAt                  time.Time
	// RequireLeaseFree makes the release fail while another worker holds the
	// order recovery lease (operator tool). The service path holds it itself.
	RequireLeaseFree bool
	// AllowReadOnlyProgress accepts a newer order/reservation revision when
	// the only progress since the evidence was further reconciliation reads
	// that did not reveal the order (operator tool: recovery keeps polling a
	// stuck order while the evidence is collected). The order history and the
	// released amounts are re-checked inside the transaction either way.
	AllowReadOnlyProgress bool
	// ExpectedReleaseBalance and ExpectedReleaseShares are the reservation
	// remainder the evidence describes; the transaction refuses any other.
	ExpectedReleaseBalance Decimal
	ExpectedReleaseShares  Decimal
}

// noFillHistoryStatuses are the only order states an unaccepted order can have
// passed through. Any acknowledgement, fill, cancel or rejection means the
// venue (or an operator) has already spoken.
var noFillHistoryStatuses = map[OrderStatus]struct{}{
	OrderStatusReceived:    {},
	OrderStatusValidating:  {},
	OrderStatusReserved:    {},
	OrderStatusSubmitting:  {},
	OrderStatusUnknown:     {},
	OrderStatusReconciling: {},
}

// noFillReadFailureCodes are reconciliation read outcomes that do not reveal
// the order: the order was not found or the read itself failed.
var noFillReadFailureCodes = map[string]struct{}{
	"CLOB_ORDER_NOT_FOUND":       {},
	"CLOB_TRANSPORT_UNAVAILABLE": {},
	"CLOB_SERVER_ERROR":          {},
	"CLOB_RATE_LIMITED":          {},
	"CLOB_RESPONSE_UNAVAILABLE":  {},
}

// noFillAcceptedSubmitCodes are SUBMIT outcomes recorded after the venue
// answered the POST with a success response (accepted, but the body was
// unusable or named another order id). Such an order was accepted, and with
// a mismatching id every hash-keyed source (single read, trades, open orders,
// OrderFilled) would look at the wrong order, so it never qualifies.
var noFillAcceptedSubmitCodes = map[string]struct{}{
	"CLOB_ORDER_ID_MISSING":      {},
	"CLOB_ORDER_ID_MISMATCH":     {},
	"CLOB_INVALID_POST_RESPONSE": {},
	"INVALID_VENUE_RESPONSE":     {},
}

// NoFillReadFailureCode reports whether a reconciliation error code is a read
// that did not reveal the order.
func NoFillReadFailureCode(code string) bool {
	_, ok := noFillReadFailureCodes[strings.ToUpper(strings.TrimSpace(code))]
	return ok
}

// NoFillHistoryError returns nil only for the history of an order that the
// venue never revealed: exactly one SUBMIT attempt (UNKNOWN, or STARTED and
// abandoned), no CANCEL attempt, every RECONCILE attempt either interrupted or
// a read failure that did not reveal the order, and only pre-acceptance and
// recovery states in its events.
func NoFillHistoryError(attempts []OrderAttempt, eventStatuses []OrderStatus) error {
	for _, status := range eventStatuses {
		if _, allowed := noFillHistoryStatuses[status]; !allowed {
			return fmt.Errorf("order history contains %s", status)
		}
	}
	submits := 0
	for _, attempt := range attempts {
		switch attempt.Kind {
		case OrderAttemptSubmit:
			submits++
			if attempt.Outcome != AttemptOutcomeUnknown && attempt.Outcome != AttemptOutcomeStarted {
				return fmt.Errorf("submit attempt outcome is %s", attempt.Outcome)
			}
			if _, accepted := noFillAcceptedSubmitCodes[strings.ToUpper(strings.TrimSpace(attempt.ErrorCode))]; accepted ||
				(attempt.HTTPStatus >= 200 && attempt.HTTPStatus < 300) {
				return fmt.Errorf("submit attempt ended with a venue success response (%s)", attempt.ErrorCode)
			}
		case OrderAttemptCancel:
			return fmt.Errorf("order has a cancel attempt")
		case OrderAttemptReconcile:
			switch {
			case attempt.Outcome == AttemptOutcomeStarted:
			case attempt.Outcome == AttemptOutcomeSucceeded:
				return fmt.Errorf("a reconciliation read observed the order")
			case !NoFillReadFailureCode(attempt.ErrorCode):
				return fmt.Errorf("reconciliation read %s ended with %q", attempt.ID, attempt.ErrorCode)
			}
		default:
			return fmt.Errorf("unknown attempt kind %q", attempt.Kind)
		}
	}
	if submits != 1 {
		return fmt.Errorf("order has %d submit attempts", submits)
	}
	return nil
}

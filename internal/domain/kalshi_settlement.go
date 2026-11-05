package domain

import "time"

// KalshiSettlement is the exchange's authoritative record that a market
// settled and what it paid the account. Counts and cost are the exchange's
// view of the holding at settlement; Revenue is the cash credited in dollars.
type KalshiSettlement struct {
	Ticker       string
	EventTicker  string
	MarketResult string
	YesCount     Decimal
	YesTotalCost Decimal
	NoCount      Decimal
	NoTotalCost  Decimal
	Revenue      Decimal
	// PayoutPerYes is what one YES contract paid in dollars: 1 for a yes
	// result, 0 for no, anything between for a scalar market. Empty when the
	// exchange omitted it.
	PayoutPerYes Decimal
	// FeeCost is the cumulative trading fee on the market, already charged at
	// fill time. It is evidence only and must never be deducted again.
	FeeCost   Decimal
	SettledAt time.Time
	// Raw is the exact response row, kept for audit.
	Raw []byte
}

// KalshiSettlementResult is what applying a settlement to the ledger did.
type KalshiSettlementResult string

const (
	// KalshiSettlementApplied means positions were closed and cash credited.
	KalshiSettlementApplied KalshiSettlementResult = "APPLIED"
	// KalshiSettlementAlreadyApplied means this account/market was applied before.
	KalshiSettlementAlreadyApplied KalshiSettlementResult = "ALREADY_APPLIED"
	// KalshiSettlementDeferred means shares are still reserved by an open order,
	// so the market is retried on a later sweep.
	KalshiSettlementDeferred KalshiSettlementResult = "DEFERRED"
	// KalshiSettlementNoPosition means the account holds nothing in the market.
	KalshiSettlementNoPosition KalshiSettlementResult = "NO_POSITION"
	// KalshiSettlementManualReview means the exchange record and the ledger
	// disagree, or the result is not binary; nothing was changed.
	KalshiSettlementManualReview KalshiSettlementResult = "MANUAL_REVIEW"
)

// KalshiSettlementOutcome reports the effect of one ApplyKalshiSettlement call.
type KalshiSettlementOutcome struct {
	Result      KalshiSettlementResult
	Reason      string
	Payout      Decimal
	RealizedPnL Decimal
}

// KalshiBalanceCheck is the outcome of comparing the ledger with the exchange.
type KalshiBalanceCheck struct {
	// Skipped is set when open orders or reservations make the two legitimately
	// differ, so no comparison was made.
	Skipped         bool
	LocalBalance    Decimal
	ExchangeBalance Decimal
	Difference      Decimal
	Drifted         bool
}

// KalshiMarketPosition is one row of the exchange's account positions: the
// signed net contracts of one market, YES positive and NO negative.
type KalshiMarketPosition struct {
	Ticker     string
	PositionFP Decimal
	// Raw is the exact response row, kept for audit.
	Raw []byte
}

// KalshiLedgerSnapshot is the unlocked local state read before the exchange
// is asked, so the locked check can prove nothing moved in between.
type KalshiLedgerSnapshot struct {
	AccountVersion int64
	// Quiet is true when the account has no active reservation, no reserved
	// cash, and no non-terminal order.
	Quiet bool
}

// KalshiLedgerObservation is what one sweep read from the exchange, handed to
// the locked running sync.
type KalshiLedgerObservation struct {
	// AccountVersionBefore is execution_accounts.version read before the
	// exchange balance.
	AccountVersionBefore int64
	ExchangeBalance      Decimal
	// PositionCheckEnabled requires a matching position check before cash is
	// synced. Positions is nil when they were not read this sweep.
	PositionCheckEnabled bool
	Positions            []KalshiMarketPosition
	PositionsError       string
	// SettlementActivity is set when the sweep saw any exchange settlement
	// record of a held market; SettlementReadFailed when a read failed.
	SettlementActivity   bool
	SettlementReadFailed bool
	Threshold            Decimal
	ObservedAt           time.Time
}

// Kalshi running-sync outcomes, stored in kalshi_ledger_checks.
const (
	KalshiBalanceInSync              = "IN_SYNC"
	KalshiBalancePendingConfirmation = "PENDING_CONFIRMATION"
	KalshiBalanceSynced              = "SYNCED"
	KalshiBalanceSkipped             = "SKIPPED"

	KalshiPositionsMatched = "MATCHED"
	KalshiPositionsDrifted = "DRIFTED"
	KalshiPositionsSkipped = "SKIPPED"
	KalshiPositionsError   = "ERROR"
)

// Reasons a running cash sync was skipped, in the order they are checked.
const (
	KalshiSyncSkipReservationActive   = "RESERVATION_ACTIVE"
	KalshiSyncSkipOrderInFlight       = "ORDER_IN_FLIGHT"
	KalshiSyncSkipLedgerChanged       = "LEDGER_CHANGED"
	KalshiSyncSkipSettlementPending   = "SETTLEMENT_PENDING"
	KalshiSyncSkipNettingPending      = "NETTING_PENDING"
	KalshiSyncSkipPositionsUnverified = "POSITIONS_UNVERIFIED"
	KalshiSyncSkipPositionMismatch    = "POSITION_MISMATCH"
)

// KalshiLedgerSyncResult reports one running sync.
type KalshiLedgerSyncResult struct {
	BalanceOutcome     string
	SkipReason         string
	PreviousSkipReason string
	ConsecutiveSkips   int64
	TotalSkips         int64
	LocalBalance       Decimal
	ExchangeBalance    Decimal
	Difference         Decimal
	// Drifted is set when the difference exceeded the threshold and was
	// recorded in kalshi_balance_drift.
	Drifted bool
	// SyncEventID is the KALSHI_BALANCE_SYNC event of a SYNCED outcome.
	SyncEventID     string
	FirstObservedAt time.Time
	// PositionsOutcome is empty when the position check is disabled.
	PositionsOutcome           string
	PositionsSkipReason        string
	OpenPositionDrifts         int
	PreviousOpenPositionDrifts int
}

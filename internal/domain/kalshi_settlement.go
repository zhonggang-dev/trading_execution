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

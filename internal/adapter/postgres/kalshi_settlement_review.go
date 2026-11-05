package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// KalshiSettlementReviewStore finishes a settlement the sweep held for manual
// review (migration 0045). It is an operator tool's adapter and is not wired
// into the server. It applies the settlement only when the exchange evidence
// proves the ledger agrees with the exchange: the market's settlement record is
// a single binary result, every exchange fill of the market is booked locally
// and nothing else is, the exchange no longer holds the market, and the cash
// agrees afterwards. Netting the ledger skipped is replayed first.
type KalshiSettlementReviewStore struct {
	db  *sql.DB
	now func() time.Time
}

func NewKalshiSettlementReviewStore(db *sql.DB, now func() time.Time) (*KalshiSettlementReviewStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if now == nil {
		now = time.Now
	}
	return &KalshiSettlementReviewStore{db: db, now: now}, nil
}

// KalshiReviewFill is one exchange fill of the market.
type KalshiReviewFill struct {
	FillID string         `json:"fill_id"`
	Count  domain.Decimal `json:"count"`
}

// KalshiSettlementReviewEvidence is what the exchange reported, read before the
// transaction starts.
type KalshiSettlementReviewEvidence struct {
	Settlements []domain.KalshiSettlement
	Fills       []KalshiReviewFill
	PositionFP  domain.Decimal
	Cash        domain.Decimal
}

// KalshiSettlementReviewRequest selects one held settlement.
type KalshiSettlementReviewRequest struct {
	ExecutionAccountID string
	Ticker             string
	Actor              string
	Reason             string
	Evidence           KalshiSettlementReviewEvidence
	ExpectedCashDiff   domain.Decimal
	CashTolerance      domain.Decimal
	Apply              bool
	Confirmation       string
}

// KalshiSettlementReviewReport is the plan. Fingerprint hashes everything but
// itself and Applied and contains no time, so a dry run and the apply that
// follows it agree only while the ledger and the evidence are unchanged.
type KalshiSettlementReviewReport struct {
	ExecutionAccountID   string                 `json:"execution_account_id"`
	Ticker               string                 `json:"ticker"`
	Actor                string                 `json:"actor"`
	Reason               string                 `json:"reason"`
	AlreadyApplied       bool                   `json:"already_applied"`
	PreviousReason       string                 `json:"previous_reason"`
	MarketResult         string                 `json:"market_result"`
	ExchangeYesCount     domain.Decimal         `json:"exchange_yes_count"`
	ExchangeNoCount      domain.Decimal         `json:"exchange_no_count"`
	ExchangeRevenue      domain.Decimal         `json:"exchange_revenue"`
	ExchangePayoutPerYes domain.Decimal         `json:"exchange_payout_per_yes"`
	ExchangePositionFP   domain.Decimal         `json:"exchange_position_fp"`
	ExchangeCash         domain.Decimal         `json:"exchange_cash"`
	FillIDs              []string               `json:"fill_ids"`
	NettingClosures      []KalshiNettingClosure `json:"netting_closures"`
	Before               KalshiNettingSnapshot  `json:"before"`
	AfterNetting         KalshiNettingSnapshot  `json:"after_netting"`
	After                KalshiNettingSnapshot  `json:"after"`
	Payout               domain.Decimal         `json:"payout"`
	RealizedPnL          domain.Decimal         `json:"realized_pnl"`
	ExpectedCashDiff     domain.Decimal         `json:"expected_cash_diff"`
	CashTolerance        domain.Decimal         `json:"cash_tolerance"`
	CashDifference       domain.Decimal         `json:"cash_difference_after"`
	Fingerprint          string                 `json:"fingerprint"`
	Applied              bool                   `json:"applied"`
}

// maxKalshiReviewCashTolerance is one cent, the rounding of a whole-cents
// exchange balance.
const maxKalshiReviewCashTolerance = domain.Decimal("0.01")

// Run reviews one market in a single SERIALIZABLE transaction under the
// account row lock and rolls it back unless the request applies with the
// fingerprint of an identical plan. A refused plan returns its report with
// the error so the operator sees the numbers behind the refusal.
func (store *KalshiSettlementReviewStore) Run(ctx context.Context, request KalshiSettlementReviewRequest) (KalshiSettlementReviewReport, error) {
	accountID, ticker := strings.TrimSpace(request.ExecutionAccountID), strings.TrimSpace(request.Ticker)
	actor, reason := strings.TrimSpace(request.Actor), strings.TrimSpace(request.Reason)
	if !strings.HasPrefix(accountID, "kalshi:") || ticker == "" || strings.Contains(ticker, ":") {
		return KalshiSettlementReviewReport{}, fmt.Errorf("Kalshi settlement review needs a kalshi: account and a market ticker")
	}
	if actor == "" || reason == "" {
		return KalshiSettlementReviewReport{}, fmt.Errorf("Kalshi settlement review needs an actor and a reason")
	}
	evidence := request.Evidence
	if evidence.PositionFP.IsEmpty() || evidence.Cash.IsEmpty() || evidence.Fills == nil {
		return KalshiSettlementReviewReport{}, fmt.Errorf("Kalshi settlement review needs the exchange settlement, fills, position and cash")
	}
	report := KalshiSettlementReviewReport{
		ExecutionAccountID: accountID, Ticker: ticker, Actor: actor, Reason: reason,
		ExchangePositionFP: evidence.PositionFP, ExchangeCash: evidence.Cash,
		ExpectedCashDiff: domain.Decimal(decimalOrZero(request.ExpectedCashDiff)),
		CashTolerance:    domain.Decimal(decimalOrZero(request.CashTolerance)),
		FillIDs:          []string{}, NettingClosures: []KalshiNettingClosure{},
	}
	if sign, err := report.CashTolerance.Sign(); err != nil || sign < 0 {
		return KalshiSettlementReviewReport{}, fmt.Errorf("Kalshi settlement review cash tolerance must be a non-negative decimal")
	}
	if comparison, err := report.CashTolerance.Compare(maxKalshiReviewCashTolerance); err != nil || comparison > 0 {
		return KalshiSettlementReviewReport{}, fmt.Errorf("Kalshi settlement review cash tolerance must not exceed %s", maxKalshiReviewCashTolerance)
	}
	if _, err := report.ExpectedCashDiff.Sign(); err != nil {
		return KalshiSettlementReviewReport{}, fmt.Errorf("Kalshi settlement review expected cash difference is invalid")
	}
	settlement, err := singleBinaryKalshiSettlement(ticker, evidence.Settlements)
	if err != nil {
		return KalshiSettlementReviewReport{}, err
	}
	report.MarketResult, report.ExchangeYesCount, report.ExchangeNoCount = settlement.MarketResult, settlement.YesCount, settlement.NoCount
	report.ExchangeRevenue, report.ExchangePayoutPerYes = settlement.Revenue, settlement.PayoutPerYes
	if sign, err := evidence.PositionFP.Sign(); err != nil || sign != 0 {
		return report, fmt.Errorf("the exchange still holds %s contracts of %s; a settled market must be flat", evidence.PositionFP, ticker)
	}
	exchangeFills, err := indexKalshiReviewFills(evidence.Fills)
	if err != nil {
		return report, err
	}

	yesToken, noToken := "kalshi:"+ticker+":YES", "kalshi:"+ticker+":NO"
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return report, fmt.Errorf("begin Kalshi settlement review: %w", err)
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, accountID); err != nil {
		return report, err
	}
	var status, previousReason, previousRecord string
	switch err := tx.QueryRowContext(ctx, `
		SELECT status, review_reason, exchange_record::text FROM kalshi_settlements
		WHERE execution_account_id=$1 AND ticker=$2 FOR UPDATE`, accountID, ticker).Scan(&status, &previousReason, &previousRecord); {
	case errors.Is(err, sql.ErrNoRows):
		return report, fmt.Errorf("market %s has no settlement row to review", ticker)
	case err != nil:
		return report, fmt.Errorf("read Kalshi settlement row: %w", err)
	}
	if status == "APPLIED" {
		report.AlreadyApplied = true
		return report, nil
	}
	report.PreviousReason = previousReason
	if report.Before, err = loadKalshiNettingSnapshot(ctx, tx, accountID, yesToken, noToken); err != nil {
		return report, err
	}
	if err := requireKalshiMarketIdle(ctx, tx, accountID, yesToken, noToken); err != nil {
		return report, err
	}
	if report.FillIDs, err = verifyKalshiReviewFills(ctx, tx, accountID, yesToken, noToken, exchangeFills); err != nil {
		return report, err
	}

	at := store.now().UTC()
	fills, err := loadKalshiFillsToNet(ctx, tx, accountID, yesToken, noToken)
	if err != nil {
		return report, err
	}
	for _, fill := range fills {
		order, err := selectOrder(ctx, tx, `WHERE order_id = $1`, fill.OrderID)
		if err != nil {
			return report, err
		}
		netting, err := applyKalshiNetting(ctx, tx, kalshiNettingParams{Order: order, Fill: fill, At: at, AppliedBefore: fill.AppliedAt})
		if err != nil {
			return report, fmt.Errorf("replay netting of fill %s: %w", fill.Key, err)
		}
		if netting.Deferred {
			return report, fmt.Errorf("replay netting of fill %s: the opposite side is reserved by an open order", fill.Key)
		}
		report.NettingClosures = append(report.NettingClosures, netting.Closures...)
	}
	if report.AfterNetting, err = loadKalshiNettingSnapshot(ctx, tx, accountID, yesToken, noToken); err != nil {
		return report, err
	}

	holdings, reserved, err := loadKalshiHoldings(ctx, tx, accountID, settlement.MarketResult, yesToken, noToken)
	if err != nil {
		return report, err
	}
	if reserved {
		return report, fmt.Errorf("shares of %s are reserved by an open order", ticker)
	}
	if len(holdings) == 0 {
		return report, fmt.Errorf("the ledger holds nothing in %s; there is no settlement to apply", ticker)
	}
	if mismatch, err := validateKalshiSettlementAgainstLedger(ctx, tx, settlement, holdings, yesToken); err != nil {
		return report, err
	} else if mismatch != "" {
		return report, fmt.Errorf("the exchange settlement still disagrees with the ledger: %s", mismatch)
	}
	if report.RealizedPnL, err = applyValidatedKalshiSettlement(ctx, tx, accountID, ticker, settlement, holdings, at); err != nil {
		return report, err
	}
	report.Payout = settlement.Revenue
	if err := markKalshiSettlementReviewed(ctx, tx, accountID, ticker, settlement, holdings, report.RealizedPnL, at); err != nil {
		return report, err
	}
	if report.After, err = loadKalshiNettingSnapshot(ctx, tx, accountID, yesToken, noToken); err != nil {
		return report, err
	}
	if report.CashDifference, err = numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`,
		evidence.Cash.String(), report.After.TotalBalance.String()); err != nil {
		return report, err
	}
	if report.Fingerprint, err = kalshiSettlementReviewFingerprint(report); err != nil {
		return report, err
	}
	var cashAgrees bool
	if err := tx.QueryRowContext(ctx, `SELECT ABS($1::numeric-$2::numeric) <= $3::numeric`,
		report.CashDifference.String(), report.ExpectedCashDiff.String(), report.CashTolerance.String()).Scan(&cashAgrees); err != nil {
		return report, err
	}
	if !cashAgrees {
		return report, fmt.Errorf("exchange cash %s minus local cash %s is %s after the review, expected %s within %s",
			evidence.Cash, report.After.TotalBalance, report.CashDifference, report.ExpectedCashDiff, report.CashTolerance)
	}
	auditEvidence, err := json.Marshal(map[string]any{
		"fill_ids": report.FillIDs, "exchange_position_fp": evidence.PositionFP.String(),
		"exchange_cash": evidence.Cash.String(), "netting_closures": len(report.NettingClosures),
		"payout": report.Payout.String(), "realized_pnl": report.RealizedPnL.String(),
	})
	if err != nil {
		return report, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO kalshi_settlement_reviews (
			execution_account_id, ticker, previous_reason, previous_exchange_record, exchange_record, evidence,
			cash_difference_after, expected_cash_diff, cash_tolerance, actor, reason, fingerprint, applied_at
		) VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7::numeric,$8::numeric,$9::numeric,$10,$11,$12,$13)`,
		accountID, ticker, previousReason, previousRecord, string(settlement.Raw), string(auditEvidence),
		report.CashDifference.String(), report.ExpectedCashDiff.String(), report.CashTolerance.String(),
		actor, reason, report.Fingerprint, at); err != nil {
		return report, fmt.Errorf("record Kalshi settlement review: %w", err)
	}
	if !request.Apply {
		if err := tx.Rollback(); err != nil {
			return report, fmt.Errorf("roll back Kalshi settlement review dry run: %w", err)
		}
		return report, nil
	}
	if strings.TrimSpace(request.Confirmation) != report.Fingerprint {
		return report, fmt.Errorf("--confirmation does not match this plan's fingerprint %s", report.Fingerprint)
	}
	if err := tx.Commit(); err != nil {
		return report, fmt.Errorf("commit Kalshi settlement review: %w", err)
	}
	report.Applied = true
	return report, nil
}

// singleBinaryKalshiSettlement requires exactly one settlement record of the
// market with a yes/no result. Scalar and void results are not supported.
func singleBinaryKalshiSettlement(ticker string, records []domain.KalshiSettlement) (domain.KalshiSettlement, error) {
	if len(records) != 1 {
		return domain.KalshiSettlement{}, fmt.Errorf("the exchange returned %d settlement records for %s, want exactly one", len(records), ticker)
	}
	record := records[0]
	if strings.TrimSpace(record.Ticker) != ticker || len(record.Raw) == 0 {
		return domain.KalshiSettlement{}, fmt.Errorf("the exchange settlement record does not belong to %s", ticker)
	}
	if record.MarketResult != "yes" && record.MarketResult != "no" {
		return domain.KalshiSettlement{}, fmt.Errorf("market result %q is not a binary yes/no result; non-binary settlements are not supported by this tool", record.MarketResult)
	}
	return record, nil
}

func indexKalshiReviewFills(fills []KalshiReviewFill) (map[string]domain.Decimal, error) {
	index := make(map[string]domain.Decimal, len(fills))
	for _, fill := range fills {
		id := strings.TrimSpace(fill.FillID)
		if id == "" || fill.Count.IsEmpty() {
			return nil, fmt.Errorf("an exchange fill has no id or count")
		}
		if sign, err := fill.Count.Sign(); err != nil || sign <= 0 {
			return nil, fmt.Errorf("exchange fill %s has an invalid count", id)
		}
		if _, duplicate := index[id]; duplicate {
			return nil, fmt.Errorf("the exchange listed fill %s twice", id)
		}
		index[id] = fill.Count
	}
	return index, nil
}

// requireKalshiMarketIdle refuses a market with reserved shares, an active
// reservation, or a non-terminal order.
func requireKalshiMarketIdle(ctx context.Context, tx *sql.Tx, accountID, yesToken, noToken string) error {
	for _, token := range []string{yesToken, noToken} {
		if err := requireKalshiSideUnreserved(ctx, tx, accountID, token); err != nil {
			return err
		}
	}
	var reservations, orders bool
	if err := tx.QueryRowContext(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM asset_reservations
			        WHERE execution_account_id=$1 AND token_id IN ($2,$3)
			          AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')),
			EXISTS (SELECT 1 FROM execution_orders
			        WHERE execution_account_id=$1 AND token_id IN ($2,$3)
			          AND status NOT IN ('CANCELLED','REJECTED','FILLED'))`,
		accountID, yesToken, noToken).Scan(&reservations, &orders); err != nil {
		return fmt.Errorf("inspect Kalshi market activity: %w", err)
	}
	if reservations {
		return fmt.Errorf("the market has an active reservation")
	}
	if orders {
		return fmt.Errorf("the market has a non-terminal order")
	}
	return nil
}

// verifyKalshiReviewFills requires the exchange's fills of the market and the
// ledger's booked fills to be the same set with the same counts. Failed local
// fills are ignored; any other unbooked local fill is refused.
func verifyKalshiReviewFills(
	ctx context.Context, tx *sql.Tx, accountID, yesToken, noToken string, exchange map[string]domain.Decimal,
) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT venue_fill_id, shares::text, status, applied_at IS NOT NULL
		FROM execution_fills
		WHERE execution_account_id=$1 AND token_id IN ($2,$3) AND status <> 'FAILED'
		ORDER BY venue_fill_id`, accountID, yesToken, noToken)
	if err != nil {
		return nil, fmt.Errorf("list local Kalshi fills: %w", err)
	}
	local := make(map[string]domain.Decimal)
	var unbooked []string
	for rows.Next() {
		var id, status string
		var shares domain.Decimal
		var applied bool
		if err := rows.Scan(&id, &shares, &status, &applied); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if status != "CONFIRMED" || !applied {
			unbooked = append(unbooked, id)
			continue
		}
		if _, duplicate := local[id]; duplicate {
			_ = rows.Close()
			return nil, fmt.Errorf("the ledger booked fill %s twice", id)
		}
		local[id] = shares
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(unbooked) > 0 {
		return nil, fmt.Errorf("the ledger has unbooked fills of this market: %s; run order recovery first", strings.Join(unbooked, ","))
	}
	var missing, extra, different []string
	for id, count := range exchange {
		shares, ok := local[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		if comparison, err := shares.Compare(count); err != nil || comparison != 0 {
			different = append(different, fmt.Sprintf("%s (local %s, exchange %s)", id, shares, count))
		}
	}
	for id := range local {
		if _, ok := exchange[id]; !ok {
			extra = append(extra, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	sort.Strings(different)
	if len(missing)+len(extra)+len(different) > 0 {
		return nil, fmt.Errorf("exchange and ledger fills of this market differ: missing locally [%s], not on the exchange [%s], different counts [%s]",
			strings.Join(missing, ","), strings.Join(extra, ","), strings.Join(different, ","))
	}
	ids := make([]string, 0, len(local))
	for id := range local {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// markKalshiSettlementReviewed turns the MANUAL_REVIEW row into the APPLIED row
// ApplyKalshiSettlement would have written. The previous reason and record are
// kept in kalshi_settlement_reviews.
func markKalshiSettlementReviewed(
	ctx context.Context, tx *sql.Tx, accountID, ticker string, settlement domain.KalshiSettlement,
	holdings []kalshiHolding, realizedPnL domain.Decimal, at time.Time,
) error {
	localYes, localNo, cost := "0", "0", domain.Decimal("0")
	var err error
	for _, holding := range holdings {
		if strings.HasSuffix(holding.position.TokenID, ":YES") {
			localYes = holding.position.TotalShares.String()
		} else {
			localNo = holding.position.TotalShares.String()
		}
		if cost, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, cost.String(), holding.position.CostBasis.String()); err != nil {
			return err
		}
	}
	var settledAt any
	if !settlement.SettledAt.IsZero() {
		settledAt = settlement.SettledAt
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE kalshi_settlements SET
			status='APPLIED', market_result=$3,
			exchange_yes_count=$4::numeric, exchange_no_count=$5::numeric, exchange_revenue=$6::numeric,
			exchange_payout_per_yes=NULLIF($7,'')::numeric, exchange_fee_cost=NULLIF($8,'')::numeric,
			local_yes_shares=$9::numeric, local_no_shares=$10::numeric, local_cost_basis=$11::numeric,
			realized_pnl=$12::numeric, review_reason='', settled_at=$13, applied_at=$14, exchange_record=$15::jsonb
		WHERE execution_account_id=$1 AND ticker=$2 AND status='MANUAL_REVIEW'`,
		accountID, ticker, settlement.MarketResult, settlement.YesCount.String(), settlement.NoCount.String(),
		settlement.Revenue.String(), settlement.PayoutPerYes.String(), settlement.FeeCost.String(),
		localYes, localNo, cost.String(), realizedPnL.String(), settledAt, at, string(settlement.Raw))
	if err != nil {
		return fmt.Errorf("apply reviewed Kalshi settlement row: %w", err)
	}
	if !oneRow(result) {
		return fmt.Errorf("Kalshi settlement row %s changed during the review", ticker)
	}
	return nil
}

func kalshiSettlementReviewFingerprint(report KalshiSettlementReviewReport) (string, error) {
	report.Fingerprint, report.Applied = "", false
	body, err := json.Marshal(report)
	if err != nil {
		return "", fmt.Errorf("encode Kalshi settlement review plan: %w", err)
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

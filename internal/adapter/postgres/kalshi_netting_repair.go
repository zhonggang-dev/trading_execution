package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// KalshiNettingRepairStore replays the netting of already booked BUY fills of one
// market. It is an operator tool's adapter and is not wired into the server. A
// fill is replayed only if it has no netting account event, so running it again
// after the live ledger or an earlier repair netted a fill pays nothing twice.
type KalshiNettingRepairStore struct {
	db  *sql.DB
	now func() time.Time
}

func NewKalshiNettingRepairStore(db *sql.DB, now func() time.Time) (*KalshiNettingRepairStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if now == nil {
		now = time.Now
	}
	return &KalshiNettingRepairStore{db: db, now: now}, nil
}

// KalshiNettingEvidence is what the exchange reports. PositionFP is the signed
// net contracts of the market: YES positive, NO negative.
type KalshiNettingEvidence struct {
	PositionFP domain.Decimal `json:"position_fp"`
	Cash       domain.Decimal `json:"cash"`
}

// KalshiNettingRepairRequest selects one market. ExpectedCashDiff is the known
// exchange-minus-local cash left over by something else, and CashTolerance widens
// the comparison when the exchange reports cash in whole cents.
type KalshiNettingRepairRequest struct {
	ExecutionAccountID string
	Ticker             string
	Evidence           KalshiNettingEvidence
	ExpectedCashDiff   domain.Decimal
	CashTolerance      domain.Decimal
	Apply              bool
	Confirmation       string
}

// KalshiNettingSide is one token's ledger state.
type KalshiNettingSide struct {
	Shares      domain.Decimal `json:"shares"`
	CostBasis   domain.Decimal `json:"cost_basis"`
	RealizedPnL domain.Decimal `json:"realized_pnl"`
}

// KalshiNettingSnapshot is the market and account state on one side of the replay.
type KalshiNettingSnapshot struct {
	Yes              KalshiNettingSide `json:"yes"`
	No               KalshiNettingSide `json:"no"`
	TotalBalance     domain.Decimal    `json:"total_balance"`
	AvailableBalance domain.Decimal    `json:"available_balance"`
}

// KalshiNettingRepairReport is the plan. Fingerprint hashes everything except
// itself and Applied, and contains no time, so a dry run and the apply that
// follows it agree only while the ledger and the exchange evidence are unchanged.
type KalshiNettingRepairReport struct {
	ExecutionAccountID string                 `json:"execution_account_id"`
	Ticker             string                 `json:"ticker"`
	Official           KalshiNettingEvidence  `json:"official"`
	ExpectedCashDiff   domain.Decimal         `json:"expected_cash_diff"`
	CashTolerance      domain.Decimal         `json:"cash_tolerance"`
	CashDifference     domain.Decimal         `json:"cash_difference_after"`
	Before             KalshiNettingSnapshot  `json:"before"`
	After              KalshiNettingSnapshot  `json:"after"`
	Closures           []KalshiNettingClosure `json:"closures"`
	Fingerprint        string                 `json:"fingerprint"`
	Applied            bool                   `json:"applied"`
}

// Run replays the market in one transaction and rolls it back unless the request
// applies with the fingerprint of an identical plan. The report is returned with
// a failed comparison too, so the operator can see the numbers behind the refusal.
func (store *KalshiNettingRepairStore) Run(ctx context.Context, request KalshiNettingRepairRequest) (KalshiNettingRepairReport, error) {
	accountID, ticker := strings.TrimSpace(request.ExecutionAccountID), strings.TrimSpace(request.Ticker)
	if !strings.HasPrefix(accountID, "kalshi:") || ticker == "" || strings.Contains(ticker, ":") {
		return KalshiNettingRepairReport{}, fmt.Errorf("Kalshi netting repair needs a kalshi: account and a market ticker")
	}
	if request.Evidence.PositionFP.IsEmpty() || request.Evidence.Cash.IsEmpty() {
		return KalshiNettingRepairReport{}, fmt.Errorf("Kalshi netting repair needs the exchange position and cash")
	}
	report := KalshiNettingRepairReport{
		ExecutionAccountID: accountID, Ticker: ticker, Official: request.Evidence,
		ExpectedCashDiff: domain.Decimal(decimalOrZero(request.ExpectedCashDiff)),
		CashTolerance:    domain.Decimal(decimalOrZero(request.CashTolerance)),
		Closures:         []KalshiNettingClosure{},
	}
	if sign, err := report.CashTolerance.Sign(); err != nil || sign < 0 {
		return KalshiNettingRepairReport{}, fmt.Errorf("Kalshi netting cash tolerance must be a non-negative decimal")
	}
	yesToken, noToken := "kalshi:"+ticker+":YES", "kalshi:"+ticker+":NO"
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return KalshiNettingRepairReport{}, fmt.Errorf("begin Kalshi netting repair: %w", err)
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, accountID); err != nil {
		return KalshiNettingRepairReport{}, err
	}
	var settlementStatus string
	switch err := tx.QueryRowContext(ctx, `
		SELECT status FROM kalshi_settlements WHERE execution_account_id=$1 AND ticker=$2`,
		accountID, ticker).Scan(&settlementStatus); {
	case err == nil:
		return KalshiNettingRepairReport{}, fmt.Errorf("market %s already has a %s settlement row", ticker, settlementStatus)
	case !errors.Is(err, sql.ErrNoRows):
		return KalshiNettingRepairReport{}, fmt.Errorf("read Kalshi settlement row: %w", err)
	}
	if report.Before, err = loadKalshiNettingSnapshot(ctx, tx, accountID, yesToken, noToken); err != nil {
		return KalshiNettingRepairReport{}, err
	}
	// A repair can close an old lot, which a live SELL order may be targeting.
	// Live accounting only ever closes a lot opened in the same transaction.
	for _, token := range []string{yesToken, noToken} {
		if err := requireKalshiSideUnreserved(ctx, tx, accountID, token); err != nil {
			return report, err
		}
	}
	fills, err := loadKalshiFillsToNet(ctx, tx, accountID, yesToken, noToken)
	if err != nil {
		return KalshiNettingRepairReport{}, err
	}
	at := store.now().UTC()
	for _, fill := range fills {
		order, err := selectOrder(ctx, tx, `WHERE order_id = $1`, fill.OrderID)
		if err != nil {
			return report, err
		}
		netting, err := applyKalshiNetting(ctx, tx, kalshiNettingParams{
			Order: order, Fill: fill, At: at, AppliedBefore: fill.AppliedAt,
		})
		if err != nil {
			return report, fmt.Errorf("replay fill %s: %w", fill.Key, err)
		}
		if netting.Deferred {
			return report, fmt.Errorf("replay fill %s: the opposite side is reserved by an open order", fill.Key)
		}
		report.Closures = append(report.Closures, netting.Closures...)
	}
	if report.After, err = loadKalshiNettingSnapshot(ctx, tx, accountID, yesToken, noToken); err != nil {
		return report, err
	}
	if report.CashDifference, err = numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`,
		request.Evidence.Cash.String(), report.After.TotalBalance.String()); err != nil {
		return report, err
	}
	report.Fingerprint, err = kalshiNettingFingerprint(report)
	if err != nil {
		return report, err
	}
	if err := verifyKalshiNettingRepair(ctx, tx, report, accountID, yesToken, noToken); err != nil {
		return report, err
	}
	if !request.Apply {
		if err := tx.Rollback(); err != nil {
			return report, fmt.Errorf("roll back Kalshi netting dry run: %w", err)
		}
		return report, nil
	}
	if request.Confirmation != report.Fingerprint {
		return report, fmt.Errorf("--confirmation does not match this plan's fingerprint %s", report.Fingerprint)
	}
	if err := tx.Commit(); err != nil {
		return report, fmt.Errorf("commit Kalshi netting repair: %w", err)
	}
	report.Applied = true
	return report, nil
}

func loadKalshiNettingSnapshot(ctx context.Context, tx *sql.Tx, accountID, yesToken, noToken string) (KalshiNettingSnapshot, error) {
	var snapshot KalshiNettingSnapshot
	var err error
	if snapshot.Yes, err = loadKalshiNettingSide(ctx, tx, accountID, yesToken); err != nil {
		return KalshiNettingSnapshot{}, err
	}
	if snapshot.No, err = loadKalshiNettingSide(ctx, tx, accountID, noToken); err != nil {
		return KalshiNettingSnapshot{}, err
	}
	account, err := selectAccountSnapshot(ctx, tx, accountID)
	if err != nil {
		return KalshiNettingSnapshot{}, err
	}
	snapshot.TotalBalance, snapshot.AvailableBalance = account.Total, account.Available
	return snapshot, nil
}

func loadKalshiNettingSide(ctx context.Context, tx *sql.Tx, accountID, tokenID string) (KalshiNettingSide, error) {
	position, err := selectPosition(ctx, tx, accountID, tokenID, false)
	if errors.Is(err, port.ErrPositionNotFound) {
		return KalshiNettingSide{Shares: "0", CostBasis: "0", RealizedPnL: "0"}, nil
	}
	if err != nil {
		return KalshiNettingSide{}, err
	}
	return KalshiNettingSide{Shares: position.TotalShares, CostBasis: position.CostBasis, RealizedPnL: position.RealizedPnL}, nil
}

func requireKalshiSideUnreserved(ctx context.Context, tx *sql.Tx, accountID, tokenID string) error {
	position, err := selectPosition(ctx, tx, accountID, tokenID, true)
	if errors.Is(err, port.ErrPositionNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if sign, err := position.ReservedShares.Sign(); err != nil {
		return err
	} else if sign > 0 {
		return fmt.Errorf("position %s has %s shares reserved by an open order", tokenID, position.ReservedShares)
	}
	return nil
}

// loadKalshiFillsToNet returns the market's booked BUY fills in booking order,
// without those that already have a netting account event.
func loadKalshiFillsToNet(ctx context.Context, tx *sql.Tx, accountID, yesToken, noToken string) ([]domain.Fill, error) {
	rows, err := tx.QueryContext(ctx, fillSelect+`
		WHERE execution_account_id=$1 AND token_id IN ($2,$3) AND side='BUY'
		  AND status='CONFIRMED' AND applied_at IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1 FROM execution_account_events AS event
			WHERE event.account_event_id='kalshi-netting:'||execution_fills.fill_key)
		ORDER BY applied_at, fill_key`, accountID, yesToken, noToken)
	if err != nil {
		return nil, fmt.Errorf("list Kalshi fills to net: %w", err)
	}
	defer rows.Close()
	fills := make([]domain.Fill, 0)
	for rows.Next() {
		fill, err := scanFill(rows)
		if err != nil {
			return nil, err
		}
		fills = append(fills, fill)
	}
	return fills, rows.Err()
}

func verifyKalshiNettingRepair(
	ctx context.Context, tx *sql.Tx, report KalshiNettingRepairReport, accountID, yesToken, noToken string,
) error {
	yes, no := report.After.Yes.Shares, report.After.No.Shares
	yesSign, err := yes.Sign()
	if err != nil {
		return err
	}
	noSign, err := no.Sign()
	if err != nil {
		return err
	}
	if yesSign > 0 && noSign > 0 {
		return fmt.Errorf("the ledger still holds both sides after netting: %s YES and %s NO", yes, no)
	}
	net, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, yes.String(), no.String())
	if err != nil {
		return err
	}
	if equal, err := numericEqual(ctx, tx, net.String(), report.Official.PositionFP.String()); err != nil {
		return err
	} else if !equal {
		return fmt.Errorf("local net position %s does not match the exchange's %s", net, report.Official.PositionFP)
	}
	var cashAgrees bool
	if err := tx.QueryRowContext(ctx, `SELECT ABS($1::numeric-$2::numeric) <= $3::numeric`,
		report.CashDifference.String(), report.ExpectedCashDiff.String(), report.CashTolerance.String()).Scan(&cashAgrees); err != nil {
		return err
	}
	if !cashAgrees {
		return fmt.Errorf("exchange cash %s minus local cash %s is %s, expected %s within %s",
			report.Official.Cash, report.After.TotalBalance, report.CashDifference, report.ExpectedCashDiff, report.CashTolerance)
	}
	for _, token := range []string{yesToken, noToken} {
		if err := verifyKalshiNettingLots(ctx, tx, accountID, token); err != nil {
			return err
		}
	}
	return nil
}

// verifyKalshiNettingLots applies the check settlement will apply later.
func verifyKalshiNettingLots(ctx context.Context, tx *sql.Tx, accountID, tokenID string) error {
	position, err := selectPosition(ctx, tx, accountID, tokenID, true)
	if errors.Is(err, port.ErrPositionNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	lots, err := loadKalshiLots(ctx, tx, accountID, tokenID)
	if err != nil {
		return err
	}
	if sign, err := position.TotalShares.Sign(); err != nil {
		return err
	} else if sign == 0 {
		if len(lots) != 0 || !position.CostBasis.Equal("0") {
			return fmt.Errorf("position %s has no shares but still has open lots or a cost basis", tokenID)
		}
		return nil
	}
	return validateKalshiLots(ctx, tx, kalshiHolding{position: position, lots: lots})
}

func kalshiNettingFingerprint(report KalshiNettingRepairReport) (string, error) {
	report.Fingerprint, report.Applied = "", false
	body, err := json.Marshal(report)
	if err != nil {
		return "", fmt.Errorf("encode Kalshi netting plan: %w", err)
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

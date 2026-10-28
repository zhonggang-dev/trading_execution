package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// KalshiSettlementStore applies Kalshi's own settlement records to the local
// ledger. Kalshi pays winners into cash by itself, so unlike a Polymarket
// redemption there is nothing to submit: the exchange record is the single
// piece of evidence, and it is only applied when it agrees exactly with what the
// ledger holds.
type KalshiSettlementStore struct {
	db         *sql.DB
	accountIDs map[string]struct{}
}

var _ port.KalshiSettlementStore = (*KalshiSettlementStore)(nil)

func NewKalshiSettlementStore(database *sql.DB, executionAccountIDs []string) (*KalshiSettlementStore, error) {
	if database == nil {
		return nil, fmt.Errorf("Kalshi settlement store requires PostgreSQL")
	}
	managed := make(map[string]struct{}, len(executionAccountIDs))
	for _, raw := range executionAccountIDs {
		accountID := strings.TrimSpace(raw)
		if !strings.HasPrefix(accountID, "kalshi:") {
			return nil, fmt.Errorf("Kalshi settlement account %q is not a Kalshi execution account", raw)
		}
		managed[accountID] = struct{}{}
	}
	if len(managed) == 0 {
		return nil, fmt.Errorf("Kalshi settlement store requires at least one managed account")
	}
	return &KalshiSettlementStore{db: database, accountIDs: managed}, nil
}

func (store *KalshiSettlementStore) requireManaged(accountID string) error {
	if _, ok := store.accountIDs[accountID]; !ok {
		return fmt.Errorf("Kalshi settlement account %q is outside the managed account scope", accountID)
	}
	return nil
}

func (store *KalshiSettlementStore) OpenKalshiSettlementTickers(ctx context.Context, executionAccountID string) ([]string, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if err := store.requireManaged(executionAccountID); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT DISTINCT SUBSTRING(position.token_id FROM '^kalshi:(.+):(?:YES|NO)$') AS ticker
		FROM execution_positions position
		WHERE position.execution_account_id=$1
		  AND position.lifecycle_status='OPEN' AND position.total_shares > 0
		  AND position.token_id ~ '^kalshi:.+:(YES|NO)$'
		  AND NOT EXISTS (
			SELECT 1 FROM kalshi_settlements settlement
			WHERE settlement.execution_account_id=position.execution_account_id
			  AND position.token_id IN ('kalshi:'||settlement.ticker||':YES','kalshi:'||settlement.ticker||':NO'))
		ORDER BY ticker`, executionAccountID)
	if err != nil {
		return nil, fmt.Errorf("list open Kalshi settlement markets: %w", err)
	}
	defer rows.Close()
	tickers := make([]string, 0)
	for rows.Next() {
		var ticker string
		if err := rows.Scan(&ticker); err != nil {
			return nil, err
		}
		tickers = append(tickers, ticker)
	}
	return tickers, rows.Err()
}

type kalshiHolding struct {
	position domain.Position
	lots     []kalshiLot
	winner   bool
}

type kalshiLot struct {
	id           string
	shares, cost domain.Decimal
}

func (store *KalshiSettlementStore) ApplyKalshiSettlement(
	ctx context.Context, executionAccountID string, settlement domain.KalshiSettlement, appliedAt time.Time,
) (domain.KalshiSettlementOutcome, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if err := store.requireManaged(executionAccountID); err != nil {
		return domain.KalshiSettlementOutcome{}, err
	}
	ticker := strings.TrimSpace(settlement.Ticker)
	if ticker == "" || strings.Contains(ticker, ":") || appliedAt.IsZero() || len(settlement.Raw) == 0 {
		return domain.KalshiSettlementOutcome{}, fmt.Errorf("Kalshi settlement needs a ticker, an application time, and the exchange record")
	}
	appliedAt = appliedAt.UTC()
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.KalshiSettlementOutcome{}, fmt.Errorf("begin Kalshi settlement: %w", err)
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, executionAccountID); err != nil {
		return domain.KalshiSettlementOutcome{}, err
	}
	var existingStatus, existingReason string
	switch err := tx.QueryRowContext(ctx, `
		SELECT status, review_reason FROM kalshi_settlements
		WHERE execution_account_id=$1 AND ticker=$2`, executionAccountID, ticker).Scan(&existingStatus, &existingReason); {
	case err == nil:
		if existingStatus == "APPLIED" {
			return domain.KalshiSettlementOutcome{Result: domain.KalshiSettlementAlreadyApplied}, tx.Commit()
		}
		return domain.KalshiSettlementOutcome{Result: domain.KalshiSettlementManualReview, Reason: existingReason}, tx.Commit()
	case err != sql.ErrNoRows:
		return domain.KalshiSettlementOutcome{}, fmt.Errorf("read Kalshi settlement row: %w", err)
	}

	yesToken, noToken := "kalshi:"+ticker+":YES", "kalshi:"+ticker+":NO"
	holdings, reserved, err := loadKalshiHoldings(ctx, tx, executionAccountID, settlement.MarketResult, yesToken, noToken)
	if err != nil {
		return domain.KalshiSettlementOutcome{}, err
	}
	if len(holdings) == 0 {
		return domain.KalshiSettlementOutcome{Result: domain.KalshiSettlementNoPosition}, tx.Commit()
	}
	if reserved {
		return domain.KalshiSettlementOutcome{Result: domain.KalshiSettlementDeferred,
			Reason: "shares are reserved by an open order"}, tx.Commit()
	}
	review := func(reason string) (domain.KalshiSettlementOutcome, error) {
		if err := insertKalshiSettlementRow(ctx, tx, executionAccountID, ticker, settlement, holdings, "MANUAL_REVIEW", reason, "", appliedAt); err != nil {
			return domain.KalshiSettlementOutcome{}, err
		}
		if err := tx.Commit(); err != nil {
			return domain.KalshiSettlementOutcome{}, fmt.Errorf("commit Kalshi settlement review: %w", err)
		}
		return domain.KalshiSettlementOutcome{Result: domain.KalshiSettlementManualReview, Reason: reason}, nil
	}

	if settlement.MarketResult != "yes" && settlement.MarketResult != "no" {
		return review(fmt.Sprintf("market result %q is not a binary yes/no result", settlement.MarketResult))
	}
	if !settlement.PayoutPerYes.IsEmpty() {
		want := "0"
		if settlement.MarketResult == "yes" {
			want = "1"
		}
		if equal, err := numericEqual(ctx, tx, settlement.PayoutPerYes.String(), want); err != nil {
			return domain.KalshiSettlementOutcome{}, err
		} else if !equal {
			return review(fmt.Sprintf("payout per yes contract %s contradicts market result %s", settlement.PayoutPerYes, settlement.MarketResult))
		}
	}
	for _, field := range []domain.Decimal{settlement.YesCount, settlement.NoCount, settlement.Revenue} {
		if field.IsEmpty() {
			return review("exchange settlement record is missing a count or revenue")
		}
	}
	localYes, localNo := kalshiLocalShares(holdings, yesToken)
	if equal, err := numericEqual(ctx, tx, settlement.YesCount.String(), localYes.String()); err != nil {
		return domain.KalshiSettlementOutcome{}, err
	} else if !equal {
		return review(fmt.Sprintf("exchange held %s YES but the ledger holds %s", settlement.YesCount, localYes))
	}
	if equal, err := numericEqual(ctx, tx, settlement.NoCount.String(), localNo.String()); err != nil {
		return domain.KalshiSettlementOutcome{}, err
	} else if !equal {
		return review(fmt.Sprintf("exchange held %s NO but the ledger holds %s", settlement.NoCount, localNo))
	}
	winning := localNo
	if settlement.MarketResult == "yes" {
		winning = localYes
	}
	if equal, err := numericEqual(ctx, tx, settlement.Revenue.String(), winning.String()); err != nil {
		return domain.KalshiSettlementOutcome{}, err
	} else if !equal {
		return review(fmt.Sprintf("exchange paid %s but %s winning shares are held", settlement.Revenue, winning))
	}
	for _, holding := range holdings {
		if err := validateKalshiLots(ctx, tx, holding); err != nil {
			return review(err.Error())
		}
	}

	realizedTotal := domain.Decimal("0")
	for _, holding := range holdings {
		realized, err := closeKalshiHolding(ctx, tx, ticker, holding, settlement, appliedAt)
		if err != nil {
			return domain.KalshiSettlementOutcome{}, err
		}
		realizedTotal, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, realizedTotal.String(), realized.String())
		if err != nil {
			return domain.KalshiSettlementOutcome{}, err
		}
	}
	if err := creditKalshiSettlement(ctx, tx, executionAccountID, ticker, settlement.Revenue, appliedAt); err != nil {
		return domain.KalshiSettlementOutcome{}, err
	}
	if err := insertKalshiSettlementRow(ctx, tx, executionAccountID, ticker, settlement, holdings, "APPLIED", "", realizedTotal.String(), appliedAt); err != nil {
		return domain.KalshiSettlementOutcome{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.KalshiSettlementOutcome{}, fmt.Errorf("commit Kalshi settlement: %w", err)
	}
	return domain.KalshiSettlementOutcome{Result: domain.KalshiSettlementApplied, Payout: settlement.Revenue, RealizedPnL: realizedTotal}, nil
}

// loadKalshiHoldings locks the YES and NO positions of one market that still
// hold shares. reserved reports whether any of them is held by an open order.
func loadKalshiHoldings(
	ctx context.Context, tx *sql.Tx, accountID, marketResult, yesToken, noToken string,
) ([]kalshiHolding, bool, error) {
	rows, err := tx.QueryContext(ctx, positionSelect+`
		WHERE execution_account_id=$1 AND token_id IN ($2,$3) AND total_shares > 0
		ORDER BY token_id FOR UPDATE`, accountID, yesToken, noToken)
	if err != nil {
		return nil, false, fmt.Errorf("lock Kalshi positions: %w", err)
	}
	positions := make([]domain.Position, 0, 2)
	for rows.Next() {
		position, scanErr := scanPosition(rows)
		if scanErr != nil {
			rows.Close()
			return nil, false, scanErr
		}
		positions = append(positions, position)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	holdings := make([]kalshiHolding, 0, len(positions))
	reserved := false
	for _, position := range positions {
		if position.LifecycleStatus != domain.PositionLifecycleOpen {
			return nil, false, fmt.Errorf("Kalshi position %s is %s, not OPEN", position.TokenID, position.LifecycleStatus)
		}
		if sign, err := position.ReservedShares.Sign(); err != nil || sign != 0 {
			reserved = true
		}
		winner := (position.TokenID == yesToken && marketResult == "yes") || (position.TokenID == noToken && marketResult == "no")
		lots, err := loadKalshiLots(ctx, tx, accountID, position.TokenID)
		if err != nil {
			return nil, false, err
		}
		holdings = append(holdings, kalshiHolding{position: position, lots: lots, winner: winner})
	}
	return holdings, reserved, nil
}

func loadKalshiLots(ctx context.Context, tx *sql.Tx, accountID, tokenID string) ([]kalshiLot, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT lot_id, remaining_shares::text, remaining_cost::text
		FROM position_lots
		WHERE execution_account_id=$1 AND token_id=$2 AND status='OPEN'
		ORDER BY opened_at, lot_id FOR UPDATE`, accountID, tokenID)
	if err != nil {
		return nil, fmt.Errorf("lock Kalshi lots: %w", err)
	}
	defer rows.Close()
	lots := make([]kalshiLot, 0)
	for rows.Next() {
		var lot kalshiLot
		if err := rows.Scan(&lot.id, &lot.shares, &lot.cost); err != nil {
			return nil, err
		}
		lots = append(lots, lot)
	}
	return lots, rows.Err()
}

func kalshiLocalShares(holdings []kalshiHolding, yesToken string) (yes, no domain.Decimal) {
	yes, no = "0", "0"
	for _, holding := range holdings {
		if holding.position.TokenID == yesToken {
			yes = holding.position.TotalShares
		} else {
			no = holding.position.TotalShares
		}
	}
	return yes, no
}

// validateKalshiLots loads the position's open lots and checks that they add up
// to the position, so the payout is allocated to exactly the shares held.
func validateKalshiLots(ctx context.Context, tx *sql.Tx, holding kalshiHolding) error {
	if len(holding.lots) == 0 {
		return fmt.Errorf("position %s has shares but no open lots", holding.position.TokenID)
	}
	shares, cost := domain.Decimal("0"), domain.Decimal("0")
	var err error
	for _, lot := range holding.lots {
		if shares, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, shares.String(), lot.shares.String()); err != nil {
			return err
		}
		if cost, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, cost.String(), lot.cost.String()); err != nil {
			return err
		}
	}
	sharesEqual, err := numericEqual(ctx, tx, shares.String(), holding.position.TotalShares.String())
	if err != nil {
		return err
	}
	costEqual, err := numericEqual(ctx, tx, cost.String(), holding.position.CostBasis.String())
	if err != nil {
		return err
	}
	if !sharesEqual || !costEqual {
		return fmt.Errorf("open lots of %s do not add up to the position", holding.position.TokenID)
	}
	return nil
}

func numericEqual(ctx context.Context, tx *sql.Tx, left, right string) (bool, error) {
	var equal bool
	if err := tx.QueryRowContext(ctx, `SELECT $1::numeric = $2::numeric`, left, right).Scan(&equal); err != nil {
		return false, err
	}
	return equal, nil
}

// closeKalshiHolding closes one settled position and its lots and returns the
// realized profit or loss. The position is marked settled and then redeemed in
// one step, matching the event trail of a Polymarket settlement.
func closeKalshiHolding(
	ctx context.Context, tx *sql.Tx, ticker string, holding kalshiHolding, settlement domain.KalshiSettlement, at time.Time,
) (domain.Decimal, error) {
	position := holding.position
	price := domain.Decimal("0")
	if holding.winner {
		price = "1"
	}
	source := "kalshi:" + ticker
	settledAt := settlement.SettledAt
	if settledAt.IsZero() {
		settledAt = at
	}
	payout, err := numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric)::text`, position.TotalShares.String(), price.String())
	if err != nil {
		return "", err
	}
	realizedDelta, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, payout.String(), position.CostBasis.String())
	if err != nil {
		return "", err
	}
	realizedAfter, err := numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, position.RealizedPnL.String(), realizedDelta.String())
	if err != nil {
		return "", err
	}
	for _, lot := range holding.lots {
		lotPayout, err := numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric)::text`, lot.shares.String(), price.String())
		if err != nil {
			return "", err
		}
		lotPnL, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, lotPayout.String(), lot.cost.String())
		if err != nil {
			return "", err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE position_lots SET remaining_shares=0, remaining_cost=0, status='CLOSED', closed_at=$2
			WHERE lot_id=$1 AND status='OPEN'`, lot.id, at)
		if err != nil {
			return "", err
		}
		if !oneRow(result) {
			return "", fmt.Errorf("Kalshi lot %s changed before settlement accounting", lot.id)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO kalshi_settlement_lots (
				lot_id, execution_account_id, ticker, token_id, settled_shares,
				allocated_cost, allocated_payout, realized_pnl, settled_at
			) VALUES ($1,$2,$3,$4,$5::numeric,$6::numeric,$7::numeric,$8::numeric,$9)`,
			lot.id, position.ExecutionAccountID, ticker, position.TokenID, lot.shares.String(),
			lot.cost.String(), lotPayout.String(), lotPnL.String(), at); err != nil {
			return "", err
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_positions
		SET total_shares=0, available_shares=0, reserved_shares=0, cost_basis=0,
		    average_cost_price=0, realized_pnl=$3::numeric, mark_price=$4::numeric,
		    market_value=0, unrealized_pnl=0, is_dust=FALSE, lifecycle_status='CLOSED',
		    settlement_price=$4::numeric, settlement_source=$5, settled_at=$6,
		    last_marked_at=$7, updated_at=$7, version=version+1
		WHERE execution_account_id=$1 AND token_id=$2 AND lifecycle_status='OPEN'`,
		position.ExecutionAccountID, position.TokenID, realizedAfter.String(), price.String(), source, settledAt, at)
	if err != nil {
		return "", fmt.Errorf("close Kalshi position: %w", err)
	}
	if !oneRow(result) {
		return "", fmt.Errorf("Kalshi position %s changed before settlement accounting", position.TokenID)
	}
	settled := domain.PositionEvent{
		EventID:   "position-settled:" + position.ExecutionAccountID + ":" + position.TokenID + ":" + source,
		EventType: domain.PositionEventSettled, ExecutionAccountID: position.ExecutionAccountID,
		MarketID: position.MarketID, TokenID: position.TokenID, FillKey: "settlement:" + source,
		SharesDelta: "0", CashDelta: "0", CostBasisDelta: "0", RealizedPnLDelta: "0",
		SharesAfter: position.TotalShares, CostBasisAfter: position.CostBasis,
		AverageCostAfter: position.AverageCostPrice, RealizedPnLAfter: position.RealizedPnL,
		MarkPrice: price, UnrealizedPnLAfter: "0", OccurredAt: at,
	}
	redeemed := domain.PositionEvent{
		EventID:   "position-redemption:" + source + ":" + position.TokenID,
		EventType: domain.PositionEventRedeemed, ExecutionAccountID: position.ExecutionAccountID,
		MarketID: position.MarketID, TokenID: position.TokenID, FillKey: "redemption:" + source,
		SharesDelta: domain.Decimal("-" + position.TotalShares.String()), CashDelta: payout,
		CostBasisDelta: domain.Decimal("-" + position.CostBasis.String()), RealizedPnLDelta: realizedDelta,
		SharesAfter: "0", CostBasisAfter: "0", AverageCostAfter: "0",
		RealizedPnLAfter: realizedAfter, MarkPrice: price, UnrealizedPnLAfter: "0", OccurredAt: at,
	}
	for _, item := range []struct {
		event domain.PositionEvent
		topic string
	}{{settled, "trading.position.settled.v1"}, {redeemed, "trading.position.redeemed.v1"}} {
		if err := insertPositionEvent(ctx, tx, item.event); err != nil {
			return "", err
		}
		if _, err := insertOutbox(ctx, tx, item.topic, item.event.EventID,
			position.ExecutionAccountID+":"+position.TokenID, item.event, at); err != nil {
			return "", err
		}
	}
	return realizedDelta, nil
}

// creditKalshiSettlement adds the exchange's payout to the account cash and
// records one account event for it. The fee on the market was charged at fill
// time and is deliberately not touched here.
func creditKalshiSettlement(
	ctx context.Context, tx *sql.Tx, accountID, ticker string, revenue domain.Decimal, at time.Time,
) error {
	var totalBefore, availableBefore, reservedBefore domain.Decimal
	if err := tx.QueryRowContext(ctx, `
		SELECT total_balance::text, available_balance::text, reserved_balance::text
		FROM execution_accounts WHERE execution_account_id=$1`, accountID).Scan(&totalBefore, &availableBefore, &reservedBefore); err != nil {
		return err
	}
	totalAfter, err := numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, totalBefore.String(), revenue.String())
	if err != nil {
		return err
	}
	availableAfter, err := numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, availableBefore.String(), revenue.String())
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_accounts
		SET total_balance=$2::numeric, available_balance=$3::numeric, version=version+1, updated_at=$4
		WHERE execution_account_id=$1`, accountID, totalAfter.String(), availableAfter.String(), at); err != nil {
		return err
	}
	eventID := "kalshi-settlement:" + accountID + ":" + ticker
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO execution_account_events (
			account_event_id, execution_account_id, event_type, fill_key, total_balance_delta,
			available_balance_delta, reserved_balance_delta, total_balance_after,
			available_balance_after, reserved_balance_after, occurred_at
		) VALUES ($1,$2,'KALSHI_SETTLED',$3,$4::numeric,$4::numeric,0,$5::numeric,$6::numeric,$7::numeric,$8)`,
		eventID, accountID, "kalshi-settlement:"+ticker, revenue.String(), totalAfter.String(),
		availableAfter.String(), reservedBefore.String(), at); err != nil {
		return err
	}
	_, err = insertOutbox(ctx, tx, "trading.account.kalshi_settled.v1", eventID, accountID,
		map[string]string{"execution_account_id": accountID, "ticker": ticker, "payout": revenue.String()}, at)
	return err
}

func insertKalshiSettlementRow(
	ctx context.Context, tx *sql.Tx, accountID, ticker string, settlement domain.KalshiSettlement,
	holdings []kalshiHolding, status, reason, realizedPnL string, at time.Time,
) error {
	localYes, localNo, cost := "0", "0", "0"
	var err error
	for _, holding := range holdings {
		if strings.HasSuffix(holding.position.TokenID, ":YES") {
			localYes = holding.position.TotalShares.String()
		} else {
			localNo = holding.position.TotalShares.String()
		}
		if c, calcErr := numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, cost, holding.position.CostBasis.String()); calcErr != nil {
			return calcErr
		} else {
			cost = c.String()
		}
	}
	var settledAt, appliedAt any
	if !settlement.SettledAt.IsZero() {
		settledAt = settlement.SettledAt
	}
	if status == "APPLIED" {
		appliedAt = at
	}
	var pnl any
	if realizedPnL != "" {
		pnl = realizedPnL
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO kalshi_settlements (
			execution_account_id, ticker, status, market_result, exchange_yes_count, exchange_no_count,
			exchange_revenue, exchange_payout_per_yes, exchange_fee_cost, local_yes_shares, local_no_shares,
			local_cost_basis, realized_pnl, review_reason, settled_at, applied_at, exchange_record
		) VALUES ($1,$2,$3,$4,COALESCE(NULLIF($5,''),'0')::numeric,COALESCE(NULLIF($6,''),'0')::numeric,
			COALESCE(NULLIF($7,''),'0')::numeric,NULLIF($8,'')::numeric,NULLIF($9,'')::numeric,$10::numeric,
			$11::numeric,$12::numeric,$13::numeric,$14,$15,$16,$17::jsonb)`,
		accountID, ticker, status, settlement.MarketResult, settlement.YesCount.String(), settlement.NoCount.String(),
		settlement.Revenue.String(), settlement.PayoutPerYes.String(), settlement.FeeCost.String(), localYes, localNo,
		cost, pnl, reason, settledAt, appliedAt, string(settlement.Raw))
	if err != nil {
		return fmt.Errorf("record Kalshi settlement: %w", err)
	}
	return nil
}

// CheckKalshiBalance compares the exchange cash with the local total. Open
// orders and reservations legitimately move the two apart, so the comparison is
// skipped while any exist.
func (store *KalshiSettlementStore) CheckKalshiBalance(
	ctx context.Context, executionAccountID string, exchangeBalance, threshold domain.Decimal, observedAt time.Time,
) (domain.KalshiBalanceCheck, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if err := store.requireManaged(executionAccountID); err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	if exchangeBalance.IsEmpty() || threshold.IsEmpty() || observedAt.IsZero() {
		return domain.KalshiBalanceCheck{}, fmt.Errorf("Kalshi balance check needs the exchange balance, a threshold, and a time")
	}
	observedAt = observedAt.UTC()
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, executionAccountID); err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	var local, reserved domain.Decimal
	if err := tx.QueryRowContext(ctx, `
		SELECT total_balance::text, reserved_balance::text FROM execution_accounts
		WHERE execution_account_id=$1`, executionAccountID).Scan(&local, &reserved); err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM asset_reservations
			WHERE execution_account_id=$1 AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
		) OR EXISTS (
			SELECT 1 FROM execution_orders
			WHERE execution_account_id=$1 AND status NOT IN ('CANCELLED','REJECTED','FILLED')
		)`, executionAccountID).Scan(&unresolved); err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	reservedSign, signErr := reserved.Sign()
	if signErr != nil {
		return domain.KalshiBalanceCheck{}, signErr
	}
	if unresolved || reservedSign > 0 {
		return domain.KalshiBalanceCheck{Skipped: true, LocalBalance: local, ExchangeBalance: exchangeBalance}, tx.Commit()
	}
	difference, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, exchangeBalance.String(), local.String())
	if err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	var drifted bool
	if err := tx.QueryRowContext(ctx, `SELECT ABS($1::numeric) > $2::numeric`, difference.String(), threshold.String()).Scan(&drifted); err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	if drifted {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO kalshi_balance_drift (
				execution_account_id, status, local_balance, exchange_balance, difference,
				observations, first_seen_at, last_seen_at
			) VALUES ($1,'OPEN',$2::numeric,$3::numeric,$4::numeric,1,$5,$5)
			ON CONFLICT (execution_account_id) DO UPDATE SET
				first_seen_at = CASE WHEN kalshi_balance_drift.status='OPEN' THEN kalshi_balance_drift.first_seen_at ELSE EXCLUDED.first_seen_at END,
				observations = CASE WHEN kalshi_balance_drift.status='OPEN' THEN kalshi_balance_drift.observations+1 ELSE 1 END,
				status='OPEN', local_balance=EXCLUDED.local_balance, exchange_balance=EXCLUDED.exchange_balance,
				difference=EXCLUDED.difference, last_seen_at=EXCLUDED.last_seen_at, resolved_at=NULL`,
			executionAccountID, local.String(), exchangeBalance.String(), difference.String(), observedAt)
	} else {
		_, err = tx.ExecContext(ctx, `
			UPDATE kalshi_balance_drift
			SET status='RESOLVED', local_balance=$2::numeric, exchange_balance=$3::numeric, difference=$4::numeric,
			    last_seen_at=$5, resolved_at=$5
			WHERE execution_account_id=$1 AND status='OPEN'`,
			executionAccountID, local.String(), exchangeBalance.String(), difference.String(), observedAt)
	}
	if err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.KalshiBalanceCheck{}, err
	}
	return domain.KalshiBalanceCheck{LocalBalance: local, ExchangeBalance: exchangeBalance, Difference: difference, Drifted: drifted}, nil
}

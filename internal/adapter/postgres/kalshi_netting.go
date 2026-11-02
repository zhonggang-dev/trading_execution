package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Kalshi nets opposite sides of one market by itself: buying NO while holding
// YES (or the reverse) removes one contract of each and pays one dollar per pair.
// Without the same step here the ledger keeps both sides, understates cash, and
// the market's settlement record can no longer agree with the local shares.

// kalshiNettingParams describes one fill to net against the opposite side.
type kalshiNettingParams struct {
	Order domain.Order
	Fill  domain.Fill
	At    time.Time
	// AppliedBefore is set only by the repair replay. It limits the opposite lots
	// to those opened by a fill applied earlier, so fills net in their original
	// order. Live accounting leaves it nil: the current fill has no applied_at yet.
	AppliedBefore *time.Time
	DustShares    domain.Decimal
	DustNotional  domain.Decimal
}

// KalshiNettingClosure is the allocation of one closed lot.
type KalshiNettingClosure struct {
	ClosureID      string         `json:"closure_id"`
	ClosingFillKey string         `json:"closing_fill_key"`
	LotID          string         `json:"lot_id"`
	TokenID        string         `json:"token_id"`
	ClosedShares   domain.Decimal `json:"closed_shares"`
	AllocatedCost  domain.Decimal `json:"allocated_cost"`
	AllocatedCash  domain.Decimal `json:"allocated_net_proceeds"`
	RealizedPnL    domain.Decimal `json:"realized_pnl"`
	lotShares      domain.Decimal
	lotCost        domain.Decimal
}

// kalshiNettingResult reports what one fill netted. Deferred means the opposite
// side is reserved by an open order and nothing was changed.
type kalshiNettingResult struct {
	Deferred bool
	Pairs    domain.Decimal
	Closures []KalshiNettingClosure
	Events   []domain.PositionEvent
}

// kalshiOppositeToken returns the market ticker and the other side's token.
func kalshiOppositeToken(tokenID string) (ticker, opposite string, ok bool) {
	if !strings.HasPrefix(tokenID, "kalshi:") {
		return "", "", false
	}
	body := strings.TrimPrefix(tokenID, "kalshi:")
	switch {
	case strings.HasSuffix(body, ":YES") && len(body) > len(":YES"):
		ticker = strings.TrimSuffix(body, ":YES")
		return ticker, "kalshi:" + ticker + ":NO", true
	case strings.HasSuffix(body, ":NO") && len(body) > len(":NO"):
		ticker = strings.TrimSuffix(body, ":NO")
		return ticker, "kalshi:" + ticker + ":YES", true
	}
	return "", "", false
}

// applyKalshiNetting closes min(this fill's open lot, opposite open lots) pairs
// inside the caller's transaction. The caller holds the account row lock and must
// have written the fill's own account event, so that event's balance delta does
// not include the payout. It is a no-op for anything but a Kalshi BUY.
func applyKalshiNetting(ctx context.Context, tx *sql.Tx, params kalshiNettingParams) (kalshiNettingResult, error) {
	fill := params.Fill
	ticker, oppositeToken, ok := kalshiOppositeToken(fill.TokenID)
	if fill.Side != domain.SideBuy || !ok {
		return kalshiNettingResult{}, nil
	}
	opposite, err := selectPosition(ctx, tx, fill.ExecutionAccountID, oppositeToken, true)
	if errors.Is(err, port.ErrPositionNotFound) {
		return kalshiNettingResult{}, nil
	}
	if err != nil {
		return kalshiNettingResult{}, err
	}
	if sign, signErr := opposite.TotalShares.Sign(); signErr != nil || sign == 0 ||
		opposite.LifecycleStatus != domain.PositionLifecycleOpen {
		return kalshiNettingResult{}, signErr
	}
	if sign, signErr := opposite.ReservedShares.Sign(); signErr != nil {
		return kalshiNettingResult{}, signErr
	} else if sign > 0 {
		return kalshiNettingResult{Deferred: true}, nil
	}
	own, err := selectPosition(ctx, tx, fill.ExecutionAccountID, fill.TokenID, true)
	if errors.Is(err, port.ErrPositionNotFound) {
		return kalshiNettingResult{}, nil
	}
	if err != nil {
		return kalshiNettingResult{}, err
	}
	if own.LifecycleStatus != domain.PositionLifecycleOpen {
		return kalshiNettingResult{}, nil
	}
	ownLot, found, err := loadKalshiFillLot(ctx, tx, fill.Key)
	if err != nil || !found {
		return kalshiNettingResult{}, err
	}
	oppositeLots, err := loadKalshiNettingLots(ctx, tx, fill.ExecutionAccountID, oppositeToken, params.AppliedBefore)
	if err != nil {
		return kalshiNettingResult{}, err
	}
	closures, pairs, err := planKalshiNettingClosures(ctx, tx, fill, ownLot, oppositeLots, oppositeToken)
	if err != nil || len(closures) == 0 {
		return kalshiNettingResult{}, err
	}
	if err := allocateKalshiNettingCash(ctx, tx, closures, pairs); err != nil {
		return kalshiNettingResult{}, err
	}
	at := params.At.UTC()
	for _, closure := range closures {
		if err := closeKalshiNettedLot(ctx, tx, closure, at); err != nil {
			return kalshiNettingResult{}, err
		}
	}
	events := make([]domain.PositionEvent, 0, 2)
	for _, position := range []domain.Position{own, opposite} {
		event, err := settleKalshiNettedPosition(ctx, tx, params, position, closures, at)
		if err != nil {
			return kalshiNettingResult{}, err
		}
		events = append(events, event)
	}
	if err := creditKalshiNetting(ctx, tx, params, ticker, pairs, at); err != nil {
		return kalshiNettingResult{}, err
	}
	return kalshiNettingResult{Pairs: pairs, Closures: closures, Events: events}, nil
}

// loadKalshiFillLot locks the open lot this fill opened, if it is still open.
func loadKalshiFillLot(ctx context.Context, tx *sql.Tx, fillKey string) (kalshiLot, bool, error) {
	lot := kalshiLot{id: "lot:" + fillKey}
	err := tx.QueryRowContext(ctx, `
		SELECT remaining_shares::text, remaining_cost::text
		FROM position_lots WHERE lot_id=$1 AND status='OPEN' FOR UPDATE`, lot.id).Scan(&lot.shares, &lot.cost)
	if errors.Is(err, sql.ErrNoRows) {
		return kalshiLot{}, false, nil
	}
	if err != nil {
		return kalshiLot{}, false, fmt.Errorf("lock Kalshi fill lot: %w", err)
	}
	return lot, true, nil
}

// loadKalshiNettingLots locks the opposite side's open lots oldest first.
func loadKalshiNettingLots(
	ctx context.Context, tx *sql.Tx, accountID, tokenID string, appliedBefore *time.Time,
) ([]kalshiLot, error) {
	if appliedBefore == nil {
		return loadKalshiLots(ctx, tx, accountID, tokenID)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT lot.lot_id, lot.remaining_shares::text, lot.remaining_cost::text
		FROM position_lots AS lot
		LEFT JOIN execution_fills AS opening ON opening.fill_key=lot.opening_fill_key
		WHERE lot.execution_account_id=$1 AND lot.token_id=$2 AND lot.status='OPEN'
		  AND (opening.fill_key IS NULL OR opening.applied_at < $3)
		ORDER BY lot.opened_at, lot.lot_id FOR UPDATE OF lot`, accountID, tokenID, appliedBefore.UTC())
	if err != nil {
		return nil, fmt.Errorf("lock Kalshi replay lots: %w", err)
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

// planKalshiNettingClosures picks the shares and cost of every lot to close: the
// fill's own lot and the opposite lots FIFO, each side closing exactly pairs.
func planKalshiNettingClosures(
	ctx context.Context, tx *sql.Tx, fill domain.Fill, ownLot kalshiLot, oppositeLots []kalshiLot, oppositeToken string,
) ([]KalshiNettingClosure, domain.Decimal, error) {
	oppositeShares := domain.Decimal("0")
	var err error
	for _, lot := range oppositeLots {
		if oppositeShares, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, oppositeShares.String(), lot.shares.String()); err != nil {
			return nil, "", err
		}
	}
	pairs, err := smallerDecimal(ownLot.shares, oppositeShares)
	if err != nil {
		return nil, "", err
	}
	if sign, err := pairs.Sign(); err != nil || sign <= 0 {
		return nil, "", err
	}
	closures := make([]KalshiNettingClosure, 0, len(oppositeLots)+1)
	own, err := planKalshiLotClosure(ctx, tx, fill.Key, fill.TokenID, ownLot, pairs)
	if err != nil {
		return nil, "", err
	}
	closures = append(closures, own)
	remaining := pairs
	for _, lot := range oppositeLots {
		if sign, err := remaining.Sign(); err != nil {
			return nil, "", err
		} else if sign == 0 {
			break
		}
		shares, err := smallerDecimal(lot.shares, remaining)
		if err != nil {
			return nil, "", err
		}
		closure, err := planKalshiLotClosure(ctx, tx, fill.Key, oppositeToken, lot, shares)
		if err != nil {
			return nil, "", err
		}
		closures = append(closures, closure)
		if remaining, err = numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, remaining.String(), shares.String()); err != nil {
			return nil, "", err
		}
	}
	return closures, pairs, nil
}

// planKalshiLotClosure allocates cost with the same formula as a SELL fill.
func planKalshiLotClosure(
	ctx context.Context, tx *sql.Tx, fillKey, tokenID string, lot kalshiLot, shares domain.Decimal,
) (KalshiNettingClosure, error) {
	cost := lot.cost
	if !shares.Equal(lot.shares) {
		var err error
		cost, err = numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric/$3::numeric)::text`,
			lot.cost.String(), shares.String(), lot.shares.String())
		if err != nil {
			return KalshiNettingClosure{}, err
		}
	}
	return KalshiNettingClosure{
		ClosureID: "closure:netting:" + fillKey + ":" + lot.id, ClosingFillKey: fillKey, LotID: lot.id,
		TokenID: tokenID, ClosedShares: shares, AllocatedCost: cost, lotShares: lot.shares, lotCost: lot.cost,
	}, nil
}

// allocateKalshiNettingCash splits the payout by cost. The largest entry takes
// the division remainder, so the allocations add up to exactly pairs dollars.
func allocateKalshiNettingCash(ctx context.Context, tx *sql.Tx, closures []KalshiNettingClosure, pairs domain.Decimal) error {
	totalCost, largest := domain.Decimal("0"), 0
	var err error
	for index, closure := range closures {
		if totalCost, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, totalCost.String(), closure.AllocatedCost.String()); err != nil {
			return err
		}
		comparison, err := closure.AllocatedCost.Compare(closures[largest].AllocatedCost)
		if err != nil {
			return err
		}
		if comparison > 0 {
			largest = index
		}
	}
	if sign, err := totalCost.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("%w: Kalshi netting has no cost to allocate its payout by", port.ErrFillConflict)
	}
	allocated := domain.Decimal("0")
	for index := range closures {
		if index == largest {
			continue
		}
		cash, err := numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric/$3::numeric)::text`,
			pairs.String(), closures[index].AllocatedCost.String(), totalCost.String())
		if err != nil {
			return err
		}
		closures[index].AllocatedCash = cash
		if allocated, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, allocated.String(), cash.String()); err != nil {
			return err
		}
	}
	if closures[largest].AllocatedCash, err = numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, pairs.String(), allocated.String()); err != nil {
		return err
	}
	for index := range closures {
		if sign, err := closures[index].AllocatedCash.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("%w: Kalshi netting allocated a negative payout", port.ErrFillConflict)
		}
		if closures[index].RealizedPnL, err = numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`,
			closures[index].AllocatedCash.String(), closures[index].AllocatedCost.String()); err != nil {
			return err
		}
	}
	return nil
}

// closeKalshiNettedLot reduces one lot and records its closure.
func closeKalshiNettedLot(ctx context.Context, tx *sql.Tx, closure KalshiNettingClosure, at time.Time) error {
	newShares, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, closure.lotShares.String(), closure.ClosedShares.String())
	if err != nil {
		return err
	}
	newCost, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, closure.lotCost.String(), closure.AllocatedCost.String())
	if err != nil {
		return err
	}
	closed := newShares.Equal("0")
	result, err := tx.ExecContext(ctx, `
		UPDATE position_lots
		SET remaining_shares=$2::numeric,
		    remaining_cost=CASE WHEN $3::boolean THEN 0 ELSE $4::numeric END,
		    status=CASE WHEN $3::boolean THEN 'CLOSED' ELSE 'OPEN' END,
		    closed_at=CASE WHEN $3::boolean THEN $5::timestamptz ELSE NULL::timestamptz END
		WHERE lot_id=$1 AND status='OPEN' AND remaining_shares=$6::numeric AND remaining_cost=$7::numeric`,
		closure.LotID, newShares.String(), closed, newCost.String(), at, closure.lotShares.String(), closure.lotCost.String())
	if err != nil {
		return fmt.Errorf("close Kalshi netted lot: %w", err)
	}
	if !oneRow(result) {
		return port.ErrFillConflict
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO position_lot_closures (
			closure_id, closing_fill_key, lot_id, closed_shares, allocated_cost,
			allocated_net_proceeds, realized_pnl, closed_at
		) VALUES ($1,$2,$3,$4::numeric,$5::numeric,$6::numeric,$7::numeric,$8::timestamptz)`,
		closure.ClosureID, closure.ClosingFillKey, closure.LotID, closure.ClosedShares.String(),
		closure.AllocatedCost.String(), closure.AllocatedCash.String(), closure.RealizedPnL.String(), at)
	if err != nil {
		return fmt.Errorf("insert Kalshi netting closure: %w", err)
	}
	return nil
}

// settleKalshiNettedPosition removes one token's share of the closures from its
// position and records the position event. The position loses exactly what its
// lots lost, because Kalshi settlement requires the two to add up exactly.
func settleKalshiNettedPosition(
	ctx context.Context, tx *sql.Tx, params kalshiNettingParams, position domain.Position,
	closures []KalshiNettingClosure, at time.Time,
) (domain.PositionEvent, error) {
	shares, cost, cash, realized := domain.Decimal("0"), domain.Decimal("0"), domain.Decimal("0"), domain.Decimal("0")
	var err error
	for _, closure := range closures {
		if closure.TokenID != position.TokenID {
			continue
		}
		for _, item := range []struct {
			total *domain.Decimal
			value domain.Decimal
		}{{&shares, closure.ClosedShares}, {&cost, closure.AllocatedCost}, {&cash, closure.AllocatedCash}, {&realized, closure.RealizedPnL}} {
			if *item.total, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, item.total.String(), item.value.String()); err != nil {
				return domain.PositionEvent{}, err
			}
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_positions
		SET total_shares = total_shares - $3::numeric,
		    available_shares = available_shares - $3::numeric,
		    cost_basis = CASE WHEN total_shares = $3::numeric THEN 0 ELSE cost_basis - $4::numeric END,
		    average_cost_price = CASE WHEN total_shares = $3::numeric THEN 0
		        ELSE (cost_basis - $4::numeric)/(total_shares - $3::numeric) END,
		    realized_pnl = realized_pnl + $5::numeric,
		    market_value = CASE WHEN mark_price IS NULL THEN 0 ELSE mark_price*(total_shares-$3::numeric) END,
		    unrealized_pnl = CASE WHEN total_shares = $3::numeric THEN 0
		        WHEN mark_price IS NULL THEN 0
		        ELSE mark_price*(total_shares-$3::numeric)-(cost_basis-$4::numeric) END,
		    is_dust = (total_shares-$3::numeric) > 0 AND (
		        (total_shares-$3::numeric) <= $6::numeric OR
		        (CASE WHEN mark_price IS NULL THEN cost_basis-$4::numeric
		              ELSE mark_price*(total_shares-$3::numeric) END) <= $7::numeric),
		    lifecycle_status = CASE WHEN total_shares = $3::numeric THEN 'CLOSED' ELSE lifecycle_status END,
		    version = version + 1,
		    updated_at = GREATEST(updated_at, $8::timestamptz)
		WHERE execution_account_id = $1 AND token_id = $2 AND lifecycle_status = 'OPEN'
		  AND total_shares >= $3::numeric AND available_shares >= $3::numeric AND cost_basis >= $4::numeric`,
		position.ExecutionAccountID, position.TokenID, shares.String(), cost.String(), realized.String(),
		decimalOrZero(params.DustShares), decimalOrZero(params.DustNotional), at)
	if err != nil {
		return domain.PositionEvent{}, fmt.Errorf("settle Kalshi netted position: %w", err)
	}
	if !oneRow(result) {
		return domain.PositionEvent{}, port.ErrFillConflict
	}
	after, err := selectPosition(ctx, tx, position.ExecutionAccountID, position.TokenID, true)
	if err != nil {
		return domain.PositionEvent{}, err
	}
	event := domain.PositionEvent{
		EventID:   "position-netting:" + params.Fill.Key + ":" + position.TokenID,
		EventType: domain.PositionEventSold, ExecutionAccountID: position.ExecutionAccountID,
		MarketID: after.MarketID, TokenID: position.TokenID, OrderID: params.Order.ID, FillKey: params.Fill.Key,
		ModelID: params.Order.Intent.ModelID, StrategyID: params.Order.Intent.StrategyID,
		SharesDelta: domain.Decimal("-" + shares.String()), CashDelta: cash,
		CostBasisDelta: domain.Decimal("-" + cost.String()), RealizedPnLDelta: realized,
		SharesAfter: after.TotalShares, CostBasisAfter: after.CostBasis, AverageCostAfter: after.AverageCostPrice,
		RealizedPnLAfter: after.RealizedPnL, MarkPrice: after.MarkPrice,
		UnrealizedPnLAfter: after.UnrealizedPnL, OccurredAt: at,
	}
	if err := insertPositionEvent(ctx, tx, event); err != nil {
		return domain.PositionEvent{}, fmt.Errorf("record Kalshi netting position event: %w", err)
	}
	return event, nil
}

// creditKalshiNetting adds the payout to the account and records it. The account
// event's primary key is what stops one fill from being paid twice. Fees were
// charged when the fill was booked; netting charges none.
func creditKalshiNetting(
	ctx context.Context, tx *sql.Tx, params kalshiNettingParams, ticker string, pairs domain.Decimal, at time.Time,
) error {
	accountID, fillKey := params.Fill.ExecutionAccountID, params.Fill.Key
	var total, available, reserved domain.Decimal
	err := tx.QueryRowContext(ctx, `
		UPDATE execution_accounts
		SET total_balance=total_balance+$2::numeric, available_balance=available_balance+$2::numeric,
		    version=version+1, updated_at=GREATEST(updated_at,$3::timestamptz)
		WHERE execution_account_id=$1
		RETURNING total_balance::text, available_balance::text, reserved_balance::text`,
		accountID, pairs.String(), at).Scan(&total, &available, &reserved)
	if err != nil {
		return fmt.Errorf("credit Kalshi netting: %w", err)
	}
	eventID := "kalshi-netting:" + fillKey
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO execution_account_events (
			account_event_id, execution_account_id, event_type, order_id, fill_key,
			total_balance_delta, available_balance_delta, reserved_balance_delta,
			total_balance_after, available_balance_after, reserved_balance_after, occurred_at
		) VALUES ($1,$2,'KALSHI_NETTED',$3,$1,$4::numeric,$4::numeric,0,$5::numeric,$6::numeric,$7::numeric,$8)`,
		eventID, accountID, params.Order.ID, pairs.String(), total.String(), available.String(), reserved.String(), at); err != nil {
		return fmt.Errorf("record Kalshi netting account event: %w", err)
	}
	_, err = insertOutbox(ctx, tx, "trading.account.kalshi_netted.v1", eventID, accountID, map[string]string{
		"execution_account_id": accountID, "ticker": ticker, "fill_key": fillKey,
		"order_id": params.Order.ID, "pairs": pairs.String(), "payout": pairs.String(),
	}, at)
	return err
}

// smallerDecimal returns the smaller of two decimals.
func smallerDecimal(left, right domain.Decimal) (domain.Decimal, error) {
	comparison, err := left.Compare(right)
	if err != nil {
		return "", err
	}
	if comparison > 0 {
		return right, nil
	}
	return left, nil
}

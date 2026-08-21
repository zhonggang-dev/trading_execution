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

// applyBuy 应用 Buy 的领域变更。
func (ledger *FillLedger) applyBuy(
	ctx context.Context,
	tx *sql.Tx,
	order domain.Order,
	fill domain.Fill,
	reservation domain.AssetReservation,
	cumulativeShares, cumulativeNotional, cumulativeFees domain.Decimal,
	target domain.OrderStatus,
) (domain.Position, []domain.PositionEvent, error) {
	if sign, err := fill.NetCashDelta.Sign(); err != nil || sign >= 0 {
		return domain.Position{}, nil, fmt.Errorf("BUY fill must have a negative net cash delta")
	}
	fillCost, err := numeric(ctx, tx, `SELECT (-$1::numeric)::text`, fill.NetCashDelta.String())
	if err != nil {
		return domain.Position{}, nil, err
	}
	remainingShares, targetReserved, err := targetReservationAmounts(ctx, tx, reservation, cumulativeShares, target)
	if err != nil {
		return domain.Position{}, nil, err
	}
	now := ledger.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_accounts
		SET total_balance = total_balance + $4::numeric,
		    reserved_balance = reserved_balance - $2::numeric + $3::numeric,
		    available_balance = (total_balance + $4::numeric)
		        - (reserved_balance - $2::numeric + $3::numeric),
		    version = version + 1,
		    updated_at = $5
		WHERE execution_account_id = $1
		  AND reserved_balance >= $2::numeric
		  AND (total_balance + $4::numeric) >= 0
		  AND (reserved_balance - $2::numeric + $3::numeric) >= 0
		  AND ((total_balance + $4::numeric)
		       - (reserved_balance - $2::numeric + $3::numeric)) >= 0`,
		fill.ExecutionAccountID, reservation.RemainingReservedBalance.String(), targetReserved.String(),
		fill.NetCashDelta.String(), now)
	if err != nil {
		return domain.Position{}, nil, fmt.Errorf("settle BUY cash: %w", err)
	}
	if !oneRow(result) {
		return domain.Position{}, nil, reject("BUY_FILL_EXCEEDS_PROTECTED_BALANCE", "confirmed fill plus fees exceeds the account's protected balance")
	}

	sharesThreshold := decimalOrZero(ledger.dustSharesThreshold)
	notionalThreshold := decimalOrZero(ledger.dustNotionalThreshold)
	result, err = tx.ExecContext(ctx, `
		INSERT INTO execution_positions (
			execution_account_id, market_id, condition_id, token_id, outcome_index, outcome_name,
			total_shares, available_shares, reserved_shares, cost_basis, average_cost_price,
			realized_pnl, market_value, unrealized_pnl, is_dust, lifecycle_status, created_at, updated_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7::numeric,$7::numeric,0,$8::numeric,
			($8::numeric/$7::numeric),0,0,0,
			($7::numeric > 0 AND ($7::numeric <= $9::numeric OR $8::numeric <= $10::numeric)),
			'OPEN',$11,$11
		)
		ON CONFLICT (execution_account_id, token_id) DO UPDATE
		SET condition_id = CASE WHEN execution_positions.condition_id = '' THEN EXCLUDED.condition_id ELSE execution_positions.condition_id END,
		    outcome_index = COALESCE(execution_positions.outcome_index, EXCLUDED.outcome_index),
		    outcome_name = CASE WHEN execution_positions.outcome_name = '' THEN EXCLUDED.outcome_name ELSE execution_positions.outcome_name END,
		    total_shares = execution_positions.total_shares + EXCLUDED.total_shares,
		    available_shares = execution_positions.available_shares + EXCLUDED.available_shares,
		    cost_basis = execution_positions.cost_basis + EXCLUDED.cost_basis,
		    average_cost_price = (execution_positions.cost_basis + EXCLUDED.cost_basis)
		        / (execution_positions.total_shares + EXCLUDED.total_shares),
		    market_value = CASE WHEN execution_positions.mark_price IS NULL THEN 0
		        ELSE execution_positions.mark_price * (execution_positions.total_shares + EXCLUDED.total_shares) END,
		    unrealized_pnl = CASE WHEN execution_positions.mark_price IS NULL
		        THEN 0
		        ELSE execution_positions.mark_price * (execution_positions.total_shares + EXCLUDED.total_shares)
		             - (execution_positions.cost_basis + EXCLUDED.cost_basis) END,
		    is_dust = (execution_positions.total_shares + EXCLUDED.total_shares) > 0 AND (
		        (execution_positions.total_shares + EXCLUDED.total_shares) <= $9::numeric OR
		        (CASE WHEN execution_positions.mark_price IS NULL
		              THEN execution_positions.cost_basis + EXCLUDED.cost_basis
		              ELSE execution_positions.mark_price * (execution_positions.total_shares + EXCLUDED.total_shares) END) <= $10::numeric),
		    lifecycle_status = CASE WHEN execution_positions.lifecycle_status = 'CLOSED' THEN 'OPEN'
		                            ELSE execution_positions.lifecycle_status END,
		    version = execution_positions.version + 1,
		    updated_at = EXCLUDED.updated_at
		WHERE execution_positions.market_id = EXCLUDED.market_id
		  AND (execution_positions.condition_id = '' OR EXCLUDED.condition_id = '' OR execution_positions.condition_id = EXCLUDED.condition_id)
		  AND (execution_positions.outcome_index IS NULL OR EXCLUDED.outcome_index IS NULL OR execution_positions.outcome_index = EXCLUDED.outcome_index)
		  AND (execution_positions.outcome_name = '' OR EXCLUDED.outcome_name = '' OR execution_positions.outcome_name = EXCLUDED.outcome_name)`,
		fill.ExecutionAccountID, fill.MarketID, fill.ConditionID, fill.TokenID,
		order.Intent.OutcomeIndex, order.Intent.OutcomeName, fill.Shares.String(), fillCost.String(),
		sharesThreshold, notionalThreshold, now)
	if err != nil {
		return domain.Position{}, nil, fmt.Errorf("credit BUY position: %w", err)
	}
	if !oneRow(result) {
		return domain.Position{}, nil, reject("POSITION_IDENTITY_MISMATCH", "token is already associated with different market or outcome metadata")
	}
	position, err := selectPosition(ctx, tx, fill.ExecutionAccountID, fill.TokenID, true)
	if err != nil {
		return domain.Position{}, nil, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO position_lots (
			lot_id, execution_account_id, market_id, condition_id, token_id, outcome_index, outcome_name, neg_risk, model_id, strategy_id,
			opening_order_id, opening_fill_key, original_shares, remaining_shares,
			original_cost, remaining_cost, average_entry_price, status, opened_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::numeric,$13::numeric,$14::numeric,$14::numeric,$15::numeric,'OPEN',$16)`,
		"lot:"+fill.Key, fill.ExecutionAccountID, fill.MarketID, fill.ConditionID, fill.TokenID,
		order.Intent.OutcomeIndex, order.Intent.OutcomeName, order.Intent.ExpectedNegRisk, order.Intent.ModelID, order.Intent.StrategyID,
		order.ID, fill.Key, fill.Shares.String(), fillCost.String(), fill.Price.String(), fill.MatchedAt)
	if err != nil {
		return domain.Position{}, nil, fmt.Errorf("insert BUY position lot: %w", err)
	}
	if err := updateReservationForFill(ctx, tx, &reservation, cumulativeShares, cumulativeNotional, cumulativeFees,
		targetReserved, "0", remainingShares, target, fill); err != nil {
		return domain.Position{}, nil, err
	}
	event := positionEventForFill(order, fill, position, fill.Shares, fill.NetCashDelta, fillCost, "0")
	event.EventType = domain.PositionEventBought
	if err := insertPositionEvent(ctx, tx, event); err != nil {
		return domain.Position{}, nil, err
	}
	return position, []domain.PositionEvent{event}, nil
}

// applySell 应用 Sell 的领域变更。
func (ledger *FillLedger) applySell(
	ctx context.Context,
	tx *sql.Tx,
	order domain.Order,
	fill domain.Fill,
	reservation domain.AssetReservation,
	cumulativeShares, cumulativeNotional, cumulativeFees domain.Decimal,
	target domain.OrderStatus,
) (domain.Position, []domain.PositionEvent, error) {
	if sign, err := fill.NetCashDelta.Sign(); err != nil || sign < 0 {
		return domain.Position{}, nil, fmt.Errorf("SELL fill must have a non-negative net cash delta")
	}
	before, err := selectPosition(ctx, tx, fill.ExecutionAccountID, fill.TokenID, true)
	if err != nil {
		return domain.Position{}, nil, err
	}
	if cmp, err := before.TotalShares.Compare(fill.Shares); err != nil || cmp < 0 {
		return domain.Position{}, nil, reject("SELL_FILL_EXCEEDS_POSITION", "confirmed SELL fill exceeds authoritative shares")
	}
	if sign, _ := before.TotalShares.Sign(); sign > 0 {
		if costSign, _ := before.CostBasis.Sign(); costSign <= 0 {
			return domain.Position{}, nil, reject("POSITION_COST_BASIS_MISSING", "position has shares but no cost basis; backfill is required before live SELL settlement")
		}
	}
	targetLot, err := lockTargetSellLot(ctx, tx, order, before)
	if err != nil {
		return domain.Position{}, nil, err
	}
	if comparison, compareErr := targetLot.shares.Compare(fill.Shares); compareErr != nil || comparison < 0 {
		return domain.Position{}, nil, reject("SELL_FILL_EXCEEDS_TARGET_LOT", "confirmed SELL fill exceeds the selected position lot")
	}
	remainingShares, targetReserved, err := targetReservationAmounts(ctx, tx, reservation, cumulativeShares, target)
	if err != nil {
		return domain.Position{}, nil, err
	}
	allocatedCost, err := numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric/$3::numeric)::text`,
		targetLot.cost.String(), fill.Shares.String(), targetLot.shares.String())
	if err != nil {
		return domain.Position{}, nil, err
	}
	realizedDelta, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, fill.NetCashDelta.String(), allocatedCost.String())
	if err != nil {
		return domain.Position{}, nil, err
	}
	now := ledger.now().UTC()
	sharesThreshold := decimalOrZero(ledger.dustSharesThreshold)
	notionalThreshold := decimalOrZero(ledger.dustNotionalThreshold)
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_positions
		SET total_shares = total_shares - $3::numeric,
		    reserved_shares = reserved_shares - $4::numeric + $5::numeric,
		    available_shares = (total_shares - $3::numeric)
		        - (reserved_shares - $4::numeric + $5::numeric),
		    cost_basis = CASE WHEN total_shares = $3::numeric THEN 0 ELSE cost_basis - $6::numeric END,
		    average_cost_price = CASE WHEN total_shares = $3::numeric THEN 0
		        ELSE (cost_basis - $6::numeric)/(total_shares - $3::numeric) END,
		    realized_pnl = realized_pnl + $7::numeric,
		    market_value = CASE WHEN mark_price IS NULL THEN 0 ELSE mark_price*(total_shares-$3::numeric) END,
		    unrealized_pnl = CASE WHEN total_shares = $3::numeric THEN 0
		        WHEN mark_price IS NULL THEN 0
		        ELSE mark_price*(total_shares-$3::numeric)-(cost_basis-$6::numeric) END,
		    is_dust = (total_shares-$3::numeric) > 0 AND (
		        (total_shares-$3::numeric) <= $8::numeric OR
		        (CASE WHEN mark_price IS NULL THEN cost_basis-$6::numeric
		              ELSE mark_price*(total_shares-$3::numeric) END) <= $9::numeric),
		    lifecycle_status = CASE WHEN total_shares = $3::numeric THEN 'CLOSED' ELSE lifecycle_status END,
		    version = version + 1,
		    updated_at = $10
		WHERE execution_account_id = $1 AND token_id = $2 AND market_id = $11
		  AND total_shares >= $3::numeric
		  AND reserved_shares >= $4::numeric
		  AND (reserved_shares-$4::numeric+$5::numeric) >= 0
		  AND (total_shares-$3::numeric) >= (reserved_shares-$4::numeric+$5::numeric)
		  AND cost_basis >= $6::numeric`,
		fill.ExecutionAccountID, fill.TokenID, fill.Shares.String(), reservation.RemainingReservedShares.String(),
		targetReserved.String(), allocatedCost.String(), realizedDelta.String(), sharesThreshold,
		notionalThreshold, now, fill.MarketID)
	if err != nil {
		return domain.Position{}, nil, fmt.Errorf("settle SELL position: %w", err)
	}
	if !oneRow(result) {
		return domain.Position{}, nil, reject("SELL_SETTLEMENT_POSITION_MISMATCH", "confirmed SELL fill exceeds reserved or authoritative shares")
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE execution_accounts
		SET total_balance = total_balance + $2::numeric,
		    available_balance = available_balance + $2::numeric,
		    version = version + 1,
		    updated_at = $3
		WHERE execution_account_id = $1`, fill.ExecutionAccountID, fill.NetCashDelta.String(), now)
	if err != nil {
		return domain.Position{}, nil, fmt.Errorf("credit SELL proceeds: %w", err)
	}
	if !oneRow(result) {
		return domain.Position{}, nil, reject("EXECUTION_ACCOUNT_NOT_FOUND", "execution account disappeared during SELL settlement")
	}
	if err := closeTargetLot(ctx, tx, fill, targetLot, allocatedCost); err != nil {
		return domain.Position{}, nil, err
	}
	position, err := selectPosition(ctx, tx, fill.ExecutionAccountID, fill.TokenID, true)
	if err != nil {
		return domain.Position{}, nil, err
	}
	if err := updateReservationForFill(ctx, tx, &reservation, cumulativeShares, cumulativeNotional, cumulativeFees,
		"0", targetReserved, remainingShares, target, fill); err != nil {
		return domain.Position{}, nil, err
	}
	negativeShares := domain.Decimal("-" + fill.Shares.String())
	negativeCost := domain.Decimal("-" + allocatedCost.String())
	event := positionEventForFill(order, fill, position, negativeShares, fill.NetCashDelta, negativeCost, realizedDelta)
	event.EventType = domain.PositionEventSold
	if err := insertPositionEvent(ctx, tx, event); err != nil {
		return domain.Position{}, nil, err
	}
	return position, []domain.PositionEvent{event}, nil
}

// targetReservationAmounts 计算对应业务操作所需的目标数值。
func targetReservationAmounts(
	ctx context.Context,
	tx *sql.Tx,
	reservation domain.AssetReservation,
	cumulativeShares domain.Decimal,
	target domain.OrderStatus,
) (domain.Decimal, domain.Decimal, error) {
	remaining, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, reservation.RequestedShares.String(), cumulativeShares.String())
	if err != nil {
		return "", "", err
	}
	// CANCELLED is not final for reservations until the separate trade
	// propagation grace completes. A late confirmed fill may still arrive.
	terminal := target == domain.OrderStatusFilled || target == domain.OrderStatusRejected
	if terminal {
		return remaining, "0", nil
	}
	if reservation.Side == domain.SideSell {
		return remaining, remaining, nil
	}
	reserved, err := numeric(ctx, tx, `SELECT ($1::numeric*$2::numeric)::text`, reservation.ReserveUnitPrice.String(), remaining.String())
	return remaining, reserved, err
}

// updateReservationForFill 在当前事务中更新 Reservation For Fill。
func updateReservationForFill(
	ctx context.Context,
	tx *sql.Tx,
	reservation *domain.AssetReservation,
	cumulativeShares, cumulativeNotional, cumulativeFees, targetBalance, targetShares, unfilledShares domain.Decimal,
	target domain.OrderStatus,
	fill domain.Fill,
) error {
	terminal := target == domain.OrderStatusFilled || target == domain.OrderStatusRejected
	status := domain.ReservationStatusActive
	if target == domain.OrderStatusFilled {
		status = domain.ReservationStatusSettled
	} else if terminal {
		status = domain.ReservationStatusReleased
	} else if target == domain.OrderStatusCancelled || target == domain.OrderStatusManualReview ||
		target == domain.OrderStatusUnknown || target == domain.OrderStatusReconciling {
		status = domain.ReservationStatusReconciliationRequired
	}
	now := fill.ObservedAt.UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE asset_reservations
		SET remaining_reserved_balance=$2::numeric, remaining_reserved_shares=$3::numeric,
		    settled_shares=$4::numeric, settled_notional=$5::numeric, settled_fees=$6::numeric,
		    status=$7,
		    uncertain_reason=CASE WHEN $7='RECONCILIATION_REQUIRED' THEN uncertain_reason ELSE '' END,
		    last_venue_observed_at=$8::timestamptz,
		    revision=revision+1, updated_at=$9::timestamptz,
		    released_at=CASE WHEN $10::boolean THEN $9::timestamptz ELSE NULL::timestamptz END
		WHERE order_id=$1 AND revision=$11`,
		reservation.OrderID, targetBalance.String(), targetShares.String(), cumulativeShares.String(),
		cumulativeNotional.String(), cumulativeFees.String(), string(status), nullTime(fill.VenueUpdatedAt),
		now, terminal, reservation.Revision)
	if err != nil {
		return fmt.Errorf("update fill reservation: %w", err)
	}
	if !oneRow(result) {
		return port.ErrReservationConflict
	}
	reservation.RemainingReservedBalance = targetBalance
	reservation.RemainingReservedShares = targetShares
	reservation.SettledShares = cumulativeShares
	reservation.SettledNotional = cumulativeNotional
	reservation.SettledFees = cumulativeFees
	reservation.Status = status
	reservation.UpdatedAt = now
	reservation.Revision++
	if terminal {
		reservation.ReleasedAt = &now
	}
	eventKey := fmt.Sprintf("fill:%s:%s:%s:%s", fill.Key, cumulativeShares, cumulativeNotional, cumulativeFees)
	details := ""
	if sign, _ := unfilledShares.Sign(); terminal && sign > 0 {
		details = "terminal order released unfilled remainder"
	}
	eventType := "RECONCILED"
	if status == domain.ReservationStatusReleased {
		eventType = "RELEASED"
	}
	return insertEvent(ctx, tx, *reservation, eventKey, eventType, string(target), details)
}

// lotBalance 表示后端使用的 lotBalance 类型。
type lotBalance struct {
	id       string
	account  string
	market   string
	token    string
	origin   string
	model    string
	strategy string
	shares   domain.Decimal
	cost     domain.Decimal
}

// lockTargetSellLot 在当前事务中锁定 Target Sell Lot。
func lockTargetSellLot(ctx context.Context, tx *sql.Tx, order domain.Order, before domain.Position) (lotBalance, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT lot.lot_id, lot.execution_account_id, lot.market_id, lot.token_id,
		       lot.model_id, COALESCE(route.logical_model_id, lot.model_id), lot.strategy_id,
		       lot.remaining_shares::text, lot.remaining_cost::text
		FROM position_lots AS lot
		LEFT JOIN position_lot_model_routes AS route ON route.lot_id=lot.lot_id
		WHERE lot.execution_account_id=$1 AND lot.token_id=$2 AND lot.status='OPEN'
		ORDER BY lot.opened_at, lot.lot_id FOR UPDATE OF lot`, order.Intent.ExecutionAccountID, order.Intent.TokenID)
	if err != nil {
		return lotBalance{}, fmt.Errorf("lock open position lots: %w", err)
	}
	var lots []lotBalance
	for rows.Next() {
		var lot lotBalance
		if err := rows.Scan(&lot.id, &lot.account, &lot.market, &lot.token, &lot.origin, &lot.model, &lot.strategy, &lot.shares, &lot.cost); err != nil {
			rows.Close()
			return lotBalance{}, err
		}
		lots = append(lots, lot)
	}
	if err := rows.Close(); err != nil {
		return lotBalance{}, err
	}
	if len(lots) == 0 {
		return lotBalance{}, reject("POSITION_LOTS_MISSING", "position has shares but no source lots; backfill is required before live SELL settlement")
	}
	lotShares, lotCost, err := sumLotBalances(ctx, tx, lots)
	if err != nil {
		return lotBalance{}, err
	}
	if !lotShares.Equal(before.TotalShares) || !lotCost.Equal(before.CostBasis) {
		return lotBalance{}, reject("POSITION_LOT_SNAPSHOT_MISMATCH", "open lots do not reconcile with the authoritative position snapshot")
	}
	for _, lot := range lots {
		if lot.id == order.Intent.TargetLotID {
			if lot.account != order.Intent.ExecutionAccountID || lot.market != order.Intent.MarketID || lot.token != order.Intent.TokenID ||
				lot.model != order.Intent.ModelID || domain.CanonicalStrategyID(lot.strategy) != domain.CanonicalStrategyID(order.Intent.StrategyID) {
				return lotBalance{}, reject("TARGET_LOT_IDENTITY_MISMATCH", "selected lot does not belong to the SELL order identity")
			}
			return lot, nil
		}
	}
	return lotBalance{}, reject("TARGET_LOT_NOT_FOUND", "selected lot is not open")
}

// closeTargetLot 按卖出成交数量和成本关闭指定持仓批次。
func closeTargetLot(ctx context.Context, tx *sql.Tx, fill domain.Fill, lot lotBalance, allocatedCost domain.Decimal) error {
	newShares, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, lot.shares.String(), fill.Shares.String())
	if err != nil {
		return err
	}
	newCost, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, lot.cost.String(), allocatedCost.String())
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
		WHERE lot_id=$1 AND remaining_shares=$6::numeric AND remaining_cost=$7::numeric`,
		lot.id, newShares.String(), closed, newCost.String(), fill.ObservedAt, lot.shares.String(), lot.cost.String())
	if err != nil {
		return fmt.Errorf("close target position lot: %w", err)
	}
	if !oneRow(result) {
		return port.ErrFillConflict
	}
	realized, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, fill.NetCashDelta.String(), allocatedCost.String())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO position_lot_closures (
			closure_id, closing_fill_key, lot_id, closed_shares, allocated_cost,
			allocated_net_proceeds, realized_pnl, closed_at
		) VALUES ($1,$2,$3,$4::numeric,$5::numeric,$6::numeric,$7::numeric,$8::timestamptz)`,
		"closure:"+fill.Key+":"+lot.id, fill.Key, lot.id, fill.Shares.String(), allocatedCost.String(),
		fill.NetCashDelta.String(), realized.String(), fill.ObservedAt)
	if err != nil {
		return fmt.Errorf("insert position lot closure: %w", err)
	}
	return nil
}

// sumLotBalances 汇总 Lot Balances。
func sumLotBalances(ctx context.Context, tx *sql.Tx, lots []lotBalance) (domain.Decimal, domain.Decimal, error) {
	shares, cost := domain.Decimal("0"), domain.Decimal("0")
	var err error
	for _, lot := range lots {
		shares, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, shares.String(), lot.shares.String())
		if err != nil {
			return "", "", err
		}
		cost, err = numeric(ctx, tx, `SELECT ($1::numeric+$2::numeric)::text`, cost.String(), lot.cost.String())
		if err != nil {
			return "", "", err
		}
	}
	return shares, cost, nil
}

// positionEventForFill 根据成交前后仓位构建不可变仓位事件。
func positionEventForFill(order domain.Order, fill domain.Fill, after domain.Position, sharesDelta, cashDelta, costDelta, realizedDelta domain.Decimal) domain.PositionEvent {
	return domain.PositionEvent{
		EventID: "position-event:" + fill.Key, ExecutionAccountID: fill.ExecutionAccountID,
		MarketID: fill.MarketID, TokenID: fill.TokenID, OrderID: order.ID, FillKey: fill.Key,
		ModelID: order.Intent.ModelID, StrategyID: order.Intent.StrategyID,
		SharesDelta: sharesDelta, CashDelta: cashDelta, CostBasisDelta: costDelta,
		RealizedPnLDelta: realizedDelta, SharesAfter: after.TotalShares, CostBasisAfter: after.CostBasis,
		AverageCostAfter: after.AverageCostPrice, RealizedPnLAfter: after.RealizedPnL,
		MarkPrice: after.MarkPrice, UnrealizedPnLAfter: after.UnrealizedPnL, OccurredAt: fill.ObservedAt,
	}
}

// insertPositionEvent 在当前事务中插入 Position Event。
func insertPositionEvent(ctx context.Context, tx *sql.Tx, event domain.PositionEvent) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO position_events (
			position_event_id, event_type, execution_account_id, market_id, token_id,
			order_id, fill_key, model_id, strategy_id, shares_delta, cash_delta,
			cost_basis_delta, realized_pnl_delta, shares_after, cost_basis_after,
			average_cost_after, realized_pnl_after, mark_price, unrealized_pnl_after, occurred_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::numeric,$11::numeric,$12::numeric,$13::numeric,
			$14::numeric,$15::numeric,$16::numeric,$17::numeric,NULLIF($18,'')::numeric,$19::numeric,$20)`,
		event.EventID, string(event.EventType), event.ExecutionAccountID, event.MarketID, event.TokenID,
		event.OrderID, event.FillKey, event.ModelID, event.StrategyID, event.SharesDelta.String(),
		event.CashDelta.String(), event.CostBasisDelta.String(), event.RealizedPnLDelta.String(),
		event.SharesAfter.String(), event.CostBasisAfter.String(), event.AverageCostAfter.String(),
		event.RealizedPnLAfter.String(), event.MarkPrice.String(), event.UnrealizedPnLAfter.String(), event.OccurredAt)
	return err
}

const positionSelect = `
	SELECT execution_account_id, market_id, condition_id, token_id, outcome_index, outcome_name,
	       total_shares::text, available_shares::text, reserved_shares::text,
	       cost_basis::text, average_cost_price::text, realized_pnl::text,
	       COALESCE(mark_price::text,''), market_value::text, unrealized_pnl::text,
	       is_dust, lifecycle_status, last_marked_at, updated_at, version
	FROM execution_positions`

// selectPosition 从 PostgreSQL 查询 Position。
func selectPosition(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, accountID, tokenID string, forUpdate bool) (domain.Position, error) {
	statement := positionSelect + ` WHERE execution_account_id=$1 AND token_id=$2`
	if forUpdate {
		statement += ` FOR UPDATE`
	}
	return scanPosition(query.QueryRowContext(ctx, statement, accountID, tokenID))
}

// scanPosition 将数据库行扫描为 Position。
func scanPosition(row rowScanner) (domain.Position, error) {
	var position domain.Position
	var outcome sql.NullInt16
	var marked sql.NullTime
	err := row.Scan(
		&position.ExecutionAccountID, &position.MarketID, &position.ConditionID, &position.TokenID,
		&outcome, &position.OutcomeName, &position.TotalShares, &position.AvailableShares,
		&position.ReservedShares, &position.CostBasis, &position.AverageCostPrice, &position.RealizedPnL,
		&position.MarkPrice, &position.MarketValue, &position.UnrealizedPnL, &position.IsDust,
		&position.LifecycleStatus, &marked, &position.UpdatedAt, &position.Revision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Position{}, port.ErrPositionNotFound
	}
	if err != nil {
		return domain.Position{}, err
	}
	if outcome.Valid {
		value := int(outcome.Int16)
		position.OutcomeIndex = &value
	}
	if marked.Valid {
		value := marked.Time.UTC()
		position.LastMarkedAt = &value
	}
	position.UpdatedAt = position.UpdatedAt.UTC()
	return position, nil
}

// GetFill 按成交幂等键查询成交记录。
func (ledger *FillLedger) GetFill(ctx context.Context, fillKey string) (domain.Fill, error) {
	return scanFill(ledger.db.QueryRowContext(ctx, fillSelect+` WHERE fill_key=$1`, fillKey))
}

// ListOrderFills 查询并返回指定订单的真实成交分量。
func (ledger *FillLedger) ListOrderFills(ctx context.Context, orderID string) ([]domain.Fill, error) {
	rows, err := ledger.db.QueryContext(ctx, fillSelect+` WHERE order_id=$1 ORDER BY matched_at, venue_fill_id, fill_key`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fills []domain.Fill
	for rows.Next() {
		fill, err := scanFill(rows)
		if err != nil {
			return nil, err
		}
		fills = append(fills, fill)
	}
	return fills, rows.Err()
}

// GetPosition 查询指定执行账户和 token 的权威仓位。
func (ledger *FillLedger) GetPosition(ctx context.Context, accountID, tokenID string) (domain.Position, error) {
	return selectPosition(ctx, ledger.db, accountID, tokenID, false)
}

// ListLots 查询指定账户和 token 的持仓批次。
func (ledger *FillLedger) ListLots(ctx context.Context, accountID, tokenID string) ([]domain.PositionLot, error) {
	rows, err := ledger.db.QueryContext(ctx, `
		SELECT lot.lot_id, lot.execution_account_id, lot.market_id, lot.condition_id, lot.token_id,
		       lot.outcome_index, lot.outcome_name, lot.neg_risk,
		       lot.model_id, COALESCE(route.logical_model_id, lot.model_id), lot.strategy_id,
		       COALESCE(lot.opening_order_id,'external-adoption:'||lot.external_adoption_id),
		       COALESCE(lot.opening_fill_key,'external-adoption:'||lot.external_adoption_id),
		       lot.original_shares::text, lot.remaining_shares::text,
		       lot.original_cost::text, lot.remaining_cost::text, lot.average_entry_price::text,
		       lot.status, lot.opened_at, lot.closed_at
		FROM position_lots AS lot
		LEFT JOIN position_lot_model_routes AS route ON route.lot_id=lot.lot_id
		WHERE lot.execution_account_id=$1 AND lot.token_id=$2
		ORDER BY lot.opened_at, lot.lot_id`, accountID, tokenID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lots []domain.PositionLot
	for rows.Next() {
		var lot domain.PositionLot
		var status string
		var closed sql.NullTime
		var outcome sql.NullInt64
		var negRisk sql.NullBool
		if err := rows.Scan(&lot.LotID, &lot.ExecutionAccountID, &lot.MarketID, &lot.ConditionID, &lot.TokenID,
			&outcome, &lot.OutcomeName, &negRisk, &lot.OriginModelID, &lot.ModelID, &lot.StrategyID, &lot.OpeningOrderID, &lot.OpeningFillKey,
			&lot.OriginalShares, &lot.RemainingShares, &lot.OriginalCost, &lot.RemainingCost,
			&lot.AverageEntryPrice, &status, &lot.OpenedAt, &closed); err != nil {
			return nil, err
		}
		if outcome.Valid {
			value := int(outcome.Int64)
			lot.OutcomeIndex = &value
		}
		if negRisk.Valid {
			value := negRisk.Bool
			lot.NegRisk = &value
		}
		lot.Status = domain.PositionLotStatus(status)
		lot.OpenedAt = lot.OpenedAt.UTC()
		if closed.Valid {
			value := closed.Time.UTC()
			lot.ClosedAt = &value
		}
		lots = append(lots, lot)
	}
	return lots, rows.Err()
}

// ListOpenLots 查询指定账户所有未关闭的持仓批次。
func (ledger *FillLedger) ListOpenLots(ctx context.Context, accountID string) ([]domain.PositionLot, error) {
	rows, err := ledger.db.QueryContext(ctx, `
		SELECT lot.lot_id, lot.execution_account_id, lot.market_id, lot.condition_id, lot.token_id,
		       lot.outcome_index, lot.outcome_name, lot.neg_risk,
		       lot.model_id, COALESCE(route.logical_model_id, lot.model_id), lot.strategy_id,
		       COALESCE(lot.opening_order_id,'external-adoption:'||lot.external_adoption_id),
		       COALESCE(lot.opening_fill_key,'external-adoption:'||lot.external_adoption_id),
		       lot.original_shares::text, lot.remaining_shares::text,
		       lot.original_cost::text, lot.remaining_cost::text, lot.average_entry_price::text,
		       lot.status, lot.opened_at, lot.closed_at
		FROM position_lots AS lot
		LEFT JOIN position_lot_model_routes AS route ON route.lot_id=lot.lot_id
		WHERE lot.execution_account_id=$1 AND lot.status='OPEN'
		ORDER BY lot.opened_at, lot.lot_id`, accountID)
	if err != nil {
		return nil, fmt.Errorf("query open position lots: %w", err)
	}
	defer rows.Close()
	var lots []domain.PositionLot
	for rows.Next() {
		lot, err := scanPositionLot(rows)
		if err != nil {
			return nil, err
		}
		lots = append(lots, lot)
	}
	return lots, rows.Err()
}

// ListOpenPositionExitTrades 查询可供退出策略评估的开放持仓批次和预占。
func (ledger *FillLedger) ListOpenPositionExitTrades(ctx context.Context, accountID string) ([]domain.PositionExitTrade, error) {
	rows, err := ledger.db.QueryContext(ctx, `
		SELECT lot.lot_id,
		       COALESCE(fill.venue_fill_id,'external-adoption:'||lot.external_adoption_id),
		       COALESCE(lot.opening_order_id,'external-adoption:'||lot.external_adoption_id),
		       lot.market_id, lot.condition_id, lot.outcome_index, lot.outcome_name,
		       lot.token_id, lot.neg_risk, lot.opened_at,
		       lot.original_shares::text, lot.remaining_shares::text,
		       CASE WHEN legacy.token_id IS NOT NULL THEN 0
		            ELSE lot.remaining_shares - COALESCE(reserved.shares, 0) END::text,
		       CASE WHEN legacy.token_id IS NOT NULL THEN lot.remaining_shares
		            ELSE COALESCE(reserved.shares, 0) END::text,
		       lot.average_entry_price::text, lot.remaining_cost::text,
		       lot.model_id, COALESCE(route.logical_model_id, lot.model_id),
		       lot.strategy_id, lot.execution_account_id
		FROM position_lots AS lot
		LEFT JOIN execution_fills AS fill ON fill.fill_key = lot.opening_fill_key
		LEFT JOIN position_lot_model_routes AS route ON route.lot_id=lot.lot_id
		LEFT JOIN (
			SELECT execution_account_id, target_lot_id,
			       SUM(remaining_reserved_shares) AS shares
			FROM asset_reservations
			WHERE side = 'SELL'
			  AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
			GROUP BY execution_account_id, target_lot_id
		) AS reserved
		  ON reserved.execution_account_id = lot.execution_account_id
		 AND reserved.target_lot_id = lot.lot_id
		LEFT JOIN (
			SELECT execution_account_id, token_id
			FROM asset_reservations
			WHERE side = 'SELL' AND target_lot_id IS NULL
			  AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
			GROUP BY execution_account_id, token_id
		) AS legacy
		  ON legacy.execution_account_id = lot.execution_account_id
		 AND legacy.token_id = lot.token_id
		WHERE lot.execution_account_id = $1 AND lot.status = 'OPEN'
		ORDER BY lot.opened_at, lot.lot_id`, accountID)
	if err != nil {
		return nil, fmt.Errorf("query open position exit trades: %w", err)
	}
	defer rows.Close()
	var trades []domain.PositionExitTrade
	for rows.Next() {
		var trade domain.PositionExitTrade
		var outcome sql.NullInt64
		var negRisk sql.NullBool
		if err := rows.Scan(
			&trade.LotID, &trade.VenueTradeID, &trade.OpeningOrderID,
			&trade.MarketID, &trade.ConditionID, &outcome, &trade.OutcomeName,
			&trade.TokenID, &negRisk, &trade.EnteredAt,
			&trade.OriginalShares, &trade.RemainingShares, &trade.AvailableShares,
			&trade.ReservedShares, &trade.EntryPrice, &trade.RemainingCost,
			&trade.OriginModelID, &trade.ModelID, &trade.StrategyID, &trade.ExecutionAccountID,
		); err != nil {
			return nil, fmt.Errorf("scan position exit trade: %w", err)
		}
		if !outcome.Valid || !negRisk.Valid {
			return nil, fmt.Errorf("position lot %q is missing outcome_index or neg_risk", trade.LotID)
		}
		trade.OutcomeIndex = int(outcome.Int64)
		trade.NegRisk = negRisk.Bool
		trade.EnteredAt = trade.EnteredAt.UTC()
		trades = append(trades, trade)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate position exit trades: %w", err)
	}
	return trades, nil
}

// scanPositionLot 将数据库行扫描为 Position Lot。
func scanPositionLot(row rowScanner) (domain.PositionLot, error) {
	var lot domain.PositionLot
	var status string
	var outcome sql.NullInt64
	var negRisk sql.NullBool
	var closed sql.NullTime
	if err := row.Scan(&lot.LotID, &lot.ExecutionAccountID, &lot.MarketID, &lot.ConditionID, &lot.TokenID,
		&outcome, &lot.OutcomeName, &negRisk, &lot.OriginModelID, &lot.ModelID, &lot.StrategyID, &lot.OpeningOrderID, &lot.OpeningFillKey,
		&lot.OriginalShares, &lot.RemainingShares, &lot.OriginalCost, &lot.RemainingCost,
		&lot.AverageEntryPrice, &status, &lot.OpenedAt, &closed); err != nil {
		return domain.PositionLot{}, fmt.Errorf("scan position lot: %w", err)
	}
	lot.Status = domain.PositionLotStatus(status)
	lot.OpenedAt = lot.OpenedAt.UTC()
	if outcome.Valid {
		value := int(outcome.Int64)
		lot.OutcomeIndex = &value
	}
	if negRisk.Valid {
		value := negRisk.Bool
		lot.NegRisk = &value
	}
	if closed.Valid {
		value := closed.Time.UTC()
		lot.ClosedAt = &value
	}
	return lot, nil
}

// ListPositionEvents 查询指定仓位的不可变事件列表。
func (ledger *FillLedger) ListPositionEvents(ctx context.Context, accountID, tokenID string) ([]domain.PositionEvent, error) {
	rows, err := ledger.db.QueryContext(ctx, `
		SELECT position_event_id, event_type, execution_account_id, market_id, token_id,
		       order_id, fill_key, model_id, strategy_id, shares_delta::text, cash_delta::text,
		       cost_basis_delta::text, realized_pnl_delta::text, shares_after::text,
		       cost_basis_after::text, average_cost_after::text, realized_pnl_after::text,
		       COALESCE(mark_price::text,''), unrealized_pnl_after::text, occurred_at
		FROM position_events WHERE execution_account_id=$1 AND token_id=$2
		ORDER BY occurred_at, position_event_id`, accountID, tokenID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []domain.PositionEvent
	for rows.Next() {
		var event domain.PositionEvent
		var eventType string
		if err := rows.Scan(&event.EventID, &eventType, &event.ExecutionAccountID, &event.MarketID,
			&event.TokenID, &event.OrderID, &event.FillKey, &event.ModelID, &event.StrategyID,
			&event.SharesDelta, &event.CashDelta, &event.CostBasisDelta, &event.RealizedPnLDelta,
			&event.SharesAfter, &event.CostBasisAfter, &event.AverageCostAfter,
			&event.RealizedPnLAfter, &event.MarkPrice, &event.UnrealizedPnLAfter, &event.OccurredAt); err != nil {
			return nil, err
		}
		event.EventType = domain.PositionEventType(eventType)
		event.OccurredAt = event.OccurredAt.UTC()
		events = append(events, event)
	}
	return events, rows.Err()
}

// MarkPosition 使用最新标记价格更新仓位未实现盈亏。
func (ledger *FillLedger) MarkPosition(ctx context.Context, mark domain.PositionMark) (domain.Position, error) {
	if mark.ExecutionAccountID == "" || mark.TokenID == "" || mark.ObservedAt.IsZero() {
		return domain.Position{}, fmt.Errorf("execution_account_id, token_id, and observed_at are required")
	}
	if sign, err := mark.Price.Sign(); err != nil || sign <= 0 {
		return domain.Position{}, fmt.Errorf("mark price must be positive")
	}
	if comparison, err := mark.Price.Compare("1"); err != nil || comparison > 0 {
		return domain.Position{}, fmt.Errorf("mark price must not exceed 1")
	}
	tx, err := ledger.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return domain.Position{}, err
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, mark.ExecutionAccountID); err != nil {
		return domain.Position{}, err
	}
	before, err := selectPosition(ctx, tx, mark.ExecutionAccountID, mark.TokenID, true)
	if err != nil {
		return domain.Position{}, err
	}
	if before.LastMarkedAt != nil && mark.ObservedAt.Before(*before.LastMarkedAt) {
		return before, tx.Commit()
	}
	if before.LastMarkedAt != nil && mark.ObservedAt.Equal(*before.LastMarkedAt) {
		if !before.MarkPrice.Equal(mark.Price) {
			return domain.Position{}, port.ErrFillConflict
		}
		return before, tx.Commit()
	}
	sharesThreshold := decimalOrZero(ledger.dustSharesThreshold)
	notionalThreshold := decimalOrZero(ledger.dustNotionalThreshold)
	_, err = tx.ExecContext(ctx, `
		UPDATE execution_positions
		SET mark_price=$3::numeric, market_value=total_shares*$3::numeric,
		    unrealized_pnl=total_shares*$3::numeric-cost_basis,
		    is_dust=total_shares>0 AND (total_shares<=$4::numeric OR total_shares*$3::numeric<=$5::numeric),
		    last_marked_at=$6, updated_at=$6, version=version+1
		WHERE execution_account_id=$1 AND token_id=$2`, mark.ExecutionAccountID, mark.TokenID,
		mark.Price.String(), sharesThreshold, notionalThreshold, mark.ObservedAt.UTC())
	if err != nil {
		return domain.Position{}, err
	}
	after, err := selectPosition(ctx, tx, mark.ExecutionAccountID, mark.TokenID, false)
	if err != nil {
		return domain.Position{}, err
	}
	event := domain.PositionEvent{
		EventID:   fmt.Sprintf("position-mark:%s:%s:%d", mark.ExecutionAccountID, mark.TokenID, mark.ObservedAt.UnixNano()),
		EventType: domain.PositionEventMarked, ExecutionAccountID: after.ExecutionAccountID,
		MarketID: after.MarketID, TokenID: after.TokenID, SharesDelta: "0", CashDelta: "0",
		CostBasisDelta: "0", RealizedPnLDelta: "0", SharesAfter: after.TotalShares,
		CostBasisAfter: after.CostBasis, AverageCostAfter: after.AverageCostPrice,
		RealizedPnLAfter: after.RealizedPnL, MarkPrice: after.MarkPrice,
		UnrealizedPnLAfter: after.UnrealizedPnL, OccurredAt: mark.ObservedAt.UTC(),
	}
	if err := insertPositionEvent(ctx, tx, event); err != nil {
		return domain.Position{}, err
	}
	if _, err := insertOutbox(ctx, tx, "trading.position.marked.v1", event.EventID,
		mark.ExecutionAccountID+":"+mark.TokenID, event, event.OccurredAt); err != nil {
		return domain.Position{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Position{}, err
	}
	return after, nil
}

// ListPositions 查询指定执行账户的权威仓位列表。
func (ledger *FillLedger) ListPositions(ctx context.Context, accountID string) ([]domain.Position, error) {
	rows, err := ledger.db.QueryContext(ctx, positionSelect+`
		WHERE execution_account_id=$1
		ORDER BY condition_id, outcome_index, token_id`, strings.TrimSpace(accountID))
	if err != nil {
		return nil, fmt.Errorf("query reconciliation positions: %w", err)
	}
	defer rows.Close()
	positions := make([]domain.Position, 0)
	for rows.Next() {
		position, err := scanPosition(rows)
		if err != nil {
			return nil, err
		}
		positions = append(positions, position)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reconciliation positions: %w", err)
	}
	return positions, nil
}

// MarkPositionSettled 在证据充分时将开放仓位标记为待赎回结算。
func (ledger *FillLedger) MarkPositionSettled(
	ctx context.Context,
	accountID, tokenID, sourceReference string,
	observedAt time.Time,
) (domain.Position, error) {
	accountID = strings.TrimSpace(accountID)
	tokenID = strings.TrimSpace(tokenID)
	sourceReference = strings.TrimSpace(sourceReference)
	if accountID == "" || tokenID == "" || sourceReference == "" || observedAt.IsZero() {
		return domain.Position{}, fmt.Errorf("account, token, settlement source, and observed_at are required")
	}
	tx, err := ledger.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.Position{}, fmt.Errorf("begin position settlement mark: %w", err)
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, accountID); err != nil {
		return domain.Position{}, err
	}
	before, err := selectPosition(ctx, tx, accountID, tokenID, true)
	if err != nil {
		return domain.Position{}, err
	}
	if before.LifecycleStatus == domain.PositionLifecycleSettledPendingRedeem {
		if err := tx.Commit(); err != nil {
			return domain.Position{}, err
		}
		return before, nil
	}
	if before.LifecycleStatus != domain.PositionLifecycleOpen {
		return domain.Position{}, reject("POSITION_SETTLEMENT_STATE_INVALID", "only an OPEN position can be marked settled")
	}
	if sign, err := before.ReservedShares.Sign(); err != nil || sign != 0 {
		return domain.Position{}, reject("POSITION_SETTLEMENT_HAS_ACTIVE_ORDER", "settlement cannot be marked while shares are reserved")
	}
	observedAt = observedAt.UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_positions
		SET lifecycle_status='SETTLED_PENDING_REDEEM', version=version+1, updated_at=$3
		WHERE execution_account_id=$1 AND token_id=$2 AND lifecycle_status='OPEN'`, accountID, tokenID, observedAt)
	if err != nil {
		return domain.Position{}, fmt.Errorf("mark position settled: %w", err)
	}
	if !oneRow(result) {
		return domain.Position{}, port.ErrPositionNotFound
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE position_lots
		SET status='SETTLED_PENDING_REDEEM'
		WHERE execution_account_id=$1 AND token_id=$2 AND status='OPEN'`, accountID, tokenID); err != nil {
		return domain.Position{}, fmt.Errorf("mark position lots settled: %w", err)
	}
	after, err := selectPosition(ctx, tx, accountID, tokenID, false)
	if err != nil {
		return domain.Position{}, err
	}
	event := domain.PositionEvent{
		EventID:   "position-settled:" + accountID + ":" + tokenID + ":" + sourceReference,
		EventType: domain.PositionEventSettled, ExecutionAccountID: accountID,
		MarketID: after.MarketID, TokenID: tokenID, FillKey: "settlement:" + sourceReference,
		SharesDelta: "0", CashDelta: "0", CostBasisDelta: "0", RealizedPnLDelta: "0",
		SharesAfter: after.TotalShares, CostBasisAfter: after.CostBasis,
		AverageCostAfter: after.AverageCostPrice, RealizedPnLAfter: after.RealizedPnL,
		MarkPrice: after.MarkPrice, UnrealizedPnLAfter: after.UnrealizedPnL, OccurredAt: observedAt,
	}
	if err := insertPositionEvent(ctx, tx, event); err != nil {
		return domain.Position{}, fmt.Errorf("record position settlement: %w", err)
	}
	if _, err := insertOutbox(ctx, tx, "trading.position.settled.v1", event.EventID,
		accountID+":"+tokenID, event, observedAt); err != nil {
		return domain.Position{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Position{}, fmt.Errorf("commit position settlement mark: %w", err)
	}
	return after, nil
}

// GetBalance 查询指定执行账户的权威资金余额。
func (ledger *FillLedger) GetBalance(ctx context.Context, accountID string) (domain.AccountBalance, error) {
	var balance domain.AccountBalance
	var reconciled sql.NullTime
	err := ledger.db.QueryRowContext(ctx, `
		SELECT execution_account_id, wallet_address, collateral_asset,
		       total_balance::text, available_balance::text, reserved_balance::text,
		       reconciled_at, updated_at, version
		FROM execution_accounts WHERE execution_account_id=$1`, accountID).Scan(
		&balance.ExecutionAccountID, &balance.WalletAddress, &balance.CollateralAsset,
		&balance.TotalBalance, &balance.AvailableBalance, &balance.ReservedBalance,
		&reconciled, &balance.UpdatedAt, &balance.Revision,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AccountBalance{}, port.ErrAccountNotFound
	}
	if err != nil {
		return domain.AccountBalance{}, err
	}
	if reconciled.Valid {
		value := reconciled.Time.UTC()
		balance.ReconciledAt = &value
	}
	balance.UpdatedAt = balance.UpdatedAt.UTC()
	return balance, nil
}

// ListAccountEvents 查询指定执行账户的资金事件列表。
func (ledger *FillLedger) ListAccountEvents(ctx context.Context, accountID string) ([]domain.AccountEvent, error) {
	rows, err := ledger.db.QueryContext(ctx, `
		SELECT account_event_id, execution_account_id, event_type, order_id, fill_key,
		       total_balance_delta::text, available_balance_delta::text, reserved_balance_delta::text,
		       total_balance_after::text, available_balance_after::text, reserved_balance_after::text,
		       occurred_at
		FROM execution_account_events WHERE execution_account_id=$1
		ORDER BY occurred_at, account_event_id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []domain.AccountEvent
	for rows.Next() {
		var event domain.AccountEvent
		if err := rows.Scan(&event.EventID, &event.ExecutionAccountID, &event.EventType,
			&event.OrderID, &event.FillKey, &event.TotalBalanceDelta, &event.AvailableDelta,
			&event.ReservedDelta, &event.TotalBalanceAfter, &event.AvailableAfter,
			&event.ReservedAfter, &event.OccurredAt); err != nil {
			return nil, err
		}
		event.OccurredAt = event.OccurredAt.UTC()
		events = append(events, event)
	}
	return events, rows.Err()
}

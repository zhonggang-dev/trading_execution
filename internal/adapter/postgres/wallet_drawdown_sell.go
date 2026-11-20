package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
)

// ListDueSells reads open lots from fully filled, audited drawdown BUYs.
func (s *WalletDrawdownStore) ListDueSells(ctx context.Context, boughtBefore, retryBefore time.Time) ([]walletdrawdown.DueSell, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT op.slot_at, op.client_order_id, COALESCE(prior.attempt, 0),
		       lot.lot_id, lot.market_id, lot.condition_id, lot.token_id,
		       lot.outcome_index, lot.outcome_name, lot.neg_risk, lot.remaining_shares::text,
		       buy.filled_size::text, (buy.filled_notional + buy.total_fees)::text
		FROM wallet_drawdown_order_operations op
		JOIN execution_orders buy USING (client_order_id, execution_account_id)
		JOIN position_lots lot ON lot.opening_order_id = buy.order_id
		LEFT JOIN LATERAL (
		    SELECT sell.attempt, sell.recorded_at, ord.status
		    FROM wallet_drawdown_order_operations sell
		    LEFT JOIN execution_orders ord USING (client_order_id, execution_account_id)
		    WHERE sell.target_lot_id = lot.lot_id
		    ORDER BY sell.attempt DESC
		    LIMIT 1
		) prior ON TRUE
		WHERE op.execution_account_id = 'main' AND op.side = 'BUY'
		  AND buy.venue = 'polymarket' AND buy.status = 'FILLED' AND buy.filled_size > 0
		  AND lot.status = 'OPEN' AND lot.remaining_shares > 0
		  AND EXISTS (
		    SELECT 1 FROM execution_fills f WHERE f.order_id = buy.order_id
		      AND f.status = 'CONFIRMED' AND f.applied_at IS NOT NULL
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM execution_fills f WHERE f.order_id = buy.order_id
		      AND f.status <> 'FAILED'
		      AND (f.status <> 'CONFIRMED' OR f.applied_at IS NULL OR f.matched_at > $1)
		  )
		  AND (prior.attempt IS NULL OR (prior.status = 'REJECTED' AND prior.recorded_at <= $2))
		ORDER BY op.slot_at, lot.opened_at, lot.lot_id`, boughtBefore, retryBefore)
	if err != nil {
		return nil, fmt.Errorf("list due drawdown sell lots: %w", err)
	}
	defer rows.Close()
	var result []walletdrawdown.DueSell
	for rows.Next() {
		var sell walletdrawdown.DueSell
		lot := &sell.Lot
		if err := rows.Scan(&sell.SlotAt, &sell.OpeningClientOrderID, &sell.PreviousAttempt,
			&lot.LotID, &lot.MarketID, &lot.ConditionID, &lot.TokenID, &lot.OutcomeIndex,
			&lot.OutcomeName, &lot.NegRisk, &lot.RemainingShares, &sell.BoughtShares, &sell.BoughtCost); err != nil {
			return nil, fmt.Errorf("read due drawdown sell lot: %w", err)
		}
		result = append(result, sell)
	}
	return result, rows.Err()
}

// ClaimSell records a single new attempt per lot and assigns the next audit
// ordinal for the opening sample. Concurrent claims can retry on the next scan.
func (s *WalletDrawdownStore) ClaimSell(ctx context.Context, due walletdrawdown.DueSell, intent domain.OrderIntent) (bool, error) {
	var claimed string
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO wallet_drawdown_order_operations
		    (client_order_id, execution_account_id, slot_at, attempt, market_id, condition_id,
		     token_id, side, time_in_force, price, size, opening_client_order_id, target_lot_id)
		SELECT $1, $2, $3, COALESCE(MAX(attempt), 0) + 1, $4, $5, $6, 'SELL', $7, $8::numeric, $9::numeric, $10, $11
		FROM wallet_drawdown_order_operations
		WHERE execution_account_id = $2 AND slot_at = $3
		HAVING EXISTS (
		    SELECT 1 FROM position_lots lot
		    WHERE lot.lot_id = $11 AND lot.execution_account_id = $2 AND lot.status = 'OPEN'
		      AND lot.remaining_shares = $9::numeric
		)
		ON CONFLICT DO NOTHING
		RETURNING client_order_id`,
		intent.ClientOrderID, intent.ExecutionAccountID, due.SlotAt, intent.MarketID, intent.ConditionID,
		intent.TokenID, intent.TimeInForce, intent.WorstPrice.String(), intent.Size.String(),
		due.OpeningClientOrderID, intent.TargetLotID).Scan(&claimed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim drawdown sell for lot %s: %w", intent.TargetLotID, err)
	}
	return true, nil
}

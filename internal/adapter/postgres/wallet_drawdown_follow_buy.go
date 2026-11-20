package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
)

func (s *WalletDrawdownStore) ListOpenSellsForBuy(ctx context.Context) ([]walletdrawdown.OpenSell, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT sell.intent, (op.size - sell.filled_size)::text
		FROM wallet_drawdown_order_operations op
		JOIN execution_orders sell USING (client_order_id, execution_account_id)
		WHERE op.execution_account_id = 'main' AND op.side = 'SELL' AND op.time_in_force = 'GTC'
		  AND op.opening_client_order_id IS NOT NULL
		  AND sell.venue = 'polymarket' AND sell.venue_order_id <> ''
		  AND sell.status IN ('LIVE', 'PARTIALLY_FILLED') AND op.size > sell.filled_size
		  AND NOT EXISTS (
		    SELECT 1 FROM execution_fills fill WHERE fill.order_id = sell.order_id
		      AND fill.status <> 'FAILED' AND fill.applied_at IS NULL
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM wallet_drawdown_order_operations buy WHERE buy.source_sell_client_order_id = op.client_order_id
		  )
		ORDER BY op.slot_at, op.attempt`)
	if err != nil {
		return nil, fmt.Errorf("list open drawdown sells for buy: %w", err)
	}
	defer rows.Close()
	var result []walletdrawdown.OpenSell
	for rows.Next() {
		var sell walletdrawdown.OpenSell
		var raw []byte
		if err := rows.Scan(&raw, &sell.Remaining); err != nil {
			return nil, fmt.Errorf("read open drawdown sell: %w", err)
		}
		if err := json.Unmarshal(raw, &sell.Intent); err != nil {
			return nil, fmt.Errorf("decode open drawdown sell intent: %w", err)
		}
		result = append(result, sell)
	}
	return result, rows.Err()
}

// ClaimFollowBuy verifies that the source is still open with the same quote
// and size in the ledger before claiming the only associated public buy.
func (s *WalletDrawdownStore) ClaimFollowBuy(ctx context.Context, sourceSellID string, intent domain.OrderIntent) (bool, error) {
	var claimed string
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO wallet_drawdown_order_operations
		    (client_order_id, execution_account_id, slot_at, attempt, market_id, condition_id,
		     token_id, side, time_in_force, price, size, source_sell_client_order_id)
		SELECT $2, $3, op.slot_at, ordinal.attempt, op.market_id, op.condition_id,
		       op.token_id, 'BUY', $4, op.price, $6::numeric, op.client_order_id
		FROM wallet_drawdown_order_operations op
		JOIN execution_orders sell USING (client_order_id, execution_account_id)
		CROSS JOIN LATERAL (
		    SELECT COALESCE(MAX(other.attempt), 0) + 1 AS attempt
		    FROM wallet_drawdown_order_operations other
		    WHERE other.execution_account_id = $3 AND other.slot_at = op.slot_at
		) ordinal
		WHERE op.client_order_id = $1 AND op.execution_account_id = 'main'
		  AND op.side = 'SELL' AND op.time_in_force = 'GTC' AND op.opening_client_order_id IS NOT NULL
		  AND sell.venue = 'polymarket' AND sell.venue_order_id <> '' AND sell.status IN ('LIVE', 'PARTIALLY_FILLED')
		  AND op.price = $5::numeric AND op.size - sell.filled_size = $6::numeric
		  AND NOT EXISTS (
		    SELECT 1 FROM execution_fills fill WHERE fill.order_id = sell.order_id
		      AND fill.status <> 'FAILED' AND fill.applied_at IS NULL
		  )
		ON CONFLICT DO NOTHING
		RETURNING client_order_id`,
		sourceSellID, intent.ClientOrderID, intent.ExecutionAccountID,
		intent.TimeInForce, intent.Price.String(), intent.Size.String()).Scan(&claimed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim drawdown buy from sell %s: %w", sourceSellID, err)
	}
	return true, nil
}

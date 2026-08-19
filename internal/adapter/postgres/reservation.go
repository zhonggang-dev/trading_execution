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

const defaultTransactionAttempts = 3

// ReservationManager 表示 PostgreSQL 中资金与份额预占的权威实现，每个事务按执行账户、预占、SELL 仓位的顺序加锁以避免死锁。
type ReservationManager struct {
	db               *sql.DB
	now              func() time.Time
	maxAttempts      int
	maxBuyFeeRateBPS domain.Decimal
}

// ReservationManagerParams 表示后端使用的 ReservationManagerParams 类型。
type ReservationManagerParams struct {
	DB               *sql.DB
	Now              func() time.Time
	MaxAttempts      int
	MaxBuyFeeRateBPS domain.Decimal
}

var _ port.AssetReservationManager = (*ReservationManager)(nil)

// NewReservationManager 创建并初始化 Reservation Manager。
func NewReservationManager(params ReservationManagerParams) (*ReservationManager, error) {
	if params.DB == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.MaxAttempts == 0 {
		params.MaxAttempts = defaultTransactionAttempts
	}
	if params.MaxAttempts < 1 {
		return nil, fmt.Errorf("transaction attempts must be positive")
	}
	if params.MaxBuyFeeRateBPS.IsEmpty() {
		return nil, fmt.Errorf("maximum BUY fee rate bps is required")
	}
	if sign, err := params.MaxBuyFeeRateBPS.Sign(); err != nil || sign < 0 {
		return nil, fmt.Errorf("maximum BUY fee rate bps must be a non-negative decimal")
	}
	return &ReservationManager{
		db: params.DB, now: params.Now, maxAttempts: params.MaxAttempts,
		maxBuyFeeRateBPS: params.MaxBuyFeeRateBPS,
	}, nil
}

// Reserve 幂等检查并原子预占订单所需的资金或份额。
func (manager *ReservationManager) Reserve(ctx context.Context, order domain.Order) (domain.AssetReservation, error) {
	order = domain.CloneOrder(order)
	fingerprint, worstPrice, err := validateReservationOrder(order)
	if err != nil {
		return domain.AssetReservation{}, err
	}
	now := manager.now().UTC()
	return manager.transact(ctx, func(tx *sql.Tx) (domain.AssetReservation, error) {
		if err := lockExecutionAccount(ctx, tx, order.Intent.ExecutionAccountID); err != nil {
			return domain.AssetReservation{}, err
		}
		reserveUnitPrice := worstPrice
		if order.Intent.Side == domain.SideBuy {
			reserveUnitPrice, err = numeric(ctx, tx, `
				SELECT ($1::numeric * (1 + $2::numeric / 10000))::text`,
				worstPrice.String(), manager.maxBuyFeeRateBPS.String())
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("calculate fee-protected BUY reserve: %w", err)
			}
		}

		existing, err := selectReservationByClientOrderID(ctx, tx, order.Intent.ClientOrderID, true)
		if err == nil {
			if err := verifyReservation(existing, order, fingerprint); err != nil {
				return domain.AssetReservation{}, err
			}
			if requiresAtomicLiveRisk(order) {
				authorization, authErr := manager.authorizeLiveRisk(
					ctx, tx, order, now, reserveUnitPrice, existing.OrderID,
				)
				if authErr != nil {
					return domain.AssetReservation{}, authErr
				}
				if existing.RiskPolicyID != authorization.policyID ||
					existing.RiskPolicyVersion != authorization.policyVersion ||
					!existing.DailyRiskNotional.Equal(authorization.dailyRiskNotional) {
					return domain.AssetReservation{}, reject("RISK_AUTHORIZATION_CHANGED", "the existing live reservation was authorized by different risk state")
				}
			}
			return existing.AssetReservation, nil
		}
		if !errors.Is(err, port.ErrReservationNotFound) {
			return domain.AssetReservation{}, err
		}
		var activeOrderID string
		if order.Intent.Side == domain.SideSell {
			err = tx.QueryRowContext(ctx, `
				SELECT order_id
				FROM asset_reservations
				WHERE execution_account_id = $1
				  AND (target_lot_id = $2 OR (target_lot_id IS NULL AND token_id = $3))
				  AND side = 'SELL'
				  AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
				LIMIT 1
				FOR UPDATE`, order.Intent.ExecutionAccountID, order.Intent.TargetLotID, order.Intent.TokenID).Scan(&activeOrderID)
		} else {
			err = tx.QueryRowContext(ctx, `
				SELECT order_id
				FROM asset_reservations
				WHERE execution_account_id = $1 AND token_id = $2 AND side = 'BUY'
				  AND status IN ('ACTIVE', 'RECONCILIATION_REQUIRED')
				LIMIT 1
				FOR UPDATE`, order.Intent.ExecutionAccountID, order.Intent.TokenID).Scan(&activeOrderID)
		}
		if err == nil {
			code := "SAME_DIRECTION_ORDER_EXISTS"
			reason := "an active order already reserves this token and direction"
			if order.Intent.Side == domain.SideSell {
				code = "DUPLICATE_SELL_ORDER"
				reason = "an active sell already reserves this position lot"
			}
			return domain.AssetReservation{}, reject(code, reason)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return domain.AssetReservation{}, fmt.Errorf("check active token reservation: %w", err)
		}
		authorization := liveRiskAuthorization{}
		if requiresAtomicLiveRisk(order) {
			authorization, err = manager.authorizeLiveRisk(ctx, tx, order, now, reserveUnitPrice, "")
			if err != nil {
				return domain.AssetReservation{}, err
			}
		}

		switch order.Intent.Side {
		case domain.SideBuy:
			result, err := tx.ExecContext(ctx, `
				UPDATE execution_accounts
				SET available_balance = available_balance - ($2::numeric * $3::numeric),
				    reserved_balance = reserved_balance + ($2::numeric * $3::numeric),
				    version = version + 1,
				    updated_at = $4
				WHERE execution_account_id = $1
				  AND available_balance >= ($2::numeric * $3::numeric)`,
				order.Intent.ExecutionAccountID, reserveUnitPrice.String(), order.Intent.Size.String(), now)
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("reserve buy balance: %w", err)
			}
			if !oneRow(result) {
				return domain.AssetReservation{}, reject("INSUFFICIENT_AVAILABLE_BALANCE", "available balance is below worst-price order notional plus the configured maximum BUY fee buffer")
			}
		case domain.SideSell:
			// Lock explicitly so concurrent SELL reservations cannot both observe
			// the same available_shares value.
			var lockedToken string
			err := tx.QueryRowContext(ctx, `
				SELECT token_id
				FROM execution_positions
				WHERE execution_account_id = $1 AND token_id = $2
				FOR UPDATE`, order.Intent.ExecutionAccountID, order.Intent.TokenID).Scan(&lockedToken)
			if errors.Is(err, sql.ErrNoRows) {
				return domain.AssetReservation{}, reject("INSUFFICIENT_AVAILABLE_SHARES", "no sellable position exists for this token")
			}
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("lock sell position: %w", err)
			}
			var lotToken, lotMarket, lotModel, lotStrategy, lotStatus string
			var lotRemaining domain.Decimal
			err = tx.QueryRowContext(ctx, `
				SELECT token_id, market_id, model_id, strategy_id, status, remaining_shares::text
				FROM position_lots
				WHERE lot_id=$1 AND execution_account_id=$2
				FOR UPDATE`, order.Intent.TargetLotID, order.Intent.ExecutionAccountID).Scan(
				&lotToken, &lotMarket, &lotModel, &lotStrategy, &lotStatus, &lotRemaining)
			if errors.Is(err, sql.ErrNoRows) {
				return domain.AssetReservation{}, reject("TARGET_LOT_NOT_FOUND", "the selected position lot does not exist in this execution account")
			}
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("lock target position lot: %w", err)
			}
			if lotStatus != string(domain.PositionLotOpen) || lotToken != order.Intent.TokenID || lotMarket != order.Intent.MarketID ||
				lotModel != order.Intent.ModelID || domain.CanonicalStrategyID(lotStrategy) != domain.CanonicalStrategyID(order.Intent.StrategyID) {
				return domain.AssetReservation{}, reject("TARGET_LOT_IDENTITY_MISMATCH", "the selected lot does not belong to this model, strategy, market, and token")
			}
			if comparison, compareErr := lotRemaining.Compare(order.Intent.Size); compareErr != nil || comparison < 0 {
				return domain.AssetReservation{}, reject("TARGET_LOT_INSUFFICIENT_SHARES", "sell size exceeds the selected lot's remaining shares")
			}
			result, err := tx.ExecContext(ctx, `
				UPDATE execution_positions
				SET available_shares = available_shares - $3::numeric,
				    reserved_shares = reserved_shares + $3::numeric,
				    version = version + 1,
				    updated_at = $4
				WHERE execution_account_id = $1 AND token_id = $2
				  AND market_id = $5
				  AND available_shares >= $3::numeric`,
				order.Intent.ExecutionAccountID, order.Intent.TokenID, order.Intent.Size.String(), now, order.Intent.MarketID)
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("reserve sell shares: %w", err)
			}
			if !oneRow(result) {
				return domain.AssetReservation{}, reject("INSUFFICIENT_AVAILABLE_SHARES", "sell size exceeds available shares")
			}
		default:
			return domain.AssetReservation{}, fmt.Errorf("unsupported reservation side %q", order.Intent.Side)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO asset_reservations (
				order_id, client_order_id, intent_fingerprint,
				execution_account_id, strategy_id, market_id, token_id, target_lot_id, side,
				requested_shares, reserve_unit_price,
				initial_reserved_balance, remaining_reserved_balance,
				initial_reserved_shares, remaining_reserved_shares,
				settled_shares, settled_notional,
				risk_policy_id, risk_policy_version, risk_day, daily_risk_notional,
				status, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9,
				$10::numeric, $11::numeric,
				CASE WHEN $9 = 'BUY' THEN $10::numeric * $11::numeric ELSE 0 END,
				CASE WHEN $9 = 'BUY' THEN $10::numeric * $11::numeric ELSE 0 END,
				CASE WHEN $9 = 'SELL' THEN $10::numeric ELSE 0 END,
				CASE WHEN $9 = 'SELL' THEN $10::numeric ELSE 0 END,
				0, 0, $13, $14, NULLIF($15, '')::date, $16::numeric,
				'ACTIVE', $12, $12
			)`,
			order.ID, order.Intent.ClientOrderID, fingerprint,
			order.Intent.ExecutionAccountID, order.Intent.StrategyID, order.Intent.MarketID,
			order.Intent.TokenID, order.Intent.TargetLotID, string(order.Intent.Side), order.Intent.Size.String(),
			reserveUnitPrice.String(), now,
			authorization.policyID, authorization.policyVersion,
			authorization.riskDay, decimalOrZero(authorization.dailyRiskNotional))
		if err != nil {
			return domain.AssetReservation{}, fmt.Errorf("insert asset reservation: %w", err)
		}
		storedReservation, err := selectReservationByOrderID(ctx, tx, order.ID, false)
		if err != nil {
			return domain.AssetReservation{}, err
		}
		reservation := storedReservation.AssetReservation
		if err := insertEvent(ctx, tx, reservation, "reserve", "RESERVED", string(order.Status), ""); err != nil {
			return domain.AssetReservation{}, err
		}
		return reservation, nil
	})
}

// Reconcile 按订单累计成交和终态结算或释放资产预占。
func (manager *ReservationManager) Reconcile(ctx context.Context, order domain.Order) (domain.AssetReservation, error) {
	order = domain.CloneOrder(order)
	fingerprint, _, err := validateSettlementOrder(order)
	if err != nil {
		return domain.AssetReservation{}, err
	}
	now := manager.now().UTC()
	return manager.transact(ctx, func(tx *sql.Tx) (domain.AssetReservation, error) {
		if err := lockExecutionAccount(ctx, tx, order.Intent.ExecutionAccountID); err != nil {
			return domain.AssetReservation{}, err
		}
		storedReservation, err := selectReservationByOrderID(ctx, tx, order.ID, true)
		if err != nil {
			return domain.AssetReservation{}, err
		}
		if err := verifyReservation(storedReservation, order, fingerprint); err != nil {
			return domain.AssetReservation{}, err
		}
		reservation := storedReservation.AssetReservation

		filled := order.FilledSize
		if filled.IsEmpty() {
			filled = "0"
		}
		if comparison, err := filled.Compare(reservation.SettledShares); err != nil || comparison < 0 {
			return domain.AssetReservation{}, fmt.Errorf("cumulative filled shares moved backward")
		}
		if comparison, err := filled.Compare(reservation.RequestedShares); err != nil || comparison > 0 {
			return domain.AssetReservation{}, fmt.Errorf("cumulative filled shares exceed requested shares")
		}
		averagePrice := order.AverageFillPrice
		if sign, _ := filled.Sign(); sign == 0 {
			averagePrice = "0"
		}

		deltaShares, cumulativeNotional, deltaNotional, remainingShares, err := settlementNumbers(
			ctx, tx, filled, reservation.SettledShares, averagePrice,
			reservation.SettledNotional, reservation.RequestedShares,
		)
		if err != nil {
			return domain.AssetReservation{}, err
		}
		if sign, _ := deltaNotional.Sign(); sign < 0 {
			return domain.AssetReservation{}, fmt.Errorf("cumulative fill notional moved backward")
		}

		terminal := order.Status == domain.OrderStatusFilled || order.Status == domain.OrderStatusCanceled || order.Status == domain.OrderStatusRejected
		if (reservation.Status == domain.ReservationStatusReleased || reservation.Status == domain.ReservationStatusSettled) && !terminal {
			return domain.AssetReservation{}, fmt.Errorf("terminal reservation cannot return to active status")
		}
		targetRemainingShares := remainingShares
		if terminal {
			targetRemainingShares = "0"
		}

		switch reservation.Side {
		case domain.SideBuy:
			targetBalance, err := numericProduct(ctx, tx, reservation.ReserveUnitPrice, targetRemainingShares)
			if err != nil {
				return domain.AssetReservation{}, err
			}
			result, err := tx.ExecContext(ctx, `
				UPDATE execution_accounts
				SET total_balance = total_balance - $4::numeric,
				    reserved_balance = reserved_balance - $2::numeric + $3::numeric,
				    available_balance = (total_balance - $4::numeric)
				        - (reserved_balance - $2::numeric + $3::numeric),
				    version = version + 1,
				    updated_at = $5
				WHERE execution_account_id = $1
				  AND reserved_balance >= $2::numeric
				  AND total_balance >= $4::numeric
				  AND (reserved_balance - $2::numeric + $3::numeric) >= 0
				  AND ((total_balance - $4::numeric)
				       - (reserved_balance - $2::numeric + $3::numeric)) >= 0`,
				reservation.ExecutionAccountID, reservation.RemainingReservedBalance.String(),
				targetBalance.String(), deltaNotional.String(), now)
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("settle buy balance: %w", err)
			}
			if !oneRow(result) {
				return domain.AssetReservation{}, reject("BUY_SETTLEMENT_EXCEEDS_RESERVATION", "fill cost exceeds protected balance or account state is inconsistent")
			}
			if sign, _ := deltaShares.Sign(); sign > 0 {
				result, err = tx.ExecContext(ctx, `
					INSERT INTO execution_positions (
						execution_account_id, market_id, token_id,
						total_shares, available_shares, reserved_shares,
						created_at, updated_at
					) VALUES ($1, $2, $3, $4::numeric, $4::numeric, 0, $5, $5)
					ON CONFLICT (execution_account_id, token_id) DO UPDATE
					SET total_shares = execution_positions.total_shares + EXCLUDED.total_shares,
					    available_shares = execution_positions.available_shares + EXCLUDED.available_shares,
					    version = execution_positions.version + 1,
					    updated_at = EXCLUDED.updated_at
					WHERE execution_positions.market_id = EXCLUDED.market_id`,
					reservation.ExecutionAccountID, reservation.MarketID, reservation.TokenID,
					deltaShares.String(), now)
				if err != nil {
					return domain.AssetReservation{}, fmt.Errorf("credit bought shares: %w", err)
				}
				if !oneRow(result) {
					return domain.AssetReservation{}, fmt.Errorf("position token is already associated with a different market")
				}
			}
			reservation.RemainingReservedBalance = targetBalance

		case domain.SideSell:
			var lockedToken string
			err := tx.QueryRowContext(ctx, `
				SELECT token_id FROM execution_positions
				WHERE execution_account_id = $1 AND token_id = $2
				FOR UPDATE`, reservation.ExecutionAccountID, reservation.TokenID).Scan(&lockedToken)
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("lock position for sell settlement: %w", err)
			}
			result, err := tx.ExecContext(ctx, `
				UPDATE execution_positions
				SET total_shares = total_shares - $5::numeric,
				    reserved_shares = reserved_shares - $3::numeric + $4::numeric,
				    available_shares = (total_shares - $5::numeric)
				        - (reserved_shares - $3::numeric + $4::numeric),
				    version = version + 1,
				    updated_at = $6
				WHERE execution_account_id = $1 AND token_id = $2
				  AND market_id = $7
				  AND total_shares >= $5::numeric
				  AND reserved_shares >= $3::numeric
				  AND (reserved_shares - $3::numeric + $4::numeric) >= 0
				  AND ((total_shares - $5::numeric)
				       - (reserved_shares - $3::numeric + $4::numeric)) >= 0`,
				reservation.ExecutionAccountID, reservation.TokenID,
				reservation.RemainingReservedShares.String(), targetRemainingShares.String(),
				deltaShares.String(), now, reservation.MarketID)
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("settle sell shares: %w", err)
			}
			if !oneRow(result) {
				return domain.AssetReservation{}, reject("SELL_SETTLEMENT_POSITION_MISMATCH", "settled sell exceeds reserved position or position state is inconsistent")
			}
			_, err = tx.ExecContext(ctx, `
				UPDATE execution_accounts
				SET total_balance = total_balance + $2::numeric,
				    available_balance = available_balance + $2::numeric,
				    version = version + 1,
				    updated_at = $3
				WHERE execution_account_id = $1`,
				reservation.ExecutionAccountID, deltaNotional.String(), now)
			if err != nil {
				return domain.AssetReservation{}, fmt.Errorf("credit sell proceeds: %w", err)
			}
			reservation.RemainingReservedShares = targetRemainingShares
		default:
			return domain.AssetReservation{}, fmt.Errorf("unsupported reservation side %q", reservation.Side)
		}

		reservation.SettledShares = filled
		reservation.SettledNotional = cumulativeNotional
		reservation.Status = nextReservationStatus(order.Status)
		reservation.UncertainReason = ""
		reservation.UpdatedAt = now
		reservation.Revision++
		if terminal {
			reservation.RemainingReservedBalance = "0"
			reservation.RemainingReservedShares = "0"
			reservation.ReleasedAt = &now
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE asset_reservations
			SET remaining_reserved_balance = $2::numeric,
			    remaining_reserved_shares = $3::numeric,
			    settled_shares = $4::numeric,
			    settled_notional = $5::numeric,
			    status = $6,
			    uncertain_reason = '',
			    last_venue_observed_at = $7,
			    revision = revision + 1,
			    updated_at = $8::timestamptz,
			    released_at = CASE WHEN $9::boolean THEN $8::timestamptz ELSE NULL::timestamptz END
			WHERE order_id = $1`,
			reservation.OrderID, reservation.RemainingReservedBalance.String(),
			reservation.RemainingReservedShares.String(), reservation.SettledShares.String(),
			reservation.SettledNotional.String(), string(reservation.Status),
			order.VenueLastObservedAt, now, terminal)
		if err != nil {
			return domain.AssetReservation{}, fmt.Errorf("update asset reservation: %w", err)
		}
		eventType := "RECONCILED"
		if terminal && order.Status != domain.OrderStatusFilled {
			eventType = "RELEASED"
		}
		eventKey := strings.Join([]string{string(order.Status), filled.String(), cumulativeNotional.String()}, ":")
		if err := insertEvent(ctx, tx, reservation, eventKey, eventType, string(order.Status), ""); err != nil {
			return domain.AssetReservation{}, err
		}
		storedReservation, err = selectReservationByOrderID(ctx, tx, order.ID, false)
		if err != nil {
			return domain.AssetReservation{}, err
		}
		return storedReservation.AssetReservation, nil
	})
}

// MarkUncertain 将订单资产预占标记为不确定并继续冻结。
func (manager *ReservationManager) MarkUncertain(ctx context.Context, order domain.Order, reason string) error {
	order = domain.CloneOrder(order)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return fmt.Errorf("uncertain reservation reason is required")
	}
	fingerprint, _, err := validateReservationOrder(order)
	if err != nil {
		return err
	}
	now := manager.now().UTC()
	_, err = manager.transact(ctx, func(tx *sql.Tx) (domain.AssetReservation, error) {
		if err := lockExecutionAccount(ctx, tx, order.Intent.ExecutionAccountID); err != nil {
			return domain.AssetReservation{}, err
		}
		storedReservation, err := selectReservationByOrderID(ctx, tx, order.ID, true)
		if err != nil {
			return domain.AssetReservation{}, err
		}
		if err := verifyReservation(storedReservation, order, fingerprint); err != nil {
			return domain.AssetReservation{}, err
		}
		reservation := storedReservation.AssetReservation
		if reservation.Status == domain.ReservationStatusReleased || reservation.Status == domain.ReservationStatusSettled {
			return reservation, nil
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE asset_reservations
			SET status = 'RECONCILIATION_REQUIRED', uncertain_reason = $2,
			    revision = revision + 1, updated_at = $3
			WHERE order_id = $1`, reservation.OrderID, reason, now)
		if err != nil {
			return domain.AssetReservation{}, fmt.Errorf("mark reservation uncertain: %w", err)
		}
		reservation.Status = domain.ReservationStatusReconciliationRequired
		reservation.UncertainReason = reason
		reservation.UpdatedAt = now
		reservation.Revision++
		if err := insertEvent(ctx, tx, reservation, "uncertain:"+reason, "MARKED_UNCERTAIN", string(order.Status), reason); err != nil {
			return domain.AssetReservation{}, err
		}
		return reservation, nil
	})
	return err
}

// transact 以串行化隔离和有限重试执行资产预占事务。
func (manager *ReservationManager) transact(ctx context.Context, operation func(*sql.Tx) (domain.AssetReservation, error)) (domain.AssetReservation, error) {
	var lastErr error
	for attempt := 1; attempt <= manager.maxAttempts; attempt++ {
		tx, err := manager.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return domain.AssetReservation{}, fmt.Errorf("begin reservation transaction: %w", err)
		}
		result, operationErr := operation(tx)
		if operationErr != nil {
			_ = tx.Rollback()
			if retryablePostgresError(operationErr) && attempt < manager.maxAttempts {
				lastErr = operationErr
				continue
			}
			return domain.AssetReservation{}, operationErr
		}
		if err := tx.Commit(); err != nil {
			// A failed Commit can have an unknown outcome. Never compensate by
			// releasing collateral here; the caller must reconcile by order ID.
			return domain.AssetReservation{}, fmt.Errorf("commit reservation transaction (outcome may be unknown): %w", err)
		}
		return result, nil
	}
	return domain.AssetReservation{}, lastErr
}

// validateReservationOrder 校验 Reservation Order 的字段和业务约束。
func validateReservationOrder(order domain.Order) (string, domain.Decimal, error) {
	if strings.TrimSpace(order.ID) == "" {
		return "", "", fmt.Errorf("reservation order_id is required")
	}
	if err := order.Intent.Validate(); err != nil {
		return "", "", fmt.Errorf("invalid reservation intent: %w", err)
	}
	price := domain.Decimal("0")
	if order.Intent.Side == domain.SideBuy {
		price = order.Intent.WorstPrice
		if sign, err := price.Sign(); err != nil || sign <= 0 {
			return "", "", fmt.Errorf("BUY reservation requires a positive worst_price")
		}
	}
	fingerprint, err := intentFingerprint(order.Intent)
	return fingerprint, price, err
}

// validateSettlementOrder 校验 Settlement Order 的字段和业务约束。
func validateSettlementOrder(order domain.Order) (string, domain.Decimal, error) {
	fingerprint, price, err := validateReservationOrder(order)
	if err != nil {
		return "", "", err
	}
	if order.Status != domain.OrderStatusSubmitted && order.Status != domain.OrderStatusOpen &&
		order.Status != domain.OrderStatusPartiallyFilled && order.Status != domain.OrderStatusFilled &&
		order.Status != domain.OrderStatusCanceled && order.Status != domain.OrderStatusRejected {
		return "", "", fmt.Errorf("order status %q cannot reconcile a reservation", order.Status)
	}
	filled := order.FilledSize
	if filled.IsEmpty() {
		filled = "0"
	}
	if sign, err := filled.Sign(); err != nil || sign < 0 {
		return "", "", fmt.Errorf("filled_size must be non-negative")
	} else if sign > 0 {
		if priceSign, priceErr := order.AverageFillPrice.Sign(); priceErr != nil || priceSign <= 0 {
			return "", "", fmt.Errorf("positive fills require average_fill_price")
		}
	}
	return fingerprint, price, nil
}

// intentFingerprint 根据稳定业务身份生成幂等标识。
func intentFingerprint(intent domain.OrderIntent) (string, error) {
	payload, err := json.Marshal(intent.Normalize())
	if err != nil {
		return "", fmt.Errorf("marshal reservation intent fingerprint: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// lockExecutionAccount 在当前事务中锁定 Execution Account。
func lockExecutionAccount(ctx context.Context, tx *sql.Tx, accountID string) error {
	var locked string
	err := tx.QueryRowContext(ctx, `
		SELECT execution_account_id
		FROM execution_accounts
		WHERE execution_account_id = $1
		FOR UPDATE`, accountID).Scan(&locked)
	if errors.Is(err, sql.ErrNoRows) {
		return reject("EXECUTION_ACCOUNT_NOT_FOUND", "execution account balance has not been initialized")
	}
	if err != nil {
		return fmt.Errorf("lock execution account: %w", err)
	}
	return nil
}

const reservationColumns = `
	order_id, client_order_id, execution_account_id, strategy_id, market_id, token_id, COALESCE(target_lot_id, ''), side,
	requested_shares::text, reserve_unit_price::text,
	initial_reserved_balance::text, remaining_reserved_balance::text,
	initial_reserved_shares::text, remaining_reserved_shares::text,
	settled_shares::text, settled_notional::text, settled_fees::text,
	risk_policy_id, risk_policy_version, COALESCE(risk_day::text, ''), daily_risk_notional::text,
	status, uncertain_reason, created_at, updated_at, released_at, revision, intent_fingerprint`

// storedReservation 表示后端使用的 storedReservation 类型。
type storedReservation struct {
	domain.AssetReservation
	IntentFingerprint string
}

// selectReservationByOrderID 从 PostgreSQL 查询 Reservation By Order 标识。
func selectReservationByOrderID(ctx context.Context, tx *sql.Tx, orderID string, forUpdate bool) (storedReservation, error) {
	query := `SELECT ` + reservationColumns + ` FROM asset_reservations WHERE order_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	return scanReservation(tx.QueryRowContext(ctx, query, orderID))
}

// selectReservationByClientOrderID 从 PostgreSQL 查询 Reservation By Client Order 标识。
func selectReservationByClientOrderID(ctx context.Context, tx *sql.Tx, clientOrderID string, forUpdate bool) (storedReservation, error) {
	query := `SELECT ` + reservationColumns + ` FROM asset_reservations WHERE client_order_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	return scanReservation(tx.QueryRowContext(ctx, query, clientOrderID))
}

// rowScanner 表示后端使用的 rowScanner 类型。
type rowScanner interface {
	Scan(...any) error
}

// scanReservation 将数据库行扫描为 Reservation。
func scanReservation(row rowScanner) (storedReservation, error) {
	var stored storedReservation
	reservation := &stored.AssetReservation
	var side, status, fingerprint string
	var releasedAt sql.NullTime
	err := row.Scan(
		&reservation.OrderID, &reservation.ClientOrderID, &reservation.ExecutionAccountID,
		&reservation.StrategyID, &reservation.MarketID, &reservation.TokenID, &reservation.TargetLotID, &side,
		&reservation.RequestedShares, &reservation.ReserveUnitPrice,
		&reservation.InitialReservedBalance, &reservation.RemainingReservedBalance,
		&reservation.InitialReservedShares, &reservation.RemainingReservedShares,
		&reservation.SettledShares, &reservation.SettledNotional, &reservation.SettledFees,
		&reservation.RiskPolicyID, &reservation.RiskPolicyVersion, &reservation.RiskDay, &reservation.DailyRiskNotional,
		&status, &reservation.UncertainReason, &reservation.CreatedAt, &reservation.UpdatedAt,
		&releasedAt, &reservation.Revision, &fingerprint,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storedReservation{}, port.ErrReservationNotFound
	}
	if err != nil {
		return storedReservation{}, fmt.Errorf("scan asset reservation: %w", err)
	}
	reservation.Side = domain.Side(side)
	reservation.Status = domain.ReservationStatus(status)
	if releasedAt.Valid {
		value := releasedAt.Time.UTC()
		reservation.ReleasedAt = &value
	}
	stored.IntentFingerprint = fingerprint
	return stored, nil
}

// verifyReservation 验证 Reservation 的身份和一致性。
func verifyReservation(stored storedReservation, order domain.Order, fingerprint string) error {
	reservation := stored.AssetReservation
	if reservation.OrderID != order.ID || reservation.ClientOrderID != order.Intent.ClientOrderID ||
		reservation.ExecutionAccountID != order.Intent.ExecutionAccountID || stored.IntentFingerprint != fingerprint {
		return port.ErrReservationConflict
	}
	return nil
}

// settlementNumbers 设置 tlement Numbers。
func settlementNumbers(
	ctx context.Context,
	tx *sql.Tx,
	filled, settledShares, averagePrice, settledNotional, requestedShares domain.Decimal,
) (domain.Decimal, domain.Decimal, domain.Decimal, domain.Decimal, error) {
	var deltaShares, cumulativeNotional, deltaNotional, remainingShares domain.Decimal
	err := tx.QueryRowContext(ctx, `
		SELECT
			($1::numeric - $2::numeric)::text,
			($3::numeric * $1::numeric)::text,
			(($3::numeric * $1::numeric) - $4::numeric)::text,
			($5::numeric - $1::numeric)::text`,
		filled.String(), settledShares.String(), averagePrice.String(),
		settledNotional.String(), requestedShares.String(),
	).Scan(&deltaShares, &cumulativeNotional, &deltaNotional, &remainingShares)
	if err != nil {
		return "", "", "", "", fmt.Errorf("calculate cumulative settlement: %w", err)
	}
	return deltaShares, cumulativeNotional, deltaNotional, remainingShares, nil
}

// numericProduct 通过 PostgreSQL numeric 精确计算两个十进制值的乘积。
func numericProduct(ctx context.Context, tx *sql.Tx, left, right domain.Decimal) (domain.Decimal, error) {
	var product domain.Decimal
	if err := tx.QueryRowContext(ctx, `SELECT ($1::numeric * $2::numeric)::text`, left.String(), right.String()).Scan(&product); err != nil {
		return "", fmt.Errorf("calculate reserved balance: %w", err)
	}
	return product, nil
}

// nextReservationStatus 将外部或订单状态映射为目标领域状态。
func nextReservationStatus(status domain.OrderStatus) domain.ReservationStatus {
	switch status {
	case domain.OrderStatusFilled:
		return domain.ReservationStatusSettled
	case domain.OrderStatusCanceled, domain.OrderStatusRejected:
		return domain.ReservationStatusReleased
	default:
		return domain.ReservationStatusActive
	}
}

// insertEvent 在当前事务中插入 Event。
func insertEvent(ctx context.Context, tx *sql.Tx, reservation domain.AssetReservation, eventKey, eventType, orderStatus, details string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO asset_reservation_events (
			order_id, event_key, event_type, order_status,
			cumulative_filled_shares, cumulative_fill_notional, cumulative_fees,
			remaining_reserved_balance, remaining_reserved_shares, details
		) VALUES ($1, $2, $3, $4, $5::numeric, $6::numeric, $7::numeric, $8::numeric, $9::numeric, $10)
		ON CONFLICT (order_id, event_key) DO NOTHING`,
		reservation.OrderID, eventKey, eventType, orderStatus,
		reservation.SettledShares.String(), reservation.SettledNotional.String(), decimalOrZero(reservation.SettledFees),
		reservation.RemainingReservedBalance.String(), reservation.RemainingReservedShares.String(), details)
	if err != nil {
		return fmt.Errorf("insert reservation event: %w", err)
	}
	return nil
}

// oneRow 判断数据库写操作是否恰好影响一行。
func oneRow(result sql.Result) bool {
	count, err := result.RowsAffected()
	return err == nil && count == 1
}

// postgresStateError 表示后端使用的 postgresStateError 类型。
type postgresStateError interface {
	SQLState() string
}

// retryablePostgresError 判断 PostgreSQL 错误是否允许重试整个事务。
func retryablePostgresError(err error) bool {
	var stateError postgresStateError
	if !errors.As(err, &stateError) {
		return false
	}
	return stateError.SQLState() == "40001" || stateError.SQLState() == "40P01"
}

// reject 构建并返回 对应数据 的拒绝结果。
func reject(code, reason string) error {
	return &port.Rejection{Code: code, Reason: reason}
}

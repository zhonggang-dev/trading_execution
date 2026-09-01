package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	domainorderstate "github.com/UniPat-AI/trading_execution/internal/domain/orderstate"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// FillLedgerParams 表示后端使用的 FillLedgerParams 类型。
type FillLedgerParams struct {
	DB                    *sql.DB
	Now                   func() time.Time
	MaxAttempts           int
	DustSharesThreshold   domain.Decimal
	DustNotionalThreshold domain.Decimal
}

// FillLedger 表示后端使用的 FillLedger 类型。
type FillLedger struct {
	db                    *sql.DB
	now                   func() time.Time
	maxAttempts           int
	dustSharesThreshold   domain.Decimal
	dustNotionalThreshold domain.Decimal
}

var (
	_ port.FillLedger              = (*FillLedger)(nil)
	_ port.PositionLedger          = (*FillLedger)(nil)
	_ port.FundsLedger             = (*FillLedger)(nil)
	_ port.PositionExitTradeSource = (*FillLedger)(nil)
	_ port.ReconciliationLedger    = (*FillLedger)(nil)
)

// NewFillLedger 创建并初始化 Fill Ledger。
func NewFillLedger(params FillLedgerParams) (*FillLedger, error) {
	if params.DB == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.MaxAttempts == 0 {
		params.MaxAttempts = defaultTransactionAttempts
	}
	if params.MaxAttempts < 1 || params.MaxAttempts > 20 {
		return nil, fmt.Errorf("fill transaction attempts must be between 1 and 20")
	}
	for name, value := range map[string]domain.Decimal{
		"dust shares threshold":   params.DustSharesThreshold,
		"dust notional threshold": params.DustNotionalThreshold,
	} {
		if value.IsEmpty() {
			continue
		}
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return nil, fmt.Errorf("%s must be non-negative", name)
		}
	}
	return &FillLedger{
		db: params.DB, now: params.Now, maxAttempts: params.MaxAttempts,
		dustSharesThreshold:   params.DustSharesThreshold,
		dustNotionalThreshold: params.DustNotionalThreshold,
	}, nil
}

// Record 在可重试串行化事务中幂等记录成交并更新订单、资金、仓位和 Outbox。
func (ledger *FillLedger) Record(ctx context.Context, expected domain.Order, fill domain.Fill) (domain.FillApplication, error) {
	fill = fill.Normalize()
	if err := fill.ValidateAccounting(); err != nil {
		return domain.FillApplication{}, err
	}
	var lastErr error
	for attempt := 1; attempt <= ledger.maxAttempts; attempt++ {
		application, err := ledger.recordOnce(ctx, expected, fill)
		if err == nil {
			return application, nil
		}
		if !retryablePostgresError(err) || attempt == ledger.maxAttempts {
			return domain.FillApplication{}, err
		}
		lastErr = err
	}
	return domain.FillApplication{}, lastErr
}

// recordOnce 在单个串行化事务中应用一次成交观察。
func (ledger *FillLedger) recordOnce(ctx context.Context, expected domain.Order, fill domain.Fill) (domain.FillApplication, error) {
	tx, err := ledger.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return domain.FillApplication{}, fmt.Errorf("begin fill ledger transaction: %w", err)
	}
	defer tx.Rollback()
	if err := lockExecutionAccount(ctx, tx, fill.ExecutionAccountID); err != nil {
		return domain.FillApplication{}, err
	}
	order, err := selectOrder(ctx, tx, `WHERE order_id = $1 FOR UPDATE`, fill.OrderID)
	if err != nil {
		return domain.FillApplication{}, err
	}
	if order.Revision != expected.Revision {
		return domain.FillApplication{}, port.ErrOrderRevisionConflict
	}
	if err := verifyFillOrder(order, fill); err != nil {
		return domain.FillApplication{}, err
	}
	stored, found, err := selectFillForUpdate(ctx, tx, fill.Key)
	if err != nil {
		return domain.FillApplication{}, err
	}
	if found {
		if err := verifyFillIdentity(stored, fill); err != nil {
			return domain.FillApplication{}, err
		}
		if !stored.Status.CanTransitionTo(fill.Status) {
			return domain.FillApplication{}, fmt.Errorf("%w: fill status %s -> %s", port.ErrFillConflict, stored.Status, fill.Status)
		}
		if stored.AppliedAt != nil {
			position, _ := selectPosition(ctx, tx, fill.ExecutionAccountID, fill.TokenID, false)
			return commitApplication(tx, domain.FillApplication{
				Fill: stored, Order: order, Position: positionOrNil(position), Duplicate: true,
			})
		}
		if err := updateFillObservation(ctx, tx, fill); err != nil {
			return domain.FillApplication{}, err
		}
	} else if err := insertFill(ctx, tx, fill); err != nil {
		return domain.FillApplication{}, err
	}
	if err := insertFillEvent(ctx, tx, fill); err != nil {
		return domain.FillApplication{}, err
	}
	if fill.Status != domain.FillStatusConfirmed {
		outboxID, err := insertOutbox(ctx, tx, fillObservationTopic(fill.Status), fill.Key+":"+string(fill.Status), fill.OrderID, fill, fill.ObservedAt)
		if err != nil {
			return domain.FillApplication{}, err
		}
		return commitApplication(tx, domain.FillApplication{Fill: fill, Order: order, OutboxEventID: outboxID})
	}

	if order.Status == domain.OrderStatusSubmitting {
		if err := transitionOrderForFill(ctx, tx, &order, fillOrderTransitionParams{Target: domain.OrderStatusAcknowledged, Fill: fill}); err != nil {
			return domain.FillApplication{}, err
		}
	}
	if order.Status == domain.OrderStatusUnknown {
		if err := transitionOrderForFill(ctx, tx, &order, fillOrderTransitionParams{Target: domain.OrderStatusReconciling, Fill: fill}); err != nil {
			return domain.FillApplication{}, err
		}
	}

	reservation, err := selectReservationByOrderID(ctx, tx, order.ID, true)
	if err != nil {
		return domain.FillApplication{}, err
	}
	if reservation.ExecutionAccountID != fill.ExecutionAccountID || reservation.TokenID != fill.TokenID || reservation.Side != fill.Side {
		return domain.FillApplication{}, port.ErrReservationConflict
	}
	cumulativeShares, err := numeric(ctx, tx, `SELECT ($1::numeric + $2::numeric)::text`, decimalOrZero(order.FilledSize), fill.Shares.String())
	if err != nil {
		return domain.FillApplication{}, err
	}
	sharesComparison, err := cumulativeShares.Compare(order.Intent.Size)
	if err != nil || (sharesComparison > 0 && !order.AllowsBuySharePriceImprovement()) {
		return domain.FillApplication{}, fmt.Errorf("confirmed fills exceed requested shares")
	}
	cumulativeNotional, err := numeric(ctx, tx, `SELECT ($1::numeric + $2::numeric)::text`, decimalOrZero(order.FilledNotional), fill.GrossNotional.String())
	if err != nil {
		return domain.FillApplication{}, err
	}
	cumulativeFees, err := numeric(ctx, tx, `SELECT ($1::numeric + $2::numeric)::text`, decimalOrZero(order.TotalFees), fill.TotalFee.String())
	if err != nil {
		return domain.FillApplication{}, err
	}
	if sharesComparison > 0 {
		maximumGross, err := numeric(ctx, tx, `SELECT ($1::numeric * $2::numeric)::text`,
			order.Intent.Size.String(), order.Intent.WorstPrice.String())
		if err != nil {
			return domain.FillApplication{}, err
		}
		if comparison, compareErr := cumulativeNotional.Compare(maximumGross); compareErr != nil || comparison > 0 {
			return domain.FillApplication{}, reject(
				"BUY_PRICE_IMPROVEMENT_EXCEEDS_LIMIT_NOTIONAL",
				"Polymarket BUY delivered extra shares but exceeded the signed limit-price notional",
			)
		}
		protectedCost, err := numeric(ctx, tx, `SELECT ($1::numeric + $2::numeric)::text`,
			cumulativeNotional.String(), cumulativeFees.String())
		if err != nil {
			return domain.FillApplication{}, err
		}
		if comparison, compareErr := protectedCost.Compare(reservation.InitialReservedBalance); compareErr != nil || comparison > 0 {
			return domain.FillApplication{}, reject(
				"BUY_PRICE_IMPROVEMENT_EXCEEDS_RESERVATION",
				"Polymarket BUY delivered extra shares but exceeded the order's protected balance",
			)
		}
	}
	averagePrice, err := numeric(ctx, tx, `SELECT ($1::numeric / $2::numeric)::text`, cumulativeNotional.String(), cumulativeShares.String())
	if err != nil {
		return domain.FillApplication{}, err
	}
	target := domain.OrderStatusPartiallyFilled
	if sharesComparison == 0 || (sharesComparison > 0 && order.AllowsBuySharePriceImprovement()) {
		target = domain.OrderStatusFilled
	} else if order.Status == domain.OrderStatusManualReview {
		target = domain.OrderStatusManualReview
	} else if order.Status == domain.OrderStatusCancelled || order.Intent.TimeInForce == domain.TimeInForceFAK ||
		order.Intent.TimeInForce == domain.TimeInForceIOC {
		target = domain.OrderStatusCancelled
	}

	accountBefore, err := selectAccountSnapshot(ctx, tx, fill.ExecutionAccountID)
	if err != nil {
		return domain.FillApplication{}, err
	}
	var position domain.Position
	var positionEvents []domain.PositionEvent
	switch fill.Side {
	case domain.SideBuy:
		position, positionEvents, err = ledger.applyBuy(ctx, tx, order, fill, reservation.AssetReservation, cumulativeShares, cumulativeNotional, cumulativeFees, target)
	case domain.SideSell:
		position, positionEvents, err = ledger.applySell(ctx, tx, order, fill, reservation.AssetReservation, cumulativeShares, cumulativeNotional, cumulativeFees, target)
	default:
		err = fmt.Errorf("unsupported fill side %q", fill.Side)
	}
	if err != nil {
		return domain.FillApplication{}, err
	}
	accountAfter, err := selectAccountSnapshot(ctx, tx, fill.ExecutionAccountID)
	if err != nil {
		return domain.FillApplication{}, err
	}
	if err := insertAccountEvent(ctx, tx, order, fill, accountBefore, accountAfter); err != nil {
		return domain.FillApplication{}, err
	}
	amounts := fillOrderTransitionParams{
		Fill: fill, FilledSize: cumulativeShares, FilledNotional: cumulativeNotional,
		TotalFees: cumulativeFees, AverageFillPrice: averagePrice, IncludeAmounts: true,
	}
	if target == domain.OrderStatusCancelled && order.Status != domain.OrderStatusCancelled {
		// An IOC/FAK trade can fill a strict subset and cancel the remainder in
		// one venue action. Persist both facts rather than erasing the partial
		// fill behind a single CANCELLED transition.
		amounts.Target = domain.OrderStatusPartiallyFilled
		if err := transitionOrderForFill(ctx, tx, &order, amounts); err != nil {
			return domain.FillApplication{}, err
		}
		if err := transitionOrderForFill(ctx, tx, &order, fillOrderTransitionParams{Target: domain.OrderStatusCancelled, Fill: fill}); err != nil {
			return domain.FillApplication{}, err
		}
	} else {
		amounts.Target = target
		if err := transitionOrderForFill(ctx, tx, &order, amounts); err != nil {
			return domain.FillApplication{}, err
		}
	}
	appliedAt := ledger.now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_fills SET applied_at = $2, last_observed_at = $3
		WHERE fill_key = $1 AND applied_at IS NULL AND status = 'CONFIRMED'`, fill.Key, appliedAt, fill.ObservedAt)
	if err != nil || !oneRow(result) {
		return domain.FillApplication{}, port.ErrFillConflict
	}
	fill.AppliedAt = &appliedAt
	outboxPayload := struct {
		Fill     domain.Fill     `json:"fill"`
		Order    domain.Order    `json:"order"`
		Position domain.Position `json:"position"`
	}{Fill: fill, Order: order, Position: position}
	outboxID, err := insertOutbox(ctx, tx, "trading.fill.confirmed.v1", fill.Key, order.ID, outboxPayload, appliedAt)
	if err != nil {
		return domain.FillApplication{}, err
	}
	return commitApplication(tx, domain.FillApplication{
		Fill: fill, Order: order, Position: &position, PositionEvents: positionEvents,
		Applied: true, OutboxEventID: outboxID,
	})
}

// verifyFillOrder 验证 Fill Order 的身份和一致性。
func verifyFillOrder(order domain.Order, fill domain.Fill) error {
	if order.ID != fill.OrderID || order.Intent.ExecutionAccountID != fill.ExecutionAccountID ||
		order.Intent.Venue != fill.Venue || order.Intent.MarketID != fill.MarketID ||
		order.Intent.TokenID != fill.TokenID || order.Intent.Side != fill.Side ||
		!strings.EqualFold(order.VenueOrderID, fill.VenueOrderID) {
		return port.ErrFillConflict
	}
	return nil
}

// fillOrderTransitionParams 表示后端使用的 fillOrderTransitionParams 类型。
type fillOrderTransitionParams struct {
	Target           domain.OrderStatus
	Fill             domain.Fill
	FilledSize       domain.Decimal
	FilledNotional   domain.Decimal
	TotalFees        domain.Decimal
	AverageFillPrice domain.Decimal
	IncludeAmounts   bool
}

// Build 构建成交驱动的领域订单状态迁移并保持交易所观察时间单调递增。
func (params fillOrderTransitionParams) Build(order domain.Order) domainorderstate.Transition {
	observedAt := params.Fill.VenueUpdatedAt.UTC()
	if observedAt.IsZero() {
		observedAt = params.Fill.ObservedAt.UTC()
	}
	if order.VenueLastObservedAt != nil && observedAt.Before(order.VenueLastObservedAt.UTC()) {
		observedAt = order.VenueLastObservedAt.UTC()
	}
	transition := domainorderstate.Transition{
		EventID:         fmt.Sprintf("event:%s:%d", order.ID, order.Revision+1),
		To:              params.Target,
		Trigger:         domain.TransitionTriggerFill,
		VenueStatus:     string(params.Fill.Status),
		VenueOrderID:    params.Fill.VenueOrderID,
		VenueObservedAt: &observedAt,
		At:              params.Fill.ObservedAt.UTC(),
	}
	if params.IncludeAmounts {
		transition.FillKey = params.Fill.Key
		transition.FilledSize = params.FilledSize
		transition.FilledNotional = params.FilledNotional
		transition.TotalFees = params.TotalFees
		transition.AverageFillPrice = params.AverageFillPrice
	}
	if params.Target == domain.OrderStatusManualReview {
		transition.ReasonCode = order.FailureCode
		transition.Reason = order.FailureReason
	}
	return transition
}

// transitionOrderForFill 通过领域状态机迁移订单并在同一事务写入订单和审计事件。
func transitionOrderForFill(ctx context.Context, tx *sql.Tx, order *domain.Order, params fillOrderTransitionParams) error {
	next, event, err := domainorderstate.Apply(*order, params.Build(*order))
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE execution_orders
		SET status = $4, filled_size = $5::numeric, filled_notional = $6::numeric,
		    total_fees = $7::numeric, average_fill_price = NULLIF($8, '')::numeric,
		    venue_last_observed_at = $9, revision = $3, updated_at = $10,
		    failure_code = $11, failure_reason = $12
		WHERE order_id = $1 AND revision = $2`,
		next.ID, order.Revision, next.Revision, string(next.Status),
		decimalOrZero(next.FilledSize), decimalOrZero(next.FilledNotional), decimalOrZero(next.TotalFees),
		next.AverageFillPrice.String(), next.VenueLastObservedAt, next.UpdatedAt, next.FailureCode, next.FailureReason)
	if err != nil || !oneRow(result) {
		return port.ErrOrderRevisionConflict
	}
	if err := insertOrderEvent(ctx, tx, event); err != nil {
		return err
	}
	*order = next
	return nil
}

// commitApplication 提交成交账本事务并返回应用结果。
func commitApplication(tx *sql.Tx, application domain.FillApplication) (domain.FillApplication, error) {
	if err := tx.Commit(); err != nil {
		return domain.FillApplication{}, fmt.Errorf("commit fill ledger transaction (outcome may be unknown): %w", err)
	}
	return application, nil
}

// accountSnapshot 表示后端使用的 accountSnapshot 类型。
type accountSnapshot struct {
	Total     domain.Decimal
	Available domain.Decimal
	Reserved  domain.Decimal
}

// selectAccountSnapshot 从 PostgreSQL 查询 Account Snapshot。
func selectAccountSnapshot(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, accountID string) (accountSnapshot, error) {
	var snapshot accountSnapshot
	err := query.QueryRowContext(ctx, `
		SELECT total_balance::text, available_balance::text, reserved_balance::text
		FROM execution_accounts WHERE execution_account_id = $1`, accountID).
		Scan(&snapshot.Total, &snapshot.Available, &snapshot.Reserved)
	return snapshot, err
}

// insertAccountEvent 在当前事务中插入 Account Event。
func insertAccountEvent(ctx context.Context, tx *sql.Tx, order domain.Order, fill domain.Fill, before, after accountSnapshot) error {
	totalDelta, _ := numeric(ctx, tx, `SELECT ($1::numeric - $2::numeric)::text`, after.Total.String(), before.Total.String())
	availableDelta, _ := numeric(ctx, tx, `SELECT ($1::numeric - $2::numeric)::text`, after.Available.String(), before.Available.String())
	reservedDelta, _ := numeric(ctx, tx, `SELECT ($1::numeric - $2::numeric)::text`, after.Reserved.String(), before.Reserved.String())
	_, err := tx.ExecContext(ctx, `
		INSERT INTO execution_account_events (
			account_event_id, execution_account_id, event_type, order_id, fill_key,
			total_balance_delta, available_balance_delta, reserved_balance_delta,
			total_balance_after, available_balance_after, reserved_balance_after, occurred_at
		) VALUES ($1,$2,'FILL_SETTLED',$3,$4,$5::numeric,$6::numeric,$7::numeric,$8::numeric,$9::numeric,$10::numeric,$11)`,
		"account-event:"+fill.Key, fill.ExecutionAccountID, order.ID, fill.Key,
		totalDelta.String(), availableDelta.String(), reservedDelta.String(),
		after.Total.String(), after.Available.String(), after.Reserved.String(), fill.ObservedAt)
	return err
}

// insertOutbox 在当前事务中插入 Outbox。
func insertOutbox(ctx context.Context, tx *sql.Tx, topic, eventKey, aggregateID string, payload any, occurredAt time.Time) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	outboxID := "outbox:" + eventKey
	aggregateType := "ORDER"
	if strings.HasPrefix(topic, "trading.position.") {
		aggregateType = "POSITION"
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO execution_outbox (
			outbox_event_id, topic, event_key, aggregate_type, aggregate_id, payload, created_at, next_attempt_at
		) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$7)
		ON CONFLICT (topic, event_key) DO NOTHING`, outboxID, topic, eventKey, aggregateType, aggregateID, body, occurredAt)
	return outboxID, err
}

// fillObservationTopic 根据成交状态选择对应的观察事件主题。
func fillObservationTopic(status domain.FillStatus) string {
	if status == domain.FillStatusFailed {
		return "trading.fill.failed.v1"
	}
	return "trading.fill.observed.v1"
}

// numeric 通过 PostgreSQL numeric 表达式执行精确十进制计算。
func numeric(ctx context.Context, tx *sql.Tx, statement string, arguments ...any) (domain.Decimal, error) {
	var value domain.Decimal
	if err := tx.QueryRowContext(ctx, statement, arguments...).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

// positionOrNil 将不存在的仓位零值转换为空指针。
func positionOrNil(position domain.Position) *domain.Position {
	if position.ExecutionAccountID == "" {
		return nil
	}
	return &position
}

// verifyFillIdentity 验证 Fill Identity 的身份和一致性。
func verifyFillIdentity(stored, incoming domain.Fill) error {
	storedEvidence, err := canonicalSettlementEvidenceIdentity(stored.SettlementEvidence)
	if err != nil {
		return fmt.Errorf("canonicalize stored settlement evidence: %w", err)
	}
	incomingEvidence, err := canonicalSettlementEvidenceIdentity(incoming.SettlementEvidence)
	if err != nil {
		return fmt.Errorf("canonicalize incoming settlement evidence: %w", err)
	}
	if stored.Key != incoming.Key || stored.Venue != incoming.Venue || stored.VenueFillID != incoming.VenueFillID ||
		stored.OrderID != incoming.OrderID || stored.VenueOrderID != incoming.VenueOrderID ||
		stored.ExecutionAccountID != incoming.ExecutionAccountID || stored.MarketID != incoming.MarketID ||
		stored.ConditionID != incoming.ConditionID || stored.TokenID != incoming.TokenID ||
		stored.Side != incoming.Side || stored.LiquidityRole != incoming.LiquidityRole ||
		!persistedDecimalEqual(stored.Shares, incoming.Shares) ||
		!persistedDecimalEqual(stored.Price, incoming.Price) ||
		!persistedDecimalEqual(stored.GrossNotional, incoming.GrossNotional) ||
		!persistedDecimalEqual(stored.FeeRateBPS, incoming.FeeRateBPS) ||
		!persistedDecimalEqual(stored.PlatformFeeRate, incoming.PlatformFeeRate) ||
		!persistedDecimalEqual(stored.FeeExponent, incoming.FeeExponent) ||
		!persistedDecimalEqual(stored.PlatformFee, incoming.PlatformFee) ||
		!persistedDecimalEqual(stored.BuilderFeeRateBPS, incoming.BuilderFeeRateBPS) ||
		!persistedDecimalEqual(stored.BuilderFee, incoming.BuilderFee) ||
		!persistedDecimalEqual(stored.TotalFee, incoming.TotalFee) ||
		!persistedDecimalEqual(stored.NetCashDelta, incoming.NetCashDelta) ||
		stored.FeeSource != incoming.FeeSource ||
		!strings.EqualFold(stored.TransactionHash, incoming.TransactionHash) ||
		stored.RawPayloadSHA256 != incoming.RawPayloadSHA256 ||
		!stored.MatchedAt.Equal(incoming.MatchedAt) ||
		string(storedEvidence) != string(incomingEvidence) {
		return port.ErrFillConflict
	}
	return nil
}

// persistedDecimalEqual mirrors the ledger's empty-to-zero SQL encoding so
// replaying an old non-fee observation is not rejected merely because numeric
// zero scans back from PostgreSQL as "0".
func persistedDecimalEqual(left, right domain.Decimal) bool {
	return domain.Decimal(decimalOrZero(left)).Equal(domain.Decimal(decimalOrZero(right)))
}

// canonicalSettlementEvidence serializes the normalized struct rather than
// comparing JSONB text, whose key ordering is controlled by PostgreSQL.
func canonicalSettlementEvidence(evidence *domain.SettlementEvidence) ([]byte, error) {
	if evidence == nil {
		return []byte("{}"), nil
	}
	return evidence.CanonicalJSON()
}

// canonicalSettlementEvidenceIdentity excludes mutable observation metadata
// such as confirmations while retaining every immutable event and money field.
func canonicalSettlementEvidenceIdentity(evidence *domain.SettlementEvidence) ([]byte, error) {
	if evidence == nil {
		return []byte("{}"), nil
	}
	return evidence.CanonicalIdentityJSON()
}

// selectFillForUpdate 从 PostgreSQL 查询 Fill For Update。
func selectFillForUpdate(ctx context.Context, tx *sql.Tx, fillKey string) (domain.Fill, bool, error) {
	fill, err := scanFill(tx.QueryRowContext(ctx, fillSelect+` WHERE fill_key = $1 FOR UPDATE`, fillKey))
	if errors.Is(err, port.ErrFillNotFound) {
		return domain.Fill{}, false, nil
	}
	return fill, err == nil, err
}

// insertFill 在当前事务中插入 Fill。
func insertFill(ctx context.Context, tx *sql.Tx, fill domain.Fill) error {
	evidence, err := canonicalSettlementEvidence(fill.SettlementEvidence)
	if err != nil {
		return fmt.Errorf("encode settlement evidence: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO execution_fills (
			fill_key, venue, venue_fill_id, order_id, venue_order_id, execution_account_id,
			market_id, condition_id, token_id, side, liquidity_role, status, shares, price,
			gross_notional, fee_rate_bps, platform_fee_rate, fee_exponent, platform_fee,
			builder_fee_rate_bps, builder_fee, total_fee, net_cash_delta, settlement_evidence,
			fee_source, transaction_hash, raw_payload_sha256, matched_at, venue_updated_at,
			first_observed_at, last_observed_at, confirmed_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::numeric,$14::numeric,
			$15::numeric,$16::numeric,$17::numeric,$18::numeric,$19::numeric,$20::numeric,$21::numeric,
			$22::numeric,$23::numeric,$24::jsonb,$25,$26,$27,$28,$29,$30,$30,$31)`,
		fill.Key, fill.Venue, fill.VenueFillID, fill.OrderID, fill.VenueOrderID, fill.ExecutionAccountID,
		fill.MarketID, fill.ConditionID, fill.TokenID, string(fill.Side), string(fill.LiquidityRole), string(fill.Status),
		fill.Shares.String(), fill.Price.String(), fill.GrossNotional.String(), decimalOrZero(fill.FeeRateBPS),
		decimalOrZero(fill.PlatformFeeRate), decimalOrZero(fill.FeeExponent), decimalOrZero(fill.PlatformFee),
		decimalOrZero(fill.BuilderFeeRateBPS), decimalOrZero(fill.BuilderFee), decimalOrZero(fill.TotalFee),
		fill.NetCashDelta.String(), evidence,
		fill.FeeSource, fill.TransactionHash, fill.RawPayloadSHA256, fill.MatchedAt, nullTime(fill.VenueUpdatedAt),
		fill.ObservedAt, fill.ConfirmedAt)
	return err
}

// updateFillObservation 在当前事务中更新 Fill Observation。
func updateFillObservation(ctx context.Context, tx *sql.Tx, fill domain.Fill) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE execution_fills SET status=$2, transaction_hash=CASE WHEN $3='' THEN transaction_hash ELSE $3 END,
			venue_updated_at=$4, last_observed_at=$5, confirmed_at=COALESCE($6, confirmed_at),
			raw_payload_sha256=CASE WHEN $7='' THEN raw_payload_sha256 ELSE $7 END
		WHERE fill_key=$1`, fill.Key, string(fill.Status), fill.TransactionHash,
		nullTime(fill.VenueUpdatedAt), fill.ObservedAt, fill.ConfirmedAt, fill.RawPayloadSHA256)
	return err
}

// insertFillEvent 在当前事务中插入 Fill Event。
func insertFillEvent(ctx context.Context, tx *sql.Tx, fill domain.Fill) error {
	id := fmt.Sprintf("fill-event:%s:%s:%d", fill.Key, fill.Status, fill.ObservedAt.UnixNano())
	_, err := tx.ExecContext(ctx, `
		INSERT INTO execution_fill_events (
			fill_event_id, fill_key, order_id, status, transaction_hash,
			venue_updated_at, observed_at, payload_sha256
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT DO NOTHING`, id, fill.Key, fill.OrderID, string(fill.Status), fill.TransactionHash,
		nullTime(fill.VenueUpdatedAt), fill.ObservedAt, fill.RawPayloadSHA256)
	return err
}

// nullTime 将零时间转换为可写入数据库的空值。
func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

const fillSelect = `
	SELECT fill_key, venue, venue_fill_id, order_id, venue_order_id, execution_account_id,
	       market_id, condition_id, token_id, side, liquidity_role, status,
	       shares::text, price::text, gross_notional::text, fee_rate_bps::text,
	       platform_fee_rate::text, fee_exponent::text, platform_fee::text,
	       builder_fee_rate_bps::text, builder_fee::text, total_fee::text, net_cash_delta::text,
	       settlement_evidence,
	       fee_source, transaction_hash, raw_payload_sha256, matched_at, venue_updated_at,
	       last_observed_at, confirmed_at, applied_at
	FROM execution_fills`

// scanFill 将数据库行扫描为 Fill。
func scanFill(row rowScanner) (domain.Fill, error) {
	var fill domain.Fill
	var side, role, status string
	var venueUpdated, confirmed, applied sql.NullTime
	var settlementEvidence []byte
	err := row.Scan(
		&fill.Key, &fill.Venue, &fill.VenueFillID, &fill.OrderID, &fill.VenueOrderID, &fill.ExecutionAccountID,
		&fill.MarketID, &fill.ConditionID, &fill.TokenID, &side, &role, &status,
		&fill.Shares, &fill.Price, &fill.GrossNotional, &fill.FeeRateBPS,
		&fill.PlatformFeeRate, &fill.FeeExponent, &fill.PlatformFee,
		&fill.BuilderFeeRateBPS, &fill.BuilderFee, &fill.TotalFee, &fill.NetCashDelta,
		&settlementEvidence,
		&fill.FeeSource, &fill.TransactionHash, &fill.RawPayloadSHA256, &fill.MatchedAt, &venueUpdated,
		&fill.ObservedAt, &confirmed, &applied,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Fill{}, port.ErrFillNotFound
	}
	if err != nil {
		return domain.Fill{}, err
	}
	if payload := strings.TrimSpace(string(settlementEvidence)); payload != "" && payload != "{}" {
		var evidence domain.SettlementEvidence
		if err := json.Unmarshal(settlementEvidence, &evidence); err != nil {
			return domain.Fill{}, fmt.Errorf("decode settlement evidence: %w", err)
		}
		fill.SettlementEvidence = &evidence
	}
	fill.Side = domain.Side(side)
	fill.LiquidityRole = domain.LiquidityRole(role)
	fill.Status = domain.FillStatus(status)
	if venueUpdated.Valid {
		fill.VenueUpdatedAt = venueUpdated.Time.UTC()
	}
	if confirmed.Valid {
		value := confirmed.Time.UTC()
		fill.ConfirmedAt = &value
	}
	if applied.Valid {
		value := applied.Time.UTC()
		fill.AppliedAt = &value
	}
	return fill.Normalize(), nil
}

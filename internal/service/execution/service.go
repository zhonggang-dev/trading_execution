package execution

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/domain/orderstate"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

var (
	ErrInvalidIntent         = errors.New("invalid order intent")
	ErrIdempotencyConflict   = errors.New("client order id is already used by a different intent")
	ErrIntentExpired         = errors.New("order intent has expired")
	ErrOrderNotCancelable    = errors.New("order is not in a cancelable state")
	ErrCancelFinalityPending = port.ErrCancelFinalityPending
)

const (
	clobFillDetailsUnavailableCode = "CLOB_FILL_DETAILS_UNAVAILABLE"
	venueFillEvidencePendingCode   = "VENUE_FILL_EVIDENCE_PENDING"
)

// Params 表示后端使用的 Params 类型。
type Params struct {
	Repository              port.OrderRepository
	Venue                   port.Venue
	Guard                   port.Guard
	MarketValidator         port.MarketValidator
	Reservations            port.AssetReservationManager
	Reconciliation          port.ReconciliationTriggerer
	FillSynchronizer        port.FillSynchronizer
	AuthoritativeFills      bool
	CancelFillFinalityGrace time.Duration
	ImmediateCancelFinality bool
	MaxReconcileAttempts    int
	// EntrySubmissionDisabled is a process-wide sell-only gate. It is enforced
	// here, below every caller (HTTP, decision cycle, and crash recovery), so a
	// BUY can never reach Venue.Place while the operator is exiting positions.
	EntrySubmissionDisabled bool
	// RequirePreparedPlacement is mandatory for live composition. Paper and
	// legacy in-memory tests may keep the one-step Venue contract.
	RequirePreparedPlacement bool
	Now                      func() time.Time
	NewID                    func() string
}

// Service 表示后端使用的 Service 类型。
type Service struct {
	repository               port.OrderRepository
	venue                    port.Venue
	guard                    port.Guard
	marketValidator          port.MarketValidator
	reservations             port.AssetReservationManager
	reconciliation           port.ReconciliationTriggerer
	fillSynchronizer         port.FillSynchronizer
	authoritativeFills       bool
	cancelFillFinalityGrace  time.Duration
	immediateCancelFinality  bool
	maxReconcileAttempts     int
	entrySubmissionDisabled  bool
	requirePreparedPlacement bool
	now                      func() time.Time
	newID                    func() string
}

// SubmitResult 表示后端使用的 SubmitResult 类型。
type SubmitResult = port.OrderSubmitResult

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.Repository == nil || params.Venue == nil || params.Guard == nil || params.MarketValidator == nil || params.Reservations == nil {
		return nil, fmt.Errorf("execution repository, venue, guard, market validator, and asset reservations are required")
	}
	if strings.TrimSpace(params.Venue.Name()) == "" {
		return nil, fmt.Errorf("execution venue name is required")
	}
	if params.RequirePreparedPlacement {
		if _, ok := params.Venue.(port.PreparedVenue); !ok {
			return nil, fmt.Errorf("live execution venue must support prepared placement")
		}
	}
	if params.AuthoritativeFills && params.FillSynchronizer == nil {
		return nil, fmt.Errorf("authoritative fills require a fill synchronizer")
	}
	if params.AuthoritativeFills && params.ImmediateCancelFinality {
		return nil, fmt.Errorf("authoritative fills cannot use immediate cancel finality")
	}
	if params.MaxReconcileAttempts == 0 {
		params.MaxReconcileAttempts = 5
	}
	if params.CancelFillFinalityGrace == 0 {
		params.CancelFillFinalityGrace = 30 * time.Second
	}
	if params.CancelFillFinalityGrace < time.Second || params.CancelFillFinalityGrace > 24*time.Hour {
		return nil, fmt.Errorf("cancel fill finality grace must be between one second and 24 hours")
	}
	if params.MaxReconcileAttempts < 1 || params.MaxReconcileAttempts > 100 {
		return nil, fmt.Errorf("max reconcile attempts must be between 1 and 100")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.NewID == nil {
		params.NewID = newOrderID
	}
	return &Service{
		repository:               params.Repository,
		venue:                    params.Venue,
		guard:                    params.Guard,
		marketValidator:          params.MarketValidator,
		reservations:             params.Reservations,
		reconciliation:           params.Reconciliation,
		fillSynchronizer:         params.FillSynchronizer,
		authoritativeFills:       params.AuthoritativeFills,
		cancelFillFinalityGrace:  params.CancelFillFinalityGrace,
		immediateCancelFinality:  params.ImmediateCancelFinality,
		maxReconcileAttempts:     params.MaxReconcileAttempts,
		entrySubmissionDisabled:  params.EntrySubmissionDisabled,
		requirePreparedPlacement: params.RequirePreparedPlacement,
		now:                      params.Now,
		newID:                    params.NewID,
	}, nil
}

// Submit 按幂等键受理订单意图并完成风控、市场校验、资产预占和交易所提交。
func (service *Service) Submit(ctx context.Context, intent domain.OrderIntent) (SubmitResult, error) {
	intent = intent.Normalize()
	if err := intent.Validate(); err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidIntent, err)
	}
	if intent.Venue != strings.ToLower(strings.TrimSpace(service.venue.Name())) {
		return SubmitResult{}, fmt.Errorf("%w: venue %q is not served by %q", ErrInvalidIntent, intent.Venue, service.venue.Name())
	}
	if existing, err := service.repository.GetByClientOrderID(ctx, intent.ClientOrderID); err == nil {
		if !existing.Intent.Equivalent(intent) {
			return SubmitResult{}, ErrIdempotencyConflict
		}
		return SubmitResult{Order: existing, Created: false}, nil
	} else if !errors.Is(err, port.ErrOrderNotFound) {
		return SubmitResult{}, fmt.Errorf("look up order intent: %w", err)
	}
	if service.entrySubmissionDisabled && intent.Side == domain.SideBuy {
		return SubmitResult{}, entrySubmissionDisabledError()
	}

	now := service.now().UTC()
	order, err := (domain.OrderParams{ID: service.newID(), Intent: intent, CreatedAt: now}).Build()
	if err != nil {
		return SubmitResult{}, fmt.Errorf("build order: %w", err)
	}
	stored, created, err := service.repository.Create(ctx, order)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("claim order intent: %w", err)
	}
	if !created {
		if !stored.Intent.Equivalent(intent) {
			return SubmitResult{}, ErrIdempotencyConflict
		}
		return SubmitResult{Order: stored, Created: false}, nil
	}
	if err := service.transition(ctx, &stored, domain.OrderStatusValidating, domain.TransitionTriggerValidation, transitionDetails{}); err != nil {
		return SubmitResult{Order: stored, Created: true}, fmt.Errorf("begin validation: %w", err)
	}
	if intent.ExpiresAt != nil && !intent.ExpiresAt.After(service.now().UTC()) {
		rejectErr := service.reject(ctx, &stored, "INTENT_EXPIRED", ErrIntentExpired)
		return SubmitResult{Order: stored, Created: true}, errors.Join(ErrIntentExpired, rejectErr)
	}
	if err := service.guard.Check(ctx, intent); err != nil {
		code := errorCode(err, "HARD_RISK_REJECTED")
		rejectErr := service.reject(ctx, &stored, code, err)
		return SubmitResult{Order: stored, Created: true}, errors.Join(err, rejectErr)
	}
	marketValidation, err := service.marketValidator.Validate(ctx, intent)
	if err != nil {
		code := errorCode(err, "MARKET_VALIDATION_FAILED")
		rejectErr := service.reject(ctx, &stored, code, err)
		return SubmitResult{Order: stored, Created: true}, errors.Join(err, rejectErr)
	}
	stored.MarketValidation = &marketValidation
	if _, err := service.reservations.Reserve(ctx, stored); err != nil {
		code := errorCode(err, "ASSET_RESERVATION_FAILED")
		rejectErr := service.reject(ctx, &stored, code, err)
		return SubmitResult{Order: stored, Created: true}, errors.Join(fmt.Errorf("reserve order assets: %w", err), rejectErr)
	}
	if err := service.transition(ctx, &stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, transitionDetails{}); err != nil {
		uncertainErr := service.reservations.MarkUncertain(ctx, stored, "RESERVATION_STATUS_PERSIST_FAILED: "+err.Error())
		return SubmitResult{Order: stored, Created: true}, errors.Join(fmt.Errorf("persist reserved order: %w", err), uncertainErr)
	}
	return service.submitReserved(ctx, stored, true)
}

// submitReserved 为已预占订单记录提交尝试并处理交易所结果。
func (service *Service) submitReserved(ctx context.Context, stored domain.Order, created bool) (SubmitResult, error) {
	if service.entrySubmissionDisabled && stored.Intent.Side == domain.SideBuy {
		cause := entrySubmissionDisabledError()
		if err := service.reject(ctx, &stored, cause.Code, cause); err != nil {
			uncertainErr := service.reservations.MarkUncertain(ctx, stored, "ENTRY_SUBMISSION_DISABLED_REJECTION_PERSIST_FAILED: "+err.Error())
			return SubmitResult{Order: stored, Created: created}, errors.Join(cause, err, uncertainErr)
		}
		_, releaseErr := service.reservations.Reconcile(ctx, stored)
		return SubmitResult{Order: stored, Created: created}, errors.Join(cause, releaseErr)
	}
	var prepared port.PreparedPlacement
	preparedVenue, supportsPrepared := service.venue.(port.PreparedVenue)
	if service.requirePreparedPlacement && !supportsPrepared {
		// New rejects this configuration, but keep the runtime check as a
		// fail-closed guard against an incorrectly assembled Service value.
		return SubmitResult{Order: stored, Created: created}, fmt.Errorf("live execution venue does not support prepared placement")
	}
	if supportsPrepared {
		var prepareErr error
		prepared, prepareErr = preparedVenue.PreparePlace(ctx, stored)
		if prepareErr != nil {
			return service.finishPrepareError(ctx, stored, prepareErr, created)
		}
		expectedVenueOrderID := ""
		if prepared != nil {
			expectedVenueOrderID = strings.TrimSpace(prepared.ExpectedVenueOrderID())
		}
		if expectedVenueOrderID == "" {
			return service.finishPrepareError(ctx, stored, errors.New("prepared placement omitted expected venue order id"), created)
		}
		if stored.VenueOrderID != "" && stored.VenueOrderID != expectedVenueOrderID {
			return service.finishPrepareError(ctx, stored, fmt.Errorf("prepared venue order id changed from %q to %q", stored.VenueOrderID, expectedVenueOrderID), created)
		}
		// StartAttempt atomically writes this hash to both execution_orders and
		// execution_order_attempts before PlacePrepared can issue the POST.
		stored.VenueOrderID = expectedVenueOrderID
	}
	attempt, err := service.startAttempt(ctx, &stored, domain.OrderStatusSubmitting, domain.OrderAttemptSubmit, domain.TransitionTriggerSubmit)
	if err != nil {
		var localRejection *port.Rejection
		if errors.As(err, &localRejection) {
			rejectErr := service.reject(ctx, &stored, localRejection.Code, localRejection)
			if rejectErr != nil {
				uncertainErr := service.reservations.MarkUncertain(ctx, stored, "LOCAL_SUBMIT_REJECTION_PERSIST_FAILED: "+rejectErr.Error())
				return SubmitResult{Order: stored, Created: created}, errors.Join(
					fmt.Errorf("start submit attempt: %w", err), rejectErr, uncertainErr,
				)
			}
			_, releaseErr := service.reservations.Reconcile(ctx, stored)
			return SubmitResult{Order: stored, Created: created}, errors.Join(
				fmt.Errorf("start submit attempt: %w", err), releaseErr,
			)
		}
		uncertainErr := service.reservations.MarkUncertain(ctx, stored, "SUBMIT_ATTEMPT_PERSIST_FAILED: "+err.Error())
		return SubmitResult{Order: stored, Created: created}, errors.Join(fmt.Errorf("start submit attempt: %w", err), uncertainErr)
	}
	var venueOrder port.VenueOrder
	var placeErr error
	if supportsPrepared {
		venueOrder, placeErr = preparedVenue.PlacePrepared(ctx, stored, prepared)
	} else {
		venueOrder, placeErr = service.venue.Place(ctx, stored)
	}
	if placeErr != nil {
		return service.finishSubmitError(ctx, stored, attempt, placeErr, created)
	}
	if strings.TrimSpace(venueOrder.ID) == "" {
		return service.finishSubmitError(ctx, stored, attempt, &port.VenueError{
			Kind:    port.VenueErrorAmbiguous,
			Code:    "INVALID_VENUE_RESPONSE",
			Message: "successful venue response omitted order id",
		}, created)
	}
	if err := validateVenueOrder(venueOrder, stored.Intent.Size); err != nil {
		return service.finishSubmitError(ctx, stored, attempt, &port.VenueError{
			Kind:         port.VenueErrorAmbiguous,
			Code:         "INVALID_VENUE_RESPONSE",
			Message:      err.Error(),
			VenueOrderID: venueOrder.ID,
		}, created)
	}
	if venueOrder.State == port.VenueOrderRejected {
		rejection := &port.VenueError{Kind: port.VenueErrorRejected, Code: "CLOB_REJECTED", Message: venueOrder.RawStatus}
		return service.finishSubmitError(ctx, stored, attempt, rejection, created)
	}

	// POST acknowledgement and fill/open state are separate audit events even
	// if Polymarket reports a synchronous match in the same response.
	acknowledgement := venueOrder
	acknowledgement.FilledSize = stored.FilledSize
	acknowledgement.AverageFillPrice = ""
	if err := service.finishAttempt(ctx, &stored, &attempt, domain.OrderStatusAcknowledged, domain.TransitionTriggerVenueResponse, acknowledgement, "", nil); err != nil {
		uncertainErr := service.reservations.MarkUncertain(ctx, stored, "SUBMIT_ACK_PERSIST_FAILED: "+err.Error())
		return SubmitResult{Order: stored, Created: created}, errors.Join(err, uncertainErr)
	}
	if venueOrder.State != port.VenueOrderAcknowledged {
		if err := service.applyVenueState(ctx, &stored, venueOrder, domain.TransitionTriggerVenueResponse, attempt.ID); err != nil {
			unknownErr := service.forceUnknown(ctx, &stored, "INVALID_VENUE_RESPONSE", err, venueOrder.ID)
			uncertainErr := service.reservations.MarkUncertain(ctx, stored, "INVALID_VENUE_RESPONSE: "+err.Error())
			return SubmitResult{Order: stored, Created: created}, errors.Join(err, unknownErr, uncertainErr)
		}
	}
	pendingFillEvidence, syncErr := service.syncObservedFills(ctx, &stored, venueOrder)
	if syncErr != nil {
		if stored.Status == domain.OrderStatusCancelled {
			return SubmitResult{Order: stored, Created: created}, errors.Join(syncErr, service.holdCancellationFinality(ctx, stored, true))
		}
		uncertainErr := service.reservations.MarkUncertain(ctx, stored, "AUTHORITATIVE_FILL_SYNC_FAILED: "+syncErr.Error())
		service.triggerReconciliation(stored, domain.ReconciliationTriggerOrderUnknown)
		return SubmitResult{Order: stored, Created: created}, errors.Join(syncErr, uncertainErr)
	}
	if pendingFillEvidence && stored.Status != domain.OrderStatusCancelled {
		if err := service.markFillEvidencePending(ctx, &stored, domain.ReconciliationTriggerOrderUnknown, true); err != nil {
			return SubmitResult{Order: stored, Created: created}, err
		}
		return SubmitResult{Order: stored, Created: created}, nil
	}
	if stored.Status == domain.OrderStatusUnknown {
		_ = service.reservations.MarkUncertain(ctx, stored, "VENUE_RESPONSE_UNKNOWN")
		service.triggerReconciliation(stored, domain.ReconciliationTriggerOrderUnknown)
	} else if stored.Status == domain.OrderStatusCancelled {
		if err := service.holdCancellationFinality(ctx, stored, true); err != nil {
			return SubmitResult{Order: stored, Created: created}, err
		}
	} else if stored.Status != domain.OrderStatusAcknowledged && !service.authoritativeFills {
		if _, err := service.reservations.Reconcile(ctx, stored); err != nil {
			uncertainErr := service.reservations.MarkUncertain(ctx, stored, "INITIAL_RECONCILIATION_FAILED: "+err.Error())
			return SubmitResult{Order: stored, Created: created}, errors.Join(fmt.Errorf("reconcile initial asset reservation: %w", err), uncertainErr)
		}
	}
	return SubmitResult{Order: stored, Created: created}, nil
}

func entrySubmissionDisabledError() *port.Rejection {
	return &port.Rejection{
		Code:   domain.StrategyEntryBlockSubmissionDisabled,
		Reason: "BUY submission is disabled while the operator-owned sell-only gate is active",
	}
}

// finishPrepareError handles only failures that occur before a venue POST is
// possible. They are definitive for this order attempt, so no STARTED attempt
// is created and the reservation can be released safely.
func (service *Service) finishPrepareError(ctx context.Context, order domain.Order, cause error, created bool) (SubmitResult, error) {
	code := errorCode(cause, "VENUE_PREPARE_FAILED")
	if err := service.reject(ctx, &order, code, cause); err != nil {
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "PREPARE_REJECTION_PERSIST_FAILED: "+err.Error())
		return SubmitResult{Order: order, Created: created}, errors.Join(fmt.Errorf("prepare venue order: %w", cause), err, uncertainErr)
	}
	_, releaseErr := service.reservations.Reconcile(ctx, order)
	return SubmitResult{Order: order, Created: created}, errors.Join(fmt.Errorf("prepare venue order: %w", cause), releaseErr)
}

// finishSubmitError 完成并持久化 Submit Error。
func (service *Service) finishSubmitError(ctx context.Context, order domain.Order, attempt domain.OrderAttempt, cause error, created bool) (SubmitResult, error) {
	target := domain.OrderStatusUnknown
	outcome := domain.AttemptOutcomeUnknown
	code := errorCode(cause, "VENUE_PLACE_OUTCOME_UNKNOWN")
	venueOrderID := venueOrderIDFromError(cause)
	if isDefinitiveRejection(cause) {
		target = domain.OrderStatusRejected
		outcome = domain.AttemptOutcomeRejected
	}
	venueOrder := port.VenueOrder{ID: venueOrderID, State: port.VenueOrderUnknown, RawStatus: code, FilledSize: "0", ObservedAt: service.now().UTC()}
	attempt.Outcome = outcome
	if err := service.finishAttemptWithOutcome(ctx, &order, &attempt, target, domain.TransitionTriggerVenueResponse, venueOrder, code, cause, outcome); err != nil {
		return SubmitResult{Order: order, Created: created}, errors.Join(fmt.Errorf("place venue order: %w", cause), err)
	}
	if target == domain.OrderStatusUnknown {
		uncertainErr := service.reservations.MarkUncertain(ctx, order, code+": "+cause.Error())
		service.triggerReconciliation(order, domain.ReconciliationTriggerOrderUnknown)
		return SubmitResult{Order: order, Created: created}, errors.Join(fmt.Errorf("place venue order: %w", cause), uncertainErr)
	}
	_, reconcileErr := service.reservations.Reconcile(ctx, order)
	if code == "CLOB_INSUFFICIENT_BALANCE_ALLOWANCE" {
		service.triggerReconciliation(order, domain.ReconciliationTriggerAssetDrift)
	}
	return SubmitResult{Order: order, Created: created}, errors.Join(fmt.Errorf("place venue order: %w", cause), reconcileErr)
}

// Resume 恢复尚未发起外部提交的中断订单并重新校验后继续执行。
func (service *Service) Resume(ctx context.Context, orderID string) (domain.Order, error) {
	order, err := service.Get(ctx, orderID)
	if err != nil || order.Terminal() {
		return order, err
	}
	if order.Status == domain.OrderStatusReceived {
		if err := service.transition(ctx, &order, domain.OrderStatusValidating, domain.TransitionTriggerValidation, transitionDetails{}); err != nil {
			return order, err
		}
	}
	if order.Status == domain.OrderStatusValidating {
		if order.Intent.ExpiresAt != nil && !order.Intent.ExpiresAt.After(service.now().UTC()) {
			rejectErr := service.reject(ctx, &order, "INTENT_EXPIRED", ErrIntentExpired)
			return order, errors.Join(ErrIntentExpired, rejectErr)
		}
		if err := service.guard.Check(ctx, order.Intent); err != nil {
			rejectErr := service.reject(ctx, &order, errorCode(err, "HARD_RISK_REJECTED"), err)
			return order, errors.Join(err, rejectErr)
		}
		validation, err := service.marketValidator.Validate(ctx, order.Intent)
		if err != nil {
			rejectErr := service.reject(ctx, &order, errorCode(err, "MARKET_VALIDATION_FAILED"), err)
			return order, errors.Join(err, rejectErr)
		}
		order.MarketValidation = &validation
		if _, err := service.reservations.Reserve(ctx, order); err != nil {
			rejectErr := service.reject(ctx, &order, errorCode(err, "ASSET_RESERVATION_FAILED"), err)
			return order, errors.Join(err, rejectErr)
		}
		if err := service.transition(ctx, &order, domain.OrderStatusReserved, domain.TransitionTriggerReservation, transitionDetails{}); err != nil {
			_ = service.reservations.MarkUncertain(ctx, order, "RESUME_RESERVATION_STATUS_FAILED: "+err.Error())
			return order, err
		}
	}
	if order.Status != domain.OrderStatusReserved {
		return order, fmt.Errorf("order status %s cannot be resumed before submit", order.Status)
	}
	if order.Intent.ExpiresAt != nil && !order.Intent.ExpiresAt.After(service.now().UTC()) {
		if err := service.reject(ctx, &order, "INTENT_EXPIRED", ErrIntentExpired); err != nil {
			return order, err
		}
		_, reconcileErr := service.reservations.Reconcile(ctx, order)
		return order, errors.Join(ErrIntentExpired, reconcileErr)
	}
	// Recheck market/tick/book after a crash; a persisted pre-crash snapshot may
	// no longer be safe. This self-transition atomically replaces the evidence.
	validation, err := service.marketValidator.Validate(ctx, order.Intent)
	if err != nil {
		if rejectErr := service.reject(ctx, &order, errorCode(err, "MARKET_REVALIDATION_FAILED"), err); rejectErr != nil {
			return order, errors.Join(err, rejectErr)
		}
		_, reconcileErr := service.reservations.Reconcile(ctx, order)
		return order, errors.Join(err, reconcileErr)
	}
	order.MarketValidation = &validation
	if err := service.transition(ctx, &order, domain.OrderStatusReserved, domain.TransitionTriggerValidation, transitionDetails{}); err != nil {
		return order, err
	}
	result, err := service.submitReserved(ctx, order, false)
	return result.Order, err
}

// Get 按标识查询并返回当前组件管理的记录。
func (service *Service) Get(ctx context.Context, orderID string) (domain.Order, error) {
	return service.repository.Get(ctx, strings.TrimSpace(orderID))
}

// Refresh 查询交易所最新状态并通过审计状态机对账本地订单。
func (service *Service) Refresh(ctx context.Context, orderID string) (domain.Order, error) {
	order, err := service.Get(ctx, orderID)
	if err != nil || order.Terminal() {
		return order, err
	}
	if order.Status == domain.OrderStatusSubmitting || order.Status == domain.OrderStatusReconciling {
		if err := service.forceUnknown(ctx, &order, "INCOMPLETE_SUBMIT_ATTEMPT", errors.New("process recovered while submit attempt was still STARTED"), order.VenueOrderID); err != nil {
			return order, err
		}
	}
	if order.Status != domain.OrderStatusUnknown && order.Status != domain.OrderStatusAcknowledged &&
		order.Status != domain.OrderStatusLive && order.Status != domain.OrderStatusPartiallyFilled &&
		order.Status != domain.OrderStatusCancelPending {
		return order, fmt.Errorf("order status %s cannot be reconciled", order.Status)
	}
	attempt, err := service.startAttempt(ctx, &order, domain.OrderStatusReconciling, domain.OrderAttemptReconcile, domain.TransitionTriggerReconciliation)
	if err != nil {
		return order, fmt.Errorf("start reconciliation attempt: %w", err)
	}
	venueOrder, getErr := service.venue.Get(ctx, order)
	if getErr != nil {
		code := errorCode(getErr, "RECONCILE_FAILED")
		fillEvidencePending := strings.EqualFold(strings.TrimSpace(code), clobFillDetailsUnavailableCode)
		target := domain.OrderStatusUnknown
		outcome := domain.AttemptOutcomeUnknown
		if !fillEvidencePending && service.reconcileAttemptCount(ctx, order.ID) >= service.maxReconcileAttempts {
			target = domain.OrderStatusManualReview
			outcome = domain.AttemptOutcomeFailed
		}
		placeholder := port.VenueOrder{ID: order.VenueOrderID, State: port.VenueOrderUnknown, RawStatus: code, FilledSize: order.FilledSize, ObservedAt: service.now().UTC()}
		if fillEvidencePending {
			// The adapter deliberately returns normalized order evidence together
			// with the enrichment error. Preserve its raw venue status/timestamp in
			// the audit event while keeping execution amounts ledger-authoritative.
			placeholder = venueOrder
			placeholder.State = port.VenueOrderUnknown
			placeholder.ID = firstNonEmpty(placeholder.ID, order.VenueOrderID)
			placeholder.RawStatus = firstNonEmpty(placeholder.RawStatus, code)
			if placeholder.FilledSize.IsEmpty() {
				placeholder.FilledSize = order.FilledSize
			}
			if placeholder.ObservedAt.IsZero() {
				placeholder.ObservedAt = service.now().UTC()
			}
		}
		if err := service.finishAttemptWithOutcome(ctx, &order, &attempt, target, domain.TransitionTriggerReconciliation, placeholder, code, getErr, outcome); err != nil {
			return order, errors.Join(fmt.Errorf("get venue order: %w", getErr), err)
		}
		if target == domain.OrderStatusUnknown {
			if fillEvidencePending {
				// A CLOB response may carry normalized size/trade-id evidence together
				// with this error. Let the FillLedger try to recover confirmed trades;
				// the aggregate venue size remains observation-only.
				stillPending, syncErr := service.syncObservedFills(ctx, &order, venueOrder)
				// Refresh may itself be running inside a focused reconciliation. Keep
				// the order recoverable for the scheduled scan, but do not enqueue a
				// new runner request from inside the same failing path.
				var transitionErr error
				if stillPending && order.Status != domain.OrderStatusUnknown {
					transitionErr = service.transition(ctx, &order, domain.OrderStatusUnknown, domain.TransitionTriggerReconciliation, transitionDetails{
						reasonCode: venueFillEvidencePendingCode,
						reason:     "venue-reported execution is awaiting confirmation by the authoritative fill ledger",
					})
				}
				var pendingErr error
				if stillPending || syncErr != nil {
					pendingErr = service.reservations.MarkUncertain(ctx, order, venueFillEvidencePendingCode)
				}
				if pendingErr != nil {
					pendingErr = fmt.Errorf("retain reservation while confirmed fill evidence is pending: %w", pendingErr)
				}
				// Missing CLOB fill enrichment is an expected eventual-consistency
				// state, not a failed reconciliation run. The attempt retains getErr
				// for audit; only local synchronization/retention failures escape.
				return order, errors.Join(syncErr, transitionErr, pendingErr)
			}
			_ = service.reservations.MarkUncertain(ctx, order, "RECONCILE_FAILED: "+getErr.Error())
		}
		return order, fmt.Errorf("get venue order: %w", getErr)
	}
	target, err := service.statusForVenueObservation(order, venueOrder.State)
	if err != nil {
		target = domain.OrderStatusUnknown
	}
	if err := service.finishAttempt(ctx, &order, &attempt, target, domain.TransitionTriggerReconciliation, venueOrder, "", nil); err != nil {
		_ = service.reservations.MarkUncertain(ctx, order, "RECONCILE_PERSIST_FAILED: "+err.Error())
		return order, err
	}
	pendingFillEvidence, syncErr := service.syncObservedFills(ctx, &order, venueOrder)
	if syncErr != nil {
		if order.Status == domain.OrderStatusCancelled {
			return order, errors.Join(syncErr, service.holdCancellationFinality(ctx, order, false))
		}
		pendingErr := service.markFillEvidencePending(ctx, &order, domain.ReconciliationTriggerOrderUnknown, false)
		return order, errors.Join(syncErr, pendingErr)
	}
	if pendingFillEvidence && order.Status != domain.OrderStatusCancelled {
		if err := service.markFillEvidencePending(ctx, &order, domain.ReconciliationTriggerOrderUnknown, false); err != nil {
			return order, err
		}
		return order, nil
	}
	if order.Status == domain.OrderStatusUnknown || order.Status == domain.OrderStatusAcknowledged {
		_ = service.reservations.MarkUncertain(ctx, order, "RECONCILIATION_NOT_FINAL")
		return order, nil
	}
	if order.Status == domain.OrderStatusCancelled {
		return order, service.holdCancellationFinality(ctx, order, false)
	}
	if service.authoritativeFills && order.Status != domain.OrderStatusRejected {
		return order, nil
	}
	if _, err := service.reservations.Reconcile(ctx, order); err != nil {
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "REFRESH_RESERVATION_RECONCILIATION_FAILED: "+err.Error())
		return order, errors.Join(fmt.Errorf("reconcile refreshed asset reservation: %w", err), uncertainErr)
	}
	return order, nil
}

// Cancel 为可撤订单记录撤单尝试并处理明确或不确定的交易所结果。
func (service *Service) Cancel(ctx context.Context, orderID string) (domain.Order, error) {
	order, err := service.Get(ctx, orderID)
	if err != nil || order.Terminal() {
		return order, err
	}
	if order.Status != domain.OrderStatusAcknowledged && order.Status != domain.OrderStatusLive && order.Status != domain.OrderStatusPartiallyFilled {
		return order, fmt.Errorf("%w: %s", ErrOrderNotCancelable, order.Status)
	}
	attempt, err := service.startAttempt(ctx, &order, domain.OrderStatusCancelPending, domain.OrderAttemptCancel, domain.TransitionTriggerCancel)
	if err != nil {
		return order, fmt.Errorf("start cancel attempt: %w", err)
	}
	venueOrder, cancelErr := service.venue.Cancel(ctx, order)
	if cancelErr != nil {
		placeholder := port.VenueOrder{ID: firstNonEmpty(venueOrderIDFromError(cancelErr), order.VenueOrderID), State: port.VenueOrderUnknown, RawStatus: errorCode(cancelErr, "VENUE_CANCEL_OUTCOME_UNKNOWN"), FilledSize: order.FilledSize, ObservedAt: service.now().UTC()}
		if err := service.finishAttemptWithOutcome(ctx, &order, &attempt, domain.OrderStatusUnknown, domain.TransitionTriggerCancel, placeholder, errorCode(cancelErr, "VENUE_CANCEL_OUTCOME_UNKNOWN"), cancelErr, domain.AttemptOutcomeUnknown); err != nil {
			return order, errors.Join(fmt.Errorf("cancel venue order: %w", cancelErr), err)
		}
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "VENUE_CANCEL_OUTCOME_UNKNOWN: "+cancelErr.Error())
		service.triggerReconciliation(order, domain.ReconciliationTriggerCancelUnknown)
		return order, errors.Join(fmt.Errorf("cancel venue order: %w", cancelErr), uncertainErr)
	}
	target, err := service.statusForVenueObservation(order, venueOrder.State)
	if err != nil || venueOrder.State == port.VenueOrderAcknowledged || venueOrder.State == port.VenueOrderRejected {
		target = domain.OrderStatusUnknown
	}
	if err := service.finishAttempt(ctx, &order, &attempt, target, domain.TransitionTriggerCancel, venueOrder, "", nil); err != nil {
		_ = service.reservations.MarkUncertain(ctx, order, "CANCEL_RESULT_PERSIST_FAILED: "+err.Error())
		return order, err
	}
	pendingFillEvidence, syncErr := service.syncObservedFills(ctx, &order, venueOrder)
	if syncErr != nil {
		if order.Status == domain.OrderStatusCancelled {
			return order, errors.Join(syncErr, service.holdCancellationFinality(ctx, order, true))
		}
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "AUTHORITATIVE_FILL_SYNC_FAILED: "+syncErr.Error())
		service.triggerReconciliation(order, domain.ReconciliationTriggerCancelUnknown)
		return order, errors.Join(syncErr, uncertainErr)
	}
	if pendingFillEvidence && order.Status != domain.OrderStatusCancelled {
		if err := service.markFillEvidencePending(ctx, &order, domain.ReconciliationTriggerCancelUnknown, true); err != nil {
			return order, err
		}
		return order, nil
	}
	if order.Status == domain.OrderStatusUnknown {
		_ = service.reservations.MarkUncertain(ctx, order, "CANCEL_RESULT_UNKNOWN")
		service.triggerReconciliation(order, domain.ReconciliationTriggerCancelUnknown)
		return order, nil
	}
	if order.Status == domain.OrderStatusCancelled {
		return order, service.holdCancellationFinality(ctx, order, true)
	}
	if service.authoritativeFills {
		return order, nil
	}
	if _, err := service.reservations.Reconcile(ctx, order); err != nil {
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "CANCEL_RECONCILIATION_FAILED: "+err.Error())
		return order, errors.Join(fmt.Errorf("reconcile canceled asset reservation: %w", err), uncertainErr)
	}
	return order, nil
}

// FinalizeCancellation 在成交传播宽限期结束后释放已撤订单的剩余预占。
func (service *Service) FinalizeCancellation(ctx context.Context, orderID string) (domain.Order, error) {
	order, err := service.Get(ctx, orderID)
	if err != nil {
		return order, err
	}
	if order.Status == domain.OrderStatusFilled {
		return order, nil
	}
	if order.Status != domain.OrderStatusCancelled {
		return order, fmt.Errorf("order status %s is not awaiting cancel finality", order.Status)
	}
	if order.VenueLastObservedAt == nil || service.now().UTC().Sub(order.VenueLastObservedAt.UTC()) < service.cancelFillFinalityGrace {
		return order, ErrCancelFinalityPending
	}
	if _, err := service.reservations.Reconcile(ctx, order); err != nil {
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "CANCEL_FINALITY_RECONCILIATION_FAILED: "+err.Error())
		return order, errors.Join(fmt.Errorf("finalize cancelled reservation: %w", err), uncertainErr)
	}
	return order, nil
}

// holdCancellationFinality 保留撤单后的资产预占并触发成交终局对账。
func (service *Service) holdCancellationFinality(ctx context.Context, order domain.Order, enqueue bool) error {
	if service.immediateCancelFinality {
		if _, err := service.reservations.Reconcile(ctx, order); err != nil {
			uncertainErr := service.reservations.MarkUncertain(ctx, order, "IMMEDIATE_CANCEL_RECONCILIATION_FAILED: "+err.Error())
			return errors.Join(fmt.Errorf("release immediately cancelled reservation: %w", err), uncertainErr)
		}
		return nil
	}
	err := service.reservations.MarkUncertain(ctx, order, "CANCEL_FILL_FINALITY_PENDING")
	if enqueue {
		service.triggerReconciliation(order, domain.ReconciliationTriggerCancelUnknown)
	}
	return err
}

// triggerReconciliation 触发 Reconciliation。
func (service *Service) triggerReconciliation(order domain.Order, trigger domain.ReconciliationTrigger) {
	if service.reconciliation == nil {
		return
	}
	service.reconciliation.Trigger(order.Intent.ExecutionAccountID, trigger, order.ID)
}

// syncObservedFills routes any venue indication of a possible fill through the
// confirmed-fill synchronizer, then reloads the order aggregate written by the
// FillLedger. The CLOB order response is observation evidence only.
func (service *Service) syncObservedFills(ctx context.Context, order *domain.Order, observed port.VenueOrder) (bool, error) {
	if !service.authoritativeFills || !venueObservationNeedsFillSync(observed) {
		return false, nil
	}
	_, syncErr := service.fillSynchronizer.SyncOrder(ctx, order.ID)
	fillEvidencePending := isFillEvidencePendingError(syncErr)
	if fillEvidencePending {
		// MATCHED/MINED/RETRYING is an expected propagation state. Keep the
		// reservation and order durable, but do not turn the reconciliation
		// runner unhealthy merely because final settlement evidence is not ready.
		syncErr = nil
	}
	refreshed, reloadErr := service.repository.Get(ctx, order.ID)
	if reloadErr == nil {
		*order = refreshed
	}
	if syncErr != nil {
		syncErr = fmt.Errorf("sync authoritative fills for order %s: %w", order.ID, syncErr)
	}
	if reloadErr != nil {
		reloadErr = fmt.Errorf("reload order %s after authoritative fill sync: %w", order.ID, reloadErr)
	}
	return fillEvidencePending || authoritativeFillEvidencePending(observed, *order), errors.Join(syncErr, reloadErr)
}

func isFillEvidencePendingError(err error) bool {
	var venueError *port.VenueError
	return errors.As(err, &venueError) &&
		strings.EqualFold(strings.TrimSpace(venueError.Code), clobFillDetailsUnavailableCode)
}

// markFillEvidencePending keeps collateral frozen until a later reconciliation
// observes the corresponding confirmed trades.
func (service *Service) markFillEvidencePending(
	ctx context.Context,
	order *domain.Order,
	trigger domain.ReconciliationTrigger,
	enqueue bool,
) error {
	// Persist the reason even when the venue observation already mapped to
	// UNKNOWN. Without this audited self-transition, the fast coordinator cannot
	// distinguish durable fill enrichment from an ordinary retryable read error.
	transitionErr := service.transition(ctx, order, domain.OrderStatusUnknown, domain.TransitionTriggerReconciliation, transitionDetails{
		reasonCode: venueFillEvidencePendingCode,
		reason:     "venue-reported execution is awaiting confirmation by the authoritative fill ledger",
	})
	uncertainErr := service.reservations.MarkUncertain(ctx, *order, venueFillEvidencePendingCode)
	if enqueue {
		service.triggerReconciliation(*order, trigger)
	}
	if uncertainErr != nil {
		uncertainErr = fmt.Errorf("retain reservation while confirmed fill evidence is pending: %w", uncertainErr)
	}
	return errors.Join(transitionErr, uncertainErr)
}

// venueObservationNeedsFillSync identifies observations that may race with the
// venue's confirmed trade feed. A successful cancel always requires one final
// pre-finality scan, even when size_matched is zero.
func venueObservationNeedsFillSync(observed port.VenueOrder) bool {
	if len(observed.TradeIDs) > 0 {
		return true
	}
	switch observed.State {
	case port.VenueOrderPartiallyFilled, port.VenueOrderFilled, port.VenueOrderCancelled:
		return true
	}
	filled := observed.FilledSize
	if filled.IsEmpty() {
		return false
	}
	sign, err := filled.Sign()
	return err != nil || sign > 0
}

// authoritativeFillEvidencePending reports whether the venue has observed more
// execution than the confirmed-fill ledger has applied so far.
func authoritativeFillEvidencePending(observed port.VenueOrder, order domain.Order) bool {
	venueFilled := observed.FilledSize
	if venueFilled.IsEmpty() {
		venueFilled = "0"
	}
	confirmedFilled := order.FilledSize
	if confirmedFilled.IsEmpty() {
		confirmedFilled = "0"
	}
	comparison, err := venueFilled.Compare(confirmedFilled)
	if err != nil || comparison > 0 {
		return true
	}
	confirmedSign, err := confirmedFilled.Sign()
	if err != nil {
		return true
	}
	if confirmedSign == 0 && len(observed.TradeIDs) > 0 {
		return true
	}
	switch observed.State {
	case port.VenueOrderFilled:
		return order.Status != domain.OrderStatusFilled
	case port.VenueOrderPartiallyFilled:
		return confirmedSign <= 0 || (order.Status != domain.OrderStatusPartiallyFilled &&
			order.Status != domain.OrderStatusFilled && order.Status != domain.OrderStatusCancelled)
	default:
		return false
	}
}

// Events 查询指定订单的不可变状态事件。
func (service *Service) Events(ctx context.Context, orderID string) ([]domain.OrderEvent, error) {
	return service.repository.Events(ctx, strings.TrimSpace(orderID))
}

// Attempts 查询指定订单的外部操作尝试。
func (service *Service) Attempts(ctx context.Context, orderID string) ([]domain.OrderAttempt, error) {
	return service.repository.Attempts(ctx, strings.TrimSpace(orderID))
}

// transitionDetails 表示后端使用的 transitionDetails 类型。
type transitionDetails struct {
	attemptID        string
	reasonCode       string
	reason           string
	venueOrder       port.VenueOrder
	includeVenueData bool
}

// transition 迁移并持久化 对应数据。
func (service *Service) transition(ctx context.Context, order *domain.Order, target domain.OrderStatus, trigger domain.OrderTransitionTrigger, details transitionDetails) error {
	transition := service.buildTransition(*order, target, trigger, details)
	next, event, err := orderstate.Apply(*order, transition)
	if err != nil {
		return err
	}
	if err := service.repository.Transition(ctx, next, event); err != nil {
		return err
	}
	*order = next
	return nil
}

// startAttempt 开始并持久化 Attempt。
func (service *Service) startAttempt(ctx context.Context, order *domain.Order, target domain.OrderStatus, kind domain.OrderAttemptKind, trigger domain.OrderTransitionTrigger) (domain.OrderAttempt, error) {
	existing, err := service.repository.Attempts(ctx, order.ID)
	if err != nil {
		return domain.OrderAttempt{}, err
	}
	sequence := len(existing) + 1
	startedAt := service.now().UTC()
	attempt, err := (domain.OrderAttemptParams{
		ID:                 fmt.Sprintf("attempt:%s:%d", order.ID, sequence),
		OrderID:            order.ID,
		Sequence:           sequence,
		Kind:               kind,
		RequestFingerprint: attemptFingerprint(kind, *order),
		VenueOrderID:       order.VenueOrderID,
		StartedAt:          startedAt,
	}).Build()
	if err != nil {
		return domain.OrderAttempt{}, err
	}
	transition := service.buildTransition(*order, target, trigger, transitionDetails{attemptID: attempt.ID})
	transition.At = startedAt
	next, event, err := orderstate.Apply(*order, transition)
	if err != nil {
		return domain.OrderAttempt{}, err
	}
	if err := service.repository.StartAttempt(ctx, next, event, attempt); err != nil {
		return domain.OrderAttempt{}, err
	}
	*order = next
	return attempt, nil
}

// finishAttempt 完成并持久化 Attempt。
func (service *Service) finishAttempt(ctx context.Context, order *domain.Order, attempt *domain.OrderAttempt, target domain.OrderStatus, trigger domain.OrderTransitionTrigger, venueOrder port.VenueOrder, reasonCode string, cause error) error {
	outcome := domain.AttemptOutcomeSucceeded
	if target == domain.OrderStatusRejected {
		outcome = domain.AttemptOutcomeRejected
	} else if target == domain.OrderStatusUnknown || target == domain.OrderStatusManualReview {
		outcome = domain.AttemptOutcomeUnknown
	}
	return service.finishAttemptWithOutcome(ctx, order, attempt, target, trigger, venueOrder, reasonCode, cause, outcome)
}

// finishAttemptWithOutcome 完成并持久化 Attempt With Outcome。
func (service *Service) finishAttemptWithOutcome(ctx context.Context, order *domain.Order, attempt *domain.OrderAttempt, target domain.OrderStatus, trigger domain.OrderTransitionTrigger, venueOrder port.VenueOrder, reasonCode string, cause error, outcome domain.OrderAttemptOutcome) error {
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	details := transitionDetails{
		attemptID:        attempt.ID,
		reasonCode:       reasonCode,
		reason:           reason,
		venueOrder:       venueOrder,
		includeVenueData: true,
	}
	transition := service.buildTransition(*order, target, trigger, details)
	next, event, err := orderstate.Apply(*order, transition)
	if err != nil {
		return err
	}
	completedAt := transition.At
	attempt.Outcome = outcome
	attempt.CompletedAt = &completedAt
	attempt.VenueOrderID = firstNonEmpty(venueOrder.ID, attempt.VenueOrderID)
	attempt.VenueStatus = venueOrder.RawStatus
	attempt.ErrorCode = reasonCode
	attempt.ErrorMessage = reason
	var venueError *port.VenueError
	if errors.As(cause, &venueError) {
		attempt.HTTPStatus = venueError.HTTPStatus
	}
	if err := service.repository.FinishAttempt(ctx, next, event, *attempt); err != nil {
		return err
	}
	*order = next
	return nil
}

// buildTransition 根据订单、触发原因和交易所观察构建状态迁移参数。
func (service *Service) buildTransition(order domain.Order, target domain.OrderStatus, trigger domain.OrderTransitionTrigger, details transitionDetails) orderstate.Transition {
	transition := orderstate.Transition{
		EventID:    fmt.Sprintf("event:%s:%d", order.ID, order.Revision+1),
		To:         target,
		Trigger:    trigger,
		AttemptID:  details.attemptID,
		ReasonCode: details.reasonCode,
		Reason:     details.reason,
		At:         service.now().UTC(),
	}
	if details.includeVenueData {
		venueOrder := details.venueOrder
		transition.VenueStatus = venueOrder.RawStatus
		transition.VenueOrderID = venueOrder.ID
		if service.authoritativeFills {
			// In live composition only confirmed fills processed by FillLedger may
			// change cumulative execution amounts.
			transition.FilledSize = order.FilledSize
			transition.AverageFillPrice = order.AverageFillPrice
		} else {
			transition.FilledSize = venueOrder.FilledSize
			transition.AverageFillPrice = venueOrder.AverageFillPrice
		}
		if !venueOrder.ObservedAt.IsZero() {
			observedAt := venueOrder.ObservedAt.UTC()
			transition.VenueObservedAt = &observedAt
		}
	}
	return transition
}

// applyVenueState 应用 Venue State 的领域变更。
func (service *Service) applyVenueState(ctx context.Context, order *domain.Order, venueOrder port.VenueOrder, trigger domain.OrderTransitionTrigger, attemptID string) error {
	target, err := service.statusForVenueObservation(*order, venueOrder.State)
	if err != nil {
		return err
	}
	return service.transition(ctx, order, target, trigger, transitionDetails{
		attemptID:        attemptID,
		venueOrder:       venueOrder,
		includeVenueData: true,
	})
}

// statusForVenueObservation maps order-state evidence without allowing an
// unconfirmed CLOB size_matched value to create PARTIALLY_FILLED or FILLED.
func (service *Service) statusForVenueObservation(order domain.Order, state port.VenueOrderState) (domain.OrderStatus, error) {
	target, err := statusForVenue(state)
	if err != nil || !service.authoritativeFills {
		return target, err
	}
	filled := order.FilledSize
	if filled.IsEmpty() {
		filled = "0"
	}
	sign, err := filled.Sign()
	if err != nil {
		return "", fmt.Errorf("parse authoritative filled size: %w", err)
	}
	comparison, err := filled.Compare(order.Intent.Size)
	if err != nil || comparison > 0 {
		return "", fmt.Errorf("compare authoritative filled size with requested size")
	}
	if target == domain.OrderStatusRejected && sign > 0 {
		// A post-ack rejection cannot release an order that already has a
		// confirmed fill; require reconciliation instead.
		return domain.OrderStatusUnknown, nil
	}
	switch target {
	case domain.OrderStatusAcknowledged, domain.OrderStatusLive, domain.OrderStatusPartiallyFilled, domain.OrderStatusFilled:
		if comparison == 0 {
			return domain.OrderStatusFilled, nil
		}
		if sign > 0 {
			return domain.OrderStatusPartiallyFilled, nil
		}
		if target == domain.OrderStatusAcknowledged {
			return domain.OrderStatusAcknowledged, nil
		}
		return domain.OrderStatusLive, nil
	default:
		return target, nil
	}
}

// reject 构建并返回 对应数据 的拒绝结果。
func (service *Service) reject(ctx context.Context, order *domain.Order, code string, cause error) error {
	return service.transition(ctx, order, domain.OrderStatusRejected, domain.TransitionTriggerValidation, transitionDetails{
		reasonCode: code,
		reason:     cause.Error(),
	})
}

// forceUnknown 将不确定外部结果通过审计状态机迁移为 UNKNOWN。
func (service *Service) forceUnknown(ctx context.Context, order *domain.Order, code string, cause error, venueOrderID string) error {
	return service.transition(ctx, order, domain.OrderStatusUnknown, domain.TransitionTriggerVenueResponse, transitionDetails{
		reasonCode: code,
		reason:     cause.Error(),
		venueOrder: port.VenueOrder{
			ID:         firstNonEmpty(venueOrderID, order.VenueOrderID),
			State:      port.VenueOrderUnknown,
			RawStatus:  code,
			FilledSize: order.FilledSize,
			ObservedAt: service.now().UTC(),
		},
		includeVenueData: true,
	})
}

// reconcileAttemptCount 对账并处理 Attempt Count。
func (service *Service) reconcileAttemptCount(ctx context.Context, orderID string) int {
	attempts, err := service.repository.Attempts(ctx, orderID)
	if err != nil {
		return 0
	}
	count := 0
	for _, attempt := range attempts {
		if attempt.Kind == domain.OrderAttemptReconcile &&
			!strings.EqualFold(strings.TrimSpace(attempt.ErrorCode), clobFillDetailsUnavailableCode) {
			count++
		}
	}
	return count
}

// statusForVenue 将外部或订单状态映射为目标领域状态。
func statusForVenue(state port.VenueOrderState) (domain.OrderStatus, error) {
	switch state {
	case port.VenueOrderAcknowledged:
		return domain.OrderStatusAcknowledged, nil
	case port.VenueOrderLive:
		return domain.OrderStatusLive, nil
	case port.VenueOrderPartiallyFilled:
		return domain.OrderStatusPartiallyFilled, nil
	case port.VenueOrderFilled:
		return domain.OrderStatusFilled, nil
	case port.VenueOrderCancelled:
		return domain.OrderStatusCancelled, nil
	case port.VenueOrderRejected:
		return domain.OrderStatusRejected, nil
	case port.VenueOrderUnknown:
		return domain.OrderStatusUnknown, nil
	default:
		return "", fmt.Errorf("unsupported venue order state %q", state)
	}
}

// validateVenueOrder 校验 Venue Order 的字段和业务约束。
func validateVenueOrder(order port.VenueOrder, requestedSize domain.Decimal) error {
	if strings.TrimSpace(order.ID) == "" {
		return fmt.Errorf("venue order id is required")
	}
	if _, err := statusForVenue(order.State); err != nil {
		return err
	}
	filled := order.FilledSize
	if filled.IsEmpty() {
		filled = "0"
	}
	if sign, err := filled.Sign(); err != nil || sign < 0 {
		return fmt.Errorf("filled size must be non-negative")
	}
	if comparison, err := filled.Compare(requestedSize); err != nil || comparison > 0 {
		return fmt.Errorf("filled size exceeds requested size")
	}
	return nil
}

// isDefinitiveRejection 判断当前业务条件是否成立。
func isDefinitiveRejection(err error) bool {
	var venueError *port.VenueError
	return errors.As(err, &venueError) && (venueError.Kind == port.VenueErrorRejected || venueError.Kind == port.VenueErrorInvalid)
}

// errorCode 从领域拒绝或交易所错误中提取稳定错误码。
func errorCode(err error, fallback string) string {
	var rejection *port.Rejection
	if errors.As(err, &rejection) && strings.TrimSpace(rejection.Code) != "" {
		return rejection.Code
	}
	var venueError *port.VenueError
	if errors.As(err, &venueError) && strings.TrimSpace(venueError.Code) != "" {
		return venueError.Code
	}
	return fallback
}

// venueOrderIDFromError 从交易所错误中提取可能已生成的订单标识。
func venueOrderIDFromError(err error) string {
	var venueError *port.VenueError
	if errors.As(err, &venueError) {
		return strings.TrimSpace(venueError.VenueOrderID)
	}
	return ""
}

// attemptFingerprint 根据稳定业务身份生成幂等标识。
func attemptFingerprint(kind domain.OrderAttemptKind, order domain.Order) string {
	payload, _ := json.Marshal(struct {
		Kind         domain.OrderAttemptKind `json:"kind"`
		OrderID      string                  `json:"order_id"`
		Intent       domain.OrderIntent      `json:"intent"`
		VenueOrderID string                  `json:"venue_order_id,omitempty"`
	}{Kind: kind, OrderID: order.ID, Intent: order.Intent.Normalize(), VenueOrderID: order.VenueOrderID})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// firstNonEmpty 返回 Non Empty 中第一个非空值。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// newOrderID 生成随机订单标识并在熵源失败时降级。
func newOrderID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("ord-%d", time.Now().UnixNano())
	}
	return "ord-" + hex.EncodeToString(buffer)
}

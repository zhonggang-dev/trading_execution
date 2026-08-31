package orderstate

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var (
	ErrIllegalTransition  = errors.New("illegal order status transition")
	ErrInvalidObservation = errors.New("invalid venue observation")
)

// Transition 描述一次订单聚合状态迁移及其不可变审计凭据。
type Transition struct {
	EventID          string
	To               domain.OrderStatus
	Trigger          domain.OrderTransitionTrigger
	AttemptID        string
	FillKey          string
	ReasonCode       string
	Reason           string
	VenueStatus      string
	VenueOrderID     string
	FilledSize       domain.Decimal
	FilledNotional   domain.Decimal
	TotalFees        domain.Decimal
	AverageFillPrice domain.Decimal
	VenueObservedAt  *time.Time
	At               time.Time
}

// Apply 校验状态迁移并同时构建下一版订单聚合和不可变订单事件。
func Apply(current domain.Order, transition Transition) (domain.Order, domain.OrderEvent, error) {
	if !current.CanTransitionTo(transition.To) {
		return current, domain.OrderEvent{}, fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, current.Status, transition.To)
	}
	if strings.TrimSpace(transition.EventID) == "" {
		return current, domain.OrderEvent{}, fmt.Errorf("event_id is required")
	}
	if transition.At.IsZero() {
		return current, domain.OrderEvent{}, fmt.Errorf("transition time is required")
	}
	at := transition.At.UTC()
	if at.Before(current.UpdatedAt.UTC()) {
		return current, domain.OrderEvent{}, fmt.Errorf("transition time moved backward")
	}

	next := domain.CloneOrder(current)
	if err := applyVenueObservation(&next, transition); err != nil {
		return current, domain.OrderEvent{}, err
	}
	if err := validateFillState(next, transition.To); err != nil {
		return current, domain.OrderEvent{}, err
	}
	next.Status = transition.To
	next.FailureCode = strings.TrimSpace(transition.ReasonCode)
	next.FailureReason = strings.TrimSpace(transition.Reason)
	next.UpdatedAt = at
	next.Revision++

	event := domain.OrderEvent{
		ID:              strings.TrimSpace(transition.EventID),
		OrderID:         next.ID,
		Revision:        next.Revision,
		FromStatus:      current.Status,
		ToStatus:        next.Status,
		Trigger:         transition.Trigger,
		AttemptID:       strings.TrimSpace(transition.AttemptID),
		FillKey:         strings.TrimSpace(transition.FillKey),
		ReasonCode:      next.FailureCode,
		Reason:          next.FailureReason,
		VenueStatus:     strings.TrimSpace(transition.VenueStatus),
		VenueOrderID:    next.VenueOrderID,
		FilledSize:      next.FilledSize,
		FilledNotional:  next.FilledNotional,
		TotalFees:       next.TotalFees,
		FillPrice:       next.AverageFillPrice,
		VenueObservedAt: cloneTime(next.VenueLastObservedAt),
		OccurredAt:      at,
	}
	return next, event, nil
}

// applyVenueObservation 将交易所观察值安全合并到订单聚合。
func applyVenueObservation(order *domain.Order, transition Transition) error {
	venueOrderID := strings.TrimSpace(transition.VenueOrderID)
	if venueOrderID != "" {
		if order.VenueOrderID != "" && order.VenueOrderID != venueOrderID {
			clientOrderID := strings.TrimSpace(order.Intent.ClientOrderID)
			canAdoptAuthoritativeID := strings.EqualFold(strings.TrimSpace(order.Intent.Venue), "kalshi") &&
				clientOrderID != "" && order.VenueOrderID == clientOrderID
			if !canAdoptAuthoritativeID {
				return fmt.Errorf("%w: venue order id changed from %q to %q", ErrInvalidObservation, order.VenueOrderID, venueOrderID)
			}
		}
		order.VenueOrderID = venueOrderID
	}
	if transition.VenueObservedAt != nil {
		observedAt := transition.VenueObservedAt.UTC()
		if observedAt.IsZero() {
			return fmt.Errorf("%w: venue observation time is zero", ErrInvalidObservation)
		}
		if order.VenueLastObservedAt != nil && observedAt.Before(order.VenueLastObservedAt.UTC()) {
			return fmt.Errorf("%w: venue observation moved backward", ErrInvalidObservation)
		}
		order.VenueLastObservedAt = &observedAt
	}
	if !transition.FilledSize.IsEmpty() {
		if err := validateCumulativeFill(*order, transition.FilledSize); err != nil {
			return err
		}
		order.FilledSize = transition.FilledSize
	}
	if !transition.FilledNotional.IsEmpty() {
		if err := validateCumulativeDecimal("filled notional", order.FilledNotional, transition.FilledNotional); err != nil {
			return err
		}
		order.FilledNotional = transition.FilledNotional
	}
	if !transition.TotalFees.IsEmpty() {
		if err := validateCumulativeDecimal("total fees", order.TotalFees, transition.TotalFees); err != nil {
			return err
		}
		order.TotalFees = transition.TotalFees
	}
	if !transition.AverageFillPrice.IsEmpty() {
		if sign, err := transition.AverageFillPrice.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("%w: average fill price must be positive", ErrInvalidObservation)
		}
		order.AverageFillPrice = transition.AverageFillPrice
	}
	return nil
}

// validateCumulativeDecimal 校验累计小数值非负且不会回退。
func validateCumulativeDecimal(name string, current, next domain.Decimal) error {
	if current.IsEmpty() {
		current = "0"
	}
	if sign, err := next.Sign(); err != nil || sign < 0 {
		return fmt.Errorf("%w: %s must be non-negative", ErrInvalidObservation, name)
	}
	if comparison, err := next.Compare(current); err != nil || comparison < 0 {
		return fmt.Errorf("%w: cumulative %s moved backward", ErrInvalidObservation, name)
	}
	return nil
}

// validateCumulativeFill 校验累计成交数量不回退且不超过委托数量。
func validateCumulativeFill(order domain.Order, filled domain.Decimal) error {
	if sign, err := filled.Sign(); err != nil || sign < 0 {
		return fmt.Errorf("%w: filled size must be non-negative", ErrInvalidObservation)
	}
	current := order.FilledSize
	if current.IsEmpty() {
		current = "0"
	}
	if comparison, err := filled.Compare(current); err != nil || comparison < 0 {
		return fmt.Errorf("%w: cumulative filled size moved backward", ErrInvalidObservation)
	}
	if comparison, err := filled.Compare(order.Intent.Size); err != nil || comparison > 0 {
		return fmt.Errorf("%w: cumulative filled size exceeds requested size", ErrInvalidObservation)
	}
	return nil
}

// validateFillState 校验目标订单状态与累计成交数量保持一致。
func validateFillState(order domain.Order, status domain.OrderStatus) error {
	filled := order.FilledSize
	if filled.IsEmpty() {
		filled = "0"
	}
	comparison, err := filled.Compare(order.Intent.Size)
	if err != nil {
		return fmt.Errorf("%w: compare filled and requested size: %v", ErrInvalidObservation, err)
	}
	sign, err := filled.Sign()
	if err != nil {
		return fmt.Errorf("%w: parse filled size: %v", ErrInvalidObservation, err)
	}
	switch status {
	case domain.OrderStatusPartiallyFilled:
		if sign <= 0 || comparison >= 0 {
			return fmt.Errorf("%w: PARTIALLY_FILLED requires 0 < filled_size < order size", ErrInvalidObservation)
		}
	case domain.OrderStatusFilled:
		if comparison != 0 {
			return fmt.Errorf("%w: FILLED requires filled_size equal to order size", ErrInvalidObservation)
		}
	case domain.OrderStatusRejected:
		if sign != 0 {
			return fmt.Errorf("%w: REJECTED cannot contain a fill", ErrInvalidObservation)
		}
	}
	return nil
}

// cloneTime 复制时间指针以避免订单和事件共享可变地址。
func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

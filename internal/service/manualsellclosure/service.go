// Package manualsellclosure is an operator-only, evidence-gated maintenance
// path. It is deliberately not wired into the server or an automatic scanner.
package manualsellclosure

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const ReasonCode = "POLYMARKET_AUDITED_CANCEL_CONFIRMED"

type Target struct {
	Account, Order, VenueOrder, Market, Condition, Token, Lot, Wallet string
	Trade, Transaction                                                string
	Block, Log                                                        uint64
	Requested, Filled, Price, Gross, Remaining                        domain.Decimal
}

func (t Target) Fingerprint() string {
	b, _ := json.Marshal(t)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type Source interface {
	Get(context.Context, domain.Order) (port.VenueOrder, error)
	ListOrderFills(context.Context, domain.Order) ([]domain.Fill, error)
}
type Reservations interface {
	port.AssetReservationManager
	port.OrderReservationReader
}
type Synchronizer interface {
	SyncOrder(context.Context, string) (port.FillSyncResult, error)
}
type Params struct {
	Target       Target
	Orders       port.OrderRepository
	Reservations Reservations
	Leases       port.OrderRecoveryLeaseStore
	Source       Source
	Synchronizer Synchronizer
	// CheckLocal must verify the exact already-applied fill and cash event,
	// not merely equal aggregate balances. It must be read-only.
	CheckLocal func(context.Context, domain.Order) error
	Grace      time.Duration
	Now        func() time.Time
	Wait       func(context.Context, time.Duration) error
	Audit      func(string, any)
}
type Result struct {
	AlreadyClosed bool                    `json:"already_closed"`
	Applied       bool                    `json:"applied"`
	Order         domain.Order            `json:"order"`
	Reservation   domain.AssetReservation `json:"reservation"`
}

func Run(ctx context.Context, p Params, apply bool) (Result, error) {
	if p.Orders == nil || p.Reservations == nil || p.Leases == nil || p.Source == nil || p.Synchronizer == nil || p.CheckLocal == nil || p.Grace < time.Second || p.Grace > time.Minute {
		return Result{}, fmt.Errorf("complete maintenance dependencies and bounded finality grace required")
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Audit == nil {
		p.Audit = func(string, any) {}
	}
	if p.Wait == nil {
		p.Wait = func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	read := func() (domain.Order, domain.AssetReservation, error) {
		o, err := p.Orders.Get(ctx, p.Target.Order)
		if err != nil {
			return o, domain.AssetReservation{}, err
		}
		r, err := p.Reservations.GetOrderReservation(ctx, o.ID)
		if err != nil {
			return o, r, err
		}
		if err = validateLocal(p.Target, o, r); err != nil {
			return o, r, err
		}
		return o, r, p.CheckLocal(ctx, o)
	}
	o, r, err := read()
	if err != nil {
		return Result{}, err
	}
	p.Audit("before", Result{Order: o, Reservation: r})
	first, err := observe(ctx, p, o)
	if err != nil {
		return Result{}, err
	}
	if o.Status == domain.OrderStatusCancelled && r.Status == domain.ReservationStatusReleased {
		return Result{AlreadyClosed: true, Order: o, Reservation: r}, nil
	}
	if err = p.Wait(ctx, p.Grace+time.Second); err != nil {
		return Result{}, err
	}
	resolved := false
	if apply {
		holder := fmt.Sprintf("manual-sell-closure:%s:%d", p.Target.Fingerprint()[:12], p.Now().UnixNano())
		_, err = p.Leases.AcquireOrderRecoveryLease(ctx, domain.OrderRecoveryLeaseRequest{OrderID: o.ID, ExecutionAccountID: o.Intent.ExecutionAccountID, OrderRevision: o.Revision, Holder: holder, Now: p.Now().UTC(), TTL: 3 * time.Minute, BypassBackoff: true})
		if err != nil {
			return Result{}, err
		}
		defer func() {
			kind := domain.OrderRecoveryOutcomeWaiting
			if resolved {
				kind = domain.OrderRecoveryOutcomeResolved
			}
			releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, e := p.Leases.ReleaseOrderRecoveryLease(releaseCtx, domain.OrderRecoveryRelease{OrderID: o.ID, Holder: holder, Now: p.Now().UTC(), Kind: kind, NextRetryAt: p.Now().UTC().Add(time.Minute)})
			if e != nil {
				p.Audit("lease_release_error", e.Error())
			}
		}()
	}
	o, r, err = read()
	if err != nil {
		return Result{}, err
	}
	second, err := observe(ctx, p, o)
	if err != nil {
		return Result{}, err
	}
	if second.ObservedAt.Sub(first.ObservedAt) < p.Grace {
		return Result{}, fmt.Errorf("independent cancellation observations do not span finality grace")
	}
	if !apply {
		return Result{Order: o, Reservation: r}, nil
	}
	// The fill is already applied. Replay only through the normal ledger and
	// require a duplicate, never manufacture a fill from cumulative quantities.
	sync, err := p.Synchronizer.SyncOrder(ctx, o.ID)
	p.Audit("fill_replay", map[string]any{"observed": sync.Observed, "applied": sync.Applied, "duplicates": sync.Duplicates})
	if err != nil {
		return Result{}, err
	}
	if sync.Observed != 1 || sync.Applied != 0 || sync.Duplicates != 1 {
		return Result{}, fmt.Errorf("unexpected authoritative fill replay; reservation remains protected")
	}
	o, r, err = read()
	if err != nil {
		return Result{}, err
	}
	latest, err := observe(ctx, p, o)
	if err != nil {
		return Result{}, err
	}
	if latest.ObservedAt.Sub(first.ObservedAt) < p.Grace {
		return Result{}, fmt.Errorf("finality observation moved backward")
	}
	if o.Status == domain.OrderStatusManualReview {
		// This explicit OPERATOR transition is separate from the automated
		// state machine, which intentionally cannot clear MANUAL_REVIEW.
		next := domain.CloneOrder(o)
		next.Status = domain.OrderStatusCancelled
		next.Revision++
		next.UpdatedAt = p.Now().UTC()
		next.VenueLastObservedAt = &latest.ObservedAt
		next.FailureCode = ReasonCode
		next.FailureReason = "operator-authorized exact cancellation closure; evidence_sha256=" + p.Target.Fingerprint()
		event := domain.OrderEvent{ID: fmt.Sprintf("event:%s:%d", o.ID, next.Revision), OrderID: o.ID, Revision: next.Revision, FromStatus: o.Status, ToStatus: next.Status, Trigger: domain.TransitionTriggerOperator, ReasonCode: ReasonCode, Reason: next.FailureReason, VenueStatus: latest.RawStatus, VenueOrderID: o.VenueOrderID, FilledSize: o.FilledSize, FilledNotional: o.FilledNotional, TotalFees: o.TotalFees, FillPrice: o.AverageFillPrice, VenueObservedAt: &latest.ObservedAt, OccurredAt: next.UpdatedAt}
		if err = p.Orders.Transition(ctx, next, event); err != nil {
			return Result{}, err
		}
		p.Audit("operator_transition", event)
	}
	// A crash after the audited transition leaves a CANCELLED order with the
	// reservation still frozen. A rerun resumes here after rechecking evidence.
	o, r, err = read()
	if err != nil {
		return Result{}, err
	}
	if r.Status != domain.ReservationStatusReleased {
		if _, err = p.Reservations.Reconcile(ctx, o); err != nil {
			return Result{}, err
		}
	}
	o, r, err = read()
	if err != nil {
		return Result{}, err
	}
	if o.Status != domain.OrderStatusCancelled || r.Status != domain.ReservationStatusReleased {
		return Result{}, fmt.Errorf("closure not complete")
	}
	resolved = true
	result := Result{Applied: true, Order: o, Reservation: r}
	p.Audit("after", result)
	return result, nil
}

func validateLocal(t Target, o domain.Order, r domain.AssetReservation) error {
	if t.Account == "" || t.Order == "" || t.Lot == "" || t.Wallet == "" || t.Trade == "" || t.Transaction == "" {
		return fmt.Errorf("explicit complete target required")
	}
	if o.ID != t.Order || o.Intent.ExecutionAccountID != t.Account || o.VenueOrderID != t.VenueOrder || o.Intent.Venue != "polymarket" || o.Intent.MarketID != t.Market || o.Intent.ConditionID != t.Condition || o.Intent.TokenID != t.Token || o.Intent.TargetLotID != t.Lot || o.Intent.Side != domain.SideSell || o.Intent.TimeInForce != domain.TimeInForceIOC || !o.Intent.Size.Equal(t.Requested) || !o.Intent.Price.Equal(t.Price) || !o.Intent.WorstPrice.Equal(t.Price) || !o.FilledSize.Equal(t.Filled) || !o.FilledNotional.Equal(t.Gross) || !o.AverageFillPrice.Equal(t.Price) || !o.TotalFees.Equal("0") {
		return fmt.Errorf("order does not match reviewed SELL identity and applied totals")
	}
	if o.Status == domain.OrderStatusManualReview {
		if o.FailureCode != "RECONCILE_FAILED" || o.FailureReason != "CLOB size_matched exceeds original_size" {
			return fmt.Errorf("manual-review cause changed")
		}
	} else if o.Status != domain.OrderStatusCancelled || o.FailureCode != ReasonCode || !strings.Contains(o.FailureReason, "evidence_sha256="+t.Fingerprint()) {
		return fmt.Errorf("only reviewed manual state or this exact resumable closure is accepted")
	}
	if r.OrderID != o.ID || r.ClientOrderID != o.Intent.ClientOrderID || r.ExecutionAccountID != t.Account || r.StrategyID != o.Intent.StrategyID || r.MarketID != t.Market || r.TokenID != t.Token || r.TargetLotID != t.Lot || r.Side != domain.SideSell || !r.RequestedShares.Equal(t.Requested) || !r.InitialReservedShares.Equal(t.Requested) || !r.InitialReservedBalance.Equal("0") || !r.RemainingReservedBalance.Equal("0") || !r.SettledShares.Equal(t.Filled) || !r.SettledNotional.Equal(t.Gross) || !r.SettledFees.Equal("0") {
		return fmt.Errorf("reservation identity or applied settlement differs")
	}
	if r.Status == domain.ReservationStatusReleased && o.Status == domain.OrderStatusCancelled && r.RemainingReservedShares.Equal("0") {
		return nil
	}
	if r.Status != domain.ReservationStatusReconciliationRequired || !r.RemainingReservedShares.Equal(t.Remaining) {
		return fmt.Errorf("remaining reservation changed")
	}
	return nil
}

func observe(ctx context.Context, p Params, o domain.Order) (port.VenueOrder, error) {
	v, err := p.Source.Get(ctx, o)
	if err != nil {
		return v, err
	}
	t := p.Target
	age := p.Now().UTC().Sub(v.ObservedAt)
	if v.ID != t.VenueOrder || v.State != port.VenueOrderCancelled || !v.FilledSize.Equal(t.Filled) || !v.AverageFillPrice.Equal(t.Price) || len(v.TradeIDs) != 1 || v.TradeIDs[0] != t.Trade || v.ObservedAt.IsZero() || age < -time.Second || age > 20*time.Second {
		return v, fmt.Errorf("fresh exact cancellation evidence required")
	}
	fills, err := p.Source.ListOrderFills(ctx, o)
	if err != nil {
		return v, err
	}
	if len(fills) != 1 {
		return v, fmt.Errorf("complete fill enumeration changed")
	}
	f := fills[0]
	e := f.SettlementEvidence
	if err = ValidateFill(t, f); err != nil {
		return v, err
	}
	p.Audit("confirmed_observation", map[string]any{"order": v, "receipt": e, "fill": f, "evidence_sha256": t.Fingerprint()})
	return v, nil
}

// ValidateFill validates both a freshly verified receipt and its durable copy.
func ValidateFill(t Target, f domain.Fill) error {
	e := f.SettlementEvidence
	if f.Venue != "polymarket" || f.OrderID != t.Order || f.ExecutionAccountID != t.Account || f.VenueOrderID != t.VenueOrder || f.VenueFillID != t.Trade || f.TransactionHash != t.Transaction || f.MarketID != t.Market || f.ConditionID != t.Condition || f.TokenID != t.Token || f.Side != domain.SideSell || f.Status != domain.FillStatusConfirmed || !f.Shares.Equal(t.Filled) || !f.Price.Equal(t.Price) || !f.GrossNotional.Equal(t.Gross) || !f.TotalFee.Equal("0") || e == nil || e.MakerAddress != t.Wallet || e.BlockNumber != t.Block || e.LogIndex != t.Log || e.Confirmations < 64 {
		return fmt.Errorf("exact finalized fill receipt differs")
	}
	return e.ValidateAgainst(f)
}

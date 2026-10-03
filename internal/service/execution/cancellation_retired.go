package execution

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// retiredCancellationMinAge is how long a confirmed cancellation must have
// stood before the venue's trade feed alone may release the remainder. It
// covers CLOB trade propagation and the on-chain confirmation depth.
const retiredCancellationMinAge = 10 * time.Minute

// finalizeRetiredCancellation completes cancel finality for a partially filled
// order whose GET-order record the venue has retired (HTTP 200/null). The
// order-level cumulative size is unavailable, so the venue's trade feed is the
// only evidence: every trade it lists must be CONFIRMED with receipt evidence
// (the adapter fails on any non-terminal trade), already applied to the ledger,
// and sum exactly to the local filled size. A zero-fill order has no positive
// evidence that the order ever existed and stays frozen for an operator.
func (service *Service) finalizeRetiredCancellation(ctx context.Context, order domain.Order, getErr error) (domain.Order, error) {
	failRead := func() (domain.Order, error) {
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "CANCEL_FINALITY_ORDER_READ_FAILED: "+getErr.Error())
		deferErr := service.deferCancellationFinality(ctx, &order, "CANCEL_FINALITY_ORDER_READ_FAILED", getErr.Error())
		return order, errors.Join(fmt.Errorf("read cancelled venue order before finality: %w", getErr), uncertainErr, deferErr)
	}
	if sign, err := order.FilledSize.Sign(); err != nil || sign <= 0 || order.VenueLastObservedAt == nil ||
		service.now().UTC().Sub(order.VenueLastObservedAt.UTC()) < max(service.cancelFillFinalityGrace, retiredCancellationMinAge) {
		return failRead()
	}
	pending := func(reason string) (domain.Order, error) {
		_ = service.reservations.MarkUncertain(ctx, order, "CANCEL_FINALITY_RETIRED_ORDER_EVIDENCE_PENDING: "+reason)
		deferErr := service.deferCancellationFinality(ctx, &order, "CANCEL_FINALITY_RETIRED_ORDER_EVIDENCE_PENDING", reason)
		return order, errors.Join(ErrCancelFinalityPending, deferErr)
	}
	result, syncErr := service.fillSynchronizer.SyncOrder(ctx, order.ID)
	if syncErr != nil {
		if refreshed, err := service.repository.Get(ctx, order.ID); err == nil {
			order = refreshed
		}
		if isFillEvidencePendingError(syncErr) {
			return pending("authoritative fill details are not visible yet")
		}
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "CANCEL_FINALITY_FILL_SYNC_FAILED: "+syncErr.Error())
		deferErr := service.deferCancellationFinality(ctx, &order, "CANCEL_FINALITY_FILL_SYNC_FAILED", syncErr.Error())
		return order, errors.Join(fmt.Errorf("sync retired cancelled order fills: %w", syncErr), uncertainErr, deferErr)
	}
	refreshed, err := service.repository.Get(ctx, order.ID)
	if err != nil {
		return order, fmt.Errorf("reload order %s after retired-order fill sync: %w", order.ID, err)
	}
	order = refreshed
	if order.Status == domain.OrderStatusFilled {
		return order, nil
	}
	if order.Status != domain.OrderStatusCancelled {
		return pending("order left the cancelled state during fill synchronization")
	}
	// A fill applied just now means the ledger changed under this pass; let the
	// next pass observe a stable, fully duplicate trade set before releasing.
	if result.Observed == 0 || result.Applied != 0 || len(result.Applications) != result.Observed {
		return pending("trade feed has not shown a stable, fully applied fill set")
	}
	total := new(big.Rat)
	for _, application := range result.Applications {
		if application.Fill.Status != domain.FillStatusConfirmed || !application.Duplicate {
			return pending("trade feed contains a fill that is not confirmed and already applied")
		}
		shares, ok := new(big.Rat).SetString(string(application.Fill.Shares))
		if !ok {
			return pending("trade feed fill size is not a decimal")
		}
		total.Add(total, shares)
	}
	local, ok := new(big.Rat).SetString(string(order.FilledSize))
	if !ok || total.Cmp(local) != 0 {
		_ = service.reservations.MarkUncertain(ctx, order, "CANCEL_FINALITY_FILL_TOTAL_MISMATCH")
		deferErr := service.deferCancellationFinality(ctx, &order, "CANCEL_FINALITY_FILL_TOTAL_MISMATCH", "trade feed and local cumulative fill totals do not match")
		return order, errors.Join(ErrCancelFinalityPending, deferErr)
	}
	if _, err := service.reservations.Reconcile(ctx, order); err != nil {
		uncertainErr := service.reservations.MarkUncertain(ctx, order, "CANCEL_FINALITY_RECONCILIATION_FAILED: "+err.Error())
		deferErr := service.deferCancellationFinality(ctx, &order, "CANCEL_FINALITY_RECONCILIATION_FAILED", err.Error())
		return order, errors.Join(fmt.Errorf("finalize cancelled reservation: %w", err), uncertainErr, deferErr)
	}
	return order, nil
}

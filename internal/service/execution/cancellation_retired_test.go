package execution_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
)

type tradeFeedSynchronizer struct {
	result port.FillSyncResult
	err    error
}

func (s *tradeFeedSynchronizer) SyncOrder(context.Context, string) (port.FillSyncResult, error) {
	return s.result, s.err
}

func retiredFill(shares domain.Decimal, status domain.FillStatus, duplicate bool) domain.FillApplication {
	return domain.FillApplication{Fill: domain.Fill{Shares: shares, Status: status}, Duplicate: duplicate}
}

// TestFinalizeCancellationUsesTradeFeedWhenVenueRetiredOrder 验证 CLOB 查不到订单时只有完整、已确认、已入账且总量一致的成交流才能释放剩余预占。
func TestFinalizeCancellationUsesTradeFeedWhenVenueRetiredOrder(t *testing.T) {
	notFound := &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_ORDER_NOT_FOUND", Message: "CLOB returned no order"}
	confirmed := func(shares domain.Decimal) port.FillSyncResult {
		return port.FillSyncResult{Observed: 1, Duplicates: 1, Applications: []domain.FillApplication{retiredFill(shares, domain.FillStatusConfirmed, true)}}
	}
	cases := []struct {
		name          string
		filled        domain.Decimal
		observedAge   time.Duration
		result        port.FillSyncResult
		syncErr       error
		wantRelease   bool
		wantPending   bool
		wantUncertain bool
	}{
		{name: "release_exact_confirmed_applied", filled: "7.92", observedAge: time.Hour, result: confirmed("7.92"), wantRelease: true},
		{name: "two_fills_sum_to_total", filled: "7.92", observedAge: time.Hour, result: port.FillSyncResult{Observed: 2, Duplicates: 2, Applications: []domain.FillApplication{
			retiredFill("5", domain.FillStatusConfirmed, true), retiredFill("2.92", domain.FillStatusConfirmed, true)}}, wantRelease: true},
		{name: "recent_cancellation", filled: "7.92", observedAge: time.Minute, result: confirmed("7.92"), wantUncertain: true},
		{name: "zero_fill_stays_frozen", filled: "0", observedAge: time.Hour, result: port.FillSyncResult{}, wantUncertain: true},
		{name: "fill_applied_this_pass", filled: "7.92", observedAge: time.Hour, result: port.FillSyncResult{Observed: 1, Applied: 1, Applications: []domain.FillApplication{retiredFill("7.92", domain.FillStatusConfirmed, false)}}, wantPending: true},
		{name: "no_trades_listed", filled: "7.92", observedAge: time.Hour, result: port.FillSyncResult{}, wantPending: true},
		{name: "fill_not_confirmed", filled: "7.92", observedAge: time.Hour, result: port.FillSyncResult{Observed: 1, Duplicates: 1, Applications: []domain.FillApplication{retiredFill("7.92", domain.FillStatusMined, true)}}, wantPending: true},
		{name: "feed_total_below_local", filled: "7.92", observedAge: time.Hour, result: confirmed("5"), wantPending: true},
		{name: "feed_total_above_local", filled: "7.92", observedAge: time.Hour, result: confirmed("9"), wantPending: true},
		{name: "sync_error", filled: "7.92", observedAge: time.Hour, syncErr: errors.New("trade source unavailable"), wantUncertain: true},
		{name: "fill_details_pending", filled: "7.92", observedAge: time.Hour, syncErr: &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE"}, wantPending: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			observedAt := now.Add(-tc.observedAge)
			repo := memory.NewOrderRepository()
			order := domain.Order{ID: "retired-order", VenueOrderID: "retired-venue-order", Status: domain.OrderStatusCancelled, Revision: 2, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-time.Minute), VenueLastObservedAt: &observedAt, FilledSize: tc.filled, FilledNotional: "3.66696", TotalFees: "0", AverageFillPrice: "0.463", Intent: domain.OrderIntent{ClientOrderID: "retired-client", ExecutionAccountID: "account-1", StrategyID: "strategy-1", Venue: "polymarket", MarketID: "market-1", ConditionID: "condition-1", TokenID: "token-1", TargetLotID: "lot-1", Side: domain.SideSell, Size: "10", Price: "0.463", WorstPrice: "0.463", TimeInForce: domain.TimeInForceIOC}}
			if _, _, err := repo.Create(ctx, order); err != nil {
				t.Fatal(err)
			}
			reservations := &successfulReservations{}
			venue := &fakeVenue{getErr: notFound}
			svc, err := execution.New(execution.Params{Repository: repo, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{}, Reservations: reservations, FillSynchronizer: &tradeFeedSynchronizer{result: tc.result, err: tc.syncErr}, AuthoritativeFills: true})
			if err != nil {
				t.Fatal(err)
			}
			_, err = svc.FinalizeCancellation(ctx, order.ID)
			if tc.wantRelease {
				if err != nil || reservations.reconcileCalls.Load() != 1 {
					t.Fatalf("FinalizeCancellation() err=%v reconcile=%d, want release", err, reservations.reconcileCalls.Load())
				}
				return
			}
			if err == nil || reservations.reconcileCalls.Load() != 0 {
				t.Fatalf("FinalizeCancellation() err=%v reconcile=%d, want frozen", err, reservations.reconcileCalls.Load())
			}
			if tc.wantPending && !errors.Is(err, execution.ErrCancelFinalityPending) {
				t.Fatalf("err=%v, want pending", err)
			}
			if reservations.uncertainCalls.Load() == 0 {
				t.Fatal("reservation must be re-marked uncertain while frozen")
			}
		})
	}
}

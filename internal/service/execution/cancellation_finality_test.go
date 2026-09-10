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

type finalizedReservations struct {
	*successfulReservations
	value domain.AssetReservation
	err   error
}

func (r *finalizedReservations) GetOrderReservation(context.Context, string) (domain.AssetReservation, error) {
	return r.value, r.err
}

func TestFinalizedCancellationSurvivesRetiredVenueOrder(t *testing.T) {
	for _, name := range []string{"released", "still_protected", "nonzero_remaining", "wrong_account", "wrong_settled_total", "missing_release_time", "reservation_read_failed", "trade_read_failed", "late_full_fill"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			repo := memory.NewOrderRepository()
			o := domain.Order{ID: "finalized-order", VenueOrderID: "archived-venue-order", Status: domain.OrderStatusCancelled, Revision: 2, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Minute), FilledSize: "19.01", FilledNotional: "0.72238", TotalFees: "0", AverageFillPrice: "0.038", Intent: domain.OrderIntent{ClientOrderID: "finalized-client", ExecutionAccountID: "account-1", StrategyID: "strategy-1", Venue: "polymarket-paper", MarketID: "market-1", ConditionID: "condition-1", TokenID: "token-1", TargetLotID: "lot-1", Side: domain.SideSell, Size: "48", Price: "0.038", WorstPrice: "0.038", TimeInForce: domain.TimeInForceIOC}}
			if _, _, err := repo.Create(ctx, o); err != nil {
				t.Fatal(err)
			}
			r := &finalizedReservations{successfulReservations: &successfulReservations{}, value: domain.AssetReservation{OrderID: o.ID, ClientOrderID: o.Intent.ClientOrderID, ExecutionAccountID: o.Intent.ExecutionAccountID, StrategyID: o.Intent.StrategyID, MarketID: o.Intent.MarketID, TokenID: o.Intent.TokenID, TargetLotID: o.Intent.TargetLotID, Side: o.Intent.Side, RequestedShares: o.Intent.Size, Status: domain.ReservationStatusReleased, RemainingReservedBalance: "0", RemainingReservedShares: "0", SettledShares: o.FilledSize, SettledNotional: o.FilledNotional, SettledFees: o.TotalFees, ReleasedAt: &now}}
			venue := &fakeVenue{getErr: &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_ORDER_NOT_FOUND", Message: "HTTP 200 null"}}
			sync := &fakeFillSynchronizer{}
			switch name {
			case "still_protected":
				r.value.Status = domain.ReservationStatusReconciliationRequired
				r.value.RemainingReservedShares = "28.99"
				r.value.ReleasedAt = nil
			case "nonzero_remaining":
				r.value.RemainingReservedShares = "28.99"
			case "wrong_account":
				r.value.ExecutionAccountID = "other"
			case "wrong_settled_total":
				r.value.SettledShares = "18"
			case "missing_release_time":
				r.value.ReleasedAt = nil
			case "reservation_read_failed":
				r.err = errors.New("database unavailable")
			case "trade_read_failed":
				sync.sync = func(context.Context, string) error { return errors.New("trade source unavailable") }
			case "late_full_fill":
				sync.sync = func(ctx context.Context, id string) error {
					return persistConfirmedFill(ctx, repo, id, domain.OrderStatusFilled, "48", "0.038")
				}
			}
			svc, err := execution.New(execution.Params{Repository: repo, Venue: venue, Guard: allowGuard{}, MarketValidator: allowMarketValidator{}, Reservations: r, FillSynchronizer: sync, AuthoritativeFills: true})
			if err != nil {
				t.Fatal(err)
			}
			got, err := svc.FinalizeCancellation(ctx, o.ID)
			if name == "released" || name == "late_full_fill" {
				if err != nil {
					t.Fatal(err)
				}
				if sync.calls.Load() != 1 || venue.getCalls.Load() != 0 || r.reconcileCalls.Load() != 0 || r.uncertainCalls.Load() != 0 {
					t.Fatal("finalized order must sync trades without GET, release, or re-gating")
				}
				if name == "released" && got.Revision != o.Revision {
					t.Fatal("historical finality check mutated the order")
				}
				if name == "late_full_fill" && (got.Status != domain.OrderStatusFilled || !got.FilledSize.Equal("48")) {
					t.Fatal("late confirmed fill was not retained")
				}
			} else if err == nil {
				t.Fatal("incomplete or inconsistent evidence unexpectedly passed")
			}
			if r.reconcileCalls.Load() != 0 {
				t.Fatal("none of these cases may release additional assets")
			}
			if name == "still_protected" && (venue.getCalls.Load() != 1 || r.uncertainCalls.Load() != 1) {
				t.Fatal("still-protected cancellation must retain original fail-closed path")
			}
		})
	}
}

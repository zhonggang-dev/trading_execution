package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type fakeOrdersWithAppliedFills struct {
	fakeOrders
	applied []domain.AppliedFillRef
	readErr error
}

func (repository *fakeOrdersWithAppliedFills) ListAppliedConfirmedFills(context.Context, string, time.Time) ([]domain.AppliedFillRef, error) {
	return repository.applied, repository.readErr
}

// TestSettledFilledOrdersSkipPerOrderFillSync 验证只有“FILLED、交易所成交全部 CONFIRMED 且本地已入账”的订单才跳过逐单成交同步，其余情况照旧同步。
func TestSettledFilledOrdersSkipPerOrderFillSync(t *testing.T) {
	trade := func(id string, status domain.FillStatus) domain.VenueTradeSnapshot {
		return domain.VenueTradeSnapshot{VenueTradeID: id, OrderIDs: []string{"venue-o"}, ConditionID: "condition-1", TokenID: "token-1", Status: status, ObservedAt: testNow}
	}
	ref := func(id string) domain.AppliedFillRef {
		return domain.AppliedFillRef{VenueOrderID: "venue-o", VenueFillID: id}
	}
	cases := []struct {
		name     string
		status   domain.OrderStatus
		focus    bool
		trades   []domain.VenueTradeSnapshot
		tradeErr error
		applied  []domain.AppliedFillRef
		readErr  error
		noRepo   bool
		wantSync bool
	}{
		{name: "settled_skips", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, applied: []domain.AppliedFillRef{ref("t1")}},
		{name: "two_trades_all_applied_skips", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed), trade("t2", domain.FillStatusConfirmed)}, applied: []domain.AppliedFillRef{ref("t1"), ref("t2")}},
		{name: "one_trade_not_applied", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed), trade("t2", domain.FillStatusConfirmed)}, applied: []domain.AppliedFillRef{ref("t1")}, wantSync: true},
		{name: "venue_trade_not_confirmed", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusMined)}, applied: []domain.AppliedFillRef{ref("t1")}, wantSync: true},
		{name: "no_local_fills", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, wantSync: true},
		{name: "fill_belongs_to_other_order", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, applied: []domain.AppliedFillRef{{VenueOrderID: "other", VenueFillID: "t1"}}, wantSync: true},
		{name: "no_venue_trade_listed_unchanged_no_sync", status: domain.OrderStatusFilled, applied: []domain.AppliedFillRef{ref("t1")}},
		{name: "applied_read_failed", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, readErr: errors.New("db down"), wantSync: true},
		{name: "repository_without_support", status: domain.OrderStatusFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, noRepo: true, wantSync: true},
		{name: "trade_scan_failed", status: domain.OrderStatusFilled, tradeErr: errors.New("clob down"), applied: []domain.AppliedFillRef{ref("t1")}, wantSync: false},
		{name: "focus_order_always_syncs", status: domain.OrderStatusFilled, focus: true, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, applied: []domain.AppliedFillRef{ref("t1")}, wantSync: true},
		{name: "cancelled_is_never_skipped", status: domain.OrderStatusCancelled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, applied: []domain.AppliedFillRef{ref("t1")}, wantSync: true},
		{name: "partially_filled_is_never_skipped", status: domain.OrderStatusPartiallyFilled, trades: []domain.VenueTradeSnapshot{trade("t1", domain.FillStatusConfirmed)}, applied: []domain.AppliedFillRef{ref("t1")}, wantSync: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			order := testOrder("order-o", "venue-o", tc.status)
			var repository port.ReconciliationOrderRepository
			if tc.noRepo {
				repository = &fakeOrders{orders: []domain.Order{order}}
			} else {
				repository = &fakeOrdersWithAppliedFills{fakeOrders: fakeOrders{orders: []domain.Order{order}}, applied: tc.applied, readErr: tc.readErr}
			}
			fills := &fakeFills{}
			service := newTestService(t, Params{
				Orders: repository, Venue: &fakeVenue{trades: tc.trades, tradesErr: tc.tradeErr},
				Ledger: &fakeLedger{balance: testBalance("100")}, Fills: fills, OrderRefresher: &fakeRefresher{},
				PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) { return nil, nil })},
				BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
					return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
				})},
			})
			params := RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled}
			if tc.focus {
				params.FocusOrderID = order.ID
			}
			if _, err := service.RunAccount(context.Background(), params); err != nil && tc.tradeErr == nil {
				t.Fatalf("RunAccount() error = %v", err)
			}
			if synced := len(fills.calls) > 0; synced != tc.wantSync {
				t.Fatalf("fill sync calls = %#v, want sync=%v", fills.calls, tc.wantSync)
			}
		})
	}
}

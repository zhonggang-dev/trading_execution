package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestOrderRepositoryUsesClientOrderIDAndRevision 验证 Order Repository Uses Client Order ID And Revision 场景下的行为。
func TestOrderRepositoryUsesClientOrderIDAndRevision(t *testing.T) {
	repository := NewOrderRepository()
	order := domain.Order{
		ID:       "ord-1",
		Intent:   domain.OrderIntent{ClientOrderID: "client-1"},
		Status:   domain.OrderStatusAccepted,
		Revision: 1,
	}
	stored, created, err := repository.Create(context.Background(), order)
	if err != nil || !created || stored.ID != order.ID {
		t.Fatalf("Create() = %#v, %v, %v", stored, created, err)
	}
	duplicate := order
	duplicate.ID = "ord-2"
	stored, created, err = repository.Create(context.Background(), duplicate)
	if err != nil || created || stored.ID != order.ID {
		t.Fatalf("duplicate Create() = %#v, %v, %v", stored, created, err)
	}

	stale := order
	if err := repository.Update(context.Background(), stale); !errors.Is(err, port.ErrOrderRevisionConflict) {
		t.Fatalf("stale Update() error = %v", err)
	}
	order.Revision = 2
	order.Status = domain.OrderStatusOpen
	if err := repository.Update(context.Background(), order); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	loaded, err := repository.GetByClientOrderID(context.Background(), "client-1")
	if err != nil || loaded.Status != domain.OrderStatusOpen || loaded.Revision != 2 {
		t.Fatalf("GetByClientOrderID() = %#v, %v", loaded, err)
	}
}

func TestListPendingDefersPolymarketFillEvidenceButKeepsKalshiCoordinatorEligible(t *testing.T) {
	repository := NewOrderRepository()
	now := time.Now().UTC()
	for index, failureCode := range []string{
		"CLOB_FILL_DETAILS_UNAVAILABLE",
		"VENUE_FILL_EVIDENCE_PENDING",
		"CLOB_ORDER_NOT_FOUND",
	} {
		order := domain.Order{
			ID:     "order-" + failureCode,
			Intent: domain.OrderIntent{ClientOrderID: "client-" + failureCode},
			Status: domain.OrderStatusUnknown, FailureCode: failureCode,
			Revision: 1, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(time.Duration(index) * time.Second),
		}
		if _, _, err := repository.Create(context.Background(), order); err != nil {
			t.Fatal(err)
		}
	}
	kalshi := domain.Order{
		ID: "order-kalshi-fill-pending",
		Intent: domain.OrderIntent{
			ClientOrderID: "client-kalshi-fill-pending", Venue: "kalshi",
			MarketSource: domain.MarketSourceKalshi,
		},
		Status: domain.OrderStatusUnknown, FailureCode: "VENUE_FILL_EVIDENCE_PENDING",
		Revision: 1, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Second),
	}
	if _, _, err := repository.Create(context.Background(), kalshi); err != nil {
		t.Fatal(err)
	}

	orders, err := repository.ListPending(context.Background(), now.Add(time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 2 || orders[0].ID != kalshi.ID && orders[1].ID != kalshi.ID {
		t.Fatalf("ListPending() = %#v, want ordinary UNKNOWN plus Kalshi fill-pending", orders)
	}
}

func TestListPendingForAccountsExcludesRetiredOrders(t *testing.T) {
	repository := NewOrderRepository()
	now := time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC)
	for _, accountID := range []string{"account-active", "account-retired"} {
		order := domain.Order{
			ID: "order-" + accountID,
			Intent: domain.OrderIntent{
				ClientOrderID: "client-" + accountID, ExecutionAccountID: accountID,
			},
			Status: domain.OrderStatusUnknown, FilledSize: "0",
			CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), Revision: 1,
		}
		if _, created, err := repository.Create(context.Background(), order); err != nil || !created {
			t.Fatalf("create %s: created=%t err=%v", accountID, created, err)
		}
	}
	orders, err := repository.ListPendingForAccounts(
		context.Background(), []string{"account-active"}, now, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 || orders[0].Intent.ExecutionAccountID != "account-active" {
		t.Fatalf("scoped pending orders = %#v", orders)
	}
}

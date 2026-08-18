package memory

import (
	"context"
	"errors"
	"testing"

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

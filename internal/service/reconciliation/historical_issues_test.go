package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
)

type historicalIssueOrders struct {
	*fakeOrders
	issueOrders []domain.Order
	issueErr    error
}

func (source *historicalIssueOrders) ListWithOpenReconciliationIssues(context.Context, string) ([]domain.Order, error) {
	return source.issueOrders, source.issueErr
}

func TestHistoricalTerminalIssueRequiresFillReadAndIsDeduplicated(t *testing.T) {
	for _, presentInNormalScan := range []bool{false, true} {
		fixture := newRecoveryFixture()
		fixture.external[0].Shares = "10"
		old := testOrder("old-filled", "venue-old-filled", domain.OrderStatusFilled)
		old.CreatedAt = testNow.Add(-7 * 24 * time.Hour)
		old.UpdatedAt = old.CreatedAt
		old.Revision = 1
		healthy := testOrder("healthy", "venue-healthy", domain.OrderStatusFilled)
		fixture.orders.orders = []domain.Order{healthy}
		if presentInNormalScan {
			fixture.orders.orders = append(fixture.orders.orders, old)
		}
		service := fixture.service(t, orderrecovery.Policy{})
		service.orders = &historicalIssueOrders{fakeOrders: fixture.orders, issueOrders: []domain.Order{old}}
		result := runScheduled(t, service)
		if len(fixture.fills.calls) != 1 || fixture.fills.calls[0] != old.ID {
			t.Fatalf("historical fill proof was skipped or duplicated: %v", fixture.fills.calls)
		}
		if result.Run.Summary["local_orders"] != 2 || result.Run.Summary["verified_order:"+old.ID] != 1 {
			t.Fatalf("historical recovery was not recorded: %#v", result.Run.Summary)
		}
	}
}

func TestHistoricalTerminalIssueFailureKeepsScopeAndBackoff(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.external[0].Shares = "10"
	old := testOrder("old-filled", "venue-old-filled", domain.OrderStatusFilled)
	old.Revision = 1
	healthy := testOrder("healthy", "venue-healthy", domain.OrderStatusFilled)
	healthy.Intent.TokenID = "token-2"
	fixture.orders.orders = []domain.Order{healthy}
	fixture.fills.errors = map[string]error{old.ID: errVenueDown}
	service := fixture.service(t, orderrecovery.Policy{BaseBackoff: time.Minute})
	service.orders = &historicalIssueOrders{fakeOrders: fixture.orders, issueOrders: []domain.Order{old}}
	for attempt := 0; attempt < 3; attempt++ {
		result := runScheduled(t, service)
		if result.Run.Summary["verified_order:"+old.ID] != 0 || result.Run.Summary["verified_order:"+healthy.ID] != 1 || result.Impact.AccountWide {
			t.Fatalf("failed proof cleared its gate or blocked healthy work: %#v", result)
		}
	}
	if len(fixture.fills.calls) != 1 {
		t.Fatalf("historical issue bypassed backoff: %v", fixture.fills.calls)
	}
	lease, exists := fixture.leases.Lease(old.ID)
	if !exists || lease.NextRetryAt == nil || lease.Attempts != 1 {
		t.Fatalf("historical issue has no durable retry: %#v", lease)
	}
}

func TestHistoricalIssueReadFailureCannotSilentlySkipGates(t *testing.T) {
	fixture := newRecoveryFixture()
	service := fixture.service(t, orderrecovery.Policy{})
	failure := errors.New("historical issue query failed")
	service.orders = &historicalIssueOrders{fakeOrders: fixture.orders, issueErr: failure}
	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if !errors.Is(err, failure) || result.Run.Status != domain.ReconciliationRunFailed || len(fixture.fills.calls) != 0 {
		t.Fatalf("failed recovery-set read was accepted: %#v %v", result, err)
	}
}

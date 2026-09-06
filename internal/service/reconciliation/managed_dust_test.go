package reconciliation

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type knownBalanceFunc func(context.Context, string, string) (domain.Decimal, error)

func (f knownBalanceFunc) GetKnownPositionBalance(ctx context.Context, wallet, token string) (domain.Decimal, error) {
	return f(ctx, wallet, token)
}

func TestMissingDustRequiresExactChainEvidence(t *testing.T) {
	for _, mode := range []string{"exact", "mismatch", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			positions := []domain.Position{}
			for _, token := range []string{"a", "b", "c", "d", "e"} {
				positions = append(positions, domain.Position{ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1", TokenID: token, TotalShares: "0.006664", AvailableShares: "0.006664", ReservedShares: "0", LifecycleStatus: domain.PositionLifecycleOpen})
			}
			calls := 0
			service := newTestService(t, Params{Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: &fakeLedger{balance: testBalance("0"), positions: positions}, Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
				PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) { return nil, nil })},
				BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
					return domain.ExternalBalance{Asset: "USDC", Amount: "0", Source: "CHAIN", ObservedAt: testNow}, nil
				})},
				KnownPositionBalances: knownBalanceFunc(func(context.Context, string, string) (domain.Decimal, error) {
					calls++
					if mode == "unavailable" {
						return "", errors.New("RPC offline")
					}
					if mode == "mismatch" {
						return "0.006665", nil
					}
					return "0.006664", nil
				}),
			})
			result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
			if err != nil && mode != "unavailable" {
				t.Fatal(err)
			}
			if mode == "exact" {
				if result.Run.Status != domain.ReconciliationRunCompleted || calls != 5 || result.Run.Summary["missing_dust_verified_onchain"] != 5 {
					t.Fatalf("result %#v calls %d", result, calls)
				}
			} else if result.Run.Status == domain.ReconciliationRunCompleted {
				t.Fatalf("unsafe completed: %#v", result)
			}
		})
	}
}

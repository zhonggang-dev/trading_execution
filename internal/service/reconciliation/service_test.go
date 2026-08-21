package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
)

var testNow = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

// TestRunAccountRepairsOnlyProvableFacts 验证 Run Account Repairs Only Provable Facts 场景下的行为。
func TestRunAccountRepairsOnlyProvableFacts(t *testing.T) {
	order := testOrder("order-1", "venue-1", domain.OrderStatusLive)
	ledger := &fakeLedger{
		balance: testBalance("100"),
		positions: []domain.Position{{
			ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
			TokenID: "token-1", OutcomeName: "YES", TotalShares: "5", AvailableShares: "5", ReservedShares: "0",
			CostBasis: "2.5", AverageCostPrice: "0.5", LifecycleStatus: domain.PositionLifecycleOpen,
		}},
	}
	service := newTestService(t, Params{
		Orders: &fakeOrders{orders: []domain.Order{order}}, Venue: &fakeVenue{}, Ledger: ledger,
		Fills: &fakeFills{results: map[string]fillprocessor.SyncResult{
			order.ID: {OrderID: order.ID, Observed: 1, Applied: 1, Applications: []domain.FillApplication{{
				Applied: true, Fill: domain.Fill{VenueFillID: "trade-1", Side: domain.SideBuy, Shares: "5"},
			}}},
		}},
		OrderRefresher: &fakeRefresher{orders: map[string]domain.Order{
			order.ID: withStatus(order, domain.OrderStatusCancelled),
		}},
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return []domain.ExternalPosition{{
				ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "5",
				Redeemable: true, Source: "DATA_API", ObservedAt: testNow,
			}}, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerStartup})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("run status = %s, want COMPLETED", result.Run.Status)
	}
	for _, issueType := range []domain.ReconciliationIssueType{
		domain.ReconciliationIssueMissedBuyFill,
		domain.ReconciliationIssueLocalOrderCancelled,
		domain.ReconciliationIssuePositionSettled,
	} {
		assertIssue(t, result.Issues, issueType, domain.ReconciliationIssueResolved)
	}
	if len(ledger.settled) != 1 || ledger.settled[0] != "token-1" {
		t.Fatalf("settled tokens = %#v, want token-1", ledger.settled)
	}
	if ledger.positions[0].TotalShares != "5" || ledger.positions[0].CostBasis != "2.5" {
		t.Fatalf("settlement erased economic position: %#v", ledger.positions[0])
	}
}

func TestStartupUsesExplicitAccountReconciliationBaselineForVenueTrades(t *testing.T) {
	baseline := testNow.Add(-2 * time.Minute)
	balance := testBalance("100")
	balance.ReconciledAt = &baseline
	venue := &fakeVenue{}
	service := newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: venue, Ledger: &fakeLedger{balance: balance},
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerStartup,
	})
	if err != nil || result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("RunAccount() result/error = %#v/%v", result, err)
	}
	if !venue.tradesAfter.Equal(baseline) {
		t.Fatalf("venue trades after = %s, want %s", venue.tradesAfter, baseline)
	}
}

func TestScheduledReconciliationNeverScansBeforeAccountOwnershipBaseline(t *testing.T) {
	baseline := testNow.Add(-2 * time.Minute)
	balance := testBalance("100")
	balance.ReconciledAt = &baseline
	venue := &fakeVenue{}
	service := newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: venue, Ledger: &fakeLedger{balance: balance},
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		TradeLookback: 30 * time.Minute,
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil || result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("RunAccount() result/error = %#v/%v", result, err)
	}
	if !venue.tradesAfter.Equal(baseline) {
		t.Fatalf("venue trades after = %s, want ownership baseline %s", venue.tradesAfter, baseline)
	}
}

func TestScheduledReconciliationKeepsNewerLookbackThanAccountBaseline(t *testing.T) {
	baseline := testNow.Add(-30 * time.Minute)
	balance := testBalance("100")
	balance.ReconciledAt = &baseline
	venue := &fakeVenue{}
	service := newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: venue, Ledger: &fakeLedger{balance: balance},
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		TradeLookback: 5 * time.Minute,
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	_, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := testNow.Add(-5 * time.Minute)
	if !venue.tradesAfter.Equal(want) {
		t.Fatalf("venue trades after = %s, want lookback %s", venue.tradesAfter, want)
	}
}

func TestStartupWithoutAccountBaselineStillScansFullVenueHistory(t *testing.T) {
	venue := &fakeVenue{}
	service := newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: venue, Ledger: &fakeLedger{balance: testBalance("100")},
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	_, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerStartup,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !venue.tradesAfter.IsZero() {
		t.Fatalf("venue trades after = %s, want full-history zero time", venue.tradesAfter)
	}
}

func TestExternalTradeDispositionRequiresExactTradeAndPositionIdentity(t *testing.T) {
	baseDisposition := domain.ExternalPositionDispositionTrade{
		ExecutionAccountID: "account-1",
		VenueTradeID:       "trade-1",
		VenueOrderID:       "external-order-1",
		ConditionID:        "condition-1",
		TokenID:            "token-1",
	}
	tests := []struct {
		name        string
		disposition domain.ExternalPositionDispositionTrade
		wantOpen    bool
	}{
		{name: "exact", disposition: baseDisposition},
		{name: "different trade", disposition: withDispositionTradeID(baseDisposition, "trade-other"), wantOpen: true},
		{name: "different order", disposition: withDispositionOrderID(baseDisposition, "external-order-other"), wantOpen: true},
		{name: "different condition", disposition: withDispositionConditionID(baseDisposition, "condition-other"), wantOpen: true},
		{name: "different token", disposition: withDispositionTokenID(baseDisposition, "token-other"), wantOpen: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newTestService(t, Params{
				Orders: &fakeOrders{},
				Venue: &fakeVenue{trades: []domain.VenueTradeSnapshot{{
					VenueTradeID: "trade-1", OrderIDs: []string{"external-order-1"},
					ConditionID: "condition-1", TokenID: "token-1",
				}}},
				Ledger:         &fakeLedger{balance: testBalance("100")},
				Fills:          &fakeFills{},
				OrderRefresher: &fakeRefresher{},
				PositionDispositionTrades: positionDispositionTradeSourceFunc(func(context.Context, string) ([]domain.ExternalPositionDispositionTrade, error) {
					return []domain.ExternalPositionDispositionTrade{test.disposition}, nil
				}),
				PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
					return nil, nil
				})},
				BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
					return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
				})},
			})

			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerStartup,
			})
			if err != nil {
				t.Fatalf("RunAccount() error = %v", err)
			}
			if test.wantOpen {
				assertIssue(t, result.Issues, domain.ReconciliationIssueExternalTrade, domain.ReconciliationIssueOpen)
				if result.Run.Summary["external_trades_accounted"] != 0 {
					t.Fatalf("accounted external trades = %d, want 0", result.Run.Summary["external_trades_accounted"])
				}
				return
			}
			for _, issue := range result.Issues {
				if issue.Type == domain.ReconciliationIssueExternalTrade {
					t.Fatalf("exact disposition manufactured reconciliation issue %#v", issue)
				}
			}
			if result.Run.Summary["external_trades_accounted"] != 1 {
				t.Fatalf("accounted external trades = %d, want 1", result.Run.Summary["external_trades_accounted"])
			}
		})
	}
}

func TestExternalTradeDispositionSuppressesOnlyOneOwnedOrderComponent(t *testing.T) {
	disposition := domain.ExternalPositionDispositionTrade{
		ExecutionAccountID: "account-1", VenueTradeID: "trade-1",
		VenueOrderID: "external-order-1", ConditionID: "condition-1", TokenID: "token-1",
	}
	service := newTestService(t, Params{
		Orders: &fakeOrders{},
		Venue: &fakeVenue{trades: []domain.VenueTradeSnapshot{{
			VenueTradeID: "trade-1", OrderIDs: []string{"external-order-1", "external-order-2"},
			ConditionID: "condition-1", TokenID: "token-1",
		}}},
		Ledger:         &fakeLedger{balance: testBalance("100")},
		Fills:          &fakeFills{},
		OrderRefresher: &fakeRefresher{},
		PositionDispositionTrades: positionDispositionTradeSourceFunc(func(context.Context, string) ([]domain.ExternalPositionDispositionTrade, error) {
			return []domain.ExternalPositionDispositionTrade{disposition}, nil
		}),
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})
	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerStartup,
	})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Summary["external_trades_accounted"] != 1 {
		t.Fatalf("accounted external trades = %d, want 1", result.Run.Summary["external_trades_accounted"])
	}
	openOrders := make([]string, 0)
	for _, issue := range result.Issues {
		if issue.Type == domain.ReconciliationIssueExternalTrade && issue.Status == domain.ReconciliationIssueOpen {
			openOrders = append(openOrders, issue.VenueOrderID)
		}
	}
	if len(openOrders) != 1 || openOrders[0] != "external-order-2" {
		t.Fatalf("open external trade orders = %#v, want only external-order-2", openOrders)
	}
}

func TestExternalTradeDispositionReadFailureFailsClosed(t *testing.T) {
	unavailable := errors.New("disposition ledger unavailable")
	service := newTestService(t, Params{
		Orders: &fakeOrders{},
		Venue: &fakeVenue{trades: []domain.VenueTradeSnapshot{{
			VenueTradeID: "trade-1", OrderIDs: []string{"external-order-1"},
			ConditionID: "condition-1", TokenID: "token-1",
		}}},
		Ledger:         &fakeLedger{balance: testBalance("100")},
		Fills:          &fakeFills{},
		OrderRefresher: &fakeRefresher{},
		PositionDispositionTrades: positionDispositionTradeSourceFunc(func(context.Context, string) ([]domain.ExternalPositionDispositionTrade, error) {
			return nil, unavailable
		}),
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerStartup,
	})
	if !errors.Is(err, unavailable) {
		t.Fatalf("RunAccount() error = %v, want disposition source failure", err)
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssueExternalTrade, domain.ReconciliationIssueOpen)
	assertIssue(t, result.Issues, domain.ReconciliationIssueSourceUnavailable, domain.ReconciliationIssueOpen)
}

func TestManagedPositionIdentityMismatchFailsClosed(t *testing.T) {
	basePosition := domain.Position{
		ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
		TokenID: "token-1", OutcomeIndex: intPointer(0), OutcomeName: "YES",
		TotalShares: "2", AvailableShares: "2", ReservedShares: "0",
		LifecycleStatus: domain.PositionLifecycleOpen,
	}
	baseExternal := domain.ExternalPosition{
		ConditionID: "condition-1", TokenID: "token-1", OutcomeIndex: intPointer(0), OutcomeName: "YES",
		Shares: "2", Redeemable: true, Source: "DATA_API", ObservedAt: testNow,
	}
	tests := []struct {
		name     string
		position domain.Position
		external domain.ExternalPosition
	}{
		{name: "account", position: withPositionAccountID(basePosition, "account-other"), external: baseExternal},
		{name: "condition", position: basePosition, external: withExternalConditionID(baseExternal, "condition-other")},
		{name: "outcome index", position: basePosition, external: withExternalOutcomeIndex(baseExternal, intPointer(1))},
		{name: "outcome name", position: basePosition, external: withExternalOutcomeName(baseExternal, "NO")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{test.position}}
			service := newPositionBaselineTestService(t, ledger, nil, []domain.ExternalPosition{test.external})

			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			if err != nil {
				t.Fatalf("RunAccount() error = %v", err)
			}
			assertIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift, domain.ReconciliationIssueOpen)
			if len(ledger.settled) != 0 {
				t.Fatalf("identity mismatch settled a managed position: %#v", ledger.settled)
			}
		})
	}
}

func TestExternalPositionBaselineOnlyIsHealthyAndNeverBecomesManaged(t *testing.T) {
	ledger := &fakeLedger{balance: testBalance("100")}
	service := newPositionBaselineTestService(t, ledger, []domain.ExternalPositionBaseline{
		testExternalPositionBaseline("5"),
	}, []domain.ExternalPosition{
		testExternalPosition("5"),
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerStartup,
	})
	if err != nil || result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("RunAccount() result/error = %#v/%v", result, err)
	}
	if len(result.Issues) != 0 || len(ledger.positions) != 0 || len(ledger.settled) != 0 {
		t.Fatalf("unmanaged baseline leaked into managed state: issues=%#v positions=%#v settled=%#v", result.Issues, ledger.positions, ledger.settled)
	}
}

func TestExternalPositionBaselineAddsToManagedShares(t *testing.T) {
	ledger := &fakeLedger{
		balance: testBalance("100"),
		positions: []domain.Position{{
			ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
			TokenID: "token-1", OutcomeIndex: intPointer(0), OutcomeName: "YES",
			TotalShares: "2", AvailableShares: "2", ReservedShares: "0",
			LifecycleStatus: domain.PositionLifecycleOpen,
		}},
	}
	service := newPositionBaselineTestService(t, ledger, []domain.ExternalPositionBaseline{
		testExternalPositionBaseline("5"),
	}, []domain.ExternalPosition{
		testExternalPosition("7"),
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil || result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("RunAccount() result/error = %#v/%v", result, err)
	}
	if len(result.Issues) != 0 || ledger.positions[0].TotalShares != "2" || len(ledger.settled) != 0 {
		t.Fatalf("baseline changed managed state: issues=%#v positions=%#v settled=%#v", result.Issues, ledger.positions, ledger.settled)
	}
}

func TestExternalPositionBaselineQuantityDriftFailsClosed(t *testing.T) {
	for _, remoteShares := range []domain.Decimal{"6.9", "7.1", "7.0000001"} {
		t.Run(remoteShares.String(), func(t *testing.T) {
			ledger := &fakeLedger{
				balance: testBalance("100"),
				positions: []domain.Position{{
					ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
					TokenID: "token-1", OutcomeIndex: intPointer(0), OutcomeName: "YES",
					TotalShares: "2", AvailableShares: "2", ReservedShares: "0",
					LifecycleStatus: domain.PositionLifecycleOpen,
				}},
			}
			service := newPositionBaselineTestService(t, ledger, []domain.ExternalPositionBaseline{
				testExternalPositionBaseline("5"),
			}, []domain.ExternalPosition{
				testExternalPosition(remoteShares),
			})

			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			if err != nil || result.Run.Status != domain.ReconciliationRunAttentionRequired {
				t.Fatalf("RunAccount() result/error = %#v/%v", result, err)
			}
			assertIssue(t, result.Issues, domain.ReconciliationIssueExternalPositionBaselineDrift, domain.ReconciliationIssueOpen)
			if ledger.positions[0].TotalShares != "2" || len(ledger.settled) != 0 {
				t.Fatalf("drift caused a managed mutation: positions=%#v settled=%#v", ledger.positions, ledger.settled)
			}
		})
	}
}

func TestExternalPositionBaselineIdentityMismatchFailsClosed(t *testing.T) {
	remote := testExternalPosition("5")
	remote.ConditionID = "condition-other"
	ledger := &fakeLedger{balance: testBalance("100")}
	service := newPositionBaselineTestService(t, ledger, []domain.ExternalPositionBaseline{
		testExternalPositionBaseline("5"),
	}, []domain.ExternalPosition{remote})

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil || result.Run.Status != domain.ReconciliationRunAttentionRequired {
		t.Fatalf("RunAccount() result/error = %#v/%v", result, err)
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssueExternalPositionBaselineDrift, domain.ReconciliationIssueOpen)
	if len(ledger.positions) != 0 {
		t.Fatalf("identity mismatch created a managed position: %#v", ledger.positions)
	}
}

func TestUnbaselinedExternalPositionRemainsPhantom(t *testing.T) {
	ledger := &fakeLedger{balance: testBalance("100")}
	service := newPositionBaselineTestService(t, ledger, nil, []domain.ExternalPosition{
		testExternalPosition("1"),
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil || result.Run.Status != domain.ReconciliationRunAttentionRequired {
		t.Fatalf("RunAccount() result/error = %#v/%v", result, err)
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssuePhantomPosition, domain.ReconciliationIssueOpen)
	if len(ledger.positions) != 0 {
		t.Fatalf("phantom position was imported into managed state: %#v", ledger.positions)
	}
}

// TestRunAccountLeavesUnattributableStateForManualReview 验证 Run Account Leaves Unattributable State For Manual Review 场景下的行为。
func TestRunAccountLeavesUnattributableStateForManualReview(t *testing.T) {
	unknown := testOrder("order-unknown", "", domain.OrderStatusUnknown)
	ledger := &fakeLedger{
		balance: testBalance("100"),
		positions: []domain.Position{{
			ExecutionAccountID: "account-1", MarketID: "market-1", TokenID: "token-1",
			TotalShares: "2", AvailableShares: "2", ReservedShares: "0", LifecycleStatus: domain.PositionLifecycleOpen,
		}},
	}
	venue := &fakeVenue{
		openOrders: []domain.VenueOrderSnapshot{{VenueOrderID: "manual-order", ConditionID: "condition-2", TokenID: "token-2"}},
		trades:     []domain.VenueTradeSnapshot{{VenueTradeID: "manual-trade", OrderIDs: []string{"manual-order"}, TokenID: "token-2"}},
	}
	refresher := &fakeRefresher{}
	service := newTestService(t, Params{
		Orders: &fakeOrders{orders: []domain.Order{unknown}}, Venue: venue, Ledger: ledger,
		Fills: &fakeFills{}, OrderRefresher: refresher,
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return []domain.ExternalPosition{
				{ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "3", Redeemable: true, Source: "DATA_API", ObservedAt: testNow},
				{ConditionID: "condition-2", TokenID: "token-2", OutcomeName: "NO", Shares: "4", Source: "DATA_API", ObservedAt: testNow},
			}, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "90", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerOrderUnknown, FocusOrderID: unknown.ID})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if result.Run.Status != domain.ReconciliationRunAttentionRequired {
		t.Fatalf("run status = %s, want ATTENTION_REQUIRED", result.Run.Status)
	}
	for _, issueType := range []domain.ReconciliationIssueType{
		domain.ReconciliationIssueSubmitUnconfirmed,
		domain.ReconciliationIssueExternalOrder,
		domain.ReconciliationIssueExternalTrade,
		domain.ReconciliationIssuePositionDrift,
		domain.ReconciliationIssuePhantomPosition,
		domain.ReconciliationIssueBalanceDrift,
	} {
		assertIssue(t, result.Issues, issueType, domain.ReconciliationIssueOpen)
	}
	if refresher.calls != 0 || len(ledger.settled) != 0 {
		t.Fatalf("unsafe repair attempted: refreshes=%d settled=%#v", refresher.calls, ledger.settled)
	}
}

// TestRunAccountDisablesRepairWhenSourcesConflict 验证 Run Account Disables Repair When Sources Conflict 场景下的行为。
func TestRunAccountDisablesRepairWhenSourcesConflict(t *testing.T) {
	ledger := &fakeLedger{
		balance: testBalance("100"),
		positions: []domain.Position{{
			ExecutionAccountID: "account-1", MarketID: "market-1", TokenID: "token-1",
			TotalShares: "5", AvailableShares: "5", ReservedShares: "0", LifecycleStatus: domain.PositionLifecycleOpen,
		}},
	}
	position := func(shares domain.Decimal) port.ExternalPositionSource {
		return positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return []domain.ExternalPosition{{
				ConditionID: "condition-1", TokenID: "token-1", Shares: shares,
				Redeemable: true, Source: "SOURCE_" + shares.String(), ObservedAt: testNow,
			}}, nil
		})
	}
	balance := func(amount domain.Decimal) port.ExternalBalanceSource {
		return balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: amount, Source: "SOURCE_" + amount.String(), ObservedAt: testNow}, nil
		})
	}
	service := newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger, Fills: &fakeFills{},
		OrderRefresher:  &fakeRefresher{},
		PositionSources: []port.ExternalPositionSource{position("5"), position("6")},
		BalanceSources:  []port.ExternalBalanceSource{balance("100"), balance("99")},
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssueSourceConflict, domain.ReconciliationIssueOpen)
	if len(ledger.settled) != 0 {
		t.Fatalf("conflicting sources caused repair: %#v", ledger.settled)
	}
}

// TestRunAccountNeverTreatsUnavailableSourceAsEmpty 验证 Run Account Never Treats Unavailable Source As Empty 场景下的行为。
func TestRunAccountNeverTreatsUnavailableSourceAsEmpty(t *testing.T) {
	unavailable := errors.New("upstream unavailable")
	service := newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{openErr: unavailable, tradesErr: unavailable},
		Ledger: &fakeLedger{balance: testBalance("0")}, Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, unavailable
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{}, unavailable
		})},
	})

	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err == nil {
		t.Fatal("RunAccount() error = nil, want upstream uncertainty")
	}
	assertIssue(t, result.Issues, domain.ReconciliationIssueSourceUnavailable, domain.ReconciliationIssueOpen)
	for _, issue := range result.Issues {
		if issue.Type == domain.ReconciliationIssuePhantomPosition || issue.Type == domain.ReconciliationIssuePositionDrift {
			t.Fatalf("unavailable source was treated as empty: %#v", issue)
		}
	}
}

// TestRunAccountDoesNotFinalizeCancelledReservationWithoutCompleteFillEvidence 验证 Run Account Does Not Finalize Cancelled Reservation Without Complete Fill Evidence 场景下的行为。
func TestRunAccountDoesNotFinalizeCancelledReservationWithoutCompleteFillEvidence(t *testing.T) {
	cancelled := testOrder("order-cancelled", "venue-cancelled", domain.OrderStatusCancelled)
	refresher := &fakeRefresher{orders: map[string]domain.Order{cancelled.ID: cancelled}}
	service := newTestService(t, Params{
		Orders: &fakeOrders{orders: []domain.Order{cancelled}},
		Venue:  &fakeVenue{tradesErr: errors.New("trades delayed")},
		Ledger: &fakeLedger{balance: testBalance("100")}, Fills: &fakeFills{}, OrderRefresher: refresher,
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	if _, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled}); err == nil {
		t.Fatal("RunAccount() error = nil, want incomplete trade evidence")
	}
	if refresher.finalizations != 0 {
		t.Fatalf("cancel finalizations = %d, want 0 while trades are unavailable", refresher.finalizations)
	}
}

// TestRunAccountFinalizesCancelledReservationAfterSuccessfulFillScan 验证 Run Account Finalizes Cancelled Reservation After Successful Fill Scan 场景下的行为。
func TestRunAccountFinalizesCancelledReservationAfterSuccessfulFillScan(t *testing.T) {
	cancelled := testOrder("order-cancelled", "venue-cancelled", domain.OrderStatusCancelled)
	refresher := &fakeRefresher{orders: map[string]domain.Order{cancelled.ID: cancelled}}
	service := newTestService(t, Params{
		Orders: &fakeOrders{orders: []domain.Order{cancelled}}, Venue: &fakeVenue{},
		Ledger: &fakeLedger{balance: testBalance("100")}, Fills: &fakeFills{}, OrderRefresher: refresher,
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return nil, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})

	if _, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled}); err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if refresher.finalizations != 1 {
		t.Fatalf("cancel finalizations = %d, want 1 after successful fill scan", refresher.finalizations)
	}
}

// positionSourceFunc 表示后端使用的 positionSourceFunc 类型。
type positionSourceFunc func(context.Context, string) ([]domain.ExternalPosition, error)

// ListExternalPositions 返回模拟数据源中的测试列表。
func (function positionSourceFunc) ListExternalPositions(ctx context.Context, wallet string) ([]domain.ExternalPosition, error) {
	return function(ctx, wallet)
}

type positionBaselineSourceFunc func(context.Context, string) ([]domain.ExternalPositionBaseline, error)

func (function positionBaselineSourceFunc) ListExternalPositionBaselines(
	ctx context.Context,
	executionAccountID string,
) ([]domain.ExternalPositionBaseline, error) {
	return function(ctx, executionAccountID)
}

type positionDispositionTradeSourceFunc func(context.Context, string) ([]domain.ExternalPositionDispositionTrade, error)

func (function positionDispositionTradeSourceFunc) ListExternalPositionDispositionTrades(
	ctx context.Context,
	executionAccountID string,
) ([]domain.ExternalPositionDispositionTrade, error) {
	return function(ctx, executionAccountID)
}

// balanceSourceFunc 表示后端使用的 balanceSourceFunc 类型。
type balanceSourceFunc func(context.Context, string, string) (domain.ExternalBalance, error)

// GetExternalBalance 返回模拟仓储中的测试记录。
func (function balanceSourceFunc) GetExternalBalance(ctx context.Context, wallet, asset string) (domain.ExternalBalance, error) {
	return function(ctx, wallet, asset)
}

// fakeOrders 表示后端使用的 fakeOrders 类型。
type fakeOrders struct {
	orders []domain.Order
	err    error
}

// ListForReconciliation 返回模拟数据源中的测试列表。
func (repository *fakeOrders) ListForReconciliation(context.Context, string, time.Time) ([]domain.Order, error) {
	return append([]domain.Order(nil), repository.orders...), repository.err
}

// fakeVenue 表示后端使用的 fakeVenue 类型。
type fakeVenue struct {
	openOrders  []domain.VenueOrderSnapshot
	trades      []domain.VenueTradeSnapshot
	openErr     error
	tradesErr   error
	tradesAfter time.Time
}

// ListReconciliationOpenOrders 返回模拟数据源中的测试列表。
func (venue *fakeVenue) ListReconciliationOpenOrders(context.Context, string) ([]domain.VenueOrderSnapshot, error) {
	return append([]domain.VenueOrderSnapshot(nil), venue.openOrders...), venue.openErr
}

// ListReconciliationTrades 返回模拟数据源中的测试列表。
func (venue *fakeVenue) ListReconciliationTrades(_ context.Context, _ string, after time.Time) ([]domain.VenueTradeSnapshot, error) {
	venue.tradesAfter = after
	return append([]domain.VenueTradeSnapshot(nil), venue.trades...), venue.tradesErr
}

// fakeLedger 表示后端使用的 fakeLedger 类型。
type fakeLedger struct {
	balance   domain.AccountBalance
	positions []domain.Position
	settled   []string
}

// GetBalance 返回模拟仓储中的测试记录。
func (ledger *fakeLedger) GetBalance(context.Context, string) (domain.AccountBalance, error) {
	return ledger.balance, nil
}

// ListPositions 返回模拟数据源中的测试列表。
func (ledger *fakeLedger) ListPositions(context.Context, string) ([]domain.Position, error) {
	return append([]domain.Position(nil), ledger.positions...), nil
}

// MarkPositionSettled 记录模拟状态变更。
func (ledger *fakeLedger) MarkPositionSettled(_ context.Context, _ string, tokenID, _ string, _ time.Time) (domain.Position, error) {
	ledger.settled = append(ledger.settled, tokenID)
	for index := range ledger.positions {
		if ledger.positions[index].TokenID == tokenID {
			ledger.positions[index].LifecycleStatus = domain.PositionLifecycleSettledPendingRedeem
			return ledger.positions[index], nil
		}
	}
	return domain.Position{}, errors.New("position missing")
}

// fakeFills 表示后端使用的 fakeFills 类型。
type fakeFills struct {
	results map[string]fillprocessor.SyncResult
	errors  map[string]error
}

// SyncOrder 模拟真实成交同步。
func (fills *fakeFills) SyncOrder(_ context.Context, orderID string) (fillprocessor.SyncResult, error) {
	return fills.results[orderID], fills.errors[orderID]
}

// fakeRefresher 表示后端使用的 fakeRefresher 类型。
type fakeRefresher struct {
	orders        map[string]domain.Order
	errors        map[string]error
	finalizeErr   error
	calls         int
	finalizations int
}

// FinalizeCancellation 实现测试替身所需的接口行为。
func (refresher *fakeRefresher) FinalizeCancellation(_ context.Context, orderID string) (domain.Order, error) {
	refresher.finalizations++
	return refresher.orders[orderID], refresher.finalizeErr
}

// Refresh 记录模拟订单刷新。
func (refresher *fakeRefresher) Refresh(_ context.Context, orderID string) (domain.Order, error) {
	refresher.calls++
	return refresher.orders[orderID], refresher.errors[orderID]
}

// fakeRecorder 表示后端使用的 fakeRecorder 类型。
type fakeRecorder struct {
	runs   []domain.ReconciliationRun
	issues []domain.ReconciliationIssue
}

// Start 模拟外部尝试开始。
func (recorder *fakeRecorder) Start(_ context.Context, run domain.ReconciliationRun) error {
	recorder.runs = append(recorder.runs, run)
	return nil
}

// RecordIssue 记录模拟调用并返回配置结果。
func (recorder *fakeRecorder) RecordIssue(_ context.Context, issue domain.ReconciliationIssue) error {
	recorder.issues = append(recorder.issues, issue)
	return nil
}

// Complete 实现测试替身所需的接口行为。
func (recorder *fakeRecorder) Complete(_ context.Context, run domain.ReconciliationRun) error {
	recorder.runs = append(recorder.runs, run)
	return nil
}

// newTestService 创建测试所需的模拟对象。
func newTestService(t *testing.T, params Params) *Service {
	t.Helper()
	params.Recorder = &fakeRecorder{}
	if params.PositionBaselines == nil {
		params.PositionBaselines = positionBaselineSourceFunc(func(context.Context, string) ([]domain.ExternalPositionBaseline, error) {
			return nil, nil
		})
	}
	if params.PositionDispositionTrades == nil {
		params.PositionDispositionTrades = positionDispositionTradeSourceFunc(func(context.Context, string) ([]domain.ExternalPositionDispositionTrade, error) {
			return nil, nil
		})
	}
	params.Now = func() time.Time { return testNow }
	sequence := 0
	params.NewID = func() string {
		sequence++
		return fmt.Sprintf("id-%d", sequence)
	}
	service, err := New(params)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

// testOrder 实现当前测试场景所需的辅助行为。
func testOrder(id, venueID string, status domain.OrderStatus) domain.Order {
	return domain.Order{
		ID: id, VenueOrderID: venueID, Status: status, FilledSize: "0", CreatedAt: testNow.Add(-time.Hour), UpdatedAt: testNow.Add(-time.Minute),
		Intent: domain.OrderIntent{
			ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "condition-1",
			TokenID: "token-1", Size: "5", Side: domain.SideBuy,
		},
	}
}

// withStatus 实现当前测试场景所需的辅助行为。
func withStatus(order domain.Order, status domain.OrderStatus) domain.Order {
	order.Status = status
	return order
}

func withDispositionTradeID(value domain.ExternalPositionDispositionTrade, venueTradeID string) domain.ExternalPositionDispositionTrade {
	value.VenueTradeID = venueTradeID
	return value
}

func withDispositionOrderID(value domain.ExternalPositionDispositionTrade, venueOrderID string) domain.ExternalPositionDispositionTrade {
	value.VenueOrderID = venueOrderID
	return value
}

func withDispositionConditionID(value domain.ExternalPositionDispositionTrade, conditionID string) domain.ExternalPositionDispositionTrade {
	value.ConditionID = conditionID
	return value
}

func withDispositionTokenID(value domain.ExternalPositionDispositionTrade, tokenID string) domain.ExternalPositionDispositionTrade {
	value.TokenID = tokenID
	return value
}

func withExternalConditionID(value domain.ExternalPosition, conditionID string) domain.ExternalPosition {
	value.ConditionID = conditionID
	return value
}

func withExternalOutcomeIndex(value domain.ExternalPosition, outcomeIndex *int) domain.ExternalPosition {
	value.OutcomeIndex = outcomeIndex
	return value
}

func withExternalOutcomeName(value domain.ExternalPosition, outcomeName string) domain.ExternalPosition {
	value.OutcomeName = outcomeName
	return value
}

func withPositionAccountID(value domain.Position, executionAccountID string) domain.Position {
	value.ExecutionAccountID = executionAccountID
	return value
}

func hasIssue(issues []domain.ReconciliationIssue, issueType domain.ReconciliationIssueType) bool {
	for _, issue := range issues {
		if issue.Type == issueType {
			return true
		}
	}
	return false
}

// testBalance 实现当前测试场景所需的辅助行为。
func testBalance(total domain.Decimal) domain.AccountBalance {
	return domain.AccountBalance{
		ExecutionAccountID: "account-1", WalletAddress: "0x1111111111111111111111111111111111111111",
		CollateralAsset: "USDC", TotalBalance: total, AvailableBalance: total, ReservedBalance: "0",
	}
}

func newPositionBaselineTestService(
	t *testing.T,
	ledger *fakeLedger,
	baselines []domain.ExternalPositionBaseline,
	remote []domain.ExternalPosition,
) *Service {
	t.Helper()
	return newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger,
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{},
		PositionBaselines: positionBaselineSourceFunc(func(context.Context, string) ([]domain.ExternalPositionBaseline, error) {
			return append([]domain.ExternalPositionBaseline(nil), baselines...), nil
		}),
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return append([]domain.ExternalPosition(nil), remote...), nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: "100", Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})
}

func testExternalPositionBaseline(shares domain.Decimal) domain.ExternalPositionBaseline {
	return domain.ExternalPositionBaseline{
		BaselineID: "baseline-1", ExecutionAccountID: "account-1",
		ConditionID: "condition-1", TokenID: "token-1", OutcomeIndex: intPointer(0),
		OutcomeName: "YES", Shares: shares, Source: "DATA_API", ObservedAt: testNow.Add(-time.Hour),
		Evidence: json.RawMessage(`{"snapshot_sha256":"abc123"}`), Actor: "operator-1", Reason: "initial live account cutover",
	}
}

func testExternalPosition(shares domain.Decimal) domain.ExternalPosition {
	return domain.ExternalPosition{
		ConditionID: "condition-1", TokenID: "token-1", OutcomeIndex: intPointer(0),
		OutcomeName: "YES", Shares: shares, Source: "DATA_API", ObservedAt: testNow,
	}
}

func intPointer(value int) *int {
	return &value
}

// assertIssue 执行对应的测试断言。
func assertIssue(t *testing.T, issues []domain.ReconciliationIssue, issueType domain.ReconciliationIssueType, status domain.ReconciliationIssueStatus) {
	t.Helper()
	for _, issue := range issues {
		if issue.Type == issueType && issue.Status == status {
			return
		}
	}
	t.Fatalf("issue %s/%s missing from %#v", issueType, status, issues)
}

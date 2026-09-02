package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type redemptionProgressFunc func(context.Context, string) ([]domain.InFlightRedemption, error)

func (function redemptionProgressFunc) ListInFlightRedemptions(ctx context.Context, executionAccountID string) ([]domain.InFlightRedemption, error) {
	return function(ctx, executionAccountID)
}

func (redemptionProgressFunc) ListAppliedRedemptions(context.Context, string, time.Time) ([]domain.AppliedRedemption, error) {
	return nil, nil
}

// fakeRedemptionProgress serves both views and records the grace boundary the
// service asked for.
type fakeRedemptionProgress struct {
	inFlight   []domain.InFlightRedemption
	applied    []domain.AppliedRedemption
	appliedErr error
	since      time.Time
}

func (fake *fakeRedemptionProgress) ListInFlightRedemptions(context.Context, string) ([]domain.InFlightRedemption, error) {
	return fake.inFlight, nil
}

func (fake *fakeRedemptionProgress) ListAppliedRedemptions(_ context.Context, _ string, since time.Time) ([]domain.AppliedRedemption, error) {
	fake.since = since
	return fake.applied, fake.appliedErr
}

// closedRedeemedPosition is the ledger row after ApplyRedemption: zero shares,
// CLOSED, cost basis released, settlement evidence retained.
func closedRedeemedPosition() domain.Position {
	position := settledPendingRedeemPosition()
	position.LifecycleStatus = domain.PositionLifecycleClosed
	position.TotalShares, position.AvailableShares, position.CostBasis, position.AverageCostPrice = "0", "0", "0", "0"
	return position
}

func appliedRedemption(appliedAt time.Time, shares domain.Decimal) domain.AppliedRedemption {
	return domain.AppliedRedemption{
		ExecutionAccountID: "account-1", ConditionID: "0xcondition-1",
		TransactionHash: "0xab", AppliedAt: appliedAt,
		RedeemedShares: map[string]domain.Decimal{"token-1": shares},
	}
}

// staleRedeemedExternal is the Data API row for a token whose burn the indexer
// has not processed yet.
func staleRedeemedExternal(shares domain.Decimal) []domain.ExternalPosition {
	return []domain.ExternalPosition{{
		ConditionID: "0xCondition-1", TokenID: "token-1", OutcomeName: "No", Shares: shares,
		CurrentPrice: "1", Redeemable: true, Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}
}

func settledPendingRedeemPosition() domain.Position {
	settledAt := testNow.Add(-time.Hour)
	return domain.Position{
		ExecutionAccountID: "account-1", MarketID: "market-1", ConditionID: "0xCondition-1",
		TokenID: "token-1", OutcomeName: "No", TotalShares: "3.35", AvailableShares: "3.35", ReservedShares: "0",
		CostBasis: "0.8589", AverageCostPrice: "0.2564", LifecycleStatus: domain.PositionLifecycleSettledPendingRedeem,
		SettlementPrice: "1", SettlementSource: "POLYMARKET_DATA_API:0xcondition-1", SettledAt: &settledAt,
	}
}

func inFlightRedemption(status domain.RedemptionStatus, payout domain.Decimal) domain.InFlightRedemption {
	return domain.InFlightRedemption{
		ExecutionAccountID: "account-1", ConditionID: "0xcondition-1", Status: status, ExpectedPayout: payout,
	}
}

// newRedemptionWindowTestService models the window after the redeem transaction
// executed: the position is gone from the Data API and the chain balance grew.
func newRedemptionWindowTestService(
	t *testing.T, ledger *fakeLedger, external []domain.ExternalPosition, chainBalance domain.Decimal,
	redemptions port.RedemptionProgressSource,
) *Service {
	t.Helper()
	return newTestService(t, Params{
		Orders: &fakeOrders{}, Venue: &fakeVenue{}, Ledger: ledger,
		Fills: &fakeFills{}, OrderRefresher: &fakeRefresher{}, Redemptions: redemptions,
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return external, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: chainBalance, Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})
}

func TestRedeemWindowIsNotRecordedAsDrift(t *testing.T) {
	for _, status := range []domain.RedemptionStatus{
		domain.RedemptionRedeemSubmitting, domain.RedemptionRedeemSubmitted, domain.RedemptionConfirmed,
	} {
		t.Run(string(status), func(t *testing.T) {
			ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{settledPendingRedeemPosition()}}
			service := newRedemptionWindowTestService(t, ledger, nil, "103.35",
				redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
					return []domain.InFlightRedemption{inFlightRedemption(status, "3.35")}, nil
				}))

			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			if err != nil {
				t.Fatalf("RunAccount() error = %v", err)
			}
			if result.Run.Status != domain.ReconciliationRunCompleted {
				t.Fatalf("run status = %s, want COMPLETED", result.Run.Status)
			}
			if len(result.Issues) != 0 {
				t.Fatalf("redeem window produced issues: %#v", result.Issues)
			}
			if result.Run.Summary["in_flight_redemptions"] != 1 || result.Run.Summary["redeeming_positions_pending_ledger"] != 1 {
				t.Fatalf("summary = %#v", result.Run.Summary)
			}
		})
	}
}

func TestRedeemWindowBeforeTransactionLandsIsNotDrift(t *testing.T) {
	// The redeem was submitted but has not executed: the wallet still holds the
	// settled position and the chain balance is unchanged.
	ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{settledPendingRedeemPosition()}}
	service := newRedemptionWindowTestService(t, ledger, []domain.ExternalPosition{{
		ConditionID: "0xCondition-1", TokenID: "token-1", OutcomeName: "No", Shares: "3.35",
		CurrentPrice: "1", Redeemable: true, Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}, "100", redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
		return []domain.InFlightRedemption{inFlightRedemption(domain.RedemptionRedeemSubmitted, "3.35")}, nil
	}))

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if len(result.Issues) != 0 || len(ledger.settled) != 0 {
		t.Fatalf("pre-execution window produced issues=%#v settled=%#v", result.Issues, ledger.settled)
	}
}

func TestRedeemWindowToleratesSnapshotLagBetweenSources(t *testing.T) {
	// The RPC balance already shows the payout while the Data API snapshot
	// still lists the position: nothing "landed" by the position rule, but the
	// full in-flight payout explains the balance.
	ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{settledPendingRedeemPosition()}}
	service := newRedemptionWindowTestService(t, ledger, []domain.ExternalPosition{{
		ConditionID: "0xCondition-1", TokenID: "token-1", OutcomeName: "No", Shares: "3.35",
		CurrentPrice: "1", Redeemable: true, Source: "POLYMARKET_DATA_API", ObservedAt: testNow,
	}}, "103.35", redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
		return []domain.InFlightRedemption{inFlightRedemption(domain.RedemptionRedeemSubmitted, "3.35")}, nil
	}))

	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if hasIssue(result.Issues, domain.ReconciliationIssueBalanceDrift) {
		t.Fatalf("balance explained by in-flight payout was recorded as drift: %#v", result.Issues)
	}
}

func TestRedeemWindowWithoutInFlightRedemptionStillRecordsDrift(t *testing.T) {
	ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{settledPendingRedeemPosition()}}
	for name, source := range map[string]port.RedemptionProgressSource{
		"nil source": nil,
		"empty source": redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
			return nil, nil
		}),
		"other condition": redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
			// A different condition with a different payout explains neither
			// the vanished position nor the 3.35 balance gain.
			redemption := inFlightRedemption(domain.RedemptionConfirmed, "1")
			redemption.ConditionID = "0xcondition-other"
			return []domain.InFlightRedemption{redemption}, nil
		}),
		"not yet submitted": redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
			return nil, nil // READY/APPROVAL_* rows are never returned as in flight
		}),
	} {
		t.Run(name, func(t *testing.T) {
			service := newRedemptionWindowTestService(t, ledger, nil, "103.35", source)
			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			if err != nil {
				t.Fatalf("RunAccount() error = %v", err)
			}
			assertIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift, domain.ReconciliationIssueOpen)
			assertIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift, domain.ReconciliationIssueOpen)
		})
	}
}

func TestRedeemWindowDoesNotExcuseOpenPositionOrWrongPayout(t *testing.T) {
	redemptions := redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
		return []domain.InFlightRedemption{inFlightRedemption(domain.RedemptionConfirmed, "3.35")}, nil
	})
	t.Run("open position vanished", func(t *testing.T) {
		position := settledPendingRedeemPosition()
		position.LifecycleStatus = domain.PositionLifecycleOpen
		position.SettlementPrice, position.SettlementSource, position.SettledAt = "", "", nil
		ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{position}}
		service := newRedemptionWindowTestService(t, ledger, nil, "103.35", redemptions)
		result, err := service.RunAccount(context.Background(), RunAccountParams{
			ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
		})
		if err != nil {
			t.Fatalf("RunAccount() error = %v", err)
		}
		assertIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift, domain.ReconciliationIssueOpen)
	})
	t.Run("balance differs from payout", func(t *testing.T) {
		ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{settledPendingRedeemPosition()}}
		service := newRedemptionWindowTestService(t, ledger, nil, "104", redemptions)
		result, err := service.RunAccount(context.Background(), RunAccountParams{
			ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
		})
		if err != nil {
			t.Fatalf("RunAccount() error = %v", err)
		}
		if hasIssue(result.Issues, domain.ReconciliationIssuePositionDrift) {
			t.Fatalf("settled redeeming position was recorded as drift: %#v", result.Issues)
		}
		assertIssue(t, result.Issues, domain.ReconciliationIssueBalanceDrift, domain.ReconciliationIssueOpen)
	})
}

func TestRedemptionProgressFailureSkipsAssetComparisonWithoutManualDrift(t *testing.T) {
	ledger := &fakeLedger{balance: testBalance("100"), positions: []domain.Position{settledPendingRedeemPosition()}}
	for name, source := range map[string]port.RedemptionProgressSource{
		"read error": redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
			return nil, errors.New("postgres unavailable")
		}),
		"invalid status": redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
			return []domain.InFlightRedemption{inFlightRedemption(domain.RedemptionReady, "3.35")}, nil
		}),
		"foreign account": redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
			redemption := inFlightRedemption(domain.RedemptionConfirmed, "3.35")
			redemption.ExecutionAccountID = "account-other"
			return []domain.InFlightRedemption{redemption}, nil
		}),
		"negative payout": redemptionProgressFunc(func(context.Context, string) ([]domain.InFlightRedemption, error) {
			return []domain.InFlightRedemption{inFlightRedemption(domain.RedemptionConfirmed, "-1")}, nil
		}),
	} {
		t.Run(name, func(t *testing.T) {
			service := newRedemptionWindowTestService(t, ledger, nil, "103.35", source)
			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			if err == nil {
				t.Fatal("RunAccount() error = nil, want redemption read failure")
			}
			if result.Run.Status == domain.ReconciliationRunCompleted {
				t.Fatalf("run completed despite unreadable redemption state: %#v", result.Run)
			}
			for _, issue := range result.Issues {
				if issue.Resolution == domain.ReconciliationResolutionManual {
					t.Fatalf("manual issue recorded without redemption visibility: %#v", issue)
				}
			}
			assertIssue(t, result.Issues, domain.ReconciliationIssueSourceUnavailable, domain.ReconciliationIssueOpen)
		})
	}
}

func TestAppliedRedemptionIndexerLagIsRetryLaterNotManualDrift(t *testing.T) {
	ledger := &fakeLedger{balance: testBalance("103.35"), positions: []domain.Position{closedRedeemedPosition()}}
	progress := &fakeRedemptionProgress{applied: []domain.AppliedRedemption{appliedRedemption(testNow.Add(-5*time.Minute), "3.35")}}
	service := newRedemptionWindowTestService(t, ledger, staleRedeemedExternal("3.35"), "103.35", progress)
	result, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
	})
	if err != nil {
		t.Fatalf("RunAccount() error = %v", err)
	}
	if !progress.since.Equal(testNow.Add(-appliedRedemptionGracePeriod)) {
		t.Fatalf("applied redemptions were read since %s, want the 30 minute grace boundary", progress.since)
	}
	if len(result.Issues) != 1 {
		t.Fatalf("issues = %#v, want exactly one transient position issue", result.Issues)
	}
	issue := result.Issues[0]
	if issue.Type != domain.ReconciliationIssuePositionDrift || issue.Resolution != domain.ReconciliationResolutionRetry ||
		issue.Status != domain.ReconciliationIssueOpen || issue.TokenID != "token-1" {
		t.Fatalf("indexer lag issue = %#v, want OPEN POSITION_DRIFT with RETRY_LATER", issue)
	}
	if result.Run.Summary["redeemed_positions_awaiting_index"] != 1 || result.Run.Summary["applied_redemptions_in_grace"] != 1 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
}

func TestAppliedRedemptionGraceDoesNotExcuseUnexplainedRemotePosition(t *testing.T) {
	for name, tc := range map[string]struct {
		local    domain.Position
		external []domain.ExternalPosition
		progress *fakeRedemptionProgress
	}{
		"no applied redemption in grace": {
			local: closedRedeemedPosition(), external: staleRedeemedExternal("3.35"),
			progress: &fakeRedemptionProgress{},
		},
		"remote shares differ from redeemed shares": {
			local: closedRedeemedPosition(), external: staleRedeemedExternal("5"),
			progress: &fakeRedemptionProgress{applied: []domain.AppliedRedemption{appliedRedemption(testNow.Add(-5*time.Minute), "3.35")}},
		},
		"remote token is no longer redeemable": {
			local: closedRedeemedPosition(),
			external: func() []domain.ExternalPosition {
				external := staleRedeemedExternal("3.35")
				external[0].Redeemable = false
				return external
			}(),
			progress: &fakeRedemptionProgress{applied: []domain.AppliedRedemption{appliedRedemption(testNow.Add(-5*time.Minute), "3.35")}},
		},
		"redemption burned a different token": {
			local: closedRedeemedPosition(), external: staleRedeemedExternal("3.35"),
			progress: &fakeRedemptionProgress{applied: []domain.AppliedRedemption{func() domain.AppliedRedemption {
				applied := appliedRedemption(testNow.Add(-5*time.Minute), "3.35")
				applied.RedeemedShares = map[string]domain.Decimal{"token-2": "3.35"}
				return applied
			}()}},
		},
		"local position still open": {
			local: func() domain.Position {
				position := closedRedeemedPosition()
				position.LifecycleStatus = domain.PositionLifecycleOpen
				return position
			}(),
			external: staleRedeemedExternal("3.35"),
			progress: &fakeRedemptionProgress{applied: []domain.AppliedRedemption{appliedRedemption(testNow.Add(-5*time.Minute), "3.35")}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ledger := &fakeLedger{balance: testBalance("103.35"), positions: []domain.Position{tc.local}}
			service := newRedemptionWindowTestService(t, ledger, tc.external, "103.35", tc.progress)
			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			if err != nil {
				t.Fatalf("RunAccount() error = %v", err)
			}
			assertIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift, domain.ReconciliationIssueOpen)
			for _, issue := range result.Issues {
				if issue.Type == domain.ReconciliationIssuePositionDrift && issue.Resolution != domain.ReconciliationResolutionManual {
					t.Fatalf("unexplained remote position was not manual drift: %#v", issue)
				}
			}
		})
	}
}

func TestAppliedRedemptionReadFailureSkipsAssetComparisonWithoutManualDrift(t *testing.T) {
	ledger := &fakeLedger{balance: testBalance("103.35"), positions: []domain.Position{closedRedeemedPosition()}}
	for name, progress := range map[string]*fakeRedemptionProgress{
		"read error":           {appliedErr: errors.New("postgres unavailable")},
		"applied before grace": {applied: []domain.AppliedRedemption{appliedRedemption(testNow.Add(-2*time.Hour), "3.35")}},
		"zero redeemed shares": {applied: []domain.AppliedRedemption{appliedRedemption(testNow.Add(-5*time.Minute), "0")}},
		"foreign account": {applied: []domain.AppliedRedemption{func() domain.AppliedRedemption {
			applied := appliedRedemption(testNow.Add(-5*time.Minute), "3.35")
			applied.ExecutionAccountID = "account-other"
			return applied
		}()}},
	} {
		t.Run(name, func(t *testing.T) {
			service := newRedemptionWindowTestService(t, ledger, staleRedeemedExternal("3.35"), "103.35", progress)
			result, err := service.RunAccount(context.Background(), RunAccountParams{
				ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled,
			})
			if err == nil {
				t.Fatal("RunAccount() error = nil, want applied redemption read failure")
			}
			for _, issue := range result.Issues {
				if issue.Resolution == domain.ReconciliationResolutionManual {
					t.Fatalf("manual issue recorded without redemption visibility: %#v", issue)
				}
			}
			assertIssue(t, result.Issues, domain.ReconciliationIssueSourceUnavailable, domain.ReconciliationIssueOpen)
		})
	}
}

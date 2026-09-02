package reconciliation

import (
	"context"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// inFlightRedemptions is the per-run view of redemptions that may already have
// mutated the chain. The window between the redeem transaction landing and
// ApplyRedemption writing payout/PnL is expected, not drift: the settled
// position disappears from the wallet and the collateral grows by exactly the
// binary payout. Without this view every automated redemption would leave a
// permanent MANUAL_REVIEW issue that blocks the account's new orders.
type inFlightRedemptions struct {
	byCondition map[string]domain.InFlightRedemption
	// landed holds conditions whose settled position is already absent from
	// the external snapshot, i.e. the redeem transaction has executed.
	landed map[string]struct{}
}

// loadInFlightRedemptions reads the account's in-flight redemptions before any
// position or balance comparison. A read failure is infrastructure uncertainty
// (RETRY_LATER), never a reason to fall back to recording manual drift.
func (state *runState) loadInFlightRedemptions(ctx context.Context, executionAccountID string) error {
	state.redemptions = inFlightRedemptions{
		byCondition: make(map[string]domain.InFlightRedemption),
		landed:      make(map[string]struct{}),
	}
	if state.service.redemptions == nil {
		return nil
	}
	values, err := state.service.redemptions.ListInFlightRedemptions(ctx, executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "read in-flight redemptions", err)
		return err
	}
	for _, value := range values {
		conditionID := strings.ToLower(strings.TrimSpace(value.ConditionID))
		if strings.TrimSpace(value.ExecutionAccountID) != executionAccountID || conditionID == "" || !value.Status.InFlight() {
			err := fmt.Errorf("in-flight redemption %q/%q has invalid identity or status %q", value.ExecutionAccountID, value.ConditionID, value.Status)
			state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "validate in-flight redemptions", err)
			return err
		}
		if sign, signErr := value.ExpectedPayout.Sign(); signErr != nil || sign < 0 {
			err := fmt.Errorf("in-flight redemption %s has invalid expected payout %q", conditionID, value.ExpectedPayout)
			state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "validate in-flight redemptions", err)
			return err
		}
		if _, duplicate := state.redemptions.byCondition[conditionID]; duplicate {
			err := fmt.Errorf("in-flight redemption %s is duplicated", conditionID)
			state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "validate in-flight redemptions", err)
			return err
		}
		state.redemptions.byCondition[conditionID] = value
	}
	state.run.Summary["in_flight_redemptions"] = len(state.redemptions.byCondition)
	return nil
}

// absentPositionIsRedeeming reports whether a local position missing from the
// external snapshot is explained by an in-flight redemption of its condition.
// Only a SETTLED_PENDING_REDEEM position qualifies; an OPEN position that
// vanishes is still unexplained drift even if the condition is being redeemed.
func (state *runState) absentPositionIsRedeeming(position domain.Position) bool {
	if position.LifecycleStatus != domain.PositionLifecycleSettledPendingRedeem {
		return false
	}
	conditionID := strings.ToLower(strings.TrimSpace(position.ConditionID))
	if _, inFlight := state.redemptions.byCondition[conditionID]; !inFlight {
		return false
	}
	state.redemptions.landed[conditionID] = struct{}{}
	return true
}

// balanceExplainedByRedemptions accepts an on-chain balance that equals the
// ledger plus the payout of redemptions that have already executed. Both the
// "landed" subset (position already gone from the snapshot) and the full
// in-flight set are tried because the RPC balance and the Data API snapshot
// are observed at slightly different moments.
func (state *runState) balanceExplainedByRedemptions(local, external domain.Decimal) bool {
	if len(state.redemptions.byCondition) == 0 {
		return false
	}
	epsilon := state.service.balanceEpsilon
	for _, subset := range [][]string{state.landedConditions(), state.inFlightConditions()} {
		if len(subset) == 0 {
			continue
		}
		expected := local
		for _, conditionID := range subset {
			sum, err := addDecimals(expected, state.redemptions.byCondition[conditionID].ExpectedPayout)
			if err != nil {
				return false
			}
			expected = sum
		}
		if within(expected, external, epsilon) {
			return true
		}
	}
	return false
}

func (state *runState) landedConditions() []string {
	result := make([]string, 0, len(state.redemptions.landed))
	for conditionID := range state.redemptions.landed {
		result = append(result, conditionID)
	}
	return result
}

func (state *runState) inFlightConditions() []string {
	result := make([]string, 0, len(state.redemptions.byCondition))
	for conditionID := range state.redemptions.byCondition {
		result = append(result, conditionID)
	}
	return result
}

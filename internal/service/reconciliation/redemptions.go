package reconciliation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// appliedRedemptionGracePeriod bounds how long after ApplyRedemption a burned
// token may still appear in the external position snapshot before the
// discrepancy is treated as manual drift. Redemption receipts wait for the
// configured confirmation depth, so the Data API normally catches up within
// seconds; the window only absorbs an indexer that is unusually behind.
const appliedRedemptionGracePeriod = 30 * time.Minute

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
	// applied holds redemptions the ledger already applied within the grace
	// period, keyed by lower-case condition id. The external snapshot may still
	// list their burned tokens until the indexer catches up.
	applied map[string]domain.AppliedRedemption
}

// loadInFlightRedemptions reads the account's in-flight redemptions before any
// position or balance comparison. A read failure is infrastructure uncertainty
// (RETRY_LATER), never a reason to fall back to recording manual drift.
func (state *runState) loadInFlightRedemptions(ctx context.Context, executionAccountID string) error {
	state.redemptions = inFlightRedemptions{
		byCondition: make(map[string]domain.InFlightRedemption),
		landed:      make(map[string]struct{}),
		applied:     make(map[string]domain.AppliedRedemption),
	}
	if state.service.redemptions == nil {
		return nil
	}
	if err := state.loadAppliedRedemptions(ctx, executionAccountID); err != nil {
		return err
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

// loadAppliedRedemptions reads redemptions applied within the grace period so a
// burned token still present in a lagging external snapshot is classified as
// transient rather than manual drift.
func (state *runState) loadAppliedRedemptions(ctx context.Context, executionAccountID string) error {
	since := state.now.Add(-appliedRedemptionGracePeriod)
	values, err := state.service.redemptions.ListAppliedRedemptions(ctx, executionAccountID, since)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "read applied redemptions", err)
		return err
	}
	for _, value := range values {
		conditionID := strings.ToLower(strings.TrimSpace(value.ConditionID))
		if strings.TrimSpace(value.ExecutionAccountID) != executionAccountID || conditionID == "" ||
			value.AppliedAt.IsZero() || value.AppliedAt.Before(since) || len(value.RedeemedShares) == 0 {
			err := fmt.Errorf("applied redemption %q/%q has invalid identity, time, or shares", value.ExecutionAccountID, value.ConditionID)
			state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "validate applied redemptions", err)
			return err
		}
		for tokenID, shares := range value.RedeemedShares {
			if sign, signErr := shares.Sign(); strings.TrimSpace(tokenID) == "" || signErr != nil || sign <= 0 {
				err := fmt.Errorf("applied redemption %s has invalid redeemed shares for token %q", conditionID, tokenID)
				state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "validate applied redemptions", err)
				return err
			}
		}
		if _, duplicate := state.redemptions.applied[conditionID]; duplicate {
			err := fmt.Errorf("applied redemption %s is duplicated", conditionID)
			state.addInfrastructureIssue(ctx, "POSTGRES_REDEMPTIONS", "validate applied redemptions", err)
			return err
		}
		state.redemptions.applied[conditionID] = value
	}
	state.run.Summary["applied_redemptions_in_grace"] = len(state.redemptions.applied)
	return nil
}

// externalPositionIsRedeemedAwaitingIndex reports whether an external position
// that the ledger already closed through a redemption is explained by indexer
// lag: the local position is CLOSED with zero shares, the redemption was
// applied within the grace period, the token is still flagged redeemable, and
// the external quantity equals exactly what the redemption burned.
func (state *runState) externalPositionIsRedeemedAwaitingIndex(position domain.Position, external domain.ExternalPosition) bool {
	if position.LifecycleStatus != domain.PositionLifecycleClosed || !external.Redeemable {
		return false
	}
	if sign, err := position.TotalShares.Sign(); err != nil || sign != 0 {
		return false
	}
	applied, exists := state.redemptions.applied[strings.ToLower(strings.TrimSpace(position.ConditionID))]
	if !exists {
		return false
	}
	redeemed, exists := applied.RedeemedShares[strings.TrimSpace(position.TokenID)]
	if !exists {
		return false
	}
	return managedPositionSharesMatch(redeemed, external, state.service.positionEpsilon)
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

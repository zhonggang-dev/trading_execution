package liveoperations

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// livePositionBaselineSet is one sealed account cutover snapshot indexed by
// token. The unmanaged shares remain evidence and never become a live position.
type livePositionBaselineSet struct {
	baselineID string
	byToken    map[string]domain.ExternalPositionBaseline
}

func makeLivePositionBaselineSet(
	executionAccountID string,
	values []domain.ExternalPositionBaseline,
) (livePositionBaselineSet, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	result := livePositionBaselineSet{byToken: make(map[string]domain.ExternalPositionBaseline, len(values))}
	for _, baseline := range values {
		baseline.BaselineID = strings.TrimSpace(baseline.BaselineID)
		baseline.ExecutionAccountID = strings.TrimSpace(baseline.ExecutionAccountID)
		baseline.ConditionID = strings.TrimSpace(baseline.ConditionID)
		baseline.TokenID = strings.TrimSpace(baseline.TokenID)
		baseline.OutcomeName = strings.TrimSpace(baseline.OutcomeName)
		baseline.Source = strings.TrimSpace(baseline.Source)
		baseline.Actor = strings.TrimSpace(baseline.Actor)
		baseline.Reason = strings.TrimSpace(baseline.Reason)
		if baseline.BaselineID == "" || baseline.ExecutionAccountID != executionAccountID ||
			baseline.ConditionID == "" || baseline.TokenID == "" || baseline.OutcomeName == "" ||
			baseline.Source == "" || baseline.Actor == "" || baseline.Reason == "" || baseline.ObservedAt.IsZero() {
			return livePositionBaselineSet{}, fmt.Errorf("external position baseline has incomplete or mismatched identity for token %q", baseline.TokenID)
		}
		if result.baselineID == "" {
			result.baselineID = baseline.BaselineID
		} else if result.baselineID != baseline.BaselineID {
			return livePositionBaselineSet{}, fmt.Errorf("execution account %q has multiple external position baseline sets", executionAccountID)
		}
		if baseline.OutcomeIndex != nil && *baseline.OutcomeIndex < 0 {
			return livePositionBaselineSet{}, fmt.Errorf("external position baseline token %q has invalid outcome index", baseline.TokenID)
		}
		if shares, err := decimalRat(baseline.Shares); err != nil || shares.Sign() <= 0 {
			return livePositionBaselineSet{}, fmt.Errorf("external position baseline token %q must have positive shares", baseline.TokenID)
		}
		var evidence map[string]any
		if err := json.Unmarshal(baseline.Evidence, &evidence); err != nil || len(evidence) == 0 {
			return livePositionBaselineSet{}, fmt.Errorf("external position baseline token %q must have non-empty object evidence", baseline.TokenID)
		}
		if _, duplicate := result.byToken[baseline.TokenID]; duplicate {
			return livePositionBaselineSet{}, fmt.Errorf("external position baseline contains duplicate token %q", baseline.TokenID)
		}
		result.byToken[baseline.TokenID] = baseline
	}
	return result, nil
}

// managedExternalPosition validates the immutable ownership equation and
// projects the remote total down to the managed local quantity used by the
// operations API and risk display. A baseline-only token returns include=false.
func managedExternalPosition(
	accountID string,
	external domain.ExternalPosition,
	baseline domain.ExternalPositionBaseline,
	localByKey map[string]domain.LiveLedgerPosition,
) (domain.ExternalPosition, bool, error) {
	local, hasLocal := localByKey[livePositionKey(accountID, external.TokenID)]
	if !baselineMatchesLiveExternal(baseline, external) {
		return domain.ExternalPosition{}, false, fmt.Errorf(
			"external position baseline identity drift for account %q token %q",
			accountID, strings.TrimSpace(external.TokenID),
		)
	}
	if hasLocal && !baselineMatchesLiveLocal(baseline, local.Position) {
		return domain.ExternalPosition{}, false, fmt.Errorf(
			"managed position and external baseline identity differ for account %q token %q",
			accountID, strings.TrimSpace(external.TokenID),
		)
	}
	baselineShares, err := decimalRat(baseline.Shares)
	if err != nil {
		return domain.ExternalPosition{}, false, fmt.Errorf("parse baseline shares for token %q: %w", baseline.TokenID, err)
	}
	expected := baselineShares
	if hasLocal {
		managedShares, parseErr := decimalRat(local.Position.TotalShares)
		if parseErr != nil || managedShares.Sign() < 0 {
			return domain.ExternalPosition{}, false, fmt.Errorf("managed shares for token %q are invalid", baseline.TokenID)
		}
		expected = expected.Add(expected, managedShares)
	}
	remoteShares, err := decimalRat(external.Shares)
	if err != nil || remoteShares.Sign() <= 0 {
		return domain.ExternalPosition{}, false, fmt.Errorf("external shares for baseline token %q are invalid", baseline.TokenID)
	}
	if expected.Cmp(remoteShares) != 0 {
		return domain.ExternalPosition{}, false, fmt.Errorf(
			"external position baseline quantity drift for account %q token %q",
			accountID, baseline.TokenID,
		)
	}
	if !hasLocal {
		return domain.ExternalPosition{}, false, nil
	}
	managed := external
	managed.Shares = local.Position.TotalShares
	return managed, true, nil
}

func baselineMatchesLiveExternal(baseline domain.ExternalPositionBaseline, external domain.ExternalPosition) bool {
	return baseline.ConditionID == strings.TrimSpace(external.ConditionID) &&
		baseline.TokenID == strings.TrimSpace(external.TokenID) &&
		baseline.OutcomeName == strings.TrimSpace(external.OutcomeName) &&
		baseline.NegRisk == external.NegRisk &&
		!external.ObservedAt.IsZero() && !external.ObservedAt.Before(baseline.ObservedAt) &&
		liveOutcomeIndexesEqual(baseline.OutcomeIndex, external.OutcomeIndex)
}

func baselineMatchesLiveLocal(baseline domain.ExternalPositionBaseline, local domain.Position) bool {
	return baseline.ExecutionAccountID == strings.TrimSpace(local.ExecutionAccountID) &&
		baseline.ConditionID == strings.TrimSpace(local.ConditionID) &&
		baseline.TokenID == strings.TrimSpace(local.TokenID) &&
		baseline.OutcomeName == strings.TrimSpace(local.OutcomeName) &&
		liveOutcomeIndexesEqual(baseline.OutcomeIndex, local.OutcomeIndex)
}

func liveOutcomeIndexesEqual(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

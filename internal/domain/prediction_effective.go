package domain

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// SelectEffectivePredictions converts an immutable lookback snapshot into one
// deterministic PIT row per (market, configured source model). It also proves
// that models referring to the same Market agree on its execution identity.
func SelectEffectivePredictions(predictions []Prediction, modelIDs []string) ([]Prediction, error) {
	models := predictionModelSet(modelIDs)
	latest := make(map[string]Prediction)
	for _, prediction := range predictions {
		predictionModelID := strings.TrimSpace(prediction.Model.Name)
		if _, selected := models[predictionModelID]; !selected {
			continue
		}
		key := strings.TrimSpace(prediction.MarketID) + "\x00" + predictionModelID
		current, exists := latest[key]
		if !exists {
			latest[key] = prediction
			continue
		}
		newer, err := comparePredictionRecency(prediction, current)
		if err != nil {
			return nil, fmt.Errorf(
				"prediction snapshot has ambiguous revisions for market %q and model %q (%s, %s): %w",
				prediction.MarketID, predictionModelID, current.PredictionID, prediction.PredictionID, err,
			)
		}
		if newer {
			latest[key] = prediction
		}
	}
	result := make([]Prediction, 0, len(latest))
	for _, prediction := range latest {
		result = append(result, prediction)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].MarketID != result[j].MarketID {
			return result[i].MarketID < result[j].MarketID
		}
		if result[i].Model.Name != result[j].Model.Name {
			return result[i].Model.Name < result[j].Model.Name
		}
		return result[i].PredictionID < result[j].PredictionID
	})
	if err := validatePredictionMarketIdentities(result); err != nil {
		return nil, err
	}
	return result, nil
}

// SelectEffectivePredictionExpectations reduces the immutable producer task
// manifest to the latest task for each (market, configured source model). A
// newer PENDING task deliberately replaces an older COMPLETED task so an old
// probability cannot make the current generation look complete.
func SelectEffectivePredictionExpectations(expectations []PredictionExpectation, modelIDs []string) ([]PredictionExpectation, error) {
	models := predictionModelSet(modelIDs)
	latest := make(map[string]PredictionExpectation)
	for _, expectation := range expectations {
		modelID := strings.TrimSpace(expectation.PredictionModelID)
		if _, selected := models[modelID]; !selected {
			continue
		}
		key := strings.TrimSpace(expectation.MarketID) + "\x00" + modelID
		current, exists := latest[key]
		if !exists || predictionExpectationIsNewer(expectation, current) {
			latest[key] = expectation
		}
	}
	result := make([]PredictionExpectation, 0, len(latest))
	for _, expectation := range latest {
		result = append(result, expectation)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].MarketID != result[j].MarketID {
			return result[i].MarketID < result[j].MarketID
		}
		if result[i].PredictionModelID != result[j].PredictionModelID {
			return result[i].PredictionModelID < result[j].PredictionModelID
		}
		return result[i].PredictionID < result[j].PredictionID
	})
	if err := validateExpectationMarketIdentities(result); err != nil {
		return nil, err
	}
	return result, nil
}

func predictionModelSet(modelIDs []string) map[string]struct{} {
	models := make(map[string]struct{}, len(modelIDs))
	for _, modelID := range modelIDs {
		modelID = strings.TrimSpace(modelID)
		if modelID != "" {
			models[modelID] = struct{}{}
		}
	}
	return models
}

func predictionExpectationIsNewer(candidate, current PredictionExpectation) bool {
	if !candidate.PredictionAsOf.Equal(current.PredictionAsOf) {
		return candidate.PredictionAsOf.After(current.PredictionAsOf)
	}
	if !candidate.TaskAvailableAt.Equal(current.TaskAvailableAt) {
		return candidate.TaskAvailableAt.After(current.TaskAvailableAt)
	}
	if candidate.SelectionRunID != current.SelectionRunID {
		return candidate.SelectionRunID > current.SelectionRunID
	}
	if candidate.SelectionID != current.SelectionID {
		return candidate.SelectionID > current.SelectionID
	}
	return candidate.PredictionID > current.PredictionID
}

func comparePredictionRecency(candidate, current Prediction) (bool, error) {
	if !candidate.PredictionAsOf.Equal(current.PredictionAsOf) {
		return candidate.PredictionAsOf.After(current.PredictionAsOf), nil
	}
	if !candidate.AvailableAt.Equal(current.AvailableAt) {
		return candidate.AvailableAt.After(current.AvailableAt), nil
	}
	if !candidate.CompletedAt.Equal(current.CompletedAt) {
		return candidate.CompletedAt.After(current.CompletedAt), nil
	}
	if !equivalentPredictionPayload(candidate, current) {
		return false, fmt.Errorf("equal PIT timestamps have different prediction payloads")
	}
	return candidate.PredictionID > current.PredictionID, nil
}

func equivalentPredictionPayload(first, second Prediction) bool {
	stripDeliveryIdentity := func(prediction Prediction) Prediction {
		prediction.PredictionID = ""
		prediction.SourceJobID = ""
		prediction.SandboxID = ""
		prediction.CompletedAt = time.Time{}
		prediction.AvailableAt = time.Time{}
		return prediction
	}
	return reflect.DeepEqual(stripDeliveryIdentity(first), stripDeliveryIdentity(second))
}

func validatePredictionMarketIdentities(predictions []Prediction) error {
	type identity struct {
		conditionID string
		negRisk     bool
		outcomes    []PredictionOutcome
	}
	byMarket := make(map[string]identity)
	for _, prediction := range predictions {
		marketID := strings.TrimSpace(prediction.MarketID)
		current, exists := byMarket[marketID]
		if !exists {
			byMarket[marketID] = identity{
				conditionID: strings.TrimSpace(prediction.ConditionID),
				negRisk:     prediction.NegRisk,
				outcomes:    append([]PredictionOutcome(nil), prediction.Outcomes...),
			}
			continue
		}
		if current.conditionID != strings.TrimSpace(prediction.ConditionID) || current.negRisk != prediction.NegRisk ||
			!samePredictionOutcomes(current.outcomes, prediction.Outcomes) {
			return fmt.Errorf(
				"prediction snapshot has conflicting market identity across models for market %q", prediction.MarketID,
			)
		}
	}
	return nil
}

func validateExpectationMarketIdentities(expectations []PredictionExpectation) error {
	type identity struct {
		conditionID string
		outcomes    []PredictionOutcome
	}
	byMarket := make(map[string]identity)
	for _, expectation := range expectations {
		marketID := strings.TrimSpace(expectation.MarketID)
		current, exists := byMarket[marketID]
		if !exists {
			byMarket[marketID] = identity{
				conditionID: strings.TrimSpace(expectation.ConditionID),
				outcomes:    append([]PredictionOutcome(nil), expectation.Outcomes...),
			}
			continue
		}
		if current.conditionID != strings.TrimSpace(expectation.ConditionID) ||
			!samePredictionOutcomes(current.outcomes, expectation.Outcomes) {
			return fmt.Errorf(
				"prediction task manifest has conflicting market identity across models for market %q", expectation.MarketID,
			)
		}
	}
	return nil
}

func samePredictionOutcomes(first, second []PredictionOutcome) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index].Index != second[index].Index ||
			strings.TrimSpace(first[index].Name) != strings.TrimSpace(second[index].Name) ||
			strings.TrimSpace(first[index].TokenID) != strings.TrimSpace(second[index].TokenID) {
			return false
		}
	}
	return true
}

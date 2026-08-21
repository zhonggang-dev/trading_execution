package domain

import (
	"strings"
	"testing"
	"time"
)

func TestSelectEffectivePredictionsChoosesNewestPITRow(t *testing.T) {
	decisionAt := time.Date(2026, 8, 21, 4, 20, 0, 0, time.UTC)
	old := effectivePredictionFixture(decisionAt)
	newest := old
	newest.Outcomes = append([]PredictionOutcome(nil), old.Outcomes...)
	newest.PredictionID = "pred-new"
	newest.SourceJobID = "job-new"
	newest.PredictionAsOf = old.PredictionAsOf.Add(10 * time.Minute)
	newest.CompletedAt = old.CompletedAt.Add(10 * time.Minute)
	newest.AvailableAt = old.AvailableAt.Add(10 * time.Minute)
	newest.Outcomes[0].Probability = 0.8
	newest.Outcomes[1].Probability = 0.2

	selected, err := SelectEffectivePredictions([]Prediction{newest, old}, []string{"producer-a"})
	if err != nil {
		t.Fatalf("SelectEffectivePredictions() error = %v", err)
	}
	if len(selected) != 1 || selected[0].PredictionID != "pred-new" || selected[0].Outcomes[0].Probability != 0.8 {
		t.Fatalf("selected = %#v", selected)
	}
}

func TestSelectEffectivePredictionsRejectsAmbiguousEqualTimestampPayloads(t *testing.T) {
	decisionAt := time.Date(2026, 8, 21, 4, 20, 0, 0, time.UTC)
	first := effectivePredictionFixture(decisionAt)
	second := first
	second.Outcomes = append([]PredictionOutcome(nil), first.Outcomes...)
	second.PredictionID = "pred-conflict"
	second.SourceJobID = "job-conflict"
	second.Outcomes[0].Probability = 0.6
	second.Outcomes[1].Probability = 0.4

	for _, input := range [][]Prediction{{first, second}, {second, first}} {
		if _, err := SelectEffectivePredictions(input, []string{"producer-a"}); err == nil ||
			!strings.Contains(err.Error(), "ambiguous revisions") {
			t.Fatalf("SelectEffectivePredictions() error = %v", err)
		}
	}
}

func TestSelectEffectivePredictionsAllowsEquivalentDeliveryDuplicates(t *testing.T) {
	decisionAt := time.Date(2026, 8, 21, 4, 20, 0, 0, time.UTC)
	first := effectivePredictionFixture(decisionAt)
	second := first
	second.PredictionID = "pred-z"
	second.SourceJobID = "job-z"

	selected, err := SelectEffectivePredictions([]Prediction{second, first}, []string{"producer-a"})
	if err != nil {
		t.Fatalf("SelectEffectivePredictions() error = %v", err)
	}
	if len(selected) != 1 || selected[0].PredictionID != "pred-z" {
		t.Fatalf("selected = %#v, want deterministic equivalent row", selected)
	}
}

func TestSelectEffectivePredictionExpectationsKeepsNewestPendingGeneration(t *testing.T) {
	decisionAt := time.Date(2026, 8, 21, 4, 20, 0, 0, time.UTC)
	old := effectiveExpectationFixture(decisionAt)
	newest := old
	newest.PredictionID = "pred-new"
	newest.SourceJobID = "job-new"
	newest.SelectionID = 2
	newest.SelectionRunID = 2
	newest.PredictionAsOf = old.PredictionAsOf.Add(10 * time.Minute)
	newest.TaskAvailableAt = old.TaskAvailableAt.Add(10 * time.Minute)
	newest.Status = PredictionExpectationPending
	newest.ResultAvailableAt = nil

	selected, err := SelectEffectivePredictionExpectations(
		[]PredictionExpectation{newest, old}, []string{"producer-a"},
	)
	if err != nil {
		t.Fatalf("SelectEffectivePredictionExpectations() error = %v", err)
	}
	if len(selected) != 1 || selected[0].PredictionID != "pred-new" || selected[0].Status != PredictionExpectationPending {
		t.Fatalf("selected = %#v, want newest pending task", selected)
	}
}

func TestSelectEffectivePredictionExpectationsRejectsCrossModelIdentityMismatch(t *testing.T) {
	decisionAt := time.Date(2026, 8, 21, 4, 20, 0, 0, time.UTC)
	first := effectiveExpectationFixture(decisionAt)
	second := first
	second.PredictionID = "pred-b"
	second.SourceJobID = "job-b"
	second.PredictionModelID = "producer-b"
	second.ConditionID = "another-condition"

	if _, err := SelectEffectivePredictionExpectations(
		[]PredictionExpectation{first, second}, []string{"producer-a", "producer-b"},
	); err == nil || !strings.Contains(err.Error(), "conflicting market identity") {
		t.Fatalf("SelectEffectivePredictionExpectations() error = %v", err)
	}
}

func effectivePredictionFixture(decisionAt time.Time) Prediction {
	return Prediction{
		PredictionID: "pred-a", SourceJobID: "job-a", MarketID: "market-1", ConditionID: "condition-1",
		Question: "Will it happen?", Domains: []string{"Other"},
		Outcomes: []PredictionOutcome{
			{Index: 0, Name: "Yes", TokenID: "yes-token", Probability: 0.7},
			{Index: 1, Name: "No", TokenID: "no-token", Probability: 0.3},
		},
		PredictionAsOf: decisionAt.Add(-30 * time.Minute),
		CompletedAt:    decisionAt.Add(-10 * time.Minute), AvailableAt: decisionAt.Add(-9 * time.Minute),
		Model: PredictionModel{Name: "producer-a", PredictorVersion: "v1"},
	}
}

func effectiveExpectationFixture(decisionAt time.Time) PredictionExpectation {
	prediction := effectivePredictionFixture(decisionAt)
	resultAvailableAt := prediction.AvailableAt
	return PredictionExpectation{
		PredictionID: prediction.PredictionID, SourceJobID: prediction.SourceJobID,
		PredictionModelID: prediction.Model.Name, SelectionID: 1, SelectionRunID: 1,
		MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
		Outcomes:       append([]PredictionOutcome(nil), prediction.Outcomes...),
		PredictionAsOf: prediction.PredictionAsOf, TaskAvailableAt: prediction.PredictionAsOf,
		Status: PredictionExpectationCompleted, ResultAvailableAt: &resultAvailableAt,
	}
}

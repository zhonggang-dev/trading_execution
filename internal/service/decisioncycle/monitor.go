package decisioncycle

import (
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// EffectivePredictionsForMonitoring applies the exact production source,
// freshness and revision rules without projecting or executing a strategy.
// It deliberately shares the implementation used by Run.
func EffectivePredictionsForMonitoring(predictions []domain.Prediction, modelIDs []string, modes map[string]domain.PredictionSourceMode, freshAfter time.Time) ([]domain.Prediction, error) {
	return selectAvailablePredictions(predictions, modelIDs, modes, freshAfter)
}

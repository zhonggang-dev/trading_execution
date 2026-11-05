package decisioncycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// accountInput is one account's frozen strategy input for a cycle. A failed
// account keeps its failure and is never sent to the strategy.
type accountInput struct {
	predictions []domain.Prediction
	positions   []domain.StrategyPositionLot
	targets     []domain.BookTarget
	failure     *accountFailure
}

type accountFailure struct {
	stage string
	err   error
}

// cyclePredictions is the prediction input of a cycle. unavailable is set
// (and selected is empty) when the prediction source was unavailable.
type cyclePredictions struct {
	snapshotID  string
	selected    []domain.Prediction
	unavailable error
}

// unavailablePredictionSnapshotID is the stable local snapshot identity of an
// exit-only cycle. Real identities are predsnap-<hex>, so the prefix cannot
// collide, and the value is the same on every replay of one boundary.
func unavailablePredictionSnapshotID(decisionAt time.Time) string {
	return "predsnap-unavailable-" + decisionAt.UTC().Format("20060102T150405Z")
}

// loadPredictions reads and selects the cycle's predictions under its own
// budget. An availability failure (port.ErrPredictionSourceUnavailable or the
// request's own deadline) degrades the cycle to exit-only. Authentication
// errors, invalid data and an ended cycle context fail the cycle.
func (service *Service) loadPredictions(ctx context.Context, decisionAt time.Time) (cyclePredictions, error) {
	snapshotContext, cancel := withOptionalTimeout(ctx, service.predictionTimeout)
	snapshot, err := service.predictionSource.Snapshot(snapshotContext, decisionAt, service.predictionLookback)
	cancel()
	if err != nil {
		if ctx.Err() != nil || !predictionSourceUnavailable(err) {
			return cyclePredictions{}, fmt.Errorf("load prediction snapshot: %w", err)
		}
		category := "unavailable"
		if errors.Is(err, context.DeadlineExceeded) {
			category = "timeout"
		}
		snapshotID := unavailablePredictionSnapshotID(decisionAt)
		service.log().Warn("prediction source unavailable; decision cycle is exit-only",
			"decision_at", decisionAt, "prediction_snapshot_id", snapshotID,
			"category", category, "error", err)
		unavailable := fmt.Errorf("load prediction snapshot (cycle is exit-only): %w", err)
		if !errors.Is(err, port.ErrPredictionSourceUnavailable) {
			unavailable = fmt.Errorf("%w: load prediction snapshot (cycle is exit-only): %w",
				port.ErrPredictionSourceUnavailable, err)
		}
		return cyclePredictions{snapshotID: snapshotID, unavailable: unavailable}, nil
	}
	if err := snapshot.Validate(decisionAt); err != nil {
		return cyclePredictions{}, fmt.Errorf("validate prediction snapshot: %w", err)
	}
	selected, err := selectAvailablePredictions(
		snapshot.Predictions, configuredPredictionModels(service.strategyEntryEnabledBindings),
		service.predictionSourceModes, decisionAt.Add(-service.predictionLookback),
	)
	if err != nil {
		return cyclePredictions{}, err
	}
	return cyclePredictions{snapshotID: snapshot.SnapshotID, selected: selected}, nil
}

func predictionSourceUnavailable(err error) bool {
	return errors.Is(err, port.ErrPredictionSourceUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

// prepareAccountInputs loads and validates each routed strategy account on its
// own. A failure excludes only that account; an ended cycle context fails the
// cycle, because cancellation is not bad account data.
func (service *Service) prepareAccountInputs(
	ctx context.Context,
	decisionAt time.Time,
	routedAccounts map[string]struct{},
	selectedPredictions []domain.Prediction,
) (map[string]*accountInput, error) {
	inputs := make(map[string]*accountInput, len(service.strategyBindings))
	for _, binding := range service.strategyBindings {
		if _, routed := routedAccounts[binding.ExecutionAccountID]; !routed {
			continue
		}
		input := &accountInput{predictions: make([]domain.Prediction, 0)}
		if !service.accountEntryDisabled(binding) {
			input.predictions = predictionsForBinding(selectedPredictions, binding)
		}
		inputs[binding.ExecutionAccountID] = input
		lots, err := service.loadAccountLots(ctx, decisionAt, binding)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, fmt.Errorf("load position lots for %s: %w", binding.ExecutionAccountID, errors.Join(err, ctxErr))
			}
			service.excludeAccount(binding, input, FailureStagePositions, err)
			continue
		}
		input.positions = lots
		targets, err := buildInputTargets(input.predictions, lots)
		if err != nil {
			service.excludeAccount(binding, input, FailureStageTargets, err)
			continue
		}
		input.targets = targets
	}
	return inputs, nil
}

func (service *Service) excludeAccount(
	binding domain.StrategyExecutionBinding, input *accountInput, stage string, err error,
) {
	input.failure = &accountFailure{stage: stage, err: err}
	service.log().Warn("decision cycle excluded account with unusable input",
		"execution_account_id", binding.ExecutionAccountID, "strategy_id", binding.StrategyID,
		"failure_stage", stage, "error", err)
}

// loadAccountLots loads the OPEN lots of one account that is sent to the
// strategy this cycle. A lot belongs to its account: the model and strategy
// that opened it are audit history, so a model or strategy change hands the
// account's lots to the strategy now running it. A lot must still match the
// account's venue. Any error belongs to this account only.
func (service *Service) loadAccountLots(
	ctx context.Context, decisionAt time.Time, binding domain.StrategyExecutionBinding,
) ([]domain.StrategyPositionLot, error) {
	lots, err := service.positionSource.ListOpenLots(ctx, binding.Context())
	if err != nil {
		return nil, fmt.Errorf("load position lots for %s: %w", binding.ExecutionAccountID, err)
	}
	kalshiAccount := binding.MarketSource == domain.MarketSourceKalshi
	strategyLots := make([]domain.StrategyPositionLot, 0, len(lots))
	seen := make(map[string]struct{}, len(lots))
	for _, lot := range lots {
		if lot.ExecutionAccountID != binding.ExecutionAccountID || lot.Status != domain.PositionLotOpen ||
			lot.OutcomeIndex == nil || lot.NegRisk == nil {
			return nil, fmt.Errorf("position lot %q does not belong to account %s", lot.LotID, binding.ExecutionAccountID)
		}
		if strings.HasPrefix(strings.TrimSpace(lot.TokenID), domain.KalshiExecutionAccountPrefix) != kalshiAccount {
			return nil, fmt.Errorf("position lot %q is not a %s lot of account %s",
				lot.LotID, binding.MarketSource, binding.ExecutionAccountID)
		}
		if _, exists := seen[lot.LotID]; exists {
			return nil, fmt.Errorf("duplicate open position lot %q", lot.LotID)
		}
		seen[lot.LotID] = struct{}{}
		// Polymarket lots keep the legacy empty market_source on the wire.
		marketSource := domain.MarketSource("")
		if kalshiAccount {
			marketSource = domain.MarketSourceKalshi
		}
		strategyLot, err := (domain.StrategyPositionLotParams{
			LotID: lot.LotID, MarketSource: marketSource, MarketID: lot.MarketID, ConditionID: lot.ConditionID,
			OutcomeIndex: *lot.OutcomeIndex, OutcomeName: lot.OutcomeName, TokenID: lot.TokenID,
			NegRisk:   *lot.NegRisk,
			EnteredAt: lot.OpenedAt.UTC(), Shares: lot.RemainingShares, EntryPrice: lot.AverageEntryPrice,
		}).Build(decisionAt)
		if err != nil {
			return nil, fmt.Errorf("position lot %q: %w", lot.LotID, err)
		}
		strategyLots = append(strategyLots, strategyLot)
	}
	return strategyLots, nil
}

// sharedTargets builds the one capture target set of the cycle: the
// predictions of every venue a healthy entry account trades, plus the targets
// of every healthy account. The prediction targets are validated as a whole,
// so a malformed snapshot still fails the cycle. A lot whose identity differs
// from the prediction of the same token excludes only that account; two lots
// with different identities for one token exclude every account holding it.
func (service *Service) sharedTargets(
	selectedPredictions []domain.Prediction,
	routedAccounts map[string]struct{},
	inputs map[string]*accountInput,
) ([]domain.BookTarget, error) {
	predictionTargets, err := buildInputTargets(
		predictionsForSources(selectedPredictions, service.entrySources(routedAccounts)), nil,
	)
	if err != nil {
		return nil, err
	}
	predictionByToken := make(map[string]domain.BookTarget, len(predictionTargets))
	for _, target := range predictionTargets {
		predictionByToken[target.TokenID] = target
	}
	healthy := func() []domain.StrategyExecutionBinding {
		result := make([]domain.StrategyExecutionBinding, 0, len(inputs))
		for _, binding := range service.strategyBindings {
			if input, exists := inputs[binding.ExecutionAccountID]; exists && input.failure == nil {
				result = append(result, binding)
			}
		}
		return result
	}
	for _, binding := range healthy() {
		input := inputs[binding.ExecutionAccountID]
		for _, target := range input.targets {
			if prediction, predicted := predictionByToken[target.TokenID]; predicted && prediction != target {
				service.excludeAccount(binding, input, FailureStageTargetConflict,
					fmt.Errorf("position token %q conflicts with the prediction market identity", target.TokenID))
				break
			}
		}
	}
	holders := make(map[string]map[domain.BookTarget][]string)
	for _, binding := range healthy() {
		for _, target := range inputs[binding.ExecutionAccountID].targets {
			if _, predicted := predictionByToken[target.TokenID]; predicted {
				continue
			}
			if holders[target.TokenID] == nil {
				holders[target.TokenID] = make(map[domain.BookTarget][]string)
			}
			holders[target.TokenID][target] = append(holders[target.TokenID][target], binding.ExecutionAccountID)
		}
	}
	conflictTokens := make([]string, 0)
	for token, identities := range holders {
		if len(identities) > 1 {
			conflictTokens = append(conflictTokens, token)
		}
	}
	sort.Strings(conflictTokens)
	for _, token := range conflictTokens {
		for _, binding := range healthy() {
			input := inputs[binding.ExecutionAccountID]
			for _, target := range input.targets {
				if target.TokenID == token {
					service.excludeAccount(binding, input, FailureStageTargetConflict,
						fmt.Errorf("position token %q has conflicting market identity across accounts", token))
					break
				}
			}
		}
	}
	accounts := healthy()
	// Same rule as entrySources, restricted to healthy accounts.
	entryVenues := make(map[domain.MarketSource]struct{}, 2)
	for _, binding := range service.strategyEntryEnabledBindings {
		if input, exists := inputs[binding.ExecutionAccountID]; exists && input.failure == nil {
			entryVenues[binding.MarketSource.Normalize()] = struct{}{}
		}
	}
	byToken := make(map[string]domain.BookTarget, len(predictionTargets))
	for _, target := range predictionTargets {
		// Predictions of a venue no healthy account can buy on are not captured.
		if _, wanted := entryVenues[target.MarketSource.Normalize()]; wanted {
			byToken[target.TokenID] = target
		}
	}
	for _, binding := range accounts {
		for _, target := range inputs[binding.ExecutionAccountID].targets {
			byToken[target.TokenID] = target
		}
	}
	targets := make([]domain.BookTarget, 0, len(byToken))
	for _, target := range byToken {
		targets = append(targets, target)
	}
	sortBookTargets(targets)
	return targets, nil
}

// degradedOrderBookVenues returns the venues whose whole capture failed.
func degradedOrderBookVenues(books []domain.OrderBookSnapshot) map[domain.MarketSource]struct{} {
	venues := make(map[domain.MarketSource]struct{})
	for _, book := range books {
		if book.Status == domain.OrderBookStatusError && book.ErrorCode == domain.OrderBookErrorVenueCaptureFailed {
			venues[book.MarketSource.Normalize()] = struct{}{}
		}
	}
	return venues
}

func sortedMarketSources(sources map[domain.MarketSource]struct{}) []domain.MarketSource {
	result := make([]domain.MarketSource, 0, len(sources))
	for source := range sources {
		result = append(result, source)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// withOptionalTimeout derives a per-call budget. It is never longer than the
// parent's remaining time; a non-positive budget only inherits the parent.
func withOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func (service *Service) log() *slog.Logger {
	if service.logger == nil {
		return slog.Default()
	}
	return service.logger
}

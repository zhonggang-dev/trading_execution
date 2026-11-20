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
// account keeps its failure and is never sent to the strategy. targets are
// exactly the books this account captures before its strategy call: its
// predictions (after the leader gate) plus its lots.
type accountInput struct {
	predictions []domain.Prediction
	positions   []domain.StrategyPositionLot
	targets     []domain.BookTarget
	// leaderGated counts the predictions held back because the group leader's
	// prediction of the same Sandbox has not arrived yet.
	leaderGated int
	failure     *accountFailure
}

type accountFailure struct {
	stage string
	err   error
}

// cyclePredictions is the prediction input of a cycle. unavailable is set
// (and selected is empty) when the prediction source was unavailable. fresh
// holds every fresh prediction of the configured models before the latest
// revision per (market, model, run) is chosen; the leader gate uses it to see
// whether a leader's prediction of a Sandbox has arrived at all.
type cyclePredictions struct {
	snapshotID  string
	selected    []domain.Prediction
	fresh       []domain.Prediction
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
	fresh, err := filterAvailablePredictions(
		snapshot.Predictions, service.predictionModels,
		service.predictionSourceModes, decisionAt.Add(-service.predictionLookback),
	)
	if err != nil {
		return cyclePredictions{}, err
	}
	selected, err := domain.SelectEffectivePredictions(fresh, service.predictionModels)
	if err != nil {
		return cyclePredictions{}, err
	}
	return cyclePredictions{snapshotID: snapshot.SnapshotID, selected: selected, fresh: fresh}, nil
}

func predictionSourceUnavailable(err error) bool {
	return errors.Is(err, port.ErrPredictionSourceUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

// prepareAccountInputs loads and validates each routed strategy account on its
// own. A failure excludes only that account; an ended cycle context fails the
// cycle, because cancellation is not bad account data. A follower's BUY
// predictions pass the leader gate here, before its book targets are built.
func (service *Service) prepareAccountInputs(
	ctx context.Context,
	decisionAt time.Time,
	routedAccounts map[string]struct{},
	predictions cyclePredictions,
) (map[string]*accountInput, error) {
	inputs := make(map[string]*accountInput, len(service.strategyBindings))
	leaderKeys := make(map[string]map[string]struct{})
	for _, binding := range service.strategyBindings {
		if _, routed := routedAccounts[binding.ExecutionAccountID]; !routed {
			continue
		}
		input := &accountInput{predictions: make([]domain.Prediction, 0)}
		if !service.accountEntryDisabled(binding) {
			input.predictions = predictionsForBinding(predictions.selected, binding)
		}
		if leader, follower := service.leaders[binding.ExecutionAccountID]; follower && len(input.predictions) > 0 {
			keys, cached := leaderKeys[leader.ExecutionAccountID]
			if !cached {
				keys = leaderPredictionKeys(predictions.fresh, leader)
				leaderKeys[leader.ExecutionAccountID] = keys
			}
			input.predictions, input.leaderGated = gateFollowerPredictions(input.predictions, keys)
			if input.leaderGated > 0 {
				service.log().Info("leader_gate_held",
					"decision_at", decisionAt, "execution_account_id", binding.ExecutionAccountID,
					"leader_execution_account_id", leader.ExecutionAccountID, "strategy_id", binding.StrategyID,
					"held", input.leaderGated, "kept", len(input.predictions))
			}
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

// excludeTargetConflicts checks token identities across accounts before any
// account captures its books. The prediction targets of every venue a routed
// entry account trades are validated as a whole, so a malformed snapshot
// still fails the cycle. A lot whose identity differs from the prediction of
// the same token excludes only that account; two lots with different
// identities for one token exclude every account holding it.
func (service *Service) excludeTargetConflicts(
	selectedPredictions []domain.Prediction,
	routedAccounts map[string]struct{},
	inputs map[string]*accountInput,
) error {
	predictionTargets, err := buildInputTargets(
		predictionsForSources(selectedPredictions, service.entrySources(routedAccounts)), nil,
	)
	if err != nil {
		return err
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
	return nil
}

// venueCaptureFailed reports whether the account's venue capture failed as a
// whole (error or its own budget expired) this cycle.
func venueCaptureFailed(books []domain.OrderBookSnapshot) bool {
	for _, book := range books {
		if book.Status == domain.OrderBookStatusError && book.ErrorCode == domain.OrderBookErrorVenueCaptureFailed {
			return true
		}
	}
	return false
}

// leaderPredictionKeys collects the Sandboxes (DIRECT: market and as-of) for
// which the leader's own prediction (model, run and venue) is present among
// the cycle's fresh predictions. Whether the leader may buy, or is sent to the
// strategy at all, does not matter: only the arrival of its prediction does.
func leaderPredictionKeys(fresh []domain.Prediction, leader domain.StrategyExecutionBinding) map[string]struct{} {
	keys := make(map[string]struct{})
	venue := leader.MarketSource.Normalize()
	for _, prediction := range fresh {
		if prediction.MarketSource.Normalize() == venue && leader.SelectsPrediction(prediction) {
			keys[leaderGateKey(prediction)] = struct{}{}
		}
	}
	return keys
}

// gateFollowerPredictions keeps the follower predictions whose Sandbox the
// leader has already predicted and returns how many were held back. It never
// mutates the caller's slice.
func gateFollowerPredictions(predictions []domain.Prediction, leaderKeys map[string]struct{}) ([]domain.Prediction, int) {
	kept := make([]domain.Prediction, 0, len(predictions))
	for _, prediction := range predictions {
		if _, arrived := leaderKeys[leaderGateKey(prediction)]; arrived {
			kept = append(kept, prediction)
		}
	}
	return kept, len(predictions) - len(kept)
}

// leaderGateKey identifies one prediction target across models and runs: the
// Sandbox, or for a DIRECT prediction without a Sandbox the market and its
// prediction_as_of.
func leaderGateKey(prediction domain.Prediction) string {
	if sandboxID := strings.TrimSpace(prediction.SandboxID); sandboxID != "" {
		return "sandbox\x00" + sandboxID + "\x00" + strings.TrimSpace(prediction.MarketID)
	}
	return "direct\x00" + strings.TrimSpace(prediction.MarketID) + "\x00" +
		prediction.PredictionAsOf.UTC().Format(time.RFC3339Nano)
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

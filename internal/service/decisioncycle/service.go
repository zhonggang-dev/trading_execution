package decisioncycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	decisionInterval        = 10 * time.Minute
	defaultDeliveryBatch    = 100
	defaultDeliveryStaleAge = 9 * time.Minute
)

var (
	ErrInvalidBoundary = errors.New("decision_at is not an exact 10-minute UTC boundary")
	ErrInvalidStrategy = errors.New("strategy response is invalid")
)

// IntentSubmissionPolicy is a fail-closed venue/binding allowlist. It can
// narrow durable delivery, but it does not override account entry gates.
type IntentSubmissionPolicy interface {
	Enabled(domain.OrderIntent) bool
}

// Params 表示后端使用的 Params 类型。
type Params struct {
	PredictionSource port.PredictionSource
	PositionSource   port.StrategyPositionSource
	OrderBookSource  port.OrderBookSource
	Strategy         port.StrategyClient
	Recorder         port.DecisionRecorder
	Executor         port.OrderExecutor
	SubmissionPolicy IntentSubmissionPolicy
	// SubmitEnabled is an independent, fail-closed gate. A disabled cycle still
	// records the validated strategy output but never invokes execution.Submit.
	SubmitEnabled bool
	// SubmissionDisabledAccounts keeps selected bindings in the configured
	// topology while excluding them from automatic data capture, strategy calls,
	// durable delivery, and recovery.
	SubmissionDisabledAccounts []string
	// EntrySubmissionDisabled keeps validated exits executable while
	// suppressing every BUY intent.
	EntrySubmissionDisabled bool
	// EntryDisabledAccounts keeps selected active bindings sell-only while the
	// remaining bindings may emit and deliver BUY intents.
	EntryDisabledAccounts []string
	// RequireCompleteModelCoverage keeps live BUY submission fail closed for a
	// binding that has no fresh probability. Coverage is evaluated independently
	// per configured source model; a Market is never required to have results
	// from any other model.
	RequireCompleteModelCoverage bool
	Bindings                     []domain.StrategyExecutionBinding
	PredictionSourceModes        map[string]domain.PredictionSourceMode
	Venue                        string
	PredictionLookback           time.Duration
	DeliveryStaleAge             time.Duration
	Now                          func() time.Time
}

// Service 表示后端使用的 Service 类型。
type Service struct {
	predictionSource                 port.PredictionSource
	positionSource                   port.StrategyPositionSource
	orderBookSource                  port.OrderBookSource
	strategy                         port.StrategyClient
	recorder                         port.DecisionRecorder
	executor                         port.OrderExecutor
	submissionPolicy                 IntentSubmissionPolicy
	submitEnabled                    bool
	submissionDisabledAccounts       []string
	submissionDisabledAccountSet     map[string]struct{}
	entrySubmissionDisabled          bool
	entryDisabledAccounts            []string
	entryDisabledAccountSet          map[string]struct{}
	requireCompleteModelCoverage     bool
	bindings                         []domain.StrategyExecutionBinding
	predictionSourceModes            map[string]domain.PredictionSourceMode
	activeBindings                   []domain.StrategyExecutionBinding
	entryEnabledBindings             []domain.StrategyExecutionBinding
	activeExecutionAccountIDs        []string
	entryEnabledExecutionAccountIDs  []string
	entryDisabledExecutionAccountIDs []string
	venue                            string
	predictionLookback               time.Duration
	deliveryStaleAge                 time.Duration
	now                              func() time.Time
}

// IntentResult 表示后端使用的 IntentResult 类型。
type IntentResult struct {
	Intent             domain.OrderIntent
	Result             port.OrderSubmitResult
	DeliveryStatus     domain.DecisionIntentDeliveryStatus
	DeliveryAttempt    int
	SubmissionDisabled bool
	Error              error
}

// BindingRunResult 表示后端使用的 BindingRunResult 类型。
type BindingRunResult struct {
	Context                   domain.StrategyExecutionContext
	PredictionModelID         string
	PredictionCount           int
	PositionCount             int
	OrderSubmissionEnabled    bool
	AccountSubmissionDisabled bool
	EntrySubmissionEnabled    bool
	EntryBlockReason          string
	Request                   domain.StrategyDecisionRequest
	Response                  domain.StrategyDecisionResponse
	Intents                   []IntentResult
	Error                     error
}

// RunResult 表示后端使用的 RunResult 类型。
type RunResult struct {
	DecisionAt           time.Time
	PredictionSnapshotID string
	Runs                 []BindingRunResult
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.PredictionSource == nil || params.PositionSource == nil || params.OrderBookSource == nil || params.Strategy == nil ||
		params.Recorder == nil {
		return nil, fmt.Errorf("prediction, position, orderbook, strategy, and recorder dependencies are required")
	}
	if params.SubmitEnabled && params.Executor == nil {
		return nil, fmt.Errorf("order executor is required when decision-cycle submission is enabled")
	}
	if params.SubmitEnabled && !params.RequireCompleteModelCoverage {
		return nil, fmt.Errorf("complete prediction-model coverage is required when decision-cycle submission is enabled")
	}
	params.Venue = strings.ToLower(strings.TrimSpace(params.Venue))
	bindings, err := normalizeBindings(params.Bindings)
	if err != nil {
		return nil, err
	}
	predictionSourceModes, err := normalizePredictionSourceModes(bindings, params.PredictionSourceModes)
	if err != nil {
		return nil, err
	}
	disabledAccounts, disabledAccountSet, err := normalizeSubmissionDisabledAccounts(
		bindings,
		params.SubmissionDisabledAccounts,
	)
	if err != nil {
		return nil, err
	}
	entryDisabledAccounts, entryDisabledAccountSet, err := normalizeEntryDisabledAccounts(
		bindings,
		params.EntryDisabledAccounts,
	)
	if err != nil {
		return nil, err
	}
	activeBindings := filterSubmissionEnabledBindings(bindings, disabledAccountSet)
	if len(activeBindings) == 0 {
		return nil, fmt.Errorf("submission-disabled accounts cannot exclude every decision binding")
	}
	entryEnabledBindings := filterSubmissionEnabledBindings(activeBindings, entryDisabledAccountSet)
	entryDisabledBindings := filterEntryDisabledBindings(activeBindings, entryDisabledAccountSet)
	if len(entryEnabledBindings)+len(entryDisabledBindings) != len(activeBindings) {
		return nil, fmt.Errorf("entry-disabled accounts do not partition every active decision binding")
	}
	if params.Venue == "" {
		return nil, fmt.Errorf("execution venue is required")
	}
	if params.PredictionLookback == 0 {
		params.PredictionLookback = 3 * time.Hour
	}
	if params.PredictionLookback < decisionInterval {
		return nil, fmt.Errorf("prediction lookback must be at least %s", decisionInterval)
	}
	if params.DeliveryStaleAge == 0 {
		params.DeliveryStaleAge = defaultDeliveryStaleAge
	}
	if params.DeliveryStaleAge <= 0 || params.DeliveryStaleAge > 24*time.Hour {
		return nil, fmt.Errorf("decision intent delivery stale age must be positive and at most 24h")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{
		predictionSource:                 params.PredictionSource,
		positionSource:                   params.PositionSource,
		orderBookSource:                  params.OrderBookSource,
		strategy:                         params.Strategy,
		recorder:                         params.Recorder,
		executor:                         params.Executor,
		submissionPolicy:                 params.SubmissionPolicy,
		submitEnabled:                    params.SubmitEnabled,
		submissionDisabledAccounts:       disabledAccounts,
		submissionDisabledAccountSet:     disabledAccountSet,
		entrySubmissionDisabled:          params.EntrySubmissionDisabled,
		entryDisabledAccounts:            entryDisabledAccounts,
		entryDisabledAccountSet:          entryDisabledAccountSet,
		requireCompleteModelCoverage:     params.RequireCompleteModelCoverage,
		bindings:                         bindings,
		predictionSourceModes:            predictionSourceModes,
		activeBindings:                   activeBindings,
		entryEnabledBindings:             entryEnabledBindings,
		activeExecutionAccountIDs:        executionAccountIDs(activeBindings),
		entryEnabledExecutionAccountIDs:  executionAccountIDs(entryEnabledBindings),
		entryDisabledExecutionAccountIDs: executionAccountIDs(entryDisabledBindings),
		venue:                            params.Venue,
		predictionLookback:               params.PredictionLookback,
		deliveryStaleAge:                 params.DeliveryStaleAge,
		now:                              params.Now,
	}, nil
}

// Run 采集一次冻结决策快照，并只为未隔离的绑定调用策略和提交订单。
func (service *Service) Run(ctx context.Context, decisionAt time.Time) (RunResult, error) {
	decisionAt = decisionAt.UTC()
	if decisionAt.IsZero() || decisionAt.Unix()%int64(decisionInterval/time.Second) != 0 {
		return RunResult{}, ErrInvalidBoundary
	}
	if err := service.RecoverPending(ctx); err != nil {
		return RunResult{}, fmt.Errorf("recover pending strategy intents: %w", err)
	}
	snapshot, err := service.predictionSource.Snapshot(ctx, decisionAt, service.predictionLookback)
	if err != nil {
		return RunResult{}, fmt.Errorf("load prediction snapshot: %w", err)
	}
	if err := snapshot.Validate(decisionAt); err != nil {
		return RunResult{}, fmt.Errorf("validate prediction snapshot: %w", err)
	}
	modelIDs := configuredPredictionModels(service.entryEnabledBindings)
	selectedPredictions, err := selectAvailablePredictions(
		snapshot.Predictions, modelIDs, service.predictionSourceModes,
		decisionAt.Add(-service.predictionLookback),
	)
	if err != nil {
		return RunResult{}, err
	}
	positionLots, err := service.loadPositionLots(ctx, decisionAt)
	if err != nil {
		return RunResult{}, err
	}
	targets, err := buildInputTargets(selectedPredictions, flattenPositionLots(positionLots))
	if err != nil {
		return RunResult{}, err
	}
	books, err := service.orderBookSource.Capture(ctx, decisionAt, targets)
	if err != nil {
		return RunResult{}, wrapError("capture orderbooks", err)
	}
	books, err = alignBooks(targets, books, service.now().UTC())
	if err != nil {
		return RunResult{}, err
	}
	result := RunResult{
		DecisionAt:           decisionAt,
		PredictionSnapshotID: snapshot.SnapshotID,
		Runs:                 make([]BindingRunResult, 0, len(service.bindings)),
	}
	runErrors := make([]error, 0)
	for _, binding := range service.bindings {
		executionContext := binding.Context()
		_, bindingSubmissionDisabled := service.submissionDisabledAccountSet[binding.ExecutionAccountID]
		if bindingSubmissionDisabled {
			result.Runs = append(result.Runs, BindingRunResult{
				Context: executionContext, PredictionModelID: binding.PredictionModelID,
				PredictionCount:        len(predictionsForBinding(selectedPredictions, binding)),
				OrderSubmissionEnabled: false, AccountSubmissionDisabled: true,
			})
			continue
		}
		bindingSubmissionEnabled := service.submitEnabled
		_, accountEntryDisabled := service.entryDisabledAccountSet[binding.ExecutionAccountID]
		accountEntryDisabled = accountEntryDisabled || service.entrySubmissionDisabled
		predictions := make([]domain.Prediction, 0)
		if !accountEntryDisabled {
			predictions = predictionsForBinding(selectedPredictions, binding)
		}
		var bindingCoverageErr error
		if service.requireCompleteModelCoverage && len(predictions) == 0 {
			bindingCoverageErr = fmt.Errorf(
				"prediction snapshot has no fresh result for configured model %q", binding.PredictionModelID,
			)
		}
		positions := positionLots[binding.ExecutionAccountID]
		bindingTargets, selectErr := buildInputTargets(predictions, positions)
		if selectErr != nil {
			run := BindingRunResult{
				Context: executionContext, PredictionModelID: binding.PredictionModelID,
				PredictionCount: len(predictions), PositionCount: len(positions),
				OrderSubmissionEnabled: bindingSubmissionEnabled, Error: selectErr,
			}
			result.Runs = append(result.Runs, run)
			runErrors = append(runErrors, fmt.Errorf("run %s/%s: %w", binding.ModelID, binding.StrategyID, selectErr))
			continue
		}
		bindingBooks, selectErr := booksForTargets(bindingTargets, books)
		if selectErr != nil {
			run := BindingRunResult{
				Context: executionContext, PredictionModelID: binding.PredictionModelID,
				PredictionCount: len(predictions), PositionCount: len(positions),
				OrderSubmissionEnabled: bindingSubmissionEnabled, Error: selectErr,
			}
			result.Runs = append(result.Runs, run)
			runErrors = append(runErrors, fmt.Errorf("run %s/%s: %w", binding.ModelID, binding.StrategyID, selectErr))
			continue
		}
		run, runErr := service.runBinding(ctx, runBindingParams{
			decisionAt: decisionAt, predictionSnapshotID: snapshot.SnapshotID, binding: executionContext,
			predictions: predictions, positions: positions, books: bindingBooks,
			allowEntries:     bindingCoverageErr == nil && !accountEntryDisabled,
			entryBlockReason: entryBlockReason(bindingCoverageErr, accountEntryDisabled),
			submitEnabled:    bindingSubmissionEnabled,
		})
		run.PredictionModelID = binding.PredictionModelID
		run.Error = runErr
		result.Runs = append(result.Runs, run)
		if runErr != nil {
			runErrors = append(runErrors, fmt.Errorf("run %s/%s: %w", binding.ModelID, binding.StrategyID, runErr))
		}
	}
	return result, errors.Join(runErrors...)
}

// loadPositionLots 加载 Position Lots。
func (service *Service) loadPositionLots(ctx context.Context, decisionAt time.Time) (map[string][]domain.StrategyPositionLot, error) {
	result := make(map[string][]domain.StrategyPositionLot, len(service.activeBindings))
	for _, binding := range service.activeBindings {
		lots, err := service.positionSource.ListOpenLots(ctx, binding.ExecutionAccountID)
		if err != nil {
			return nil, fmt.Errorf("load position lots for %s: %w", binding.ExecutionAccountID, err)
		}
		strategyLots := make([]domain.StrategyPositionLot, 0, len(lots))
		seen := make(map[string]struct{}, len(lots))
		for _, lot := range lots {
			if lot.ExecutionAccountID != binding.ExecutionAccountID || domain.CanonicalStrategyID(lot.StrategyID) != binding.StrategyID ||
				strings.TrimSpace(lot.ModelID) != binding.ModelID || lot.Status != domain.PositionLotOpen || lot.OutcomeIndex == nil || lot.NegRisk == nil {
				return nil, fmt.Errorf("position lot %q does not belong to binding %s/%s/%s", lot.LotID, binding.ModelID, binding.StrategyID, binding.ExecutionAccountID)
			}
			if _, exists := seen[lot.LotID]; exists {
				return nil, fmt.Errorf("duplicate open position lot %q", lot.LotID)
			}
			seen[lot.LotID] = struct{}{}
			if lot.HasDustRemainder() {
				// A remainder below the SELL size precision cannot be exited by
				// any strategy order. It stays in the ledger and leaves through
				// settlement redemption, so the strategy must not see it as a
				// sellable lot.
				continue
			}
			strategyLot, err := (domain.StrategyPositionLotParams{
				LotID: lot.LotID, MarketSource: lot.MarketSource, MarketID: lot.MarketID, ConditionID: lot.ConditionID,
				OutcomeIndex: *lot.OutcomeIndex, OutcomeName: lot.OutcomeName, TokenID: lot.TokenID,
				NegRisk:   *lot.NegRisk,
				EnteredAt: lot.OpenedAt.UTC(), Shares: lot.RemainingShares, EntryPrice: lot.AverageEntryPrice,
			}).Build(decisionAt)
			if err != nil {
				return nil, fmt.Errorf("position lot %q: %w", lot.LotID, err)
			}
			strategyLots = append(strategyLots, strategyLot)
		}
		result[binding.ExecutionAccountID] = strategyLots
	}
	return result, nil
}

// flattenPositionLots 展平 Position Lots。
func flattenPositionLots(byAccount map[string][]domain.StrategyPositionLot) []domain.StrategyPositionLot {
	var result []domain.StrategyPositionLot
	for _, lots := range byAccount {
		result = append(result, lots...)
	}
	return result
}

// wrapError 为 Error 包装操作上下文。
func wrapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func entryBlockReason(coverageErr error, entrySubmissionDisabled bool) string {
	if entrySubmissionDisabled {
		return domain.StrategyEntryBlockSubmissionDisabled
	}
	if coverageErr == nil {
		return ""
	}
	return domain.StrategyEntryBlockIncompleteModelCoverage
}

// runBindingParams 收拢单个模型策略绑定执行一次决策所需的冻结输入。
type runBindingParams struct {
	decisionAt           time.Time
	predictionSnapshotID string
	binding              domain.StrategyExecutionContext
	predictions          []domain.Prediction
	positions            []domain.StrategyPositionLot
	books                []domain.OrderBookSnapshot
	allowEntries         bool
	entryBlockReason     string
	submitEnabled        bool
}

// runBinding 为单个绑定冻结输入、调用策略、持久化输出并提交订单意图。
func (service *Service) runBinding(ctx context.Context, params runBindingParams) (BindingRunResult, error) {
	request, err := (domain.StrategyDecisionRequestParams{
		CycleID:              cycleID(params.binding, params.decisionAt),
		Context:              params.binding,
		DecisionAt:           params.decisionAt,
		GeneratedAt:          service.now().UTC(),
		PredictionSnapshotID: params.predictionSnapshotID,
		Predictions:          params.predictions,
		Positions:            params.positions,
		OrderBooks:           params.books,
	}).Build()
	run := BindingRunResult{
		Context: params.binding, PredictionCount: len(params.predictions), PositionCount: len(params.positions),
		OrderSubmissionEnabled: params.submitEnabled,
		EntrySubmissionEnabled: params.allowEntries, EntryBlockReason: params.entryBlockReason, Request: request,
	}
	if err != nil {
		return run, err
	}
	proposedCycleID := request.CycleID
	request, _, err = service.recorder.ClaimInput(ctx, request)
	run.Request = request
	if err != nil {
		return run, fmt.Errorf("record strategy input: %w", err)
	}
	if err := validateClaimedInput(request, proposedCycleID, params.binding, params.decisionAt); err != nil {
		return run, err
	}
	response, err := service.strategy.Decide(ctx, request)
	if err != nil {
		run.Response = response
		return run, fmt.Errorf("request strategy decision: %w", err)
	}
	// EntryPolicy is a Go-owned additive audit field. Keep it absent during a
	// normal cycle so replay of pre-upgrade v1 output stays byte-compatible;
	// overwrite any untrusted Python value in both modes.
	response.EntryPolicy = nil
	if !params.allowEntries {
		response.EntryPolicy = &domain.StrategyEntryPolicy{
			Enabled: false, BlockReason: params.entryBlockReason,
		}
	}
	run.Response = response
	if response.DecidedAt.After(service.now().UTC()) {
		return run, fmt.Errorf("%w: decided_at must not be in the future", ErrInvalidStrategy)
	}
	intents, err := validateResponseWithEntryPolicy(request, response, service.venue, params.allowEntries, params.entryBlockReason)
	if err != nil {
		return run, err
	}
	deliverableIntents, venueDryRunIntents := service.submissionIntents(intents)
	response, _, err = service.recorder.ClaimOutput(ctx, response, deliverableIntents, params.submitEnabled)
	run.Response = response
	if err != nil {
		return run, fmt.Errorf("record strategy output: %w", err)
	}
	intents, err = validateResponseWithEntryPolicy(request, response, service.venue, params.allowEntries, params.entryBlockReason)
	if err != nil {
		return run, fmt.Errorf("validate recorded strategy output: %w", err)
	}
	_, venueDryRunIntents = service.submissionIntents(intents)
	if !params.submitEnabled {
		run.Intents = make([]IntentResult, 0, len(intents))
		for _, intent := range intents {
			run.Intents = append(run.Intents, IntentResult{Intent: intent, SubmissionDisabled: true})
		}
		return run, nil
	}
	delivered, deliveryErr := service.deliverPending(ctx, response.CycleID)
	deliveryByID := make(map[string]IntentResult, len(delivered))
	for _, result := range delivered {
		deliveryByID[result.Intent.ClientOrderID] = result
	}
	storedDeliveries, listErr := service.recorder.ListIntents(ctx, response.CycleID)
	if listErr != nil {
		return run, errors.Join(deliveryErr, fmt.Errorf("list strategy intent deliveries: %w", listErr))
	}
	run.Intents = make([]IntentResult, 0, len(storedDeliveries)+len(venueDryRunIntents))
	for _, intent := range venueDryRunIntents {
		run.Intents = append(run.Intents, IntentResult{Intent: intent, SubmissionDisabled: true})
	}
	for _, delivery := range storedDeliveries {
		if result, exists := deliveryByID[delivery.ClientOrderID]; exists {
			run.Intents = append(run.Intents, result)
			continue
		}
		run.Intents = append(run.Intents, IntentResult{
			Intent: delivery.Intent, DeliveryStatus: delivery.Status, DeliveryAttempt: delivery.Attempt,
			Result: port.OrderSubmitResult{Order: domain.Order{ID: delivery.OrderID, Status: delivery.OrderStatus}},
		})
	}
	return run, deliveryErr
}

// submissionIntents keeps the current Polymarket execution/reconciliation
// pipeline isolated from Kalshi. Kalshi intents are fully validated and
// exposed as dry-run results, but are not inserted into the durable delivery
// queue until a venue-specific executor, fills, positions, and reconciliation
// path are composed.
func (service *Service) submissionIntents(intents []domain.OrderIntent) (deliverable, venueDryRun []domain.OrderIntent) {
	for _, intent := range intents {
		enabled := intent.MarketSource.Normalize() == domain.MarketSourcePolymarket
		if service.submissionPolicy != nil {
			enabled = service.submissionPolicy.Enabled(intent)
		}
		if enabled {
			deliverable = append(deliverable, intent)
			continue
		}
		venueDryRun = append(venueDryRun, intent)
	}
	return deliverable, venueDryRun
}

// RecoverPending safely resumes durable strategy intents from an earlier
// process. It is a no-op while the independent submission gate is disabled.
// SUBMITTING claims are retried only after a bounded lease and always through
// execution.Submit's stable client_order_id lookup.
func (service *Service) RecoverPending(ctx context.Context) error {
	if !service.submitEnabled {
		return nil
	}
	if err := service.guardDisabledAccountRecovery(ctx); err != nil {
		return err
	}
	return service.recoverDeliveries(ctx, service.now().UTC().Add(-service.deliveryStaleAge))
}

// RecoverStartup takes ownership of every durable claim left by the previous
// process before a new schedule is published. A single systemd instance is the
// supported deployment model; future active-active operation must add an
// explicit database owner lease before using this eager startup takeover.
func (service *Service) RecoverStartup(ctx context.Context) error {
	if !service.submitEnabled {
		return nil
	}
	if err := service.guardDisabledAccountRecovery(ctx); err != nil {
		return err
	}
	return service.recoverDeliveries(ctx, service.now().UTC())
}

func (service *Service) guardDisabledAccountRecovery(ctx context.Context) error {
	if len(service.submissionDisabledAccounts) == 0 {
		return nil
	}
	count, err := service.recorder.CountUnresolvedIntentsForAccounts(ctx, service.submissionDisabledAccounts)
	if err != nil {
		return fmt.Errorf("check quarantined strategy intents: %w", err)
	}
	if count != 0 {
		return fmt.Errorf(
			"automatic submission is disabled for %d account(s), but %d unresolved intent(s) require operator disposition",
			len(service.submissionDisabledAccounts), count,
		)
	}
	return nil
}

func (service *Service) recoverDeliveries(ctx context.Context, cutoff time.Time) error {
	for _, cohort := range service.deliveryCohorts() {
		for {
			requeued, err := service.recorder.RequeueStaleSubmitting(
				ctx, cohort.executionAccountIDs, cutoff, cohort.side, defaultDeliveryBatch,
			)
			if err != nil {
				return fmt.Errorf("requeue stale strategy intents: %w", err)
			}
			if requeued < defaultDeliveryBatch {
				break
			}
		}
	}
	_, err := service.deliverPending(ctx, "")
	return err
}

func (service *Service) deliverPending(ctx context.Context, cycleID string) ([]IntentResult, error) {
	results := make([]IntentResult, 0)
	deliveryErrors := make([]error, 0)
	for _, cohort := range service.deliveryCohorts() {
		for {
			deliveries, err := service.recorder.ClaimPendingIntents(
				ctx, cohort.executionAccountIDs, cycleID, cohort.side, defaultDeliveryBatch,
			)
			if err != nil {
				return results, errors.Join(errors.Join(deliveryErrors...), fmt.Errorf("claim strategy intents: %w", err))
			}
			if len(deliveries) == 0 {
				break
			}
			for _, delivery := range deliveries {
				result := IntentResult{
					Intent: delivery.Intent, DeliveryStatus: delivery.Status, DeliveryAttempt: delivery.Attempt,
				}
				// A persisted delivery may predate a venue route entering
				// maintenance-only mode. Keep the lease durable without creating a
				// new execution order or failing process startup; a later healthy
				// restart requeues and resumes the exact frozen intent.
				if service.submissionPolicy != nil && !service.submissionPolicy.Enabled(delivery.Intent) {
					result.SubmissionDisabled = true
					results = append(results, result)
					continue
				}
				result.Result, result.Error = service.executor.Submit(ctx, delivery.Intent)
				completion, complete := decisionIntentCompletion(result.Result, result.Error)
				if complete {
					if completeErr := service.recorder.CompleteIntent(ctx, delivery.ClientOrderID, delivery.Attempt, completion); completeErr != nil {
						result.Error = errors.Join(result.Error, fmt.Errorf("complete durable strategy intent: %w", completeErr))
					} else {
						result.DeliveryStatus = completion.Status
						if completion.Status == domain.DecisionIntentFailed || completion.Status == domain.DecisionIntentUnknown {
							result.Error = errors.Join(result.Error, fmt.Errorf(
								"execution order %s completed durable delivery as %s (%s)",
								completion.OrderID, completion.Status, completion.OrderStatus,
							))
						}
					}
				} else if result.Error == nil {
					result.Error = fmt.Errorf("execution did not return a durable order id; delivery remains leased for recovery")
				}
				if result.Error != nil {
					deliveryErrors = append(deliveryErrors, fmt.Errorf("submit %s: %w", delivery.ClientOrderID, result.Error))
				}
				results = append(results, result)
			}
			if len(deliveries) < defaultDeliveryBatch {
				break
			}
		}
	}
	return results, errors.Join(deliveryErrors...)
}

type deliveryCohort struct {
	executionAccountIDs []string
	side                domain.Side
}

func (service *Service) deliveryCohorts() []deliveryCohort {
	if service.entrySubmissionDisabled {
		return []deliveryCohort{{executionAccountIDs: service.activeExecutionAccountIDs, side: domain.SideSell}}
	}
	result := make([]deliveryCohort, 0, 2)
	if len(service.entryEnabledExecutionAccountIDs) > 0 {
		result = append(result, deliveryCohort{executionAccountIDs: service.entryEnabledExecutionAccountIDs})
	}
	if len(service.entryDisabledExecutionAccountIDs) > 0 {
		result = append(result, deliveryCohort{
			executionAccountIDs: service.entryDisabledExecutionAccountIDs,
			side:                domain.SideSell,
		})
	}
	return result
}

// decisionIntentCompletion marks a delivery terminal only after execution has
// durably claimed an order_id. An infrastructure failure before that point is
// left SUBMITTING so its lease can expire and be retried without data loss.
func decisionIntentCompletion(result port.OrderSubmitResult, submitErr error) (domain.DecisionIntentCompletion, bool) {
	if strings.TrimSpace(result.Order.ID) == "" {
		return domain.DecisionIntentCompletion{}, false
	}
	completion := domain.DecisionIntentCompletion{
		OrderID: result.Order.ID, OrderStatus: result.Order.Status,
	}
	if submitErr != nil {
		completion.LastError = submitErr.Error()
	}
	switch result.Order.Status {
	case domain.OrderStatusRejected:
		completion.Status = domain.DecisionIntentFailed
	case domain.OrderStatusUnknown, domain.OrderStatusManualReview:
		completion.Status = domain.DecisionIntentUnknown
	default:
		completion.Status = domain.DecisionIntentSubmitted
	}
	return completion, true
}

// normalizeBindings 规范化 Bindings 的字段和表示。
func normalizeBindings(bindings []domain.StrategyExecutionBinding) ([]domain.StrategyExecutionBinding, error) {
	if len(bindings) == 0 {
		return nil, fmt.Errorf("at least one model/strategy/execution account binding is required")
	}
	result := make([]domain.StrategyExecutionBinding, len(bindings))
	seenPairs := make(map[string]struct{}, len(bindings))
	seenAccounts := make(map[string]struct{}, len(bindings))
	logicalModelSources := make(map[string]string, len(bindings))
	sourceModelTargets := make(map[string]string, len(bindings))
	for index, binding := range bindings {
		binding = binding.Normalize()
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("binding %d: %w", index, err)
		}
		pairKey := binding.ModelID + "\x00" + binding.StrategyID
		if _, exists := seenPairs[pairKey]; exists {
			return nil, fmt.Errorf("duplicate model/strategy binding %q/%q", binding.ModelID, binding.StrategyID)
		}
		if _, exists := seenAccounts[binding.ExecutionAccountID]; exists {
			return nil, fmt.Errorf("execution account %q is bound more than once", binding.ExecutionAccountID)
		}
		if sourceModelID, exists := logicalModelSources[binding.ModelID]; exists && sourceModelID != binding.PredictionModelID {
			return nil, fmt.Errorf("logical model %q is routed from multiple prediction models", binding.ModelID)
		}
		if logicalModelID, exists := sourceModelTargets[binding.PredictionModelID]; exists && logicalModelID != binding.ModelID {
			return nil, fmt.Errorf("prediction model %q is routed to multiple logical models", binding.PredictionModelID)
		}
		seenPairs[pairKey] = struct{}{}
		seenAccounts[binding.ExecutionAccountID] = struct{}{}
		logicalModelSources[binding.ModelID] = binding.PredictionModelID
		sourceModelTargets[binding.PredictionModelID] = binding.ModelID
		result[index] = binding
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ModelID != result[j].ModelID {
			return result[i].ModelID < result[j].ModelID
		}
		if result[i].StrategyID != result[j].StrategyID {
			return result[i].StrategyID < result[j].StrategyID
		}
		return result[i].ExecutionAccountID < result[j].ExecutionAccountID
	})
	return result, nil
}

func normalizeSubmissionDisabledAccounts(
	bindings []domain.StrategyExecutionBinding,
	accounts []string,
) ([]string, map[string]struct{}, error) {
	bound := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		bound[binding.ExecutionAccountID] = struct{}{}
	}
	result := make([]string, 0, len(accounts))
	seen := make(map[string]struct{}, len(accounts))
	for index, accountID := range accounts {
		accountID = strings.TrimSpace(accountID)
		if accountID == "" {
			return nil, nil, fmt.Errorf("submission-disabled account %d is empty", index)
		}
		if _, duplicate := seen[accountID]; duplicate {
			return nil, nil, fmt.Errorf("duplicate submission-disabled account %q", accountID)
		}
		if _, exists := bound[accountID]; !exists {
			return nil, nil, fmt.Errorf("submission-disabled account %q is not a configured binding", accountID)
		}
		seen[accountID] = struct{}{}
		result = append(result, accountID)
	}
	sort.Strings(result)
	return result, seen, nil
}

func normalizeEntryDisabledAccounts(
	bindings []domain.StrategyExecutionBinding,
	accounts []string,
) ([]string, map[string]struct{}, error) {
	bound := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		bound[binding.ExecutionAccountID] = struct{}{}
	}
	result := make([]string, 0, len(accounts))
	seen := make(map[string]struct{}, len(accounts))
	for index, rawAccountID := range accounts {
		accountID := strings.TrimSpace(rawAccountID)
		if accountID == "" {
			return nil, nil, fmt.Errorf("entry-disabled account %d is empty", index)
		}
		if _, duplicate := seen[accountID]; duplicate {
			return nil, nil, fmt.Errorf("duplicate entry-disabled account %q", accountID)
		}
		if _, exists := bound[accountID]; !exists {
			return nil, nil, fmt.Errorf("entry-disabled account %q is not a configured binding", accountID)
		}
		seen[accountID] = struct{}{}
		result = append(result, accountID)
	}
	sort.Strings(result)
	return result, seen, nil
}

func filterSubmissionEnabledBindings(
	bindings []domain.StrategyExecutionBinding,
	disabledAccounts map[string]struct{},
) []domain.StrategyExecutionBinding {
	result := make([]domain.StrategyExecutionBinding, 0, len(bindings))
	for _, binding := range bindings {
		if _, disabled := disabledAccounts[binding.ExecutionAccountID]; disabled {
			continue
		}
		result = append(result, binding)
	}
	return result
}

func filterEntryDisabledBindings(
	bindings []domain.StrategyExecutionBinding,
	disabledAccounts map[string]struct{},
) []domain.StrategyExecutionBinding {
	result := make([]domain.StrategyExecutionBinding, 0, len(disabledAccounts))
	for _, binding := range bindings {
		if _, disabled := disabledAccounts[binding.ExecutionAccountID]; disabled {
			result = append(result, binding)
		}
	}
	return result
}

func executionAccountIDs(bindings []domain.StrategyExecutionBinding) []string {
	result := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		result = append(result, binding.ExecutionAccountID)
	}
	sort.Strings(result)
	return result
}

// configuredPredictionModels 返回去重且稳定排序的上游模型身份。
func configuredPredictionModels(bindings []domain.StrategyExecutionBinding) []string {
	models := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		models[binding.PredictionModelID] = struct{}{}
	}
	result := make([]string, 0, len(models))
	for modelID := range models {
		result = append(result, modelID)
	}
	sort.Strings(result)
	return result
}

// selectAvailablePredictions first enforces each model's configured result
// channel, then applies freshness and deterministic PIT selection. A result
// from the other channel can never shadow or satisfy the configured source.
func selectAvailablePredictions(
	predictions []domain.Prediction,
	modelIDs []string,
	sourceModes map[string]domain.PredictionSourceMode,
	freshAfter time.Time,
) ([]domain.Prediction, error) {
	configuredModes := make(map[string]domain.PredictionSourceMode, len(modelIDs))
	for _, rawModelID := range modelIDs {
		modelID := strings.TrimSpace(rawModelID)
		mode, exists := sourceModes[modelID]
		if !exists {
			return nil, fmt.Errorf("prediction source mode is missing for configured model %q", modelID)
		}
		if !mode.Valid() {
			return nil, fmt.Errorf("prediction source mode for configured model %q is unknown: %q", modelID, mode)
		}
		configuredModes[modelID] = mode
	}
	fresh := make([]domain.Prediction, 0, len(predictions))
	for _, prediction := range predictions {
		mode, configured := configuredModes[strings.TrimSpace(prediction.Model.Name)]
		if !configured {
			continue
		}
		hasSandboxID := strings.TrimSpace(prediction.SandboxID) != ""
		if (mode == domain.PredictionSourceModeDirect && hasSandboxID) ||
			(mode == domain.PredictionSourceModeSandbox && !hasSandboxID) {
			continue
		}
		if prediction.PredictionAsOf.Before(freshAfter) || prediction.CompletedAt.Before(freshAfter) {
			continue
		}
		fresh = append(fresh, prediction)
	}
	return domain.SelectEffectivePredictions(fresh, modelIDs)
}

func normalizePredictionSourceModes(
	bindings []domain.StrategyExecutionBinding,
	modes map[string]domain.PredictionSourceMode,
) (map[string]domain.PredictionSourceMode, error) {
	configured := configuredPredictionModels(bindings)
	result := make(map[string]domain.PredictionSourceMode, len(configured))
	for _, modelID := range configured {
		mode, exists := modes[modelID]
		if !exists {
			return nil, fmt.Errorf("prediction source mode is missing for configured model %q", modelID)
		}
		if !mode.Valid() {
			return nil, fmt.Errorf("prediction source mode for configured model %q is unknown: %q", modelID, mode)
		}
		result[modelID] = mode
	}
	for modelID := range modes {
		if _, exists := result[modelID]; !exists {
			return nil, fmt.Errorf("prediction source mode is configured for unknown model %q", modelID)
		}
	}
	return result, nil
}

// predictionsForBinding 选择一个上游预测模型的 Market，再投影为 Python 和执行层使用的稳定业务模型。
// 只修改副本，原始 PIT snapshot 始终保留上游 producer identity。
func predictionsForBinding(predictions []domain.Prediction, binding domain.StrategyExecutionBinding) []domain.Prediction {
	selected := predictionsForModel(predictions, binding.PredictionModelID)
	result := make([]domain.Prediction, len(selected))
	for index, prediction := range selected {
		result[index] = prediction
		result[index].Model.Name = binding.ModelID
	}
	return result
}

// predictionsForModel 筛选属于指定模型的预测记录。
func predictionsForModel(predictions []domain.Prediction, modelID string) []domain.Prediction {
	result := make([]domain.Prediction, 0)
	for _, prediction := range predictions {
		if strings.TrimSpace(prediction.Model.Name) == modelID {
			result = append(result, prediction)
		}
	}
	return result
}

// booksForTargets 按目标顺序选取共享订单簿快照。
func booksForTargets(targets []domain.BookTarget, books []domain.OrderBookSnapshot) ([]domain.OrderBookSnapshot, error) {
	byToken := make(map[string]domain.OrderBookSnapshot, len(books))
	for _, book := range books {
		byToken[book.TokenID] = book
	}
	result := make([]domain.OrderBookSnapshot, 0, len(targets))
	for _, target := range targets {
		book, found := byToken[target.TokenID]
		if !found {
			return nil, fmt.Errorf("shared orderbook snapshot is missing token %q", target.TokenID)
		}
		result = append(result, book)
	}
	return result, nil
}

// buildBookTargets 根据预测结果构建去重的行情采集目标。
func buildBookTargets(predictions []domain.Prediction) ([]domain.BookTarget, error) {
	return buildInputTargets(predictions, nil)
}

// buildInputTargets 根据预测和持仓构建去重且身份一致的行情目标。
func buildInputTargets(predictions []domain.Prediction, positions []domain.StrategyPositionLot) ([]domain.BookTarget, error) {
	byToken := make(map[string]domain.BookTarget)
	for _, prediction := range predictions {
		for _, outcome := range prediction.Outcomes {
			target, err := (domain.BookTargetParams{
				MarketSource: prediction.MarketSource,
				MarketID:     prediction.MarketID,
				ConditionID:  prediction.ConditionID,
				OutcomeIndex: outcome.Index,
				TokenID:      outcome.TokenID,
				OutcomeID:    outcome.OutcomeID,
			}).Build()
			if err != nil {
				return nil, err
			}
			if existing, found := byToken[target.TokenID]; found && existing != target {
				return nil, fmt.Errorf("token %q has conflicting market identity", target.TokenID)
			}
			byToken[target.TokenID] = target
		}
	}
	for _, lot := range positions {
		target, err := (domain.BookTargetParams{
			MarketSource: lot.MarketSource, MarketID: lot.MarketID, ConditionID: lot.ConditionID,
			OutcomeIndex: lot.OutcomeIndex, TokenID: lot.TokenID,
		}).Build()
		if err != nil {
			return nil, err
		}
		if existing, found := byToken[target.TokenID]; found && existing != target {
			return nil, fmt.Errorf("position token %q has conflicting market identity", target.TokenID)
		}
		byToken[target.TokenID] = target
	}
	targets := make([]domain.BookTarget, 0, len(byToken))
	for _, target := range byToken {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].ConditionID != targets[j].ConditionID {
			return targets[i].ConditionID < targets[j].ConditionID
		}
		return targets[i].OutcomeIndex < targets[j].OutcomeIndex
	})
	return targets, nil
}

// alignBooks 按目标身份对齐 Books。
func alignBooks(targets []domain.BookTarget, books []domain.OrderBookSnapshot, observedAt time.Time) ([]domain.OrderBookSnapshot, error) {
	byToken := make(map[string]domain.OrderBookSnapshot, len(books))
	for _, book := range books {
		if _, exists := byToken[book.TokenID]; exists {
			return nil, fmt.Errorf("orderbook source returned duplicate token %q", book.TokenID)
		}
		byToken[book.TokenID] = book
	}
	result := make([]domain.OrderBookSnapshot, 0, len(targets))
	for _, target := range targets {
		book, found := byToken[target.TokenID]
		if !found {
			book = domain.OrderBookSnapshot{
				MarketSource: target.MarketSource,
				MarketID:     target.MarketID,
				ConditionID:  target.ConditionID,
				OutcomeIndex: target.OutcomeIndex,
				TokenID:      target.TokenID,
				OutcomeID:    target.OutcomeID,
				Status:       domain.OrderBookStatusMissing,
				DepthLimit:   domain.StrategyOrderBookDepth,
				ObservedAt:   observedAt,
				Bids:         []domain.PriceLevel{},
				Asks:         []domain.PriceLevel{},
				ErrorCode:    "SOURCE_DID_NOT_RETURN_TOKEN",
			}
		} else if book.MarketSource.Normalize() != target.MarketSource.Normalize() || book.MarketID != target.MarketID ||
			book.ConditionID != target.ConditionID || book.OutcomeIndex != target.OutcomeIndex ||
			strings.ToUpper(strings.TrimSpace(book.OutcomeID)) != strings.ToUpper(strings.TrimSpace(target.OutcomeID)) {
			return nil, fmt.Errorf("orderbook token %q has mismatched market identity", target.TokenID)
		}
		book = failClosedInvalidMinimumOrderSize(book)
		if len(book.Bids) > 0 {
			book.BestBid = book.Bids[0].Price
		}
		if len(book.Asks) > 0 {
			book.BestAsk = book.Asks[0].Price
		}
		if err := book.Validate(); err != nil {
			return nil, fmt.Errorf("invalid orderbook for token %q: %w", book.TokenID, err)
		}
		result = append(result, book)
		delete(byToken, target.TokenID)
	}
	if len(byToken) != 0 {
		return nil, fmt.Errorf("orderbook source returned an unexpected token")
	}
	return result, nil
}

// failClosedInvalidMinimumOrderSize isolates one upstream market whose minimum
// size metadata is unusable. The ERROR book remains visible to the strategy,
// while the execution path rejects every intent against a non-OK book. This
// prevents one malformed market from aborting the complete account cycle.
func failClosedInvalidMinimumOrderSize(book domain.OrderBookSnapshot) domain.OrderBookSnapshot {
	if book.MinOrderSize.IsEmpty() {
		return book
	}
	sign, err := book.MinOrderSize.Sign()
	if err == nil && sign > 0 {
		return book
	}
	book.Status = domain.OrderBookStatusError
	book.MinOrderSize = ""
	book.BestBid = ""
	book.BestAsk = ""
	book.Bids = []domain.PriceLevel{}
	book.Asks = []domain.PriceLevel{}
	book.ErrorCode = "INVALID_MIN_ORDER_SIZE"
	return book
}

// validateClaimedInput 校验 Claimed Input 的字段和业务约束。
func validateClaimedInput(request domain.StrategyDecisionRequest, expectedCycleID string, expectedContext domain.StrategyExecutionContext, expectedDecisionAt time.Time) error {
	if request.SchemaVersion != domain.StrategyInputSchemaVersion || request.CycleID != expectedCycleID || request.InputID == "" ||
		!request.Context.Equal(expectedContext) || !request.DecisionAt.Equal(expectedDecisionAt) {
		return fmt.Errorf("claimed strategy input identity is invalid")
	}
	if request.PredictionScope != domain.PredictionScopeAllEffective {
		return fmt.Errorf("claimed strategy input must contain all effective predictions")
	}
	if err := validateExecutionConstraints(request.ExecutionConstraints); err != nil {
		return fmt.Errorf("claimed strategy execution constraints: %w", err)
	}
	tokens := make(map[string]domain.BookTarget)
	for _, prediction := range request.Predictions {
		if strings.TrimSpace(prediction.Model.Name) != request.Context.ModelID {
			return fmt.Errorf("claimed strategy input contains prediction from another model")
		}
		if err := prediction.Validate(request.DecisionAt); err != nil {
			return fmt.Errorf("claimed strategy prediction %q: %w", prediction.PredictionID, err)
		}
		for _, outcome := range prediction.Outcomes {
			target := domain.BookTarget{
				MarketSource: prediction.MarketSource, MarketID: prediction.MarketID, ConditionID: prediction.ConditionID,
				OutcomeIndex: outcome.Index, TokenID: outcome.TokenID, OutcomeID: outcome.OutcomeID,
			}
			if existing, exists := tokens[outcome.TokenID]; exists && existing != target {
				return fmt.Errorf("claimed strategy prediction token %q has conflicting market identity", outcome.TokenID)
			}
			tokens[outcome.TokenID] = target
		}
	}
	seenLots := make(map[string]struct{}, len(request.Positions))
	for _, lot := range request.Positions {
		if err := lot.Validate(request.DecisionAt); err != nil {
			return fmt.Errorf("claimed strategy position lot %q: %w", lot.LotID, err)
		}
		if _, exists := seenLots[lot.LotID]; exists {
			return fmt.Errorf("claimed strategy input contains duplicate position lot %q", lot.LotID)
		}
		seenLots[lot.LotID] = struct{}{}
		target := domain.BookTarget{MarketID: lot.MarketID, ConditionID: lot.ConditionID, OutcomeIndex: lot.OutcomeIndex, TokenID: lot.TokenID}
		if existing, exists := tokens[lot.TokenID]; exists && existing != target {
			return fmt.Errorf("claimed strategy position token %q has conflicting market identity", lot.TokenID)
		}
		tokens[lot.TokenID] = target
	}
	if len(request.OrderBooks) != len(tokens) {
		return fmt.Errorf("claimed strategy input must contain one orderbook for every prediction or position token")
	}
	for _, book := range request.OrderBooks {
		if err := book.Validate(); err != nil {
			return fmt.Errorf("claimed strategy orderbook %q: %w", book.TokenID, err)
		}
		if book.DepthLimit != domain.StrategyOrderBookDepth {
			return fmt.Errorf("claimed strategy orderbook %q must have depth_limit=%d", book.TokenID, domain.StrategyOrderBookDepth)
		}
		target, exists := tokens[book.TokenID]
		if !exists || target.MarketSource.Normalize() != book.MarketSource.Normalize() || target.MarketID != book.MarketID ||
			target.ConditionID != book.ConditionID || target.OutcomeIndex != book.OutcomeIndex ||
			strings.ToUpper(strings.TrimSpace(target.OutcomeID)) != strings.ToUpper(strings.TrimSpace(book.OutcomeID)) {
			return fmt.Errorf("claimed strategy orderbook %q has unexpected identity", book.TokenID)
		}
		delete(tokens, book.TokenID)
	}
	if len(tokens) != 0 {
		return fmt.Errorf("claimed strategy input is missing an orderbook")
	}
	inputID, err := domain.ComputeStrategyInputID(request)
	if err != nil {
		return err
	}
	if inputID != request.InputID {
		return fmt.Errorf("claimed strategy input hash does not match its payload")
	}
	return nil
}

// validateExecutionConstraints 校验 Execution Constraints 的字段和业务约束。
func validateExecutionConstraints(constraints domain.StrategyExecutionConstraints) error {
	if constraints.SizeUnit != "SHARES" || constraints.SizeDecimalPlaces != 2 || constraints.BuyNotionalDecimals != 4 ||
		constraints.PriceProtectionPolicy != domain.StrategyPriceProtectionDepthAwareLimit ||
		constraints.MaxPriceSlippageTicks < 1 || constraints.MaxPriceSlippageTicks > domain.DefaultStrategyMaxPriceSlippageTicks ||
		len(constraints.AllowedTimeInForce) != 1 ||
		constraints.AllowedTimeInForce[0] != domain.TimeInForceFOK {
		return fmt.Errorf("unsupported strategy execution constraint set")
	}
	if sign, err := constraints.MinimumBuyNotional.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("minimum_buy_notional must be positive")
	}
	return nil
}

// expectedEvaluation 表示后端使用的 expectedEvaluation 类型。
type expectedEvaluation struct {
	prediction domain.Prediction
	outcome    domain.PredictionOutcome
}

// validateResponse 校验 Response 的字段和业务约束。
func validateResponse(request domain.StrategyDecisionRequest, response domain.StrategyDecisionResponse, venue string) ([]domain.OrderIntent, error) {
	return validateResponseWithEntryPolicy(request, response, venue, true, "")
}

// validateResponseWithEntryPolicy keeps the frozen input truthful during a
// model-coverage incident while enforcing a hard Go-side entry gate. Blocked
// BUY proposals are audited in the strategy response but never become durable
// OrderIntents; valid exits in the same response remain executable.
func validateResponseWithEntryPolicy(
	request domain.StrategyDecisionRequest,
	response domain.StrategyDecisionResponse,
	venue string,
	allowEntries bool,
	expectedBlockReason string,
) ([]domain.OrderIntent, error) {
	if err := request.Context.Validate(); err != nil {
		return nil, fmt.Errorf("%w: request execution context: %v", ErrInvalidStrategy, err)
	}
	if allowEntries {
		if expectedBlockReason != "" || response.EntryPolicy != nil {
			return nil, fmt.Errorf("%w: healthy Trading entry policy must be absent", ErrInvalidStrategy)
		}
	} else if expectedBlockReason == "" || response.EntryPolicy == nil || response.EntryPolicy.Enabled ||
		response.EntryPolicy.BlockReason != expectedBlockReason {
		return nil, fmt.Errorf("%w: blocked Trading entry policy mismatch", ErrInvalidStrategy)
	}
	if response.SchemaVersion != domain.StrategyOutputSchemaVersion || response.CycleID != request.CycleID ||
		response.InputID != request.InputID || !response.Context.Equal(request.Context) {
		return nil, fmt.Errorf("%w: schema, cycle, input, or execution context mismatch", ErrInvalidStrategy)
	}
	if response.DecidedAt.IsZero() || response.DecidedAt.Before(request.DecisionAt) {
		return nil, fmt.Errorf("%w: decided_at is invalid", ErrInvalidStrategy)
	}
	expected := make(map[string]expectedEvaluation)
	for _, prediction := range request.Predictions {
		for _, outcome := range prediction.Outcomes {
			expected[evaluationKey(prediction.PredictionID, outcome.TokenID)] = expectedEvaluation{prediction: prediction, outcome: outcome}
		}
	}
	if len(response.Evaluations) != len(expected) {
		return nil, fmt.Errorf("%w: expected %d evaluations, got %d", ErrInvalidStrategy, len(expected), len(response.Evaluations))
	}
	seenEvaluations := make(map[string]struct{}, len(response.Evaluations))
	seenEvaluationDecisions := make(map[string]struct{}, len(response.Evaluations))
	seenDecisions := make(map[string]struct{}, len(response.Evaluations)+len(response.Exits))
	intents := make([]domain.OrderIntent, 0)
	for index, evaluation := range response.Evaluations {
		key := evaluationKey(evaluation.PredictionID, evaluation.TokenID)
		expectedValue, found := expected[key]
		if !found || evaluation.MarketID != expectedValue.prediction.MarketID ||
			evaluation.ConditionID != expectedValue.prediction.ConditionID || evaluation.OutcomeIndex != expectedValue.outcome.Index {
			return nil, fmt.Errorf("%w: evaluation %d does not belong to the decision input", ErrInvalidStrategy, index)
		}
		if _, exists := seenEvaluations[key]; exists {
			return nil, fmt.Errorf("%w: duplicate evaluation for prediction/token", ErrInvalidStrategy)
		}
		seenEvaluations[key] = struct{}{}
		evaluation.DecisionID = strings.TrimSpace(evaluation.DecisionID)
		evaluation.ReasonCode = domain.StrategyReasonCode(strings.TrimSpace(string(evaluation.ReasonCode)))
		if evaluation.DecisionID == "" || evaluation.ReasonCode == "" {
			return nil, fmt.Errorf("%w: evaluation %d requires decision_id and reason_code", ErrInvalidStrategy, index)
		}
		if _, exists := seenEvaluationDecisions[evaluation.DecisionID]; exists {
			return nil, fmt.Errorf("%w: duplicate decision_id %q", ErrInvalidStrategy, evaluation.DecisionID)
		}
		seenEvaluationDecisions[evaluation.DecisionID] = struct{}{}
		// A coverage-suppressed SUBMIT never becomes an intent, so its
		// audit-only decision ID must not be able to suppress an independent
		// SELL exit. It remains unique among evaluations.
		if evaluation.Action != domain.StrategyActionSubmit || allowEntries {
			if _, exists := seenDecisions[evaluation.DecisionID]; exists {
				return nil, fmt.Errorf("%w: duplicate decision_id %q", ErrInvalidStrategy, evaluation.DecisionID)
			}
			seenDecisions[evaluation.DecisionID] = struct{}{}
		}
		if math.IsNaN(evaluation.Evidence.Probability) || math.IsInf(evaluation.Evidence.Probability, 0) ||
			math.Abs(evaluation.Evidence.Probability-expectedValue.outcome.Probability) > 1e-12 {
			return nil, fmt.Errorf("%w: evaluation %d probability does not match the input", ErrInvalidStrategy, index)
		}
		if err := validateEvidenceMetrics(
			evaluation.Evidence, request, evaluation.TokenID,
			evaluation.Action == domain.StrategyActionSubmit && allowEntries,
		); err != nil {
			return nil, fmt.Errorf("%w: evaluation %d: %v", ErrInvalidStrategy, index, err)
		}
		requiredReason := inputFailureReason(request, evaluation.TokenID)
		switch evaluation.Action {
		case domain.StrategyActionSkip:
			if evaluation.Order != nil {
				return nil, fmt.Errorf("%w: SKIP evaluation %d must not contain order parameters", ErrInvalidStrategy, index)
			}
			if !validSkipReason(evaluation.ReasonCode) {
				return nil, fmt.Errorf("%w: evaluation %d has unsupported SKIP reason_code %q", ErrInvalidStrategy, index, evaluation.ReasonCode)
			}
			if requiredReason != "" && evaluation.ReasonCode != requiredReason {
				return nil, fmt.Errorf("%w: evaluation %d must map input status to %s", ErrInvalidStrategy, index, requiredReason)
			}
		case domain.StrategyActionSubmit:
			if !allowEntries {
				continue
			}
			if requiredReason != "" {
				return nil, fmt.Errorf("%w: evaluation %d cannot SUBMIT with %s input", ErrInvalidStrategy, index, requiredReason)
			}
			if evaluation.ReasonCode != domain.StrategyReasonEntrySignal {
				return nil, fmt.Errorf("%w: evaluation %d SUBMIT reason_code must be ENTRY_SIGNAL", ErrInvalidStrategy, index)
			}
			intent, err := buildEntryIntent(request, response.DecidedAt, evaluation, venue)
			if err != nil {
				return nil, fmt.Errorf("%w: evaluation %d: %v", ErrInvalidStrategy, index, err)
			}
			intents = append(intents, intent)
		default:
			return nil, fmt.Errorf("%w: evaluation %d has unsupported action %q", ErrInvalidStrategy, index, evaluation.Action)
		}
	}
	seenExits := make(map[string]struct{}, len(response.Exits))
	for index, exit := range response.Exits {
		exit.DecisionID = strings.TrimSpace(exit.DecisionID)
		exit.LotID = strings.TrimSpace(exit.LotID)
		exit.TokenID = strings.TrimSpace(exit.TokenID)
		exit.ReasonCode = domain.StrategyReasonCode(strings.TrimSpace(string(exit.ReasonCode)))
		if exit.DecisionID == "" || exit.LotID == "" || exit.TokenID == "" {
			return nil, fmt.Errorf("%w: exit %d requires decision_id, lot_id, and token_id", ErrInvalidStrategy, index)
		}
		if _, exists := seenDecisions[exit.DecisionID]; exists {
			return nil, fmt.Errorf("%w: duplicate decision_id %q", ErrInvalidStrategy, exit.DecisionID)
		}
		seenDecisions[exit.DecisionID] = struct{}{}
		if _, exists := seenExits[exit.LotID]; exists {
			return nil, fmt.Errorf("%w: duplicate exit for lot %q", ErrInvalidStrategy, exit.LotID)
		}
		seenExits[exit.LotID] = struct{}{}
		intent, err := buildExitIntent(request, response.DecidedAt, exit, venue)
		if err != nil {
			return nil, fmt.Errorf("%w: exit %d: %v", ErrInvalidStrategy, index, err)
		}
		intents = append(intents, intent)
	}
	return intents, nil
}

// buildEntryIntent 根据已验证的策略评估构建买入订单意图。
func buildEntryIntent(request domain.StrategyDecisionRequest, signalAt time.Time, evaluation domain.StrategyEvaluation, venue string) (domain.OrderIntent, error) {
	if evaluation.Order == nil {
		return domain.OrderIntent{}, fmt.Errorf("SUBMIT requires order parameters")
	}
	order := *evaluation.Order
	strategyID := request.Context.Normalize().StrategyID
	if strategyID != domain.StrategyIDMultfactorV1 && strategyID != domain.StrategyIDMultfactorV2 {
		return domain.OrderIntent{}, fmt.Errorf("unsupported strategy_id %q", strategyID)
	}
	prediction, found := strategyPrediction(request.Predictions, evaluation.PredictionID)
	if !found || evaluation.OutcomeIndex < 0 || evaluation.OutcomeIndex >= len(prediction.Outcomes) {
		return domain.OrderIntent{}, fmt.Errorf("strategy prediction/outcome identity is missing")
	}
	book, found := strategyBook(request.OrderBooks, evaluation.TokenID)
	if !found || book.Status != domain.OrderBookStatusOK {
		return domain.OrderIntent{}, fmt.Errorf("SUBMIT requires an OK orderbook")
	}
	if err := validateStrategyOrderForMarket(
		order, domain.SideBuy, book, request.ExecutionConstraints, prediction.MarketSource,
	); err != nil {
		return domain.OrderIntent{}, err
	}
	roundedSize, err := roundBuyShares(order.Size)
	if err != nil {
		return domain.OrderIntent{}, fmt.Errorf("round BUY order.size: %w", err)
	}
	order.Size = roundedSize
	outcomeIndex := evaluation.OutcomeIndex
	intentVenue, err := prediction.MarketSource.Venue(venue)
	if err != nil {
		return domain.OrderIntent{}, err
	}
	expectedNegRisk := prediction.NegRisk
	marketSnapshotAt := book.SourceAt
	signalAt = signalAt.UTC()
	limitPrice := domain.Decimal("")
	if order.Type == domain.OrderTypeLimit {
		limitPrice = order.WorstPrice
	}
	metadata := map[string]string{
		"cycle_id":                 request.CycleID,
		"input_id":                 request.InputID,
		"prediction_id":            evaluation.PredictionID,
		"strategy_decision_id":     evaluation.DecisionID,
		"strategy_reason_code":     string(evaluation.ReasonCode),
		"strategy_reference_price": book.Asks[0].Price.String(),
		"strategy_worst_price":     order.WorstPrice.String(),
		"predicted_probability":    strconv.FormatFloat(evaluation.Evidence.Probability, 'f', -1, 64),
		"market_question":          prediction.Question,
		"model_id":                 request.Context.ModelID,
		"execution_account_id":     request.Context.ExecutionAccountID,
		"market_source":            string(prediction.MarketSource.Normalize()),
		"outcome_id":               prediction.Outcomes[outcomeIndex].OutcomeID,
	}
	executionTimeInForce := order.TimeInForce
	if prediction.MarketSource.Normalize() == domain.MarketSourceKalshi {
		executionTimeInForce = domain.TimeInForceIOC
		metadata["strategy_time_in_force"] = string(order.TimeInForce)
		metadata["execution_time_in_force"] = string(executionTimeInForce)
	}
	if !evaluation.Evidence.Edge.IsEmpty() {
		metadata["strategy_edge"] = evaluation.Evidence.Edge.String()
	}
	if !order.Size.Equal(evaluation.Order.Size) {
		metadata["strategy_requested_size"] = evaluation.Order.Size.String()
	}
	intent, err := (domain.OrderIntentParams{
		ModelID:            request.Context.ModelID,
		StrategyID:         request.Context.StrategyID,
		ExecutionAccountID: request.Context.ExecutionAccountID,
		SignalID:           evaluation.DecisionID,
		ClientOrderID:      clientOrderID(request.CycleID, evaluation.DecisionID),
		Venue:              intentVenue,
		MarketSource:       prediction.MarketSource,
		MarketID:           evaluation.MarketID,
		ConditionID:        evaluation.ConditionID,
		OutcomeIndex:       &outcomeIndex,
		OutcomeName:        prediction.Outcomes[outcomeIndex].Name,
		OutcomeID:          prediction.Outcomes[outcomeIndex].OutcomeID,
		TokenID:            evaluation.TokenID,
		ExpectedNegRisk:    &expectedNegRisk,
		MarketSnapshotAt:   &marketSnapshotAt,
		SignalAt:           &signalAt,
		Side:               domain.SideBuy,
		Type:               order.Type,
		Price:              limitPrice,
		WorstPrice:         order.WorstPrice,
		Size:               order.Size,
		TimeInForce:        executionTimeInForce,
		ExpiresAt:          order.ExpiresAt,
		Metadata:           metadata,
	}).Build()
	return intent, err
}

// buildExitIntent 根据已验证的策略退出结果构建卖出订单意图。
func buildExitIntent(request domain.StrategyDecisionRequest, signalAt time.Time, exit domain.StrategyExit, venue string) (domain.OrderIntent, error) {
	if exit.ReasonCode != domain.StrategyReasonHold48H {
		return domain.OrderIntent{}, fmt.Errorf("exit reason_code must be HOLD_48H")
	}
	lot, found := strategyPositionLot(request.Positions, exit.LotID)
	if !found || lot.TokenID != exit.TokenID {
		return domain.OrderIntent{}, fmt.Errorf("exit lot/token does not belong to the decision input")
	}
	if request.DecisionAt.Sub(lot.EnteredAt) < 48*time.Hour {
		return domain.OrderIntent{}, fmt.Errorf("position lot has not been held for 48 hours")
	}
	book, found := strategyBook(request.OrderBooks, lot.TokenID)
	if !found || book.Status != domain.OrderBookStatusOK {
		return domain.OrderIntent{}, fmt.Errorf("exit requires an OK orderbook")
	}
	if exit.Order == nil {
		return domain.OrderIntent{}, fmt.Errorf("exit requires order parameters")
	}
	if err := validateStrategyOrderForMarket(
		*exit.Order, domain.SideSell, book, request.ExecutionConstraints, lot.MarketSource,
	); err != nil {
		return domain.OrderIntent{}, err
	}
	if comparison, err := exit.Order.Size.Compare(lot.Shares); err != nil || comparison > 0 {
		return domain.OrderIntent{}, fmt.Errorf("exit size must not exceed the selected lot's remaining shares")
	}
	outcomeIndex := lot.OutcomeIndex
	outcomeID, err := executionOutcomeID(lot.MarketSource, lot.ConditionID, lot.TokenID)
	if err != nil {
		return domain.OrderIntent{}, err
	}
	expectedNegRisk := lot.NegRisk
	marketSnapshotAt := book.SourceAt
	signalAt = signalAt.UTC()
	metadata := map[string]string{
		"cycle_id":                 request.CycleID,
		"input_id":                 request.InputID,
		"strategy_decision_id":     exit.DecisionID,
		"strategy_reason_code":     string(exit.ReasonCode),
		"strategy_reference_price": book.Bids[0].Price.String(),
		"strategy_worst_price":     exit.Order.WorstPrice.String(),
		"target_lot_id":            lot.LotID,
		"model_id":                 request.Context.ModelID,
		"execution_account_id":     request.Context.ExecutionAccountID,
	}
	executionTimeInForce := exit.Order.TimeInForce
	if lot.MarketSource.Normalize() == domain.MarketSourceKalshi {
		executionTimeInForce = domain.TimeInForceIOC
		metadata["strategy_time_in_force"] = string(exit.Order.TimeInForce)
		metadata["execution_time_in_force"] = string(executionTimeInForce)
	}
	intentVenue, err := lot.MarketSource.Venue(venue)
	if err != nil {
		return domain.OrderIntent{}, err
	}
	intent, err := (domain.OrderIntentParams{
		ModelID: request.Context.ModelID, StrategyID: request.Context.StrategyID,
		ExecutionAccountID: request.Context.ExecutionAccountID,
		SignalID:           exit.DecisionID, ClientOrderID: clientOrderID(request.CycleID, exit.DecisionID),
		Venue: intentVenue, MarketSource: lot.MarketSource, MarketID: lot.MarketID, ConditionID: lot.ConditionID,
		OutcomeIndex: &outcomeIndex, OutcomeName: lot.OutcomeName, OutcomeID: outcomeID, TokenID: lot.TokenID,
		TargetLotID: lot.LotID, ExpectedNegRisk: &expectedNegRisk,
		MarketSnapshotAt: &marketSnapshotAt, SignalAt: &signalAt,
		Side: domain.SideSell, Type: exit.Order.Type, Price: exit.Order.WorstPrice,
		WorstPrice: exit.Order.WorstPrice, Size: exit.Order.Size,
		TimeInForce: executionTimeInForce, ExpiresAt: exit.Order.ExpiresAt, Metadata: metadata,
	}).Build()
	return intent, err
}

func executionOutcomeID(source domain.MarketSource, conditionID, tokenID string) (string, error) {
	if source.Normalize() != domain.MarketSourceKalshi {
		return "", nil
	}
	prefix := strings.TrimSpace(conditionID) + ":"
	if !strings.HasPrefix(strings.TrimSpace(tokenID), prefix) {
		return "", fmt.Errorf("Kalshi position token does not match its condition")
	}
	outcomeID := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(tokenID), prefix)))
	if outcomeID != "YES" && outcomeID != "NO" {
		return "", fmt.Errorf("Kalshi position token has an invalid outcome")
	}
	return outcomeID, nil
}

// validateStrategyOrder 校验 Strategy Order 的字段和业务约束。
func validateStrategyOrder(order domain.StrategyOrderParams, side domain.Side, book domain.OrderBookSnapshot, constraints domain.StrategyExecutionConstraints) error {
	return validateStrategyOrderForMarket(order, side, book, constraints, domain.MarketSourcePolymarket)
}

// validateStrategyOrderForMarket keeps the existing two-tick protection for
// Polymarket's dense books. Kalshi books can legitimately skip price levels;
// for them the strategy's explicit worst price plus cumulative visible depth
// is the protection boundary, and the live validator repeats that check using
// a fresh official book immediately before placement.
func validateStrategyOrderForMarket(
	order domain.StrategyOrderParams,
	side domain.Side,
	book domain.OrderBookSnapshot,
	constraints domain.StrategyExecutionConstraints,
	marketSource domain.MarketSource,
) error {
	if order.Side != side || order.Type != domain.OrderTypeLimit || order.TimeInForce != domain.TimeInForceFOK || order.ExpiresAt != nil {
		return fmt.Errorf("order must be a %s LIMIT FOK without expires_at", side)
	}
	if sign, err := order.Size.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("order.size must be positive shares")
	}
	if decimalPlaces(order.Size) > constraints.SizeDecimalPlaces {
		return fmt.Errorf("order.size exceeds %d decimal places", constraints.SizeDecimalPlaces)
	}
	effectiveSize := order.Size
	if side == domain.SideBuy {
		var err error
		effectiveSize, err = roundBuyShares(order.Size)
		if err != nil {
			return fmt.Errorf("round BUY order.size: %w", err)
		}
		if sign, err := effectiveSize.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("BUY order.size rounds below one whole share")
		}
	}
	if !book.MinOrderSize.IsEmpty() {
		if comparison, err := effectiveSize.Compare(book.MinOrderSize); err != nil || comparison < 0 {
			return fmt.Errorf("order.size is below orderbook min_order_size")
		}
	}
	if err := validateStrategyProtectedPrice(order.WorstPrice, side, book); err != nil {
		return err
	}
	levels := book.Asks
	if side == domain.SideSell {
		levels = book.Bids
	}
	availableShares, err := protectedPriceLiquidity(levels, side, order.WorstPrice)
	if err != nil {
		return fmt.Errorf("calculate %s protected-price liquidity: %w", side, err)
	}
	requestedShares, err := effectiveSize.Multiply("1")
	if err != nil {
		return fmt.Errorf("calculate effective %s shares: %w", side, err)
	}
	if marketSource.Normalize() == domain.MarketSourceKalshi {
		if availableShares.Sign() <= 0 {
			return fmt.Errorf("%s order has no protected-price liquidity", side)
		}
	} else if requestedShares.Cmp(availableShares) > 0 {
		return fmt.Errorf("%s order.size exceeds protected-price liquidity", side)
	}
	if side == domain.SideBuy {
		notional, err := order.WorstPrice.Multiply(effectiveSize)
		if err != nil {
			return fmt.Errorf("calculate BUY notional: %w", err)
		}
		minimum, err := constraints.MinimumBuyNotional.Multiply("1")
		if err != nil || notional.Cmp(minimum) < 0 {
			return fmt.Errorf("BUY notional is below minimum_buy_notional")
		}
		if rationalDecimalPlaces(notional) > constraints.BuyNotionalDecimals {
			return fmt.Errorf("BUY price*size exceeds %d decimal places", constraints.BuyNotionalDecimals)
		}
	}
	return nil
}

// validateStrategyProtectedPrice validates only the strategy's explicit limit
// boundary. Trading intentionally does not cap its distance from top-of-book:
// the strategy owns worst_price, while execution still requires a valid tick,
// the executable side of the book, and protected-price liquidity.
func validateStrategyProtectedPrice(
	protectedPrice domain.Decimal,
	side domain.Side,
	book domain.OrderBookSnapshot,
) error {
	if book.TickSize.IsEmpty() {
		return fmt.Errorf("DEPTH_AWARE_LIMIT requires orderbook tick_size")
	}
	if sign, err := book.TickSize.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("orderbook tick_size must be positive")
	}
	if sign, err := protectedPrice.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("order.worst_price must be positive")
	}
	if comparison, err := protectedPrice.Compare("1"); err != nil || comparison > 0 {
		return fmt.Errorf("order.worst_price must not exceed one")
	}
	if multiple, err := protectedPrice.IsMultipleOf(book.TickSize); err != nil || !multiple {
		return fmt.Errorf("order.worst_price must be an exact multiple of orderbook tick_size")
	}

	switch side {
	case domain.SideBuy:
		if len(book.Asks) == 0 {
			return fmt.Errorf("BUY price protection requires an ask")
		}
		if comparison, err := protectedPrice.Compare(book.Asks[0].Price); err != nil || comparison < 0 {
			return fmt.Errorf("BUY order.worst_price must be at or above the strategy snapshot best ask")
		}
	case domain.SideSell:
		if len(book.Bids) == 0 {
			return fmt.Errorf("SELL price protection requires a bid")
		}
		if comparison, err := protectedPrice.Compare(book.Bids[0].Price); err != nil || comparison > 0 {
			return fmt.Errorf("SELL order.worst_price must be at or below the strategy snapshot best bid")
		}
	default:
		return fmt.Errorf("unsupported strategy order side %q", side)
	}
	return nil
}

// protectedPriceLiquidity 汇总保护价范围内可由 FOK 限价单成交的可见 shares。
func protectedPriceLiquidity(levels []domain.PriceLevel, side domain.Side, protectedPrice domain.Decimal) (*big.Rat, error) {
	total := new(big.Rat)
	for _, level := range levels {
		comparison, err := level.Price.Compare(protectedPrice)
		if err != nil {
			return nil, err
		}
		executable := side == domain.SideBuy && comparison <= 0 || side == domain.SideSell && comparison >= 0
		if !executable {
			continue
		}
		size, err := level.Size.Multiply("1")
		if err != nil {
			return nil, err
		}
		total.Add(total, size)
	}
	return total, nil
}

// roundBuyShares 使用精确有理数运算将 BUY shares 四舍五入为整数，半数向上。
func roundBuyShares(size domain.Decimal) (domain.Decimal, error) {
	value, err := size.Multiply("1")
	if err != nil {
		return "", err
	}
	quotient, remainder := new(big.Int).QuoRem(value.Num(), value.Denom(), new(big.Int))
	doubleRemainder := new(big.Int).Mul(new(big.Int).Abs(remainder), big.NewInt(2))
	if doubleRemainder.Cmp(value.Denom()) >= 0 {
		if value.Sign() >= 0 {
			quotient.Add(quotient, big.NewInt(1))
		} else {
			quotient.Sub(quotient, big.NewInt(1))
		}
	}
	return domain.Decimal(quotient.String()), nil
}

// decimalPlaces 计算十进制值所需的小数位数。
func decimalPlaces(value domain.Decimal) int {
	text := strings.TrimSpace(value.String())
	if point := strings.IndexByte(text, '.'); point >= 0 {
		return len(strings.TrimRight(text[point+1:], "0"))
	}
	return 0
}

// rationalDecimalPlaces 计算十进制值所需的小数位数。
func rationalDecimalPlaces(value *big.Rat) int {
	denominator := new(big.Int).Set(value.Denom())
	two, five := big.NewInt(2), big.NewInt(5)
	twoCount, fiveCount := 0, 0
	zero := big.NewInt(0)
	for remainder := new(big.Int); ; twoCount++ {
		remainder.Mod(denominator, two)
		if remainder.Cmp(zero) != 0 {
			break
		}
		denominator.Div(denominator, two)
	}
	for remainder := new(big.Int); ; fiveCount++ {
		remainder.Mod(denominator, five)
		if remainder.Cmp(zero) != 0 {
			break
		}
		denominator.Div(denominator, five)
	}
	if denominator.Cmp(big.NewInt(1)) != 0 {
		return math.MaxInt
	}
	return max(twoCount, fiveCount)
}

// validateEvidenceMetrics 校验 Evidence Metrics 的字段和业务约束。
func validateEvidenceMetrics(evidence domain.StrategyEvidence, request domain.StrategyDecisionRequest, tokenID string, requireAll bool) error {
	strategyID := request.Context.Normalize().StrategyID
	for key, value := range evidence.Metrics {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("evidence.metrics key must be non-empty")
		}
		if _, err := domain.ParseDecimal(value); err != nil {
			return fmt.Errorf("evidence.metrics.%s must be a decimal string", key)
		}
	}
	if !requireAll {
		return nil
	}
	required := []string{"best_ask", "near_logdiff_usd", "rel_spread"}
	if strategyID == domain.StrategyIDMultfactorV2 {
		required = append(required, "MOM", "MACD_SIGNAL")
	}
	for _, key := range required {
		if _, exists := evidence.Metrics[key]; !exists {
			return fmt.Errorf("SUBMIT requires evidence.metrics.%s", key)
		}
	}
	book, found := strategyBook(request.OrderBooks, tokenID)
	if !found || book.Status != domain.OrderBookStatusOK || len(book.Asks) == 0 ||
		!domain.Decimal(evidence.Metrics["best_ask"]).Equal(book.Asks[0].Price) {
		return fmt.Errorf("evidence.metrics.best_ask must equal the input best ask")
	}
	return nil
}

// inputFailureReason 根据订单簿状态返回强制跳过原因。MOM/MACD 所需历史由策略服务自行读取。
func inputFailureReason(request domain.StrategyDecisionRequest, tokenID string) domain.StrategyReasonCode {
	book, found := strategyBook(request.OrderBooks, tokenID)
	if !found || book.Status != domain.OrderBookStatusOK {
		return domain.StrategyReasonInvalidBook
	}
	return ""
}

// validSkipReason 判断当前业务条件是否成立。
func validSkipReason(reason domain.StrategyReasonCode) bool {
	switch reason {
	case domain.StrategyReasonEdgeTooLow, domain.StrategyReasonSpreadTooWide,
		domain.StrategyReasonLiquidityTooLow, domain.StrategyReasonPriceOutOfRange,
		domain.StrategyReasonHourlyVeto, domain.StrategyReasonFactorWarmup,
		domain.StrategyReasonStaleData, domain.StrategyReasonInvalidBook,
		domain.StrategyReasonOutsideUniverse:
		return true
	default:
		return false
	}
}

// strategyPrediction 按预测标识查找冻结输入中的预测记录。
func strategyPrediction(predictions []domain.Prediction, predictionID string) (domain.Prediction, bool) {
	for _, prediction := range predictions {
		if prediction.PredictionID == predictionID {
			return prediction, true
		}
	}
	return domain.Prediction{}, false
}

// strategyPositionLot 按批次标识查找冻结输入中的持仓批次。
func strategyPositionLot(positions []domain.StrategyPositionLot, lotID string) (domain.StrategyPositionLot, bool) {
	for _, lot := range positions {
		if lot.LotID == lotID {
			return lot, true
		}
	}
	return domain.StrategyPositionLot{}, false
}

// strategyBook 按 token 查找冻结输入中的订单簿快照。
func strategyBook(books []domain.OrderBookSnapshot, tokenID string) (domain.OrderBookSnapshot, bool) {
	for _, book := range books {
		if book.TokenID == tokenID {
			return book, true
		}
	}
	return domain.OrderBookSnapshot{}, false
}

// evaluationKey 根据稳定业务身份生成幂等标识。
func evaluationKey(predictionID, tokenID string) string {
	return predictionID + "\x00" + tokenID
}

// clientOrderID 根据稳定业务身份生成幂等标识。
func clientOrderID(cycleID, decisionID string) string {
	digest := sha256.Sum256([]byte(cycleID + "\x00" + decisionID))
	return "strategy-order-" + hex.EncodeToString(digest[:16])
}

// cycleID 根据稳定业务身份生成幂等标识。
func cycleID(executionContext domain.StrategyExecutionContext, decisionAt time.Time) string {
	return executionContext.ExecutionAccountID + ":" + decisionAt.UTC().Format("20060102T150405Z")
}

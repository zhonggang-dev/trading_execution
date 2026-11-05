package decisioncycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/marketbooks"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// The isolation fixture mirrors production: three Polymarket accounts and one
// Kalshi account sharing one prediction model, each holding a lot older than
// 48 hours. Every strategy call exits every lot it is given.

const isolationKalshiAccount = "kalshi:main"

var isolationDecisionAt = time.Date(2026, 11, 5, 3, 10, 0, 0, time.UTC)

func isolationBindings() []domain.StrategyExecutionBinding {
	return []domain.StrategyExecutionBinding{
		{ModelID: "test", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main"},
		{ModelID: "test", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-6"},
		{ModelID: "test", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7"},
		{ModelID: "test", StrategyID: "brier_mid_edge_once", ExecutionAccountID: isolationKalshiAccount, MarketSource: domain.MarketSourceKalshi},
	}
}

func isolationLot(account, lotID, marketID, conditionID, tokenID string, outcomeIndex int) domain.PositionLot {
	negRisk := false
	return domain.PositionLot{
		LotID: lotID, ExecutionAccountID: account, MarketID: marketID, ConditionID: conditionID,
		TokenID: tokenID, OutcomeIndex: &outcomeIndex, OutcomeName: "Yes", NegRisk: &negRisk,
		ModelID: "test", StrategyID: "opened-by-another-strategy",
		RemainingShares: "10", AverageEntryPrice: "0.40", Status: domain.PositionLotOpen,
		OpenedAt: isolationDecisionAt.Add(-49 * time.Hour),
	}
}

func isolationPositions() accountPositionSource {
	return accountPositionSource{
		"main":     {isolationLot("main", "lot-main", "market-1", "condition-1", "yes-token", 0)},
		"wallet-6": {isolationLot("wallet-6", "lot-w6", "market-1", "condition-1", "no-token", 1)},
		"wallet-7": {isolationLot("wallet-7", "lot-w7", "market-1", "condition-1", "yes-token", 0)},
		isolationKalshiAccount: {
			isolationLot(isolationKalshiAccount, "lot-kalshi", "KXLOT", "kalshi:KXLOT", "kalshi:KXLOT:YES", 0),
		},
	}
}

func isolationSnapshot() domain.PredictionSnapshot {
	return mixedVenueSnapshot(isolationDecisionAt, validPrediction(isolationDecisionAt), kalshiPrediction(isolationDecisionAt))
}

// exitStrategy skips every prediction outcome and exits every lot. Accounts in
// block wait for their context to end, like a stuck strategy service.
type exitStrategy struct {
	requests []domain.StrategyDecisionRequest
	block    map[string]bool
}

func (strategy *exitStrategy) Decide(ctx context.Context, request domain.StrategyDecisionRequest) (domain.StrategyDecisionResponse, error) {
	strategy.requests = append(strategy.requests, request)
	if strategy.block[request.Context.ExecutionAccountID] {
		<-ctx.Done()
		return domain.StrategyDecisionResponse{}, ctx.Err()
	}
	evaluations := make([]domain.StrategyEvaluation, 0)
	for _, prediction := range request.Predictions {
		for _, outcome := range prediction.Outcomes {
			evaluation := domain.StrategyEvaluation{
				DecisionID:   request.CycleID + ":" + prediction.PredictionID + ":" + outcome.TokenID,
				PredictionID: prediction.PredictionID, MarketID: prediction.MarketID,
				ConditionID: prediction.ConditionID, OutcomeIndex: outcome.Index, TokenID: outcome.TokenID,
				Action: domain.StrategyActionSkip, ReasonCode: inputFailureReason(request, outcome.TokenID),
				Evidence: domain.StrategyEvidence{Probability: outcome.Probability},
			}
			if evaluation.ReasonCode == "" {
				evaluation.ReasonCode = domain.StrategyReasonEdgeTooLow
			}
			evaluations = append(evaluations, evaluation)
		}
	}
	exits := make([]domain.StrategyExit, 0, len(request.Positions))
	for _, position := range request.Positions {
		exits = append(exits, domain.StrategyExit{
			DecisionID: request.CycleID + ":exit:" + position.LotID,
			LotID:      position.LotID, TokenID: position.TokenID, ReasonCode: domain.StrategyReasonHold48H,
			Order: &domain.StrategyOrderParams{
				Side: domain.SideSell, Type: domain.OrderTypeLimit, WorstPrice: "0.40",
				Size: position.Shares, TimeInForce: domain.TimeInForceFOK,
			},
		})
	}
	return domain.StrategyDecisionResponse{
		SchemaVersion: domain.StrategyOutputSchemaVersion, CycleID: request.CycleID, InputID: request.InputID,
		Context: request.Context, DecidedAt: request.DecisionAt.Add(time.Second),
		Evaluations: evaluations, Exits: exits,
	}, nil
}

func (strategy *exitStrategy) calledAccounts() []string {
	accounts := make([]string, 0, len(strategy.requests))
	for _, request := range strategy.requests {
		accounts = append(accounts, request.Context.ExecutionAccountID)
	}
	return accounts
}

// appendingRecorder queues the intents of every account's output, unlike
// fakeRecorder, which only queues the first output it sees.
type appendingRecorder struct {
	*fakeRecorder
}

func newAppendingRecorder(preloaded ...domain.DecisionIntentDelivery) appendingRecorder {
	return appendingRecorder{fakeRecorder: &fakeRecorder{deliveries: preloaded}}
}

func (recorder appendingRecorder) ClaimOutput(
	_ context.Context, response domain.StrategyDecisionResponse, intents []domain.OrderIntent, submissionEnabled bool,
) (domain.StrategyDecisionResponse, bool, error) {
	recorder.outputRecorded = true
	recorder.claimedOutputs = append(recorder.claimedOutputs, response)
	recorder.claimedIntents = append(recorder.claimedIntents, intents...)
	recorder.claimedSubmissionEnabled = append(recorder.claimedSubmissionEnabled, submissionEnabled)
	if submissionEnabled {
		for index, intent := range intents {
			recorder.deliveries = append(recorder.deliveries, domain.DecisionIntentDelivery{
				CycleID: response.CycleID, ClientOrderID: intent.ClientOrderID,
				Sequence: index, Intent: intent, Status: domain.DecisionIntentPending,
			})
		}
	}
	return response, true, nil
}

// scriptedExecutor acknowledges every intent except the scripted ones.
type scriptedExecutor struct {
	intents  []domain.OrderIntent
	statuses map[string]domain.OrderStatus // client_order_id -> status; "" = no durable order id
}

func (executor *scriptedExecutor) Submit(_ context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	executor.intents = append(executor.intents, intent)
	status, scripted := executor.statuses[intent.ClientOrderID]
	if !scripted {
		status = domain.OrderStatusAcknowledged
	}
	if status == "" {
		return port.OrderSubmitResult{}, errors.New("execution database unavailable")
	}
	return port.OrderSubmitResult{Order: domain.Order{ID: "order-" + intent.ClientOrderID, Intent: intent, Status: status}}, nil
}

func (executor *scriptedExecutor) submittedLots() map[string]domain.Side {
	lots := make(map[string]domain.Side)
	for _, intent := range executor.intents {
		lots[intent.TargetLotID] = intent.Side
	}
	return lots
}

func (executor *scriptedExecutor) buyCount() int {
	count := 0
	for _, intent := range executor.intents {
		if intent.Side == domain.SideBuy {
			count++
		}
	}
	return count
}

// failingPositionSource fails ListOpenLots for selected accounts.
type failingPositionSource struct {
	accountPositionSource
	fail map[string]bool
}

func (source failingPositionSource) ListOpenLots(ctx context.Context, binding domain.StrategyExecutionContext) ([]domain.PositionLot, error) {
	if source.fail[binding.ExecutionAccountID] {
		return nil, errors.New("read open lots: connection reset")
	}
	return source.accountPositionSource.ListOpenLots(ctx, binding)
}

type predictionSourceFunc func(ctx context.Context) (domain.PredictionSnapshot, error)

func (source predictionSourceFunc) Snapshot(ctx context.Context, _ time.Time, _ time.Duration) (domain.PredictionSnapshot, error) {
	return source(ctx)
}

type failingGate struct{ account string }

func (gate failingGate) AcquireExecutionAccount(_ context.Context, account string) (func(), error) {
	if account == gate.account {
		return nil, errors.New("account lock unavailable")
	}
	return func() {}, nil
}

type isolationFixture struct {
	params    Params
	strategy  *exitStrategy
	recorder  appendingRecorder
	executor  *scriptedExecutor
	snapshots *fakeOrderBookSnapshotRecorder
	books     *fakeOrderBookSource
}

func newIsolationFixture() *isolationFixture {
	fixture := &isolationFixture{
		strategy:  &exitStrategy{block: map[string]bool{}},
		recorder:  newAppendingRecorder(),
		executor:  &scriptedExecutor{statuses: map[string]domain.OrderStatus{}},
		snapshots: &fakeOrderBookSnapshotRecorder{},
		books:     &fakeOrderBookSource{books: polymarketBooks(isolationDecisionAt, validPrediction(isolationDecisionAt))},
	}
	fixture.params = Params{
		PredictionSource:             fakePredictionSource{snapshot: isolationSnapshot()},
		PositionSource:               isolationPositions(),
		OrderBookSource:              fixture.books,
		SnapshotRecorder:             fixture.snapshots,
		Strategy:                     fixture.strategy,
		Executor:                     fixture.executor,
		SubmitEnabled:                true,
		RequireCompleteModelCoverage: true,
		Bindings:                     isolationBindings(),
		Venue:                        "polymarket",
		Now:                          func() time.Time { return isolationDecisionAt.Add(20 * time.Second) },
	}
	return fixture
}

func (fixture *isolationFixture) run(t *testing.T, ctx context.Context) (RunResult, error) {
	t.Helper()
	fixture.params.Recorder = fixture.recorder
	service, err := newTestService(fixture.params)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service.Run(ctx, isolationDecisionAt)
}

func runFor(t *testing.T, result RunResult, account string) BindingRunResult {
	t.Helper()
	for _, run := range result.Runs {
		if run.Context.ExecutionAccountID == account {
			return run
		}
	}
	t.Fatalf("no run for %s in %#v", account, result.Runs)
	return BindingRunResult{}
}

func assertCalled(t *testing.T, strategy *exitStrategy, want ...string) {
	t.Helper()
	if got := strings.Join(strategy.calledAccounts(), ","); got != strings.Join(want, ",") {
		t.Fatalf("strategy called for %q, want %q", got, strings.Join(want, ","))
	}
}

func TestRunExcludesAccountWithBrokenInputAndOthersStillSell(t *testing.T) {
	for name, testCase := range map[string]struct {
		mutate func(*isolationFixture)
		broken string
		stage  string
	}{
		"lot without outcome index": {
			mutate: func(fixture *isolationFixture) {
				positions := isolationPositions()
				positions["wallet-6"][0].OutcomeIndex = nil
				fixture.params.PositionSource = positions
			},
			broken: "wallet-6", stage: FailureStagePositions,
		},
		"position read failure": {
			mutate: func(fixture *isolationFixture) {
				fixture.params.PositionSource = failingPositionSource{
					accountPositionSource: isolationPositions(), fail: map[string]bool{"wallet-6": true},
				}
			},
			broken: "wallet-6", stage: FailureStagePositions,
		},
		"Kalshi lot token does not match its condition": {
			mutate: func(fixture *isolationFixture) {
				positions := isolationPositions()
				positions[isolationKalshiAccount][0].TokenID = "kalshi:OTHER:YES"
				fixture.params.PositionSource = positions
			},
			broken: isolationKalshiAccount, stage: FailureStageTargets,
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newIsolationFixture()
			testCase.mutate(fixture)
			result, err := fixture.run(t, context.Background())
			if err == nil || !strings.Contains(err.Error(), testCase.broken) {
				t.Fatalf("Run() error = %v, want an error naming %s", err, testCase.broken)
			}
			healthy := make([]string, 0, 3)
			for _, binding := range []string{"main", "wallet-6", "wallet-7", isolationKalshiAccount} {
				if binding != testCase.broken {
					healthy = append(healthy, binding)
				}
			}
			assertCalled(t, fixture.strategy, healthy...)
			broken := runFor(t, result, testCase.broken)
			if !broken.InputUnavailable || broken.FailureStage != testCase.stage || broken.Error == nil || len(broken.Intents) != 0 {
				t.Fatalf("broken run = %#v, want input unavailable at %s", broken, testCase.stage)
			}
			for _, account := range healthy {
				if run := runFor(t, result, account); run.Error != nil || run.InputUnavailable {
					t.Fatalf("healthy run %s = %#v", account, run)
				}
			}
			lots := fixture.executor.submittedLots()
			if lots["lot-w7"] != domain.SideSell || lots["lot-main"] != domain.SideSell {
				t.Fatalf("submitted lots = %#v, want the healthy Polymarket exits", lots)
			}
			for _, intent := range fixture.executor.intents {
				if intent.ExecutionAccountID == testCase.broken {
					t.Fatalf("excluded account %s submitted %#v", testCase.broken, intent)
				}
			}
			if len(fixture.snapshots.batches) != 1 {
				t.Fatalf("snapshot batches = %d, want 1", len(fixture.snapshots.batches))
			}
		})
	}
}

func TestRunExcludesOnlyAccountsWithConflictingLotIdentity(t *testing.T) {
	t.Run("two lots disagree", func(t *testing.T) {
		fixture := newIsolationFixture()
		positions := isolationPositions()
		positions["wallet-6"] = append(positions["wallet-6"], isolationLot("wallet-6", "lot-w6-orphan", "market-a", "condition-a", "orphan-token", 0))
		positions["wallet-7"] = append(positions["wallet-7"], isolationLot("wallet-7", "lot-w7-orphan", "market-b", "condition-b", "orphan-token", 0))
		fixture.params.PositionSource = positions
		result, err := fixture.run(t, context.Background())
		if err == nil || !strings.Contains(err.Error(), "wallet-6") || !strings.Contains(err.Error(), "wallet-7") {
			t.Fatalf("Run() error = %v, want both conflicting accounts named", err)
		}
		assertCalled(t, fixture.strategy, "main", isolationKalshiAccount)
		for _, account := range []string{"wallet-6", "wallet-7"} {
			if run := runFor(t, result, account); !run.InputUnavailable || run.FailureStage != FailureStageTargetConflict {
				t.Fatalf("run %s = %#v, want a target conflict", account, run)
			}
		}
		for _, target := range fixture.books.targets {
			if target.TokenID == "orphan-token" {
				t.Fatalf("conflicting token %#v was captured", target)
			}
		}
	})
	t.Run("sell-only lot disagrees with the prediction", func(t *testing.T) {
		fixture := newIsolationFixture()
		fixture.params.EntryDisabledAccounts = []string{"main"}
		positions := isolationPositions()
		positions["main"][0].ConditionID = "condition-other"
		fixture.params.PositionSource = positions
		result, err := fixture.run(t, context.Background())
		if err == nil || !strings.Contains(err.Error(), "main") {
			t.Fatalf("Run() error = %v, want main named", err)
		}
		assertCalled(t, fixture.strategy, "wallet-6", "wallet-7", isolationKalshiAccount)
		if run := runFor(t, result, "main"); !run.InputUnavailable || run.FailureStage != FailureStageTargetConflict {
			t.Fatalf("main run = %#v", run)
		}
	})
}

func TestRunSharedTargetsMatchAccountUnion(t *testing.T) {
	fixture := newIsolationFixture()
	if _, err := fixture.run(t, context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// Two Polymarket outcomes, two Kalshi outcomes and the Kalshi lot token, in
	// condition/outcome order, exactly as the former global target build.
	var tokens []string
	for _, target := range fixture.books.targets {
		tokens = append(tokens, target.TokenID)
	}
	if got := strings.Join(tokens, ","); got != "yes-token,no-token,kalshi:KXLOT:YES,kalshi:KXTEST:YES,kalshi:KXTEST:NO" {
		t.Fatalf("targets = %s", got)
	}
}

// venueFailingBookSource returns ERROR books with the given code for one
// venue and the configured books for the other.
type venueFailingBookSource struct {
	fakeOrderBookSource
	venue domain.MarketSource
	code  string
}

func (source *venueFailingBookSource) Capture(ctx context.Context, decisionAt time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	books, _ := source.fakeOrderBookSource.Capture(ctx, decisionAt, targets)
	result := make([]domain.OrderBookSnapshot, 0, len(targets))
	for _, book := range books {
		if book.MarketSource.Normalize() != source.venue {
			result = append(result, book)
		}
	}
	for _, target := range targets {
		if target.MarketSource.Normalize() != source.venue {
			continue
		}
		result = append(result, domain.OrderBookSnapshot{
			MarketSource: target.MarketSource, MarketID: target.MarketID, ConditionID: target.ConditionID,
			OutcomeIndex: target.OutcomeIndex, OutcomeID: target.OutcomeID, TokenID: target.TokenID,
			Status: domain.OrderBookStatusError, ObservedAt: decisionAt, DepthLimit: domain.StrategyOrderBookDepth,
			Bids: []domain.PriceLevel{}, Asks: []domain.PriceLevel{}, ErrorCode: source.code,
		})
	}
	return result, nil
}

func TestRunDegradedVenueSkipsOnlyItsAccounts(t *testing.T) {
	t.Run("Kalshi capture failed", func(t *testing.T) {
		fixture := newIsolationFixture()
		fixture.params.OrderBookSource = &venueFailingBookSource{
			fakeOrderBookSource: *fixture.books, venue: domain.MarketSourceKalshi, code: domain.OrderBookErrorVenueCaptureFailed,
		}
		result, err := fixture.run(t, context.Background())
		if err == nil || !strings.Contains(err.Error(), isolationKalshiAccount) {
			t.Fatalf("Run() error = %v, want the Kalshi account named", err)
		}
		assertCalled(t, fixture.strategy, "main", "wallet-6", "wallet-7")
		if run := runFor(t, result, isolationKalshiAccount); !run.VenueBooksUnavailable || run.FailureStage != FailureStageOrderBooks {
			t.Fatalf("Kalshi run = %#v", run)
		}
		if len(result.DegradedVenues) != 1 || result.DegradedVenues[0] != domain.MarketSourceKalshi {
			t.Fatalf("degraded venues = %#v", result.DegradedVenues)
		}
		lots := fixture.executor.submittedLots()
		if lots["lot-main"] != domain.SideSell || lots["lot-w6"] != domain.SideSell || lots["lot-w7"] != domain.SideSell {
			t.Fatalf("submitted lots = %#v, want every Polymarket exit delivered", lots)
		}
		if len(fixture.snapshots.batches) != 1 || fixture.snapshots.batches[0].ErrorCount != 3 {
			t.Fatalf("batches = %#v, want one batch holding the three failed Kalshi books", fixture.snapshots.batches)
		}
	})
	t.Run("Polymarket capture failed", func(t *testing.T) {
		fixture := newIsolationFixture()
		fixture.params.OrderBookSource = &venueFailingBookSource{
			fakeOrderBookSource: *fixture.books, venue: domain.MarketSourcePolymarket, code: domain.OrderBookErrorVenueCaptureFailed,
		}
		result, err := fixture.run(t, context.Background())
		if err == nil || !strings.Contains(err.Error(), "wallet-6") {
			t.Fatalf("Run() error = %v, want the Polymarket accounts named", err)
		}
		assertCalled(t, fixture.strategy, isolationKalshiAccount)
		for _, account := range []string{"main", "wallet-6", "wallet-7"} {
			if run := runFor(t, result, account); !run.VenueBooksUnavailable {
				t.Fatalf("run %s = %#v", account, run)
			}
		}
		if run := runFor(t, result, isolationKalshiAccount); run.Error != nil || len(run.Intents) != 1 {
			t.Fatalf("Kalshi run = %#v, want its exit", run)
		}
	})
	t.Run("an ordinary ERROR book does not degrade the venue", func(t *testing.T) {
		fixture := newIsolationFixture()
		fixture.params.OrderBookSource = &venueFailingBookSource{
			fakeOrderBookSource: *fixture.books, venue: domain.MarketSourceKalshi, code: "KALSHI_MARKET_NOT_TRADABLE",
		}
		result, err := fixture.run(t, context.Background())
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		assertCalled(t, fixture.strategy, "main", "wallet-6", "wallet-7", isolationKalshiAccount)
		if len(result.DegradedVenues) != 0 {
			t.Fatalf("degraded venues = %#v", result.DegradedVenues)
		}
	})
}

type erroringOrderBookSource struct{ err error }

func (source erroringOrderBookSource) Capture(context.Context, time.Time, []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	return nil, source.err
}

// TestRunWithRealVenueRouterDegradesFailedKalshiCapture wires the production
// marketbooks router: a Kalshi capture error reaches the cycle as degraded
// Kalshi books and the Polymarket accounts still trade.
func TestRunWithRealVenueRouterDegradesFailedKalshiCapture(t *testing.T) {
	fixture := newIsolationFixture()
	router, err := marketbooks.New(marketbooks.Params{
		Polymarket: fixture.books, Kalshi: erroringOrderBookSource{err: errors.New("kalshi unreachable")},
		Now: func() time.Time { return isolationDecisionAt.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.params.OrderBookSource = router
	result, err := fixture.run(t, context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want the degraded Kalshi account reported")
	}
	assertCalled(t, fixture.strategy, "main", "wallet-6", "wallet-7")
	if run := runFor(t, result, isolationKalshiAccount); !run.VenueBooksUnavailable {
		t.Fatalf("Kalshi run = %#v", run)
	}
	if fixture.executor.submittedLots()["lot-w7"] != domain.SideSell {
		t.Fatalf("Polymarket exits were not delivered: %#v", fixture.executor.intents)
	}
}

func TestRunStrategyTimeoutFailsOnlyThatAccount(t *testing.T) {
	fixture := newIsolationFixture()
	fixture.strategy.block["wallet-6"] = true
	fixture.params.StrategyTimeout = 50 * time.Millisecond
	result, err := fixture.run(t, context.Background())
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wallet-6") {
		t.Fatalf("Run() error = %v, want wallet-6 strategy deadline", err)
	}
	assertCalled(t, fixture.strategy, "main", "wallet-6", "wallet-7", isolationKalshiAccount)
	run := runFor(t, result, "wallet-6")
	if !run.StrategyTimedOut || run.FailureStage != FailureStageStrategy || !errors.Is(run.Error, context.DeadlineExceeded) {
		t.Fatalf("wallet-6 run = %#v", run)
	}
	for _, account := range []string{"main", "wallet-7", isolationKalshiAccount} {
		if run := runFor(t, result, account); run.Error != nil {
			t.Fatalf("run %s = %#v", account, run)
		}
	}
	if fixture.executor.submittedLots()["lot-w7"] != domain.SideSell {
		t.Fatal("wallet-7 exit was not delivered after the wallet-6 timeout")
	}
}

func TestRunCycleDeadlineMarksRemainingAccountsNotRun(t *testing.T) {
	fixture := newIsolationFixture()
	fixture.strategy.block["wallet-6"] = true
	fixture.params.StrategyTimeout = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result, err := fixture.run(t, ctx)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want the cycle deadline", err)
	}
	assertCalled(t, fixture.strategy, "main", "wallet-6")
	if run := runFor(t, result, "wallet-6"); run.StrategyTimedOut || run.FailureStage != FailureStageStrategy {
		t.Fatalf("wallet-6 run = %#v, want a strategy failure that is not its own timeout", run)
	}
	for _, account := range []string{"wallet-7", isolationKalshiAccount} {
		run := runFor(t, result, account)
		if !run.NotRun || run.FailureStage != FailureStageCycleDeadline || !errors.Is(run.Error, context.DeadlineExceeded) {
			t.Fatalf("run %s = %#v, want not run", account, run)
		}
	}
}

func pendingRecoveryDelivery(account, clientOrderID string) domain.DecisionIntentDelivery {
	intent := domain.OrderIntent{
		ClientOrderID: clientOrderID, ExecutionAccountID: account, Side: domain.SideSell, TargetLotID: "old-lot",
	}
	return domain.DecisionIntentDelivery{
		CycleID: account + ":old-cycle", ClientOrderID: clientOrderID, Intent: intent, Status: domain.DecisionIntentPending,
	}
}

func TestRunRecoveryRejectedOrUnknownDeliveryDoesNotStopCycle(t *testing.T) {
	for _, status := range []domain.OrderStatus{domain.OrderStatusRejected, domain.OrderStatusUnknown} {
		t.Run(string(status), func(t *testing.T) {
			fixture := newIsolationFixture()
			fixture.recorder = newAppendingRecorder(pendingRecoveryDelivery("wallet-6", "old-client"))
			fixture.executor.statuses["old-client"] = status
			result, err := fixture.run(t, context.Background())
			if err == nil || !strings.Contains(err.Error(), "old-client") {
				t.Fatalf("Run() error = %v, want the recovered delivery reported", err)
			}
			assertCalled(t, fixture.strategy, "main", "wallet-6", "wallet-7", isolationKalshiAccount)
			wantRejected, wantUnknown := 1, 0
			if status == domain.OrderStatusUnknown {
				wantRejected, wantUnknown = 0, 1
			}
			if result.Recovery.Rejected != wantRejected || result.Recovery.Unknown != wantUnknown || result.Recovery.BusinessErr == nil {
				t.Fatalf("recovery = %#v", result.Recovery)
			}
			for _, run := range result.Runs {
				if run.Error != nil {
					t.Fatalf("run %s failed: %v", run.Context.ExecutionAccountID, run.Error)
				}
			}
			if fixture.executor.submittedLots()["lot-w6"] != domain.SideSell {
				t.Fatal("wallet-6 exit was not delivered after its rejected recovery")
			}
		})
	}
}

func TestRunRecoveryInfrastructureFailureStillStopsCycle(t *testing.T) {
	t.Run("no durable order id", func(t *testing.T) {
		fixture := newIsolationFixture()
		fixture.recorder = newAppendingRecorder(pendingRecoveryDelivery("wallet-6", "old-client"))
		fixture.executor.statuses["old-client"] = ""
		if _, err := fixture.run(t, context.Background()); err == nil || !strings.Contains(err.Error(), "recover pending strategy intents") {
			t.Fatalf("Run() error = %v, want recovery failure", err)
		}
		if len(fixture.strategy.requests) != 0 {
			t.Fatalf("strategy called %d times after a recovery infrastructure failure", len(fixture.strategy.requests))
		}
		if fixture.recorder.deliveries[0].Status != domain.DecisionIntentSubmitting {
			t.Fatalf("delivery = %#v, want it left leased", fixture.recorder.deliveries[0])
		}
	})
	t.Run("account lock unavailable", func(t *testing.T) {
		fixture := newIsolationFixture()
		fixture.params.AccountGate = failingGate{account: "wallet-6"}
		if _, err := fixture.run(t, context.Background()); err == nil || !strings.Contains(err.Error(), "account lock unavailable") {
			t.Fatalf("Run() error = %v, want lock failure", err)
		}
		if len(fixture.strategy.requests) != 0 {
			t.Fatalf("strategy called %d times after a lock failure", len(fixture.strategy.requests))
		}
	})
}

func TestRecoverStartupIgnoresTerminalRejectionButNotMissingOrder(t *testing.T) {
	newService := func(status domain.OrderStatus) (*Service, appendingRecorder) {
		recorder := newAppendingRecorder(pendingRecoveryDelivery("account-active", "old-client"))
		service := &Service{
			recorder: recorder, executor: &scriptedExecutor{statuses: map[string]domain.OrderStatus{"old-client": status}},
			submitEnabled:                   true,
			activeExecutionAccountIDs:       []string{"account-active"},
			entryEnabledExecutionAccountIDs: []string{"account-active"},
			now:                             func() time.Time { return isolationDecisionAt },
		}
		return service, recorder
	}
	for _, status := range []domain.OrderStatus{domain.OrderStatusRejected, domain.OrderStatusUnknown} {
		service, _ := newService(status)
		if err := service.RecoverStartup(context.Background()); err != nil {
			t.Fatalf("RecoverStartup() with %s delivery error = %v", status, err)
		}
	}
	service, recorder := newService("")
	if err := service.RecoverStartup(context.Background()); err == nil {
		t.Fatal("RecoverStartup() accepted a delivery without a durable order id")
	}
	if recorder.deliveries[0].Status != domain.DecisionIntentSubmitting {
		t.Fatalf("delivery = %#v, want it left leased", recorder.deliveries[0])
	}
}

func unavailablePredictionSource() port.PredictionSource {
	return predictionSourceFunc(func(context.Context) (domain.PredictionSnapshot, error) {
		return domain.PredictionSnapshot{}, fmt.Errorf("%w: prediction snapshot HTTP 503: UNAVAILABLE: down", port.ErrPredictionSourceUnavailable)
	})
}

func TestRunPredictionSourceUnavailableIsExitOnly(t *testing.T) {
	for name, source := range map[string]port.PredictionSource{
		"HTTP 503": unavailablePredictionSource(),
		"request exceeds its budget": predictionSourceFunc(func(ctx context.Context) (domain.PredictionSnapshot, error) {
			<-ctx.Done()
			return domain.PredictionSnapshot{}, ctx.Err()
		}),
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newIsolationFixture()
			fixture.params.PredictionSource = source
			fixture.params.PredictionTimeout = 50 * time.Millisecond
			result, err := fixture.run(t, context.Background())
			if err == nil || !errors.Is(err, port.ErrPredictionSourceUnavailable) {
				t.Fatalf("Run() error = %v, want the unavailable prediction source reported", err)
			}
			wantSnapshotID := "predsnap-unavailable-20261105T031000Z"
			if !result.PredictionSourceUnavailable || result.PredictionSnapshotID != wantSnapshotID {
				t.Fatalf("result = %#v", result)
			}
			assertCalled(t, fixture.strategy, "main", "wallet-6", "wallet-7", isolationKalshiAccount)
			for _, request := range fixture.strategy.requests {
				if len(request.Predictions) != 0 || request.PredictionSnapshotID != wantSnapshotID {
					t.Fatalf("request = %#v, want no predictions and the local snapshot id", request)
				}
			}
			for _, output := range fixture.recorder.claimedOutputs {
				if output.EntryPolicy == nil || output.EntryPolicy.Enabled ||
					output.EntryPolicy.BlockReason != domain.StrategyEntryBlockPredictionSourceUnavailable {
					t.Fatalf("output entry policy = %#v", output.EntryPolicy)
				}
			}
			for _, run := range result.Runs {
				if run.Error != nil || run.EntrySubmissionEnabled ||
					run.EntryBlockReason != domain.StrategyEntryBlockPredictionSourceUnavailable {
					t.Fatalf("run %s = %#v", run.Context.ExecutionAccountID, run)
				}
			}
			lots := fixture.executor.submittedLots()
			if lots["lot-w6"] != domain.SideSell || lots["lot-w7"] != domain.SideSell {
				t.Fatalf("submitted lots = %#v, want the 48h exits of wallet-6 and wallet-7", lots)
			}
			if fixture.executor.buyCount() != 0 {
				t.Fatalf("exit-only cycle submitted %d BUY orders", fixture.executor.buyCount())
			}
			if len(fixture.snapshots.batches) != 1 || fixture.snapshots.batches[0].PredictionSnapshotID != wantSnapshotID {
				t.Fatalf("batches = %#v", fixture.snapshots.batches)
			}
			for _, target := range fixture.books.targets {
				if target.TokenID == "kalshi:KXTEST:YES" {
					t.Fatal("an exit-only cycle captured a prediction-only token")
				}
			}
		})
	}
}

func TestRunPredictionHardErrorsStillFailCycle(t *testing.T) {
	invalid := isolationSnapshot()
	invalid.DecisionAt = isolationDecisionAt.Add(10 * time.Minute)
	for name, source := range map[string]port.PredictionSource{
		"HTTP 401": predictionSourceFunc(func(context.Context) (domain.PredictionSnapshot, error) {
			return domain.PredictionSnapshot{}, errors.New("prediction snapshot HTTP 401: UNAUTHORIZED: bad token")
		}),
		"invalid snapshot": fakePredictionSource{snapshot: invalid},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newIsolationFixture()
			fixture.params.PredictionSource = source
			result, err := fixture.run(t, context.Background())
			if err == nil || errors.Is(err, port.ErrPredictionSourceUnavailable) || result.PredictionSourceUnavailable {
				t.Fatalf("Run() = %#v, %v, want a hard failure", result, err)
			}
			if len(fixture.strategy.requests) != 0 || len(fixture.snapshots.batches) != 0 {
				t.Fatalf("strategy calls = %d, batches = %d, want none", len(fixture.strategy.requests), len(fixture.snapshots.batches))
			}
		})
	}
	t.Run("cycle context ended", func(t *testing.T) {
		fixture := newIsolationFixture()
		fixture.params.PredictionSource = predictionSourceFunc(func(ctx context.Context) (domain.PredictionSnapshot, error) {
			<-ctx.Done()
			return domain.PredictionSnapshot{}, ctx.Err()
		})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		result, err := fixture.run(t, ctx)
		if err == nil || result.PredictionSourceUnavailable || len(fixture.strategy.requests) != 0 {
			t.Fatalf("Run() = %#v, %v, want the cycle deadline to fail the cycle", result, err)
		}
	})
}

func TestRunPredictionUnavailableKeepsConfiguredSellOnlyReason(t *testing.T) {
	fixture := newIsolationFixture()
	fixture.params.PredictionSource = unavailablePredictionSource()
	fixture.params.EntryDisabledAccounts = []string{"main"}
	result, _ := fixture.run(t, context.Background())
	if reason := runFor(t, result, "main").EntryBlockReason; reason != domain.StrategyEntryBlockSubmissionDisabled {
		t.Fatalf("main entry block reason = %q", reason)
	}
	if reason := runFor(t, result, "wallet-6").EntryBlockReason; reason != domain.StrategyEntryBlockPredictionSourceUnavailable {
		t.Fatalf("wallet-6 entry block reason = %q", reason)
	}
}

// firstWriterSnapshotRecorder keeps the first batch of a decision_at, like
// the PostgreSQL recorder, and rejects a different one.
type firstWriterSnapshotRecorder struct {
	stored map[time.Time]domain.OrderBookSnapshotBatch
}

func (recorder *firstWriterSnapshotRecorder) ClaimBatch(_ context.Context, batch domain.OrderBookSnapshotBatch) (domain.OrderBookSnapshotBatch, bool, error) {
	if existing, exists := recorder.stored[batch.DecisionAt]; exists {
		if existing.SnapshotSetID != batch.SnapshotSetID || existing.PredictionSnapshotID != batch.PredictionSnapshotID {
			return domain.OrderBookSnapshotBatch{}, false, port.ErrOrderBookSnapshotConflict
		}
		return existing, false, nil
	}
	recorder.stored[batch.DecisionAt] = batch
	return batch, true, nil
}

// TestRunReplayAfterExitOnlyCycleNeverEntries replays the same boundary with
// predictions available again: the first-writer batch claim rejects it, so
// the boundary can never be upgraded to an entry cycle.
func TestRunReplayAfterExitOnlyCycleNeverEntries(t *testing.T) {
	fixture := newIsolationFixture()
	recorder := &firstWriterSnapshotRecorder{stored: map[time.Time]domain.OrderBookSnapshotBatch{}}
	fixture.params.SnapshotRecorder = recorder
	fixture.params.PredictionSource = unavailablePredictionSource()
	if _, err := fixture.run(t, context.Background()); !errors.Is(err, port.ErrPredictionSourceUnavailable) {
		t.Fatalf("first Run() error = %v", err)
	}
	calls := len(fixture.strategy.requests)
	fixture.params.PredictionSource = fakePredictionSource{snapshot: isolationSnapshot()}
	_, err := fixture.run(t, context.Background())
	if !errors.Is(err, port.ErrOrderBookSnapshotConflict) {
		t.Fatalf("replay Run() error = %v, want the snapshot batch conflict", err)
	}
	if len(fixture.strategy.requests) != calls || fixture.executor.buyCount() != 0 {
		t.Fatalf("replay called the strategy %d more times / submitted %d BUY orders",
			len(fixture.strategy.requests)-calls, fixture.executor.buyCount())
	}
}

func TestEntryBlockReasonPriority(t *testing.T) {
	coverage := errors.New("no coverage")
	cases := []struct {
		unavailable, disabled bool
		coverage              error
		want                  string
	}{
		{want: ""},
		{coverage: coverage, want: domain.StrategyEntryBlockIncompleteModelCoverage},
		{unavailable: true, coverage: coverage, want: domain.StrategyEntryBlockPredictionSourceUnavailable},
		{unavailable: true, disabled: true, coverage: coverage, want: domain.StrategyEntryBlockSubmissionDisabled},
	}
	for _, testCase := range cases {
		if got := entryBlockReason(testCase.unavailable, testCase.coverage, testCase.disabled); got != testCase.want {
			t.Fatalf("entryBlockReason(%v, %v, %v) = %q, want %q", testCase.unavailable, testCase.coverage, testCase.disabled, got, testCase.want)
		}
	}
}

func TestNewRejectsNegativeCallTimeouts(t *testing.T) {
	for _, params := range []Params{{StrategyTimeout: -time.Second}, {PredictionTimeout: -time.Second}} {
		fixture := newIsolationFixture()
		fixture.params.StrategyTimeout, fixture.params.PredictionTimeout = params.StrategyTimeout, params.PredictionTimeout
		fixture.params.Recorder = fixture.recorder
		if _, err := newTestService(fixture.params); err == nil {
			t.Fatalf("New() accepted negative timeouts %#v", params)
		}
	}
	fixture := newIsolationFixture()
	fixture.params.Recorder = fixture.recorder
	service, err := newTestService(fixture.params)
	if err != nil {
		t.Fatal(err)
	}
	if service.strategyTimeout != 90*time.Second || service.predictionTimeout != 30*time.Second {
		t.Fatalf("default timeouts = %s/%s, want 90s/30s", service.strategyTimeout, service.predictionTimeout)
	}
}

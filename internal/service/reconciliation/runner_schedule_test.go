package reconciliation

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestNextAlignedTick(t *testing.T) {
	interval := 5 * time.Minute
	offset := 3*time.Minute + 30*time.Second
	base := time.Date(2026, time.September, 30, 9, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		now    time.Time
		offset time.Duration
		want   time.Time
	}{
		{name: "arbitrary start before offset", now: base.Add(time.Minute + 12*time.Second), offset: offset, want: base.Add(offset)},
		{name: "arbitrary start after offset", now: base.Add(4*time.Minute + 1*time.Second), offset: offset, want: base.Add(8*time.Minute + 30*time.Second)},
		{name: "exactly at tick advances one interval", now: base.Add(offset), offset: offset, want: base.Add(8*time.Minute + 30*time.Second)},
		{name: "just before tick", now: base.Add(offset - time.Nanosecond), offset: offset, want: base.Add(offset)},
		{name: "crossing the hour", now: base.Add(58*time.Minute + 40*time.Second), offset: offset, want: base.Add(63*time.Minute + 30*time.Second)},
		{name: "zero offset on boundary", now: base, offset: 0, want: base.Add(interval)},
		{name: "zero offset mid interval", now: base.Add(2 * time.Minute), offset: 0, want: base.Add(interval)},
		{name: "non-UTC input is aligned on UTC", now: base.Add(time.Minute).In(time.FixedZone("CST", 8*3600)), offset: offset, want: base.Add(offset)},
	} {
		got := nextAlignedTick(test.now, interval, test.offset)
		if !got.Equal(test.want) || got.Location() != time.UTC {
			t.Fatalf("%s: nextAlignedTick(%s) = %s, want %s UTC", test.name, test.now, got, test.want)
		}
	}
}

func TestNewRunnerValidatesScheduleOffset(t *testing.T) {
	service := &fakeAccountReconciler{}
	for _, invalid := range []time.Duration{-time.Nanosecond, 5 * time.Minute, 6 * time.Minute} {
		offset := invalid
		if _, err := NewRunner(RunnerParams{
			Service: service, Accounts: []string{"wallet-6"}, Interval: 5 * time.Minute, ScheduleOffset: &offset,
		}); err == nil || !strings.Contains(err.Error(), "schedule offset") {
			t.Fatalf("NewRunner(offset %s) error = %v, want offset rejection", invalid, err)
		}
	}
	offset := 3*time.Minute + 30*time.Second
	runner, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"wallet-6"}, Interval: 5 * time.Minute, ScheduleOffset: &offset,
	})
	if err != nil {
		t.Fatal(err)
	}
	offset = 0
	if runner.scheduleOffset == nil || *runner.scheduleOffset != 3*time.Minute+30*time.Second {
		t.Fatalf("runner schedule offset = %v, want an independent copy of 3m30s", runner.scheduleOffset)
	}
	legacy, err := NewRunner(RunnerParams{Service: service, Accounts: []string{"wallet-6"}, Interval: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if legacy.scheduleOffset != nil {
		t.Fatal("unset schedule offset must keep the start-relative ticker")
	}
}

// manualAfter replaces time.After. Each call publishes the requested duration
// and the channel the test fires, so nothing waits on the wall clock.
type manualAfter struct {
	calls chan manualAfterCall
}

type manualAfterCall struct {
	duration time.Duration
	fire     chan time.Time
}

func newManualAfter() *manualAfter {
	return &manualAfter{calls: make(chan manualAfterCall, 16)}
}

func (after *manualAfter) After(duration time.Duration) <-chan time.Time {
	fire := make(chan time.Time, 1)
	after.calls <- manualAfterCall{duration: duration, fire: fire}
	return fire
}

func (after *manualAfter) next(t *testing.T) manualAfterCall {
	t.Helper()
	select {
	case call := <-after.calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("expected a timer request")
		return manualAfterCall{}
	}
}

func (after *manualAfter) expectNone(t *testing.T) {
	t.Helper()
	select {
	case call := <-after.calls:
		t.Fatalf("unexpected timer request for %s", call.duration)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRunnerAlignedScheduleTriggersAtOffsetAfterAnyStart(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.September, 30, 9, 1, 12, 0, time.UTC)}
	service := &fakeAccountReconciler{now: clock.Now}
	offset := 3*time.Minute + 30*time.Second
	runner, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"wallet-6"}, Interval: 5 * time.Minute,
		ScheduleOffset: &offset, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	schedule := newManualAfter()
	runner.scheduleAfter = schedule.After
	cancel, done := startReadinessTestLoop(t, runner)
	defer stopReadinessTestLoop(t, cancel, done)

	first := schedule.next(t)
	if first.duration != 2*time.Minute+18*time.Second {
		t.Fatalf("first aligned wait = %s, want 2m18s until 09:03:30", first.duration)
	}
	clock.Advance(first.duration)
	first.fire <- clock.Now()
	second := schedule.next(t)
	if second.duration != 5*time.Minute {
		t.Fatalf("second aligned wait = %s, want 5m until 09:08:30", second.duration)
	}
	waitForCalls(t, service, 1)
	if call := service.snapshotCalls()[0]; call.trigger != domain.ReconciliationTriggerScheduled || call.accountID != "wallet-6" {
		t.Fatalf("scheduled call = %#v", call)
	}
	// A late wake-up must not shift the phase: the next tick stays at x3:30.
	clock.Advance(second.duration + 40*time.Second)
	second.fire <- clock.Now()
	third := schedule.next(t)
	if third.duration != 4*time.Minute+20*time.Second {
		t.Fatalf("third aligned wait after a late wake = %s, want 4m20s until 09:13:30", third.duration)
	}
}

func TestRunnerWithoutOffsetKeepsStartRelativeTicker(t *testing.T) {
	clock := &runnerTestClock{now: time.Date(2026, time.September, 30, 9, 1, 12, 0, time.UTC)}
	runner := newReadinessTestRunner(t, clock, false)
	schedule := newManualAfter()
	runner.scheduleAfter = schedule.After
	cancel, done := startReadinessTestLoop(t, runner)
	defer stopReadinessTestLoop(t, cancel, done)
	schedule.expectNone(t)
}

type fakeDecisionActivity struct {
	mu    sync.Mutex
	busy  bool
	leads []time.Duration
}

func (activity *fakeDecisionActivity) DecisionBusy(_ time.Time, lead time.Duration) bool {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	activity.leads = append(activity.leads, lead)
	return activity.busy
}

func (activity *fakeDecisionActivity) setBusy(busy bool) {
	activity.mu.Lock()
	activity.busy = busy
	activity.mu.Unlock()
}

func (activity *fakeDecisionActivity) callCount() int {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	return len(activity.leads)
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *lockedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

type deferralFixture struct {
	runner   *Runner
	service  *fakeAccountReconciler
	clock    *runnerTestClock
	activity *fakeDecisionActivity
	poll     *manualAfter
	logs     *lockedBuffer
}

func newDeferralFixture(t *testing.T, busy, bind bool) deferralFixture {
	t.Helper()
	clock := &runnerTestClock{now: time.Date(2026, time.September, 30, 9, 0, 0, 0, time.UTC)}
	service := &fakeAccountReconciler{now: clock.Now}
	logs := &lockedBuffer{}
	runner, err := NewRunner(RunnerParams{
		Service: service, Accounts: []string{"wallet-6", "wallet-7"}, Interval: 5 * time.Minute,
		Now: clock.Now, Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	poll := newManualAfter()
	runner.deferAfter = poll.After
	activity := &fakeDecisionActivity{busy: busy}
	if bind {
		if err := runner.BindDecisionActivity(activity); err != nil {
			t.Fatal(err)
		}
	}
	return deferralFixture{runner: runner, service: service, clock: clock, activity: activity, poll: poll, logs: logs}
}

type yieldOutcome struct {
	request request
	err     error
}

func (fixture deferralFixture) yieldAsync(ctx context.Context, requested request, queue <-chan request) <-chan yieldOutcome {
	outcome := make(chan yieldOutcome, 1)
	go func() {
		result, err := fixture.runner.yieldToDecisionCycle(ctx, requested, queue)
		outcome <- yieldOutcome{request: result, err: err}
	}()
	return outcome
}

func awaitYield(t *testing.T, outcome <-chan yieldOutcome) yieldOutcome {
	t.Helper()
	select {
	case result := <-outcome:
		return result
	case <-time.After(time.Second):
		t.Fatal("yieldToDecisionCycle did not return")
		return yieldOutcome{}
	}
}

func scheduledRequest(accountID string) request {
	return request{accountID: accountID, trigger: domain.ReconciliationTriggerScheduled}
}

func TestScheduledReconciliationDefersWhileDecisionCycleBusyAndResumesAfterIt(t *testing.T) {
	fixture := newDeferralFixture(t, true, true)
	outcome := fixture.yieldAsync(context.Background(), scheduledRequest("wallet-6"), make(chan request))
	poll := fixture.poll.next(t)
	if poll.duration != decisionPollPeriod {
		t.Fatalf("deferral poll = %s, want %s", poll.duration, decisionPollPeriod)
	}
	// The account lock stays free while the scheduled run waits, so the
	// decision batch can take it immediately.
	release, err := fixture.runner.AcquireExecutionAccount(context.Background(), "wallet-6")
	if err != nil {
		t.Fatalf("AcquireExecutionAccount() during deferral error = %v", err)
	}
	release()

	fixture.clock.Advance(decisionPollPeriod)
	poll.fire <- fixture.clock.Now()
	poll = fixture.poll.next(t)
	fixture.activity.setBusy(false)
	fixture.clock.Advance(decisionPollPeriod)
	poll.fire <- fixture.clock.Now()
	result := awaitYield(t, outcome)
	if result.err != nil || result.request != scheduledRequest("wallet-6") {
		t.Fatalf("yield result = %#v", result)
	}
	for _, lead := range fixture.activity.leads {
		if lead != decisionLead {
			t.Fatalf("DecisionBusy lead = %s, want %s", lead, decisionLead)
		}
	}
	logs := fixture.logs.String()
	for _, want := range []string{
		"scheduled reconciliation deferred for decision cycle", "scheduled reconciliation resumed after decision cycle",
		"execution_account_id=wallet-6", "waited=4s",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "level=WARN") {
		t.Fatalf("resumed deferral must not warn:\n%s", logs)
	}
}

func TestScheduledReconciliationRunsAnywayAfterDeferralLimit(t *testing.T) {
	fixture := newDeferralFixture(t, true, true)
	outcome := fixture.yieldAsync(context.Background(), scheduledRequest("wallet-6"), make(chan request))
	poll := fixture.poll.next(t)
	fixture.clock.Advance(maxDecisionDefer - time.Second)
	poll.fire <- fixture.clock.Now()
	poll = fixture.poll.next(t)
	fixture.clock.Advance(time.Second)
	poll.fire <- fixture.clock.Now()
	result := awaitYield(t, outcome)
	if result.err != nil || result.request != scheduledRequest("wallet-6") {
		t.Fatalf("yield result = %#v", result)
	}
	logs := fixture.logs.String()
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "deferral limit exceeded") ||
		!strings.Contains(logs, "waited=3m0s") {
		t.Fatalf("logs missing deferral-limit warning:\n%s", logs)
	}
}

func TestNonScheduledAndUnboundReconciliationNeverDefer(t *testing.T) {
	fixture := newDeferralFixture(t, true, true)
	for _, trigger := range []domain.ReconciliationTrigger{
		domain.ReconciliationTriggerStartup, domain.ReconciliationTriggerOrderUnknown,
		domain.ReconciliationTriggerCancelUnknown, domain.ReconciliationTriggerAssetDrift,
	} {
		requested := request{accountID: "wallet-6", trigger: trigger, orderID: "order-1"}
		result := awaitYield(t, fixture.yieldAsync(context.Background(), requested, make(chan request)))
		if result.err != nil || result.request != requested {
			t.Fatalf("%s yield result = %#v", trigger, result)
		}
	}
	if fixture.activity.callCount() != 0 {
		t.Fatal("non-SCHEDULED triggers must not consult decision activity")
	}
	fixture.poll.expectNone(t)

	unbound := newDeferralFixture(t, true, false)
	result := awaitYield(t, unbound.yieldAsync(context.Background(), scheduledRequest("wallet-6"), make(chan request)))
	if result.err != nil || result.request != scheduledRequest("wallet-6") {
		t.Fatalf("unbound yield result = %#v", result)
	}
	unbound.poll.expectNone(t)

	idle := newDeferralFixture(t, false, true)
	result = awaitYield(t, idle.yieldAsync(context.Background(), scheduledRequest("wallet-6"), make(chan request)))
	if result.err != nil || result.request != scheduledRequest("wallet-6") {
		t.Fatalf("idle yield result = %#v", result)
	}
	idle.poll.expectNone(t)
}

func TestStartupSweepIgnoresBusyDecisionCycle(t *testing.T) {
	fixture := newDeferralFixture(t, true, true)
	sweep := fixture.runner.Sweep(context.Background(), domain.ReconciliationTriggerStartup)
	if len(sweep.Errors) != 0 || len(sweep.Runs) != 2 {
		t.Fatalf("startup sweep = %#v", sweep)
	}
	if fixture.activity.callCount() != 0 {
		t.Fatal("startup sweep must not consult decision activity")
	}
}

func TestDeferredScheduledReconciliationYieldsToImmediateTrigger(t *testing.T) {
	fixture := newDeferralFixture(t, true, true)
	queue := make(chan request, 1)
	outcome := fixture.yieldAsync(context.Background(), scheduledRequest("wallet-6"), queue)
	fixture.poll.next(t)
	immediate := request{accountID: "wallet-6", trigger: domain.ReconciliationTriggerOrderUnknown, orderID: "order-9"}
	queue <- immediate
	result := awaitYield(t, outcome)
	if result.err != nil || result.request != immediate {
		t.Fatalf("yield result = %#v, want the immediate trigger", result)
	}
}

func TestScheduledReconciliationDeferralRespectsCancellation(t *testing.T) {
	fixture := newDeferralFixture(t, true, true)
	ctx, cancel := context.WithCancel(context.Background())
	outcome := fixture.yieldAsync(ctx, scheduledRequest("wallet-6"), make(chan request))
	fixture.poll.next(t)
	cancel()
	if result := awaitYield(t, outcome); !errors.Is(result.err, context.Canceled) {
		t.Fatalf("yield error = %v, want context canceled", result.err)
	}
}

func TestBindDecisionActivityRejectsNilAndRebinding(t *testing.T) {
	fixture := newDeferralFixture(t, false, false)
	if err := fixture.runner.BindDecisionActivity(nil); err == nil {
		t.Fatal("BindDecisionActivity(nil) error = nil")
	}
	if err := fixture.runner.BindDecisionActivity(fixture.activity); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runner.BindDecisionActivity(fixture.activity); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("second BindDecisionActivity() error = %v", err)
	}
}

func TestRunnerLoopDefersScheduledRunOnlyForTheBusyWindow(t *testing.T) {
	fixture := newDeferralFixture(t, true, true)
	offset := time.Duration(0)
	fixture.runner.scheduleOffset = &offset
	schedule := newManualAfter()
	fixture.runner.scheduleAfter = schedule.After
	cancel, done := startReadinessTestLoop(t, fixture.runner)
	defer stopReadinessTestLoop(t, cancel, done)

	tick := schedule.next(t)
	fixture.clock.Advance(tick.duration)
	tick.fire <- fixture.clock.Now()
	// Both account workers defer independently.
	polls := []manualAfterCall{fixture.poll.next(t), fixture.poll.next(t)}
	if calls := fixture.service.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("scheduled reconciliation ran during the decision window: %#v", calls)
	}
	// An immediate trigger for wallet-7 replaces its deferred scheduled run.
	fixture.runner.Trigger("wallet-7", domain.ReconciliationTriggerOrderUnknown, "order-7")
	waitForCalls(t, fixture.service, 1)
	if call := fixture.service.snapshotCalls()[0]; call.accountID != "wallet-7" || call.trigger != domain.ReconciliationTriggerOrderUnknown {
		t.Fatalf("immediate call = %#v", call)
	}
	fixture.activity.setBusy(false)
	fixture.clock.Advance(decisionPollPeriod)
	for _, poll := range polls {
		poll.fire <- fixture.clock.Now()
	}
	waitForCalls(t, fixture.service, 2)
	calls := fixture.service.snapshotCalls()
	if calls[1].accountID != "wallet-6" || calls[1].trigger != domain.ReconciliationTriggerScheduled {
		t.Fatalf("resumed call = %#v", calls[1])
	}
}

func (reconciler *fakeAccountReconciler) snapshotCalls() []request {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	return append([]request(nil), reconciler.calls...)
}

func waitForCalls(t *testing.T, reconciler *fakeAccountReconciler, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if len(reconciler.snapshotCalls()) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reconciler calls = %d, want %d", len(reconciler.snapshotCalls()), want)
		}
		time.Sleep(time.Millisecond)
	}
}

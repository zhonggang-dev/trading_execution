package orderrecovery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var guardTestNow = time.Date(2026, time.September, 10, 9, 0, 0, 0, time.UTC)

type guardClock struct{ now time.Time }

func (clock *guardClock) Now() time.Time { return clock.now }

func newTestGuard(t *testing.T, store *memory.OrderRecoveryLeaseStore, clock *guardClock, policy Policy) *Guard {
	t.Helper()
	guard, err := NewGuard(GuardParams{Store: store, Policy: policy, Now: clock.Now, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("NewGuard() error = %v", err)
	}
	return guard
}

func recoveryOrder(id string, revision int64) domain.Order {
	return domain.Order{ID: id, Status: domain.OrderStatusUnknown, Revision: revision, Intent: domain.OrderIntent{ExecutionAccountID: "account-1", TokenID: "token-1"}}
}

func TestPolicyBackoffDoublesAndCaps(t *testing.T) {
	policy, err := Policy{BaseBackoff: 30 * time.Second, MaxBackoff: 4 * time.Minute}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 4 * time.Minute, 4 * time.Minute}
	for attempts, expected := range want {
		if got := policy.Backoff(attempts + 1); got != expected {
			t.Fatalf("Backoff(%d) = %s, want %s", attempts+1, got, expected)
		}
	}
	if _, err := (Policy{LeaseTTL: time.Second, Timeout: 5 * time.Second}).Normalize(); err == nil {
		t.Fatal("Normalize() accepted a lease shorter than the timeout")
	}
}

func TestGuardFailureBacksOffAndBlocksEarlyRetry(t *testing.T) {
	store := memory.NewOrderRecoveryLeaseStore()
	clock := &guardClock{now: guardTestNow}
	guard := newTestGuard(t, store, clock, Policy{BaseBackoff: 30 * time.Second})
	order := recoveryOrder("order-1", 3)
	venueDown := errors.New("clob 503")

	first := guard.Run(context.Background(), RunParams{Order: order, Holder: "worker-a", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeFailed, venueDown
	}})
	if first.Skipped || first.Kind != domain.OrderRecoveryOutcomeFailed || !errors.Is(first.Err, venueDown) || first.Lease.Attempts != 1 {
		t.Fatalf("first outcome = %#v", first)
	}
	if first.Lease.NextRetryAt == nil || !first.Lease.NextRetryAt.Equal(guardTestNow.Add(30*time.Second)) {
		t.Fatalf("next retry = %v, want +30s", first.Lease.NextRetryAt)
	}
	if first.Lease.FirstPendingAt == nil || !first.Lease.FirstPendingAt.Equal(guardTestNow) {
		t.Fatalf("first pending = %v, want %s", first.Lease.FirstPendingAt, guardTestNow)
	}

	clock.now = guardTestNow.Add(10 * time.Second)
	ran := false
	second := guard.Run(context.Background(), RunParams{Order: order, Holder: "worker-b", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		ran = true
		return domain.OrderRecoveryOutcomeResolved, nil
	}})
	if !second.Skipped || second.SkipReason != SkipReasonBackoff || ran {
		t.Fatalf("second outcome = %#v ran=%v, want backoff skip", second, ran)
	}

	focused := guard.Run(context.Background(), RunParams{Order: order, Holder: "worker-b", BypassBackoff: true, Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeFailed, venueDown
	}})
	if !focused.Skipped || focused.SkipReason != SkipReasonBackoff || focused.Lease.Attempts != 1 {
		t.Fatalf("focused trigger bypassed durable backoff: %#v", focused)
	}
	clock.now = guardTestNow.Add(30 * time.Second)
	retry := guard.Run(context.Background(), RunParams{Order: order, Holder: "worker-b", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeFailed, venueDown
	}})
	if retry.Lease.Attempts != 2 || !retry.Lease.NextRetryAt.Equal(clock.now.Add(time.Minute)) {
		t.Fatalf("retry=%#v", retry)
	}

	clock.now = clock.now.Add(2 * time.Minute)
	resolved := guard.Run(context.Background(), RunParams{Order: order, Holder: "worker-a", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeResolved, nil
	}})
	if !resolved.Resolved() {
		t.Fatalf("resolved outcome = %#v", resolved)
	}
	if _, exists := store.Lease(order.ID); exists {
		t.Fatal("resolved order still has a lease row")
	}
}

func TestGuardHeldLeaseIsNotStolenUntilExpiry(t *testing.T) {
	store := memory.NewOrderRecoveryLeaseStore()
	clock := &guardClock{now: guardTestNow}
	guard := newTestGuard(t, store, clock, Policy{LeaseTTL: 2 * time.Minute, Timeout: 30 * time.Second})
	order := recoveryOrder("order-1", 1)
	if _, err := store.AcquireOrderRecoveryLease(context.Background(), domain.OrderRecoveryLeaseRequest{
		OrderID: order.ID, ExecutionAccountID: "account-1", Holder: "coordinator", OrderRevision: 1, Now: guardTestNow, TTL: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	ran := false
	outcome := guard.Run(context.Background(), RunParams{Order: order, Holder: "reconciliation", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		ran = true
		return domain.OrderRecoveryOutcomeResolved, nil
	}})
	if !outcome.Skipped || outcome.SkipReason != SkipReasonHeld || ran || outcome.Lease.Holder != "coordinator" {
		t.Fatalf("outcome = %#v ran=%v, want held skip", outcome, ran)
	}
	clock.now = guardTestNow.Add(time.Minute + time.Second)
	outcome = guard.Run(context.Background(), RunParams{Order: order, Holder: "reconciliation", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeWaiting, nil
	}})
	if outcome.Skipped || outcome.Kind != domain.OrderRecoveryOutcomeWaiting || outcome.Lease.Attempts != 0 || outcome.Lease.Holder != "" {
		t.Fatalf("expired-lease takeover outcome = %#v", outcome)
	}
}

func TestGuardRejectsStaleSnapshotRevision(t *testing.T) {
	store := memory.NewOrderRecoveryLeaseStore()
	clock := &guardClock{now: guardTestNow}
	guard := newTestGuard(t, store, clock, Policy{})
	newer := guard.Run(context.Background(), RunParams{Order: recoveryOrder("order-1", 7), Holder: "a", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeWaiting, nil
	}})
	if newer.Skipped {
		t.Fatalf("newer outcome = %#v", newer)
	}
	stale := guard.Run(context.Background(), RunParams{Order: recoveryOrder("order-1", 6), Holder: "b", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		t.Fatal("stale snapshot must not act")
		return domain.OrderRecoveryOutcomeResolved, nil
	}})
	if !stale.Skipped || stale.SkipReason != SkipReasonStaleView {
		t.Fatalf("stale outcome = %#v", stale)
	}
}

func TestGuardTimeoutIsAFailureWithBackoff(t *testing.T) {
	store := memory.NewOrderRecoveryLeaseStore()
	clock := &guardClock{now: guardTestNow}
	guard := newTestGuard(t, store, clock, Policy{Timeout: time.Second, LeaseTTL: time.Minute})
	outcome := guard.Run(context.Background(), RunParams{Order: recoveryOrder("order-1", 1), Holder: "a", Work: func(ctx context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		<-ctx.Done()
		return domain.OrderRecoveryOutcomeWaiting, ctx.Err()
	}})
	if !outcome.TimedOut || outcome.Kind != domain.OrderRecoveryOutcomeFailed || outcome.Lease.Attempts != 1 || outcome.Lease.NextRetryAt == nil {
		t.Fatalf("timeout outcome = %#v", outcome)
	}
}

func TestGuardEscalatesAfterWindowWithoutResettingOnWaiting(t *testing.T) {
	store := memory.NewOrderRecoveryLeaseStore()
	clock := &guardClock{now: guardTestNow}
	guard := newTestGuard(t, store, clock, Policy{EscalateAfter: 10 * time.Minute})
	order := recoveryOrder("order-1", 1)
	wait := func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeWaiting, nil
	}
	first := guard.Run(context.Background(), RunParams{Order: order, Holder: "a", Work: wait})
	if first.Escalated || first.Lease.Attempts != 0 {
		t.Fatalf("first waiting outcome = %#v", first)
	}
	clock.now = guardTestNow.Add(9 * time.Minute)
	second := guard.Run(context.Background(), RunParams{Order: order, Holder: "a", Work: wait})
	if second.Escalated || second.Skipped {
		t.Fatalf("second waiting outcome = %#v", second)
	}
	clock.now = guardTestNow.Add(10 * time.Minute)
	third := guard.Run(context.Background(), RunParams{Order: order, Holder: "a", Work: wait})
	if !third.Escalated || third.Lease.EscalatedAt == nil || !third.Lease.EscalatedAt.Equal(clock.now) {
		t.Fatalf("third waiting outcome = %#v, want escalation", third)
	}
	// Escalated retries run only at the capped interval.
	clock.now = clock.now.Add(10 * time.Minute)
	fourth := guard.Run(context.Background(), RunParams{Order: order, Holder: "a", Work: wait})
	if !fourth.Escalated || !fourth.Lease.EscalatedAt.Equal(guardTestNow.Add(10*time.Minute)) {
		t.Fatalf("fourth waiting outcome = %#v, want sticky escalation", fourth)
	}
	clock.now = clock.now.Add(10 * time.Minute)
	resolved := guard.Run(context.Background(), RunParams{Order: order, Holder: "a", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
		return domain.OrderRecoveryOutcomeResolved, nil
	}})
	if !resolved.Resolved() || resolved.Escalated {
		t.Fatalf("resolved outcome = %#v", resolved)
	}
}

func TestSameWorkerLabelCannotReenterActiveRecovery(t *testing.T) {
	store := memory.NewOrderRecoveryLeaseStore()
	clock := &guardClock{now: guardTestNow}
	guard := newTestGuard(t, store, clock, Policy{})
	order := recoveryOrder("same-label", 1)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan Outcome, 1)
	go func() {
		done <- guard.Run(context.Background(), RunParams{Order: order, Holder: "coordinator", Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
			close(entered)
			<-release
			return domain.OrderRecoveryOutcomeWaiting, nil
		}})
	}()
	<-entered
	for i := 0; i < 100; i++ {
		outcome := guard.Run(context.Background(), RunParams{Order: order, Holder: "coordinator", BypassBackoff: true, Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
			t.Error("same label reentered lease")
			return domain.OrderRecoveryOutcomeWaiting, nil
		}})
		if !outcome.Skipped || outcome.SkipReason != SkipReasonHeld {
			t.Errorf("concurrent acquisition=%#v", outcome)
		}
	}
	close(release)
	outcome := <-done
	if outcome.StoreErr != nil || outcome.Lease.NextRetryAt == nil {
		t.Fatalf("release=%#v", outcome)
	}
	for i := 0; i < 100; i++ {
		outcome := guard.Run(context.Background(), RunParams{Order: order, Holder: "coordinator", BypassBackoff: true, Work: func(context.Context) (domain.OrderRecoveryOutcomeKind, error) {
			t.Error("focus storm bypassed waiting backoff")
			return domain.OrderRecoveryOutcomeWaiting, nil
		}})
		if !outcome.Skipped || outcome.SkipReason != SkipReasonBackoff {
			t.Errorf("waiting storm=%#v", outcome)
		}
	}
}

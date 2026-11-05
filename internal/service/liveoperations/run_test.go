package liveoperations

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// flakyBalanceSource fails its first calls, like a Polygon RPC blip during
// process startup, and then answers normally.
type flakyBalanceSource struct {
	remainingFailures atomic.Int32
	calls             atomic.Int32
	balance           domain.ExternalBalance
}

func (source *flakyBalanceSource) GetExternalBalance(context.Context, string, string) (domain.ExternalBalance, error) {
	source.calls.Add(1)
	if source.remainingFailures.Add(-1) >= 0 {
		return domain.ExternalBalance{}, errors.New("rpc unavailable")
	}
	return source.balance, nil
}

// TestRunKeepsRetryingAfterInitialRefreshFailure covers C-05: a failed first
// snapshot no longer stops the loop; the next tick builds it.
func TestRunKeepsRetryingAfterInitialRefreshFailure(t *testing.T) {
	now := time.Date(2026, 8, 19, 8, 0, 10, 0, time.UTC)
	service, _, _ := newTestService(t, &now)
	flaky := &flakyBalanceSource{balance: domain.ExternalBalance{Asset: "pUSD", Amount: "100", ObservedAt: now.Add(-time.Second)}}
	flaky.remainingFailures.Store(2)
	service.balanceSource = flaky
	// Shortened cadence for the test; production validates 1s..1m.
	service.interval = 20 * time.Millisecond
	service.refreshTimeout = 10 * time.Millisecond

	if err := service.Refresh(context.Background()); err == nil {
		t.Fatal("startup refresh must fail in this scenario")
	}
	if _, err := service.Snapshot(context.Background()); !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("Snapshot() before first success = %v, want ErrSnapshotUnavailable", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErrors := make(chan error, 1)
	go func() { runErrors <- service.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case err := <-runErrors:
			t.Fatalf("Run() stopped after a failed initial refresh: %v", err)
		case <-deadline:
			t.Fatalf("snapshot never became available; balance calls=%d", flaky.calls.Load())
		case <-time.After(5 * time.Millisecond):
		}
		if snapshot, err := service.Snapshot(context.Background()); err == nil {
			if snapshot.Capital.AvailableCash != "100" {
				t.Fatalf("recovered snapshot capital = %#v", snapshot.Capital)
			}
			break
		}
	}
	if flaky.calls.Load() < 3 {
		t.Fatalf("balance calls = %d, want the failed startup, a failed Run refresh, then a success", flaky.calls.Load())
	}
	cancel()
	if err := <-runErrors; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() after cancellation = %v", err)
	}
}

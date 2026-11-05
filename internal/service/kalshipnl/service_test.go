package kalshipnl

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
)

type sourceFunc func(context.Context, string) (kalshi.PnLSeries, error)

func (f sourceFunc) Series(ctx context.Context, r string) (kalshi.PnLSeries, error) { return f(ctx, r) }
func sample() kalshi.PnLSeries {
	return kalshi.PnLSeries{PeriodIntervalMinutes: 60, ProcessedThrough: time.Unix(100, 0), Points: []kalshi.PnLPoint{{Timestamp: 100, CumulativePnL: "1.25", PortfolioValue: "10"}}}
}
func service(t *testing.T, p Params) *Service {
	t.Helper()
	s, e := New(p)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	end := time.Now().Add(time.Second)
	for !f() {
		if time.Now().After(end) {
			t.Fatal("condition timed out")
		}
		runtime.Gosched()
	}
}

func TestAccountsIsolationCacheExpiryAndCopy(t *testing.T) {
	var calls atomic.Int32
	var now atomic.Int64
	now.Store(100)
	reader := sourceFunc(func(_ context.Context, r string) (kalshi.PnLSeries, error) { calls.Add(1); return sample(), nil })
	s := service(t, Params{Sources: map[string]Source{"kalshi:z": reader, "kalshi:a": reader}, Now: func() time.Time { return time.Unix(now.Load(), 0) }})
	list, e := s.ListAccounts(context.Background())
	if e != nil || !reflect.DeepEqual(list.Accounts, []Account{{"kalshi:a", "KALSHI"}, {"kalshi:z", "KALSHI"}}) {
		t.Fatalf("list=%+v err=%v", list, e)
	}
	if calls.Load() != 0 {
		t.Fatal("discovery contacted upstream")
	}
	q := Query{"kalshi:a", "1w"}
	r, e := s.Query(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	r.Points[0].CumulativePnL = "999"
	r, e = s.Query(context.Background(), q)
	if e != nil || calls.Load() != 1 || r.Points[0].CumulativePnL != "1.25" {
		t.Fatalf("cache=%+v err=%v calls=%d", r, e, calls.Load())
	}
	_, _ = s.Query(context.Background(), Query{"kalshi:z", "1w"})
	_, _ = s.Query(context.Background(), Query{"kalshi:a", "1d"})
	if calls.Load() != 3 {
		t.Fatal("account/range cache identities merged")
	}
	now.Store(160)
	_, _ = s.Query(context.Background(), q)
	if calls.Load() != 4 {
		t.Fatal("cache survived expiry")
	}
	for _, id := range []string{"main", "kalshi:unknown"} {
		if _, e := s.Query(context.Background(), Query{id, "1w"}); !errors.Is(e, ErrUnknownAccount) {
			t.Fatalf("unknown=%v", e)
		}
	}
	if _, e := s.Query(context.Background(), Query{"kalshi:a", "custom"}); e == nil {
		t.Fatal("invalid range accepted")
	}
}

func TestSharedReadCancellationDoesNotCancelOtherWaiters(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	s := service(t, Params{Sources: map[string]Source{"kalshi:a": sourceFunc(func(ctx context.Context, _ string) (kalshi.PnLSeries, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return sample(), nil
		case <-ctx.Done():
			return kalshi.PnLSeries{}, ctx.Err()
		}
	})}})
	q := Query{"kalshi:a", "1w"}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { _, e := s.Query(ctx, q); first <- e }()
	<-started
	go func() { _, e := s.Query(context.Background(), q); second <- e }()
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.inflight["kalshi:a|1w"].waiters == 2 })
	cancel()
	if e := <-first; !errors.Is(e, context.Canceled) {
		t.Fatalf("first=%v", e)
	}
	close(release)
	if e := <-second; e != nil {
		t.Fatalf("other waiter canceled: %v", e)
	}
	if calls.Load() != 1 {
		t.Fatal("shared source called twice")
	}
}

func TestAllWaitersCancelAndFailureIsNotCached(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	var calls atomic.Int32
	s := service(t, Params{Sources: map[string]Source{"kalshi:a": sourceFunc(func(ctx context.Context, _ string) (kalshi.PnLSeries, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			close(stopped)
			return kalshi.PnLSeries{}, ctx.Err()
		}
		return kalshi.PnLSeries{}, errors.New("secret upstream body")
	})}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := s.Query(ctx, Query{"kalshi:a", "1w"}); done <- e }()
	<-started
	cancel()
	<-done
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("unused upstream not canceled")
	}
	for i := 0; i < 2; i++ {
		_, e := s.Query(context.Background(), Query{"kalshi:a", "1w"})
		if e != ErrUnavailable {
			t.Fatalf("unsafe/incorrect error %v", e)
		}
	}
	if calls.Load() != 3 {
		t.Fatal("failure cached as success")
	}
}

func TestGlobalReadConcurrencyIsTwo(t *testing.T) {
	release := make(chan struct{})
	var active, max atomic.Int32
	f := sourceFunc(func(ctx context.Context, _ string) (kalshi.PnLSeries, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := max.Load()
			if n <= old || max.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-release:
			return sample(), nil
		case <-ctx.Done():
			return kalshi.PnLSeries{}, ctx.Err()
		}
	})
	s := service(t, Params{Sources: map[string]Source{"kalshi:a": f, "kalshi:b": f, "kalshi:c": f}})
	var wg sync.WaitGroup
	for _, id := range []string{"kalshi:a", "kalshi:b", "kalshi:c"} {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = s.Query(context.Background(), Query{id, "1w"}) }()
	}
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.inflight) == 3 && active.Load() == 2 })
	close(release)
	wg.Wait()
	if max.Load() != 2 {
		t.Fatalf("concurrency=%d", max.Load())
	}
}

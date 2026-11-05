// Package kalshipnl provides an independent, read-only official account curve.
package kalshipnl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var ErrUnknownAccount = errors.New("Kalshi PnL account is not managed")
var ErrUnavailable = errors.New("Kalshi PnL source unavailable")

type Source interface {
	Series(context.Context, string) (kalshi.PnLSeries, error)
}
type Query struct{ ExecutionAccountID, Range string }
type Account struct {
	ExecutionAccountID string `json:"execution_account_id"`
	MarketSource       string `json:"market_source"`
}
type AccountList struct {
	Accounts   []Account `json:"accounts"`
	ObservedAt time.Time `json:"observed_at"`
}
type Result struct {
	ExecutionAccountID    string            `json:"execution_account_id"`
	MarketSource          string            `json:"market_source"`
	Metric                string            `json:"metric"`
	Range                 string            `json:"range"`
	PeriodIntervalMinutes int               `json:"period_interval_minutes"`
	FetchedAt             time.Time         `json:"fetched_at"`
	ProcessedThrough      time.Time         `json:"processed_through"`
	Points                []kalshi.PnLPoint `json:"points"`
}
type Params struct {
	Sources map[string]Source
	TTL     time.Duration
	Now     func() time.Time
}
type cacheEntry struct {
	result    Result
	expiresAt time.Time
}
type pending struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	result  Result
	err     error
}
type Service struct {
	sources  map[string]Source
	ttl      time.Duration
	now      func() time.Time
	slots    chan struct{}
	mu       sync.Mutex
	cache    map[string]cacheEntry
	inflight map[string]*pending
}

func New(params Params) (*Service, error) {
	if params.TTL == 0 {
		params.TTL = time.Minute
	}
	if params.TTL <= 0 || params.TTL > time.Minute {
		return nil, fmt.Errorf("invalid Kalshi PnL cache TTL")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	sources := make(map[string]Source, len(params.Sources))
	for id, source := range params.Sources {
		if !domain.IsKalshiExecutionAccount(id) || source == nil {
			return nil, fmt.Errorf("invalid Kalshi PnL source binding")
		}
		sources[id] = source
	}
	return &Service{sources: sources, ttl: params.TTL, now: params.Now, slots: make(chan struct{}, 2), cache: map[string]cacheEntry{}, inflight: map[string]*pending{}}, nil
}

func (s *Service) ListAccounts(ctx context.Context) (AccountList, error) {
	if err := ctx.Err(); err != nil {
		return AccountList{}, err
	}
	ids := make([]string, 0, len(s.sources))
	for id := range s.sources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	accounts := make([]Account, 0, len(ids))
	for _, id := range ids {
		accounts = append(accounts, Account{id, "KALSHI"})
	}
	return AccountList{accounts, s.now().UTC()}, nil
}

func (s *Service) Query(ctx context.Context, q Query) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	source, ok := s.sources[q.ExecutionAccountID]
	if !ok {
		return Result{}, ErrUnknownAccount
	}
	if !kalshi.IsValidPnLRange(q.Range) {
		return Result{}, fmt.Errorf("invalid Kalshi PnL range")
	}
	key := q.ExecutionAccountID + "|" + q.Range
	s.mu.Lock()
	if entry, ok := s.cache[key]; ok && s.now().Before(entry.expiresAt) {
		s.mu.Unlock()
		return clone(entry.result), nil
	}
	call := s.inflight[key]
	if call == nil {
		shared, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		call = &pending{done: make(chan struct{}), cancel: cancel}
		s.inflight[key] = call
		go s.fetch(shared, call, source, q, key)
	}
	call.waiters++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.mu.Lock()
		call.waiters--
		if call.waiters == 0 {
			call.cancel()
			if s.inflight[key] == call {
				delete(s.inflight, key)
			}
		}
		s.mu.Unlock()
		return Result{}, ctx.Err()
	case <-call.done:
		s.mu.Lock()
		call.waiters--
		s.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		return clone(call.result), call.err
	}
}

func (s *Service) fetch(ctx context.Context, call *pending, source Source, q Query, key string) {
	defer call.cancel()
	var series kalshi.PnLSeries
	var err error
	select {
	case s.slots <- struct{}{}:
		series, err = source.Series(ctx, q.Range)
		<-s.slots
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		err = ErrUnavailable
	}
	result := Result{ExecutionAccountID: q.ExecutionAccountID, MarketSource: "KALSHI", Metric: "cumulative_pnl", Range: q.Range, PeriodIntervalMinutes: series.PeriodIntervalMinutes, FetchedAt: s.now().UTC(), ProcessedThrough: series.ProcessedThrough, Points: series.Points}
	if result.Points == nil {
		result.Points = []kalshi.PnLPoint{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[key] == call {
		delete(s.inflight, key)
	}
	call.result, call.err = result, err
	if err == nil {
		s.cache[key] = cacheEntry{clone(result), s.now().Add(s.ttl)}
		for k, e := range s.cache {
			if !s.now().Before(e.expiresAt) {
				delete(s.cache, k)
			}
		}
		for len(s.cache) > 128 {
			for k := range s.cache {
				delete(s.cache, k)
				break
			}
		}
	}
	close(call.done)
}

func clone(r Result) Result { r.Points = append([]kalshi.PnLPoint{}, r.Points...); return r }

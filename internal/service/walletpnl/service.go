package walletpnl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var (
	ErrUnknownAccount = errors.New("wallet PnL account is not managed")
	ErrUnavailable    = errors.New("wallet PnL source unavailable")
)

type AddressReader interface {
	WalletAddress(context.Context, string) (string, error)
}

type SeriesReader interface {
	Series(context.Context, string, string, string) (polymarketdata.PnLSeries, error)
}

type Query struct {
	ExecutionAccountID string
	Interval           string
	Fidelity           string
}

type Point struct {
	Timestamp int64          `json:"timestamp"`
	TradePnL  domain.Decimal `json:"trade_pnl"`
}

type Result struct {
	ExecutionAccountID string    `json:"execution_account_id"`
	WalletAddress      string    `json:"wallet_address"`
	Metric             string    `json:"metric"`
	Interval           string    `json:"interval"`
	Fidelity           string    `json:"fidelity"`
	FetchedAt          time.Time `json:"fetched_at"`
	Points             []Point   `json:"points"`
}

type Service struct {
	addresses AddressReader
	source    SeriesReader
	allowed   map[string]struct{}
	now       func() time.Time
	ttl       time.Duration
	mu        sync.Mutex
	cache     map[string]cacheEntry
	inflight  map[string]*pending
}

type Params struct {
	Addresses AddressReader
	Source    SeriesReader
	Accounts  []string
	TTL       time.Duration
	Now       func() time.Time
}

type cacheEntry struct {
	result    Result
	expiresAt time.Time
}
type pending struct {
	done   chan struct{}
	result Result
	err    error
}

func New(params Params) (*Service, error) {
	if params.Addresses == nil || params.Source == nil {
		return nil, fmt.Errorf("wallet PnL address and source readers are required")
	}
	allowed := make(map[string]struct{}, len(params.Accounts))
	for _, account := range params.Accounts {
		account = strings.TrimSpace(account)
		if account != "" {
			allowed[account] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf("wallet PnL managed accounts are required")
	}
	if params.TTL == 0 {
		params.TTL = 5 * time.Minute
	}
	if params.TTL < time.Minute || params.TTL > time.Hour {
		return nil, fmt.Errorf("wallet PnL cache TTL is invalid")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{addresses: params.Addresses, source: params.Source, allowed: allowed, now: params.Now, ttl: params.TTL, cache: make(map[string]cacheEntry), inflight: make(map[string]*pending)}, nil
}

func (service *Service) Query(ctx context.Context, query Query) (Result, error) {
	if _, ok := service.allowed[strings.TrimSpace(query.ExecutionAccountID)]; !ok {
		return Result{}, ErrUnknownAccount
	}
	if !validInterval(query.Interval) || !validFidelity(query.Fidelity) {
		return Result{}, fmt.Errorf("invalid wallet PnL query")
	}
	address, err := service.addresses.WalletAddress(ctx, query.ExecutionAccountID)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Result{}, err
		}
		if errors.Is(err, sql.ErrNoRows) {
			return Result{}, ErrUnknownAccount
		}
		return Result{}, fmt.Errorf("%w: read wallet address", ErrUnavailable)
	}
	key := strings.ToLower(address) + "|" + query.Interval + "|" + query.Fidelity
	now := service.now().UTC()
	service.mu.Lock()
	if entry, ok := service.cache[key]; ok && now.Before(entry.expiresAt) {
		service.mu.Unlock()
		return cloneResultFor(entry.result, query.ExecutionAccountID), nil
	}
	if call, ok := service.inflight[key]; ok {
		service.mu.Unlock()
		select {
		case <-call.done:
			return cloneResultFor(call.result, query.ExecutionAccountID), call.err
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	call := &pending{done: make(chan struct{})}
	service.inflight[key] = call
	service.mu.Unlock()
	series, fetchErr := service.source.Series(ctx, address, query.Interval, query.Fidelity)
	result := Result{ExecutionAccountID: query.ExecutionAccountID, WalletAddress: address, Metric: "trade_pnl", Interval: query.Interval, Fidelity: query.Fidelity, FetchedAt: service.now().UTC(), Points: make([]Point, 0, len(series.Points))}
	for _, point := range series.Points {
		result.Points = append(result.Points, Point{Timestamp: point.Timestamp, TradePnL: point.TradePnL})
	}
	if fetchErr != nil && !errors.Is(fetchErr, context.Canceled) && !errors.Is(fetchErr, context.DeadlineExceeded) {
		fetchErr = fmt.Errorf("%w: query Data API", ErrUnavailable)
	}
	service.mu.Lock()
	delete(service.inflight, key)
	call.result, call.err = result, fetchErr
	if fetchErr == nil {
		service.cache[key] = cacheEntry{result: cloneResult(result), expiresAt: service.now().UTC().Add(service.ttl)}
		service.trimCacheLocked()
	}
	close(call.done)
	service.mu.Unlock()
	return cloneResultFor(result, query.ExecutionAccountID), fetchErr
}

func (service *Service) trimCacheLocked() {
	if len(service.cache) <= 128 {
		return
	}
	for key, entry := range service.cache {
		if service.now().UTC().After(entry.expiresAt) {
			delete(service.cache, key)
		}
	}
	for len(service.cache) > 128 {
		for key := range service.cache {
			delete(service.cache, key)
			break
		}
	}
}
func cloneResult(result Result) Result {
	points := make([]Point, len(result.Points))
	copy(points, result.Points)
	result.Points = points
	return result
}

func cloneResultFor(result Result, accountID string) Result {
	result = cloneResult(result)
	result.ExecutionAccountID = accountID
	return result
}
func validInterval(value string) bool {
	switch value {
	case "1d", "1w", "1m", "all":
		return true
	}
	return false
}

func IsValidInterval(value string) bool { return validInterval(value) }
func validFidelity(value string) bool {
	switch value {
	case "1h", "3h", "12h", "18h", "1d":
		return true
	}
	return false
}

func IsValidFidelity(value string) bool { return validFidelity(value) }

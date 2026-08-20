// Package clobheartbeat maintains Polymarket's CLOB V2 dead-man switch and
// exposes a placement gate that never blocks cancellation or reconciliation.
package clobheartbeat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Client sends an authenticated heartbeat for an isolated execution account.
type Client interface {
	Heartbeat(ctx context.Context, executionAccountID, heartbeatID string) (string, error)
}

// Params configures the official five-second heartbeat cadence.
type Params struct {
	Client               Client
	Accounts             []string
	Interval             time.Duration
	StaleAfter           time.Duration
	CallTimeout          time.Duration
	StartupRetryDelay    time.Duration
	StartupRetryAttempts int
	Now                  func() time.Time
}

type accountState struct {
	heartbeatID string
	lastSuccess time.Time
	lastError   error
}

// Service serializes heartbeat ids per account and supplies live readiness.
type Service struct {
	client               Client
	accounts             []string
	interval             time.Duration
	staleAfter           time.Duration
	callTimeout          time.Duration
	startupRetryDelay    time.Duration
	startupRetryAttempts int
	now                  func() time.Time

	mu     sync.RWMutex
	states map[string]accountState
}

// New validates account isolation and constructs the heartbeat service.
func New(params Params) (*Service, error) {
	if params.Client == nil {
		return nil, fmt.Errorf("CLOB heartbeat client is required")
	}
	if params.Interval == 0 {
		params.Interval = 5 * time.Second
	}
	if params.StaleAfter == 0 {
		params.StaleAfter = 9 * time.Second
	}
	if params.CallTimeout == 0 {
		params.CallTimeout = 3 * time.Second
	}
	if params.StartupRetryDelay == 0 {
		params.StartupRetryDelay = params.Interval
	}
	if params.StartupRetryAttempts == 0 {
		params.StartupRetryAttempts = 5
	}
	if params.Interval < time.Second || params.Interval > 5*time.Second ||
		params.StaleAfter <= params.Interval || params.StaleAfter >= 10*time.Second ||
		params.CallTimeout <= 0 || params.CallTimeout >= params.StaleAfter ||
		params.StartupRetryDelay <= 0 || params.StartupRetryDelay > 5*time.Second ||
		params.StartupRetryAttempts < 1 || params.StartupRetryAttempts > 6 {
		return nil, fmt.Errorf("invalid CLOB heartbeat timing")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	seen := make(map[string]struct{})
	accounts := make([]string, 0, len(params.Accounts))
	states := make(map[string]accountState, len(params.Accounts))
	for _, raw := range params.Accounts {
		account := strings.TrimSpace(raw)
		if account == "" {
			return nil, fmt.Errorf("CLOB heartbeat account id is empty")
		}
		if _, exists := seen[account]; exists {
			continue
		}
		seen[account] = struct{}{}
		accounts = append(accounts, account)
		states[account] = accountState{lastError: errors.New("initial heartbeat has not completed")}
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("at least one CLOB heartbeat account is required")
	}
	return &Service{
		client: params.Client, accounts: accounts, interval: params.Interval,
		staleAfter: params.StaleAfter, callTimeout: params.CallTimeout,
		startupRetryDelay:    params.StartupRetryDelay,
		startupRetryAttempts: params.StartupRetryAttempts, now: params.Now,
		states: states,
	}, nil
}

// Start performs the mandatory initial heartbeat synchronously. The HTTP
// listener must not start if this call fails.
func (service *Service) Start(ctx context.Context) error {
	var lastError error
	for attempt := 1; attempt <= service.startupRetryAttempts; attempt++ {
		lastError = service.beatAll(ctx)
		if lastError == nil {
			return nil
		}
		if !onlyInvalidHeartbeatID(lastError) || attempt == service.startupRetryAttempts {
			return lastError
		}
		timer := time.NewTimer(service.startupRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastError
}

// onlyInvalidHeartbeatID recognizes the short server-side overlap window in
// which a stopped process's heartbeat session is still alive. Authentication,
// transport, and all other CLOB errors continue to fail startup immediately.
func onlyInvalidHeartbeatID(err error) bool {
	if err == nil {
		return false
	}
	type multiUnwrapper interface{ Unwrap() []error }
	if joined, ok := err.(multiUnwrapper); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyInvalidHeartbeatID(child) {
				return false
			}
		}
		return true
	}
	if child := errors.Unwrap(err); child != nil {
		return onlyInvalidHeartbeatID(child)
	}
	return strings.Contains(strings.ToLower(err.Error()), "invalid heartbeat id")
}

// Run maintains the session until shutdown. A failure immediately closes the
// placement gate; subsequent ticks attempt to recover with a fresh handshake.
func (service *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = service.beatAll(ctx)
		}
	}
}

// beatAll keeps the heartbeat deadline independent of account count. A
// sequential loop can make later wallets stale when one earlier CLOB request
// consumes most of its timeout, so each account gets its own bounded call in
// the same cadence window.
func (service *Service) beatAll(ctx context.Context) error {
	type result struct {
		account string
		err     error
	}
	results := make(chan result, len(service.accounts))
	var calls sync.WaitGroup
	calls.Add(len(service.accounts))
	for _, account := range service.accounts {
		account := account
		go func() {
			defer calls.Done()
			results <- result{account: account, err: service.beat(ctx, account)}
		}()
	}
	calls.Wait()
	close(results)
	var failures []error
	for completed := range results {
		if completed.err != nil {
			failures = append(failures, fmt.Errorf("heartbeat %s: %w", completed.account, completed.err))
		}
	}
	return errors.Join(failures...)
}

func (service *Service) beat(ctx context.Context, account string) error {
	service.mu.RLock()
	heartbeatID := service.states[account].heartbeatID
	service.mu.RUnlock()
	callContext, cancel := context.WithTimeout(ctx, service.callTimeout)
	defer cancel()
	nextID, err := service.client.Heartbeat(callContext, account, heartbeatID)
	now := service.now().UTC()
	service.mu.Lock()
	state := service.states[account]
	if err != nil {
		state.heartbeatID = ""
		state.lastError = err
	} else {
		state.heartbeatID = nextID
		state.lastSuccess = now
		state.lastError = nil
	}
	service.states[account] = state
	service.mu.Unlock()
	return err
}

// CheckAccount fails closed when the account is unknown, the latest call
// failed, or the last successful heartbeat is too old.
func (service *Service) CheckAccount(_ context.Context, executionAccountID string) error {
	account := strings.TrimSpace(executionAccountID)
	service.mu.RLock()
	state, exists := service.states[account]
	service.mu.RUnlock()
	if !exists {
		return fmt.Errorf("execution account %q has no heartbeat session", account)
	}
	if state.lastError != nil {
		return fmt.Errorf("CLOB heartbeat unhealthy: %w", state.lastError)
	}
	if state.lastSuccess.IsZero() || service.now().UTC().Sub(state.lastSuccess) >= service.staleAfter {
		return fmt.Errorf("CLOB heartbeat is stale")
	}
	return nil
}

// Check implements live readiness for all configured execution accounts.
func (service *Service) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, accountID := range service.accounts {
		if err := service.CheckAccount(ctx, accountID); err != nil {
			return fmt.Errorf("execution account %q: %w", accountID, err)
		}
	}
	return nil
}

// Venue hard-gates only Place on heartbeat health. Cancel and Get remain
// available so risk can always be reduced after the dead-man switch degrades.
type Venue struct {
	venue  port.Venue
	status *Service
}

func NewVenue(venue port.Venue, status *Service) (*Venue, error) {
	if venue == nil || status == nil {
		return nil, fmt.Errorf("venue and CLOB heartbeat status are required")
	}
	return &Venue{venue: venue, status: status}, nil
}

func (venue *Venue) Name() string { return venue.venue.Name() }

func (venue *Venue) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	if err := venue.checkPlace(ctx, order); err != nil {
		return port.VenueOrder{}, err
	}
	return venue.venue.Place(ctx, order)
}

func (venue *Venue) checkPlace(ctx context.Context, order domain.Order) error {
	if err := venue.status.CheckAccount(ctx, order.Intent.ExecutionAccountID); err != nil {
		return &port.VenueError{
			Kind: port.VenueErrorRejected, Code: "CLOB_HEARTBEAT_UNHEALTHY",
			Message: "new order rejected locally because the CLOB heartbeat is not healthy", Cause: err,
		}
	}
	return nil
}

type heartbeatPrepared struct{ inner port.PreparedPlacement }

func (prepared heartbeatPrepared) ExpectedVenueOrderID() string {
	if prepared.inner == nil {
		return ""
	}
	return prepared.inner.ExpectedVenueOrderID()
}

func (venue *Venue) PreparePlace(ctx context.Context, order domain.Order) (port.PreparedPlacement, error) {
	if err := venue.checkPlace(ctx, order); err != nil {
		return nil, err
	}
	underlying, ok := venue.venue.(port.PreparedVenue)
	if !ok {
		return nil, &port.VenueError{Kind: port.VenueErrorInvalid, Code: "CLOB_PREPARED_PLACEMENT_UNSUPPORTED", Message: "underlying live venue does not support prepared placement"}
	}
	prepared, err := underlying.PreparePlace(ctx, order)
	if err != nil {
		return nil, err
	}
	return heartbeatPrepared{inner: prepared}, nil
}

func (venue *Venue) PlacePrepared(ctx context.Context, order domain.Order, placement port.PreparedPlacement) (port.VenueOrder, error) {
	prepared, ok := placement.(heartbeatPrepared)
	underlying, supported := venue.venue.(port.PreparedVenue)
	if !ok || !supported || prepared.inner == nil {
		return port.VenueOrder{}, &port.VenueError{Kind: port.VenueErrorInvalid, Code: "CLOB_PREPARED_PLACEMENT_INVALID", Message: "heartbeat prepared placement is invalid"}
	}
	if err := venue.checkPlace(ctx, order); err != nil {
		return port.VenueOrder{}, err
	}
	return underlying.PlacePrepared(ctx, order, prepared.inner)
}

func (venue *Venue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Cancel(ctx, order)
}

func (venue *Venue) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Get(ctx, order)
}

var _ port.Venue = (*Venue)(nil)
var _ port.PreparedVenue = (*Venue)(nil)

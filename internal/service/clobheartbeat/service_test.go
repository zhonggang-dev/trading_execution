package clobheartbeat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestStartEstablishesAccountHeartbeat(t *testing.T) {
	now := time.Date(2026, 8, 19, 3, 0, 0, 0, time.UTC)
	client := &fakeClient{}
	service, err := New(Params{Client: client, Accounts: []string{"account-1"}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := service.CheckAccount(context.Background(), "account-1"); err != nil {
		t.Fatalf("CheckAccount() error = %v", err)
	}
	if len(client.ids) != 1 || client.ids[0] != "" {
		t.Fatalf("heartbeat ids = %#v", client.ids)
	}
}

func TestStartHeartbeatsAccountsConcurrently(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 2)
	client := &fakeClient{started: started, release: release}
	service, err := New(Params{
		Client: client, Accounts: []string{"account-1", "account-2"},
		CallTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.Start(context.Background()) }()

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case account := <-started:
			seen[account] = true
		case <-time.After(250 * time.Millisecond):
			t.Fatal("second account did not start before the first was released")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Start() error = %v", err)
	}
}

func TestHeartbeatVenueBlocksOnlyPlaceWhenUnhealthy(t *testing.T) {
	service, err := New(Params{Client: &fakeClient{err: errors.New("offline")}, Accounts: []string{"account-1"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = service.Start(context.Background())
	underlying := &fakeVenue{}
	venue, err := NewVenue(underlying, service)
	if err != nil {
		t.Fatal(err)
	}
	order := domain.Order{Intent: domain.OrderIntent{ExecutionAccountID: "account-1"}}
	_, placeErr := venue.Place(context.Background(), order)
	var venueError *port.VenueError
	if !errors.As(placeErr, &venueError) || venueError.Code != "CLOB_HEARTBEAT_UNHEALTHY" {
		t.Fatalf("Place() error = %#v", placeErr)
	}
	_, _ = venue.Cancel(context.Background(), order)
	_, _ = venue.Get(context.Background(), order)
	if underlying.placeCalls != 0 || underlying.cancelCalls != 1 || underlying.getCalls != 1 {
		t.Fatalf("calls = place %d cancel %d get %d", underlying.placeCalls, underlying.cancelCalls, underlying.getCalls)
	}
}

type fakeClient struct {
	mu      sync.Mutex
	ids     []string
	err     error
	started chan<- string
	release <-chan struct{}
}

func (client *fakeClient) Heartbeat(ctx context.Context, account, id string) (string, error) {
	if client.started != nil {
		client.started <- account
	}
	if client.release != nil {
		select {
		case <-client.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.ids = append(client.ids, id)
	if client.err != nil {
		return "", client.err
	}
	return "next-id", nil
}

type fakeVenue struct {
	placeCalls  int
	cancelCalls int
	getCalls    int
}

func (venue *fakeVenue) Name() string { return "polymarket" }
func (venue *fakeVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.placeCalls++
	return port.VenueOrder{}, nil
}
func (venue *fakeVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.cancelCalls++
	return port.VenueOrder{}, nil
}
func (venue *fakeVenue) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.getCalls++
	return port.VenueOrder{}, nil
}

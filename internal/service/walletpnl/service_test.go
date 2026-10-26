package walletpnl

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type fakeAddresses struct {
	address string
	err     error
	byID    map[string]string
}

func (fake fakeAddresses) WalletAddress(_ context.Context, accountID string) (string, error) {
	if fake.byID != nil {
		return fake.byID[accountID], fake.err
	}
	return fake.address, fake.err
}

type fakeSource struct {
	mu    sync.Mutex
	calls int
}

func (fake *fakeSource) Series(context.Context, string, string, string) (polymarketdata.PnLSeries, error) {
	fake.mu.Lock()
	fake.calls++
	fake.mu.Unlock()
	return polymarketdata.PnLSeries{Points: []polymarketdata.PnLPoint{{Timestamp: 1, TradePnL: domain.Decimal("0")}}}, nil
}

func TestServiceRejectsUnmanagedAccount(t *testing.T) {
	source := &fakeSource{}
	service, err := New(Params{Addresses: fakeAddresses{address: "0x1111111111111111111111111111111111111111"}, Source: source, Accounts: []string{"main"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Query(context.Background(), Query{ExecutionAccountID: "wallet-9", Interval: "1w", Fidelity: "3h"}); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("error = %v", err)
	}
}

func TestServiceCoalescesAndCachesRequests(t *testing.T) {
	source := &fakeSource{}
	service, err := New(Params{Addresses: fakeAddresses{address: "0x1111111111111111111111111111111111111111"}, Source: source, Accounts: []string{"main"}, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	query := Query{ExecutionAccountID: "main", Interval: "1w", Fidelity: "3h"}
	if _, err := service.Query(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Query(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	calls := source.calls
	source.mu.Unlock()
	if calls != 1 {
		t.Fatalf("source calls = %d, want 1", calls)
	}
}

func TestServiceRebindsAccountIDOnSharedAddressCache(t *testing.T) {
	source := &fakeSource{}
	service, err := New(Params{
		Addresses: fakeAddresses{byID: map[string]string{
			"main":     "0x1111111111111111111111111111111111111111",
			"wallet-1": "0x1111111111111111111111111111111111111111",
		}},
		Source: source, Accounts: []string{"main", "wallet-1"}, TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, accountID := range []string{"main", "wallet-1"} {
		result, queryErr := service.Query(context.Background(), Query{ExecutionAccountID: accountID, Interval: "1w", Fidelity: "3h"})
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		if result.ExecutionAccountID != accountID {
			t.Fatalf("result account = %q, want %q", result.ExecutionAccountID, accountID)
		}
	}
}

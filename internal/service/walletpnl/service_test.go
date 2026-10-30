package walletpnl

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
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

type discoveryAddresses struct {
	values map[string]string
	err    error
	reads  []string
}

func (reader *discoveryAddresses) WalletAddress(_ context.Context, accountID string) (string, error) {
	reader.reads = append(reader.reads, accountID)
	if reader.err != nil {
		return "", reader.err
	}
	address, ok := reader.values[accountID]
	if !ok {
		return "", sql.ErrNoRows
	}
	return address, nil
}

func TestListAccountsReadsManagedRegistrationsWithoutExternalData(t *testing.T) {
	addresses := &discoveryAddresses{values: map[string]string{
		"portfolio-b": "0x2222222222222222222222222222222222222222",
		"portfolio-a": "0x1111111111111111111111111111111111111111",
		"unmanaged":   "0x9999999999999999999999999999999999999999",
	}}
	source := &fakeSource{}
	now := time.Date(2026, 10, 30, 16, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	service, err := New(Params{Addresses: addresses, Source: source,
		Accounts: []string{"portfolio-b", "portfolio-a", "portfolio-b"}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.ListAccounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Account{{ExecutionAccountID: "portfolio-a", MarketSource: "POLYMARKET"}, {ExecutionAccountID: "portfolio-b", MarketSource: "POLYMARKET"}}
	if !reflect.DeepEqual(result.Accounts, want) || !result.ObservedAt.Equal(now) || result.ObservedAt.Location() != time.UTC {
		t.Fatalf("account list = %#v", result)
	}
	if !reflect.DeepEqual(addresses.reads, []string{"portfolio-a", "portfolio-b"}) {
		t.Fatalf("registration reads = %v", addresses.reads)
	}
	if source.calls != 0 {
		t.Fatalf("external PnL calls = %d, want 0", source.calls)
	}
}

func TestListAccountsFailsInsteadOfReturningPartialOrEmptySupport(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "missing registration", want: ErrUnavailable},
		{name: "invalid address", err: errors.New("execution account wallet address is invalid"), want: ErrUnavailable},
		{name: "database unavailable", err: errors.New("database read failed"), want: ErrUnavailable},
		{name: "deadline", err: context.DeadlineExceeded, want: context.DeadlineExceeded},
		{name: "canceled", err: context.Canceled, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			addresses := &discoveryAddresses{values: map[string]string{"a": "0x1111111111111111111111111111111111111111"}, err: test.err}
			source := &fakeSource{}
			service, err := New(Params{Addresses: addresses, Source: source, Accounts: []string{"a", "b"}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := service.ListAccounts(context.Background())
			if !errors.Is(err, test.want) || result.Accounts != nil || !result.ObservedAt.IsZero() {
				t.Fatalf("result=%#v error=%v, want %v and no partial list", result, err, test.want)
			}
			if source.calls != 0 {
				t.Fatalf("external PnL calls = %d", source.calls)
			}
		})
	}
}

func TestListAccountsHonorsAlreadyCanceledContext(t *testing.T) {
	addresses := &discoveryAddresses{}
	service, err := New(Params{Addresses: addresses, Source: &fakeSource{}, Accounts: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.ListAccounts(ctx); !errors.Is(err, context.Canceled) || len(addresses.reads) != 0 {
		t.Fatalf("error=%v reads=%v", err, addresses.reads)
	}
}

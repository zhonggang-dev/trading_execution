package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshisettlement"
)

// fakeKalshiExchange is the exchange side of one Kalshi account: its cash
// balance and the settlement records it has published.
type fakeKalshiExchange struct {
	balance     domain.Decimal
	settlements map[string][]domain.KalshiSettlement
	listErr     error
	allReads    int
	// positions is the exchange's position list; nil reads as no holdings.
	positions    []domain.KalshiMarketPosition
	positionsErr error
	positionRead int
}

func (exchange *fakeKalshiExchange) ListPositions(context.Context) ([]domain.KalshiMarketPosition, error) {
	exchange.positionRead++
	if exchange.positionsErr != nil {
		return nil, exchange.positionsErr
	}
	return append([]domain.KalshiMarketPosition{}, exchange.positions...), nil
}

func (exchange *fakeKalshiExchange) ListSettlements(_ context.Context, ticker string) ([]domain.KalshiSettlement, error) {
	if exchange.listErr != nil {
		return nil, exchange.listErr
	}
	return exchange.settlements[ticker], nil
}

func (exchange *fakeKalshiExchange) AvailableBalance(context.Context) (domain.Decimal, error) {
	return exchange.balance, nil
}

func (exchange *fakeKalshiExchange) sync(ctx context.Context, database *sql.DB, accountID string) (bool, domain.Decimal, error) {
	return SyncKalshiExecutionAccount(ctx, database, accountID, "key-"+accountID,
		func() (domain.Decimal, error) { return exchange.balance, nil },
		func() ([]domain.KalshiSettlement, error) {
			exchange.allReads++
			if exchange.listErr != nil {
				return nil, exchange.listErr
			}
			all := []domain.KalshiSettlement{}
			for _, records := range exchange.settlements {
				all = append(all, records...)
			}
			return all, nil
		})
}

func newKalshiSettlementService(t *testing.T, database *sql.DB, accountID string, exchange *fakeKalshiExchange) *kalshisettlement.Service {
	t.Helper()
	store, err := NewKalshiSettlementStore(database, []string{accountID})
	if err != nil {
		t.Fatal(err)
	}
	service, err := kalshisettlement.New(kalshisettlement.Params{
		ExecutionAccountID: accountID, Source: exchange, Store: store,
		Interval: time.Minute, BalanceDriftThreshold: "0.01",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func assertKalshiCash(t *testing.T, database *sql.DB, accountID, want string) {
	t.Helper()
	total, available, reserved := accountBalances(t, database, accountID)
	if !sameNumeric(total, want) || !sameNumeric(available, want) || !sameNumeric(reserved, "0") {
		t.Fatalf("Kalshi cash = %s/%s/%s, want %s/%s/0", total, available, reserved, want, want)
	}
}

// settledWhileDown is an account with 100 local cash holding 10 YES of a
// market that settled YES while the process was down: the exchange already
// paid 10 and reports 110.
func settledWhileDown(t *testing.T, accountID string) (*sql.DB, *fakeKalshiExchange) {
	t.Helper()
	database := kalshiSettlementDatabase(t)
	seedKalshiSettlementAccount(t, database, accountID, "100")
	seedKalshiHolding(t, database, accountID, "KXSYNC-1", "YES", "10", "4")
	return database, &fakeKalshiExchange{
		balance: "110",
		settlements: map[string][]domain.KalshiSettlement{
			"KXSYNC-1": {kalshiRecord("KXSYNC-1", "yes", "10", "0", "10", "1")},
		},
	}
}

// TestKalshiBalanceSyncDefersWhileASettlementIsUnbookedPostgresIntegration
// replays a restart after a held market settled while the process was down:
// the startup sync must not write the exchange balance (which already contains
// the payout) over the cash; the settlement loop, started after order
// recovery, books the payout once.
func TestKalshiBalanceSyncDefersWhileASettlementIsUnbookedPostgresIntegration(t *testing.T) {
	ctx := context.Background()
	t.Run("settled on the exchange, no local row", func(t *testing.T) {
		const account = "kalshi:sync-unbooked"
		database, exchange := settledWhileDown(t, account)
		deferred, _, err := exchange.sync(ctx, database, account)
		if err != nil {
			t.Fatal(err)
		}
		// The settlement loop then books the payout once and the ledger matches
		// the exchange (an overwrite before it would leave 120).
		if err := newKalshiSettlementService(t, database, account, exchange).Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		assertKalshiCash(t, database, account, "110")
		if !deferred {
			t.Fatal("sync replaced the cash while a settled market was unbooked")
		}
		if deferred, _, err := exchange.sync(ctx, database, account); err != nil || deferred {
			t.Fatalf("next restart sync = deferred %v err %v", deferred, err)
		}
		assertKalshiCash(t, database, account, "110")
	})
	t.Run("settlement held for manual review", func(t *testing.T) {
		const account = "kalshi:sync-review"
		database, exchange := settledWhileDown(t, account)
		exchange.settlements["KXSYNC-1"] = []domain.KalshiSettlement{kalshiRecord("KXSYNC-1", "yes", "10", "0", "9.5", "1")}
		exchange.balance = "109.5"
		// A sweep of the previous run held the record for review.
		if err := newKalshiSettlementService(t, database, account, exchange).Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		if got := scalar(t, database, `SELECT status FROM kalshi_settlements WHERE execution_account_id=$1`, account); got != "MANUAL_REVIEW" {
			t.Fatalf("settlement status = %s, want MANUAL_REVIEW", got)
		}
		deferred, _, err := exchange.sync(ctx, database, account)
		if err != nil || !deferred {
			t.Fatalf("sync = deferred %v err %v, want deferred", deferred, err)
		}
		assertKalshiCash(t, database, account, "100")
	})
	t.Run("exchange settlement unreadable", func(t *testing.T) {
		const account = "kalshi:sync-unreadable"
		database, exchange := settledWhileDown(t, account)
		exchange.listErr = errors.New("kalshi unavailable")
		if _, _, err := exchange.sync(ctx, database, account); err == nil {
			t.Fatal("sync overwrote the cash without knowing whether a held market had settled")
		}
		assertKalshiCash(t, database, account, "100")
	})
}

// TestKalshiBalanceSyncStillAlignsUnsettledHoldingsPostgresIntegration keeps
// the existing behaviour: with nothing settled, the exchange balance replaces
// the local cash and the difference is recorded; a new account starts from it.
func TestKalshiBalanceSyncStillAlignsUnsettledHoldingsPostgresIntegration(t *testing.T) {
	ctx := context.Background()
	const account = "kalshi:sync-open"
	database := kalshiSettlementDatabase(t)
	seedKalshiSettlementAccount(t, database, account, "100")
	seedKalshiHolding(t, database, account, "KXSYNC-2", "YES", "10", "4")
	exchange := &fakeKalshiExchange{balance: "104"}
	deferred, balance, err := exchange.sync(ctx, database, account)
	if err != nil || deferred || !balance.Equal("104") {
		t.Fatalf("sync = deferred %v balance %s err %v", deferred, balance, err)
	}
	assertKalshiCash(t, database, account, "104")
	if got := scalar(t, database, `SELECT total_balance_delta::text FROM execution_account_events
		WHERE execution_account_id=$1 AND event_type='KALSHI_BALANCE_SYNC'`, account); !sameNumeric(got, "4") {
		t.Fatalf("balance sync delta = %s, want 4", got)
	}

	// However many markets are held, startup reads the exchange settlements once;
	// with every held market already settled locally it does not read at all.
	for index := 3; index <= 8; index++ {
		seedKalshiHolding(t, database, account, fmt.Sprintf("KXSYNC-%d", index), "YES", "1", "0.5")
	}
	exchange.allReads = 0
	if deferred, _, err := exchange.sync(ctx, database, account); err != nil || deferred || exchange.allReads != 1 {
		t.Fatalf("sync with 7 held markets = deferred %v reads %d err %v, want one read", deferred, exchange.allReads, err)
	}

	const fresh = "kalshi:sync-new"
	if deferred, _, err := (&fakeKalshiExchange{balance: "50"}).sync(ctx, database, fresh); err != nil || deferred {
		t.Fatalf("new account sync = deferred %v err %v", deferred, err)
	}
	assertKalshiCash(t, database, fresh, "50")
}

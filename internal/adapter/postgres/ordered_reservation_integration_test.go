package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
)

// TestOrderedConcurrentReservationsPostgresIntegration is the funding guarantee
// behind parallel intent delivery. The wallet can pay for only three of eight
// BUY orders. The orders are started in reverse so that only the reservation
// sequence can decide who gets the money, and a fill is applied to an earlier
// order at the same time to exercise the shared account lock. The first three
// orders in batch order must win, the rest must be rejected for balance, and
// total = available + reserved must hold.
func TestOrderedConcurrentReservationsPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	const accountID = "account-ordered-reservations"
	insertAccount(t, db, accountID, "0xorderedreservations", "100", "100", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db, DustSharesThreshold: "0.01", DustNotionalThreshold: "1"})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := fillprocessor.New(fillprocessor.Params{Orders: repository, Source: noFillsSource{}, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	// An earlier acknowledged order holding 6 of the 100: 94 left to spend.
	earlier := integrationOrder("ordered-earlier", accountID, "token-earlier", domain.SideBuy, "10", "0.6")
	earlier.Intent.Venue = "paper"
	earlier = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, earlier, "0xordered-earlier", now)
	assertAccount(t, db, accountID, "100", "94", "6")

	// Eight orders of 30 each: 94 (plus the 0.4 price improvement of the fill
	// below) funds exactly three of them.
	const count = 8
	orders := make([]domain.Order, count)
	for index := range orders {
		orders[index] = integrationOrder("ordered-"+string(rune('a'+index)), accountID, "token-ordered-"+string(rune('a'+index)), domain.SideBuy, "100", "0.3")
	}
	sequence := port.NewReservationSequence(count)
	results := make([]error, count)

	var wg sync.WaitGroup
	for index := count - 1; index >= 0; index-- {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			turn := sequence.Turn(index)
			defer turn.Release()
			if err := turn.Wait(ctx); err != nil {
				results[index] = err
				return
			}
			_, results[index] = reservations.Reserve(ctx, orders[index])
			turn.Release()
		}(index)
	}
	fillErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		matched := domain.Fill{
			VenueFillID: "trade-ordered-1", LiquidityRole: domain.LiquidityRoleMaker,
			Status: domain.FillStatusMatched, Shares: "4", Price: "0.5",
			GrossNotional: "2", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0",
			PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0",
			FeeSource: "TEST_AUTHORITATIVE_SETTLEMENT",
			MatchedAt: now.Add(10 * time.Second), VenueUpdatedAt: now.Add(10 * time.Second),
			ObservedAt: now.Add(11 * time.Second),
		}
		observation, err := processor.Process(ctx, earlier, matched)
		if err != nil {
			fillErr <- err
			return
		}
		confirmedAt := now.Add(12 * time.Second)
		matched.Status = domain.FillStatusConfirmed
		matched.ConfirmedAt = &confirmedAt
		matched.ObservedAt = confirmedAt
		_, err = processor.Process(ctx, observation.Order, matched)
		fillErr <- err
	}()
	wg.Wait()
	if err := <-fillErr; err != nil {
		t.Fatalf("concurrent fill application: %v", err)
	}

	for index, err := range results {
		if index < 3 {
			if err != nil {
				t.Fatalf("order %d (inside the funded prefix) error = %v", index, err)
			}
			continue
		}
		var rejection *port.Rejection
		if !errors.As(err, &rejection) || rejection.Code != "INSUFFICIENT_AVAILABLE_BALANCE" {
			t.Fatalf("order %d error = %v, want INSUFFICIENT_AVAILABLE_BALANCE", index, err)
		}
	}
	// total 100 - 2 spent on the fill; reserved 3.6 left on the earlier order
	// plus 3 x 30; available is what is left of the 94.4.
	assertAccount(t, db, accountID, "98", "4.4", "93.6")
}

// Production reservations run the atomic live risk check inside the reservation
// transaction, and that is where "INSUFFICIENT_WALLET_BALANCE" comes from. The
// same ordering guarantee must hold on that path.
func TestOrderedConcurrentLiveRiskReservationsPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	now := time.Now().UTC().Truncate(time.Microsecond)
	manager, err := NewReservationManager(ReservationManagerParams{
		DB: db, Now: func() time.Time { return now }, MaxBuyFeeRateBPS: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		UPDATE execution_risk_global_control
		SET kill_switch=FALSE, reason='', version=version+1, updated_at=$1
		WHERE singleton=TRUE`, now); err != nil {
		t.Fatal(err)
	}
	const accountID = "account-ordered-live-risk"
	insertAccount(t, db, accountID, "0xorderedlive", "35", "35", "0")
	provisionLiveRisk(t, db, liveRiskFixture{
		accountID: accountID, now: now, binding: true,
		maxOrder: "100", maxMarket: "1000", maxStrategy: "1000", maxWallet: "1000", maxDaily: "1000",
	})

	// Eight orders of 10 each against 35 available: three fit.
	const count = 8
	sequence := port.NewReservationSequence(count)
	results := make([]error, count)
	var wg sync.WaitGroup
	for index := count - 1; index >= 0; index-- {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			turn := sequence.Turn(index)
			defer turn.Release()
			if err := turn.Wait(context.Background()); err != nil {
				results[index] = err
				return
			}
			order := liveIntegrationOrder(
				"ordered-live-"+string(rune('a'+index)), accountID, "token-ordered-live-"+string(rune('a'+index)), "20", "0.5", now,
			)
			_, results[index] = manager.Reserve(context.Background(), order)
			turn.Release()
		}(index)
	}
	wg.Wait()
	for index, err := range results {
		if index < 3 {
			if err != nil {
				t.Fatalf("order %d (inside the funded prefix) error = %v", index, err)
			}
			continue
		}
		assertRejectionCode(t, err, "INSUFFICIENT_WALLET_BALANCE")
	}
	assertAccount(t, db, accountID, "35", "5", "30")
}

package kalshi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type staticKalshiBookSource struct {
	books []domain.OrderBookSnapshot
}

func (source staticKalshiBookSource) Capture(context.Context, time.Time, []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	return source.books, nil
}

func TestKalshiMarketValidatorAllowsDepthAwareBuyMovements(t *testing.T) {
	tests := []struct {
		name string
		asks []domain.PriceLevel
	}{
		{
			name: "favourable movement",
			asks: []domain.PriceLevel{
				{Price: "0.47", Size: "2"}, {Price: "0.48", Size: "3"},
				{Price: "0.50", Size: "5"}, {Price: "0.53", Size: "100"},
			},
		},
		{
			name: "adverse movement inside worst price",
			asks: []domain.PriceLevel{
				{Price: "0.51", Size: "4"}, {Price: "0.52", Size: "6"},
				{Price: "0.53", Size: "100"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
			book.Asks = test.asks
			book.BestAsk = test.asks[0].Price
			validator := newKalshiValidatorForTest(t, now, book)

			validation, err := validator.Validate(context.Background(), intent)
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if validation.WorstPrice != "0.52" || validation.BestAsk != test.asks[0].Price {
				t.Fatalf("Validate() = %#v", validation)
			}
		})
	}
}

func TestKalshiMarketValidatorAllowsDepthAwareSellMovements(t *testing.T) {
	tests := []struct {
		name string
		bids []domain.PriceLevel
	}{
		{
			name: "favourable movement",
			bids: []domain.PriceLevel{
				{Price: "0.54", Size: "2"}, {Price: "0.52", Size: "3"},
				{Price: "0.50", Size: "5"}, {Price: "0.47", Size: "100"},
			},
		},
		{
			name: "adverse movement inside worst price",
			bids: []domain.PriceLevel{
				{Price: "0.49", Size: "4"}, {Price: "0.48", Size: "6"},
				{Price: "0.47", Size: "100"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(domain.SideSell)
			book.Bids = test.bids
			book.BestBid = test.bids[0].Price
			validator := newKalshiValidatorForTest(t, now, book)

			validation, err := validator.Validate(context.Background(), intent)
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if validation.WorstPrice != "0.48" || validation.BestBid != test.bids[0].Price {
				t.Fatalf("Validate() = %#v", validation)
			}
		})
	}
}

func TestKalshiMarketValidatorRejectsUnsafeDepthAwarePriceOrDepth(t *testing.T) {
	tests := []struct {
		name   string
		code   string
		mutate func(*domain.OrderIntent, *domain.OrderBookSnapshot)
	}{
		{
			name: "buy latest ask beyond protection", code: "KALSHI_PRICE_MOVED",
			mutate: func(_ *domain.OrderIntent, book *domain.OrderBookSnapshot) {
				book.Asks = []domain.PriceLevel{{Price: "0.53", Size: "100"}}
				book.BestAsk = "0.53"
			},
		},
		{
			name: "sell latest bid beyond protection", code: "KALSHI_PRICE_MOVED",
			mutate: func(intent *domain.OrderIntent, book *domain.OrderBookSnapshot) {
				configureKalshiSellIntent(intent)
				book.Bids = []domain.PriceLevel{{Price: "0.47", Size: "100"}}
				book.BestBid = "0.47"
			},
		},
		{
			name: "buy protection is better than strategy reference", code: "KALSHI_PRICE_PROTECTION_INVALID",
			mutate: func(intent *domain.OrderIntent, _ *domain.OrderBookSnapshot) {
				intent.Price = "0.49"
				intent.WorstPrice = "0.49"
			},
		},
		{
			name: "worst price is off tick", code: "KALSHI_PRICE_TICK_MISMATCH",
			mutate: func(intent *domain.OrderIntent, _ *domain.OrderBookSnapshot) {
				intent.Price = "0.515"
				intent.WorstPrice = "0.515"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
			test.mutate(&intent, &book)
			validator := newKalshiValidatorForTest(t, now, book)
			assertKalshiRejectionCode(t, validator, intent, test.code)
		})
	}
}

func TestKalshiMarketValidatorAllowsPartialIOCDepth(t *testing.T) {
	for _, side := range []domain.Side{domain.SideBuy, domain.SideSell} {
		t.Run(string(side), func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(side)
			intent.TimeInForce = domain.TimeInForceIOC
			if side == domain.SideBuy {
				book.Asks = []domain.PriceLevel{{Price: "0.51", Size: "4"}, {Price: "0.52", Size: "5"}, {Price: "0.53", Size: "100"}}
				book.BestAsk = "0.51"
			} else {
				book.Bids = []domain.PriceLevel{{Price: "0.49", Size: "4"}, {Price: "0.48", Size: "5"}, {Price: "0.47", Size: "100"}}
				book.BestBid = "0.49"
			}
			validator := newKalshiValidatorForTest(t, now, book)
			validation, err := validator.Validate(context.Background(), intent)
			if err != nil {
				t.Fatalf("Validate() error = %v, want protected partial depth to pass IOC", err)
			}
			if validation.ExecutableSize != "9.00" {
				t.Fatalf("executable_size = %q, want 9.00", validation.ExecutableSize)
			}
		})
	}
}

func TestKalshiMarketValidatorCapsIOCDepthAtRequestedSize(t *testing.T) {
	now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
	intent.TimeInForce = domain.TimeInForceIOC
	book.Asks = []domain.PriceLevel{{Price: "0.51", Size: "7.25"}, {Price: "0.52", Size: "8.75"}}
	book.BestAsk = "0.51"
	validator := newKalshiValidatorForTest(t, now, book)
	validation, err := validator.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.ExecutableSize != "10.00" {
		t.Fatalf("executable_size = %q, want requested size 10.00", validation.ExecutableSize)
	}
}

func TestKalshiMarketValidatorRejectsIOCDepthBelowMinimum(t *testing.T) {
	now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
	intent.TimeInForce = domain.TimeInForceIOC
	book.Asks = []domain.PriceLevel{{Price: "0.51", Size: "0.50"}}
	book.BestAsk = "0.51"
	validator := newKalshiValidatorForTest(t, now, book)
	assertKalshiRejectionCode(t, validator, intent, "KALSHI_INSUFFICIENT_VISIBLE_DEPTH")
}

func TestKalshiMarketValidatorStillRequiresFullDepthForNonIOCOrders(t *testing.T) {
	for _, side := range []domain.Side{domain.SideBuy, domain.SideSell} {
		t.Run(string(side), func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(side)
			intent.TimeInForce = domain.TimeInForceFOK
			if side == domain.SideBuy {
				book.Asks = []domain.PriceLevel{{Price: "0.51", Size: "4"}, {Price: "0.52", Size: "5"}, {Price: "0.53", Size: "100"}}
				book.BestAsk = "0.51"
			} else {
				book.Bids = []domain.PriceLevel{{Price: "0.49", Size: "4"}, {Price: "0.48", Size: "5"}, {Price: "0.47", Size: "100"}}
				book.BestBid = "0.49"
			}
			validator := newKalshiValidatorForTest(t, now, book)
			assertKalshiRejectionCode(t, validator, intent, "KALSHI_INSUFFICIENT_VISIBLE_DEPTH")
		})
	}
}

func TestKalshiMarketValidatorAllowsSparseDepthBeyondTwoTicks(t *testing.T) {
	now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
	intent.Price = "0.42"
	intent.WorstPrice = "0.42"
	intent.Size = "12"
	intent.Metadata["strategy_reference_price"] = "0.39"
	book.BestAsk = "0.39"
	book.Asks = []domain.PriceLevel{
		{Price: "0.39", Size: "7.73"},
		{Price: "0.42", Size: "15"},
		{Price: "0.43", Size: "50"},
	}
	validator := newKalshiValidatorForTest(t, now, book)
	validation, err := validator.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.BestAsk != "0.39" || validation.WorstPrice != "0.42" {
		t.Fatalf("Validate() = %#v", validation)
	}
}

func TestKalshiMarketValidatorAllowsSparseSellDepthBeyondTwoTicks(t *testing.T) {
	now, intent, book := validKalshiValidationFixtures(domain.SideSell)
	intent.Price = "0.58"
	intent.WorstPrice = "0.58"
	intent.Size = "12"
	intent.Metadata["strategy_reference_price"] = "0.61"
	book.BestBid = "0.61"
	book.Bids = []domain.PriceLevel{
		{Price: "0.61", Size: "7.73"},
		{Price: "0.58", Size: "15"},
		{Price: "0.57", Size: "50"},
	}
	validator := newKalshiValidatorForTest(t, now, book)
	validation, err := validator.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.BestBid != "0.61" || validation.WorstPrice != "0.58" {
		t.Fatalf("Validate() = %#v", validation)
	}
}

func TestKalshiMarketValidatorRequiresValidStrategyReferencePrice(t *testing.T) {
	tests := []struct {
		name      string
		reference *string
		code      string
	}{
		{name: "missing", code: "KALSHI_STRATEGY_REFERENCE_PRICE_REQUIRED"},
		{name: "invalid", reference: stringPointer("not-a-price"), code: "KALSHI_STRATEGY_REFERENCE_PRICE_INVALID"},
		{name: "off tick", reference: stringPointer("0.505"), code: "KALSHI_STRATEGY_REFERENCE_PRICE_INVALID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
			delete(intent.Metadata, "strategy_reference_price")
			if test.reference != nil {
				intent.Metadata["strategy_reference_price"] = *test.reference
			}
			validator := newKalshiValidatorForTest(t, now, book)
			assertKalshiRejectionCode(t, validator, intent, test.code)
		})
	}
}

func TestKalshiMarketValidatorRetainsOfficialBookFreshness(t *testing.T) {
	now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
	book.SourceAt = now.Add(-11 * time.Second)
	validator := newKalshiValidatorForTest(t, now, book)
	assertKalshiRejectionCode(t, validator, intent, "KALSHI_BOOK_STALE")
}

func validKalshiValidationFixtures(side domain.Side) (time.Time, domain.OrderIntent, domain.OrderBookSnapshot) {
	now := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	snapshotAt := now.Add(-time.Minute)
	outcomeIndex := 0
	negRisk := false
	intent := validKalshiIntent(domain.SideBuy, "YES")
	intent.OutcomeIndex = &outcomeIndex
	intent.OutcomeName = "YES"
	intent.ExpectedNegRisk = &negRisk
	intent.MarketSnapshotAt = &snapshotAt
	intent.Price = "0.52"
	intent.WorstPrice = "0.52"
	intent.Size = "10"
	intent.Metadata = map[string]string{"strategy_reference_price": "0.50"}
	if side == domain.SideSell {
		configureKalshiSellIntent(&intent)
	}
	book := domain.OrderBookSnapshot{
		MarketSource: domain.MarketSourceKalshi,
		MarketID:     intent.MarketID,
		ConditionID:  intent.ConditionID,
		OutcomeIndex: outcomeIndex,
		OutcomeID:    intent.OutcomeID,
		TokenID:      intent.TokenID,
		Status:       domain.OrderBookStatusOK,
		SourceAt:     now.Add(-time.Second),
		ObservedAt:   now,
		TickSize:     "0.01",
		MinOrderSize: "1",
		DepthLimit:   4,
		BestBid:      "0.49",
		BestAsk:      "0.51",
		Bids: []domain.PriceLevel{
			{Price: "0.49", Size: "4"}, {Price: "0.48", Size: "6"},
			{Price: "0.47", Size: "100"},
		},
		Asks: []domain.PriceLevel{
			{Price: "0.51", Size: "4"}, {Price: "0.52", Size: "6"},
			{Price: "0.53", Size: "100"},
		},
	}
	return now, intent, book
}

func configureKalshiSellIntent(intent *domain.OrderIntent) {
	intent.Side = domain.SideSell
	intent.TargetLotID = "lot"
	intent.Price = "0.48"
	intent.WorstPrice = "0.48"
	intent.Metadata["strategy_reference_price"] = "0.50"
}

func newKalshiValidatorForTest(t *testing.T, now time.Time, book domain.OrderBookSnapshot) *MarketValidator {
	t.Helper()
	validator, err := NewMarketValidator(staticKalshiBookSource{books: []domain.OrderBookSnapshot{book}})
	if err != nil {
		t.Fatal(err)
	}
	validator.now = func() time.Time { return now }
	return validator
}

func assertKalshiRejectionCode(t *testing.T, validator *MarketValidator, intent domain.OrderIntent, code string) {
	t.Helper()
	_, err := validator.Validate(context.Background(), intent)
	var rejection *port.Rejection
	if !errors.As(err, &rejection) || rejection.Code != code {
		t.Fatalf("Validate() error = %#v, rejection = %#v; want %s", err, rejection, code)
	}
}

func stringPointer(value string) *string { return &value }

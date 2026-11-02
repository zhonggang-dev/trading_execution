package kalshi

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestFeeScheduleSourceUsesSeriesQuadraticFee(t *testing.T) {
	privateKey := testPrivateKey(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		if request.Method != http.MethodGet {
			t.Errorf("method = %s", request.Method)
		}
		requests.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/trade-api/v2/markets/KXRT-GOD-92":
			_, _ = writer.Write([]byte(`{"market":{"ticker":"KXRT-GOD-92","event_ticker":"KXRT-GOD"}}`))
		case "/trade-api/v2/events/KXRT-GOD":
			if request.URL.Query().Get("exclude_markets") != "true" {
				t.Errorf("exclude_markets = %q", request.URL.Query().Get("exclude_markets"))
			}
			_, _ = writer.Write([]byte(`{"event":{"event_ticker":"KXRT-GOD","series_ticker":"KXRT","fee_type_override":null,"fee_multiplier_override":null}}`))
		case "/trade-api/v2/series/KXRT":
			_, _ = writer.Write([]byte(`{"series":{"ticker":"KXRT","fee_type":"quadratic","fee_multiplier":1}}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	now := time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC)
	source, err := NewFeeScheduleSource(FeeScheduleParams{
		Client: testClient(t, server.URL, "key", privateKey, false), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewFeeScheduleSource() error = %v", err)
	}
	schedule, err := source.MarketFeeSchedule(context.Background(), "KXRT-GOD-92")
	if err != nil {
		t.Fatalf("MarketFeeSchedule() error = %v", err)
	}
	if !schedule.PlatformFeeRate.Equal("0.07") || !schedule.FeeExponent.Equal("1") ||
		!schedule.RoundingAllowancePerShare.Equal("0.01") || !schedule.TakerOnly || !schedule.FetchedAt.Equal(now) {
		t.Fatalf("schedule = %#v", schedule)
	}
	if _, err := source.MarketFeeSchedule(context.Background(), "KXRT-GOD-92"); err != nil {
		t.Fatalf("cached MarketFeeSchedule() error = %v", err)
	}
	if requests.Load() != 3 {
		t.Fatalf("requests = %d, want 3 (second read must hit the cache)", requests.Load())
	}
}

func TestFeeScheduleSourcePrefersEventOverride(t *testing.T) {
	privateKey := testPrivateKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/trade-api/v2/markets/M-1":
			_, _ = writer.Write([]byte(`{"market":{"ticker":"M-1","event_ticker":"E-1"}}`))
		case "/trade-api/v2/events/E-1":
			_, _ = writer.Write([]byte(`{"event":{"event_ticker":"E-1","series_ticker":"S","fee_type_override":"quadratic_with_maker_fees","fee_multiplier_override":0.5}}`))
		default:
			t.Errorf("unexpected request %s", request.URL.Path)
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	source, err := NewFeeScheduleSource(FeeScheduleParams{Client: testClient(t, server.URL, "key", privateKey, false)})
	if err != nil {
		t.Fatalf("NewFeeScheduleSource() error = %v", err)
	}
	schedule, err := source.MarketFeeSchedule(context.Background(), "M-1")
	if err != nil {
		t.Fatalf("MarketFeeSchedule() error = %v", err)
	}
	if !schedule.PlatformFeeRate.Equal("0.035") {
		t.Fatalf("rate = %s, want 0.035", schedule.PlatformFeeRate)
	}
}

func TestFeeScheduleSourceRejectsUnsupportedOrIncompleteFees(t *testing.T) {
	tests := []struct {
		name   string
		event  string
		series string
	}{
		{
			name:   "flat series",
			event:  `{"event":{"event_ticker":"E-1","series_ticker":"S"}}`,
			series: `{"series":{"ticker":"S","fee_type":"flat","fee_multiplier":1}}`,
		},
		{
			name:   "missing series multiplier",
			event:  `{"event":{"event_ticker":"E-1","series_ticker":"S"}}`,
			series: `{"series":{"ticker":"S","fee_type":"quadratic"}}`,
		},
		{
			name:   "negative series multiplier",
			event:  `{"event":{"event_ticker":"E-1","series_ticker":"S"}}`,
			series: `{"series":{"ticker":"S","fee_type":"quadratic","fee_multiplier":-1}}`,
		},
		{
			name:  "override without multiplier",
			event: `{"event":{"event_ticker":"E-1","series_ticker":"S","fee_type_override":"quadratic","fee_multiplier_override":null}}`,
		},
		{
			name:   "different series identity",
			event:  `{"event":{"event_ticker":"E-1","series_ticker":"S"}}`,
			series: `{"series":{"ticker":"OTHER","fee_type":"quadratic","fee_multiplier":1}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			privateKey := testPrivateKey(t)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				switch request.URL.Path {
				case "/trade-api/v2/markets/M-1":
					_, _ = writer.Write([]byte(`{"market":{"ticker":"M-1","event_ticker":"E-1"}}`))
				case "/trade-api/v2/events/E-1":
					_, _ = writer.Write([]byte(test.event))
				case "/trade-api/v2/series/S":
					_, _ = writer.Write([]byte(test.series))
				default:
					http.NotFound(writer, request)
				}
			}))
			t.Cleanup(server.Close)
			source, err := NewFeeScheduleSource(FeeScheduleParams{Client: testClient(t, server.URL, "key", privateKey, false)})
			if err != nil {
				t.Fatalf("NewFeeScheduleSource() error = %v", err)
			}
			if schedule, err := source.MarketFeeSchedule(context.Background(), "M-1"); err == nil {
				t.Fatalf("MarketFeeSchedule() = %#v, want error", schedule)
			}
		})
	}
}

func TestFeeScheduleSourceKeepsExpiredScheduleOnRefreshFailure(t *testing.T) {
	privateKey := testPrivateKey(t)
	var failing atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if failing.Load() {
			http.Error(writer, `{"error":{"code":"internal","message":"down"}}`, http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/trade-api/v2/markets/M-1":
			_, _ = writer.Write([]byte(`{"market":{"ticker":"M-1","event_ticker":"E-1"}}`))
		case "/trade-api/v2/events/E-1":
			_, _ = writer.Write([]byte(`{"event":{"event_ticker":"E-1","series_ticker":"S"}}`))
		case "/trade-api/v2/series/S":
			_, _ = writer.Write([]byte(`{"series":{"ticker":"S","fee_type":"quadratic","fee_multiplier":1}}`))
		}
	}))
	t.Cleanup(server.Close)
	now := time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC)
	source, err := NewFeeScheduleSource(FeeScheduleParams{
		Client: testClient(t, server.URL, "key", privateKey, false), TTL: time.Minute, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewFeeScheduleSource() error = %v", err)
	}
	if _, err := source.MarketFeeSchedule(context.Background(), "M-1"); err != nil {
		t.Fatalf("initial MarketFeeSchedule() error = %v", err)
	}
	failing.Store(true)
	now = now.Add(2 * time.Minute)
	schedule, err := source.MarketFeeSchedule(context.Background(), "M-1")
	if err != nil || !schedule.PlatformFeeRate.Equal("0.07") {
		t.Fatalf("expired MarketFeeSchedule() = %#v, %v", schedule, err)
	}
	if _, err := source.MarketFeeSchedule(context.Background(), "M-2"); err == nil {
		t.Fatal("uncached market must fail when Kalshi is unavailable")
	}
}

type staticKalshiFeeReader struct {
	schedule domain.MarketFeeSchedule
	err      error
}

func (reader staticKalshiFeeReader) MarketFeeSchedule(context.Context, string) (domain.MarketFeeSchedule, error) {
	return reader.schedule, reader.err
}

func TestKalshiMarketValidatorRecordsVenueBuyFeeReserve(t *testing.T) {
	now, intent, book := validKalshiValidationFixtures(domain.SideBuy)
	validator := newKalshiValidatorForTest(t, now, book)
	validator.fees = staticKalshiFeeReader{schedule: domain.MarketFeeSchedule{
		PlatformFeeRate: "0.07", FeeExponent: "1", RoundingAllowancePerShare: "0.01", TakerOnly: true,
	}}
	validation, err := validator.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	// worst 0.52 can fill at 0.5: 0.07 * 0.25 + 0.01 = 0.0275
	if fee, ok := validation.BuyFeeReserve.VenueMaxFeePerShare(); !ok || !fee.Equal("0.0275") {
		t.Fatalf("BuyFeeReserve = %#v", validation.BuyFeeReserve)
	}

	validator.fees = staticKalshiFeeReader{err: errors.New("Kalshi fee type \"flat\" is not supported")}
	validation, err = validator.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.BuyFeeReserve == nil || validation.BuyFeeReserve.Source != domain.BuyFeeReserveSourceConfigCap ||
		validation.BuyFeeReserve.Reason == "" {
		t.Fatalf("fallback BuyFeeReserve = %#v", validation.BuyFeeReserve)
	}
}

func TestKalshiMarketValidatorSkipsFeeReserveForSell(t *testing.T) {
	now, intent, book := validKalshiValidationFixtures(domain.SideSell)
	validator := newKalshiValidatorForTest(t, now, book)
	validator.fees = staticKalshiFeeReader{err: errors.New("must not be called")}
	validation, err := validator.Validate(context.Background(), intent)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validation.BuyFeeReserve != nil {
		t.Fatalf("SELL BuyFeeReserve = %#v", validation.BuyFeeReserve)
	}
}

// TestKalshiFeeReserveCoversObservedFills replays the 2026-10-31 16:50 kalshi:main
// batch: the fee-protected reservation must cover each confirmed notional plus
// the fee Kalshi actually charged, and all six orders must fit the 51.25
// balance that the former 2x cap reduced to three.
func TestKalshiFeeReserveCoversObservedFills(t *testing.T) {
	schedule, err := kalshiTakerFeeSchedule("quadratic", "1")
	if err != nil {
		t.Fatalf("kalshiTakerFeeSchedule() error = %v", err)
	}
	orders := []struct {
		size, worst, fee string
	}{
		{size: "16", worst: "0.42", fee: "0.2729"},
		{size: "14", worst: "0.54", fee: "0.2435"},
		{size: "21", worst: "0.49", fee: "0.3674"},
		{size: "3", worst: "0.58"},
		{size: "6", worst: "0.79"},
		{size: "5", worst: "0.91"},
	}
	total := new(big.Rat)
	for _, order := range orders {
		perShareFee, err := domain.MaxBuyFeePerShare(domain.Decimal(order.worst), schedule)
		if err != nil {
			t.Fatalf("MaxBuyFeePerShare(%s) error = %v", order.worst, err)
		}
		unit := new(big.Rat).Add(mustRat(t, order.worst), mustRat(t, string(perShareFee)))
		reserved := new(big.Rat).Mul(unit, mustRat(t, order.size))
		total.Add(total, reserved)
		if order.fee == "" {
			continue
		}
		spent := new(big.Rat).Mul(mustRat(t, order.worst), mustRat(t, order.size))
		spent.Add(spent, mustRat(t, order.fee))
		if reserved.Cmp(spent) < 0 {
			t.Fatalf("%s@%s reserved %s < spent %s", order.size, order.worst, reserved.FloatString(6), spent.FloatString(6))
		}
	}
	if total.Cmp(mustRat(t, "51.25")) > 0 {
		t.Fatalf("batch reservation %s exceeds the 51.25 balance", total.FloatString(6))
	}
}

func mustRat(t *testing.T, value string) *big.Rat {
	t.Helper()
	parsed, ok := new(big.Rat).SetString(value)
	if !ok {
		t.Fatalf("invalid rational %q", value)
	}
	return parsed
}

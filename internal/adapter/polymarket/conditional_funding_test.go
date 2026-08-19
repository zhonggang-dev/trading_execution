package polymarket

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestConditionalFundingVenueChecksExactSellBalanceAndExchange(t *testing.T) {
	source := &conditionalFundingSource{result: BalanceAllowance{
		AssetType: BalanceAssetConditional, TokenID: "7", Balance: "2500000",
		Allowances: map[string]string{strings.ToLower(NegRiskExchangeV2Address): "1"},
	}}
	underlying := &conditionalFundingFakeVenue{}
	venue, err := NewConditionalFundingVenue(underlying, source)
	if err != nil {
		t.Fatal(err)
	}
	order := conditionalFundingSellOrder(true, "2.5")
	if _, err := venue.Place(context.Background(), order); err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if underlying.placeCalls != 1 || source.calls != 1 || source.tokenID != "7" {
		t.Fatalf("calls venue=%d source=%d token=%q", underlying.placeCalls, source.calls, source.tokenID)
	}
}

func TestConditionalFundingVenueRejectsBeforeSubmit(t *testing.T) {
	tests := map[string]struct {
		result BalanceAllowance
		err    error
		code   string
	}{
		"missing approval": {
			result: BalanceAllowance{Balance: "2500000", Allowances: map[string]string{}},
			code:   "CLOB_CONDITIONAL_ALLOWANCE_INSUFFICIENT",
		},
		"insufficient balance": {
			result: BalanceAllowance{Balance: "2499999", Allowances: map[string]string{strings.ToLower(StandardExchangeV2Address): "1"}},
			code:   "CLOB_CONDITIONAL_BALANCE_INSUFFICIENT",
		},
		"read unavailable": {
			err: errors.New("temporary read failure"), code: "CLOB_CONDITIONAL_FUNDING_CHECK_FAILED",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			source := &conditionalFundingSource{result: test.result, err: test.err}
			underlying := &conditionalFundingFakeVenue{}
			venue, err := NewConditionalFundingVenue(underlying, source)
			if err != nil {
				t.Fatal(err)
			}
			order := conditionalFundingSellOrder(false, "2.5")
			_, placeErr := venue.Place(context.Background(), order)
			var venueError *port.VenueError
			if !errors.As(placeErr, &venueError) || venueError.Kind != port.VenueErrorRejected || venueError.Code != test.code {
				t.Fatalf("Place() error = %#v, want %s rejection", placeErr, test.code)
			}
			if underlying.placeCalls != 0 {
				t.Fatal("underlying Place was called after failed pre-submit check")
			}
		})
	}
}

func TestConditionalFundingVenueBypassesBuyAndNeverBlocksCancelOrGet(t *testing.T) {
	source := &conditionalFundingSource{err: errors.New("offline")}
	underlying := &conditionalFundingFakeVenue{}
	venue, err := NewConditionalFundingVenue(underlying, source)
	if err != nil {
		t.Fatal(err)
	}
	buy := conditionalFundingSellOrder(false, "1")
	buy.Intent.Side = domain.SideBuy
	if _, err := venue.Place(context.Background(), buy); err != nil {
		t.Fatal(err)
	}
	_, _ = venue.Cancel(context.Background(), buy)
	_, _ = venue.Get(context.Background(), buy)
	if source.calls != 0 || underlying.placeCalls != 1 || underlying.cancelCalls != 1 || underlying.getCalls != 1 {
		t.Fatalf("calls source=%d place=%d cancel=%d get=%d", source.calls, underlying.placeCalls, underlying.cancelCalls, underlying.getCalls)
	}
}

func conditionalFundingSellOrder(negRisk bool, size domain.Decimal) domain.Order {
	return domain.Order{
		Intent: domain.OrderIntent{
			ExecutionAccountID: "account-1", TokenID: "7", Side: domain.SideSell, Size: size,
		},
		MarketValidation: &domain.MarketValidation{NegRisk: negRisk},
	}
}

type conditionalFundingSource struct {
	result  BalanceAllowance
	err     error
	calls   int
	tokenID string
}

func (source *conditionalFundingSource) GetBalanceAllowance(_ context.Context, _ string, _ BalanceAssetType, tokenID string) (BalanceAllowance, error) {
	source.calls++
	source.tokenID = tokenID
	return source.result, source.err
}

type conditionalFundingFakeVenue struct {
	placeCalls, cancelCalls, getCalls int
}

func (venue *conditionalFundingFakeVenue) Name() string { return "polymarket" }
func (venue *conditionalFundingFakeVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.placeCalls++
	return port.VenueOrder{}, nil
}
func (venue *conditionalFundingFakeVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.cancelCalls++
	return port.VenueOrder{}, nil
}
func (venue *conditionalFundingFakeVenue) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.getCalls++
	return port.VenueOrder{}, nil
}

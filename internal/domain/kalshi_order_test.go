package domain

import "testing"

func TestCanonicalKalshiOrderIdentityCoversAllDirections(t *testing.T) {
	tests := []struct {
		side        Side
		outcome     string
		worstPrice  Decimal
		outcomeSide string
		bookSide    string
		orderPrice  Decimal
	}{
		{SideBuy, "YES", "0.27", "yes", "bid", "0.27"},
		{SideBuy, "NO", "0.27", "no", "ask", "0.7300"},
		{SideSell, "YES", "0.62", "no", "ask", "0.62"},
		{SideSell, "NO", "0.62", "yes", "bid", "0.3800"},
	}
	for _, testCase := range tests {
		identity, err := CanonicalKalshiOrderIdentity(testCase.side, testCase.outcome, testCase.worstPrice)
		if err != nil || identity.OutcomeSide != testCase.outcomeSide || identity.BookSide != testCase.bookSide ||
			!identity.OrderPrice.Equal(testCase.orderPrice) {
			t.Errorf("CanonicalKalshiOrderIdentity(%s,%s,%s)=%#v,%v", testCase.side, testCase.outcome, testCase.worstPrice, identity, err)
		}
	}
}

func TestCanonicalKalshiOrderIdentityRejectsInvalidInputs(t *testing.T) {
	for _, testCase := range []struct {
		side    Side
		outcome string
		price   Decimal
	}{
		{SideBuy, "MAYBE", "0.5"},
		{Side("HOLD"), "YES", "0.5"},
		{SideBuy, "YES", "0"},
		{SideBuy, "YES", "1"},
		{SideBuy, "YES", "missing"},
	} {
		if _, err := CanonicalKalshiOrderIdentity(testCase.side, testCase.outcome, testCase.price); err == nil {
			t.Errorf("CanonicalKalshiOrderIdentity(%s,%s,%s) error=nil", testCase.side, testCase.outcome, testCase.price)
		}
	}
}

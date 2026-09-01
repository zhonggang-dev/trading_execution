package domain

import (
	"fmt"
	"math/big"
	"strings"
)

// KalshiOrderIdentity is the canonical single-YES-book representation used by
// both live order serialization and evidence-backed repair validation.
type KalshiOrderIdentity struct {
	OutcomeSide string
	BookSide    string
	OrderPrice  Decimal
}

// CanonicalKalshiOrderIdentity maps an outcome-denominated execution intent to
// Kalshi V2's canonical direction fields and YES-leg order price.
func CanonicalKalshiOrderIdentity(side Side, outcomeID string, worstPrice Decimal) (KalshiOrderIdentity, error) {
	outcomeID = strings.ToUpper(strings.TrimSpace(outcomeID))
	price, ok := new(big.Rat).SetString(worstPrice.String())
	if !ok || price.Sign() <= 0 || price.Cmp(big.NewRat(1, 1)) >= 0 {
		return KalshiOrderIdentity{}, fmt.Errorf("Kalshi outcome price must be between zero and one")
	}
	identity := KalshiOrderIdentity{OrderPrice: worstPrice}
	switch {
	case side == SideBuy && outcomeID == "YES":
		identity.OutcomeSide, identity.BookSide = "yes", "bid"
	case side == SideBuy && outcomeID == "NO":
		identity.OutcomeSide, identity.BookSide = "no", "ask"
	case side == SideSell && outcomeID == "YES":
		identity.OutcomeSide, identity.BookSide = "no", "ask"
	case side == SideSell && outcomeID == "NO":
		identity.OutcomeSide, identity.BookSide = "yes", "bid"
	default:
		return KalshiOrderIdentity{}, fmt.Errorf("unsupported Kalshi side/outcome identity")
	}
	if outcomeID == "NO" {
		identity.OrderPrice = Decimal(new(big.Rat).Sub(big.NewRat(1, 1), price).FloatString(4))
	}
	return identity, nil
}

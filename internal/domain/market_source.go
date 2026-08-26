package domain

import (
	"fmt"
	"strings"
)

// MarketSource identifies the venue that owns a prediction market. An empty
// value is retained as the legacy Polymarket wire representation.
type MarketSource string

const (
	MarketSourcePolymarket MarketSource = "POLYMARKET"
	MarketSourceKalshi     MarketSource = "KALSHI"
)

// Normalize returns the canonical source, defaulting legacy empty values to
// Polymarket without changing their JSON representation.
func (source MarketSource) Normalize() MarketSource {
	normalized := MarketSource(strings.ToUpper(strings.TrimSpace(string(source))))
	if normalized == "" {
		return MarketSourcePolymarket
	}
	return normalized
}

// Venue returns the execution venue owned by this source.
func (source MarketSource) Venue(defaultVenue string) (string, error) {
	switch source.Normalize() {
	case MarketSourcePolymarket:
		venue := strings.ToLower(strings.TrimSpace(defaultVenue))
		if venue == "" {
			return "", fmt.Errorf("Polymarket execution venue is required")
		}
		return venue, nil
	case MarketSourceKalshi:
		return "kalshi", nil
	default:
		return "", fmt.Errorf("unsupported market_source %q", source)
	}
}

// CanonicalInstrumentID keeps the existing strategy token_id contract while
// allowing non-token venues. Kalshi uses a deterministic Trading-owned opaque
// identifier; the strategy never has to interpret its venue-specific shape.
func CanonicalInstrumentID(source MarketSource, marketID, conditionID, outcomeID, tokenID string) (string, error) {
	marketID = strings.TrimSpace(marketID)
	conditionID = strings.TrimSpace(conditionID)
	outcomeID = strings.ToUpper(strings.TrimSpace(outcomeID))
	tokenID = strings.TrimSpace(tokenID)
	switch source.Normalize() {
	case MarketSourcePolymarket:
		// condition_id was optional in the original trading contract. Preserve
		// that wire compatibility while still requiring the venue instrument.
		if marketID == "" || tokenID == "" {
			return "", fmt.Errorf("Polymarket market and token identifiers are required")
		}
		return tokenID, nil
	case MarketSourceKalshi:
		if marketID == "" || conditionID != "kalshi:"+marketID {
			return "", fmt.Errorf("Kalshi market and condition identifiers are inconsistent")
		}
		if outcomeID != "YES" && outcomeID != "NO" {
			return "", fmt.Errorf("Kalshi outcome_id must be YES or NO")
		}
		expected := conditionID + ":" + outcomeID
		if tokenID != "" && tokenID != expected {
			return "", fmt.Errorf("Kalshi token_id must be empty upstream or equal the canonical instrument id")
		}
		return expected, nil
	default:
		return "", fmt.Errorf("unsupported market_source %q", source)
	}
}

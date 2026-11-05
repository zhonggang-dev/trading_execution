package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// GetMarketPosition returns the signed net contracts held in one market: YES
// positive, NO negative. A market the account never held has no row and is zero.
func (client *Client) GetMarketPosition(ctx context.Context, ticker string) (domain.Decimal, error) {
	ticker, cursor := strings.TrimSpace(ticker), ""
	if ticker == "" {
		return "", fmt.Errorf("Kalshi position ticker is required")
	}
	position := domain.Decimal("0")
	found := false
	for page := 0; page < 20; page++ {
		var envelope struct {
			MarketPositions *[]json.RawMessage `json:"market_positions"`
			Cursor          *string            `json:"cursor"`
		}
		query := url.Values{"ticker": []string{ticker}, "limit": []string{"100"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/portfolio/positions?"+query.Encode(), nil, &envelope); err != nil {
			return "", err
		}
		if envelope.MarketPositions == nil {
			return "", fmt.Errorf("Kalshi positions response omitted the market_positions collection")
		}
		for _, raw := range *envelope.MarketPositions {
			var row struct {
				Ticker     string         `json:"ticker"`
				PositionFP domain.Decimal `json:"position_fp"`
			}
			if err := json.Unmarshal(raw, &row); err != nil {
				return "", fmt.Errorf("decode Kalshi market position: %w", err)
			}
			if strings.TrimSpace(row.Ticker) != ticker {
				return "", fmt.Errorf("Kalshi positions response returned market %q for %q", row.Ticker, ticker)
			}
			if row.PositionFP.IsEmpty() {
				return "", fmt.Errorf("Kalshi market position omitted position_fp")
			}
			if found {
				return "", fmt.Errorf("Kalshi positions response returned market %q twice", ticker)
			}
			position, found = row.PositionFP, true
		}
		next := ""
		if envelope.Cursor != nil {
			next = strings.TrimSpace(*envelope.Cursor)
		}
		if next == "" {
			return position, nil
		}
		if next == cursor {
			return "", fmt.Errorf("Kalshi positions cursor did not advance")
		}
		cursor = next
	}
	return "", fmt.Errorf("Kalshi position lookup exceeded pagination limit")
}

// ListPositions returns every market the account holds a non-zero net position
// in. It deliberately sends neither count_filter nor subaccount (their exact
// semantics are unverified) and drops zero rows itself, so closed markets the
// exchange may still list never look like holdings.
func (client *Client) ListPositions(ctx context.Context) ([]domain.KalshiMarketPosition, error) {
	cursor := ""
	result := make([]domain.KalshiMarketPosition, 0)
	seen := make(map[string]struct{})
	for page := 0; page < 20; page++ {
		var envelope struct {
			MarketPositions *[]json.RawMessage `json:"market_positions"`
			Cursor          *string            `json:"cursor"`
		}
		query := url.Values{"limit": []string{"100"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/portfolio/positions?"+query.Encode(), nil, &envelope); err != nil {
			return nil, err
		}
		if envelope.MarketPositions == nil {
			return nil, fmt.Errorf("Kalshi positions response omitted the market_positions collection")
		}
		for _, raw := range *envelope.MarketPositions {
			var row struct {
				Ticker     string         `json:"ticker"`
				PositionFP domain.Decimal `json:"position_fp"`
			}
			if err := json.Unmarshal(raw, &row); err != nil {
				return nil, fmt.Errorf("decode Kalshi market position: %w", err)
			}
			ticker := strings.TrimSpace(row.Ticker)
			if ticker == "" || strings.Contains(ticker, ":") {
				return nil, fmt.Errorf("Kalshi positions response returned an invalid market ticker %q", row.Ticker)
			}
			if row.PositionFP.IsEmpty() {
				return nil, fmt.Errorf("Kalshi market position %s omitted position_fp", ticker)
			}
			if _, duplicate := seen[ticker]; duplicate {
				return nil, fmt.Errorf("Kalshi positions response returned market %q twice", ticker)
			}
			seen[ticker] = struct{}{}
			sign, err := row.PositionFP.Sign()
			if err != nil {
				return nil, fmt.Errorf("Kalshi market position %s is invalid: %w", ticker, err)
			}
			if sign == 0 {
				continue
			}
			result = append(result, domain.KalshiMarketPosition{
				Ticker: ticker, PositionFP: row.PositionFP, Raw: append([]byte(nil), raw...),
			})
		}
		next := ""
		if envelope.Cursor != nil {
			next = strings.TrimSpace(*envelope.Cursor)
		}
		if next == "" {
			return result, nil
		}
		if next == cursor {
			return nil, fmt.Errorf("Kalshi positions cursor did not advance")
		}
		cursor = next
	}
	return nil, fmt.Errorf("Kalshi positions lookup exceeded pagination limit")
}

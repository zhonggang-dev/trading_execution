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

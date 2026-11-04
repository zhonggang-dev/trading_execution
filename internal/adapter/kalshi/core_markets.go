package kalshi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/adapter/accountquotes"
)

// CoreMarkets reads only the requested market metadata. Unlike trading books,
// finalized/closed markets remain visible for asset valuation. No write API.
func (client *Client) CoreMarkets(ctx context.Context, tickers []string) ([]accountquotes.KalshiMarket, error) {
	if len(tickers) == 0 || len(tickers) > 100 {
		return nil, fmt.Errorf("invalid market batch")
	}
	seen := map[string]bool{}
	for _, t := range tickers {
		if t == "" || strings.ContainsAny(t, ",\r\n") || seen[t] {
			return nil, fmt.Errorf("invalid market ticker")
		}
		seen[t] = true
	}
	q := url.Values{"tickers": {strings.Join(tickers, ",")}, "limit": {"1000"}}
	var response struct {
		Markets []accountquotes.KalshiMarket `json:"markets"`
		Cursor  string                       `json:"cursor"`
	}
	if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/markets?"+q.Encode(), nil, &response); err != nil {
		return nil, err
	}
	if response.Cursor != "" {
		return nil, fmt.Errorf("market batch incomplete")
	}
	return response.Markets, nil
}

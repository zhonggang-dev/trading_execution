package polymarketdata

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// PositionMark is one wallet position valued at the Data API current price.
// It is a read-only monitoring view and deliberately separate from
// domain.ExternalPosition so reconciliation evidence never changes shape.
type PositionMark struct {
	ConditionID  string         `json:"condition_id"`
	TokenID      string         `json:"token_id"`
	Outcome      string         `json:"outcome"`
	Title        string         `json:"title"`
	Shares       domain.Decimal `json:"shares"`
	CurrentPrice domain.Decimal `json:"current_price"`
	Redeemable   bool           `json:"redeemable"`
}

type wirePositionMark struct {
	ConditionID string      `json:"condition_id"`
	TokenID     string      `json:"token_id"`
	Outcome     string      `json:"outcome"`
	Title       string      `json:"title"`
	Size        wireDecimal `json:"current_size"`
	Current     wireDecimal `json:"current_price"`
	Redeemable  bool        `json:"redeemable"`
}

// ListPositionMarks pages /v2/positions with the same completeness filters as
// reconciliation (no dust floor, archived markets included, settled but
// unredeemed rows retained by the default OPEN status). Zero-share rows are
// dropped because they carry no value.
func (client *PositionClient) ListPositionMarks(ctx context.Context, walletAddress string) ([]PositionMark, error) {
	walletAddress = strings.ToLower(strings.TrimSpace(walletAddress))
	if walletAddress == "" {
		return nil, fmt.Errorf("wallet address is required")
	}
	const pageSize = 500
	marks := make([]PositionMark, 0)
	seen := make(map[string]struct{})
	cursor := ""
	for page := 0; page < maxDataAPIPages; page++ {
		endpoint := client.baseURL.ResolveReference(&url.URL{Path: "/v2/positions"})
		query := endpoint.Query()
		query.Set("user", walletAddress)
		query.Set("filter_type", "TOKENS")
		query.Set("filter_amount", "0")
		query.Set("include_archived", "true")
		query.Set("limit", strconv.Itoa(pageSize))
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		body, err := client.doWithRetry(ctx, request, "positions")
		if err != nil {
			return nil, err
		}
		values, next, err := decodeDataAPIPage[wirePositionMark](body, "positions")
		if err != nil {
			return nil, err
		}
		for index, value := range values {
			tokenID := strings.TrimSpace(value.TokenID)
			if tokenID == "" || strings.TrimSpace(value.ConditionID) == "" {
				return nil, fmt.Errorf("Data API position %d has incomplete identity", index+page*pageSize)
			}
			shares := domain.Decimal(value.Size)
			sign, err := shares.Sign()
			if err != nil || sign < 0 {
				return nil, fmt.Errorf("Data API position %s has invalid shares", tokenID)
			}
			if sign == 0 {
				continue
			}
			price := domain.Decimal(value.Current)
			if price.IsEmpty() {
				return nil, fmt.Errorf("Data API position %s has no current price", tokenID)
			}
			if priceSign, err := price.Sign(); err != nil || priceSign < 0 {
				return nil, fmt.Errorf("Data API position %s has invalid current price", tokenID)
			}
			if _, duplicate := seen[tokenID]; duplicate {
				return nil, fmt.Errorf("Data API returned duplicate token %s", tokenID)
			}
			seen[tokenID] = struct{}{}
			marks = append(marks, PositionMark{
				ConditionID: strings.TrimSpace(value.ConditionID), TokenID: tokenID,
				Outcome: strings.TrimSpace(value.Outcome), Title: strings.TrimSpace(value.Title),
				Shares: shares, CurrentPrice: price, Redeemable: value.Redeemable,
			})
		}
		if next == "" {
			return marks, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("Data API positions pagination exceeded %d pages", maxDataAPIPages)
}

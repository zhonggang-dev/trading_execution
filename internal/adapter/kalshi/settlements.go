package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// settlementRecord is the wire form of one GET /portfolio/settlements row.
// Revenue is reported in whole cents; the dollar form is accepted when a newer
// API revision supplies it.
type settlementRecord struct {
	Ticker         string          `json:"ticker"`
	EventTicker    string          `json:"event_ticker"`
	MarketResult   string          `json:"market_result"`
	YesCount       domain.Decimal  `json:"yes_count_fp"`
	YesTotalCost   domain.Decimal  `json:"yes_total_cost_dollars"`
	NoCount        domain.Decimal  `json:"no_count_fp"`
	NoTotalCost    domain.Decimal  `json:"no_total_cost_dollars"`
	Revenue        *int64          `json:"revenue"`
	RevenueDollars domain.Decimal  `json:"revenue_dollars"`
	FeeCost        domain.Decimal  `json:"fee_cost"`
	SettledTime    string          `json:"settled_time"`
	ValueCents     *int64          `json:"value"`
	ValueDollars   domain.Decimal  `json:"value_dollars"`
	ExchangeIndex  json.RawMessage `json:"exchange_index"`
}

// ListSettlements returns the settlement records of one market ticker for the
// primary subaccount. An unsettled market has no record.
func (client *Client) ListSettlements(ctx context.Context, ticker string) ([]domain.KalshiSettlement, error) {
	ticker, cursor := strings.TrimSpace(ticker), ""
	if ticker == "" {
		return nil, fmt.Errorf("Kalshi settlement ticker is required")
	}
	result := make([]domain.KalshiSettlement, 0, 1)
	for page := 0; page < 20; page++ {
		var envelope struct {
			Settlements *[]json.RawMessage `json:"settlements"`
			Cursor      *string            `json:"cursor"`
		}
		query := url.Values{"ticker": []string{ticker}, "subaccount": []string{"0"}, "limit": []string{"100"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/portfolio/settlements?"+query.Encode(), nil, &envelope); err != nil {
			return nil, err
		}
		if envelope.Settlements == nil {
			return nil, fmt.Errorf("Kalshi settlements response omitted the settlements collection")
		}
		for _, raw := range *envelope.Settlements {
			settlement, err := parseSettlement(raw)
			if err != nil {
				return nil, err
			}
			result = append(result, settlement)
		}
		next := ""
		if envelope.Cursor != nil {
			next = strings.TrimSpace(*envelope.Cursor)
		}
		if next == "" {
			return result, nil
		}
		if next == cursor {
			return nil, fmt.Errorf("Kalshi settlements cursor did not advance")
		}
		cursor = next
	}
	return nil, fmt.Errorf("Kalshi settlement lookup exceeded pagination limit")
}

func parseSettlement(raw json.RawMessage) (domain.KalshiSettlement, error) {
	var record settlementRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return domain.KalshiSettlement{}, fmt.Errorf("decode Kalshi settlement: %w", err)
	}
	settlement := domain.KalshiSettlement{
		Ticker: strings.TrimSpace(record.Ticker), EventTicker: strings.TrimSpace(record.EventTicker),
		MarketResult: strings.ToLower(strings.TrimSpace(record.MarketResult)),
		YesCount:     record.YesCount, YesTotalCost: record.YesTotalCost,
		NoCount: record.NoCount, NoTotalCost: record.NoTotalCost,
		FeeCost: record.FeeCost, Raw: append([]byte(nil), raw...),
	}
	switch {
	case !record.RevenueDollars.IsEmpty():
		settlement.Revenue = record.RevenueDollars
	case record.Revenue != nil:
		settlement.Revenue = centsToDollars(*record.Revenue)
	}
	switch {
	case !record.ValueDollars.IsEmpty():
		settlement.PayoutPerYes = record.ValueDollars
	case record.ValueCents != nil:
		settlement.PayoutPerYes = centsToDollars(*record.ValueCents)
	}
	if text := strings.TrimSpace(record.SettledTime); text != "" {
		settledAt, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return domain.KalshiSettlement{}, fmt.Errorf("Kalshi settlement time is invalid: %w", err)
		}
		settlement.SettledAt = settledAt.UTC()
	}
	return settlement, nil
}

func centsToDollars(cents int64) domain.Decimal {
	return domain.Decimal(new(big.Rat).Quo(big.NewRat(cents, 1), big.NewRat(100, 1)).FloatString(2))
}

// AvailableBalance returns the primary account's cash in dollars.
func (client *Client) AvailableBalance(ctx context.Context) (domain.Decimal, error) {
	balance, err := client.GetBalance(ctx)
	if err != nil {
		return "", err
	}
	return balance.AvailableDollars(), nil
}

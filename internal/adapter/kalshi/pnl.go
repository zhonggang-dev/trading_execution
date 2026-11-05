package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var pnlUserID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var pnlDecimal = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]+)?$`)

type PnLPoint struct {
	Timestamp      int64          `json:"timestamp"`
	CumulativePnL  domain.Decimal `json:"cumulative_pnl"`
	PortfolioValue domain.Decimal `json:"portfolio_value"`
}

type PnLSeries struct {
	PeriodIntervalMinutes int
	ProcessedThrough      time.Time
	Points                []PnLPoint
}

// PnLSource keeps the official member identity on the server, separate from
// the execution account name and API-key identity. Only subaccount 0 is read.
type PnLSource struct {
	client *Client
	userID string
}

func NewPnLSource(client *Client, userID string) (*PnLSource, error) {
	if client == nil || !pnlUserID.MatchString(userID) {
		return nil, fmt.Errorf("Kalshi PnL client and official user id are required")
	}
	isolated := *client
	httpClient := *client.httpClient
	httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	isolated.httpClient = &httpClient
	return &PnLSource{client: &isolated, userID: userID}, nil
}

func IsValidPnLRange(value string) bool {
	switch value {
	case "1d", "1w", "1m", "1y", "all":
		return true
	}
	return false
}

func (source *PnLSource) Series(ctx context.Context, interval string) (PnLSeries, error) {
	if !IsValidPnLRange(interval) {
		return PnLSeries{}, fmt.Errorf("invalid Kalshi PnL range")
	}
	var raw struct {
		Points           json.RawMessage `json:"points"`
		PeriodInterval   *int            `json:"period_interval"`
		ProcessedThrough *time.Time      `json:"processed_through"`
	}
	path := "/v1/users/" + url.PathEscape(source.userID) + "/portfolio/pnl_history?" + url.Values{"range": {interval}, "subaccount_number": {"0"}}.Encode()
	if err := source.client.doAuthenticated(ctx, "GET", path, nil, &raw); err != nil {
		return PnLSeries{}, err
	}
	if raw.PeriodInterval == nil || (*raw.PeriodInterval != 10 && *raw.PeriodInterval != 60 && *raw.PeriodInterval != 1440) || raw.ProcessedThrough == nil || raw.ProcessedThrough.Unix() <= 0 || raw.ProcessedThrough.After(source.client.now()) || len(raw.Points) == 0 || string(raw.Points) == "null" {
		return PnLSeries{}, fmt.Errorf("invalid Kalshi PnL response metadata")
	}
	var points []struct {
		Timestamp      *time.Time      `json:"ts"`
		CumulativePnL  json.RawMessage `json:"cumulative_pnl"`
		PortfolioValue json.RawMessage `json:"portfolio_value"`
	}
	if err := json.Unmarshal(raw.Points, &points); err != nil || len(points) > 10000 {
		return PnLSeries{}, fmt.Errorf("invalid Kalshi PnL points")
	}
	result := PnLSeries{PeriodIntervalMinutes: *raw.PeriodInterval, ProcessedThrough: raw.ProcessedThrough.UTC(), Points: make([]PnLPoint, 0, len(points))}
	seen := make(map[int64]PnLPoint, len(points))
	for _, p := range points {
		pnl, pnlErr := parsePnLDecimal(p.CumulativePnL)
		value, valueErr := parsePnLDecimal(p.PortfolioValue)
		if p.Timestamp == nil || p.Timestamp.Unix() <= 0 || p.Timestamp.Unix() > 9007199254740991 || p.Timestamp.After(source.client.now().Add(time.Minute)) || pnlErr != nil || valueErr != nil {
			return PnLSeries{}, fmt.Errorf("invalid Kalshi PnL point")
		}
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return PnLSeries{}, fmt.Errorf("invalid Kalshi PnL portfolio value")
		}
		point := PnLPoint{Timestamp: p.Timestamp.Unix(), CumulativePnL: pnl, PortfolioValue: value}
		if prior, ok := seen[point.Timestamp]; ok {
			if !prior.CumulativePnL.Equal(point.CumulativePnL) || !prior.PortfolioValue.Equal(point.PortfolioValue) {
				return PnLSeries{}, fmt.Errorf("conflicting Kalshi PnL timestamp")
			}
			continue
		}
		seen[point.Timestamp] = point
		result.Points = append(result.Points, point)
	}
	sort.Slice(result.Points, func(i, j int) bool { return result.Points[i].Timestamp < result.Points[j].Timestamp })
	return result, nil
}

func parsePnLDecimal(raw json.RawMessage) (domain.Decimal, error) {
	// Bound strings before invoking big.Rat parsing, not after it.
	if len(raw) < 3 || len(raw) > 130 || raw[0] != '"' {
		return "", fmt.Errorf("invalid Kalshi PnL amount")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || len(value) > 128 || !pnlDecimal.MatchString(value) {
		return "", fmt.Errorf("invalid Kalshi PnL amount")
	}
	return domain.ParseDecimal(value)
}

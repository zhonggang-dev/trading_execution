package kalshi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	// kalshiGeneralTakerFeeRate is the general trading fee coefficient of the
	// Kalshi fee schedule: fee = multiplier * 0.07 * contracts * P * (1 - P).
	// The API publishes only the per-series multiplier, not this coefficient.
	kalshiGeneralTakerFeeRate = "0.07"
	// kalshiFeeRoundingAllowancePerShare bounds the rounding Kalshi adds to bring
	// each fill's balance change back to the member's balance precision
	// ($0.0001 for direct members, $0.01 otherwise). One cent per contract
	// covers either precision for every fill of at least one contract.
	kalshiFeeRoundingAllowancePerShare = "0.01"
	defaultKalshiFeeScheduleTTL        = 15 * time.Minute
)

type FeeScheduleParams struct {
	Client *Client
	TTL    time.Duration
	Now    func() time.Time
}

// FeeScheduleSource resolves the official taker fee curve of one Kalshi
// market from its series fee structure and any event-level override. Every
// read is GET-only. An error means the fee cannot be confirmed; the caller
// must fall back to the configured cap and never assume a zero fee.
type FeeScheduleSource struct {
	client *Client
	ttl    time.Duration
	now    func() time.Time

	mu    sync.RWMutex
	cache map[string]cachedKalshiFeeSchedule
}

type cachedKalshiFeeSchedule struct {
	schedule  domain.MarketFeeSchedule
	fetchedAt time.Time
}

func NewFeeScheduleSource(params FeeScheduleParams) (*FeeScheduleSource, error) {
	if params.Client == nil {
		return nil, fmt.Errorf("authenticated Kalshi client is required")
	}
	if params.TTL == 0 {
		params.TTL = defaultKalshiFeeScheduleTTL
	}
	if params.TTL < 0 {
		return nil, fmt.Errorf("Kalshi fee schedule TTL must be positive")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &FeeScheduleSource{
		client: params.Client, ttl: params.TTL, now: params.Now,
		cache: make(map[string]cachedKalshiFeeSchedule),
	}, nil
}

// MarketFeeSchedule returns the taker fee curve for marketTicker. An expired
// cached schedule is still the venue's last authoritative statement, so a
// transient refresh failure keeps it instead of discarding known evidence.
func (source *FeeScheduleSource) MarketFeeSchedule(ctx context.Context, marketTicker string) (domain.MarketFeeSchedule, error) {
	marketTicker = strings.TrimSpace(marketTicker)
	if marketTicker == "" {
		return domain.MarketFeeSchedule{}, fmt.Errorf("Kalshi market ticker is required for fee evidence")
	}
	now := source.now().UTC()
	source.mu.RLock()
	cached, exists := source.cache[marketTicker]
	source.mu.RUnlock()
	if exists && now.Sub(cached.fetchedAt) <= source.ttl {
		return cached.schedule, nil
	}
	schedule, err := source.fetch(ctx, marketTicker)
	if err != nil {
		if exists {
			return cached.schedule, nil
		}
		return domain.MarketFeeSchedule{}, err
	}
	schedule.FetchedAt = now
	source.mu.Lock()
	source.cache[marketTicker] = cachedKalshiFeeSchedule{schedule: schedule, fetchedAt: now}
	source.mu.Unlock()
	return schedule, nil
}

func (source *FeeScheduleSource) fetch(ctx context.Context, marketTicker string) (domain.MarketFeeSchedule, error) {
	var market struct {
		Market struct {
			Ticker      string `json:"ticker"`
			EventTicker string `json:"event_ticker"`
		} `json:"market"`
	}
	if err := source.getJSON(ctx, "/trade-api/v2/markets/"+url.PathEscape(marketTicker), &market); err != nil {
		return domain.MarketFeeSchedule{}, fmt.Errorf("read Kalshi market for fee schedule: %w", err)
	}
	eventTicker := strings.TrimSpace(market.Market.EventTicker)
	if strings.TrimSpace(market.Market.Ticker) != marketTicker || eventTicker == "" {
		return domain.MarketFeeSchedule{}, fmt.Errorf("Kalshi market fee lookup returned a different or incomplete market identity")
	}
	var event struct {
		Event struct {
			EventTicker           string          `json:"event_ticker"`
			SeriesTicker          string          `json:"series_ticker"`
			FeeTypeOverride       *string         `json:"fee_type_override"`
			FeeMultiplierOverride json.RawMessage `json:"fee_multiplier_override"`
		} `json:"event"`
	}
	if err := source.getJSON(ctx, "/trade-api/v2/events/"+url.PathEscape(eventTicker)+"?exclude_markets=true", &event); err != nil {
		return domain.MarketFeeSchedule{}, fmt.Errorf("read Kalshi event for fee schedule: %w", err)
	}
	seriesTicker := strings.TrimSpace(event.Event.SeriesTicker)
	if strings.TrimSpace(event.Event.EventTicker) != eventTicker || seriesTicker == "" {
		return domain.MarketFeeSchedule{}, fmt.Errorf("Kalshi event fee lookup returned a different or incomplete event identity")
	}
	var feeType string
	var multiplier domain.Decimal
	if event.Event.FeeTypeOverride != nil {
		feeType = *event.Event.FeeTypeOverride
		value, err := kalshiFeeMultiplier(event.Event.FeeMultiplierOverride)
		if err != nil {
			return domain.MarketFeeSchedule{}, fmt.Errorf("Kalshi event fee_multiplier_override: %w", err)
		}
		multiplier = value
	} else {
		var series struct {
			Series struct {
				Ticker        string          `json:"ticker"`
				FeeType       string          `json:"fee_type"`
				FeeMultiplier json.RawMessage `json:"fee_multiplier"`
			} `json:"series"`
		}
		if err := source.getJSON(ctx, "/trade-api/v2/series/"+url.PathEscape(seriesTicker), &series); err != nil {
			return domain.MarketFeeSchedule{}, fmt.Errorf("read Kalshi series for fee schedule: %w", err)
		}
		if strings.TrimSpace(series.Series.Ticker) != seriesTicker {
			return domain.MarketFeeSchedule{}, fmt.Errorf("Kalshi series fee lookup returned a different series identity")
		}
		feeType = series.Series.FeeType
		value, err := kalshiFeeMultiplier(series.Series.FeeMultiplier)
		if err != nil {
			return domain.MarketFeeSchedule{}, fmt.Errorf("Kalshi series fee_multiplier: %w", err)
		}
		multiplier = value
	}
	return kalshiTakerFeeSchedule(feeType, multiplier)
}

// kalshiTakerFeeSchedule maps a Kalshi fee structure to the taker curve.
// Execution submits IOC orders only, so every fill takes liquidity; the maker
// fee of the *_maker_fees structures is a fraction of the taker fee and never
// exceeds it. The flat structure follows a separate table and is left to the
// configured cap.
func kalshiTakerFeeSchedule(feeType string, multiplier domain.Decimal) (domain.MarketFeeSchedule, error) {
	switch strings.TrimSpace(feeType) {
	case "quadratic", "quadratic_with_maker_fees", "quadratic_with_combo_maker_fees":
	default:
		return domain.MarketFeeSchedule{}, fmt.Errorf("Kalshi fee type %q is not a supported quadratic fee structure", feeType)
	}
	rate, err := multiplier.Multiply(kalshiGeneralTakerFeeRate)
	if err != nil {
		return domain.MarketFeeSchedule{}, fmt.Errorf("calculate Kalshi taker fee rate: %w", err)
	}
	return domain.MarketFeeSchedule{
		PlatformFeeRate:           domain.Decimal(rate.FloatString(multiplier.Scale() + 2)).Canonical(),
		FeeExponent:               "1",
		RoundingAllowancePerShare: kalshiFeeRoundingAllowancePerShare,
		TakerOnly:                 true,
	}, nil
}

func kalshiFeeMultiplier(raw json.RawMessage) (domain.Decimal, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", fmt.Errorf("fee multiplier is missing")
	}
	var number json.Number
	if err := json.Unmarshal(trimmed, &number); err != nil {
		return "", fmt.Errorf("fee multiplier must be a JSON number")
	}
	value := domain.Decimal(number.String())
	if sign, err := value.Sign(); err != nil || sign < 0 {
		return "", fmt.Errorf("fee multiplier must be a non-negative decimal")
	}
	return value.Canonical(), nil
}

func (source *FeeScheduleSource) getJSON(ctx context.Context, requestPath string, target any) error {
	return source.client.doAuthenticated(ctx, http.MethodGet, requestPath, nil, target)
}

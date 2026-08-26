package kalshi

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const kalshiCandlestickLimit = 10_000

type MidPriceHistoryParams struct {
	Client *Client
	Now    func() time.Time
}

// MidPriceHistorySource converts one-minute Kalshi YES bid/ask candles into
// the venue-neutral raw mid-price history required by multfactor_v2.
type MidPriceHistorySource struct {
	client *Client
	now    func() time.Time
}

type candlestickEnvelope struct {
	Markets []struct {
		MarketTicker string              `json:"market_ticker"`
		Candlesticks []kalshiCandlestick `json:"candlesticks"`
	} `json:"markets"`
}

type kalshiCandlestick struct {
	EndPeriodTS int64 `json:"end_period_ts"`
	YesBid      struct {
		Close domain.Decimal `json:"close_dollars"`
	} `json:"yes_bid"`
	YesAsk struct {
		Close domain.Decimal `json:"close_dollars"`
	} `json:"yes_ask"`
}

func NewMidPriceHistorySource(params MidPriceHistoryParams) (*MidPriceHistorySource, error) {
	if params.Client == nil {
		return nil, fmt.Errorf("authenticated Kalshi client is required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &MidPriceHistorySource{client: params.Client, now: params.Now}, nil
}

func (source *MidPriceHistorySource) Capture(
	ctx context.Context,
	decisionAt time.Time,
	lookback time.Duration,
	targets []domain.BookTarget,
) ([]domain.MidPriceHistory, error) {
	decisionAt = decisionAt.UTC()
	if decisionAt.IsZero() || lookback < time.Minute {
		return nil, fmt.Errorf("Kalshi mid-price decision time and lookback are required")
	}
	if len(targets) == 0 {
		return []domain.MidPriceHistory{}, nil
	}
	windowStart := decisionAt.Add(-lookback)
	fetchedAt := source.now().UTC()
	histories := make([]domain.MidPriceHistory, len(targets))
	indicesByTicker := make(map[string][]int)
	for index, target := range targets {
		if target.MarketSource.Normalize() != domain.MarketSourceKalshi {
			return nil, fmt.Errorf("Kalshi mid-price source received non-Kalshi target %q", target.TokenID)
		}
		histories[index] = newKalshiHistory(target, windowStart, decisionAt, fetchedAt)
		indicesByTicker[target.MarketID] = append(indicesByTicker[target.MarketID], index)
	}
	tickers := make([]string, 0, len(indicesByTicker))
	for ticker := range indicesByTicker {
		tickers = append(tickers, ticker)
	}
	sort.Strings(tickers)
	pointsPerMarket := int(lookback/time.Minute) + 1
	batchSize := min(100, max(1, kalshiCandlestickLimit/pointsPerMarket))
	for start := 0; start < len(tickers); start += batchSize {
		end := min(start+batchSize, len(tickers))
		batchTickers := tickers[start:end]
		var payload candlestickEnvelope
		requestPath := candlestickPath(batchTickers, windowStart, decisionAt)
		if err := source.client.doAuthenticated(ctx, http.MethodGet, requestPath, nil, &payload); err != nil {
			markHistoryBatchError(histories, indicesByTicker, batchTickers, "KALSHI_CANDLESTICK_REQUEST_FAILED")
			continue
		}
		returned := make(map[string]struct{}, len(payload.Markets))
		for _, market := range payload.Markets {
			indices, wanted := indicesByTicker[market.MarketTicker]
			if !wanted {
				continue
			}
			returned[market.MarketTicker] = struct{}{}
			for _, index := range indices {
				points, err := normalizeKalshiCandles(
					market.Candlesticks, targets[index].OutcomeID, windowStart, decisionAt,
				)
				if err != nil {
					histories[index].Status = domain.MidPriceHistoryStatusError
					histories[index].ErrorCode = "KALSHI_INVALID_CANDLESTICKS"
					continue
				}
				applyKalshiHistoryPoints(&histories[index], points)
			}
		}
		for _, ticker := range batchTickers {
			if _, found := returned[ticker]; found {
				continue
			}
			for _, index := range indicesByTicker[ticker] {
				histories[index].Status = domain.MidPriceHistoryStatusMissing
				histories[index].ErrorCode = "SOURCE_DID_NOT_RETURN_MARKET"
			}
		}
	}
	return histories, nil
}

func candlestickPath(tickers []string, windowStart, windowEnd time.Time) string {
	query := url.Values{}
	query.Set("market_tickers", strings.Join(tickers, ","))
	query.Set("start_ts", strconv.FormatInt(windowStart.Unix(), 10))
	query.Set("end_ts", strconv.FormatInt(windowEnd.Unix(), 10))
	query.Set("period_interval", "1")
	return "/trade-api/v2/markets/candlesticks?" + query.Encode()
}

func newKalshiHistory(target domain.BookTarget, windowStart, windowEnd, fetchedAt time.Time) domain.MidPriceHistory {
	return domain.MidPriceHistory{
		MarketID: target.MarketID, ConditionID: target.ConditionID,
		OutcomeIndex: target.OutcomeIndex, TokenID: target.TokenID,
		Status:      domain.MidPriceHistoryStatusError,
		WindowStart: windowStart, WindowEnd: windowEnd, FidelitySeconds: 60,
		Sampling: domain.MidPriceSamplingUpstreamRaw, MissingValues: domain.MidPriceMissingValuePolicyNoFill,
		TimestampSemantics: domain.MidPriceTimestampSemanticsIntervalEndUTC,
		FetchedAt:          fetchedAt, MidPrices: []domain.MidPricePoint{},
	}
}

func normalizeKalshiCandles(raw []kalshiCandlestick, outcomeID string, windowStart, windowEnd time.Time) ([]domain.MidPricePoint, error) {
	outcomeID = strings.ToUpper(strings.TrimSpace(outcomeID))
	if outcomeID != "YES" && outcomeID != "NO" {
		return nil, fmt.Errorf("Kalshi outcome_id must be YES or NO")
	}
	byTimestamp := make(map[int64]domain.MidPricePoint, len(raw))
	for _, typed := range raw {
		candle := struct {
			EndPeriodTS int64
			YesBidClose domain.Decimal
			YesAskClose domain.Decimal
		}{
			EndPeriodTS: typed.EndPeriodTS,
			YesBidClose: typed.YesBid.Close,
			YesAskClose: typed.YesAsk.Close,
		}
		intervalEnd := time.Unix(candle.EndPeriodTS, 0).UTC()
		if intervalEnd.Before(windowStart) || intervalEnd.After(windowEnd) || candle.YesBidClose.IsEmpty() || candle.YesAskClose.IsEmpty() {
			continue
		}
		mid, err := midpoint(candle.YesBidClose, candle.YesAskClose)
		if err != nil {
			return nil, err
		}
		if outcomeID == "NO" {
			mid, err = complement(mid)
			if err != nil {
				return nil, err
			}
		}
		byTimestamp[candle.EndPeriodTS] = domain.MidPricePoint{IntervalEndAt: intervalEnd, P: mid}
	}
	points := make([]domain.MidPricePoint, 0, len(byTimestamp))
	for _, point := range byTimestamp {
		points = append(points, point)
	}
	sort.Slice(points, func(left, right int) bool { return points[left].IntervalEndAt.Before(points[right].IntervalEndAt) })
	return points, nil
}

func midpoint(bid, ask domain.Decimal) (domain.Decimal, error) {
	bidRat, bidOK := new(big.Rat).SetString(bid.String())
	askRat, askOK := new(big.Rat).SetString(ask.String())
	if !bidOK || !askOK || bidRat.Sign() <= 0 || askRat.Sign() <= 0 || bidRat.Cmp(askRat) > 0 || askRat.Cmp(big.NewRat(1, 1)) >= 0 {
		return "", fmt.Errorf("invalid Kalshi candle spread")
	}
	return rationalDecimal(new(big.Rat).Quo(new(big.Rat).Add(bidRat, askRat), big.NewRat(2, 1)))
}

func complement(value domain.Decimal) (domain.Decimal, error) {
	rat, ok := new(big.Rat).SetString(value.String())
	if !ok {
		return "", fmt.Errorf("invalid Kalshi midpoint")
	}
	return rationalDecimal(new(big.Rat).Sub(big.NewRat(1, 1), rat))
}

func rationalDecimal(value *big.Rat) (domain.Decimal, error) {
	formatted := strings.TrimRight(strings.TrimRight(value.FloatString(8), "0"), ".")
	return domain.ParseDecimal(formatted)
}

func applyKalshiHistoryPoints(history *domain.MidPriceHistory, points []domain.MidPricePoint) {
	if len(points) == 0 {
		history.Status = domain.MidPriceHistoryStatusEmpty
		history.ErrorCode = "KALSHI_NO_MID_PRICE_HISTORY"
		return
	}
	history.MidPrices = points
	history.CoverageStart = points[0].IntervalEndAt
	history.CoverageEnd = points[len(points)-1].IntervalEndAt
	coverage := history.CoverageEnd.Sub(history.CoverageStart)
	if coverage < time.Duration(domain.StrategyMinimumHistorySeconds)*time.Second || history.CoverageEnd.Before(history.WindowEnd.Add(-2*time.Minute)) {
		history.Status = domain.MidPriceHistoryStatusPartial
		return
	}
	history.Status = domain.MidPriceHistoryStatusOK
}

func markHistoryBatchError(histories []domain.MidPriceHistory, indicesByTicker map[string][]int, tickers []string, code string) {
	for _, ticker := range tickers {
		for _, index := range indicesByTicker[ticker] {
			histories[index].Status = domain.MidPriceHistoryStatusError
			histories[index].ErrorCode = code
		}
	}
}

var _ port.MidPriceHistorySource = (*MidPriceHistorySource)(nil)

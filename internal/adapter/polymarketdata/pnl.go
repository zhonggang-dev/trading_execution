package polymarketdata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const maxPnLResponseBytes = 8 << 20
const maxPnLPoints = 10_000
const maxPnLNumberChars = 256
const maxPnLNormalizedChars = 256

// PnLSeries 是 Polymarket Data API 的累计 trade_pnl 序列。
type PnLSeries struct {
	Points []PnLPoint
}

type PnLPoint struct {
	Timestamp int64
	TradePnL  domain.Decimal
}

// PnLClient 只读访问公开的 /v2/user-pnl 接口。
type PnLClient struct {
	baseURL    *url.URL
	httpClient *http.Client
}

type PnLClientParams struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewPnLClient(params PnLClientParams) (*PnLClient, error) {
	base, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("Polymarket Data API base URL is invalid")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &PnLClient{baseURL: base, httpClient: params.HTTPClient}, nil
}

func (client *PnLClient) Series(ctx context.Context, walletAddress, interval, fidelity string) (PnLSeries, error) {
	endpoint := client.baseURL.ResolveReference(&url.URL{Path: "/v2/user-pnl"})
	query := endpoint.Query()
	query.Set("user", strings.ToLower(strings.TrimSpace(walletAddress)))
	query.Set("interval", interval)
	query.Set("fidelity", fidelity)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return PnLSeries{}, fmt.Errorf("create Data API PnL request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return PnLSeries{}, fmt.Errorf("query Data API PnL: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxPnLResponseBytes+1))
	response.Body.Close()
	if readErr != nil {
		return PnLSeries{}, fmt.Errorf("read Data API PnL: %w", readErr)
	}
	if len(body) > maxPnLResponseBytes {
		return PnLSeries{}, fmt.Errorf("Data API PnL response is too large")
	}
	if response.StatusCode != http.StatusOK {
		return PnLSeries{}, fmt.Errorf("Data API PnL returned HTTP %d", response.StatusCode)
	}
	var envelope struct {
		Data *struct {
			Points *[]struct {
				Timestamp int64           `json:"timestamp"`
				TradePnL  json.RawMessage `json:"trade_pnl"`
			} `json:"points"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&envelope); err != nil {
		return PnLSeries{}, fmt.Errorf("decode Data API PnL: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return PnLSeries{}, fmt.Errorf("decode Data API PnL: trailing JSON document")
		}
		return PnLSeries{}, fmt.Errorf("decode Data API PnL trailing data: %w", err)
	}
	if envelope.Data == nil || envelope.Data.Points == nil {
		return PnLSeries{}, fmt.Errorf("decode Data API PnL: data.points is required")
	}
	if len(*envelope.Data.Points) > maxPnLPoints {
		return PnLSeries{}, fmt.Errorf("Data API PnL contains too many points")
	}
	points := make([]PnLPoint, 0, len(*envelope.Data.Points))
	seen := make(map[int64]domain.Decimal, len(*envelope.Data.Points))
	for _, point := range *envelope.Data.Points {
		if point.Timestamp <= 0 {
			return PnLSeries{}, fmt.Errorf("invalid Data API PnL timestamp")
		}
		if len(point.TradePnL) == 0 {
			return PnLSeries{}, fmt.Errorf("Data API PnL point is missing trade_pnl")
		}
		if string(point.TradePnL) == "null" {
			continue
		}
		value := strings.TrimSpace(string(point.TradePnL))
		if strings.HasPrefix(value, "\"") {
			var text string
			if err := json.Unmarshal(point.TradePnL, &text); err != nil {
				return PnLSeries{}, fmt.Errorf("decode Data API trade_pnl: %w", err)
			}
			value = text
		}
		pnl, err := normalizePnLDecimal(value)
		if err != nil {
			return PnLSeries{}, fmt.Errorf("invalid Data API trade_pnl")
		}
		if previous, exists := seen[point.Timestamp]; exists {
			if comparison, compareErr := pnl.Compare(previous); compareErr != nil || comparison != 0 {
				return PnLSeries{}, fmt.Errorf("conflicting Data API PnL points")
			}
			continue
		}
		seen[point.Timestamp] = pnl
		points = append(points, PnLPoint{Timestamp: point.Timestamp, TradePnL: pnl})
	}
	sort.Slice(points, func(left, right int) bool { return points[left].Timestamp < points[right].Timestamp })
	return PnLSeries{Points: points}, nil
}

// normalizePnLDecimal accepts only bounded base-10 decimal syntax and expands
// an optional exponent exactly. It intentionally does not use float64: PnL
// values are money-like data and binary rounding would make equivalent API
// points compare differently. The output is always a non-exponent Decimal.
func normalizePnLDecimal(raw string) (domain.Decimal, error) {
	value := strings.TrimSpace(raw)
	if value == "" || len(value) > maxPnLNumberChars {
		return "", fmt.Errorf("decimal lexeme is out of bounds")
	}
	negative := false
	if value[0] == '+' || value[0] == '-' {
		negative = value[0] == '-'
		value = value[1:]
	}
	if value == "" {
		return "", fmt.Errorf("decimal sign has no digits")
	}
	exponent := 0
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		if strings.IndexAny(value[index+1:], "eE") >= 0 {
			return "", fmt.Errorf("decimal has multiple exponents")
		}
		exponentText := value[index+1:]
		if exponentText == "" {
			return "", fmt.Errorf("decimal exponent is empty")
		}
		parsedExponent, exponentErr := parseBoundedExponent(exponentText)
		if exponentErr != nil {
			return "", exponentErr
		}
		exponent = parsedExponent
		value = value[:index]
	}
	dot := strings.IndexByte(value, '.')
	if dot >= 0 && strings.IndexByte(value[dot+1:], '.') >= 0 {
		return "", fmt.Errorf("decimal has multiple points")
	}
	integerPart, fractionPart := value, ""
	if dot >= 0 {
		integerPart, fractionPart = value[:dot], value[dot+1:]
	}
	if integerPart == "" && fractionPart == "" {
		return "", fmt.Errorf("decimal has no digits")
	}
	for _, digit := range integerPart + fractionPart {
		if digit < '0' || digit > '9' {
			return "", fmt.Errorf("decimal has non-base10 digits")
		}
	}
	digits := integerPart + fractionPart
	if digits == "" {
		return "", fmt.Errorf("decimal has no digits")
	}
	position := len(integerPart) + exponent
	leading := 0
	for leading < len(digits)-1 && digits[leading] == '0' {
		leading++
	}
	digits = digits[leading:]
	position -= leading
	allZero := true
	for _, digit := range digits {
		if digit != '0' {
			allZero = false
			break
		}
	}
	if allZero {
		return domain.Decimal("0"), nil
	}
	var normalized string
	switch {
	case position <= 0:
		normalized = "0." + strings.Repeat("0", -position) + digits
	case position >= len(digits):
		normalized = digits + strings.Repeat("0", position-len(digits))
	default:
		normalized = digits[:position] + "." + digits[position:]
	}
	if strings.Contains(normalized, ".") {
		normalized = strings.TrimRight(strings.TrimRight(normalized, "0"), ".")
	}
	if len(normalized) > maxPnLNormalizedChars {
		return "", fmt.Errorf("decimal expansion is out of bounds")
	}
	if negative {
		normalized = "-" + normalized
	}
	return domain.ParseDecimal(normalized)
}

func parseBoundedExponent(raw string) (int, error) {
	negative := false
	if raw[0] == '+' || raw[0] == '-' {
		negative = raw[0] == '-'
		raw = raw[1:]
	}
	if raw == "" || len(raw) > 4 {
		return 0, fmt.Errorf("decimal exponent is out of bounds")
	}
	value := 0
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("decimal exponent is invalid")
		}
		value = value*10 + int(digit-'0')
		if value > maxPnLNormalizedChars {
			return 0, fmt.Errorf("decimal exponent is out of bounds")
		}
	}
	if negative {
		return -value, nil
	}
	return value, nil
}

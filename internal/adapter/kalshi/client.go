package kalshi

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const maxAuthenticatedResponseBytes = 8 << 20

type ClientParams struct {
	BaseURL            string
	APIKeyID           string
	PrivateKeyPath     string
	PrivateKey         *rsa.PrivateKey
	HTTPClient         *http.Client
	Now                func() time.Time
	LiveTradingEnabled bool
}

// Client signs Kalshi requests with RSA-PSS. Read methods are always
// available; the state-changing submit method is fail-closed behind an
// independent configuration gate.
type Client struct {
	baseURL            *url.URL
	apiKeyID           string
	privateKey         *rsa.PrivateKey
	httpClient         *http.Client
	now                func() time.Time
	liveTradingEnabled bool
}

type Balance struct {
	Balance        int64          `json:"balance"`
	BalanceDollars domain.Decimal `json:"balance_dollars,omitempty"`
	PortfolioValue int64          `json:"portfolio_value"`
}

type APIKey struct {
	ID     string   `json:"api_key_id"`
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

type Capabilities struct {
	Authenticated bool
	Read          bool
	Write         bool
}

type OrderRequestV2 struct {
	Ticker                  string `json:"ticker"`
	ClientOrderID           string `json:"client_order_id"`
	Side                    string `json:"side"`
	Count                   string `json:"count"`
	Price                   string `json:"price"`
	TimeInForce             string `json:"time_in_force"`
	SelfTradePreventionType string `json:"self_trade_prevention_type"`
	ExpirationTime          *int64 `json:"expiration_time,omitempty"`
	PostOnly                bool   `json:"post_only"`
	CancelOrderOnPause      bool   `json:"cancel_order_on_pause"`
	ReduceOnly              bool   `json:"reduce_only"`
	Subaccount              int    `json:"subaccount"`
}

type PreparedOrder struct {
	Request     OrderRequestV2
	body        []byte
	fingerprint string
}

func (order PreparedOrder) Fingerprint() string { return order.fingerprint }

type SubmittedOrder struct {
	OrderID          string         `json:"order_id"`
	ClientOrderID    string         `json:"client_order_id"`
	FillCount        domain.Decimal `json:"fill_count"`
	RemainingCount   domain.Decimal `json:"remaining_count"`
	TimestampMS      int64          `json:"ts_ms"`
	AverageFillPrice domain.Decimal `json:"average_fill_price,omitempty"`
	AverageFeePaid   domain.Decimal `json:"average_fee_paid,omitempty"`
}

type Order struct {
	OrderID        string         `json:"order_id"`
	ClientOrderID  string         `json:"client_order_id"`
	Ticker         string         `json:"ticker"`
	Status         string         `json:"status"`
	FillCount      domain.Decimal `json:"fill_count_fp"`
	RemainingCount domain.Decimal `json:"remaining_count_fp"`
	InitialCount   domain.Decimal `json:"initial_count_fp"`
	TakerFillCost  domain.Decimal `json:"taker_fill_cost_dollars"`
	MakerFillCost  domain.Decimal `json:"maker_fill_cost_dollars"`
	TakerFees      domain.Decimal `json:"taker_fees_dollars"`
	MakerFees      domain.Decimal `json:"maker_fees_dollars"`
	LastUpdateTime time.Time      `json:"last_update_time"`
}

type Fill struct {
	FillID       string         `json:"fill_id"`
	OrderID      string         `json:"order_id"`
	Ticker       string         `json:"ticker"`
	MarketTicker string         `json:"market_ticker"`
	OutcomeSide  string         `json:"outcome_side"`
	Count        domain.Decimal `json:"count_fp"`
	YesPrice     domain.Decimal `json:"yes_price_dollars"`
	NoPrice      domain.Decimal `json:"no_price_dollars"`
	IsTaker      bool           `json:"is_taker"`
	FeeCost      domain.Decimal `json:"fee_cost"`
	Action       string         `json:"action"`
	CreatedTime  time.Time      `json:"created_time"`
}

func NewClient(params ClientParams) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("Kalshi API base URL is invalid")
	}
	params.APIKeyID = strings.TrimSpace(params.APIKeyID)
	if params.APIKeyID == "" {
		return nil, fmt.Errorf("Kalshi API key id is required")
	}
	privateKey := params.PrivateKey
	if privateKey == nil {
		privateKey, err = loadPrivateKey(strings.TrimSpace(params.PrivateKeyPath))
		if err != nil {
			return nil, err
		}
	}
	if err := privateKey.Validate(); err != nil {
		return nil, fmt.Errorf("validate Kalshi private key: %w", err)
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Client{
		baseURL: baseURL, apiKeyID: params.APIKeyID, privateKey: privateKey,
		httpClient: params.HTTPClient, now: params.Now, liveTradingEnabled: params.LiveTradingEnabled,
	}, nil
}

func (client *Client) GetBalance(ctx context.Context) (Balance, error) {
	var balance Balance
	if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/portfolio/balance", nil, &balance); err != nil {
		return Balance{}, err
	}
	return balance, nil
}

func (balance Balance) AvailableDollars() domain.Decimal {
	if !balance.BalanceDollars.IsEmpty() {
		return balance.BalanceDollars
	}
	return domain.Decimal(new(big.Rat).Quo(big.NewRat(balance.Balance, 1), big.NewRat(100, 1)).FloatString(2))
}

func (client *Client) GetOrder(ctx context.Context, orderID string) (Order, error) {
	var envelope struct {
		Order Order `json:"order"`
	}
	requestPath := "/trade-api/v2/portfolio/orders/" + url.PathEscape(strings.TrimSpace(orderID))
	if err := client.doAuthenticated(ctx, http.MethodGet, requestPath, nil, &envelope); err != nil {
		return Order{}, err
	}
	return envelope.Order, nil
}

func (client *Client) FindOrderByClientOrderID(ctx context.Context, clientOrderID string) (Order, error) {
	var envelope struct {
		Orders []Order `json:"orders"`
	}
	if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/portfolio/orders?limit=1000", nil, &envelope); err != nil {
		return Order{}, err
	}
	for _, order := range envelope.Orders {
		if order.ClientOrderID == strings.TrimSpace(clientOrderID) {
			return order, nil
		}
	}
	return Order{}, fmt.Errorf("Kalshi order for client_order_id was not found")
}

func (client *Client) CancelOrder(ctx context.Context, orderID string) (Order, error) {
	if !client.liveTradingEnabled {
		return Order{}, fmt.Errorf("Kalshi live trading is disabled")
	}
	var envelope struct {
		Order Order `json:"order"`
	}
	requestPath := "/trade-api/v2/portfolio/events/orders/" + url.PathEscape(strings.TrimSpace(orderID))
	if err := client.doAuthenticated(ctx, http.MethodDelete, requestPath, nil, &envelope); err != nil {
		return Order{}, err
	}
	return envelope.Order, nil
}

func (client *Client) ListFills(ctx context.Context, orderID string) ([]Fill, error) {
	var envelope struct {
		Fills []Fill `json:"fills"`
	}
	requestPath := "/trade-api/v2/portfolio/fills?order_id=" + url.QueryEscape(strings.TrimSpace(orderID)) + "&limit=1000"
	if err := client.doAuthenticated(ctx, http.MethodGet, requestPath, nil, &envelope); err != nil {
		return nil, err
	}
	return envelope.Fills, nil
}

// ListOrderFills returns Kalshi's authoritative account fills for the exact
// venue order. The API reports final fee_cost per fill, so no fee is inferred.
func (client *Client) ListOrderFills(ctx context.Context, order domain.Order) ([]domain.Fill, error) {
	fills, err := client.ListFills(ctx, order.VenueOrderID)
	if err != nil {
		return nil, err
	}
	result := make([]domain.Fill, 0, len(fills))
	for _, raw := range fills {
		remoteTicker := strings.TrimSpace(raw.Ticker)
		if remoteTicker == "" {
			remoteTicker = strings.TrimSpace(raw.MarketTicker)
		}
		if raw.OrderID != order.VenueOrderID || remoteTicker != order.Intent.MarketID {
			return nil, fmt.Errorf("Kalshi fill identity does not match order")
		}
		price := raw.YesPrice
		if strings.EqualFold(order.Intent.OutcomeID, "NO") {
			price = raw.NoPrice
		}
		shares, priceRat, fee, ok := new(big.Rat), new(big.Rat), new(big.Rat), false
		if shares, ok = shares.SetString(raw.Count.String()); !ok {
			return nil, fmt.Errorf("invalid Kalshi fill count")
		}
		if priceRat, ok = priceRat.SetString(price.String()); !ok {
			return nil, fmt.Errorf("invalid Kalshi fill price")
		}
		if fee, ok = fee.SetString(raw.FeeCost.String()); !ok {
			return nil, fmt.Errorf("invalid Kalshi fill fee")
		}
		gross := new(big.Rat).Mul(shares, priceRat)
		role := domain.LiquidityRoleMaker
		if raw.IsTaker {
			role = domain.LiquidityRoleTaker
		}
		payload, _ := json.Marshal(raw)
		digest := sha256.Sum256(payload)
		confirmedAt := raw.CreatedTime.UTC()
		result = append(result, domain.Fill{
			VenueFillID: raw.FillID, Venue: "kalshi", VenueOrderID: raw.OrderID,
			LiquidityRole: role, Status: domain.FillStatusConfirmed, Shares: raw.Count, Price: price,
			GrossNotional: domain.Decimal(gross.FloatString(8)), FeeRateBPS: "0", PlatformFeeRate: "0",
			FeeExponent: "0", PlatformFee: domain.Decimal(fee.FloatString(8)), BuilderFeeRateBPS: "0",
			BuilderFee: "0", TotalFee: domain.Decimal(fee.FloatString(8)), FeeSource: "KALSHI_API",
			MatchedAt: raw.CreatedTime.UTC(), VenueUpdatedAt: raw.CreatedTime.UTC(), ObservedAt: client.now().UTC(),
			ConfirmedAt: &confirmedAt, RawPayloadSHA256: hex.EncodeToString(digest[:]),
		})
	}
	return result, nil
}

func (client *Client) GetAPIKeys(ctx context.Context) ([]APIKey, error) {
	var envelope struct {
		APIKeys []APIKey `json:"api_keys"`
	}
	if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/api_keys", nil, &envelope); err != nil {
		return nil, err
	}
	return envelope.APIKeys, nil
}

func (client *Client) ProbeCapabilities(ctx context.Context) (Capabilities, error) {
	if _, err := client.GetBalance(ctx); err != nil {
		return Capabilities{}, fmt.Errorf("probe Kalshi balance: %w", err)
	}
	keys, err := client.GetAPIKeys(ctx)
	if err != nil {
		return Capabilities{}, fmt.Errorf("probe Kalshi API key scopes: %w", err)
	}
	result := Capabilities{Authenticated: true}
	for _, key := range keys {
		if key.ID != client.apiKeyID {
			continue
		}
		for _, scope := range key.Scopes {
			switch strings.ToLower(strings.TrimSpace(scope)) {
			case "read":
				result.Read = true
			case "write":
				result.Write = true
			}
		}
	}
	if !result.Read {
		return result, fmt.Errorf("Kalshi API key is authenticated but lacks read scope")
	}
	return result, nil
}

// PrepareOrder maps a venue-neutral Trading intent to Kalshi V2's single YES
// book. It performs validation and serialization only; no request is sent.
func (client *Client) PrepareOrder(intent domain.OrderIntent) (PreparedOrder, error) {
	intent = intent.Normalize()
	if err := intent.Validate(); err != nil {
		return PreparedOrder{}, fmt.Errorf("validate Kalshi order intent: %w", err)
	}
	if intent.MarketSource.Normalize() != domain.MarketSourceKalshi || intent.Venue != "kalshi" {
		return PreparedOrder{}, fmt.Errorf("Kalshi client requires a KALSHI intent")
	}
	price, err := fixedDecimal(intent.WorstPrice, 4)
	if err != nil {
		return PreparedOrder{}, fmt.Errorf("Kalshi order price: %w", err)
	}
	count, err := fixedDecimal(intent.Size, 2)
	if err != nil {
		return PreparedOrder{}, fmt.Errorf("Kalshi order count: %w", err)
	}
	request := OrderRequestV2{
		Ticker: intent.MarketID, ClientOrderID: intent.ClientOrderID, Count: count,
		TimeInForce: "fill_or_kill", SelfTradePreventionType: "taker_at_cross",
		CancelOrderOnPause: true, ReduceOnly: intent.Side == domain.SideSell, Subaccount: 0,
	}
	request.Side, request.Price, err = mapBookSideAndPrice(intent.Side, intent.OutcomeID, price)
	if err != nil {
		return PreparedOrder{}, err
	}
	switch intent.TimeInForce {
	case domain.TimeInForceFOK:
		request.TimeInForce = "fill_or_kill"
	case domain.TimeInForceIOC:
		request.TimeInForce = "immediate_or_cancel"
	case domain.TimeInForceGTC:
		request.TimeInForce = "good_till_canceled"
	case domain.TimeInForceGTD:
		request.TimeInForce = "good_till_canceled"
		if intent.ExpiresAt == nil {
			return PreparedOrder{}, fmt.Errorf("Kalshi GTD order requires expires_at")
		}
		expiresAt := intent.ExpiresAt.UTC().Unix()
		request.ExpirationTime = &expiresAt
	default:
		return PreparedOrder{}, fmt.Errorf("Kalshi does not support time_in_force %q", intent.TimeInForce)
	}
	body, err := json.Marshal(request)
	if err != nil {
		return PreparedOrder{}, fmt.Errorf("encode Kalshi order: %w", err)
	}
	digest := sha256.Sum256(body)
	return PreparedOrder{Request: request, body: body, fingerprint: hex.EncodeToString(digest[:])}, nil
}

// SubmitPrepared is the only state-changing method. Production composition
// must explicitly enable it; dry-run and tests remain fail-closed by default.
func (client *Client) SubmitPrepared(ctx context.Context, order PreparedOrder) (SubmittedOrder, error) {
	if !client.liveTradingEnabled {
		return SubmittedOrder{}, fmt.Errorf("Kalshi live trading is disabled")
	}
	if len(order.body) == 0 || strings.TrimSpace(order.Request.ClientOrderID) == "" || order.Fingerprint() == "" {
		return SubmittedOrder{}, fmt.Errorf("Kalshi prepared order is invalid")
	}
	var response SubmittedOrder
	if err := client.doAuthenticated(ctx, http.MethodPost, "/trade-api/v2/portfolio/events/orders", order.body, &response); err != nil {
		return SubmittedOrder{}, err
	}
	if response.OrderID == "" || response.ClientOrderID != order.Request.ClientOrderID {
		return SubmittedOrder{}, fmt.Errorf("Kalshi order acknowledgement identity is invalid")
	}
	return response, nil
}

func (client *Client) doAuthenticated(ctx context.Context, method, requestPath string, body []byte, target any) error {
	endpoint, err := client.baseURL.Parse(requestPath)
	if err != nil {
		return err
	}
	timestamp := fmt.Sprintf("%d", client.now().UTC().UnixMilli())
	message := timestamp + method + endpoint.EscapedPath()
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPSS(rand.Reader, client.privateKey, crypto.SHA256, digest[:], &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA256,
	})
	if err != nil {
		return fmt.Errorf("sign Kalshi request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("KALSHI-ACCESS-KEY", client.apiKeyID)
	request.Header.Set("KALSHI-ACCESS-TIMESTAMP", timestamp)
	request.Header.Set("KALSHI-ACCESS-SIGNATURE", base64.StdEncoding.EncodeToString(signature))
	if len(body) != 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("request Kalshi API: %w", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxAuthenticatedResponseBytes+1))
	if err != nil {
		return err
	}
	if len(payload) > maxAuthenticatedResponseBytes {
		return fmt.Errorf("Kalshi response exceeds %d bytes", maxAuthenticatedResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Kalshi HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode Kalshi response: %w", err)
	}
	return nil
}

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	if path == "" {
		return nil, fmt.Errorf("Kalshi private key path is required")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect Kalshi private key: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("Kalshi private key must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("Kalshi private key must not be accessible by group or other users")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Kalshi private key: %w", err)
	}
	block, _ := pem.Decode(encoded)
	if block == nil {
		return nil, fmt.Errorf("decode Kalshi private key PEM")
	}
	if privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return privateKey, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse Kalshi private key: %w", err)
	}
	privateKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("Kalshi private key must be RSA")
	}
	return privateKey, nil
}

func mapBookSideAndPrice(side domain.Side, outcomeID, outcomePrice string) (string, string, error) {
	outcomeID = strings.ToUpper(strings.TrimSpace(outcomeID))
	if outcomeID != "YES" && outcomeID != "NO" {
		return "", "", fmt.Errorf("Kalshi outcome_id must be YES or NO")
	}
	bookSide := ""
	complement := false
	switch {
	case side == domain.SideBuy && outcomeID == "YES":
		bookSide = "bid"
	case side == domain.SideBuy && outcomeID == "NO":
		bookSide, complement = "ask", true
	case side == domain.SideSell && outcomeID == "YES":
		bookSide = "ask"
	case side == domain.SideSell && outcomeID == "NO":
		bookSide, complement = "bid", true
	default:
		return "", "", fmt.Errorf("unsupported Kalshi order side %q", side)
	}
	if !complement {
		return bookSide, outcomePrice, nil
	}
	priceRat, ok := new(big.Rat).SetString(outcomePrice)
	if !ok {
		return "", "", fmt.Errorf("invalid Kalshi outcome price")
	}
	return bookSide, new(big.Rat).Sub(big.NewRat(1, 1), priceRat).FloatString(4), nil
}

func fixedDecimal(value domain.Decimal, places int) (string, error) {
	rat, ok := new(big.Rat).SetString(value.String())
	if !ok || rat.Sign() <= 0 {
		return "", fmt.Errorf("decimal must be positive")
	}
	formatted := rat.FloatString(places)
	roundTrip, _ := new(big.Rat).SetString(formatted)
	if roundTrip.Cmp(rat) != 0 {
		return "", fmt.Errorf("decimal exceeds %d places", places)
	}
	return formatted, nil
}

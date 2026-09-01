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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
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

type CancelledOrder struct {
	OrderID       string         `json:"order_id"`
	ClientOrderID string         `json:"client_order_id,omitempty"`
	ReducedBy     domain.Decimal `json:"reduced_by"`
	TimestampMS   int64          `json:"ts_ms"`
}

type Order struct {
	OrderID                 string         `json:"order_id"`
	ClientOrderID           string         `json:"client_order_id"`
	Ticker                  string         `json:"ticker"`
	Side                    string         `json:"side"`
	Action                  string         `json:"action"`
	OutcomeSide             string         `json:"outcome_side"`
	BookSide                string         `json:"book_side"`
	Type                    string         `json:"type"`
	TimeInForce             string         `json:"time_in_force"`
	Status                  string         `json:"status"`
	YesPrice                domain.Decimal `json:"yes_price_dollars"`
	NoPrice                 domain.Decimal `json:"no_price_dollars"`
	FillCount               domain.Decimal `json:"fill_count_fp"`
	RemainingCount          domain.Decimal `json:"remaining_count_fp"`
	InitialCount            domain.Decimal `json:"initial_count_fp"`
	TakerFillCost           domain.Decimal `json:"taker_fill_cost_dollars"`
	MakerFillCost           domain.Decimal `json:"maker_fill_cost_dollars"`
	TakerFees               domain.Decimal `json:"taker_fees_dollars"`
	MakerFees               domain.Decimal `json:"maker_fees_dollars"`
	SelfTradePreventionType string         `json:"self_trade_prevention_type"`
	CancelOrderOnPause      *bool          `json:"cancel_order_on_pause"`
	SubaccountNumber        *int           `json:"subaccount_number"`
	LastUpdateTime          time.Time      `json:"last_update_time"`
}

type Fill struct {
	FillID       string         `json:"fill_id"`
	OrderID      string         `json:"order_id"`
	Ticker       string         `json:"ticker"`
	MarketTicker string         `json:"market_ticker"`
	OutcomeSide  string         `json:"outcome_side"`
	BookSide     string         `json:"book_side"`
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
		Order json.RawMessage `json:"order"`
	}
	requestPath := "/trade-api/v2/portfolio/orders/" + url.PathEscape(strings.TrimSpace(orderID))
	if err := client.doAuthenticated(ctx, http.MethodGet, requestPath, nil, &envelope); err != nil {
		var venueError *port.VenueError
		if errors.As(err, &venueError) && venueError.Code == "KALSHI_ORDER_NOT_FOUND" {
			return Order{}, kalshiUnavailableError(
				"KALSHI_ORDER_VISIBILITY_PENDING",
				"Kalshi order is not yet visible by order_id",
				err,
			)
		}
		return Order{}, err
	}
	order, err := decodeDetailedOrder(envelope.Order)
	if err != nil {
		return Order{}, kalshiUnavailableError("KALSHI_INVALID_ORDER_RESPONSE", "Kalshi order response is incomplete", err)
	}
	return order, nil
}

func (client *Client) FindOrderByClientOrderID(ctx context.Context, clientOrderID string) (Order, error) {
	wanted, cursor := strings.TrimSpace(clientOrderID), ""
	for page := 0; page < 100; page++ {
		var envelope struct {
			Orders []Order `json:"orders"`
			Cursor string  `json:"cursor"`
		}
		query := url.Values{"limit": []string{"1000"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/portfolio/orders?"+query.Encode(), nil, &envelope); err != nil {
			return Order{}, err
		}
		for _, order := range envelope.Orders {
			if order.ClientOrderID == wanted {
				return order, nil
			}
		}
		next := strings.TrimSpace(envelope.Cursor)
		if next == "" {
			return Order{}, kalshiUnavailableError(
				"KALSHI_ORDER_VISIBILITY_PENDING",
				"Kalshi order is not yet visible by client_order_id",
				fmt.Errorf("Kalshi order for client_order_id was not found"),
			)
		}
		if next == cursor {
			return Order{}, fmt.Errorf("Kalshi orders cursor did not advance")
		}
		cursor = next
	}
	return Order{}, fmt.Errorf("Kalshi order lookup exceeded pagination limit")
}

func (client *Client) CancelOrder(ctx context.Context, orderID, marketTicker string, subaccount int) (Order, error) {
	if !client.liveTradingEnabled {
		return Order{}, fmt.Errorf("Kalshi live trading is disabled")
	}
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return Order{}, kalshiInvalidError("KALSHI_ORDER_ID_REQUIRED", "Kalshi order id is required", nil)
	}
	var payload json.RawMessage
	marketTicker = strings.TrimSpace(marketTicker)
	if marketTicker == "" || subaccount < 0 {
		return Order{}, kalshiInvalidError("KALSHI_CANCEL_ROUTE_REQUIRED", "Kalshi cancel market ticker and subaccount are required", nil)
	}
	query := url.Values{
		"market_ticker": []string{marketTicker},
		"subaccount":    []string{fmt.Sprintf("%d", subaccount)},
	}
	requestPath := "/trade-api/v2/portfolio/events/orders/" + url.PathEscape(orderID) + "?" + query.Encode()
	if err := client.doAuthenticated(ctx, http.MethodDelete, requestPath, nil, &payload); err != nil {
		return Order{}, err
	}
	acknowledgement, err := decodeCancelledOrder(payload)
	if err != nil {
		return Order{}, kalshiAmbiguousOrderError("KALSHI_INVALID_CANCEL_RESPONSE", orderID, err)
	}
	if acknowledgement.OrderID != orderID {
		return Order{}, kalshiAmbiguousOrderError(
			"KALSHI_INVALID_CANCEL_RESPONSE", orderID,
			fmt.Errorf("Kalshi cancel acknowledgement order_id does not match the requested order"),
		)
	}
	if sign, parseErr := acknowledgement.ReducedBy.Sign(); parseErr != nil || sign < 0 || acknowledgement.TimestampMS <= 0 {
		return Order{}, kalshiAmbiguousOrderError(
			"KALSHI_INVALID_CANCEL_RESPONSE", orderID,
			fmt.Errorf("Kalshi cancel acknowledgement reduced_by or ts_ms is invalid"),
		)
	}

	// Cancel Order V2 returns only a cancellation acknowledgement, not the
	// canonical order. Re-read the order before exposing a result so callers do
	// not mistake a zero-value nested `order` for authoritative terminal state.
	remote, err := client.GetOrder(ctx, acknowledgement.OrderID)
	if err != nil {
		return Order{}, kalshiAmbiguousOrderError("KALSHI_CANCEL_CONFIRMATION_PENDING", orderID, err)
	}
	if remote.LastUpdateTime.IsZero() {
		remote.LastUpdateTime = time.UnixMilli(acknowledgement.TimestampMS).UTC()
	}
	if acknowledgement.ClientOrderID != "" && acknowledgement.ClientOrderID != remote.ClientOrderID {
		return Order{}, kalshiAmbiguousOrderError(
			"KALSHI_CANCEL_CONFIRMATION_PENDING", orderID,
			fmt.Errorf("Kalshi cancel acknowledgement client_order_id does not match the canonical order"),
		)
	}
	if comparison, compareErr := acknowledgement.ReducedBy.Compare(remote.InitialCount); compareErr != nil || comparison > 0 {
		return Order{}, kalshiAmbiguousOrderError(
			"KALSHI_CANCEL_CONFIRMATION_PENDING", orderID,
			fmt.Errorf("Kalshi cancel acknowledgement reduced_by exceeds the canonical initial count"),
		)
	}
	filled, _ := remote.FillCount.Multiply("1")
	reduced, _ := acknowledgement.ReducedBy.Multiply("1")
	initial, _ := remote.InitialCount.Multiply("1")
	if new(big.Rat).Add(filled, reduced).Cmp(initial) > 0 {
		return Order{}, kalshiAmbiguousOrderError(
			"KALSHI_CANCEL_CONFIRMATION_PENDING", orderID,
			fmt.Errorf("Kalshi cancel acknowledgement fill_count plus reduced_by exceeds the canonical initial count"),
		)
	}
	switch strings.ToLower(strings.TrimSpace(remote.Status)) {
	case "canceled", "cancelled", "executed":
		return remote, nil
	default:
		return Order{}, kalshiAmbiguousOrderError(
			"KALSHI_CANCEL_CONFIRMATION_PENDING", orderID,
			fmt.Errorf("Kalshi canonical order has not reached a terminal status after cancellation"),
		)
	}
}

func decodeCancelledOrder(payload json.RawMessage) (CancelledOrder, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return CancelledOrder{}, fmt.Errorf("Kalshi cancel acknowledgement is missing")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return CancelledOrder{}, fmt.Errorf("decode Kalshi cancel acknowledgement fields: %w", err)
	}
	for _, name := range []string{"order_id", "reduced_by", "ts_ms"} {
		value, found := fields[name]
		if !found || len(value) == 0 || string(value) == "null" {
			return CancelledOrder{}, fmt.Errorf("Kalshi cancel acknowledgement omitted %s", name)
		}
	}
	var acknowledgement CancelledOrder
	if err := json.Unmarshal(payload, &acknowledgement); err != nil {
		return CancelledOrder{}, fmt.Errorf("decode Kalshi cancel acknowledgement: %w", err)
	}
	return acknowledgement, nil
}

func (client *Client) ListFills(ctx context.Context, orderID string) ([]Fill, error) {
	orderID, cursor := strings.TrimSpace(orderID), ""
	result := make([]Fill, 0)
	for page := 0; page < 100; page++ {
		var envelope struct {
			Fills  *[]Fill `json:"fills"`
			Cursor *string `json:"cursor"`
		}
		query := url.Values{"order_id": []string{orderID}, "limit": []string{"1000"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		if err := client.doAuthenticated(ctx, http.MethodGet, "/trade-api/v2/portfolio/fills?"+query.Encode(), nil, &envelope); err != nil {
			return nil, err
		}
		if envelope.Fills == nil {
			return nil, fmt.Errorf("Kalshi fills response omitted the fills collection")
		}
		if envelope.Cursor == nil {
			return nil, fmt.Errorf("Kalshi fills response omitted the pagination cursor")
		}
		result = append(result, (*envelope.Fills)...)
		next := strings.TrimSpace(*envelope.Cursor)
		if next == "" {
			return result, nil
		}
		if next == cursor {
			return nil, fmt.Errorf("Kalshi fills cursor did not advance")
		}
		cursor = next
	}
	return nil, fmt.Errorf("Kalshi fill lookup exceeded pagination limit")
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
		expectedIdentity, identityErr := domain.CanonicalKalshiOrderIdentity(
			order.Intent.Side, order.Intent.OutcomeID, order.Intent.WorstPrice,
		)
		if identityErr != nil || !strings.EqualFold(raw.OutcomeSide, expectedIdentity.OutcomeSide) ||
			!strings.EqualFold(raw.BookSide, expectedIdentity.BookSide) {
			return nil, fmt.Errorf("Kalshi fill canonical direction does not match order intent")
		}
		price, priceErr := kalshiOutcomeFillPrice(order.Intent.OutcomeID, raw.YesPrice)
		if priceErr != nil {
			return nil, priceErr
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

// Kalshi V2 order/fill prices use the single YES-book scale. Local positions
// remain outcome-denominated, so a NO fill is converted back to 1-YES price
// before gross notional and P&L are recorded.
func kalshiOutcomeFillPrice(outcomeID string, yesPrice domain.Decimal) (domain.Decimal, error) {
	price, ok := new(big.Rat).SetString(yesPrice.String())
	if !ok || price.Sign() <= 0 || price.Cmp(big.NewRat(1, 1)) >= 0 {
		return "", fmt.Errorf("invalid Kalshi YES-book fill price")
	}
	switch strings.ToUpper(strings.TrimSpace(outcomeID)) {
	case "YES":
		return yesPrice, nil
	case "NO":
		// Get Fills may return six decimal places. Preserve that complete
		// official precision when converting the YES-book quote to a NO price.
		return domain.Decimal(new(big.Rat).Sub(big.NewRat(1, 1), price).FloatString(6)), nil
	default:
		return "", fmt.Errorf("invalid Kalshi fill outcome")
	}
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
		if intent.ExpiresAt != nil {
			return PreparedOrder{}, fmt.Errorf("Kalshi FOK order must not contain expires_at")
		}
		request.TimeInForce = "fill_or_kill"
	case domain.TimeInForceIOC:
		if intent.ExpiresAt != nil {
			return PreparedOrder{}, fmt.Errorf("Kalshi IOC order must not contain expires_at")
		}
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
		return SubmittedOrder{}, kalshiInvalidError("KALSHI_LIVE_TRADING_DISABLED", "Kalshi live trading is disabled", nil)
	}
	if len(order.body) == 0 || strings.TrimSpace(order.Request.ClientOrderID) == "" || order.Fingerprint() == "" {
		return SubmittedOrder{}, kalshiInvalidError("KALSHI_PREPARED_ORDER_INVALID", "Kalshi prepared order is invalid", nil)
	}
	var payload json.RawMessage
	if err := client.doAuthenticated(ctx, http.MethodPost, "/trade-api/v2/portfolio/events/orders", order.body, &payload); err != nil {
		return SubmittedOrder{}, classifyPreparedSubmitError(order, err)
	}
	response, err := decodeSubmittedOrder(payload)
	if err != nil {
		return SubmittedOrder{}, kalshiAmbiguousError("KALSHI_INVALID_POST_RESPONSE", err)
	}
	if response.OrderID == "" || response.ClientOrderID != "" && response.ClientOrderID != order.Request.ClientOrderID {
		return SubmittedOrder{}, kalshiAmbiguousError(
			"KALSHI_INVALID_POST_RESPONSE",
			fmt.Errorf("Kalshi order acknowledgement identity is invalid"),
		)
	}
	if err := validateSubmittedOrderCounts(response, order.Request); err != nil {
		return SubmittedOrder{}, kalshiAmbiguousError("KALSHI_INVALID_POST_RESPONSE", err)
	}
	return response, nil
}

func decodeSubmittedOrder(payload json.RawMessage) (SubmittedOrder, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return SubmittedOrder{}, fmt.Errorf("Kalshi order acknowledgement is missing")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return SubmittedOrder{}, fmt.Errorf("decode Kalshi order acknowledgement fields: %w", err)
	}
	for _, name := range []string{"order_id", "fill_count", "remaining_count", "ts_ms"} {
		value, found := fields[name]
		if !found || len(value) == 0 || string(value) == "null" {
			return SubmittedOrder{}, fmt.Errorf("Kalshi order acknowledgement omitted %s", name)
		}
	}
	var order SubmittedOrder
	if err := json.Unmarshal(payload, &order); err != nil {
		return SubmittedOrder{}, fmt.Errorf("decode Kalshi order acknowledgement: %w", err)
	}
	return order, nil
}

func decodeDetailedOrder(payload json.RawMessage) (Order, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return Order{}, fmt.Errorf("Kalshi order detail is missing")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return Order{}, fmt.Errorf("decode Kalshi order detail fields: %w", err)
	}
	for _, name := range []string{
		"order_id", "client_order_id", "ticker", "status", "fill_count_fp",
		"remaining_count_fp", "initial_count_fp",
	} {
		value, found := fields[name]
		if !found || len(value) == 0 || string(value) == "null" {
			return Order{}, fmt.Errorf("Kalshi order detail omitted %s", name)
		}
	}
	var order Order
	if err := json.Unmarshal(payload, &order); err != nil {
		return Order{}, fmt.Errorf("decode Kalshi order detail: %w", err)
	}
	for name, value := range map[string]domain.Decimal{
		"fill_count_fp": order.FillCount, "remaining_count_fp": order.RemainingCount, "initial_count_fp": order.InitialCount,
	} {
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return Order{}, fmt.Errorf("Kalshi order detail %s is invalid", name)
		}
	}
	switch strings.ToLower(strings.TrimSpace(order.Status)) {
	case "resting", "canceled", "executed":
	default:
		return Order{}, fmt.Errorf("Kalshi order detail status is invalid")
	}
	filled, _ := order.FillCount.Multiply("1")
	remaining, _ := order.RemainingCount.Multiply("1")
	initial, _ := order.InitialCount.Multiply("1")
	if new(big.Rat).Add(new(big.Rat).Set(filled), remaining).Cmp(initial) > 0 {
		return Order{}, fmt.Errorf("Kalshi order detail fill_count_fp plus remaining_count_fp exceeds initial_count_fp")
	}
	remainingSign := remaining.Sign()
	switch strings.ToLower(strings.TrimSpace(order.Status)) {
	case "resting":
		if remainingSign <= 0 {
			return Order{}, fmt.Errorf("resting Kalshi order detail has no remaining count")
		}
	case "canceled":
		if remainingSign != 0 {
			return Order{}, fmt.Errorf("cancelled Kalshi order detail retained a remaining count")
		}
		// A positive-size order whose entire effective quantity filled must be
		// reported as executed. Treating that contradictory payload as canceled
		// would release a reservation before the fill ledger can prove the trade.
		if initial.Sign() > 0 && filled.Cmp(initial) == 0 {
			return Order{}, fmt.Errorf("cancelled Kalshi order detail is fully filled")
		}
	case "executed":
		if remainingSign != 0 || filled.Cmp(initial) != 0 {
			return Order{}, fmt.Errorf("executed Kalshi order detail is not fully filled")
		}
	}
	return order, nil
}

func validateSubmittedOrderCounts(order SubmittedOrder, request OrderRequestV2) error {
	requested := domain.Decimal(request.Count)
	if sign, err := requested.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("prepared Kalshi order count is invalid")
	}
	for name, value := range map[string]domain.Decimal{"fill_count": order.FillCount, "remaining_count": order.RemainingCount} {
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("Kalshi order acknowledgement %s is invalid", name)
		}
		if comparison, err := value.Compare(requested); err != nil || comparison > 0 {
			return fmt.Errorf("Kalshi order acknowledgement %s exceeds requested count", name)
		}
	}
	filledComparison, err := order.FillCount.Compare(requested)
	if err != nil {
		return fmt.Errorf("compare Kalshi acknowledged fill count: %w", err)
	}
	remainingSign, err := order.RemainingCount.Sign()
	if err != nil {
		return fmt.Errorf("parse Kalshi acknowledged remaining count: %w", err)
	}
	if filledComparison == 0 && remainingSign != 0 {
		return fmt.Errorf("fully filled Kalshi acknowledgement retained a remaining count")
	}
	filled, _ := order.FillCount.Multiply("1")
	remaining, _ := order.RemainingCount.Multiply("1")
	requestedValue, _ := requested.Multiply("1")
	if new(big.Rat).Add(filled, remaining).Cmp(requestedValue) > 0 {
		return fmt.Errorf("Kalshi order acknowledgement fill_count plus remaining_count exceeds requested count")
	}
	if request.TimeInForce == "immediate_or_cancel" && remainingSign != 0 {
		return fmt.Errorf("Kalshi IOC acknowledgement retained an active remaining count")
	}
	if request.TimeInForce == "fill_or_kill" && (!request.ReduceOnly && filledComparison != 0 || remainingSign != 0) {
		return fmt.Errorf("successful Kalshi FOK acknowledgement was not fully filled")
	}
	if order.TimestampMS <= 0 {
		return fmt.Errorf("Kalshi order acknowledgement ts_ms is invalid")
	}
	return nil
}

func classifyPreparedSubmitError(order PreparedOrder, err error) error {
	if order.Request.TimeInForce == "fill_or_kill" {
		return err
	}
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Code != "KALSHI_FOK_NOT_FILLED" {
		return err
	}
	copy := *venueError
	copy.Kind = port.VenueErrorAmbiguous
	copy.Code = "KALSHI_ORDER_CONFLICT"
	copy.Message = "Kalshi returned FOK-specific evidence for a non-FOK submission; the order outcome must be reconciled"
	return &copy
}

func (client *Client) doAuthenticated(ctx context.Context, method, requestPath string, body []byte, target any) error {
	endpoint, err := client.baseURL.Parse(requestPath)
	if err != nil {
		return kalshiLocalFailure("KALSHI_INVALID_REQUEST_PATH", "Kalshi API request path is invalid", err)
	}
	timestamp := fmt.Sprintf("%d", client.now().UTC().UnixMilli())
	message := timestamp + method + endpoint.EscapedPath()
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPSS(rand.Reader, client.privateKey, crypto.SHA256, digest[:], &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA256,
	})
	if err != nil {
		return kalshiLocalFailure("KALSHI_SIGN_FAILED", "sign Kalshi request", err)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return kalshiLocalFailure("KALSHI_REQUEST_BUILD_FAILED", "build Kalshi API request", err)
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
		return kalshiTransportFailure(method, err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxAuthenticatedResponseBytes+1))
	if err != nil {
		return kalshiResponseFailure(method, "KALSHI_RESPONSE_READ_FAILED", "read Kalshi response", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return mapKalshiHTTPError(method, response.StatusCode, response.Header, payload)
	}
	if len(payload) > maxAuthenticatedResponseBytes {
		return kalshiResponseFailure(
			method, "KALSHI_RESPONSE_TOO_LARGE",
			fmt.Sprintf("Kalshi response exceeds %d bytes", maxAuthenticatedResponseBytes),
			fmt.Errorf("response body exceeded limit"),
		)
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return kalshiResponseFailure(method, "KALSHI_INVALID_RESPONSE", "decode Kalshi response", err)
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
	identity, err := domain.CanonicalKalshiOrderIdentity(side, outcomeID, domain.Decimal(outcomePrice))
	if err != nil {
		return "", "", err
	}
	return identity.BookSide, identity.OrderPrice.String(), nil
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

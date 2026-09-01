package polymarket

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const maxTradingResponseBytes = 4 << 20

// TradingClientParams 表示后端使用的 TradingClientParams 类型。
type TradingClientParams struct {
	BaseURL           string
	HTTPClient        *http.Client
	Credentials       CredentialProvider
	FeeEvidence       FillFeeEvidenceSource
	RequestTimeout    time.Duration
	RequestsPerSecond float64
	Burst             int
	BuilderCode       string
	Metadata          string
	MinBuyNotional    domain.Decimal
	Now               func() time.Time
	Random            io.Reader
}

// TradingClient 表示后端使用的 TradingClient 类型。
type TradingClient struct {
	baseURL     *url.URL
	httpClient  *http.Client
	credentials CredentialProvider
	timeout     time.Duration
	limiter     *tokenBucket
	builder     orderBuilder
	feeEvidence FillFeeEvidenceSource
	now         func() time.Time

	versionMu      sync.Mutex
	versionChecked bool
	clockMu        sync.RWMutex
	clockOffset    time.Duration
	feeScheduleMu  sync.RWMutex
	feeSchedules   map[string]marketFeeSchedule
}

var (
	_ port.FillSource                = (*TradingClient)(nil)
	_ port.VenueReconciliationSource = (*TradingClient)(nil)
)

// rawOrder 表示后端使用的 rawOrder 类型。
type rawOrder struct {
	ID              string         `json:"id"`
	Status          string         `json:"status"`
	Market          string         `json:"market"`
	AssetID         string         `json:"asset_id"`
	Side            string         `json:"side"`
	OriginalSize    domain.Decimal `json:"original_size"`
	SizeMatched     domain.Decimal `json:"size_matched"`
	Price           domain.Decimal `json:"price"`
	Outcome         string         `json:"outcome"`
	OrderType       string         `json:"order_type"`
	MakerAddress    string         `json:"maker_address"`
	Owner           string         `json:"owner"`
	AssociateTrades []string       `json:"associate_trades"`
	Expiration      string         `json:"expiration"`
}

// OpenOrder 表示后端使用的 OpenOrder 类型。
type OpenOrder struct {
	ID               string
	Status           string
	Market           string
	TokenID          string
	Side             domain.Side
	OriginalSize     domain.Decimal
	FilledSize       domain.Decimal
	Price            domain.Decimal
	Outcome          string
	OrderType        string
	AssociatedTrades []string
}

// Trade 表示后端使用的 Trade 类型。
type Trade struct {
	ID              string         `json:"id"`
	TakerOrderID    string         `json:"taker_order_id"`
	Market          string         `json:"market"`
	TokenID         string         `json:"asset_id"`
	Side            string         `json:"side"`
	Size            domain.Decimal `json:"size"`
	Price           domain.Decimal `json:"price"`
	Status          string         `json:"status"`
	FeeRateBPS      domain.Decimal `json:"fee_rate_bps"`
	MatchTime       string         `json:"match_time"`
	LastUpdate      string         `json:"last_update"`
	TransactionHash string         `json:"transaction_hash"`
	// MakerAddress is decoded explicitly for account-ownership checks but is
	// excluded from re-marshalling so existing FillLedger raw-payload digests
	// remain stable across this adapter upgrade.
	MakerAddress string       `json:"-"`
	TraderSide   string       `json:"trader_side"`
	MakerOrders  []MakerOrder `json:"maker_orders"`
}

// MakerOrder 表示后端使用的 MakerOrder 类型。
type MakerOrder struct {
	OrderID       string         `json:"order_id"`
	TokenID       string         `json:"asset_id"`
	MakerAddress  string         `json:"maker_address"`
	Side          string         `json:"side"`
	Price         domain.Decimal `json:"price"`
	MatchedAmount domain.Decimal `json:"matched_amount"`
	FeeRateBPS    domain.Decimal `json:"fee_rate_bps"`
	BuilderFee    string         `json:"builder_fee"`
	BuilderCode   string         `json:"builder_code"`
}

type marketFeeSchedule struct {
	Rate      domain.Decimal
	Exponent  domain.Decimal
	TakerOnly bool
}

// UnmarshalJSON converts documented V2 order quantities from canonical
// six-decimal uint256 base units. Production matched BUY responses can return
// fractional size_matched shares in human-decimal form after price improvement;
// a decimal point selects that field's unambiguous alternate representation
// without reinterpreting documented integer strings.
func (raw *rawOrder) UnmarshalJSON(data []byte) error {
	type rawOrderAlias rawOrder
	var decoded rawOrderAlias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var quantities struct {
		OriginalSize json.RawMessage `json:"original_size"`
		SizeMatched  json.RawMessage `json:"size_matched"`
	}
	if err := json.Unmarshal(data, &quantities); err != nil {
		return err
	}
	var err error
	if len(quantities.OriginalSize) > 0 && string(quantities.OriginalSize) != "null" {
		decoded.OriginalSize, err = decimalFromWireOrderQuantity(quantities.OriginalSize, "original_size")
		if err != nil {
			return err
		}
	}
	if len(quantities.SizeMatched) > 0 && string(quantities.SizeMatched) != "null" {
		decoded.SizeMatched, err = decimalFromWireOrderQuantity(quantities.SizeMatched, "size_matched")
		if err != nil {
			return err
		}
	}
	*raw = rawOrder(decoded)
	return nil
}

func (trade *Trade) UnmarshalJSON(data []byte) error {
	type tradeAlias Trade
	var decoded tradeAlias
	var ownership struct {
		MakerAddress string `json:"maker_address"`
	}
	if err := json.Unmarshal(data, &ownership); err != nil {
		return fmt.Errorf("decode trade ownership: %w", err)
	}
	normalized, err := defaultEmptyDecimalField(data, "fee_rate_bps")
	if err != nil {
		return fmt.Errorf("normalize trade fee-rate metadata: %w", err)
	}
	if err := json.Unmarshal(normalized, &decoded); err != nil {
		return fmt.Errorf("decode trade fields: %w", err)
	}
	if decoded.Size.IsEmpty() {
		return fmt.Errorf("decode trade size: decimal is required")
	}
	if sign, err := decoded.Size.Sign(); err != nil || sign < 0 {
		return fmt.Errorf("decode trade size: size must be a non-negative decimal")
	}
	decoded.MakerAddress = ownership.MakerAddress
	*trade = Trade(decoded)
	return nil
}

func (maker *MakerOrder) UnmarshalJSON(data []byte) error {
	type makerOrderAlias MakerOrder
	var decoded makerOrderAlias
	normalized, err := defaultEmptyDecimalField(data, "fee_rate_bps")
	if err != nil {
		return fmt.Errorf("normalize maker-order fee-rate metadata: %w", err)
	}
	if err := json.Unmarshal(normalized, &decoded); err != nil {
		return fmt.Errorf("decode maker-order fields: %w", err)
	}
	if decoded.MatchedAmount.IsEmpty() {
		return fmt.Errorf("decode maker-order matched amount: decimal is required")
	}
	if sign, err := decoded.MatchedAmount.Sign(); err != nil || sign < 0 {
		return fmt.Errorf("decode maker-order matched amount: amount must be a non-negative decimal")
	}
	*maker = MakerOrder(decoded)
	return nil
}

// V2 historical trade rows may encode the deprecated fee-rate metadata as an
// empty string. The value is not authoritative money evidence; final cash
// accounting comes exclusively from the Polygon OrderFilled event. Normalize
// absent/null/empty metadata to zero so historical reconciliation can proceed
// while keeping malformed non-empty values fail-closed.
func defaultEmptyDecimalField(data []byte, field string) ([]byte, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	raw, exists := object[field]
	if exists && string(raw) != "null" {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return data, nil
		}
		if strings.TrimSpace(value) != "" {
			return data, nil
		}
	}
	object[field] = json.RawMessage(`"0"`)
	return json.Marshal(object)
}

// OpenOrderFilter 表示后端使用的 OpenOrderFilter 类型。
type OpenOrderFilter struct {
	Market  string
	TokenID string
}

// AccountProbe 表示不含敏感信息的钱包认证检查结果，只证明签名器和 API 凭证可读取私有 CLOB 订单接口。
type AccountProbe struct {
	ExecutionAccountID string
	SignerAddress      string
	FunderAddress      string
	SignatureType      SignatureType
	OpenOrderCount     int
}

// FundingProbe is a non-secret startup result. It deliberately exposes only
// booleans and counts, not wallet balances or allowance quantities.
type FundingProbe struct {
	ExecutionAccountID         string
	CollateralBalancePositive  bool
	AllowanceContractCount     int
	AllAllowancesPositive      bool
	RequiredAllowancesPositive bool
}

// ProtocolProbe is the public, non-secret CLOB V2 startup result. ClockSkew is
// measured before applying the server offset to subsequent signatures.
type ProtocolProbe struct {
	Version    int
	ServerTime time.Time
	ClockSkew  time.Duration
}

// TradeFilter 表示后端使用的 TradeFilter 类型。
type TradeFilter struct {
	ID           string
	MakerAddress string
	Market       string
	TokenID      string
	Before       int64
	After        int64
}

// NewTradingClient 创建并初始化 Trading Client。
func NewTradingClient(params TradingClientParams) (*TradingClient, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("Polymarket trading base URL is invalid")
	}
	if params.Credentials == nil {
		return nil, fmt.Errorf("Polymarket credential provider is required")
	}
	if params.RequestTimeout == 0 {
		params.RequestTimeout = 5 * time.Second
	}
	if params.RequestTimeout < 500*time.Millisecond || params.RequestTimeout > 30*time.Second {
		return nil, fmt.Errorf("Polymarket request timeout must be between 500ms and 30s")
	}
	baseHTTPClient := params.HTTPClient
	if baseHTTPClient == nil {
		baseHTTPClient = &http.Client{Timeout: params.RequestTimeout}
	}
	// Clone caller-owned clients so enforcing the CLOB no-redirect policy
	// cannot mutate shared state. Authenticated POLY_* headers must never be
	// replayed to a redirect target, including a target on the same host.
	httpClient := *baseHTTPClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	params.HTTPClient = &httpClient
	if params.RequestsPerSecond == 0 {
		params.RequestsPerSecond = 8
	}
	if params.Burst == 0 {
		params.Burst = 4
	}
	limiter, err := newTokenBucket(params.RequestsPerSecond, params.Burst)
	if err != nil {
		return nil, err
	}
	if params.MinBuyNotional.IsEmpty() {
		params.MinBuyNotional = "1"
	}
	if sign, err := params.MinBuyNotional.Sign(); err != nil || sign <= 0 {
		return nil, fmt.Errorf("Polymarket minimum BUY notional must be positive")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	client := &TradingClient{
		baseURL:      baseURL,
		httpClient:   params.HTTPClient,
		credentials:  params.Credentials,
		timeout:      params.RequestTimeout,
		limiter:      limiter,
		feeEvidence:  params.FeeEvidence,
		now:          params.Now,
		feeSchedules: make(map[string]marketFeeSchedule),
		builder: orderBuilder{
			chainID:        polygonChainID,
			builderCode:    params.BuilderCode,
			metadata:       params.Metadata,
			minBuyNotional: params.MinBuyNotional,
			random:         params.Random,
		},
	}
	client.builder.now = client.protocolNow
	return client, nil
}

// Name 返回当前交易场所适配器名称。
func (client *TradingClient) Name() string { return "polymarket" }

// preparedCLOBPlacement is an in-memory-only signed request. Credentials and
// signed bytes are deliberately opaque; only the expected EIP-712 hash may be
// copied into the durable execution ledger.
type preparedCLOBPlacement struct {
	localOrderID       string
	clientOrderID      string
	executionAccountID string
	expectedOrderID    string
	body               []byte
}

func (placement *preparedCLOBPlacement) ExpectedVenueOrderID() string {
	if placement == nil {
		return ""
	}
	return placement.expectedOrderID
}

// PreparePlace validates and signs an order but cannot send POST /order.
func (client *TradingClient) PreparePlace(ctx context.Context, order domain.Order) (port.PreparedPlacement, error) {
	account, err := client.account(ctx, order.Intent.ExecutionAccountID)
	if err != nil {
		return nil, err
	}
	if err := client.ensureV2(ctx); err != nil {
		return nil, err
	}
	currentTick, err := client.GetTickSize(ctx, order.Intent.TokenID)
	if err != nil {
		return nil, err
	}
	if order.MarketValidation == nil || !currentTick.Equal(order.MarketValidation.TickSize) {
		return nil, newInvalidError("TICK_SIZE_CHANGED", "CLOB tick_size changed after market validation")
	}
	currentNegRisk, err := client.GetNegRisk(ctx, order.Intent.TokenID)
	if err != nil {
		return nil, err
	}
	if currentNegRisk != order.MarketValidation.NegRisk {
		return nil, newInvalidError("NEG_RISK_CHANGED", "CLOB neg_risk changed after market validation")
	}
	payload, expectedOrderID, err := client.builder.build(ctx, order, account)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal signed CLOB order: %w", err)
	}
	return &preparedCLOBPlacement{
		localOrderID: order.ID, clientOrderID: order.Intent.ClientOrderID,
		executionAccountID: order.Intent.ExecutionAccountID,
		expectedOrderID:    expectedOrderID, body: append([]byte(nil), body...),
	}, nil
}

// Place preserves compatibility for direct adapter callers. Production
// execution invokes the two phases separately around its durable attempt.
func (client *TradingClient) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	prepared, err := client.PreparePlace(ctx, order)
	if err != nil {
		return port.VenueOrder{}, err
	}
	order.VenueOrderID = prepared.ExpectedVenueOrderID()
	return client.PlacePrepared(ctx, order, prepared)
}

// PlacePrepared is the only order-placement method that emits POST bytes.
func (client *TradingClient) PlacePrepared(ctx context.Context, order domain.Order, prepared port.PreparedPlacement) (port.VenueOrder, error) {
	placement, ok := prepared.(*preparedCLOBPlacement)
	if !ok || placement == nil {
		return port.VenueOrder{}, newInvalidError("CLOB_PREPARED_ORDER_INVALID", "prepared placement was not created by this CLOB client")
	}
	expectedOrderID := strings.TrimSpace(placement.expectedOrderID)
	if placement.localOrderID != order.ID || placement.clientOrderID != order.Intent.ClientOrderID ||
		placement.executionAccountID != order.Intent.ExecutionAccountID || expectedOrderID == "" ||
		strings.TrimSpace(order.VenueOrderID) != expectedOrderID {
		return port.VenueOrder{}, newInvalidError("CLOB_PREPARED_ORDER_MISMATCH", "prepared placement does not match the durably identified order")
	}
	account, err := client.account(ctx, placement.executionAccountID)
	if err != nil {
		return port.VenueOrder{}, err
	}
	responseBody, _, err := client.doAuthenticated(ctx, account, http.MethodPost, "/order", nil, placement.body, true)
	if err != nil {
		var venueError *port.VenueError
		if errors.As(err, &venueError) && venueError.Kind == port.VenueErrorAmbiguous {
			venueError.VenueOrderID = expectedOrderID
		}
		return port.VenueOrder{}, err
	}
	var response struct {
		Success     *bool    `json:"success"`
		OK          *bool    `json:"ok"`
		OrderID     string   `json:"orderID"`
		OrderIDAlt  string   `json:"orderId"`
		Status      string   `json:"status"`
		Code        string   `json:"code"`
		ErrorMsg    string   `json:"errorMsg"`
		Message     string   `json:"message"`
		TradeIDs    []string `json:"tradeIDs"`
		TradeIDsAlt []string `json:"tradeIds"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return port.VenueOrder{}, ambiguousVenueError("CLOB_INVALID_POST_RESPONSE", expectedOrderID, err)
	}
	accepted, acceptanceKnown := responseAcceptance(response.Success, response.OK)
	if !acceptanceKnown {
		return port.VenueOrder{}, ambiguousVenueError(
			"CLOB_INVALID_POST_RESPONSE", expectedOrderID,
			fmt.Errorf("successful HTTP response omitted success/ok acceptance flag"),
		)
	}
	orderID := strings.TrimSpace(response.OrderID)
	if orderID == "" {
		orderID = strings.TrimSpace(response.OrderIDAlt)
	}
	message := firstNonEmpty(response.ErrorMsg, response.Message)
	if !accepted {
		return port.VenueOrder{}, &port.VenueError{
			Kind:    port.VenueErrorRejected,
			Code:    firstNonEmpty(response.Code, classifyCLOBError(http.StatusBadRequest, message)),
			Message: message,
		}
	}
	if orderID == "" {
		return port.VenueOrder{}, ambiguousVenueError(
			"CLOB_ORDER_ID_MISSING", expectedOrderID,
			fmt.Errorf("CLOB accepted order but omitted order id"),
		)
	}
	if !strings.EqualFold(orderID, expectedOrderID) {
		return port.VenueOrder{}, ambiguousVenueError(
			"CLOB_ORDER_ID_MISMATCH", expectedOrderID,
			fmt.Errorf("CLOB accepted order but returned an id that differs from the signed EIP-712 order hash"),
		)
	}
	// Persist the locally derived canonical hash even if the response used a
	// different hexadecimal case. It is the identity later matched against the
	// Polygon OrderFilled log and the only safe key for ambiguous reconciliation.
	orderID = expectedOrderID
	state := placementState(response.Status)
	tradeIDs := append(append([]string(nil), response.TradeIDs...), response.TradeIDsAlt...)
	// A placement status of matched can represent a full or partial immediate
	// match. The POST response does not carry authoritative size_matched, so a
	// follow-up GET enriches the match with its actual cumulative fill. Missing
	// trade details must not erase the match already observed in the POST.
	if state == port.VenueOrderFilled {
		observedOrder := order
		observedOrder.VenueOrderID = orderID
		observed, getErr := client.Get(ctx, observedOrder)
		observed.TradeIDs = appendDistinct(observed.TradeIDs, tradeIDs...)
		if getErr == nil {
			return observed, nil
		}
		var venueError *port.VenueError
		if errors.As(getErr, &venueError) && venueError.Code == "CLOB_FILL_DETAILS_UNAVAILABLE" {
			// Get returns the normalized order together with this enrichment
			// error. Preserve its observed size/state and let fill reconciliation
			// retry the still-pending exact trade details.
			return observed, nil
		}
	}
	return port.VenueOrder{
		ID:         orderID,
		State:      state,
		RawStatus:  response.Status,
		FilledSize: "0",
		ObservedAt: client.now().UTC(),
		TradeIDs:   appendDistinct(nil, tradeIDs...),
	}, nil
}

// Cancel 向当前交易场所撤销指定订单。
func (client *TradingClient) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	account, err := client.account(ctx, order.Intent.ExecutionAccountID)
	if err != nil {
		return port.VenueOrder{}, err
	}
	if strings.TrimSpace(order.VenueOrderID) == "" {
		return port.VenueOrder{}, newInvalidError("VENUE_ORDER_ID_REQUIRED", "cannot cancel before the CLOB order id is known")
	}
	body, err := json.Marshal(struct {
		OrderID string `json:"orderID"`
	}{OrderID: order.VenueOrderID})
	if err != nil {
		return port.VenueOrder{}, fmt.Errorf("marshal cancel request: %w", err)
	}
	responseBody, _, err := client.doAuthenticated(ctx, account, http.MethodDelete, "/order", nil, body, true)
	if err != nil {
		return port.VenueOrder{}, err
	}
	var response struct {
		Canceled    []string          `json:"canceled"`
		Cancelled   []string          `json:"cancelled"`
		NotCanceled map[string]string `json:"not_canceled"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return port.VenueOrder{}, ambiguousTransportError("CLOB_INVALID_CANCEL_RESPONSE", err)
	}
	canceled := containsOrderID(response.Canceled, order.VenueOrderID) || containsOrderID(response.Cancelled, order.VenueOrderID)
	observed, getErr := client.Get(ctx, order)
	if getErr == nil {
		return observed, nil
	}
	var venueError *port.VenueError
	if canceled && errors.As(getErr, &venueError) && venueError.Code == "CLOB_ORDER_NOT_FOUND" {
		return port.VenueOrder{
			ID:         order.VenueOrderID,
			State:      port.VenueOrderCancelled,
			RawStatus:  "cancelled",
			FilledSize: order.FilledSize,
			ObservedAt: client.now().UTC(),
		}, nil
	}
	if reason := response.NotCanceled[order.VenueOrderID]; reason != "" {
		return port.VenueOrder{}, &port.VenueError{
			Kind:    port.VenueErrorAmbiguous,
			Code:    "CLOB_CANCEL_NOT_CONFIRMED",
			Message: reason,
			Cause:   getErr,
		}
	}
	return port.VenueOrder{}, ambiguousTransportError("CLOB_CANCEL_RECONCILE_FAILED", getErr)
}

// Get 按标识查询并返回当前组件管理的记录。
func (client *TradingClient) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	account, err := client.account(ctx, order.Intent.ExecutionAccountID)
	if err != nil {
		return port.VenueOrder{}, err
	}
	orderID := strings.TrimSpace(order.VenueOrderID)
	if orderID == "" {
		return port.VenueOrder{}, newInvalidError("VENUE_ORDER_ID_REQUIRED", "CLOB order id is required")
	}
	body, _, err := client.doAuthenticated(ctx, account, http.MethodGet, "/data/order/"+url.PathEscape(orderID), nil, nil, false)
	if err != nil {
		return port.VenueOrder{}, err
	}
	var raw rawOrder
	if err := json.Unmarshal(body, &raw); err != nil {
		return port.VenueOrder{}, &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_INVALID_ORDER_RESPONSE", Cause: err}
	}
	if strings.TrimSpace(raw.ID) == "" {
		raw.ID = orderID
	}
	normalized, err := normalizeRawOrder(raw, order, client.now().UTC())
	if err != nil {
		return port.VenueOrder{}, err
	}
	if sign, _ := normalized.FilledSize.Sign(); sign > 0 {
		averagePrice, tradeIDs, err := client.fillAveragePrice(ctx, order.Intent.ExecutionAccountID, raw)
		if err != nil {
			return normalized, &port.VenueError{
				Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE", VenueOrderID: normalized.ID,
				Message: "order fill was observed but exact trade details are not complete", Cause: err,
			}
		}
		normalized.AverageFillPrice = averagePrice
		normalized.TradeIDs = appendDistinct(normalized.TradeIDs, tradeIDs...)
	}
	return normalized, nil
}

// fillAveragePrice 根据真实成交分量计算订单累计成交均价。
func (client *TradingClient) fillAveragePrice(ctx context.Context, executionAccountID string, raw rawOrder) (domain.Decimal, []string, error) {
	if len(raw.AssociateTrades) > 1000 {
		return "", nil, fmt.Errorf("order references too many trades")
	}
	trades := make([]Trade, 0, len(raw.AssociateTrades))
	if len(raw.AssociateTrades) > 0 {
		for _, tradeID := range appendDistinct(nil, raw.AssociateTrades...) {
			page, err := client.ListTrades(ctx, executionAccountID, TradeFilter{ID: tradeID})
			if err != nil {
				return "", nil, err
			}
			trades = append(trades, page...)
		}
	} else {
		page, err := client.ListTrades(ctx, executionAccountID, TradeFilter{Market: raw.Market, TokenID: raw.AssetID})
		if err != nil {
			return "", nil, err
		}
		trades = page
	}

	totalShares := new(big.Rat)
	totalNotional := new(big.Rat)
	tradeIDs := make([]string, 0, len(trades))
	seen := make(map[string]struct{}, len(trades))
	for _, trade := range trades {
		if domain.NormalizeFillStatus(domain.FillStatus(trade.Status)) != domain.FillStatusConfirmed {
			continue
		}
		if _, exists := seen[trade.ID]; exists && strings.TrimSpace(trade.ID) != "" {
			continue
		}
		if strings.TrimSpace(trade.ID) != "" {
			seen[trade.ID] = struct{}{}
		}
		shares, price, matched := tradeFillForOrder(trade, raw.ID)
		if !matched {
			continue
		}
		sharesRat, err := decimalRat(shares)
		if err != nil {
			return "", nil, fmt.Errorf("trade %s size: %w", trade.ID, err)
		}
		priceRat, err := decimalRat(price)
		if err != nil {
			return "", nil, fmt.Errorf("trade %s price: %w", trade.ID, err)
		}
		if sharesRat.Sign() <= 0 || priceRat.Sign() <= 0 {
			return "", nil, fmt.Errorf("trade %s contains non-positive fill values", trade.ID)
		}
		totalShares.Add(totalShares, sharesRat)
		totalNotional.Add(totalNotional, new(big.Rat).Mul(sharesRat, priceRat))
		tradeIDs = appendDistinct(tradeIDs, trade.ID)
	}
	expectedShares, err := decimalRat(raw.SizeMatched)
	if err != nil || expectedShares.Sign() <= 0 {
		return "", nil, fmt.Errorf("order size_matched is invalid")
	}
	if totalShares.Cmp(expectedShares) != 0 {
		return "", nil, fmt.Errorf("trade fills total %s shares, order reports %s", totalShares.RatString(), raw.SizeMatched)
	}
	average := new(big.Rat).Quo(totalNotional, totalShares)
	averageDecimal, err := domain.ParseDecimal(strings.TrimRight(strings.TrimRight(average.FloatString(18), "0"), "."))
	if err != nil {
		return "", nil, fmt.Errorf("encode average fill price: %w", err)
	}
	return averageDecimal, tradeIDs, nil
}

// tradeFillForOrder 提取指定订单在交易中的成交数量和价格。
func tradeFillForOrder(trade Trade, orderID string) (domain.Decimal, domain.Decimal, bool) {
	if strings.EqualFold(strings.TrimSpace(trade.TakerOrderID), strings.TrimSpace(orderID)) {
		return trade.Size, trade.Price, true
	}
	for _, maker := range trade.MakerOrders {
		if strings.EqualFold(strings.TrimSpace(maker.OrderID), strings.TrimSpace(orderID)) {
			return maker.MatchedAmount, maker.Price, true
		}
	}
	return "", "", false
}

// ListOpenOrders 分页查询指定执行账户的交易所开放订单。
func (client *TradingClient) ListOpenOrders(ctx context.Context, executionAccountID string, filter OpenOrderFilter) ([]OpenOrder, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	if strings.TrimSpace(filter.Market) != "" {
		query.Set("market", strings.TrimSpace(filter.Market))
	}
	if strings.TrimSpace(filter.TokenID) != "" {
		query.Set("asset_id", strings.TrimSpace(filter.TokenID))
	}
	rawOrders, err := client.listOrdersPage(ctx, account, query)
	if err != nil {
		return nil, err
	}
	orders := make([]OpenOrder, 0, len(rawOrders))
	for _, raw := range rawOrders {
		orders = append(orders, OpenOrder{
			ID:               raw.ID,
			Status:           raw.Status,
			Market:           raw.Market,
			TokenID:          raw.AssetID,
			Side:             domain.Side(strings.ToUpper(raw.Side)),
			OriginalSize:     raw.OriginalSize,
			FilledSize:       raw.SizeMatched,
			Price:            raw.Price,
			Outcome:          raw.Outcome,
			OrderType:        raw.OrderType,
			AssociatedTrades: append([]string(nil), raw.AssociateTrades...),
		})
	}
	return orders, nil
}

// ListReconciliationOpenOrders 查询并映射账户对账所需的交易所开放订单。
func (client *TradingClient) ListReconciliationOpenOrders(
	ctx context.Context,
	executionAccountID string,
) ([]domain.VenueOrderSnapshot, error) {
	orders, err := client.ListOpenOrders(ctx, executionAccountID, OpenOrderFilter{})
	if err != nil {
		return nil, err
	}
	observedAt := client.now().UTC()
	result := make([]domain.VenueOrderSnapshot, 0, len(orders))
	for _, order := range orders {
		result = append(result, domain.VenueOrderSnapshot{
			VenueOrderID: order.ID, ConditionID: order.Market, TokenID: order.TokenID,
			Side: order.Side, OriginalSize: order.OriginalSize, FilledSize: order.FilledSize,
			Price: order.Price, Status: strings.ToLower(strings.TrimSpace(order.Status)), ObservedAt: observedAt,
		})
	}
	return result, nil
}

// ProbeAccount 通过只读接口检查执行账户凭证和连接状态。
func (client *TradingClient) ProbeAccount(ctx context.Context, executionAccountID string) (AccountProbe, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return AccountProbe{}, err
	}
	if err := client.ensureV2(ctx); err != nil {
		return AccountProbe{}, err
	}
	orders, err := client.ListOpenOrders(ctx, executionAccountID, OpenOrderFilter{})
	if err != nil {
		return AccountProbe{}, err
	}
	return AccountProbe{
		ExecutionAccountID: account.ExecutionAccountID,
		SignerAddress:      strings.ToLower(account.Signer.Address()),
		FunderAddress:      strings.ToLower(account.FunderAddress),
		SignatureType:      account.SignatureType,
		OpenOrderCount:     len(orders),
	}, nil
}

// ProbeProtocol verifies the three public V2 startup endpoints, rejects an
// unsafe local clock, and synchronizes future L2/order timestamps to CLOB time.
func (client *TradingClient) ProbeProtocol(ctx context.Context, maxClockSkew time.Duration) (ProtocolProbe, error) {
	if maxClockSkew <= 0 || maxClockSkew > time.Minute {
		return ProtocolProbe{}, fmt.Errorf("maximum CLOB clock skew must be between 1ns and 1m")
	}
	if _, _, err := client.do(ctx, TradingAccount{}, http.MethodGet, "/ok", nil, nil, false, false); err != nil {
		return ProtocolProbe{}, fmt.Errorf("CLOB /ok probe: %w", err)
	}
	if err := client.ensureV2(ctx); err != nil {
		return ProtocolProbe{}, err
	}
	body, _, err := client.do(ctx, TradingAccount{}, http.MethodGet, "/time", nil, nil, false, false)
	if err != nil {
		return ProtocolProbe{}, fmt.Errorf("CLOB /time probe: %w", err)
	}
	serverTime, err := parseProtocolTime(body)
	if err != nil {
		return ProtocolProbe{}, err
	}
	localTime := client.now().UTC()
	skew := serverTime.Sub(localTime)
	if skew < -maxClockSkew || skew > maxClockSkew {
		return ProtocolProbe{}, fmt.Errorf("CLOB clock skew %s exceeds maximum %s", skew, maxClockSkew)
	}
	client.clockMu.Lock()
	client.clockOffset = skew
	client.clockMu.Unlock()
	return ProtocolProbe{Version: 2, ServerTime: serverTime, ClockSkew: skew}, nil
}

// ClosedOnly implements the authenticated account-level placement gate used by
// CLOB V2. It is separate from the public website geoblock response.
func (client *TradingClient) ClosedOnly(ctx context.Context, executionAccountID string) (bool, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return false, err
	}
	body, _, err := client.doAuthenticated(ctx, account, http.MethodGet, "/auth/ban-status/closed-only", nil, nil, false)
	if err != nil {
		return false, err
	}
	var response struct {
		ClosedOnly *bool `json:"closed_only"`
	}
	if err := json.Unmarshal(body, &response); err != nil || response.ClosedOnly == nil {
		return false, fmt.Errorf("decode CLOB closed-only status")
	}
	return *response.ClosedOnly, nil
}

// Heartbeat maintains the CLOB V2 dead-man switch session for one account.
// The first call uses an empty id and every subsequent call must reuse the id
// returned by the server.
func (client *TradingClient) Heartbeat(
	ctx context.Context,
	executionAccountID string,
	heartbeatID string,
) (string, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(struct {
		HeartbeatID string `json:"heartbeat_id"`
	}{HeartbeatID: strings.TrimSpace(heartbeatID)})
	if err != nil {
		return "", fmt.Errorf("encode CLOB heartbeat: %w", err)
	}
	responseBody, _, err := client.doAuthenticated(ctx, account, http.MethodPost, "/v1/heartbeats", nil, body, false)
	if err != nil {
		return "", err
	}
	var response struct {
		HeartbeatID string `json:"heartbeat_id"`
		ErrorMsg    string `json:"error_msg"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return "", fmt.Errorf("decode CLOB heartbeat response: %w", err)
	}
	response.HeartbeatID = strings.TrimSpace(response.HeartbeatID)
	if response.HeartbeatID == "" {
		return "", fmt.Errorf("CLOB heartbeat response omitted heartbeat_id: %s", strings.TrimSpace(response.ErrorMsg))
	}
	return response.HeartbeatID, nil
}

// GetBalanceAllowance reads the CLOB ledger cache for one wallet and asset.
// The returned quantities remain integer base units.
func (client *TradingClient) GetBalanceAllowance(
	ctx context.Context,
	executionAccountID string,
	assetType BalanceAssetType,
	tokenID string,
) (BalanceAllowance, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return BalanceAllowance{}, err
	}
	query, err := balanceAllowanceQuery(account, assetType, tokenID)
	if err != nil {
		return BalanceAllowance{}, err
	}
	body, _, err := client.doAuthenticated(ctx, account, http.MethodGet, "/balance-allowance", query, nil, false)
	if err != nil {
		return BalanceAllowance{}, err
	}
	var response struct {
		Balance    json.RawMessage            `json:"balance"`
		Allowances map[string]json.RawMessage `json:"allowances"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return BalanceAllowance{}, fmt.Errorf("decode CLOB balance allowance: %w", err)
	}
	balance, err := nonNegativeIntegerFromJSON(response.Balance)
	if err != nil {
		return BalanceAllowance{}, fmt.Errorf("decode CLOB collateral balance: %w", err)
	}
	allowances := make(map[string]string, len(response.Allowances))
	for contract, raw := range response.Allowances {
		contract = strings.ToLower(strings.TrimSpace(contract))
		if _, ok := decodeAddress(contract); !ok {
			return BalanceAllowance{}, fmt.Errorf("decode CLOB allowance: invalid contract address")
		}
		amount, err := nonNegativeIntegerFromJSON(raw)
		if err != nil {
			return BalanceAllowance{}, fmt.Errorf("decode CLOB allowance for %s: %w", contract, err)
		}
		allowances[contract] = amount
	}
	return BalanceAllowance{
		AssetType:  assetType,
		TokenID:    strings.TrimSpace(tokenID),
		Balance:    balance,
		Allowances: allowances,
	}, nil
}

// UpdateBalanceAllowance asks CLOB V2 to refresh its authenticated cache after
// an independently finalized on-chain approval. It never signs or broadcasts
// an EVM transaction and is safe to repeat after an ambiguous transport error.
func (client *TradingClient) UpdateBalanceAllowance(
	ctx context.Context,
	executionAccountID string,
	assetType BalanceAssetType,
	tokenID string,
) error {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return err
	}
	query, err := balanceAllowanceQuery(account, assetType, tokenID)
	if err != nil {
		return err
	}
	_, _, err = client.doAuthenticated(ctx, account, http.MethodGet, "/balance-allowance/update", query, nil, false)
	return err
}

func balanceAllowanceQuery(account TradingAccount, assetType BalanceAssetType, tokenID string) (url.Values, error) {
	if assetType != BalanceAssetCollateral && assetType != BalanceAssetConditional {
		return nil, newInvalidError("CLOB_ASSET_TYPE_INVALID", "asset type must be COLLATERAL or CONDITIONAL")
	}
	tokenID = strings.TrimSpace(tokenID)
	if assetType == BalanceAssetConditional && tokenID == "" {
		return nil, newInvalidError("CLOB_TOKEN_ID_REQUIRED", "conditional balance allowance requires token id")
	}
	query := url.Values{
		"asset_type":     []string{string(assetType)},
		"signature_type": []string{strconv.Itoa(int(account.SignatureType))},
	}
	if tokenID != "" {
		query.Set("token_id", tokenID)
	}
	return query, nil
}

// ProbeFunding checks the collateral prerequisite without logging quantities.
func (client *TradingClient) ProbeFunding(ctx context.Context, executionAccountID string) (FundingProbe, error) {
	allowance, err := client.GetBalanceAllowance(ctx, executionAccountID, BalanceAssetCollateral, "")
	if err != nil {
		return FundingProbe{}, err
	}
	return FundingProbe{
		ExecutionAccountID:        executionAccountID,
		CollateralBalancePositive: allowance.Positive(),
		AllowanceContractCount:    len(allowance.Allowances),
		AllAllowancesPositive:     allowance.AllAllowancesPositive(),
		RequiredAllowancesPositive: allowance.RequiredAllowancesPositive(
			StandardExchangeV2Address,
			NegRiskExchangeV2Address,
		),
	}, nil
}

// ListTrades 分页查询指定执行账户的交易所成交。
func (client *TradingClient) ListTrades(ctx context.Context, executionAccountID string, filter TradeFilter) ([]Trade, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return nil, err
	}
	requestedMaker := strings.TrimSpace(filter.MakerAddress)
	if requestedMaker != "" && !strings.EqualFold(requestedMaker, account.FunderAddress) {
		return nil, newInvalidError(
			"CLOB_TRADE_ACCOUNT_FILTER_MISMATCH",
			"trade maker_address must match the configured execution account funder",
		)
	}
	// The CLOB contract marks maker_address as required. More importantly, L2
	// credentials identify the signer, which may be shared by several proxy or
	// Safe funders. Always bind the query to the execution account's actual
	// asset-owning wallet; callers cannot widen or redirect that boundary.
	filter.MakerAddress = account.FunderAddress
	query := url.Values{}
	setQuery(query, "id", filter.ID)
	setQuery(query, "maker_address", filter.MakerAddress)
	setQuery(query, "market", filter.Market)
	setQuery(query, "asset_id", filter.TokenID)
	if filter.Before > 0 {
		query.Set("before", strconv.FormatInt(filter.Before, 10))
	}
	if filter.After > 0 {
		query.Set("after", strconv.FormatInt(filter.After, 10))
	}
	var trades []Trade
	cursor := "MA=="
	for pages := 0; pages < 100; pages++ {
		query.Set("next_cursor", cursor)
		body, _, err := client.doAuthenticated(ctx, account, http.MethodGet, "/data/trades", query, nil, false)
		if err != nil {
			return nil, err
		}
		var envelope struct {
			Data          []Trade `json:"data"`
			Items         []Trade `json:"items"`
			NextCursor    string  `json:"next_cursor"`
			NextCursorAlt string  `json:"nextCursor"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			var bare []Trade
			if arrayErr := json.Unmarshal(body, &bare); arrayErr != nil {
				return nil, fmt.Errorf("decode CLOB trades: %w", err)
			}
			if ownershipErr := validateAccountTrades(bare, account.FunderAddress); ownershipErr != nil {
				return nil, tradeOwnershipVenueError(ownershipErr)
			}
			return append(trades, bare...), nil
		}
		page := envelope.Data
		if page == nil {
			page = envelope.Items
		}
		if ownershipErr := validateAccountTrades(page, account.FunderAddress); ownershipErr != nil {
			return nil, tradeOwnershipVenueError(ownershipErr)
		}
		trades = append(trades, page...)
		nextCursor := firstNonEmpty(envelope.NextCursor, envelope.NextCursorAlt)
		if nextCursor == "" || nextCursor == "LTE=" || nextCursor == cursor {
			return trades, nil
		}
		cursor = nextCursor
	}
	return nil, fmt.Errorf("CLOB trades pagination exceeded 100 pages")
}

// validateAccountTrades rejects an authenticated response that contains a
// trade component not attributable to the requested funder. Query filtering is
// necessary but is not sufficient authority for reconciliation or FillLedger.
func validateAccountTrades(trades []Trade, funderAddress string) error {
	for _, trade := range trades {
		if _, err := accountTradeComponents(trade, funderAddress); err != nil {
			return err
		}
	}
	return nil
}

type accountTradeComponent struct {
	OrderID string
	TokenID string
}

// accountTradeComponents applies the CLOB wire semantics to select only order
// components owned by one funder while retaining each component's asset. The
// top-level asset belongs to the taker order; a maker component's asset must
// come from that exact maker_orders entry and may differ from the taker asset.
// Legacy rows without trader_side are accepted only when an address-bearing
// shape proves ownership.
func accountTradeComponents(trade Trade, funderAddress string) ([]accountTradeComponent, error) {
	funderAddress = strings.TrimSpace(funderAddress)
	tradeID := strings.TrimSpace(trade.ID)
	if tradeID == "" {
		tradeID = "<missing>"
	}
	traderSide := strings.ToUpper(strings.TrimSpace(trade.TraderSide))
	topLevelOwned := strings.EqualFold(strings.TrimSpace(trade.MakerAddress), funderAddress)

	ownedMakerComponents := make([]accountTradeComponent, 0, len(trade.MakerOrders))
	for _, maker := range trade.MakerOrders {
		if !strings.EqualFold(strings.TrimSpace(maker.MakerAddress), funderAddress) {
			continue
		}
		orderID := strings.TrimSpace(maker.OrderID)
		if orderID == "" {
			return nil, fmt.Errorf("CLOB trade %s has an owned maker component without order_id", tradeID)
		}
		ownedMakerComponents = appendDistinctTradeComponent(ownedMakerComponents, accountTradeComponent{
			OrderID: orderID,
			TokenID: strings.TrimSpace(maker.TokenID),
		})
	}

	var components []accountTradeComponent
	switch traderSide {
	case "TAKER":
		if !topLevelOwned {
			return nil, fmt.Errorf("CLOB trade %s taker component is not owned by the configured funder", tradeID)
		}
		takerOrderID := strings.TrimSpace(trade.TakerOrderID)
		if takerOrderID == "" {
			return nil, fmt.Errorf("CLOB trade %s has an owned taker component without taker_order_id", tradeID)
		}
		components = appendDistinctTradeComponent(components, accountTradeComponent{
			OrderID: takerOrderID,
			TokenID: strings.TrimSpace(trade.TokenID),
		})
	case "MAKER":
		if len(ownedMakerComponents) == 0 {
			return nil, fmt.Errorf("CLOB trade %s has no maker component owned by the configured funder", tradeID)
		}
		components = append(components, ownedMakerComponents...)
	case "":
		// Historical observations can omit trader_side. Do not guess from IDs:
		// accept only address-proven top-level or maker-order components.
		if topLevelOwned {
			takerOrderID := strings.TrimSpace(trade.TakerOrderID)
			if takerOrderID != "" {
				components = appendDistinctTradeComponent(components, accountTradeComponent{
					OrderID: takerOrderID,
					TokenID: strings.TrimSpace(trade.TokenID),
				})
			}
		}
		for _, component := range ownedMakerComponents {
			components = appendDistinctTradeComponent(components, component)
		}
		if len(components) == 0 {
			return nil, fmt.Errorf("CLOB trade %s has no address-proven component owned by the configured funder", tradeID)
		}
	default:
		return nil, fmt.Errorf("CLOB trade %s has unsupported trader_side %q", tradeID, trade.TraderSide)
	}
	return components, nil
}

func appendDistinctTradeComponent(existing []accountTradeComponent, candidate accountTradeComponent) []accountTradeComponent {
	for _, component := range existing {
		if component.OrderID == candidate.OrderID && component.TokenID == candidate.TokenID {
			return existing
		}
	}
	return append(existing, candidate)
}

func accountTradeOrderIDs(trade Trade, funderAddress string) ([]string, error) {
	components, err := accountTradeComponents(trade, funderAddress)
	if err != nil {
		return nil, err
	}
	orderIDs := make([]string, 0, len(components))
	for _, component := range components {
		orderIDs = appendDistinct(orderIDs, component.OrderID)
	}
	return orderIDs, nil
}

func tradeOwnershipVenueError(cause error) error {
	return &port.VenueError{
		Kind:    port.VenueErrorUnavailable,
		Code:    "CLOB_TRADE_OWNERSHIP_MISMATCH",
		Message: "CLOB trade response crossed the configured execution-account ownership boundary",
		Cause:   cause,
	}
}

// ListReconciliationTrades 查询并映射账户对账所需的交易所成交。
func (client *TradingClient) ListReconciliationTrades(
	ctx context.Context,
	executionAccountID string,
	matchedAfter time.Time,
) ([]domain.VenueTradeSnapshot, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return nil, err
	}
	filter := TradeFilter{}
	if !matchedAfter.IsZero() {
		filter.After = matchedAfter.UTC().Unix()
	}
	trades, err := client.ListTrades(ctx, executionAccountID, filter)
	if err != nil {
		return nil, err
	}
	result := make([]domain.VenueTradeSnapshot, 0, len(trades))
	for _, trade := range trades {
		components, ownershipErr := accountTradeComponents(trade, account.FunderAddress)
		if ownershipErr != nil {
			return nil, tradeOwnershipVenueError(ownershipErr)
		}
		observedAt, ok := parseTradeTime(trade.LastUpdate)
		if !ok {
			observedAt, ok = parseTradeTime(trade.MatchTime)
		}
		if !ok {
			observedAt = client.now().UTC()
		}
		for _, component := range components {
			if component.TokenID == "" {
				return nil, &port.VenueError{
					Kind: port.VenueErrorUnavailable, Code: "CLOB_TRADE_COMPONENT_IDENTITY_INVALID",
					Message: "CLOB trade response omitted the owned order component asset id",
				}
			}
			result = append(result, domain.VenueTradeSnapshot{
				VenueTradeID: strings.TrimSpace(trade.ID), OrderIDs: []string{component.OrderID},
				ConditionID: strings.TrimSpace(trade.Market), TokenID: component.TokenID,
				Status: domain.NormalizeFillStatus(domain.FillStatus(trade.Status)), ObservedAt: observedAt,
			})
		}
	}
	return result, nil
}

// ListOrderFills 查询并返回指定订单的真实成交分量。
func (client *TradingClient) ListOrderFills(ctx context.Context, order domain.Order) ([]domain.Fill, error) {
	if strings.TrimSpace(order.VenueOrderID) == "" {
		return nil, newInvalidError("VENUE_ORDER_ID_REQUIRED", "cannot list fills before venue order id is known")
	}
	trades, err := client.ListTrades(ctx, order.Intent.ExecutionAccountID, TradeFilter{
		Market:  order.Intent.ConditionID,
		TokenID: order.Intent.TokenID,
	})
	if err != nil {
		return nil, err
	}
	observations := make([]domain.Fill, 0)
	seen := make(map[string]int)
	for _, trade := range trades {
		fill, matched, err := mapTradeToOrderFill(trade, order, client.now().UTC())
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		identity := fill.VenueFillID + "\x00" + fill.OrderID
		if existingIndex, exists := seen[identity]; exists {
			merged, err := preferTradeObservation(observations[existingIndex], fill)
			if err != nil {
				return nil, err
			}
			observations[existingIndex] = merged
			continue
		}
		seen[identity] = len(observations)
		observations = append(observations, fill)
	}
	fills := make([]domain.Fill, 0, len(observations))
	for _, fill := range observations {
		// CLOB's matched/mined/retrying observations do not yet carry finalized
		// receipt evidence. Returning an empty successful scan here would let
		// cancellation finality release assets while a known fill is still
		// propagating, so surface an explicit retryable evidence gap instead.
		if !fill.Status.Terminal() {
			return nil, &port.VenueError{
				Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE",
				Message:      "CLOB order fill is observed but has not reached a terminal status",
				VenueOrderID: order.VenueOrderID,
			}
		}
		if fill.Status == domain.FillStatusConfirmed {
			fills = append(fills, fill)
		}
	}
	if len(fills) == 0 {
		return fills, nil
	}
	account, err := client.account(ctx, order.Intent.ExecutionAccountID)
	if err != nil {
		return nil, err
	}
	schedule, err := client.getMarketFeeSchedule(ctx, order.Intent.ConditionID, order.Intent.TokenID)
	if err != nil {
		return nil, err
	}
	for index := range fills {
		observed := fills[index]
		enriched, evidenceErr := client.attachFillFeeEvidence(ctx, account, order, observed, schedule)
		if evidenceErr != nil {
			return nil, fmt.Errorf("CLOB trade %s fee evidence: %w", observed.VenueFillID, evidenceErr)
		}
		fills[index] = enriched
	}
	return fills, nil
}

// preferTradeObservation 合并同一成交分量的重复观察并保留更终局状态。
func preferTradeObservation(existing, incoming domain.Fill) (domain.Fill, error) {
	if existing.VenueFillID != incoming.VenueFillID || existing.OrderID != incoming.OrderID ||
		existing.LiquidityRole != incoming.LiquidityRole || !existing.Shares.Equal(incoming.Shares) ||
		!existing.Price.Equal(incoming.Price) {
		return domain.Fill{}, fmt.Errorf("duplicate CLOB trade component changed economic identity")
	}
	if existing.Status == incoming.Status {
		if incoming.VenueUpdatedAt.After(existing.VenueUpdatedAt) {
			return incoming, nil
		}
		return existing, nil
	}
	if existing.Status.CanTransitionTo(incoming.Status) {
		return incoming, nil
	}
	if incoming.Status.CanTransitionTo(existing.Status) {
		return existing, nil
	}
	return domain.Fill{}, fmt.Errorf("duplicate CLOB trade component has conflicting terminal statuses %s and %s", existing.Status, incoming.Status)
}

// mapTradeToOrderFill 将外部值映射为 Trade To Order Fill。
func mapTradeToOrderFill(trade Trade, order domain.Order, observedAt time.Time) (domain.Fill, bool, error) {
	tradeID := strings.TrimSpace(trade.ID)
	if tradeID == "" {
		return domain.Fill{}, false, fmt.Errorf("CLOB trade omitted id")
	}
	status := domain.NormalizeFillStatus(domain.FillStatus(trade.Status))
	if !status.Valid() {
		return domain.Fill{}, false, fmt.Errorf("CLOB trade %s has unsupported status %q", tradeID, trade.Status)
	}
	matchedAt, ok := parseTradeTime(trade.MatchTime)
	if !ok {
		return domain.Fill{}, false, fmt.Errorf("CLOB trade %s has invalid match_time %q", tradeID, trade.MatchTime)
	}
	updatedAt, _ := parseTradeTime(trade.LastUpdate)
	if updatedAt.IsZero() {
		updatedAt = matchedAt
	}
	role := domain.LiquidityRoleTaker
	shares := trade.Size
	price := trade.Price
	feeRate := trade.FeeRateBPS
	matched := strings.EqualFold(strings.TrimSpace(trade.TakerOrderID), strings.TrimSpace(order.VenueOrderID))
	if !matched {
		role = domain.LiquidityRoleMaker
		for _, maker := range trade.MakerOrders {
			if !strings.EqualFold(strings.TrimSpace(maker.OrderID), strings.TrimSpace(order.VenueOrderID)) {
				continue
			}
			shares = maker.MatchedAmount
			price = maker.Price
			if price.IsEmpty() {
				price = trade.Price
			}
			feeRate = maker.FeeRateBPS
			matched = true
			break
		}
	}
	if !matched {
		return domain.Fill{}, false, nil
	}
	raw, _ := json.Marshal(trade)
	digest := sha256.Sum256(raw)
	fill := domain.Fill{
		Venue:              "polymarket",
		VenueFillID:        tradeID,
		OrderID:            order.ID,
		VenueOrderID:       order.VenueOrderID,
		ExecutionAccountID: order.Intent.ExecutionAccountID,
		MarketID:           order.Intent.MarketID,
		ConditionID:        order.Intent.ConditionID,
		TokenID:            order.Intent.TokenID,
		Side:               order.Intent.Side,
		LiquidityRole:      role,
		Status:             status,
		Shares:             shares,
		Price:              price,
		FeeRateBPS:         feeRate,
		TransactionHash:    trade.TransactionHash,
		MatchedAt:          matchedAt,
		VenueUpdatedAt:     updatedAt,
		ObservedAt:         observedAt,
		RawPayloadSHA256:   hex.EncodeToString(digest[:]),
	}
	if status == domain.FillStatusConfirmed {
		confirmedAt := updatedAt
		fill.ConfirmedAt = &confirmedAt
	}
	return fill, true, nil
}

const v2OrderFilledFeeSource = domain.FeeSourcePolygonV2OrderFilled

// getMarketFeeSchedule reads the V2 fee curve rather than treating the
// fee_rate_bps field on an order component as a complete fee formula.
func (client *TradingClient) getMarketFeeSchedule(
	ctx context.Context,
	conditionID string,
	tokenID string,
) (marketFeeSchedule, error) {
	conditionID = strings.TrimSpace(conditionID)
	tokenID = strings.TrimSpace(tokenID)
	if conditionID == "" || tokenID == "" {
		return marketFeeSchedule{}, fmt.Errorf("condition id and token id are required for fee evidence")
	}
	cacheKey := conditionID + "\x00" + tokenID
	client.feeScheduleMu.RLock()
	cached, exists := client.feeSchedules[cacheKey]
	client.feeScheduleMu.RUnlock()
	if exists {
		return cached, nil
	}
	body, _, err := client.do(
		ctx, TradingAccount{}, http.MethodGet, "/clob-markets/"+url.PathEscape(conditionID), nil, nil, false, false,
	)
	if err != nil {
		return marketFeeSchedule{}, fmt.Errorf("read CLOB V2 market fee schedule: %w", err)
	}
	var response struct {
		ConditionID string `json:"c"`
		Tokens      []struct {
			TokenID string `json:"t"`
		} `json:"t"`
		FeeDetails *struct {
			Rate      json.RawMessage `json:"r"`
			Exponent  json.RawMessage `json:"e"`
			TakerOnly *bool           `json:"to"`
		} `json:"fd"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return marketFeeSchedule{}, fmt.Errorf("decode CLOB V2 market fee schedule: %w", err)
	}
	if response.ConditionID != "" && !strings.EqualFold(strings.TrimSpace(response.ConditionID), conditionID) {
		return marketFeeSchedule{}, fmt.Errorf("CLOB V2 market fee schedule condition id mismatch")
	}
	tokenFound := false
	for _, token := range response.Tokens {
		if strings.TrimSpace(token.TokenID) == tokenID {
			tokenFound = true
			break
		}
	}
	if !tokenFound {
		return marketFeeSchedule{}, fmt.Errorf("CLOB V2 market fee schedule omitted requested token")
	}
	schedule := marketFeeSchedule{Rate: "0", Exponent: "0", TakerOnly: true}
	if response.FeeDetails != nil {
		rate, err := decimalFromJSON(response.FeeDetails.Rate)
		if err != nil {
			return marketFeeSchedule{}, fmt.Errorf("decode CLOB V2 fee rate: %w", err)
		}
		exponent, err := decimalFromJSON(response.FeeDetails.Exponent)
		if err != nil {
			return marketFeeSchedule{}, fmt.Errorf("decode CLOB V2 fee exponent: %w", err)
		}
		if sign, err := rate.Sign(); err != nil || sign < 0 {
			return marketFeeSchedule{}, fmt.Errorf("CLOB V2 fee rate must be non-negative")
		}
		if err := validateFeeExponent(exponent); err != nil {
			return marketFeeSchedule{}, err
		}
		takerOnly := response.FeeDetails.TakerOnly != nil && *response.FeeDetails.TakerOnly
		if sign, _ := rate.Sign(); sign > 0 && !takerOnly {
			return marketFeeSchedule{}, fmt.Errorf("CLOB V2 non-taker-only fee schedule is unsupported")
		}
		schedule = marketFeeSchedule{Rate: rate, Exponent: exponent, TakerOnly: takerOnly}
	}
	client.feeScheduleMu.Lock()
	client.feeSchedules[cacheKey] = schedule
	client.feeScheduleMu.Unlock()
	return schedule, nil
}

func (client *TradingClient) attachFillFeeEvidence(
	ctx context.Context,
	account TradingAccount,
	order domain.Order,
	fill domain.Fill,
	schedule marketFeeSchedule,
) (domain.Fill, error) {
	if client.feeEvidence == nil {
		return domain.Fill{}, fmt.Errorf("authoritative Polygon OrderFilled fee evidence source is not configured")
	}
	if order.MarketValidation == nil {
		return domain.Fill{}, fmt.Errorf("persisted market validation is required for settlement evidence")
	}
	transactionHash := strings.TrimSpace(fill.TransactionHash)
	if transactionHash == "" {
		return domain.Fill{}, fmt.Errorf("confirmed CLOB trade omitted transaction hash")
	}
	exchange := polygonExchangeV2
	if order.MarketValidation.NegRisk {
		exchange = polygonNegRiskExchangeV2
	}
	builderCode := normalizeBytes32(client.builder.builderCode)
	if builderCode == "" {
		return domain.Fill{}, fmt.Errorf("configured builder code is invalid")
	}
	evidence, err := client.feeEvidence.ResolveFillFeeEvidence(ctx, FillFeeEvidenceRequest{
		TransactionHash:         transactionHash,
		VenueOrderID:            order.VenueOrderID,
		ExecutionAccountID:      order.Intent.ExecutionAccountID,
		ExpectedExchangeAddress: exchange,
		ExpectedMakerAddress:    account.FunderAddress,
		ExpectedBuilderCode:     builderCode,
		Side:                    fill.Side,
		TokenID:                 fill.TokenID,
		Shares:                  fill.Shares,
		Price:                   fill.Price,
	})
	if err != nil {
		return domain.Fill{}, err
	}
	return applyFillFeeEvidence(
		fill, schedule, evidence, order.MarketValidation.TickSize,
		exchange, account.FunderAddress, builderCode,
	)
}

func applyFillFeeEvidence(
	fill domain.Fill,
	schedule marketFeeSchedule,
	evidence FillFeeEvidence,
	tickSize domain.Decimal,
	expectedExchange string,
	expectedMaker string,
	expectedBuilder string,
) (domain.Fill, error) {
	if fill.FeeRateBPS.IsEmpty() {
		return domain.Fill{}, fmt.Errorf("CLOB trade omitted fee_rate_bps metadata")
	}
	if sign, err := fill.FeeRateBPS.Sign(); err != nil || sign < 0 {
		return domain.Fill{}, fmt.Errorf("CLOB trade fee_rate_bps metadata must be non-negative")
	}
	if strings.ToUpper(strings.TrimSpace(evidence.Source)) != v2OrderFilledFeeSource {
		return domain.Fill{}, fmt.Errorf("unsupported fee evidence source %q", evidence.Source)
	}
	if !strings.EqualFold(strings.TrimSpace(evidence.ExchangeAddress), expectedExchange) ||
		!strings.EqualFold(strings.TrimSpace(evidence.TransactionHash), strings.TrimSpace(fill.TransactionHash)) ||
		!strings.EqualFold(strings.TrimSpace(evidence.OrderHash), strings.TrimSpace(fill.VenueOrderID)) ||
		!strings.EqualFold(strings.TrimSpace(evidence.MakerAddress), strings.TrimSpace(expectedMaker)) ||
		strings.TrimSpace(evidence.TokenID) != strings.TrimSpace(fill.TokenID) ||
		evidence.Side != fill.Side ||
		!strings.EqualFold(strings.TrimSpace(evidence.BuilderCode), expectedBuilder) {
		return domain.Fill{}, fmt.Errorf("OrderFilled evidence identity does not match the CLOB fill")
	}
	if evidence.CollateralDecimals != 6 || evidence.OutcomeTokenDecimals != 6 {
		return domain.Fill{}, fmt.Errorf("OrderFilled evidence must use 6-decimal pUSD and outcome-token units")
	}
	if evidence.BlockNumber == 0 || strings.TrimSpace(evidence.BlockHash) == "" || evidence.Confirmations == 0 {
		return domain.Fill{}, fmt.Errorf("OrderFilled evidence is not finalized")
	}
	makerAmount, err := decimalFromBaseUnits(evidence.MakerAmountBaseUnits, evidence.CollateralDecimals)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("OrderFilled maker amount: %w", err)
	}
	takerAmount, err := decimalFromBaseUnits(evidence.TakerAmountBaseUnits, evidence.OutcomeTokenDecimals)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("OrderFilled taker amount: %w", err)
	}
	var eventShares, eventGross domain.Decimal
	switch fill.Side {
	case domain.SideBuy:
		eventGross, eventShares = makerAmount, takerAmount
	case domain.SideSell:
		eventShares, eventGross = makerAmount, takerAmount
	default:
		return domain.Fill{}, fmt.Errorf("unsupported OrderFilled side %q", fill.Side)
	}
	if !eventShares.Equal(fill.Shares) {
		return domain.Fill{}, fmt.Errorf("CLOB shares do not match OrderFilled base-unit amounts")
	}
	if err := validateEventGross(fill.Shares, fill.Price, eventGross, tickSize, evidence.CollateralDecimals); err != nil {
		return domain.Fill{}, err
	}
	totalFee, err := decimalFromBaseUnits(evidence.TotalFeeBaseUnits, evidence.CollateralDecimals)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("OrderFilled total fee: %w", err)
	}
	if !evidence.BuilderFeeKnown {
		return domain.Fill{}, fmt.Errorf("OrderFilled total fee lacks authoritative builder-fee allocation")
	}
	builderFee, err := decimalFromBaseUnits(evidence.BuilderFeeBaseUnits, evidence.CollateralDecimals)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("OrderFilled builder fee: %w", err)
	}
	if comparison, err := builderFee.Compare(totalFee); err != nil || comparison > 0 {
		return domain.Fill{}, fmt.Errorf("builder fee exceeds OrderFilled total fee")
	}
	if strings.EqualFold(expectedBuilder, zeroBytes32) && !builderFee.Equal("0") {
		return domain.Fill{}, fmt.Errorf("zero builder code cannot have a builder fee")
	}
	platformFee, err := subtractDecimal(totalFee, builderFee)
	if err != nil {
		return domain.Fill{}, err
	}
	if fill.LiquidityRole == domain.LiquidityRoleMaker {
		if !platformFee.Equal("0") {
			return domain.Fill{}, fmt.Errorf("V2 maker fill contains a platform fee")
		}
	} else {
		expectedFee, err := calculateV2PlatformFee(fill.Shares, fill.Price, schedule.Rate, schedule.Exponent)
		if err != nil {
			return domain.Fill{}, err
		}
		if !platformFee.Equal(expectedFee) {
			return domain.Fill{}, fmt.Errorf("OrderFilled platform fee %s does not match V2 fee curve %s", platformFee, expectedFee)
		}
	}
	builderRate := evidence.BuilderFeeRateBPS
	if builderRate.IsEmpty() {
		if !builderFee.Equal("0") {
			return domain.Fill{}, fmt.Errorf("positive builder fee lacks an authoritative builder fee rate")
		}
		builderRate = "0"
	}
	if sign, err := builderRate.Sign(); err != nil || sign < 0 {
		return domain.Fill{}, fmt.Errorf("builder fee rate bps must be non-negative")
	}
	fill.GrossNotional = eventGross
	fill.PlatformFeeRate = schedule.Rate
	fill.FeeExponent = schedule.Exponent
	fill.PlatformFee = platformFee
	fill.BuilderFeeRateBPS = builderRate
	fill.BuilderFee = builderFee
	fill.TotalFee = totalFee
	fill.FeeSource = v2OrderFilledFeeSource
	fill.PriceTickSize = tickSize
	fill.SettlementEvidence = &domain.SettlementEvidence{
		SchemaVersion:        domain.SettlementEvidenceSchemaV1,
		Source:               domain.FeeSourcePolygonV2OrderFilled,
		ChainID:              domain.SettlementEvidencePolygonChainID,
		ExchangeAddress:      evidence.ExchangeAddress,
		TransactionHash:      evidence.TransactionHash,
		BlockNumber:          evidence.BlockNumber,
		BlockHash:            evidence.BlockHash,
		LogIndex:             evidence.LogIndex,
		Confirmations:        evidence.Confirmations,
		OrderHash:            evidence.OrderHash,
		MakerAddress:         evidence.MakerAddress,
		TokenID:              evidence.TokenID,
		Side:                 evidence.Side,
		MakerAmountBaseUnits: evidence.MakerAmountBaseUnits,
		TakerAmountBaseUnits: evidence.TakerAmountBaseUnits,
		TotalFeeBaseUnits:    evidence.TotalFeeBaseUnits,
		BuilderCode:          evidence.BuilderCode,
		BuilderFeeKnown:      evidence.BuilderFeeKnown,
		BuilderFeeBaseUnits:  evidence.BuilderFeeBaseUnits,
		CollateralDecimals:   evidence.CollateralDecimals,
		OutcomeTokenDecimals: evidence.OutcomeTokenDecimals,
	}
	if strings.EqualFold(expectedBuilder, zeroBytes32) {
		fill.SettlementEvidence.BuilderFeeSource = domain.SettlementEvidenceZeroBuilder
	}
	fill.RawPayloadSHA256 = settlementEvidenceDigest(fill.RawPayloadSHA256, evidence)
	return fill, nil
}

func decimalFromBaseUnits(raw string, decimals uint8) (domain.Decimal, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || (len(raw) > 1 && raw[0] == '0') {
		return "", fmt.Errorf("amount must be a canonical uint256 base-unit string")
	}
	for _, character := range raw {
		if character < '0' || character > '9' {
			return "", fmt.Errorf("amount must be a canonical uint256 base-unit string")
		}
	}
	value, ok := new(big.Int).SetString(raw, 10)
	if !ok || value.Sign() < 0 || value.BitLen() > 256 {
		return "", fmt.Errorf("amount must be a canonical uint256 base-unit string")
	}
	digits := value.String()
	scale := int(decimals)
	for len(digits) <= scale {
		digits = "0" + digits
	}
	if scale > 0 {
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	parsed, err := domain.ParseDecimal(canonicalDecimalText(digits))
	if err != nil {
		return "", err
	}
	return parsed, nil
}

func decimalFromWireBaseUnits(raw json.RawMessage, field string) (domain.Decimal, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", fmt.Errorf("CLOB V2 %s is required", field)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("CLOB V2 %s must be a string-encoded uint256", field)
	}
	parsed, err := decimalFromBaseUnits(value, 6)
	if err != nil {
		return "", fmt.Errorf("CLOB V2 %s: %w", field, err)
	}
	return parsed, nil
}

func decimalFromWireOrderQuantity(raw json.RawMessage, field string) (domain.Decimal, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("CLOB V2 %s must be a decimal string", field)
	}
	if !strings.Contains(value, ".") {
		return decimalFromWireBaseUnits(raw, field)
	}
	parsed, err := domain.ParseDecimal(value)
	if err != nil {
		return "", fmt.Errorf("CLOB V2 %s: %w", field, err)
	}
	if sign, err := parsed.Sign(); err != nil || sign < 0 {
		return "", fmt.Errorf("CLOB V2 %s must be non-negative", field)
	}
	return parsed, nil
}

func validateEventGross(
	shares domain.Decimal,
	price domain.Decimal,
	eventGross domain.Decimal,
	tickSize domain.Decimal,
	decimals uint8,
) error {
	return domain.ValidateSettlementEventGross(shares, price, eventGross, tickSize, decimals)
}

func subtractDecimal(left domain.Decimal, right domain.Decimal) (domain.Decimal, error) {
	leftRat, err := decimalRat(left)
	if err != nil {
		return "", err
	}
	rightRat, err := decimalRat(right)
	if err != nil {
		return "", err
	}
	return exactRatDecimal(new(big.Rat).Sub(leftRat, rightRat), 18)
}

func validateFeeExponent(exponent domain.Decimal) error {
	value, err := decimalRat(exponent)
	if err != nil || value.Sign() < 0 || !value.IsInt() || value.Num().BitLen() > 16 {
		return fmt.Errorf("CLOB V2 fee exponent must be a non-negative integer")
	}
	return nil
}

func calculateV2PlatformFee(
	shares domain.Decimal,
	price domain.Decimal,
	feeRate domain.Decimal,
	exponent domain.Decimal,
) (domain.Decimal, error) {
	if err := validateFeeExponent(exponent); err != nil {
		return "", err
	}
	sharesRat, err := decimalRat(shares)
	if err != nil {
		return "", err
	}
	priceRat, err := decimalRat(price)
	if err != nil {
		return "", err
	}
	rateRat, err := decimalRat(feeRate)
	if err != nil || rateRat.Sign() < 0 {
		return "", fmt.Errorf("CLOB V2 fee rate must be non-negative")
	}
	curve := new(big.Rat).Mul(priceRat, new(big.Rat).Sub(big.NewRat(1, 1), priceRat))
	exponentValue := exponentInt(exponent)
	powered := big.NewRat(1, 1)
	for value := uint64(0); value < exponentValue; value++ {
		powered.Mul(powered, curve)
	}
	fee := new(big.Rat).Mul(sharesRat, rateRat)
	fee.Mul(fee, powered)
	return truncatedRatDecimal(fee, 5), nil
}

func exactRatDecimal(value *big.Rat, maxScale int) (domain.Decimal, error) {
	if value == nil || maxScale < 0 {
		return "", fmt.Errorf("invalid exact decimal conversion")
	}
	for scale := 0; scale <= maxScale; scale++ {
		text := value.FloatString(scale)
		parsed, err := domain.ParseDecimal(canonicalDecimalText(text))
		if err != nil {
			return "", err
		}
		parsedRat, err := decimalRat(parsed)
		if err == nil && parsedRat.Cmp(value) == 0 {
			return parsed, nil
		}
	}
	return "", fmt.Errorf("decimal requires more than %d fractional digits", maxScale)
}

// truncatedRatDecimal truncates to the protocol's five-decimal fee quantum.
// Polymarket documents fees below that quantum as zero, and the authoritative
// V2 OrderFilled event reports the same downward precision rule.
func truncatedRatDecimal(value *big.Rat, scale int) domain.Decimal {
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled := new(big.Rat).Mul(value, new(big.Rat).SetInt(factor))
	quotient := new(big.Int).Quo(scaled.Num(), scaled.Denom())
	text := new(big.Rat).SetFrac(quotient, factor).FloatString(scale)
	parsed, _ := domain.ParseDecimal(canonicalDecimalText(text))
	return parsed
}

func canonicalDecimalText(value string) string {
	if strings.Contains(value, ".") {
		value = strings.TrimRight(value, "0")
		value = strings.TrimRight(value, ".")
	}
	if value == "" || value == "-0" {
		return "0"
	}
	return value
}

func exponentInt(value domain.Decimal) uint64 {
	parsed, _ := decimalRat(value)
	return parsed.Num().Uint64()
}

func settlementEvidenceDigest(clobDigest string, evidence FillFeeEvidence) string {
	// Confirmations are derived from the current chain head and increase on
	// every later reconciliation. They prove the configured threshold at read
	// time, but are not part of the immutable OrderFilled event identity.
	evidence.Confirmations = 0
	payload, _ := json.Marshal(struct {
		CLOBDigest string          `json:"clob_digest"`
		Evidence   FillFeeEvidence `json:"evidence"`
	}{CLOBDigest: strings.TrimSpace(clobDigest), Evidence: evidence})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// GetTickSize 查询指定 token 当前允许的价格步长。
func (client *TradingClient) GetTickSize(ctx context.Context, tokenID string) (domain.Decimal, error) {
	query := url.Values{"token_id": []string{strings.TrimSpace(tokenID)}}
	body, _, err := client.do(ctx, TradingAccount{}, http.MethodGet, "/tick-size", query, nil, false, false)
	if err != nil {
		return "", err
	}
	var response struct {
		MinimumTickSize json.RawMessage `json:"minimum_tick_size"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("decode CLOB tick_size: %w", err)
	}
	return decimalFromJSON(response.MinimumTickSize)
}

// GetNegRisk 查询指定 token 是否属于负风险市场。
func (client *TradingClient) GetNegRisk(ctx context.Context, tokenID string) (bool, error) {
	query := url.Values{"token_id": []string{strings.TrimSpace(tokenID)}}
	body, _, err := client.do(ctx, TradingAccount{}, http.MethodGet, "/neg-risk", query, nil, false, false)
	if err != nil {
		return false, err
	}
	var response struct {
		NegRisk *bool `json:"neg_risk"`
	}
	if err := json.Unmarshal(body, &response); err != nil || response.NegRisk == nil {
		return false, fmt.Errorf("decode CLOB neg_risk response")
	}
	return *response.NegRisk, nil
}

// ensureV2 确保 V 2 满足前置条件。
func (client *TradingClient) ensureV2(ctx context.Context) error {
	client.versionMu.Lock()
	defer client.versionMu.Unlock()
	if client.versionChecked {
		return nil
	}
	body, _, err := client.do(ctx, TradingAccount{}, http.MethodGet, "/version", nil, nil, false, false)
	if err != nil {
		return err
	}
	var response struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(body, &response); err != nil || response.Version != 2 {
		return newInvalidError("CLOB_VERSION_MISMATCH", "trading adapter requires Polymarket CLOB protocol version 2")
	}
	client.versionChecked = true
	return nil
}

// account 处理对应的内部或 HTTP 业务请求。
func (client *TradingClient) account(ctx context.Context, executionAccountID string) (TradingAccount, error) {
	account, err := client.credentials.Account(ctx, executionAccountID)
	if err != nil {
		return TradingAccount{}, newInvalidError("EXECUTION_ACCOUNT_NOT_CONFIGURED", err.Error())
	}
	if err := account.validate(); err != nil {
		return TradingAccount{}, newInvalidError("EXECUTION_ACCOUNT_INVALID", err.Error())
	}
	return account, nil
}

// listOrdersPage 查询并解码一页 CLOB 订单。
func (client *TradingClient) listOrdersPage(ctx context.Context, account TradingAccount, query url.Values) ([]rawOrder, error) {
	var orders []rawOrder
	cursor := "MA=="
	for pages := 0; pages < 100; pages++ {
		query.Set("next_cursor", cursor)
		body, _, err := client.doAuthenticated(ctx, account, http.MethodGet, "/data/orders", query, nil, false)
		if err != nil {
			return nil, err
		}
		var envelope struct {
			Data          []rawOrder `json:"data"`
			Items         []rawOrder `json:"items"`
			NextCursor    string     `json:"next_cursor"`
			NextCursorAlt string     `json:"nextCursor"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			// Some deployments return a bare array for the first page.
			if arrayErr := json.Unmarshal(body, &envelope.Data); arrayErr != nil {
				return nil, fmt.Errorf("decode CLOB open orders: %w", err)
			}
		}
		page := envelope.Data
		if page == nil {
			page = envelope.Items
		}
		orders = append(orders, page...)
		nextCursor := firstNonEmpty(envelope.NextCursor, envelope.NextCursorAlt)
		if nextCursor == "" || nextCursor == "LTE=" || nextCursor == cursor {
			return orders, nil
		}
		cursor = nextCursor
	}
	return nil, fmt.Errorf("CLOB open-order pagination exceeded 100 pages")
}

// doAuthenticated 使用账户凭证发送经过 L2 鉴权的 CLOB 请求。
func (client *TradingClient) doAuthenticated(ctx context.Context, account TradingAccount, method, path string, query url.Values, body []byte, mutating bool) ([]byte, http.Header, error) {
	return client.do(ctx, account, method, path, query, body, true, mutating)
}

// do 执行限流后的 CLOB HTTP 请求并统一错误语义。
func (client *TradingClient) do(ctx context.Context, account TradingAccount, method, path string, query url.Values, body []byte, authenticated, mutating bool) ([]byte, http.Header, error) {
	if err := client.limiter.Wait(ctx, client.now()); err != nil {
		return nil, nil, err
	}
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	endpoint := client.baseURL.ResolveReference(&url.URL{Path: path})
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(requestContext, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	if authenticated {
		timestamp := client.protocolNow().Unix()
		signature, err := hmacSignature(account.API.Secret, timestamp, method, path, body)
		if err != nil {
			return nil, nil, newInvalidError("CLOB_CREDENTIALS_INVALID", err.Error())
		}
		request.Header.Set("POLY_ADDRESS", account.Signer.Address())
		request.Header.Set("POLY_SIGNATURE", signature)
		request.Header.Set("POLY_TIMESTAMP", strconv.FormatInt(timestamp, 10))
		request.Header.Set("POLY_API_KEY", account.API.Key)
		request.Header.Set("POLY_PASSPHRASE", account.API.Passphrase)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		if mutating {
			return nil, nil, ambiguousTransportError("CLOB_TRANSPORT_OUTCOME_UNKNOWN", err)
		}
		return nil, nil, &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_TRANSPORT_UNAVAILABLE", Cause: err}
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxTradingResponseBytes+1))
	if readErr != nil {
		if mutating {
			return nil, response.Header, ambiguousTransportError("CLOB_RESPONSE_OUTCOME_UNKNOWN", readErr)
		}
		return nil, response.Header, &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_RESPONSE_UNAVAILABLE", Cause: readErr}
	}
	if len(responseBody) > maxTradingResponseBytes {
		if mutating {
			return nil, response.Header, ambiguousTransportError("CLOB_RESPONSE_TOO_LARGE", fmt.Errorf("response exceeds %d bytes", maxTradingResponseBytes))
		}
		return nil, response.Header, &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_RESPONSE_TOO_LARGE"}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, response.Header, mapHTTPError(method, response.StatusCode, response.Header, responseBody)
	}
	return responseBody, response.Header, nil
}

// protocolNow applies the offset established by ProbeProtocol. Before startup
// probing it is identical to the injected/local clock.
func (client *TradingClient) protocolNow() time.Time {
	client.clockMu.RLock()
	offset := client.clockOffset
	client.clockMu.RUnlock()
	return client.now().UTC().Add(offset)
}

func parseProtocolTime(payload []byte) (time.Time, error) {
	raw := strings.TrimSpace(string(payload))
	raw = strings.Trim(raw, "\"")
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, fmt.Errorf("decode CLOB server time")
	}
	return time.Unix(seconds, 0).UTC(), nil
}

// hmacSignature 构建订单签名或鉴权所需的规范化字节数据。
func hmacSignature(secret string, timestamp int64, method, path string, body []byte) (string, error) {
	secret = strings.TrimSpace(secret)
	padded := secret + strings.Repeat("=", (4-len(secret)%4)%4)
	key, err := base64.URLEncoding.DecodeString(padded)
	if err != nil {
		return "", fmt.Errorf("decode CLOB api secret: %w", err)
	}
	message := strconv.FormatInt(timestamp, 10) + method + path
	if len(body) > 0 {
		message += string(body)
	}
	digest := hmac.New(sha256.New, key)
	_, _ = digest.Write([]byte(message))
	return base64.URLEncoding.EncodeToString(digest.Sum(nil)), nil
}

// placementState 将外部或订单状态映射为目标领域状态。
func placementState(status string) port.VenueOrderState {
	normalized := strings.ToLower(strings.TrimSpace(status))
	normalized = strings.TrimPrefix(normalized, "order_status_")
	switch normalized {
	case "matched", "filled":
		return port.VenueOrderFilled
	case "live", "open":
		return port.VenueOrderLive
	case "cancelled", "canceled", "canceled_market_resolved", "cancelled_market_resolved":
		return port.VenueOrderCancelled
	case "rejected", "failed", "invalid":
		return port.VenueOrderRejected
	case "delayed", "accepted", "acknowledged":
		return port.VenueOrderAcknowledged
	case "unmatched":
		// Placement succeeded but the response does not prove whether the
		// order is now resting. The reconciler will fetch the actual state.
		return port.VenueOrderAcknowledged
	default:
		return port.VenueOrderUnknown
	}
}

// normalizeRawOrder 规范化 原始数据 Order 的字段和表示。
func normalizeRawOrder(raw rawOrder, order domain.Order, observedAt time.Time) (port.VenueOrder, error) {
	original := raw.OriginalSize
	if original.IsEmpty() {
		original = order.Intent.Size
	}
	filled := raw.SizeMatched
	if filled.IsEmpty() {
		filled = "0"
	}
	state := placementState(raw.Status)
	filledSign, err := filled.Sign()
	if err != nil || filledSign < 0 {
		return port.VenueOrder{}, fmt.Errorf("CLOB order contains invalid size_matched")
	}
	comparison, err := filled.Compare(original)
	if err != nil || (comparison > 0 && !order.AllowsBuySharePriceImprovement()) {
		return port.VenueOrder{}, fmt.Errorf("CLOB size_matched exceeds original_size")
	}
	if state == port.VenueOrderLive && filledSign > 0 {
		state = port.VenueOrderPartiallyFilled
	}
	if state == port.VenueOrderFilled {
		if comparison == 0 || (comparison > 0 && order.AllowsBuySharePriceImprovement()) {
			state = port.VenueOrderFilled
		} else if filledSign > 0 {
			state = port.VenueOrderPartiallyFilled
		} else {
			state = port.VenueOrderAcknowledged
		}
	}
	return port.VenueOrder{
		ID:         strings.TrimSpace(raw.ID),
		State:      state,
		RawStatus:  raw.Status,
		FilledSize: filled,
		ObservedAt: observedAt,
		TradeIDs:   append([]string(nil), raw.AssociateTrades...),
	}, nil
}

// decimalFromJSON 从字符串或数字 JSON 字段解析十进制值。
func decimalFromJSON(raw json.RawMessage) (domain.Decimal, error) {
	if len(raw) == 0 {
		return "", fmt.Errorf("decimal field is missing")
	}
	var text string
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return "", err
		}
	} else {
		text = string(raw)
	}
	return domain.ParseDecimal(text)
}

// nonNegativeIntegerFromJSON accepts the string or number representation used
// by CLOB while preserving arbitrarily large base-unit values.
func nonNegativeIntegerFromJSON(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", fmt.Errorf("integer field is missing")
	}
	value := strings.TrimSpace(string(raw))
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", err
		}
		value = strings.TrimSpace(value)
	}
	integer, ok := new(big.Int).SetString(value, 10)
	if !ok || integer.Sign() < 0 {
		return "", fmt.Errorf("value must be a non-negative base-10 integer")
	}
	return integer.String(), nil
}

// containsOrderID 判断集合是否包含 Order 标识。
func containsOrderID(values []string, orderID string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(orderID)) {
			return true
		}
	}
	return false
}

// setQuery 设置 Query。
func setQuery(query url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		query.Set(key, value)
	}
}

// parseTradeTime 解析 Trade Time。
func parseTradeTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), true
	}
	integer, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	if integer > 1_000_000_000_000 {
		return time.UnixMilli(integer).UTC(), true
	}
	return time.Unix(integer, 0).UTC(), true
}

// firstNonEmpty 返回 Non Empty 中第一个非空值。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// responseAcceptance 解析并规范化 Acceptance。
func responseAcceptance(success, ok *bool) (accepted bool, known bool) {
	if success != nil && ok != nil && *success != *ok {
		return false, false
	}
	if success != nil {
		return *success, true
	}
	if ok != nil {
		return *ok, true
	}
	return false, false
}

// appendDistinct 追加并限制 Distinct。
func appendDistinct(existing []string, values ...string) []string {
	result := append([]string(nil), existing...)
	seen := make(map[string]struct{}, len(result)+len(values))
	for _, value := range result {
		seen[strings.TrimSpace(value)] = struct{}{}
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// tokenBucket 表示后端使用的 tokenBucket 类型。
type tokenBucket struct {
	mu       sync.Mutex
	rate     float64
	capacity float64
	tokens   float64
	last     time.Time
}

// newTokenBucket 创建并初始化 Token Bucket。
func newTokenBucket(rate float64, burst int) (*tokenBucket, error) {
	if rate <= 0 || rate > 1000 || burst < 1 || burst > 1000 {
		return nil, fmt.Errorf("Polymarket rate limit requires 0 < requests_per_second <= 1000 and 1 <= burst <= 1000")
	}
	return &tokenBucket{rate: rate, capacity: float64(burst), tokens: float64(burst)}, nil
}

// Wait 等待令牌桶允许下一次请求或上下文结束。
func (bucket *tokenBucket) Wait(ctx context.Context, now time.Time) error {
	for {
		bucket.mu.Lock()
		if bucket.last.IsZero() {
			bucket.last = now
		}
		elapsed := now.Sub(bucket.last).Seconds()
		if elapsed > 0 {
			bucket.tokens = min(bucket.capacity, bucket.tokens+elapsed*bucket.rate)
			bucket.last = now
		}
		if bucket.tokens >= 1 {
			bucket.tokens--
			bucket.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - bucket.tokens) / bucket.rate * float64(time.Second))
		bucket.mu.Unlock()
		timer := time.NewTimer(max(wait, time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case now = <-timer.C:
		}
	}
}

var _ port.PreparedVenue = (*TradingClient)(nil)

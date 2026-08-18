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
	BaseURL            string
	HTTPClient         *http.Client
	Credentials        CredentialProvider
	RequestTimeout     time.Duration
	RequestsPerSecond  float64
	Burst              int
	BuilderCode        string
	BuilderMakerFeeBPS domain.Decimal
	BuilderTakerFeeBPS domain.Decimal
	Metadata           string
	MinBuyNotional     domain.Decimal
	Now                func() time.Time
	Random             io.Reader
}

// TradingClient 表示后端使用的 TradingClient 类型。
type TradingClient struct {
	baseURL            *url.URL
	httpClient         *http.Client
	credentials        CredentialProvider
	timeout            time.Duration
	limiter            *tokenBucket
	builder            orderBuilder
	builderMakerFeeBPS domain.Decimal
	builderTakerFeeBPS domain.Decimal
	now                func() time.Time

	versionMu      sync.Mutex
	versionChecked bool
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
	TraderSide      string         `json:"trader_side"`
	MakerOrders     []MakerOrder   `json:"maker_orders"`
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
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: params.RequestTimeout}
	}
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
	for name, value := range map[string]*domain.Decimal{
		"builder maker fee": &params.BuilderMakerFeeBPS,
		"builder taker fee": &params.BuilderTakerFeeBPS,
	} {
		if value.IsEmpty() {
			*value = "0"
		}
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return nil, fmt.Errorf("%s bps must be non-negative", name)
		}
		if comparison, err := value.Compare("10000"); err != nil || comparison > 0 {
			return nil, fmt.Errorf("%s bps must not exceed 10000", name)
		}
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &TradingClient{
		baseURL:            baseURL,
		httpClient:         params.HTTPClient,
		credentials:        params.Credentials,
		timeout:            params.RequestTimeout,
		limiter:            limiter,
		now:                params.Now,
		builderMakerFeeBPS: params.BuilderMakerFeeBPS,
		builderTakerFeeBPS: params.BuilderTakerFeeBPS,
		builder: orderBuilder{
			chainID:        polygonChainID,
			builderCode:    params.BuilderCode,
			metadata:       params.Metadata,
			minBuyNotional: params.MinBuyNotional,
			now:            params.Now,
			random:         params.Random,
		},
	}, nil
}

// Name 返回当前交易场所适配器名称。
func (client *TradingClient) Name() string { return "polymarket" }

// Place 将已校验订单提交到当前交易场所。
func (client *TradingClient) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	account, err := client.account(ctx, order.Intent.ExecutionAccountID)
	if err != nil {
		return port.VenueOrder{}, err
	}
	if err := client.ensureV2(ctx); err != nil {
		return port.VenueOrder{}, err
	}
	currentTick, err := client.GetTickSize(ctx, order.Intent.TokenID)
	if err != nil {
		return port.VenueOrder{}, err
	}
	if order.MarketValidation == nil || !currentTick.Equal(order.MarketValidation.TickSize) {
		return port.VenueOrder{}, newInvalidError("TICK_SIZE_CHANGED", "CLOB tick_size changed after market validation")
	}
	currentNegRisk, err := client.GetNegRisk(ctx, order.Intent.TokenID)
	if err != nil {
		return port.VenueOrder{}, err
	}
	if currentNegRisk != order.MarketValidation.NegRisk {
		return port.VenueOrder{}, newInvalidError("NEG_RISK_CHANGED", "CLOB neg_risk changed after market validation")
	}
	payload, expectedOrderID, err := client.builder.build(ctx, order, account)
	if err != nil {
		return port.VenueOrder{}, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return port.VenueOrder{}, fmt.Errorf("marshal signed CLOB order: %w", err)
	}
	responseBody, _, err := client.doAuthenticated(ctx, account, http.MethodPost, "/order", nil, body, true)
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
	state := placementState(response.Status)
	tradeIDs := append(append([]string(nil), response.TradeIDs...), response.TradeIDsAlt...)
	// A placement status of matched can represent a full or partial immediate
	// match. The POST response does not carry authoritative size_matched, so a
	// follow-up GET must determine the actual cumulative fill. If that read is
	// unavailable, acceptance is still known and remains ACKNOWLEDGED.
	if state == port.VenueOrderFilled {
		observedOrder := order
		observedOrder.VenueOrderID = orderID
		if observed, getErr := client.Get(ctx, observedOrder); getErr == nil {
			observed.TradeIDs = appendDistinct(observed.TradeIDs, tradeIDs...)
			return observed, nil
		}
		state = port.VenueOrderAcknowledged
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
	normalized, err := normalizeRawOrder(raw, order.Intent.Size, client.now().UTC())
	if err != nil {
		return port.VenueOrder{}, err
	}
	if sign, _ := normalized.FilledSize.Sign(); sign > 0 {
		averagePrice, tradeIDs, err := client.fillAveragePrice(ctx, order.Intent.ExecutionAccountID, raw)
		if err != nil {
			return port.VenueOrder{}, &port.VenueError{
				Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE",
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

// ListTrades 分页查询指定执行账户的交易所成交。
func (client *TradingClient) ListTrades(ctx context.Context, executionAccountID string, filter TradeFilter) ([]Trade, error) {
	account, err := client.account(ctx, executionAccountID)
	if err != nil {
		return nil, err
	}
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
			return append(trades, bare...), nil
		}
		page := envelope.Data
		if page == nil {
			page = envelope.Items
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
		var orderIDs []string
		traderSide := strings.ToUpper(strings.TrimSpace(trade.TraderSide))
		if traderSide == "TAKER" {
			orderIDs = appendDistinct(orderIDs, trade.TakerOrderID)
		}
		if traderSide == "MAKER" || traderSide == "" {
			for _, maker := range trade.MakerOrders {
				address := strings.ToLower(strings.TrimSpace(maker.MakerAddress))
				if address == strings.ToLower(account.FunderAddress) || address == strings.ToLower(account.Signer.Address()) {
					orderIDs = appendDistinct(orderIDs, maker.OrderID)
				}
			}
		}
		observedAt, ok := parseTradeTime(trade.LastUpdate)
		if !ok {
			observedAt, ok = parseTradeTime(trade.MatchTime)
		}
		if !ok {
			observedAt = client.now().UTC()
		}
		result = append(result, domain.VenueTradeSnapshot{
			VenueTradeID: strings.TrimSpace(trade.ID), OrderIDs: orderIDs,
			ConditionID: strings.TrimSpace(trade.Market), TokenID: strings.TrimSpace(trade.TokenID),
			Status: domain.NormalizeFillStatus(domain.FillStatus(trade.Status)), ObservedAt: observedAt,
		})
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
	fills := make([]domain.Fill, 0)
	seen := make(map[string]int)
	for _, trade := range trades {
		fill, matched, err := mapTradeToOrderFill(trade, order, client.now().UTC())
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		if fill.BuilderFeeRateBPS.IsEmpty() || fill.BuilderFeeRateBPS.Equal("0") {
			if fill.LiquidityRole == domain.LiquidityRoleMaker {
				fill.BuilderFeeRateBPS = client.builderMakerFeeBPS
			} else {
				fill.BuilderFeeRateBPS = client.builderTakerFeeBPS
			}
		}
		identity := fill.VenueFillID + "\x00" + fill.OrderID
		if existingIndex, exists := seen[identity]; exists {
			merged, err := preferTradeObservation(fills[existingIndex], fill)
			if err != nil {
				return nil, err
			}
			fills[existingIndex] = merged
			continue
		}
		seen[identity] = len(fills)
		fills = append(fills, fill)
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
	builderFeeRate := domain.Decimal("0")
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
			feeRate = "0"
			builderFeeRate = maker.FeeRateBPS
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
		BuilderFeeRateBPS:  builderFeeRate,
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
		timestamp := client.now().UTC().Unix()
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
func normalizeRawOrder(raw rawOrder, fallbackSize domain.Decimal, observedAt time.Time) (port.VenueOrder, error) {
	original := raw.OriginalSize
	if original.IsEmpty() {
		original = fallbackSize
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
	if err != nil || comparison > 0 {
		return port.VenueOrder{}, fmt.Errorf("CLOB size_matched exceeds original_size")
	}
	if state == port.VenueOrderLive && filledSign > 0 {
		state = port.VenueOrderPartiallyFilled
	}
	if state == port.VenueOrderFilled {
		if comparison == 0 {
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

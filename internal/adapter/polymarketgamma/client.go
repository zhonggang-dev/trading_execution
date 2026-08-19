package polymarketgamma

import (
	"bytes"
	"context"
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
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	defaultRequestTimeout = 5 * time.Second
	defaultMaxAttempts    = 3
	maxResponseBytes      = 4 << 20
)

// Params 包含 Polymarket Gamma Market Universe 客户端配置。
type Params struct {
	BaseURL     string
	HTTPClient  *http.Client
	MaxAttempts int
	UserAgent   string
	Now         func() time.Time
}

// Client 直接从 Polymarket Gamma API 读取并严格校验权威 Market 元数据。
type Client struct {
	baseURL     *url.URL
	httpClient  *http.Client
	maxAttempts int
	userAgent   string
	now         func() time.Time
}

var _ port.MarketUniverse = (*Client)(nil)

// gammaMarket 只声明执行链路信任的 Gamma 字段。指针用于区分 false 与缺失/null。
type gammaMarket struct {
	ID                    flexibleString  `json:"id"`
	ConditionID           string          `json:"conditionId"`
	Active                *bool           `json:"active"`
	Closed                *bool           `json:"closed"`
	ClosedTime            *string         `json:"closedTime"`
	AcceptingOrders       *bool           `json:"acceptingOrders"`
	EnableOrderBook       *bool           `json:"enableOrderBook"`
	NegRisk               *bool           `json:"negRisk"`
	OrderPriceMinTickSize json.RawMessage `json:"orderPriceMinTickSize"`
	Outcomes              flexibleStrings `json:"outcomes"`
	CLOBTokenIDs          flexibleStrings `json:"clobTokenIds"`
	UMAResolutionStatus   string          `json:"umaResolutionStatus"`
}

// New 校验配置并创建 Gamma Market Universe 客户端。
func New(params Params) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return nil, fmt.Errorf("Polymarket Gamma base URL must be a valid HTTP or HTTPS URL")
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" || baseURL.User != nil {
		return nil, fmt.Errorf("Polymarket Gamma base URL cannot contain credentials, a query, or a fragment")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	if params.MaxAttempts == 0 {
		params.MaxAttempts = defaultMaxAttempts
	}
	if params.MaxAttempts < 1 || params.MaxAttempts > 6 {
		return nil, fmt.Errorf("Polymarket Gamma maximum attempts must be between 1 and 6")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Client{
		baseURL:     baseURL,
		httpClient:  params.HTTPClient,
		maxAttempts: params.MaxAttempts,
		userAgent:   strings.TrimSpace(params.UserAgent),
		now:         params.Now,
	}, nil
}

// FindByCondition 按 condition_id 查询 open/closed 两个集合并要求恰好匹配一个 Market。
func (client *Client) FindByCondition(
	ctx context.Context,
	conditionID string,
) (domain.MarketSnapshot, bool, error) {
	conditionID = strings.TrimSpace(conditionID)
	if err := validateConditionID(conditionID); err != nil {
		return domain.MarketSnapshot{}, false, err
	}
	queryConditionID := strings.ToLower(conditionID)

	markets := make([]gammaMarket, 0, 1)
	for _, closed := range []bool{false, true} {
		page, err := client.fetchByCondition(ctx, queryConditionID, closed)
		if err != nil {
			return domain.MarketSnapshot{}, false, err
		}
		for index, market := range page {
			returned := strings.TrimSpace(market.ConditionID)
			if !strings.EqualFold(returned, conditionID) {
				return domain.MarketSnapshot{}, false, fmt.Errorf(
					"Gamma %s market %d returned condition_id %q for %q",
					closedLabel(closed), index, returned, conditionID,
				)
			}
			if market.Closed != nil && *market.Closed != closed {
				return domain.MarketSnapshot{}, false, fmt.Errorf(
					"Gamma %s market %d contradicts its closed filter",
					closedLabel(closed), index,
				)
			}
		}
		markets = append(markets, page...)
	}

	if len(markets) == 0 {
		return domain.MarketSnapshot{}, false, nil
	}
	if len(markets) != 1 {
		return domain.MarketSnapshot{}, false, fmt.Errorf(
			"Gamma returned %d markets for condition_id %q; expected exactly one",
			len(markets), conditionID,
		)
	}
	observedAt := client.now().UTC()
	if observedAt.IsZero() {
		return domain.MarketSnapshot{}, false, fmt.Errorf("Gamma observation time is required")
	}
	market, err := mapMarket(markets[0], conditionID, observedAt)
	if err != nil {
		return domain.MarketSnapshot{}, false, err
	}
	return market, true, nil
}

// fetchByCondition 查询指定 closed 分区。limit=2 使服务端重复记录可被检测。
func (client *Client) fetchByCondition(
	ctx context.Context,
	conditionID string,
	closed bool,
) ([]gammaMarket, error) {
	endpoint := *client.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/markets"
	query := endpoint.Query()
	query.Set("condition_ids", conditionID)
	query.Set("closed", strconv.FormatBool(closed))
	query.Set("limit", "2")
	endpoint.RawQuery = query.Encode()

	body, err := client.get(ctx, endpoint.String())
	if err != nil {
		return nil, fmt.Errorf("query Gamma %s markets by condition_id: %w", closedLabel(closed), err)
	}
	if bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		return nil, fmt.Errorf("decode Gamma %s markets: response must be a JSON array", closedLabel(closed))
	}
	var markets []gammaMarket
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, fmt.Errorf("decode Gamma %s markets: %w", closedLabel(closed), err)
	}
	if len(markets) > 1 {
		return nil, fmt.Errorf(
			"Gamma returned multiple %s markets for condition_id %q",
			closedLabel(closed), conditionID,
		)
	}
	return markets, nil
}

// get 使用有限重试读取公开 Gamma API；上下文错误和非暂态 HTTP 错误不重试。
func (client *Client) get(ctx context.Context, requestURL string) ([]byte, error) {
	var lastErr error
	attempts := 0
	for attempt := 1; attempt <= client.maxAttempts; attempt++ {
		attempts = attempt
		body, retry, err := client.getOnce(ctx, requestURL)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !retry || attempt == client.maxAttempts {
			break
		}
		if err := waitForRetry(ctx, time.Duration(attempt)*200*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("request Gamma after %d attempt(s): %w", attempts, lastErr)
}

// getOnce 执行一次受大小限制的 Gamma GET 请求。
func (client *Client) getOnce(ctx context.Context, requestURL string) ([]byte, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("build Gamma request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if client.userAgent != "" {
		request.Header.Set("User-Agent", client.userAgent)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, false, err
		}
		return nil, true, fmt.Errorf("send Gamma request: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, true, fmt.Errorf("read Gamma response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return nil, false, fmt.Errorf("Gamma response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500,
			fmt.Errorf("Gamma returned HTTP %d: %s", response.StatusCode, responseMessage(body))
	}
	return body, false, nil
}

// mapMarket 将已唯一定位的 Gamma Market 映射到执行领域并校验全部受信字段。
func mapMarket(raw gammaMarket, requestedConditionID string, observedAt time.Time) (domain.MarketSnapshot, error) {
	marketID := strings.TrimSpace(string(raw.ID))
	if marketID == "" {
		return domain.MarketSnapshot{}, fmt.Errorf("Gamma market %q is missing id", requestedConditionID)
	}
	if raw.Active == nil || raw.Closed == nil || raw.AcceptingOrders == nil ||
		raw.EnableOrderBook == nil || raw.NegRisk == nil {
		return domain.MarketSnapshot{}, fmt.Errorf("Gamma market %q is missing required state fields", requestedConditionID)
	}

	tickSize, err := parseTickSize(raw.OrderPriceMinTickSize)
	if err != nil {
		return domain.MarketSnapshot{}, fmt.Errorf("Gamma market %q has invalid orderPriceMinTickSize: %w", requestedConditionID, err)
	}
	outcomes, err := mapOutcomes(raw.Outcomes, raw.CLOBTokenIDs)
	if err != nil {
		return domain.MarketSnapshot{}, fmt.Errorf("Gamma market %q has invalid outcome mapping: %w", requestedConditionID, err)
	}

	closedAt, err := mapClosedTime(raw.ClosedTime)
	if err != nil {
		return domain.MarketSnapshot{}, fmt.Errorf("Gamma market %q has invalid closedTime: %w", requestedConditionID, err)
	}
	resolved := strings.EqualFold(strings.TrimSpace(raw.UMAResolutionStatus), "resolved")
	if err := validateState(raw, resolved, closedAt, observedAt); err != nil {
		return domain.MarketSnapshot{}, fmt.Errorf("Gamma market %q has inconsistent state: %w", requestedConditionID, err)
	}

	return domain.MarketSnapshot{
		MarketID:        marketID,
		ConditionID:     requestedConditionID,
		Active:          *raw.Active,
		Closed:          *raw.Closed,
		Resolved:        resolved,
		ClosedAt:        closedAt,
		Paused:          !*raw.EnableOrderBook,
		AcceptingOrders: *raw.AcceptingOrders,
		NegRisk:         *raw.NegRisk,
		TickSize:        tickSize,
		Outcomes:        outcomes,
		ObservedAt:      observedAt,
	}, nil
}

// validateState 拒绝会让交易门禁产生歧义的 Gamma 状态组合。
func validateState(raw gammaMarket, resolved bool, closedAt *time.Time, observedAt time.Time) error {
	if *raw.Closed {
		if closedAt == nil {
			return fmt.Errorf("closed market requires closedTime")
		}
		if *raw.AcceptingOrders {
			return fmt.Errorf("closed market cannot accept orders")
		}
	} else if closedAt != nil {
		return fmt.Errorf("open market cannot contain closedTime")
	}
	if closedAt != nil && closedAt.After(observedAt) {
		return fmt.Errorf("closedTime cannot be after observation time")
	}
	if resolved && !*raw.Closed {
		return fmt.Errorf("resolved market must be closed")
	}
	if *raw.AcceptingOrders && (!*raw.Active || !*raw.EnableOrderBook) {
		return fmt.Errorf("market accepting orders must be active with order book enabled")
	}
	return nil
}

// mapOutcomes 保持 Gamma outcomes 与 clobTokenIds 的索引对应关系。
func mapOutcomes(names, tokenIDs []string) ([]domain.MarketOutcome, error) {
	if len(names) != 2 || len(tokenIDs) != 2 {
		return nil, fmt.Errorf("binary market requires exactly two outcomes and two token ids")
	}
	result := make([]domain.MarketOutcome, 2)
	seenNames := make(map[string]struct{}, 2)
	seenTokens := make(map[string]struct{}, 2)
	for index := range names {
		name := strings.TrimSpace(names[index])
		tokenID := strings.TrimSpace(tokenIDs[index])
		if name == "" {
			return nil, fmt.Errorf("outcome %d name is required", index)
		}
		nameKey := strings.ToLower(name)
		if _, duplicate := seenNames[nameKey]; duplicate {
			return nil, fmt.Errorf("outcome names are duplicated")
		}
		if err := validateTokenID(tokenID); err != nil {
			return nil, fmt.Errorf("outcome %d token id: %w", index, err)
		}
		if _, duplicate := seenTokens[tokenID]; duplicate {
			return nil, fmt.Errorf("outcome token ids are duplicated")
		}
		seenNames[nameKey] = struct{}{}
		seenTokens[tokenID] = struct{}{}
		result[index] = domain.MarketOutcome{Index: index, Name: name, TokenID: tokenID}
	}
	return result, nil
}

// parseTickSize 接受官方 number 以及 Gamma 历史上出现过的 numeric string。
func parseTickSize(raw json.RawMessage) (domain.Decimal, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", fmt.Errorf("value is required")
	}
	value := string(trimmed)
	if trimmed[0] == '"' {
		if err := json.Unmarshal(trimmed, &value); err != nil {
			return "", fmt.Errorf("decode numeric string: %w", err)
		}
	}
	decimal, err := domain.ParseDecimal(value)
	if err != nil {
		return "", err
	}
	if sign, err := decimal.Sign(); err != nil || sign <= 0 {
		return "", fmt.Errorf("value must be positive")
	}
	if comparison, err := decimal.Compare(domain.Decimal("1")); err != nil || comparison > 0 {
		return "", fmt.Errorf("value must not exceed 1")
	}
	return decimal, nil
}

// mapClosedTime 解析 Gamma 当前及历史 closedTime 格式并统一为 UTC。
func mapClosedTime(value *string) (*time.Time, error) {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil, nil
	}
	text := strings.TrimSpace(*value)
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05Z07",
	}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, text)
		if err == nil {
			parsed = parsed.UTC()
			return &parsed, nil
		}
	}
	return nil, fmt.Errorf("unsupported timestamp %q", text)
}

// validateConditionID 要求 Polygon CTF bytes32 的 0x 十六进制格式。
func validateConditionID(conditionID string) error {
	if len(conditionID) != 66 || !strings.HasPrefix(conditionID, "0x") {
		return fmt.Errorf("condition_id must be a 0x-prefixed 32-byte hex value")
	}
	if _, err := hex.DecodeString(conditionID[2:]); err != nil {
		return fmt.Errorf("condition_id must be a 0x-prefixed 32-byte hex value")
	}
	return nil
}

// validateTokenID 要求合法、正数且不超过 uint256 的十进制 outcome token id。
func validateTokenID(tokenID string) error {
	if tokenID == "" {
		return fmt.Errorf("value is required")
	}
	if len(tokenID) > 1 && tokenID[0] == '0' {
		return fmt.Errorf("value must use canonical unsigned decimal form")
	}
	for _, character := range tokenID {
		if character < '0' || character > '9' {
			return fmt.Errorf("value must be an unsigned decimal integer")
		}
	}
	value, ok := new(big.Int).SetString(tokenID, 10)
	if !ok || value.Sign() <= 0 || value.BitLen() > 256 {
		return fmt.Errorf("value must be a positive uint256")
	}
	return nil
}

// flexibleString 兼容 Gamma id 的字符串和历史数字形式。
type flexibleString string

// UnmarshalJSON 解析字符串或整数标量。
func (value *flexibleString) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	var text string
	if err := json.Unmarshal(trimmed, &text); err == nil {
		*value = flexibleString(text)
		return nil
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil || strings.ContainsAny(number.String(), ".eE") {
		return fmt.Errorf("expected a string or integer")
	}
	*value = flexibleString(number.String())
	return nil
}

// flexibleStrings 兼容 Gamma 字符串数组及其 JSON 字符串形式。
type flexibleStrings []string

// UnmarshalJSON 解析直接数组或 JSON-encoded 数组。
func (values *flexibleStrings) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		*values = nil
		return nil
	}
	var direct []string
	if err := json.Unmarshal(trimmed, &direct); err == nil {
		*values = direct
		return nil
	}
	var encoded string
	if err := json.Unmarshal(trimmed, &encoded); err != nil {
		return fmt.Errorf("expected a string array or JSON-encoded string array")
	}
	if err := json.Unmarshal([]byte(encoded), &direct); err != nil {
		return fmt.Errorf("decode JSON-encoded string array: %w", err)
	}
	*values = direct
	return nil
}

// waitForRetry 等待有限退避并及时响应上下文取消。
func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// responseMessage 限制错误响应长度，避免上游大页面进入日志。
func responseMessage(body []byte) string {
	message := strings.TrimSpace(string(body))
	if len(message) > 500 {
		message = message[:500]
	}
	if message == "" {
		return "empty response"
	}
	return message
}

func closedLabel(closed bool) string {
	if closed {
		return "closed"
	}
	return "open"
}

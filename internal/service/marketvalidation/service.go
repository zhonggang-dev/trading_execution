package marketvalidation

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	defaultMaxUniverseAge   = 5 * time.Minute
	defaultMaxLatestBookAge = 10 * time.Second
	defaultMaxFutureSkew    = 2 * time.Second
)

// Params 表示后端使用的 Params 类型。
type Params struct {
	Universe         port.MarketUniverse
	OrderBooks       port.OrderBookSource
	MaxUniverseAge   time.Duration
	MaxLatestBookAge time.Duration
	MaxFutureSkew    time.Duration
	Now              func() time.Time
}

// Service 使用权威 Market 元数据和最新订单簿执行失败关闭校验，不包含任何策略规则。
type Service struct {
	universe         port.MarketUniverse
	orderBooks       port.OrderBookSource
	maxUniverseAge   time.Duration
	maxLatestBookAge time.Duration
	maxFutureSkew    time.Duration
	now              func() time.Time
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.Universe == nil || params.OrderBooks == nil {
		return nil, fmt.Errorf("market universe and orderbook source are required")
	}
	if params.MaxUniverseAge == 0 {
		params.MaxUniverseAge = defaultMaxUniverseAge
	}
	if params.MaxLatestBookAge == 0 {
		params.MaxLatestBookAge = defaultMaxLatestBookAge
	}
	if params.MaxFutureSkew == 0 {
		params.MaxFutureSkew = defaultMaxFutureSkew
	}
	if params.MaxUniverseAge < 0 || params.MaxLatestBookAge < 0 || params.MaxFutureSkew < 0 {
		return nil, fmt.Errorf("market validation durations must not be negative")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{
		universe:         params.Universe,
		orderBooks:       params.OrderBooks,
		maxUniverseAge:   params.MaxUniverseAge,
		maxLatestBookAge: params.MaxLatestBookAge,
		maxFutureSkew:    params.MaxFutureSkew,
		now:              params.Now,
	}, nil
}

// Validate 校验当前模型的字段完整性和业务约束。
func (service *Service) Validate(ctx context.Context, intent domain.OrderIntent) (domain.MarketValidation, error) {
	intent = intent.Normalize()
	if err := requireMarketContext(intent); err != nil {
		return domain.MarketValidation{}, err
	}
	now := service.now().UTC()
	// The strategy snapshot is immutable audit evidence, not the execution
	// quote. Execution freshness is enforced against the official order book
	// captured below, so an old strategy snapshot must not block a fresh order.
	if err := validateNotFuture("MARKET_SNAPSHOT", *intent.MarketSnapshotAt, now, service.maxFutureSkew); err != nil {
		return domain.MarketValidation{}, err
	}

	market, found, err := service.universe.FindByCondition(ctx, intent.ConditionID)
	if err != nil {
		return domain.MarketValidation{}, fmt.Errorf("query market universe: %w", err)
	}
	if !found {
		return domain.MarketValidation{}, reject("MARKET_NOT_FOUND", "condition_id is not present in Market Universe Service")
	}
	if err := service.validateMarket(intent, market, now); err != nil {
		return domain.MarketValidation{}, err
	}
	if err := validateOutcomeMapping(market.Outcomes); err != nil {
		return domain.MarketValidation{}, err
	}
	outcome, found := outcomeAt(market.Outcomes, *intent.OutcomeIndex)
	if !found {
		return domain.MarketValidation{}, reject("OUTCOME_NOT_FOUND", "outcome_index is not present in current market metadata")
	}
	if strings.TrimSpace(outcome.TokenID) != intent.TokenID ||
		!strings.EqualFold(strings.TrimSpace(outcome.Name), intent.OutcomeName) {
		return domain.MarketValidation{}, reject("OUTCOME_TOKEN_MISMATCH", "outcome name/index does not map to the submitted token_id")
	}
	if market.NegRisk != *intent.ExpectedNegRisk {
		return domain.MarketValidation{}, reject("NEG_RISK_MISMATCH", "market neg_risk changed after the strategy snapshot")
	}
	if err := validatePrices(intent, market.TickSize); err != nil {
		return domain.MarketValidation{}, err
	}

	target, err := (domain.BookTargetParams{
		MarketID:     strings.TrimSpace(market.MarketID),
		ConditionID:  strings.TrimSpace(market.ConditionID),
		OutcomeIndex: outcome.Index,
		TokenID:      strings.TrimSpace(outcome.TokenID),
	}).Build()
	if err != nil {
		return domain.MarketValidation{}, reject("MARKET_IDENTITY_INVALID", err.Error())
	}
	books, err := service.orderBooks.Capture(ctx, now, []domain.BookTarget{target})
	if err != nil {
		return domain.MarketValidation{}, fmt.Errorf("capture latest orderbook: %w", err)
	}
	if len(books) != 1 {
		return domain.MarketValidation{}, reject("LATEST_BOOK_UNAVAILABLE", "latest orderbook response is incomplete")
	}
	book := books[0]
	if book.Status != domain.OrderBookStatusOK || len(book.Bids) == 0 || len(book.Asks) == 0 {
		return domain.MarketValidation{}, reject("LATEST_BOOK_UNAVAILABLE", "latest orderbook does not contain both sides")
	}
	if book.MarketID != target.MarketID || book.ConditionID != target.ConditionID ||
		book.OutcomeIndex != target.OutcomeIndex || book.TokenID != target.TokenID {
		return domain.MarketValidation{}, reject("LATEST_BOOK_IDENTITY_MISMATCH", "latest orderbook identity does not match the authoritative outcome")
	}
	if err := book.Validate(); err != nil {
		return domain.MarketValidation{}, reject("LATEST_BOOK_INVALID", err.Error())
	}
	if !book.TickSize.IsEmpty() && !book.TickSize.Equal(market.TickSize) {
		return domain.MarketValidation{}, reject("TICK_SIZE_CHANGED", "latest CLOB orderbook tick_size differs from Market Universe Service")
	}
	if crossed, err := book.Bids[0].Price.Compare(book.Asks[0].Price); err != nil || crossed > 0 {
		return domain.MarketValidation{}, reject("LATEST_BOOK_INVALID", "latest best bid exceeds best ask")
	}
	if err := validateAge("LATEST_BOOK_SOURCE", book.SourceAt, now, service.maxLatestBookAge, service.maxFutureSkew); err != nil {
		return domain.MarketValidation{}, err
	}
	if err := validateAge("LATEST_BOOK_OBSERVATION", book.ObservedAt, now, service.maxLatestBookAge, service.maxFutureSkew); err != nil {
		return domain.MarketValidation{}, err
	}
	if err := validateWorstPrice(intent.Side, intent.WorstPrice, book.Bids[0].Price, book.Asks[0].Price); err != nil {
		return domain.MarketValidation{}, err
	}

	params := domain.MarketValidationParams{
		Mode:                 "LIVE_CHECK",
		ValidatedAt:          now,
		MarketObservedAt:     market.ObservedAt.UTC(),
		StrategySnapshotAt:   intent.MarketSnapshotAt.UTC(),
		LatestBookSourceAt:   book.SourceAt.UTC(),
		LatestBookObservedAt: book.ObservedAt.UTC(),
		OutcomeIndex:         outcome.Index,
		OutcomeName:          strings.TrimSpace(outcome.Name),
		TokenID:              strings.TrimSpace(outcome.TokenID),
		NegRisk:              market.NegRisk,
		TickSize:             market.TickSize,
		MinOrderSize:         book.MinOrderSize,
		BestBid:              book.Bids[0].Price,
		BestAsk:              book.Asks[0].Price,
		WorstPrice:           intent.WorstPrice,
	}
	if intent.TimeInForce == domain.TimeInForceIOC {
		executableSize, err := protectedExecutableSize(intent, book)
		if err != nil {
			return domain.MarketValidation{}, err
		}
		params.ExecutableSize = executableSize
	}
	return params.Build()
}

// protectedExecutableSize measures what an IOC can take right now: the visible
// quantity on the executable side of the fresh book at prices inside
// worst_price, capped at the strategy size. The venue adapter submits that
// quantity so the emulated IOC leaves the smallest possible remainder to
// cancel. worst_price is a price ceiling, never a spend budget: size shares is
// the maximum, whatever they cost inside the limit. The only floor is the
// venue min_order_size; a thinner protected book fails closed.
func protectedExecutableSize(intent domain.OrderIntent, book domain.OrderBookSnapshot) (domain.Decimal, error) {
	levels := book.Asks
	if intent.Side == domain.SideSell {
		levels = book.Bids
	}
	depth := new(big.Rat)
	scale := 0
	for _, level := range levels {
		comparison, err := level.Price.Compare(intent.WorstPrice)
		if err != nil {
			return "", reject("LATEST_BOOK_INVALID", "latest orderbook level price is invalid")
		}
		executable := intent.Side == domain.SideBuy && comparison <= 0 || intent.Side == domain.SideSell && comparison >= 0
		if !executable {
			continue
		}
		size, err := level.Size.Multiply("1")
		if err != nil {
			return "", reject("LATEST_BOOK_INVALID", "latest orderbook level size is invalid")
		}
		depth.Add(depth, size)
		scale = max(scale, decimalScale(level.Size))
	}
	requested, err := intent.Size.Multiply("1")
	if err != nil || requested.Sign() <= 0 {
		return "", reject("INVALID_SIZE", "order size is not a positive decimal")
	}
	if depth.Sign() <= 0 {
		return "", reject("NO_PROTECTED_LIQUIDITY", "latest orderbook has no visible liquidity inside the strategy worst_price")
	}
	executable := depth
	if requested.Cmp(depth) < 0 {
		executable = requested
		scale = decimalScale(intent.Size)
	}
	if !book.MinOrderSize.IsEmpty() {
		minimum, err := book.MinOrderSize.Multiply("1")
		if err != nil || executable.Cmp(minimum) < 0 {
			return "", reject("PROTECTED_LIQUIDITY_BELOW_MIN_ORDER_SIZE",
				fmt.Sprintf("only %s shares are executable inside worst_price, below the venue min_order_size %s",
					executable.FloatString(scale), book.MinOrderSize))
		}
	}
	return canonicalDecimal(executable.FloatString(scale)), nil
}

// decimalScale 计算十进制值所需的小数位数。
func decimalScale(value domain.Decimal) int {
	text := strings.TrimRight(strings.TrimSpace(value.String()), "0")
	if point := strings.IndexByte(text, '.'); point >= 0 {
		return len(text) - point - 1
	}
	return 0
}

// canonicalDecimal 去掉尾随零，使数量的文本表示与策略输入保持一致。
func canonicalDecimal(text string) domain.Decimal {
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	return domain.Decimal(text)
}

// requireMarketContext 检查并要求 Market Context 完整。
func requireMarketContext(intent domain.OrderIntent) error {
	if intent.ConditionID == "" || intent.OutcomeIndex == nil || intent.OutcomeName == "" ||
		intent.ExpectedNegRisk == nil || intent.MarketSnapshotAt == nil || intent.WorstPrice.IsEmpty() {
		return reject("MARKET_CONTEXT_REQUIRED", "condition_id, outcome identity, expected_neg_risk, market_snapshot_at, and worst_price are required")
	}
	return nil
}

// validateMarket 校验 Market 的字段和业务约束。
func (service *Service) validateMarket(intent domain.OrderIntent, market domain.MarketSnapshot, now time.Time) error {
	if strings.TrimSpace(market.ConditionID) != intent.ConditionID || strings.TrimSpace(market.MarketID) != intent.MarketID {
		return reject("MARKET_IDENTITY_MISMATCH", "condition_id no longer resolves to the submitted market_id")
	}
	if err := validateAge("MARKET_METADATA", market.ObservedAt, now, service.maxUniverseAge, service.maxFutureSkew); err != nil {
		return err
	}
	if market.Resolved {
		return reject("MARKET_RESOLVED", "market is already resolved")
	}
	if market.Closed || !market.Active {
		return reject("MARKET_CLOSED", "market is closed or inactive")
	}
	if market.Paused {
		return reject("MARKET_PAUSED", "market trading is paused")
	}
	if !market.AcceptingOrders {
		return reject("MARKET_NOT_ACCEPTING_ORDERS", "market is not accepting orders")
	}
	if sign, err := market.TickSize.Sign(); err != nil || sign <= 0 {
		return reject("INVALID_TICK_SIZE", "Market Universe Service returned an invalid tick_size")
	}
	return nil
}

// validatePrices 校验 Prices 的字段和业务约束。
func validatePrices(intent domain.OrderIntent, tickSize domain.Decimal) error {
	if intent.Type == domain.OrderTypeLimit && !intent.Price.Equal(intent.WorstPrice) {
		return reject("LIMIT_PRICE_MISMATCH", "limit price must equal the strategy worst_price")
	}
	prices := []struct {
		name  string
		value domain.Decimal
	}{{name: "worst_price", value: intent.WorstPrice}}
	if intent.Type == domain.OrderTypeLimit {
		prices = append(prices, struct {
			name  string
			value domain.Decimal
		}{name: "price", value: intent.Price})
	}
	for _, price := range prices {
		comparison, err := price.value.Compare(domain.Decimal("1"))
		if err != nil || comparison > 0 {
			return reject("PRICE_OUT_OF_RANGE", price.name+" must be in (0,1]")
		}
		multiple, err := price.value.IsMultipleOf(tickSize)
		if err != nil || !multiple {
			return reject("PRICE_TICK_MISMATCH", price.name+" is not an exact multiple of the current tick_size")
		}
	}
	return nil
}

// validateOutcomeMapping 校验 Outcome Mapping 的字段和业务约束。
func validateOutcomeMapping(outcomes []domain.MarketOutcome) error {
	if len(outcomes) != 2 {
		return reject("MARKET_METADATA_INVALID", "binary market must contain exactly two authoritative outcomes")
	}
	seenIndexes := make(map[int]struct{}, len(outcomes))
	seenTokens := make(map[string]struct{}, len(outcomes))
	for _, outcome := range outcomes {
		name := strings.TrimSpace(outcome.Name)
		tokenID := strings.TrimSpace(outcome.TokenID)
		if (outcome.Index != 0 && outcome.Index != 1) || name == "" || tokenID == "" {
			return reject("MARKET_METADATA_INVALID", "authoritative outcome identity is incomplete")
		}
		if _, exists := seenIndexes[outcome.Index]; exists {
			return reject("MARKET_METADATA_INVALID", "authoritative outcome indexes are duplicated")
		}
		if _, exists := seenTokens[tokenID]; exists {
			return reject("MARKET_METADATA_INVALID", "authoritative outcome token_ids are duplicated")
		}
		seenIndexes[outcome.Index] = struct{}{}
		seenTokens[tokenID] = struct{}{}
	}
	return nil
}

// validateWorstPrice 校验 Worst Price 的字段和业务约束。
func validateWorstPrice(side domain.Side, worstPrice, bestBid, bestAsk domain.Decimal) error {
	var comparison int
	var err error
	switch side {
	case domain.SideBuy:
		comparison, err = bestAsk.Compare(worstPrice)
		if err != nil || comparison > 0 {
			return reject("PRICE_DRIFT", "latest best ask exceeds the strategy worst_price")
		}
	case domain.SideSell:
		comparison, err = bestBid.Compare(worstPrice)
		if err != nil || comparison < 0 {
			return reject("PRICE_DRIFT", "latest best bid is below the strategy worst_price")
		}
	default:
		return reject("INVALID_SIDE", "order side is not supported")
	}
	return nil
}

// validateAge 校验 Age 的字段和业务约束。
func validateAge(prefix string, observedAt, now time.Time, maxAge, maxFutureSkew time.Duration) error {
	if observedAt.IsZero() {
		return reject(prefix+"_MISSING", "required timestamp is missing")
	}
	observedAt = observedAt.UTC()
	if observedAt.After(now.Add(maxFutureSkew)) {
		return reject(prefix+"_FUTURE", "timestamp is later than the allowed clock skew")
	}
	if now.Sub(observedAt) > maxAge {
		return reject(prefix+"_STALE", "timestamp is older than the configured maximum age")
	}
	return nil
}

// validateNotFuture keeps the strategy timestamp honest for audit without
// treating it as the execution-time market quote.
func validateNotFuture(prefix string, observedAt, now time.Time, maxFutureSkew time.Duration) error {
	if observedAt.IsZero() {
		return reject(prefix+"_MISSING", "required timestamp is missing")
	}
	if observedAt.UTC().After(now.Add(maxFutureSkew)) {
		return reject(prefix+"_FUTURE", "timestamp is later than the allowed clock skew")
	}
	return nil
}

// outcomeAt 按结果下标查找权威市场结果。
func outcomeAt(outcomes []domain.MarketOutcome, index int) (domain.MarketOutcome, bool) {
	for _, outcome := range outcomes {
		if outcome.Index == index {
			return outcome, true
		}
	}
	return domain.MarketOutcome{}, false
}

// reject 构建并返回 对应数据 的拒绝结果。
func reject(code, reason string) error {
	return &port.Rejection{Code: code, Reason: reason}
}

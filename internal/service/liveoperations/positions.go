package liveoperations

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// buildPositionsParams 收拢仓位事实对齐和盯市计算所需参数。
type buildPositionsParams struct {
	observedAt    time.Time
	local         []domain.LiveLedgerPosition
	observations  []accountObservation
	books         map[string]domain.OrderBookSnapshot
	exposureLimit *big.Rat
	quality       *qualityCollector
	bookErr       error
}

// positionTotals 保存资金和风险模块需要的仓位聚合结果。
type positionTotals struct {
	cost               *big.Rat
	marketValue        *big.Rat
	unrealizedPnL      *big.Rat
	maxMarketCost      *big.Rat
	maxMarketAccountID string
	oldestBookAt       time.Time
}

// buildPositions 以外部真实持仓为数量事实，并用本地账本补充成本、策略和市场身份。
func buildPositions(params buildPositionsParams) ([]domain.LivePosition, positionTotals, error) {
	totals := newPositionTotals()
	localByKey := indexLocalPositions(params.local)
	externalKeys := make(map[string]struct{})
	marketCosts := make(map[string]*big.Rat)
	result := make([]domain.LivePosition, 0, len(params.local))
	if params.bookErr != nil {
		params.quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, "当前盘口批量读取失败，仓位标记价已回退到 Data API")
	}
	for _, observation := range params.observations {
		baselines, err := makeLivePositionBaselineSet(observation.account.ExecutionAccountID, observation.baselines)
		if err != nil {
			return nil, totals, fmt.Errorf("validate account %s external position baselines: %w", observation.account.ExecutionAccountID, err)
		}
		unseenBaselines := make(map[string]domain.ExternalPositionBaseline, len(baselines.byToken))
		for tokenID, baseline := range baselines.byToken {
			unseenBaselines[tokenID] = baseline
		}
		seenExternal := make(map[string]struct{}, len(observation.positions))
		for _, external := range observation.positions {
			tokenID := strings.TrimSpace(external.TokenID)
			if tokenID == "" {
				return nil, totals, fmt.Errorf("external position token is empty for account %q", observation.account.ExecutionAccountID)
			}
			if _, duplicate := seenExternal[tokenID]; duplicate {
				return nil, totals, fmt.Errorf("external position token %q is duplicated for account %q", tokenID, observation.account.ExecutionAccountID)
			}
			seenExternal[tokenID] = struct{}{}
			if baseline, exists := baselines.byToken[tokenID]; exists {
				delete(unseenBaselines, tokenID)
				managed, include, baselineErr := managedExternalPosition(
					observation.account.ExecutionAccountID, external, baseline, localByKey,
				)
				if baselineErr != nil {
					return nil, totals, baselineErr
				}
				if !include {
					continue
				}
				external = managed
			}
			position, cost, marketValue, err := buildExternalPosition(buildExternalPositionParams{
				observedAt: params.observedAt, accountID: observation.account.ExecutionAccountID,
				external: external, localByKey: localByKey, books: params.books,
				exposureLimit: params.exposureLimit, quality: params.quality,
			})
			if err != nil {
				return nil, totals, err
			}
			if position == nil {
				continue
			}
			key := livePositionKey(observation.account.ExecutionAccountID, external.TokenID)
			externalKeys[key] = struct{}{}
			result = append(result, *position)
			totals.cost.Add(totals.cost, cost)
			totals.marketValue.Add(totals.marketValue, marketValue)
			totals.unrealizedPnL.Add(totals.unrealizedPnL, new(big.Rat).Sub(marketValue, cost))
			marketKey := observation.account.ExecutionAccountID + "\x00" + position.MarketID
			if position.MarketID == "" {
				marketKey = observation.account.ExecutionAccountID + "\x00condition:" + position.ConditionID
			}
			if marketCosts[marketKey] == nil {
				marketCosts[marketKey] = new(big.Rat)
			}
			marketCosts[marketKey].Add(marketCosts[marketKey], cost)
			if book, found := params.books[external.TokenID]; found && book.Status == domain.OrderBookStatusOK {
				totals.oldestBookAt = earlierTime(totals.oldestBookAt, book.ObservedAt)
			}
		}
		for tokenID := range unseenBaselines {
			return nil, totals, fmt.Errorf(
				"external position baseline token %q is absent for account %q",
				tokenID, observation.account.ExecutionAccountID,
			)
		}
	}
	checkMissingExternalPositions(params.local, externalKeys, params.quality)
	for marketKey, cost := range marketCosts {
		if cost.Cmp(totals.maxMarketCost) > 0 {
			totals.maxMarketCost.Set(cost)
			totals.maxMarketAccountID = strings.SplitN(marketKey, "\x00", 2)[0]
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].ExecutionAccountID != result[right].ExecutionAccountID {
			return result[left].ExecutionAccountID < result[right].ExecutionAccountID
		}
		if result[left].MarketID != result[right].MarketID {
			return result[left].MarketID < result[right].MarketID
		}
		return result[left].TokenID < result[right].TokenID
	})
	if result == nil {
		result = []domain.LivePosition{}
	}
	return result, totals, nil
}

// buildExternalPositionParams 收拢单个外部仓位的身份、价格和账本上下文。
type buildExternalPositionParams struct {
	observedAt    time.Time
	accountID     string
	external      domain.ExternalPosition
	localByKey    map[string]domain.LiveLedgerPosition
	books         map[string]domain.OrderBookSnapshot
	exposureLimit *big.Rat
	quality       *qualityCollector
}

// buildExternalPosition 生成单个仓位视图并返回成本与市值精确值。
func buildExternalPosition(params buildExternalPositionParams) (*domain.LivePosition, *big.Rat, *big.Rat, error) {
	shares, err := decimalRat(params.external.Shares)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse external shares for token %s: %w", params.external.TokenID, err)
	}
	if shares.Sign() < 0 {
		return nil, nil, nil, fmt.Errorf("external shares for token %s are negative", params.external.TokenID)
	}
	if shares.Sign() == 0 {
		return nil, nil, nil, nil
	}
	key := livePositionKey(params.accountID, params.external.TokenID)
	local, found := params.localByKey[key]
	if invalidOutcomeIndex(params.external.OutcomeIndex) {
		params.quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "Data API/链上返回了超出 YES/NO 范围的 outcome_index")
	}
	if !found {
		params.quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "发现 Data API/链上存在但本地账本缺失的仓位")
	}
	if found {
		checkPositionIdentity(local.Position, params.external, params.quality)
		checkPositionShares(local.Position, params.external, params.quality)
	}
	mark, err := resolveMarkPrice(params.external, params.books[params.external.TokenID], params.quality)
	if err != nil {
		return nil, nil, nil, err
	}
	averagePrice := params.external.AveragePrice
	if found && !local.Position.AverageCostPrice.IsEmpty() {
		averagePrice = local.Position.AverageCostPrice
	}
	if averagePrice.IsEmpty() {
		return nil, nil, nil, fmt.Errorf("average price unavailable for token %s", params.external.TokenID)
	}
	if !validUnitPrice(averagePrice) {
		return nil, nil, nil, fmt.Errorf("average price is outside [0,1] for token %s", params.external.TokenID)
	}
	cost, err := multiplyDecimal(params.external.Shares, averagePrice)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("calculate cost for token %s: %w", params.external.TokenID, err)
	}
	marketValue, err := multiplyDecimal(params.external.Shares, mark)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("calculate market value for token %s: %w", params.external.TokenID, err)
	}
	view, err := makeLivePosition(makeLivePositionParams{
		observedAt: params.observedAt, accountID: params.accountID, external: params.external,
		local: local, found: found, averagePrice: averagePrice, mark: mark,
		cost: cost, marketValue: marketValue, exposureLimit: params.exposureLimit,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return &view, cost, marketValue, nil
}

// makeLivePositionParams 收拢 HTTP 仓位视图字段。
type makeLivePositionParams struct {
	observedAt    time.Time
	accountID     string
	external      domain.ExternalPosition
	local         domain.LiveLedgerPosition
	found         bool
	averagePrice  domain.Decimal
	mark          domain.Decimal
	cost          *big.Rat
	marketValue   *big.Rat
	exposureLimit *big.Rat
}

// makeLivePosition 把已校验的精确数值转换为接口模型。
func makeLivePosition(params makeLivePositionParams) (domain.LivePosition, error) {
	marketID, marketLabel, strategyID := "", "", ""
	if params.found {
		marketID, marketLabel, strategyID = params.local.Position.MarketID, params.local.MarketLabel, params.local.StrategyID
	}
	average, err := numberFromDecimal(params.averagePrice)
	if err != nil {
		return domain.LivePosition{}, err
	}
	mark, err := numberFromDecimal(params.mark)
	if err != nil {
		return domain.LivePosition{}, err
	}
	shares, err := numberFromDecimal(params.external.Shares)
	if err != nil {
		return domain.LivePosition{}, err
	}
	costNumber, err := numberFromRat(params.cost)
	if err != nil {
		return domain.LivePosition{}, err
	}
	marketValue, err := numberFromRat(params.marketValue)
	if err != nil {
		return domain.LivePosition{}, err
	}
	unrealized, err := numberFromRat(new(big.Rat).Sub(params.marketValue, params.cost))
	if err != nil {
		return domain.LivePosition{}, err
	}
	exposure, err := numberFromRat(ratio(params.cost, params.exposureLimit))
	if err != nil {
		return domain.LivePosition{}, err
	}
	result := domain.LivePosition{
		PositionID:         params.accountID + ":" + marketID + ":" + params.external.TokenID,
		ExecutionAccountID: params.accountID, MarketID: marketID,
		ConditionID: params.external.ConditionID, TokenID: params.external.TokenID,
		MarketLabel: marketLabel, OutcomeName: params.external.OutcomeName,
		Shares: shares, AveragePrice: average, MarkPrice: mark, Cost: costNumber,
		MarketValue: marketValue, UnrealizedPnL: unrealized, ExposurePct: exposure,
		StrategyID: strategyID,
	}
	if params.found && params.local.LatestSignalAt != nil {
		age := params.observedAt.Sub(*params.local.LatestSignalAt)
		if age < 0 {
			age = 0
		}
		minutes := int64(age / time.Minute)
		result.PredictionAgeMinutes = &minutes
	}
	return result, nil
}

// resolveMarkPrice 优先使用 CLOB 最优买卖价中点，失败时回退 Data API 当前价。
func resolveMarkPrice(external domain.ExternalPosition, book domain.OrderBookSnapshot, quality *qualityCollector) (domain.Decimal, error) {
	if book.Status == domain.OrderBookStatusOK && !book.BestBid.IsEmpty() && !book.BestAsk.IsEmpty() {
		price, err := midpoint(book.BestBid, book.BestAsk)
		if err == nil && validUnitPrice(price) {
			return price, nil
		}
	}
	quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, "至少一个持仓没有完整 CLOB 双边盘口，标记价已回退到 Data API")
	if external.CurrentPrice.IsEmpty() {
		return "", fmt.Errorf("mark price unavailable for token %s", external.TokenID)
	}
	if !validUnitPrice(external.CurrentPrice) {
		return "", fmt.Errorf("invalid Data API current price for token %s", external.TokenID)
	}
	return external.CurrentPrice, nil
}

// validUnitPrice 校验二元 Outcome 价格必须落在闭区间零到一。
func validUnitPrice(value domain.Decimal) bool {
	if value.IsEmpty() {
		return false
	}
	sign, err := value.Sign()
	if err != nil || sign < 0 {
		return false
	}
	comparison, err := value.Compare("1")
	return err == nil && comparison <= 0
}

// checkPositionIdentity 核对 token 对应的 condition、outcome index 和 outcome name。
func checkPositionIdentity(local domain.Position, external domain.ExternalPosition, quality *qualityCollector) {
	if local.ConditionID == "" || external.ConditionID == "" || local.ConditionID != external.ConditionID {
		quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "发现相同 token_id 的 condition_id 不一致")
	}
	if invalidOutcomeIndex(local.OutcomeIndex) || invalidOutcomeIndex(external.OutcomeIndex) {
		quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "发现 token_id 的 outcome_index 超出 YES/NO 范围")
	}
	if local.OutcomeIndex != nil && external.OutcomeIndex != nil && *local.OutcomeIndex != *external.OutcomeIndex {
		quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "发现 token_id 的 YES/NO outcome_index 映射不一致")
	}
	if strings.TrimSpace(local.OutcomeName) == "" || strings.TrimSpace(external.OutcomeName) == "" || !strings.EqualFold(local.OutcomeName, external.OutcomeName) {
		quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "发现 token_id 的 outcome_name 映射不一致")
	}
}

// invalidOutcomeIndex 判断可选 outcome index 是否超出二元 YES/NO 范围。
func invalidOutcomeIndex(value *int) bool {
	return value != nil && *value != 0 && *value != 1
}

// checkPositionShares 核对本地仓位快照与外部真实仓位数量。
func checkPositionShares(local domain.Position, external domain.ExternalPosition, quality *qualityCollector) {
	if comparison, err := local.TotalShares.Compare(external.Shares); err != nil || comparison != 0 {
		quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "Data API/链上 shares 与本地账本仓位数量不一致")
	}
}

// checkMissingExternalPositions 检查本地非零持仓在外部事实源中是否缺失。
func checkMissingExternalPositions(local []domain.LiveLedgerPosition, externalKeys map[string]struct{}, quality *qualityCollector) {
	for _, item := range local {
		if _, found := externalKeys[livePositionKey(item.Position.ExecutionAccountID, item.Position.TokenID)]; !found {
			quality.add("positions", "链上持仓", domain.LiveHealthDegraded, "本地账本存在非零仓位，但 Data API/链上未返回对应 token_id")
			return
		}
	}
}

// indexLocalPositions 按账户和 token_id 建立本地仓位索引。
func indexLocalPositions(values []domain.LiveLedgerPosition) map[string]domain.LiveLedgerPosition {
	result := make(map[string]domain.LiveLedgerPosition, len(values))
	for _, value := range values {
		result[livePositionKey(value.Position.ExecutionAccountID, value.Position.TokenID)] = value
	}
	return result
}

// livePositionKey 生成跨钱包隔离的 token 仓位键。
func livePositionKey(accountID string, tokenID string) string {
	return strings.TrimSpace(accountID) + "\x00" + strings.TrimSpace(tokenID)
}

// newPositionTotals 创建全部字段已初始化的仓位合计。
func newPositionTotals() positionTotals {
	return positionTotals{cost: new(big.Rat), marketValue: new(big.Rat), unrealizedPnL: new(big.Rat), maxMarketCost: new(big.Rat)}
}

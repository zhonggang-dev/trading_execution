package liveoperations

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// summarizeRiskPolicies 汇总当前生效的账户限额，并以 fail-closed 方式反映控制状态。
func summarizeRiskPolicies(policies []domain.LiveRiskPolicyState, quality *qualityCollector) (*big.Rat, string, domain.LiveHealth) {
	exposureLimit := new(big.Rat)
	names := make([]string, 0, len(policies))
	health := domain.LiveHealthHealthy
	if len(policies) == 0 {
		quality.add("risk", "硬风控配置", domain.LiveHealthStopped, "没有读取到任何执行账户的生效风控策略")
		return exposureLimit, "unavailable", domain.LiveHealthStopped
	}
	for _, policy := range policies {
		if policy.PolicyID != "" {
			names = append(names, policy.PolicyID)
		}
		if err := addDecimal(exposureLimit, defaultDecimal(policy.MaxWalletExposure)); err != nil {
			quality.add("risk", "硬风控配置", domain.LiveHealthStopped, "钱包总敞口限额无法解析")
			health = domain.LiveHealthStopped
		}
		if !policy.Enabled || policy.PolicyID == "" {
			quality.add("risk", "硬风控配置", domain.LiveHealthStopped, "至少一个执行账户没有启用风控策略")
			health = domain.LiveHealthStopped
		}
		if policy.KillSwitch || policy.AccountPaused {
			quality.add("risk", "硬风控配置", domain.LiveHealthStopped, "Kill Switch 或账户暂停正在生效")
			health = domain.LiveHealthStopped
		}
	}
	if _, exists := quality.items["risk"]; !exists {
		quality.add("risk", "硬风控配置", domain.LiveHealthHealthy, "所有执行账户均启用硬风控策略")
	}
	return exposureLimit, riskPresetName(names), health
}

// riskPresetName 返回单一策略名或明确的混合策略标识。
func riskPresetName(values []string) string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	sort.Strings(unique)
	if len(unique) == 0 {
		return "unavailable"
	}
	if len(unique) == 1 {
		return unique[0]
	}
	return "mixed:" + strings.Join(unique, ",")
}

// reconciliationStatus 汇总账户最近对账状态和未关闭问题。
func reconciliationStatus(observedAt time.Time, states []domain.LiveReconciliationState, policies []domain.LiveRiskPolicyState, quality *qualityCollector) domain.LiveHealth {
	if len(states) == 0 {
		quality.add("reconcile", "链上对账", domain.LiveHealthStopped, "没有读取到任何账户的对账记录")
		return domain.LiveHealthStopped
	}
	health := domain.LiveHealthHealthy
	maxAges := make(map[string]time.Duration, len(policies))
	for _, policy := range policies {
		maxAges[policy.ExecutionAccountID] = policy.MaxStateAge
	}
	for _, state := range states {
		if state.Run.RunID == "" || state.Run.Status == domain.ReconciliationRunFailed {
			quality.add("reconcile", "链上对账", domain.LiveHealthStopped, "至少一个账户从未成功完成对账或最近对账失败")
			health = domain.LiveHealthStopped
			continue
		}
		if state.Run.Status != domain.ReconciliationRunCompleted || state.OpenIssues > 0 {
			quality.add("reconcile", "链上对账", domain.LiveHealthDegraded, fmt.Sprintf("存在 %d 个未关闭对账问题", state.OpenIssues))
			health = worstHealth(health, domain.LiveHealthDegraded)
		}
		maxAge := maxAges[state.ExecutionAccountID]
		age := time.Duration(-1)
		if state.Run.CompletedAt != nil {
			age = observedAt.Sub(*state.Run.CompletedAt)
		}
		if age < 0 || maxAge <= 0 || age > maxAge {
			quality.add("reconcile", "链上对账", domain.LiveHealthDegraded, "至少一个账户的最近成功对账已经超过硬风控 state_age")
			health = worstHealth(health, domain.LiveHealthDegraded)
		}
	}
	return health
}

// liveRiskHealth 将任一风险告警提升为引擎降级状态。
func liveRiskHealth(risks []domain.LiveRisk) domain.LiveHealth {
	for _, risk := range risks {
		if risk.State == domain.LiveFlowWarning {
			return domain.LiveHealthDegraded
		}
	}
	return domain.LiveHealthHealthy
}

// buildWorkers 返回固定的三个线程；缺失 heartbeat 时必须展示 stopped。
func buildWorkers(observedAt time.Time, states []domain.LiveWorkerState) ([]domain.LiveWorker, domain.LiveHealth) {
	byID := make(map[string]domain.LiveWorkerState, len(states))
	for _, state := range states {
		byID[domain.NormalizeLiveWorkerID(state.ThreadID)] = state
	}
	definitions := defaultWorkerDefinitions()
	result := make([]domain.LiveWorker, 0, len(definitions))
	health := domain.LiveHealthHealthy
	for _, definition := range definitions {
		state, found := byID[definition.ThreadID]
		worker := workerFromState(observedAt, definition, state, found)
		result = append(result, worker)
		health = worstHealth(health, worker.Status)
	}
	return result, health
}

// defaultWorkerDefinitions 定义三个固定业务线程及其保守超时阈值。
func defaultWorkerDefinitions() []domain.LiveWorkerState {
	return []domain.LiveWorkerState{
		{ThreadID: "cycle", Name: "机会扫描", Purpose: "找新市场、获取预测并尝试开仓", Cadence: "按 Cycle 配置", MaxHeartbeatAge: time.Hour, MetricLabel: "本轮候选"},
		{ThreadID: "monitor", Name: "持仓与挂单看护", Purpose: "退出判断、撤单、重报价与成交确认", Cadence: "每 3 分钟", MaxHeartbeatAge: 6 * time.Minute, MetricLabel: "下次检查"},
		{ThreadID: "prediction", Name: "预测调度器", Purpose: "预测请求去重、轮询与新鲜度管理", Cadence: "每 15 秒轮询", MaxHeartbeatAge: time.Minute, MetricLabel: "预测队列"},
	}
}

// workerFromState 合并线程默认说明与实际 heartbeat。
func workerFromState(observedAt time.Time, definition domain.LiveWorkerState, state domain.LiveWorkerState, found bool) domain.LiveWorker {
	worker := domain.LiveWorker{
		ID: definition.ThreadID, Name: definition.Name, Purpose: definition.Purpose,
		Cadence: definition.Cadence, Status: domain.LiveHealthStopped,
		CurrentTask: "未收到 heartbeat", MetricLabel: definition.MetricLabel, MetricValue: "-",
	}
	if !found {
		return worker
	}
	worker.Name = firstNonEmpty(state.Name, worker.Name)
	worker.Purpose = firstNonEmpty(state.Purpose, worker.Purpose)
	worker.Cadence = firstNonEmpty(state.Cadence, worker.Cadence)
	worker.CurrentTask = firstNonEmpty(state.CurrentTask, "空闲")
	worker.CurrentCycleID = state.CurrentCycleID
	worker.LastError = publicWorkerError(state.LastError)
	worker.MetricLabel = firstNonEmpty(state.MetricLabel, worker.MetricLabel)
	worker.MetricValue = firstNonEmpty(state.MetricValue, "-")
	if !state.LastHeartbeatAt.IsZero() {
		value := state.LastHeartbeatAt.UTC()
		worker.LastHeartbeatAt = &value
	}
	if state.Stopped || state.LastHeartbeatAt.IsZero() {
		return worker
	}
	maxAge := state.MaxHeartbeatAge
	if maxAge <= 0 {
		maxAge = definition.MaxHeartbeatAge
	}
	age := observedAt.Sub(state.LastHeartbeatAt)
	worker.Status = domain.LiveHealthHealthy
	if age < 0 || age > maxAge || strings.TrimSpace(state.LastError) != "" {
		worker.Status = domain.LiveHealthDegraded
	}
	return worker
}

// publicWorkerError 隐藏可能包含上游 URL 或响应正文的原始错误。
func publicWorkerError(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "最近一次任务失败，详见服务日志"
}

// normalizedFunnel 补齐固定六步并保留同一 run_id/cycle_id 查询得到的计数。
func normalizedFunnel(values []domain.LiveFunnelStage) []domain.LiveFunnelStage {
	definitions := []domain.LiveFunnelStage{
		{ID: "scan", Index: 1, Name: "市场扫描", Description: "本轮获取的活跃市场", ThroughputLabel: "尚未开始", State: domain.LiveFlowIdle},
		{ID: "filter", Index: 2, Name: "策略过滤", Description: "通过流动性、到期和领域过滤", ThroughputLabel: "尚未开始", State: domain.LiveFlowIdle},
		{ID: "predict", Index: 3, Name: "预测完成", Description: "完成有效预测的市场", ThroughputLabel: "尚未开始", State: domain.LiveFlowIdle},
		{ID: "risk", Index: 4, Name: "策略与硬风控", Description: "Edge、仓位、资金和硬风控检查", ThroughputLabel: "尚未开始", State: domain.LiveFlowIdle},
		{ID: "route", Index: 5, Name: "订单执行", Description: "成功提交到 CLOB 的订单", ThroughputLabel: "尚未开始", State: domain.LiveFlowIdle},
		{ID: "ledger", Index: 6, Name: "成交入账", Description: "已通过 /trades 验真并写入 Ledger 的成交", ThroughputLabel: "尚未开始", State: domain.LiveFlowIdle},
	}
	byID := make(map[string]domain.LiveFunnelStage, len(values))
	for _, value := range values {
		byID[value.ID] = value
	}
	for index := range definitions {
		if value, found := byID[definitions[index].ID]; found {
			definitions[index] = value
		}
	}
	return definitions
}

// buildCapitalParams 收拢资金卡片的精确计算参数。
type buildCapitalParams struct {
	equity           *big.Rat
	availableCash    *big.Rat
	grossExposure    *big.Rat
	exposureLimit    *big.Rat
	unrealizedPnL    *big.Rat
	realizedPnLToday domain.Decimal
	feeToday         domain.Decimal
}

// buildCapital 生成账户合并后的资金和盈亏卡片。
func buildCapital(params buildCapitalParams) (domain.LiveCapital, error) {
	equity, err := numberFromRat(params.equity)
	if err != nil {
		return domain.LiveCapital{}, err
	}
	available, err := numberFromRat(params.availableCash)
	if err != nil {
		return domain.LiveCapital{}, err
	}
	gross, err := numberFromRat(params.grossExposure)
	if err != nil {
		return domain.LiveCapital{}, err
	}
	limit, err := numberFromRat(params.exposureLimit)
	if err != nil {
		return domain.LiveCapital{}, err
	}
	unrealized, err := numberFromRat(params.unrealizedPnL)
	if err != nil {
		return domain.LiveCapital{}, err
	}
	realized, err := numberFromDecimal(defaultDecimal(params.realizedPnLToday))
	if err != nil {
		return domain.LiveCapital{}, err
	}
	fees, err := numberFromDecimal(defaultDecimal(params.feeToday))
	if err != nil {
		return domain.LiveCapital{}, err
	}
	return domain.LiveCapital{
		Equity: equity, AvailableCash: available, GrossExposure: gross,
		ExposureLimit: limit, RealizedPnLToday: realized,
		UnrealizedPnL: unrealized, FeeToday: fees,
	}, nil
}

// buildRisksParams 收拢前端风险指标所需的账本和风控策略。
type buildRisksParams struct {
	positionTotals positionTotals
	exposureLimit  *big.Rat
	policies       []domain.LiveRiskPolicyState
	dailyTraded    domain.Decimal
	positions      []domain.LiveLedgerPosition
	observedAt     time.Time
}

// buildRisks 只展示 Go 硬风控真实存在的口径，不虚构尚未执行的当日亏损限额。
func buildRisks(params buildRisksParams) ([]domain.LiveRisk, error) {
	maxMarketLimit := marketLimitForAccount(params.policies, params.positionTotals.maxMarketAccountID)
	dailyLimit := sumPolicyDecimal(params.policies, func(policy domain.LiveRiskPolicyState) domain.Decimal { return policy.MaxDailyTradedNotional })
	dailyCurrent, err := decimalRat(defaultDecimal(params.dailyTraded))
	if err != nil {
		return nil, err
	}
	staleCount := countStalePositions(params.positions, params.policies, params.observedAt)
	definitions := []riskDefinition{
		{id: "exposure", name: "总敞口", current: params.positionTotals.cost, limit: params.exposureLimit, unit: "$", hint: "所有未结算持仓的成本口径"},
		{id: "market", name: "单市场最大敞口", current: params.positionTotals.maxMarketCost, limit: maxMarketLimit, unit: "$", hint: "当前敞口最大的账户内市场"},
		{id: "daily_volume", name: "当日交易金额", current: dailyCurrent, limit: dailyLimit, unit: "$", hint: "按各账户 daily_timezone 统计已验真成交加活动预占，与 Go 硬风控一致"},
		{id: "stale", name: "预测过期", current: big.NewRat(staleCount, 1), limit: new(big.Rat), unit: "count", hint: "超过各账户硬风控 signal_age 的持仓数量，目标为 0"},
	}
	result := make([]domain.LiveRisk, 0, len(definitions))
	for _, definition := range definitions {
		item, err := makeRisk(definition)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

// marketLimitForAccount 返回当前最大市场敞口所属账户的真实限额。
func marketLimitForAccount(policies []domain.LiveRiskPolicyState, accountID string) *big.Rat {
	fallback := new(big.Rat)
	for _, policy := range policies {
		value, err := decimalRat(defaultDecimal(policy.MaxMarketExposure))
		if err != nil || value.Sign() <= 0 {
			continue
		}
		if fallback.Sign() == 0 || value.Cmp(fallback) < 0 {
			fallback.Set(value)
		}
		if policy.ExecutionAccountID == accountID {
			return value
		}
	}
	return fallback
}

// riskDefinition 保存单个风险指标的内部精确值。
type riskDefinition struct {
	id      string
	name    string
	current *big.Rat
	limit   *big.Rat
	unit    string
	hint    string
}

// makeRisk 把风险精确值转换成接口模型并计算安全等级。
func makeRisk(definition riskDefinition) (domain.LiveRisk, error) {
	current, err := numberFromRat(definition.current)
	if err != nil {
		return domain.LiveRisk{}, err
	}
	limit, err := numberFromRat(definition.limit)
	if err != nil {
		return domain.LiveRisk{}, err
	}
	state := domain.LiveFlowSafe
	if definition.limit.Sign() == 0 {
		if definition.current.Sign() > 0 {
			state = domain.LiveFlowWarning
		}
	} else if new(big.Rat).Mul(definition.current, big.NewRat(5, 4)).Cmp(definition.limit) >= 0 {
		state = domain.LiveFlowWarning
	}
	return domain.LiveRisk{ID: definition.id, Name: definition.name, Current: current, Limit: limit, Unit: definition.unit, Hint: definition.hint, State: state}, nil
}

// sumPolicyDecimal 返回所有账户可独立使用限额的合计值。
func sumPolicyDecimal(policies []domain.LiveRiskPolicyState, selector func(domain.LiveRiskPolicyState) domain.Decimal) *big.Rat {
	result := new(big.Rat)
	for _, policy := range policies {
		value, err := decimalRat(defaultDecimal(selector(policy)))
		if err == nil {
			result.Add(result, value)
		}
	}
	return result
}

// countStalePositions 按每个执行账户的真实 signal_age 配置统计过期持仓。
func countStalePositions(positions []domain.LiveLedgerPosition, policies []domain.LiveRiskPolicyState, observedAt time.Time) int64 {
	ages := make(map[string]time.Duration, len(policies))
	for _, policy := range policies {
		ages[policy.ExecutionAccountID] = policy.MaxSignalAge
	}
	var result int64
	for _, position := range positions {
		maxAge := ages[position.Position.ExecutionAccountID]
		if position.LatestSignalAt == nil || maxAge <= 0 {
			result++
			continue
		}
		age := observedAt.Sub(*position.LatestSignalAt)
		if age < 0 || age > maxAge {
			result++
		}
	}
	return result
}

// firstNonEmpty 返回第一个非空文本。
func firstNonEmpty(left string, right string) string {
	if strings.TrimSpace(left) != "" {
		return left
	}
	return right
}

// Package tradingmonitor describes trading readiness without executing work.
package tradingmonitor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/decisioncycle"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
	pm "github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
)

type Binding struct {
	domain.StrategyExecutionBinding
	SubmissionDisabled bool
	EntryDisabled      bool
	StrategyDisabled   bool
	KalshiEnabled      bool
}

func (b Binding) Scope() string {
	return b.ExecutionAccountID + "/" + b.PredictionModelID + "/" + b.StrategyID
}

// EvidenceSource uses bounded read-only queries. It must never call a venue or
// mutate an intent, reservation, ledger, risk policy or reconciliation run.
type EvidenceSource interface {
	Observe(context.Context, time.Time, []Binding) ([]pm.Check, error)
}

type Params struct {
	Enabled             bool
	SubmissionEnabled   bool
	StartedAt           time.Time
	Interval            time.Duration
	MaxStartLateness    time.Duration
	CycleTimeout        time.Duration
	PredictionLookback  time.Duration
	Bindings            []Binding
	SourceModes         map[string]domain.PredictionSourceMode
	Predictions         port.PredictionSource
	RunnerSnapshot      func() decisionrunner.Status
	HeartbeatCheck      func(context.Context, string) error
	ReconciliationCheck func(context.Context, string) error
	Evidence            EvidenceSource
}

type Collector struct{ params Params }

func New(params Params) (*Collector, error) {
	if params.Interval <= 0 || params.MaxStartLateness <= 0 || params.CycleTimeout <= 0 || params.PredictionLookback <= 0 {
		return nil, fmt.Errorf("trading monitoring requires positive business timing limits")
	}
	params.Bindings = append([]Binding(nil), params.Bindings...)
	modes := make(map[string]domain.PredictionSourceMode, len(params.SourceModes))
	for k, v := range params.SourceModes {
		modes[k] = v
	}
	params.SourceModes = modes
	return &Collector{params: params}, nil
}

func (c *Collector) Collect(ctx context.Context, now time.Time) ([]pm.Check, error) {
	p := c.params
	if !p.Enabled {
		return []pm.Check{{ID: "decision.schedule", Scope: "engine", Status: pm.Suppressed, Summary: "决策周期按配置关闭"}}, nil
	}
	checks := c.schedule(now)
	active := make([]Binding, 0, len(p.Bindings))
	for _, b := range p.Bindings {
		if b.SubmissionDisabled || b.StrategyDisabled {
			checks = append(checks, pm.Check{ID: "binding.processing", Scope: b.Scope(), Status: pm.Suppressed, Summary: "路由按配置停用", Details: "不将主动停用的钱包解释为预测或策略故障"})
			continue
		}
		active = append(active, b)
		checks = append(checks, c.accountChecks(ctx, b)...)
	}
	if len(active) == 0 {
		return append(checks, pm.Check{ID: "prediction.coverage", Scope: "engine", Status: pm.Suppressed, Summary: "没有启用的策略路由"}), nil
	}
	needsPredictions := false
	for _, b := range active {
		if !b.EntryDisabled && p.SubmissionEnabled {
			needsPredictions = true
		}
	}
	if !needsPredictions {
		for _, b := range active {
			checks = append(checks, pm.Check{ID: "prediction.coverage", Scope: b.Scope(), Status: pm.Suppressed, Summary: "路由按配置不要求新开仓概率"})
		}
	} else if p.Predictions == nil {
		checks = append(checks, pm.Check{ID: "prediction.observation", Scope: "engine", Status: pm.Unknown, Summary: "概率快照读取器未配置"})
	} else if snapshot, err := p.Predictions.Snapshot(ctx, now, p.PredictionLookback); err != nil {
		checks = append(checks, pm.Check{ID: "prediction.observation", Scope: "engine", Status: pm.Unknown, Summary: "无法读取可交易概率", Details: "读取失败不是零预测；原始错误不进入通知以避免泄露凭证或上游响应"})
	} else {
		checks = append(checks, c.predictionChecks(snapshot, active, now)...)
	}
	if p.Evidence == nil {
		checks = append(checks, pm.Check{ID: "trading.database", Scope: "engine", Status: pm.Unknown, Summary: "交易业务观测未配置"})
	} else {
		observed, err := p.Evidence.Observe(ctx, now, active)
		checks = append(checks, observed...)
		if err != nil {
			checks = append(checks, pm.Check{ID: "trading.database", Scope: "engine", Status: pm.Unknown, Summary: "交易业务记录读取不完整", Details: "未完成部分不能视作健康或无任务"})
		}
	}
	return checks, nil
}

func (c *Collector) schedule(now time.Time) []pm.Check {
	p := c.params
	check := pm.Check{ID: "decision.schedule", Scope: "engine", Status: pm.Healthy, Summary: "决策调度循环正常"}
	if p.RunnerSnapshot == nil {
		check.Status = pm.Unknown
		check.Summary = "决策 Runner 状态不可读"
		return []pm.Check{check}
	}
	s := p.RunnerSnapshot()
	check.Evidence = map[string]string{"last_started_at": stamp(s.LastStartedAt), "last_completed_at": stamp(s.LastCompletedAt), "last_succeeded_at": stamp(s.LastSucceededAt), "next_due_at": stamp(s.NextDueAt), "skipped_boundaries": strconv.FormatUint(s.SkippedBoundaries, 10)}
	switch {
	case !s.LoopStarted || !s.LoopRunning:
		check.Status = pm.Critical
		check.Summary = "决策调度循环未运行"
	case s.InFlight && now.After(s.LastStartedAt.Add(p.CycleTimeout)):
		check.Status = pm.Critical
		check.Summary = "决策周期超过运行预算"
	case !s.InFlight && !s.NextDueAt.IsZero() && now.After(s.NextDueAt.Add(p.MaxStartLateness)):
		check.Status = pm.Critical
		check.Summary = "决策周期错过启动窗口"
	case s.LastError != "":
		check.Status = pm.Warning
		check.Summary = "最近决策周期存在错误"
		check.Details = "查看该轮策略、行情、覆盖门禁和投递的具体观测；不将所有账户一概判为不可交易"
	case s.LastCompletedAt.IsZero():
		check.Status = pm.Unknown
		check.Summary = "等待当前进程首次决策周期完成"
	}
	return []pm.Check{check}
}

func (c *Collector) accountChecks(ctx context.Context, b Binding) []pm.Check {
	checks := []pm.Check{}
	for _, p := range []struct {
		id, summary string
		check       func(context.Context, string) error
	}{
		{"account.reconciliation", "账户对账就绪", c.params.ReconciliationCheck},
		{"account.heartbeat", "交易所心跳就绪", c.params.HeartbeatCheck},
	} {
		check := pm.Check{ID: p.id, Scope: "POLYMARKET/" + b.Scope(), Status: pm.Healthy, Summary: p.summary}
		if p.check == nil {
			check.Status = pm.Unknown
			check.Summary = "账户运行时观测不可读"
		} else if err := p.check(ctx, b.ExecutionAccountID); err != nil {
			check.Status = pm.Critical
			check.Summary = p.summary + "检查未通过"
			check.Details = "仅表示本账户对应的 placement 就绪门禁；不限制 Cancel/Get，也不宣称其他钱包受到影响"
		}
		checks = append(checks, check)
	}
	if b.EntryDisabled || !c.params.SubmissionEnabled {
		checks = append(checks, pm.Check{ID: "binding.entry", Scope: b.Scope(), Status: pm.Suppressed, Summary: "新开仓按配置关闭", Details: "不据此认定合法 SELL 或持仓管理已停止"})
	}
	return checks
}

func (c *Collector) predictionChecks(snapshot domain.PredictionSnapshot, bindings []Binding, now time.Time) []pm.Check {
	models := []string{}
	seen := map[string]bool{}
	for _, b := range bindings {
		if b.EntryDisabled || !c.params.SubmissionEnabled {
			continue
		}
		if !seen[b.PredictionModelID] {
			models = append(models, b.PredictionModelID)
			seen[b.PredictionModelID] = true
		}
	}
	expectations, expectationErr := domain.SelectEffectivePredictionExpectations(snapshot.ExpectedPredictions, models)
	if expectationErr != nil {
		return []pm.Check{{ID: "prediction.observation", Scope: "engine", Status: pm.Unknown, Summary: "任务期望身份无法确定"}}
	}
	effective, err := decisioncycle.EffectivePredictionsForMonitoring(snapshot.Predictions, models, c.params.SourceModes, now.Add(-c.params.PredictionLookback))
	if err != nil {
		return []pm.Check{{ID: "prediction.observation", Scope: "engine", Status: pm.Unknown, Summary: "概率来源或版本身份无法确定", Details: "复用生产实际筛选规则，歧义结果不作为有效概率"}}
	}
	checks := []pm.Check{}
	for _, b := range bindings {
		count := 0
		wrongSource := 0
		expiresAt := time.Time{}
		byVenue := map[domain.MarketSource]int{}
		for _, raw := range snapshot.Predictions {
			if raw.Model.Name != b.PredictionModelID {
				continue
			}
			mode := c.params.SourceModes[b.PredictionModelID]
			if (mode == domain.PredictionSourceModeSandbox && strings.TrimSpace(raw.SandboxID) == "") || (mode == domain.PredictionSourceModeDirect && strings.TrimSpace(raw.SandboxID) != "") {
				wrongSource++
			}
		}
		for _, p := range effective {
			if p.Model.Name != b.PredictionModelID {
				continue
			}
			source := p.MarketSource.Normalize()
			if source == "" {
				source = domain.MarketSourcePolymarket
			}
			if source == domain.MarketSourceKalshi && !b.KalshiEnabled {
				continue
			}
			if p.EndAt != nil && !p.EndAt.After(now) {
				continue
			}
			count++
			byVenue[source]++
			expires := p.PredictionAsOf.Add(c.params.PredictionLookback)
			if t := p.CompletedAt.Add(c.params.PredictionLookback); t.Before(expires) {
				expires = t
			}
			if p.EndAt != nil && p.EndAt.Before(expires) {
				expires = *p.EndAt
			}
			if expires.After(expiresAt) {
				expiresAt = expires
			}
		}
		check := pm.Check{ID: "prediction.coverage", Scope: b.Scope(), Status: pm.Healthy, Summary: "路由具有有效开仓概率", Evidence: map[string]string{"source_model": b.PredictionModelID, "source_mode": string(c.params.SourceModes[b.PredictionModelID]), "effective_markets": strconv.Itoa(count), "polymarket_markets": strconv.Itoa(byVenue[domain.MarketSourcePolymarket]), "kalshi_markets": strconv.Itoa(byVenue[domain.MarketSourceKalshi]), "wrong_source_rows": strconv.Itoa(wrongSource), "last_input_expires_at": stamp(expiresAt), "snapshot_id": snapshot.SnapshotID}}
		switch {
		case b.EntryDisabled || !c.params.SubmissionEnabled:
			check.Status = pm.Suppressed
			check.Summary = "路由按配置不要求新开仓概率"
		case count == 0:
			check.Status = pm.Critical
			check.Summary = "活动路由缺少可交易概率"
			check.Details = "按精确源模型、DIRECT/SANDBOX、PIT 和有效期过滤后为空；只说明开仓输入不足，不说明 SELL/撤单失效"
		case expiresAt.Sub(now) <= min(30*time.Minute, c.params.PredictionLookback/4):
			check.Status = pm.Warning
			check.Summary = "当前有效概率即将全部过期"
			check.Details = "若下一批没有及时回传，该路由将缺少开仓输入"
		}
		checks = append(checks, check)
		for _, venue := range []domain.MarketSource{domain.MarketSourcePolymarket, domain.MarketSourceKalshi} {
			pending := 0
			oldestPending := time.Time{}
			for _, e := range expectations {
				if c.params.SourceModes[b.PredictionModelID] == domain.PredictionSourceModeDirect && e.PredictionModelID == b.PredictionModelID && normalizedVenue(e.MarketSource) == venue && e.Status == domain.PredictionExpectationPending {
					pending++
					if oldestPending.IsZero() || e.TaskAvailableAt.Before(oldestPending) {
						oldestPending = e.TaskAvailableAt
					}
				}
			}
			status := pm.Healthy
			summary := "分来源概率观测"
			if b.EntryDisabled || !c.params.SubmissionEnabled {
				status = pm.Suppressed
				summary = "该路由按配置不要求新开仓输入"
			} else if venue == domain.MarketSourceKalshi && !b.KalshiEnabled {
				status = pm.Suppressed
				summary = "该路由未启用 Kalshi 输入"
			} else if pending > 0 {
				budget := min(30*time.Minute, c.params.PredictionLookback/4)
				status = pm.Suppressed
				summary = "已发布的 Direct 任务仍在处理中"
				if oldestPending.IsZero() || oldestPending.After(now) {
					status = pm.Unknown
					summary = "Direct 等待任务缺少可信发布时间"
				} else if now.Sub(oldestPending) > budget {
					status = pm.Warning
					summary = "Direct 预测任务等待超过观测预算"
					if byVenue[venue] == 0 {
						status = pm.Critical
						summary = "该来源有超时预测任务且没有有效结果"
					}
				}
			} else if byVenue[venue] == 0 && pending == 0 {
				status = pm.Suppressed
				summary = "缺少该来源的应预测集合，不单独判定缺盘"
			}
			checks = append(checks, pm.Check{ID: "prediction.venue", Scope: string(venue) + "/" + b.Scope(), Status: status, Summary: summary, Details: "Sandbox 的应预测集合由中台 dispatch 观测，不能从已有结果并集反推；Direct 等待预算仅用于提醒，不改变业务资格", Evidence: map[string]string{"effective_markets": strconv.Itoa(byVenue[venue]), "pending_direct_tasks": strconv.Itoa(pending), "oldest_pending_at": stamp(oldestPending)}})
		}
	}
	return checks
}

func normalizedVenue(v domain.MarketSource) domain.MarketSource {
	if n := v.Normalize(); n != "" {
		return n
	}
	return domain.MarketSourcePolymarket
}
func stamp(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	return t.UTC().Format(time.RFC3339)
}

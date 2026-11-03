package tradingmonitor

import (
	"encoding/json"
	"html"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	pm "github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
)

// Exercise the renderer with the actual trading collector's Check shape,
// including the source-model alias and multi-wallet scope dimensions. Rendering
// must never rewrite the diagnostic records or their independent state keys.
func TestCollectedDirectModelChecksGroupOnlyForPresentation(t *testing.T) {
	checks, alerts := actualDirectCoverageAlerts(t)
	before, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 3 {
		t.Fatalf("expected independent wallet checks, got %d", len(alerts))
	}
	card, err := pm.RenderCard("trading-execution", testNow, alerts)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	var rendered struct {
		Schema string `json:"schema"`
		Body   struct {
			Elements []struct {
				Tag string `json:"tag"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &rendered); err != nil {
		t.Fatal(err)
	}
	if rendered.Schema != "2.0" {
		t.Fatalf("unexpected schema %q", rendered.Schema)
	}
	groups := 0
	for _, element := range rendered.Body.Elements {
		if element.Tag == "column_set" {
			groups++
		}
	}
	if groups != 1 {
		t.Fatalf("same model/inputs should have one visible group, got %d", groups)
	}
	for _, alert := range alerts {
		if !strings.Contains(cardText(card, false), alert.Check.Scope) {
			t.Fatalf("independent diagnostic scope lost: %s", alert.Check.Scope)
		}
	}
	front := cardForeground(card)
	if !strings.Contains(front, "无法新开仓") || !strings.Contains(front, "主钱包") || !strings.Contains(front, "钱包 6") || !strings.Contains(front, "钱包 7") || !strings.Contains(front, "平仓") || !strings.Contains(front, "撤单") {
		t.Fatalf("missing definite three-account BUY impact and exit boundaries: %s", front)
	}
	if strings.Contains(strings.ReplaceAll(front, " ", ""), "可用概率为0") {
		t.Fatalf("zero market coverage was confused with a 0%% forecast probability: %s", front)
	}
	after, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("card grouping changed the independently observed checks")
	}
}

func TestLegitimateZeroOutcomeProbabilityIsStillAvailablePrediction(t *testing.T) {
	p := fixtureParams()
	prediction := validPrediction("market-zero", domain.MarketSourcePolymarket, testNow.Add(-time.Hour))
	prediction.Outcomes[0].Probability = 0
	prediction.Outcomes[1].Probability = 1
	snapshot := domain.PredictionSnapshot{SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "zero-outcome-snapshot", DecisionAt: testNow, CompletedAfter: testNow.Add(-p.PredictionLookback), GeneratedAt: testNow, Predictions: []domain.Prediction{prediction}}
	if err := snapshot.Validate(testNow); err != nil {
		t.Fatal(err)
	}
	p.Predictions = predictionFixture{snapshot: snapshot}
	check := find(t, collect(t, p), "prediction.coverage", p.Bindings[0].Scope())
	if check.Status != pm.Healthy || check.Evidence["effective_markets"] != "1" {
		t.Fatalf("valid p=0 was treated as missing market coverage: %+v", check)
	}
}

// The deployed historical wire form can lack source_model. Build the actual
// Checks through Collect, then remove only that optional field to model the old
// wire version; never manufacture a model field to make grouping succeed.
func TestLegacyCollectedCoverageUsesScopeWithoutConflatingModels(t *testing.T) {
	_, actual := actualDirectCoverageAlerts(t)
	legacy := func() []pm.Alert {
		out := make([]pm.Alert, len(actual))
		for i, a := range actual {
			out[i] = a
			out[i].Check.Evidence = maps.Clone(a.Check.Evidence)
			delete(out[i].Check.Evidence, "source_model")
		}
		return out
	}
	for _, tc := range []struct {
		name       string
		change     func([]pm.Alert)
		wantGroups int
	}{
		{name: "same-model-three-wallets", wantGroups: 1},
		{name: "different-scope-model", wantGroups: 2, change: func(a []pm.Alert) { a[1].Check.Scope = "wallet-6/other-model/multfactor_v1" }},
		{name: "different-source-mode", wantGroups: 2, change: func(a []pm.Alert) { a[1].Check.Evidence["source_mode"] = "SANDBOX" }},
		{name: "different-reason", wantGroups: 2, change: func(a []pm.Alert) { a[1].Check.Details = "另一条不同的诊断原因" }},
		{name: "different-level", wantGroups: 2, change: func(a []pm.Alert) { a[1].Check.Status = pm.Warning }},
		{name: "unproven-model-scope", wantGroups: 2, change: func(a []pm.Alert) { a[1].Check.Scope = "wallet-6" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alerts := legacy()
			if tc.change != nil {
				tc.change(alerts)
			}
			card, err := pm.RenderCard("trading-execution", testNow, alerts)
			if err != nil {
				t.Fatal(err)
			}
			if got := visibleCardGroups(t, card); got != tc.wantGroups {
				t.Fatalf("visible groups=%d want=%d", got, tc.wantGroups)
			}
			for _, a := range alerts {
				if !strings.Contains(cardText(card, false), a.Check.Scope) {
					t.Fatalf("legacy scope lost: %s", a.Check.Scope)
				}
			}
		})
	}
}

func actualDirectCoverageAlerts(t *testing.T) ([]pm.Check, []pm.Alert) {
	t.Helper()
	p := fixtureParams()
	snapshot := domain.PredictionSnapshot{SchemaVersion: domain.PredictionSnapshotSchemaVersion, SnapshotID: "empty-input-snapshot", DecisionAt: testNow, CompletedAfter: testNow.Add(-p.PredictionLookback), GeneratedAt: testNow, Predictions: []domain.Prediction{}, ExpectedPredictions: []domain.PredictionExpectation{}}
	if err := snapshot.Validate(testNow); err != nil {
		t.Fatal(err)
	}
	p.Predictions = predictionFixture{snapshot: snapshot}
	p.SourceModes = map[string]domain.PredictionSourceMode{"v4.1 flash": domain.PredictionSourceModeDirect}
	p.Bindings = nil
	for _, r := range []struct{ account, strategy string }{{"main", "brier_mid_edge_once"}, {"wallet-6", "multfactor_v1"}, {"wallet-7", "multfactor_v2"}} {
		p.Bindings = append(p.Bindings, Binding{StrategyExecutionBinding: domain.StrategyExecutionBinding{PredictionModelID: "v4.1 flash", ModelID: "deepseek_masked", ExecutionAccountID: r.account, StrategyID: r.strategy}, KalshiEnabled: true})
	}
	checks := collect(t, p)
	alerts := []pm.Alert{}
	for _, c := range checks {
		if c.ID == "prediction.coverage" {
			alerts = append(alerts, pm.Alert{Check: c})
		}
	}
	return checks, alerts
}

func visibleCardGroups(t *testing.T, card map[string]any) int {
	t.Helper()
	data, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Body struct {
			Elements []struct {
				Tag string `json:"tag"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	groups := 0
	for _, e := range body.Body.Elements {
		if e.Tag == "column_set" {
			groups++
		}
	}
	return groups
}

func TestLogicalAndNativeAuthorizationRemainSeparateInCards(t *testing.T) {
	alerts := []pm.Alert{
		{Check: pm.Check{ID: "account.authorization", Scope: "main", Status: pm.Critical, Summary: "账户缺少必需授权或控制记录", Evidence: map[string]string{"model_id": "deepseek_masked", "strategy_id": "strategy-a"}}},
		{Check: pm.Check{ID: "account.authorization", Scope: "kalshi:main", Status: pm.Critical, Summary: "账户缺少必需授权或控制记录", Evidence: map[string]string{"model_id": "deepseek_masked", "strategy_id": "strategy-a"}}},
	}
	card, err := pm.RenderCard("trading-execution", testNow, alerts)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(card)
	var body struct {
		Body struct {
			Elements []struct {
				Tag string `json:"tag"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	groups := 0
	for _, e := range body.Body.Elements {
		if e.Tag == "column_set" {
			groups++
		}
	}
	if groups != 2 || !strings.Contains(cardText(card, false), "kalshi:main") {
		t.Fatalf("native and logical authorization were conflated: %s", data)
	}
	front := cardForeground(card)
	if !strings.Contains(front, "订单") || !strings.Contains(front, "撤单") {
		t.Fatalf("placement and cancellation boundaries are not explained: %s", front)
	}
}

func TestCardRecoveryDoesNotChangeObservedModelState(t *testing.T) {
	p := fixtureParams()
	p.SourceModes["source-model"] = domain.PredictionSourceModeSandbox
	checks := collect(t, p)
	check := find(t, checks, "prediction.coverage", p.Bindings[0].Scope())
	before, _ := json.Marshal(check)
	card, err := pm.RenderCard("trading-execution", testNow, []pm.Alert{{Check: check, Recovery: true}})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(card)
	var parsed struct {
		Header struct {
			Template string `json:"template"`
		} `json:"header"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Header.Template != "green" {
		t.Fatalf("recovery is not independently green: %s", data)
	}
	after, _ := json.Marshal(check)
	if string(before) != string(after) || check.Status != pm.Healthy {
		t.Fatal("rendering changed current model evidence")
	}
}

// These evidence keys are emitted by the existing trading Collector and PG
// reader. Presentation must use those facts rather than inventing a new source
// or changing any detector/diagnostic schema to make a card look informative.
func TestTradingAlertFamiliesShowConcreteFactsAndBoundedImpact(t *testing.T) {
	cases := []struct {
		name     string
		check    pm.Check
		recovery bool
		want     []string
		anyOf    [][]string
		avoid    []string
	}{
		{name: "cash", check: pm.Check{ID: "account.cash", Scope: "wallet-6", Status: pm.Warning, Summary: "活动账户没有可用新开仓资金", Evidence: map[string]string{"available_cash": "0", "reserved_cash": "12.5"}}, want: []string{"0", "12.5", "资金"}, anyOf: [][]string{{"买入", "开仓"}}, avoid: []string{"无法卖出", "全部交易停止"}},
		{name: "risk-state", check: pm.Check{ID: "account.risk_state", Scope: "wallet-6", Status: pm.Critical, Summary: "账户对账结果超出风控有效期", Evidence: map[string]string{"age": "12m", "max_state_age": "10m", "last_completed_at": "2026-10-30T00:18:00Z"}}, want: []string{"对账", "10"}, anyOf: [][]string{{"12", "2"}}, avoid: []string{"撤单不可用"}},
		{name: "account-reconciliation", check: pm.Check{ID: "account.reconciliation", Scope: "POLYMARKET/wallet-6/v4.1 flash/multfactor_v1", Status: pm.Critical, Summary: "账户对账就绪检查未通过"}, want: []string{"对账", "订单", "撤单"}, avoid: []string{"所有账户"}},
		{name: "heartbeat", check: pm.Check{ID: "account.heartbeat", Scope: "POLYMARKET/wallet-6/v4.1 flash/multfactor_v1", Status: pm.Critical, Summary: "交易所心跳就绪检查未通过"}, want: []string{"心跳", "订单", "撤单"}, avoid: []string{"全部交易停止"}},
		{name: "account-issues", check: pm.Check{ID: "account.reconciliation_issues", Scope: "wallet-6", Status: pm.Critical, Summary: "账户存在未解决的全账户对账门禁", Evidence: map[string]string{"sampled_account_issues": "7", "sample_issue_id": "private-long-issue-id"}}, want: []string{"7", "对账", "采样"}},
		{name: "scoped-issues", check: pm.Check{ID: "market.reconciliation_issues", Scope: "wallet-6", Status: pm.Warning, Summary: "部分市场存在未解决对账门禁", Evidence: map[string]string{"sampled_scoped_issues": "11", "sample_issue_type": "POSITION_DRIFT", "sample_token_id": "private-long-token-id"}}, want: []string{"11", "采样"}, anyOf: [][]string{{"持仓", "仓位"}}, avoid: []string{"所有市场都"}},
		{name: "decision", check: pm.Check{ID: "decision.schedule", Scope: "engine", Status: pm.Critical, Summary: "决策周期超过运行预算", Evidence: map[string]string{"last_started_at": "2026-10-30T00:21:00Z", "last_completed_at": "2026-10-30T00:10:00Z", "skipped_boundaries": "2"}}, want: []string{"决策", "2"}, anyOf: [][]string{{"9分", "9 分", "9m", "08:21"}, {"累计", "本进程"}}, avoid: []string{"整个交易已停止"}},
		{name: "strategy-books", check: pm.Check{ID: "strategy.progress", Scope: "wallet-6/v4.1 flash/multfactor_v1", Status: pm.Warning, Summary: "策略输入含无法读取的盘口", Evidence: map[string]string{"decision_at": "2026-10-30T00:20:00Z", "missing_or_error_books": "13", "cycle_id": "private-long-cycle-id"}}, want: []string{"13", "盘口", "08:20"}},
		{name: "intent", check: pm.Check{ID: "intent.delivery", Scope: "wallet-6", Status: pm.Critical, Summary: "订单意图投递存在超时未解决项", Evidence: map[string]string{"sampled_rows": "9", "stalled_sample_rows": "7", "stall_limit": "8m", "sample_state": "UNKNOWN", "oldest_observed_at": "2026-10-30T00:05:00Z", "sample_id": "private-long-order-id", "sampling_complete": "true"}}, want: []string{"7", "采样", "订单"}, anyOf: [][]string{{"25分", "25 分", "25m", "08:05"}}, avoid: []string{"已确认未成交", "已自动重投"}},
		{name: "order", check: pm.Check{ID: "order.recovery", Scope: "wallet-6", Status: pm.Critical, Summary: "未知订单与撤单恢复存在超时未解决项", Evidence: map[string]string{"sampled_rows": "9", "stalled_sample_rows": "5", "stall_limit": "8m", "sample_state": "MANUAL_REVIEW", "oldest_observed_at": "2026-10-30T00:00:00Z", "sampling_complete": "true"}}, want: []string{"5", "采样", "订单"}, avoid: []string{"已确认未成交", "已自动重投"}},
		{name: "fill", check: pm.Check{ID: "fill.application", Scope: "wallet-6", Status: pm.Critical, Summary: "已确认成交入账存在超时未解决项", Evidence: map[string]string{"sampled_rows": "4", "stalled_sample_rows": "4", "stall_limit": "8m", "oldest_observed_at": "2026-10-30T00:00:00Z", "sampling_complete": "true"}}, want: []string{"4", "成交", "入账", "采样"}},
		{name: "venue", check: pm.Check{ID: "venue.execution_errors", Scope: "wallet-6", Status: pm.Critical, Summary: "近期执行观测到交易所基础异常", Evidence: map[string]string{"sampled_orders": "7", "sample_failure_code": "CLOB_AUTH_FAILED", "window": "15m", "last_failure_at": "2026-10-30T00:29:00Z"}}, want: []string{"7", "15", "认证"}},
		{name: "unknown", check: pm.Check{ID: "trading.database", Scope: "engine", Status: pm.Unknown, Summary: "交易业务记录读取不完整"}, anyOf: [][]string{{"观测", "确认", "不能据此认定"}}, avoid: []string{"无法新开仓", "整个交易已停止"}},
		{name: "venue-recovery-under-threshold", recovery: true, check: pm.Check{ID: "venue.execution_errors", Scope: "wallet-6", Status: pm.Healthy, Summary: "近期执行未发现具名交易所基础故障", Evidence: map[string]string{"sampled_orders": "2", "sample_failure_code": "CLOB_SERVER_ERROR", "window": "15m", "last_failure_at": "2026-10-30T00:29:00Z"}}, want: []string{"2", "采样"}, anyOf: [][]string{{"阈值", "条件"}}, avoid: []string{"错误归零", "所有异常已消失"}},
		{name: "order-recovery-sampling-only", recovery: true, check: pm.Check{ID: "order.recovery", Scope: "wallet-6", Status: pm.Healthy, Summary: "未知订单与撤单恢复观测正常", Evidence: map[string]string{"sampled_rows": "9", "stalled_sample_rows": "0", "sampling_complete": "true"}}, want: []string{"采样", "订单"}, avoid: []string{"所有未知订单已解决", "所有历史订单已解决", "全部订单已成交"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			card, err := pm.RenderCard("trading-execution", testNow, []pm.Alert{{Check: tc.check, Recovery: tc.recovery}})
			if err != nil {
				t.Fatal(err)
			}
			body := cardForeground(card["body"])
			for _, token := range tc.want {
				if !strings.Contains(body, token) {
					t.Fatalf("missing operational fact %q: %s", token, body)
				}
			}
			for _, choices := range tc.anyOf {
				found := false
				for _, token := range choices {
					if strings.Contains(body, token) {
						found = true
					}
				}
				if !found {
					t.Fatalf("missing one of %q: %s", choices, body)
				}
			}
			for _, token := range append(tc.avoid, "private-long-", "stalled_sample_rows", "sampled_scoped_issues", "sample_failure_code", "missing_or_error_books") {
				if strings.Contains(body, token) {
					t.Fatalf("technical noise or unsupported impact %q on front: %s", token, body)
				}
			}
		})
	}
}

// Skip collapsed diagnostics to ensure the assertions concern the information
// visible first, rather than finding a reassuring phrase in technical details.
func cardForeground(value any) string {
	return cardText(value, true)
}

func cardText(value any, foregroundOnly bool) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "[unencodable card]"
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return "[invalid card]"
	}
	var parts []string
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if foregroundOnly && x["tag"] == "collapsible_panel" {
				return
			}
			if s, ok := x["content"].(string); ok {
				parts = append(parts, html.UnescapeString(s))
			}
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(normalized)
	return strings.Join(parts, "\n")
}

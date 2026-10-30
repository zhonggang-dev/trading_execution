package pipelinemonitor

import (
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var cardTestAt = time.Date(2026, 10, 30, 6, 30, 0, 0, time.UTC)

func coverageAlert(account, model string) Alert {
	return Alert{Check: Check{ID: "prediction.coverage", Scope: account + "/" + model + "/multfactor_v1", Status: Critical, Summary: "活动路由缺少可交易概率", Details: "按精确源模型、DIRECT/SANDBOX、PIT 和有效期过滤后为空；只说明开仓输入不足，不说明 SELL/撤单失效", Evidence: map[string]string{"source_mode": "SANDBOX", "effective_markets": "0", "polymarket_markets": "0", "kalshi_markets": "0", "wrong_source_rows": "0", "snapshot_id": "predsnap-long-debug-id"}}}
}
func cardElements(t *testing.T, card map[string]any) []any {
	t.Helper()
	return card["body"].(map[string]any)["elements"].([]any)
}
func markdownIn(value any) []string {
	var out []string
	switch v := value.(type) {
	case map[string]any:
		if v["tag"] == "markdown" {
			out = append(out, v["content"].(string))
		}
		for _, x := range v {
			out = append(out, markdownIn(x)...)
		}
	case []any:
		for _, x := range v {
			out = append(out, markdownIn(x)...)
		}
	}
	return out
}
func frontText(t *testing.T, card map[string]any) string {
	t.Helper()
	var result []string
	for _, e := range cardElements(t, card) {
		m := e.(map[string]any)
		if m["tag"] != "collapsible_panel" {
			result = append(result, markdownIn(m)...)
		}
	}
	return html.UnescapeString(strings.Join(result, "\n"))
}
func renderedText(card map[string]any) string {
	return html.UnescapeString(strings.Join(markdownIn(card), "\n"))
}

func TestCardGroupsRealCoverageScopesAndHighlightsAffectedAccounts(t *testing.T) {
	alerts := []Alert{coverageAlert("main", "v4.1 flash"), coverageAlert("wallet-6", "v4.1 flash"), coverageAlert("wallet-7", "v4.1 flash")}
	before, _ := json.Marshal(alerts)
	card, err := RenderCard("trading-execution", cardTestAt, alerts)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(alerts)
	if string(before) != string(after) {
		t.Fatal("presentation mutated detector observations")
	}
	if card["schema"] != "2.0" || card["config"].(map[string]any)["width_mode"] != "default" || card["header"].(map[string]any)["template"] != "red" {
		t.Fatal("incorrect card envelope")
	}
	if got := len(cardElements(t, card)); got != 2 {
		t.Fatalf("same-source multi-wallet must be one issue plus diagnostics, got %d", got)
	}
	front := frontText(t, card)
	for _, s := range []string{"没有可用预测结果", "**影响：主钱包、钱包 6、钱包 7当前无法新开仓；平仓、撤单按原规则处理。**", "v4.1 flash", "可用预测市场 0"} {
		if !strings.Contains(front, s) {
			t.Fatalf("missing useful foreground %q: %s", s, front)
		}
	}
	for _, noise := range []string{"wrong_source_rows", "snapshot_id", "predsnap-long-debug-id", "按精确源模型"} {
		if strings.Contains(front, noise) {
			t.Fatalf("technical detail leaked into foreground: %s", noise)
		}
	}
	text := renderedText(card)
	for _, a := range alerts {
		if !strings.Contains(text, a.Check.Scope) {
			t.Fatal("merged independent Check identity lost", a.Check.Scope)
		}
	}
	if !strings.Contains(text, "predsnap-long-debug-id") {
		t.Fatal("diagnostics lost long ID")
	}
}

func TestCardNeverMergesDifferentModelsReasonsSourcesOrLifecycle(t *testing.T) {
	base := coverageAlert("main", "model-a")
	for _, tc := range []struct {
		name  string
		alter func(*Alert)
	}{
		{"model", func(a *Alert) { a.Check.Scope = "wallet-6/model-b/multfactor_v1" }},
		{"reason", func(a *Alert) { a.Check.Details = "different filtering reason" }},
		{"venue coverage", func(a *Alert) { a.Check.Evidence["kalshi_markets"] = "1" }},
		{"source mode", func(a *Alert) { a.Check.Evidence["source_mode"] = "DIRECT" }},
		{"explicit model mismatch", func(a *Alert) { a.Check.Evidence["source_model"] = "wrong-model" }},
		{"severity", func(a *Alert) { a.Check.Status = Warning }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other := coverageAlert("wallet-6", "model-a")
			tc.alter(&other)
			card, err := RenderCard("trading-execution", cardTestAt, []Alert{base, other})
			if err != nil {
				t.Fatal(err)
			}
			if len(cardElements(t, card)) != 3 {
				t.Fatal("different dimensions incorrectly grouped")
			}
		})
	}
	recovered := base
	recovered.Recovery = true
	recovered.Check.Status = Healthy
	if _, err := RenderCard("trading-execution", cardTestAt, []Alert{base, recovered}); err == nil {
		t.Fatal("active faults and recoveries mixed into one card")
	}
}

func TestCardOptionalExplicitModelNormalizesToSameScopeIdentity(t *testing.T) {
	a, b := coverageAlert("main", "openai/model"), coverageAlert("wallet-6", "openai/model")
	a.Check.Evidence["source_model"] = "openai/model"
	card, err := RenderCard("trading-execution", cardTestAt, []Alert{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(cardElements(t, card)) != 2 {
		t.Fatal("equivalent legacy model evidence prevented valid grouping")
	}
}

func TestCardLifecycleColorsUnknownAndScopeImpact(t *testing.T) {
	for _, tc := range []struct {
		status   string
		recovery bool
		color    string
	}{{Critical, false, "red"}, {Warning, false, "orange"}, {Unknown, false, "orange"}, {Healthy, true, "green"}} {
		card, err := RenderCard("prediction-infra", cardTestAt, []Alert{{Check: Check{ID: "sandbox.original", Scope: "KALSHI/model", Status: tc.status, Summary: "构建状态"}, Recovery: tc.recovery}})
		if err != nil {
			t.Fatal(err)
		}
		if card["header"].(map[string]any)["template"] != tc.color {
			t.Fatal("incorrect severity color", tc)
		}
		front := frontText(t, card)
		if !strings.Contains(front, "**影响：") {
			t.Fatal("impact lacks emphasis")
		}
		if tc.status == Unknown && !strings.Contains(front, "暂不能确认") && !strings.Contains(front, "无法确认") {
			t.Fatal("unknown falsely represented as confirmed trading failure")
		}
		if tc.recovery && (!strings.Contains(front, "本轮原始包阶段") || strings.Contains(front, "订单已成交")) {
			t.Fatal("recovery promised execution")
		}
	}
	card, _ := RenderCard("trading-execution", cardTestAt, []Alert{{Check: Check{ID: "account.authorization", Scope: "kalshi:main", Status: Critical, Summary: "授权未通过"}}})
	front := frontText(t, card)
	if !strings.Contains(front, "Kalshi 主账户的新订单") || !strings.Contains(front, "撤单和查询按原规则处理") {
		t.Fatal("account placement/cancel boundary lost", front)
	}
}

func TestCardForegroundLimitPreservesEveryExtraCheck(t *testing.T) {
	var alerts []Alert
	for i := range 7 {
		alerts = append(alerts, Alert{Check: Check{ID: fmt.Sprintf("unique.rule.%d", i), Scope: fmt.Sprintf("scope-%d", i), Status: Warning, Summary: fmt.Sprintf("独立问题%d", i), Evidence: map[string]string{"sample_id": fmt.Sprintf("long-identity-%d", i)}}})
	}
	card, err := RenderCard("prediction-infra", cardTestAt, alerts)
	if err != nil {
		t.Fatal(err)
	}
	elements := cardElements(t, card)
	if len(elements) != 5 || elements[4].(map[string]any)["tag"] != "collapsible_panel" {
		t.Fatal("front exceeds four issues plus fold")
	}
	if elements[4].(map[string]any)["expanded"] != false {
		t.Fatal("technical diagnostics expanded by default")
	}
	text := renderedText(card)
	for _, a := range alerts {
		if !strings.Contains(text, a.Check.ID) || !strings.Contains(text, a.Check.Evidence["sample_id"]) {
			t.Fatal("extra selected check silently lost", a.Check.ID)
		}
	}
	strongest := 0
	for _, element := range elements[:4] {
		for _, m := range element.(map[string]any)["columns"].([]any) {
			for _, part := range m.(map[string]any)["elements"].([]any) {
				if part.(map[string]any)["text_size"] == "heading-4" {
					strongest++
				}
			}
		}
	}
	if strongest != 1 {
		t.Fatalf("strongest visual focus count=%d", strongest)
	}
}

func TestCardDynamicContentCannotInjectMentionsLinksOrMarkup(t *testing.T) {
	attack := "<at id=all></at> **bold** [click](https://evil.invalid) <font color='green'>ok</font> @all"
	card, err := RenderCard("prediction-infra", cardTestAt, []Alert{{Check: Check{ID: "test", Scope: attack, Status: Warning, Summary: attack, Details: attack, Evidence: map[string]string{"field": attack}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, content := range markdownIn(card) {
		for _, raw := range []string{"<at ", "[click](", "<font color='green'>ok", "@all"} {
			if strings.Contains(content, raw) {
				t.Fatalf("dynamic markdown injection remained: %s", content)
			}
		}
	}
	if !strings.Contains(renderedText(card), attack) {
		t.Fatal("escaping lost diagnostic text")
	}
}

func countTags(value any) int {
	count := 0
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v["tag"]; ok {
			count++
		}
		for _, x := range v {
			count += countTags(x)
		}
	case []any:
		for _, x := range v {
			count += countTags(x)
		}
	}
	return count
}
func TestCardTwelveWorstFieldsStayInsideSafePayloadAndKeepIdentities(t *testing.T) {
	for _, worst := range []string{strings.Repeat("最长诊断文字", 100), strings.Repeat("<>&*_[@]", 100)} {
		var alerts []Alert
		for i := range 12 {
			evidence := map[string]string{}
			for j := range 8 {
				evidence[fmt.Sprintf("technical_field_%d", j)] = worst
			}
			alerts = append(alerts, Alert{Check: Check{ID: fmt.Sprintf("worst.%d", i), Scope: fmt.Sprintf("scope-%d", i), Status: Critical, Summary: "异常" + worst, Details: worst, Evidence: evidence}})
		}
		card, err := RenderCard("prediction-infra", cardTestAt, alerts)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(card)
		if len(data) >= 28*1024 || countTags(card) >= 200 {
			t.Fatalf("card exceeded bounds: bytes=%d tags=%d", len(data), countTags(card))
		}
		text := renderedText(card)
		for _, a := range alerts {
			if !strings.Contains(text, a.Check.ID) {
				t.Fatal("bounded diagnostics dropped a selected check identity", a.Check.ID)
			}
		}
		if !strings.Contains(text, "明细已限长") {
			t.Fatal("bounded details not labelled")
		}
	}
}

func TestCardCoverageGroupTakesPrimaryFocusOverSingleAccountAuth(t *testing.T) {
	auth := Alert{Check: Check{ID: "account.authorization", Scope: "kalshi:main", Status: Critical, Summary: "原生账户授权缺失"}}
	card, err := RenderCard("trading-execution", cardTestAt, []Alert{auth, coverageAlert("main", "model"), coverageAlert("wallet-6", "model"), coverageAlert("wallet-7", "model")})
	if err != nil {
		t.Fatal(err)
	}
	first := html.UnescapeString(strings.Join(markdownIn(cardElements(t, card)[0]), "\n"))
	if !strings.Contains(first, "主钱包、钱包 6、钱包 7当前无法新开仓") {
		t.Fatal("largest same-severity impact not primary focus", first)
	}
}

func TestCardCoverageGolden(t *testing.T) {
	card, err := RenderCard("trading-execution", cardTestAt, []Alert{coverageAlert("main", "v4.1 flash"), coverageAlert("wallet-6", "v4.1 flash"), coverageAlert("wallet-7", "v4.1 flash")})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.MarshalIndent(card, "", "  ")
	got = append(got, '\n')
	path := filepath.Join("testdata", "coverage-card.golden.json")
	if os.Getenv("UPDATE_PIPELINE_CARD_GOLDEN") == "1" {
		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, got, 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("alert card presentation changed; review and update golden intentionally")
	}
}

func TestCardColumnPropertiesMatchCard2ComponentSchema(t *testing.T) {
	card, err := RenderCard("trading-execution", cardTestAt, []Alert{coverageAlert("main", "model")})
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]map[string]bool{
		"column":     {"tag": true, "width": true, "weight": true, "vertical_align": true, "direction": true, "horizontal_spacing": true, "vertical_spacing": true, "padding": true, "margin": true, "background_style": true, "action": true, "elements": true},
		"column_set": {"tag": true, "columns": true, "flex_mode": true, "horizontal_spacing": true, "horizontal_align": true, "background_style": true, "action": true, "margin": true},
	}
	var visit func(any)
	visit = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			if tag, ok := v["tag"].(string); ok {
				if fields, found := allowed[tag]; found {
					for key := range v {
						if !fields[key] {
							t.Fatalf("unsupported Card2 %s property %s", tag, key)
						}
					}
				}
			}
			for _, x := range v {
				visit(x)
			}
		case []any:
			for _, x := range v {
				visit(x)
			}
		}
	}
	visit(card)
}

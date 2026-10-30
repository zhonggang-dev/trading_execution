package pipelinemonitor

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Alert is a presentation input. Recovery is explicit: a healthy Check alone
// does not prove that the recipient was previously notified of an incident.
type Alert struct {
	Check    Check
	Recovery bool
}

type cardIssue struct {
	alerts   []Alert
	accounts []string
}

// RenderCard returns a credential-free Card 2.0 document. It does not select,
// acknowledge, mutate or persist Checks. All supplied alerts remain in the
// collapsed diagnostics even when only four issues are shown on the front.
// Callers must partition active alerts and recoveries before calling it.
func RenderCard(component string, sampledAt time.Time, alerts []Alert) (map[string]any, error) {
	for _, budget := range []int{1400, 900, 600, 300} {
		card, err := renderCard(component, sampledAt, alerts, budget)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(card)
		if err != nil {
			return nil, fmt.Errorf("encode alert card")
		}
		if len(data) < 28*1024 {
			return card, nil
		}
	}
	return nil, fmt.Errorf("alert card exceeds safe payload budget")
}
func renderCard(component string, sampledAt time.Time, alerts []Alert, diagnosticBudget int) (map[string]any, error) {
	if len(alerts) == 0 || len(alerts) > 128 {
		return nil, fmt.Errorf("alert card requires between one and 128 checks")
	}
	recovery := alerts[0].Recovery
	color := "orange"
	for _, a := range alerts {
		if a.Recovery != recovery || (a.Recovery && a.Check.Status != Healthy) || (!a.Recovery && rank(a.Check.Status) == 0) {
			return nil, fmt.Errorf("alert card requires one valid lifecycle")
		}
		if !ValidStatus(a.Check.Status) {
			return nil, fmt.Errorf("alert card status is invalid")
		}
		if a.Check.Status == Critical {
			color = "red"
		}
	}
	if recovery {
		color = "green"
	}
	issues := groupCardIssues(alerts)
	titleLimit, secondaryLimit, diagnosticTitleLimit := 80, 220, 80
	if diagnosticBudget <= 600 {
		titleLimit, secondaryLimit, diagnosticTitleLimit = 60, 140, 48
	}
	elements := make([]any, 0, 5)
	for i, issue := range issues {
		if i == 4 {
			break
		}
		c := issue.alerts[0].Check
		titleSize, background := "normal", "grey-50"
		if i == 0 {
			titleSize, background = "heading-4", color+"-50"
		}
		issueColor := color
		if !recovery && c.Status != Critical {
			issueColor = "orange"
		}
		content := []any{
			cardMarkdown("**<font color='"+issueColor+"'>"+escapeCard(shortCard(problemTitle(c), titleLimit))+"</font>**", titleSize),
			cardMarkdown("**影响："+escapeCard(issueImpact(issue, recovery))+"**", "normal"),
		}
		secondary := foregroundContext(issue)
		if metrics := keyMetrics(c, sampledAt); metrics != "" {
			if secondary != "" {
				secondary += " · "
			}
			secondary += metrics
		}
		if secondary == "" {
			if action := suggestedAction(c); action != "" {
				secondary = "建议：" + action
			}
		}

		content = append(content, cardMarkdown("<font color='grey'>"+escapeCard(shortCard(secondary, secondaryLimit))+"</font>", "notation"))
		elements = append(elements, map[string]any{
			"tag": "column_set", "flex_mode": "none", "horizontal_spacing": "8px", "margin": "0px 0px 12px 0px",
			"columns": []any{map[string]any{"tag": "column", "width": "weighted", "weight": 1,
				"background_style": background, "padding": "12px", "vertical_spacing": "4px", "elements": content}},
		})
	}
	diagnostics := make([]any, 0, len(issues))
	for i, issue := range issues {
		var details strings.Builder
		fmt.Fprintf(&details, "**%d. %s**\n**影响：%s**\n范围：%s", i+1, escapeCard(shortCard(problemTitle(issue.alerts[0].Check), diagnosticTitleLimit)), escapeCard(issueImpact(issue, recovery)), escapeCard(shortCard(issueScope(issue), 100)))
		for _, a := range issue.alerts {
			details.WriteString("\n\n" + diagnosticCheck(a.Check, diagnosticBudget))
		}

		diagnostics = append(diagnostics, cardMarkdown(details.String(), "notation"))
	}
	foldTitle := fmt.Sprintf("诊断详情 · %d 项检查", len(alerts))
	if len(issues) > 4 {
		foldTitle = fmt.Sprintf("另 %d 类问题及全部诊断详情", len(issues)-4)
	}
	elements = append(elements, map[string]any{
		"tag": "collapsible_panel", "expanded": false, "padding": "8px", "vertical_spacing": "8px", "margin": "0px",
		"header": map[string]any{"title": map[string]any{"tag": "plain_text", "content": foldTitle}}, "elements": diagnostics,
	})
	heading := "实盘链路预警"
	if color == "red" {
		heading = "实盘链路异常"
	} else if recovery {
		heading = "实盘链路恢复"
	}
	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{"update_multi": true, "width_mode": "default",
			"summary": map[string]any{"content": heading + " · " + plainCardText(shortCard(problemTitle(issues[0].alerts[0].Check), 60))}},
		"header": map[string]any{
			"title": map[string]any{"tag": "plain_text", "content": heading}, "template": color,
			"subtitle": map[string]any{"tag": "plain_text", "content": plainCardText(componentLabel(component)) + " · " + sampledAt.In(time.FixedZone("CST", 8*3600)).Format("01-02 15:04:05") + " 北京时间"},
			"icon":     map[string]any{"tag": "standard_icon", "token": "lark-logo_colorful"},
		},
		"body": map[string]any{"direction": "vertical", "padding": "12px 12px 20px 12px", "elements": elements},
	}
	return card, nil
}

func cardMarkdown(content, size string) map[string]any {
	return map[string]any{"tag": "markdown", "content": content, "text_size": size, "margin": "0px"}
}

func groupCardIssues(alerts []Alert) []cardIssue {
	ordered := append([]Alert(nil), alerts...)
	sort.SliceStable(ordered, func(i, j int) bool { return rank(ordered[i].Check.Status) > rank(ordered[j].Check.Status) })
	var groups []cardIssue
	indices := map[string]int{}
	for _, a := range ordered {
		account, groupKey := coverageGroupKey(a)
		if groupKey != "" {
			if index, exists := indices[groupKey]; exists {
				groups[index].alerts = append(groups[index].alerts, a)
				if !containsString(groups[index].accounts, account) {
					groups[index].accounts = append(groups[index].accounts, account)
				}
				continue
			}
			indices[groupKey] = len(groups)
		}
		issue := cardIssue{alerts: []Alert{a}}
		if groupKey != "" {
			issue.accounts = []string{account}
		}
		groups = append(groups, issue)
	}
	sort.SliceStable(groups, func(i, j int) bool {
		left, right := rank(groups[i].alerts[0].Check.Status), rank(groups[j].alerts[0].Check.Status)
		if left != right {
			return left > right
		}
		coverage := func(x cardIssue) int {
			if x.alerts[0].Check.ID == "prediction.coverage" && x.alerts[0].Check.Evidence["effective_markets"] == "0" && len(x.accounts) > 1 {
				return len(x.accounts)
			}
			return 0
		}
		return coverage(groups[i]) > coverage(groups[j])
	})
	return groups
}

// Only confirmed identical prediction coverage can be coalesced across wallets.
// All non-account evidence remains part of the key, including venue counts,
// source/model, reason and snapshot identity; uncertainty keeps issues separate.
func coverageGroupKey(a Alert) (string, string) {
	c := a.Check
	account, model, mode, ok := coverageRoute(c)
	if c.ID != "prediction.coverage" || !ok || (mode != "SANDBOX" && mode != "DIRECT") {
		return "", ""
	}
	evidence := make(map[string]string, len(c.Evidence))
	for key, value := range c.Evidence {
		switch key {
		case "execution_account_id", "account_id", "strategy_id", "source_model", "source_mode":
			continue
		}
		evidence[key] = value
	}
	key, _ := json.Marshal(struct {
		ID, Status, Summary, Details, Model, Mode string
		Recovery                                  bool
		Evidence                                  map[string]string
	}{c.ID, c.Status, c.Summary, c.Details, model, mode, a.Recovery, evidence})
	return account, string(key)
}

func problemTitle(c Check) string {
	if c.ID == "prediction.coverage" && c.Status == Critical && c.Evidence["effective_markets"] == "0" {
		return "没有可用预测结果"
	}
	if title := specificProblemTitle(c); title != "" {
		return title
	}

	if c.Summary != "" {
		return c.Summary
	}
	if c.Status == Unknown {
		return "该环节状态暂时无法确认"
	}
	return "该环节需要检查"
}

func issueScope(issue cardIssue) string {
	c := issue.alerts[0].Check
	if _, model, mode, ok := coverageRoute(c); c.ID == "prediction.coverage" && ok {
		if mode == "SANDBOX" {
			mode = "脱敏包预测"
		} else if mode == "DIRECT" {
			mode = "直接预测"
		}
		if mode == "" {
			return model
		}
		return model + " · " + mode
	}

	if c.Scope == "engine" {
		return "交易引擎"
	}
	if c.Scope == "prediction-infra" || c.Scope == "trading-execution" {
		return componentLabel(c.Scope)
	}
	parts := strings.Split(c.Scope, "/")
	if parts[0] == "POLYMARKET" || parts[0] == "KALSHI" {
		venue := "Polymarket"
		if parts[0] == "KALSHI" {
			venue = "Kalshi"
		}
		if len(parts) > 1 {
			return venue + " · " + accountLabel(parts[1])
		}
		return venue
	}
	if parts[0] == "main" || strings.HasPrefix(parts[0], "wallet-") || strings.HasPrefix(parts[0], "kalshi:") {
		return accountLabel(parts[0])
	}
	return shortCard(c.Scope, 90)
}

func componentLabel(s string) string {
	switch s {
	case "prediction-infra":
		return "数据中台"
	case "trading-execution":
		return "交易执行"
	case "host", "host-probe":
		return "服务器监控"
	}
	return shortCard(s, 60)
}
func accountLabel(s string) string {
	if s == "main" {
		return "主钱包"
	}
	if strings.HasPrefix(s, "wallet-") {
		return "钱包 " + strings.TrimPrefix(s, "wallet-")
	}
	if strings.HasPrefix(s, "kalshi:") {
		if s == "kalshi:main" {
			return "Kalshi 主账户"
		}
		return "Kalshi 账户 " + strings.TrimPrefix(s, "kalshi:")
	}
	return shortCard(s, 90)
}

func cardTime(s string) string {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.IsZero() {
		return ""
	}
	return t.In(time.FixedZone("CST", 8*3600)).Format("01-02 15:04")
}

// Escape every Markdown/HTML metacharacter before inserting external values.
// Numeric entities are supported by Lark markdown and cannot create at/link/font tags.
func escapeCard(s string) string {
	var out strings.Builder
	for _, r := range s {
		if strings.ContainsRune("&<>*_~#[]()!`\\@|:", r) {
			out.WriteString("&#" + strconv.Itoa(int(r)) + ";")
		} else if r != '\r' {
			out.WriteRune(r)
		}
	}
	return out.String()
}
func shortCard(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	runes := []rune(s)
	if len(runes) > max {
		return string(runes[:max]) + "…"
	}
	return s
}
func plainCardText(s string) string {
	return strings.NewReplacer("<", "＜", ">", "＞", "@", "＠", "&", "＆").Replace(s)
}
func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// Diagnostic budgets apply to details, never to which Check identities appear.
// Truncation is explicit; the full observation remains in the authenticated API.
func diagnosticCheck(c Check, budget int) string {
	var b strings.Builder
	truncated := false
	add := func(label, value string, limit int) {
		escaped, cut := escapeLimited(value, limit)
		truncated = truncated || cut
		b.WriteString(label + "：" + escaped + "\n")
	}
	add("检查", c.ID, max(30, budget/6))
	add("完整范围", c.Scope, max(50, budget/4))
	add("状态", c.Status, 24)
	add("原始说明", c.Summary, max(60, budget/3))
	if remaining := budget - b.Len(); remaining > 60 && c.Details != "" {
		add("详细解释", c.Details, min(240, remaining/2))
	} else if c.Details != "" {
		truncated = true
	}
	keys := make([]string, 0, len(c.Evidence))
	for k := range c.Evidence {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		remaining := budget - b.Len()
		if remaining < 60 {
			truncated = true
			break
		}
		key, keyCut := escapeLimited(k, min(70, remaining/3))
		value, valueCut := escapeLimited(c.Evidence[k], min(180, remaining-len(key)-8))
		truncated = truncated || keyCut || valueCut
		b.WriteString(key + "：" + value + "\n")
	}
	if truncated {
		b.WriteString("（明细已限长，完整值见只读监控接口）")
	}
	return strings.TrimSpace(b.String())
}
func escapeLimited(s string, budget int) (string, bool) {
	var b strings.Builder
	for _, r := range s {
		part := escapeCard(string(r))
		if b.Len()+len(part) > budget {
			return b.String() + "…", true
		}
		b.WriteString(part)
	}
	return b.String(), false
}
func zeroMetric(value string) bool {
	if n, err := strconv.ParseFloat(value, 64); err == nil {
		return n == 0
	}
	if d, err := time.ParseDuration(value); err == nil {
		return d == 0
	}
	return false
}

func coverageRoute(c Check) (account, model, mode string, ok bool) {
	parts := strings.Split(c.Scope, "/")
	if len(parts) < 3 || parts[0] == "" || parts[len(parts)-1] == "" {
		return "", "", "", false
	}
	model = strings.Join(parts[1:len(parts)-1], "/")
	if model == "" {
		return "", "", "", false
	}
	if explicit := c.Evidence["source_model"]; explicit != "" && explicit != model {
		return "", "", "", false
	}
	return parts[0], model, c.Evidence["source_mode"], true
}
func issueActors(issue cardIssue) string {
	accounts := append([]string(nil), issue.accounts...)
	if len(accounts) == 0 {
		scope := issue.alerts[0].Check.Scope
		parts := strings.Split(scope, "/")
		if parts[0] == "POLYMARKET" || parts[0] == "KALSHI" {
			if len(parts) > 1 && (strings.HasPrefix(issue.alerts[0].Check.ID, "account.") || strings.HasPrefix(issue.alerts[0].Check.ID, "market.") || strings.HasPrefix(issue.alerts[0].Check.ID, "prediction.")) {
				accounts = []string{parts[1]}
			}
		} else if parts[0] == "main" || strings.HasPrefix(parts[0], "wallet-") || strings.HasPrefix(parts[0], "kalshi:") {
			accounts = []string{parts[0]}
		}
	}
	sort.Strings(accounts)
	for i := range accounts {
		accounts[i] = accountLabel(accounts[i])
	}
	return strings.Join(accounts, "、")
}

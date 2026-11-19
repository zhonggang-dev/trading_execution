package walletdrawdown

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

var beijing = time.FixedZone("CST", 8*3600)

// RenderCard builds the Feishu card for one exceeded sample.
func RenderCard(cfg Config, sample Sample, peak *Sample, followUp bool) (map[string]any, error) {
	if sample.Drawdown == nil || sample.PeakEquity == nil || sample.PeakAt == nil {
		return nil, fmt.Errorf("drawdown card requires an evaluated sample")
	}
	heading := cfg.AccountID + " 钱包回撤超阈值"
	if followUp {
		heading += "（继续扩大）"
	}
	var body strings.Builder
	fmt.Fprintf(&body, "**过去 %d 分钟回撤 %s 美元**（阈值 %s）\n", sample.WindowMinutes, short(*sample.Drawdown), sample.Threshold)
	fmt.Fprintf(&body, "窗口高点：%s 美元（%s）\n", short(*sample.PeakEquity), sample.PeakAt.In(beijing).Format("15:04"))
	fmt.Fprintf(&body, "当前总资产：%s 美元 = 现金 %s + 持仓 %s", short(sample.Equity), short(sample.CashPUSD), short(sample.PositionsValue))
	elements := []any{markdown(body.String(), "normal")}
	if list := Contributors(sample, peak, 3); len(list) > 0 {
		var lines strings.Builder
		lines.WriteString("**跌得最多的仓位**（按现持份数 × 价格变化）")
		for i, c := range list {
			fmt.Fprintf(&lines, "\n%d. %s · %s：%s 美元（价格 %s → %s）", i+1, escape(c.Title), escape(c.Outcome), c.Change.FloatString(2), c.From, c.To)
		}
		elements = append(elements, markdown(lines.String(), "normal"))
	}
	elements = append(elements, markdown("<font color='grey'>总资产 = 链上 pUSD + Data API 持仓市值（中间价）。窗口内如有充值或提现，回撤会不准。</font>", "notation"))
	return map[string]any{
		"schema": "2.0",
		"config": map[string]any{"update_multi": true, "width_mode": "default",
			"summary": map[string]any{"content": fmt.Sprintf("%s · 回撤 %s 美元", heading, short(*sample.Drawdown))}},
		"header": map[string]any{
			"title":    map[string]any{"tag": "plain_text", "content": heading},
			"template": "red",
			"subtitle": map[string]any{"tag": "plain_text", "content": sample.SampledAt.In(beijing).Format("01-02 15:04") + " 北京时间"},
		},
		"body": map[string]any{"direction": "vertical", "padding": "12px 12px 16px 12px", "elements": elements},
	}, nil
}

func markdown(content, size string) map[string]any {
	return map[string]any{"tag": "markdown", "content": content, "text_size": size}
}

// short renders a six-decimal amount with two decimals.
func short(text string) string {
	value, ok := new(big.Rat).SetString(text)
	if !ok {
		return text
	}
	return value.FloatString(2)
}

func escape(text string) string {
	replacer := strings.NewReplacer("<", "&lt;", ">", "&gt;", "*", "\\*", "_", "\\_", "[", "\\[", "]", "\\]")
	if len([]rune(text)) > 60 {
		text = string([]rune(text)[:60]) + "…"
	}
	return replacer.Replace(text)
}

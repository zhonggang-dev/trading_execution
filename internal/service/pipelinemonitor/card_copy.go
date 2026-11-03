package pipelinemonitor

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Copy is deliberately separate from detectors: it explains only existing
// observations, and cannot turn a sample, threshold or Unknown into a business fact.
type cardCopy struct{ id, impact, recovered, unknown, action string }

var cardCopies = []cardCopy{
	{"publication.schedule", "{s}的新一轮输入可能未刷新，后续构建与概率更新可能延迟。", "{s}发布水位重新进入允许时效；旧积压仍需继续处理。", "无法确认{s}发布水位，是否按时刷新尚不明确。", "检查选盘与刷新调度"},
	{"publication.dispatch", "{s}的部分应预测市场缺少构建任务，下一批输入不完整。", "本轮应预测成员的任务登记已补齐；不等于构建已完成。", "无法确认{s}本轮任务登记是否完整。", "核对本轮应预测成员和任务发布"},
	{"publication.outbox", "{s}的任务发布尚未完成确认，下游交付可能不完整。", "本次未发布任务采样未发现超时；不代表历史队列已清空。", "未发布任务观测不完整，无法确认下游交付进度。", "检查任务发布重试"},
	{"sandbox.original", "{s}尚未完成原始包交付，脱敏和预测可用时间正在缩短。", "本轮原始包阶段已恢复预算内推进；后续脱敏仍需完成。", "无法确认{s}原始包交付进度，后续延迟尚不明确。", "检查构建阶段与上传记录"},
	{"sandbox.masked", "{s}的脱敏输入未完成交付，依赖它的预测和回传可能延后。", "本轮脱敏交付检查恢复，后续预测与回传仍需正常推进。", "无法确认{s}脱敏包交付状态。", "检查脱敏阶段和上传接口"},
	{"sandbox.ready", "{s}的回传任务通知缺失，本地结果也可能无法及时上传。", "本轮可覆盖的 Ready 检查恢复；本地预测进度仍按原流程推进。", "Ready 观测不能完整覆盖{s}，尚不能确认通知是否缺失。", "检查 Ready 发布与任务身份"},
	{"sandbox.result", "{s}的模型结果尚未按预算入库，后续开仓输入可能断档。", "本轮回传阶段恢复预算内推进；不代表所有在途任务已完成。", "无法确认{s}概率回传进度。", "检查模型执行与 PUT 回传"},
	{"direct.result", "{s}的直接预测任务尚未及时回传，新开仓输入可能不足。", "当前 Direct 任务观察窗口未再发现超期回传。", "无法确认{s}直接预测任务的回传状态。", "核对直接预测任务与结果身份"},
	{"queue.consumer", "{s}的消费登记或进度异常，相关任务交付可能延迟。", "本次队列消费观测恢复；ACK 不代表业务任务已完成。", "无法确认{s}等待与消费进度。", "检查对应消费者及任务进度"},
	{"queue.observation", "{s}的队列观测异常，交付时效无法确认。", "队列状态已重新可读，任务完成情况仍看业务回执。", "无法读取{s}的队列状态，不能将未知积压当作零。", "恢复队列只读观测"},
	{"prediction.observation", "可交易输入观测异常，相关路由的开仓条件无法确认。", "可交易输入观测恢复，具体覆盖仍以对应路由检查为准。", "无法确认可用开仓输入，暂不能认定实际交易已经停止。", "恢复概率快照读取"},
	{"prediction.venue", "{s}的对应来源输入不足，相关新买入可能受门禁限制。", "该来源当前观察窗口恢复允许状态；不代表所有历史任务完成。", "无法确认该来源的可用开仓输入。", "检查对应来源的任务和回传"},
	{"account.authorization", "{s}的新订单可能被授权或控制门禁阻止；撤单和查询按原规则处理。", "{s}当前授权与控制检查通过，实际下单仍需其他条件满足。", "无法确认{s}当前授权，不能据此认定所有新订单已被阻止。", "核对账户绑定、风控政策和控制项"},
	{"account.cash", "{s}没有可用资金用于新买入；平仓和撤单按原规则处理。", "{s}的可用新买入资金回到允许状态，仍需保留合法预占。", "无法确认{s}可用资金，不能用未知余额当作零。", "核对可用资金和在途预占"},
	{"account.risk_state", "{s}的对账有效性未满足风控要求，新订单可能被阻止。", "{s}对账结果重新处于政策有效期内，其他门禁仍独立检查。", "无法确认{s}对账有效期政策或时间证据。", "恢复该账户的成功对账"},
	{"account.reconciliation", "{s}的对账就绪检查未通过，新订单可能被阻止；撤单和查询按原规则处理。", "{s}本次对账就绪检查通过，不代表所有历史差异已消失。", "无法读取{s}对账就绪状态。", "查看该账户最新对账结果"},
	{"account.heartbeat", "{s}的交易所心跳未就绪，新订单会被本地门禁拦截；撤单和查询按原规则处理。", "{s}当前交易所心跳就绪检查通过。", "无法确认{s}交易所心跳状态。", "检查该账户的心跳会话与接口"},
	{"account.reconciliation_issues", "已发现{s}的账户级未解决对账问题，新订单可能被阻止。", "本次采样未再发现{s}账户级对账门禁，不代表全部历史问题已解决。", "无法确认{s}账户级对账问题是否仍阻断新订单。", "处理该账户未解决对账问题"},
	{"market.reconciliation_issues", "{s}已发现问题的市场或代币可能无法提交新订单，不涉及其他市场。", "本次采样未再发现对应市场级对账门禁。", "无法确认{s}相关市场或代币的对账门禁。", "查看对应市场或代币的对账问题"},
	{"decision.schedule", "本轮决策没有按计划正常完成，相关交易意图可能延迟。", "当前决策循环恢复允许状态；已错过的周期不会补发陈旧交易。", "无法确认当前决策循环是否按期推进。", "查看本轮决策开始和完成记录"},
	{"strategy.progress", "{s}的本轮策略输出未及时就绪，相关交易意图可能延迟。", "{s}当前策略周期检查恢复，实际意图仍由策略结果决定。", "无法确认{s}当前策略输出，旧周期不能证明本轮正常。", "检查该路由的策略接口和周期记录"},
	{"intent.delivery", "{s}本次采样仍有未完成确认的投递意图，需核对交易所结果，不能直接重投。", "本次投递采样未再发现超时项，不代表所有历史 UNKNOWN 都已解决。", "无法确认{s}投递状态，不能据此认定订单失败或直接重投。", "按原订单身份核对投递与交易所结果"},
	{"order.recovery", "{s}本次采样仍有未解决的订单或撤单恢复项，状态确认前不能重复提交。", "本次订单恢复采样回到允许状态；历史订单仍以权威证据为准。", "无法确认{s}订单恢复结果，不能据此推断成交或未成交。", "核对原订单、撤单和成交证据"},
	{"fill.application", "{s}有已确认成交尚未完成账本应用，可用资金与持仓可能滞后。", "本次采样未再发现超时的确认成交入账项，不代表补写了历史账本。", "无法确认{s}成交应用进度，不能将观测失败当作没有成交。", "恢复确认成交的幂等账本应用"},
	{"venue.execution_errors", "{s}近期执行存在具名交易所异常，相关订单提交可能受影响。", "近期交易所异常回到告警阈值内，不等于错误数量归零。", "无法确认{s}近期交易所执行异常。", "查看最新样本原因及交易所状态"},
	{"trading.database", "交易记录观测不完整，不能确认各路由当前执行状态。", "本轮交易记录观测恢复，不代表历史异常全部清除。", "无法读取完整交易记录，不能据此认定订单未执行。", "恢复交易数据库只读采样"},
	{"worker.predictor.process", "李昂预测进程未正常运行，下一批概率产出受影响。", "当前预期预测进程恢复观测；在途预测仍需完成。", "无法确认李昂预测进程是否存在，尚不能确认预测已停止。", "检查预测服务与实例归属"},
	{"worker.uploader.process", "李昂回传进程未正常运行，本地结果可能无法及时入库。", "当前预期回传进程恢复观测；待回传结果仍需推进。", "无法确认李昂回传进程是否存在。", "检查概率回传服务"},
	{"worker.predictor.sources", "部分应预测来源未获准进入模型，对应市场不会产生本轮结果。", "预测来源准入重新包含配置要求的来源。", "无法确认实际允许的预测来源。", "核对预测来源准入配置"},
	{"worker.predictor.schedule", "模型只接收指定 t0 时段的输入，全天开仓输入可能断档。", "输入准入时间窗检查回到允许状态，概率有效期没有自动放宽。", "无法确认输入时间窗能否覆盖计划交易时段。", "核对预测刷新计划与交易有效窗口"},
	{"worker.predictor.execution", "已发现预测失败、异常重试或超预算任务，本批概率回传可能延后。", "本次执行采样未再发现近期失败、异常重试或超预算；在途任务仍需完成。", "无法确认预测执行进度，不能将缺少记录视为模型成功。", "查看近期失败与异常重试原因"},
	{"worker.predictor.inputs", "本批新发现的输入已超过准入新鲜度，将被跳过预测。", "近期输入采样未再发现新入场过期包，旧包仍按原规则处理。", "无法确认新发现输入是否仍在准入有效期内。", "检查上游积压与输入冻结时间"},
	{"worker.predictor.discovery", "候选包或元数据校验存在异常，对应输入可能无法进入预测。", "近期候选发现采样未再发现 invalid/error，不代表所有历史候选已重算。", "无法读取候选发现记录，输入是否成功入场尚不明确。", "检查候选包和原始元数据"},
	{"worker.predictor.state", "预测进度账本不可用，实际执行与回传进度无法完整确认。", "预测任务账本重新可读，具体执行状态以采样为准。", "无法读取预测任务账本，不能认定任务已经失败或完成。", "恢复预测状态库只读采集"},
	{"worker.uploader.state", "已发现近期回传阻塞或失败，本地结果仍需确认是否入库。", "本次采样未再发现近期回传异常，waiting 仍需等待本地结果。", "无法读取概率回传账本，PUT 是否成功尚未确认。", "查看近期回传错误与接口状态"},
	{"worker.builder.state", "原始或脱敏构建采样发现近期失败，下一批预测输入可能延后。", "本次构建采样未再发现近期失败，在途构建与上传仍需完成。", "无法读取 Sandbox 构建账本，实际构建和上传状态尚未确认。", "检查构建失败和上传记录"},
	{"worker.qa.state", "QA 构建采样发现近期失败，QA 包更新可能延后。", "本次 QA 构建采样未再发现近期失败。", "无法读取 QA 构建状态，实际产出尚未确认。", "检查 QA 构建失败记录"},
	{"monitor.delivery", "{s}告警可能无法送达，需要恢复通知通道。", "最近一次飞书通知已成功发送，送达通道恢复观测。", "{s}通知送达未获确认；业务执行开关不会因此改变。", "检查飞书机器人与发送返回"},
	{"monitor.state", "{s}告警历史状态无法读取，去重和恢复判断不可靠。", "告警历史状态重新可读，去重和恢复判断可继续。", "无法读取告警历史状态，监控未盲目丢弃去重信息。", "修复状态文件权限或损坏内容"},
	{"monitor.persistence", "{s}告警状态无法落盘，重启后可能重复通知。", "告警状态已重新持久化，当前成功发送记录可保存。", "无法持久化告警状态，本进程内记录仍保留。", "检查告警状态目录的可写性"},
	{"monitor.*", "{s}链路观测可能停止或不完整，故障发现能力受影响。", "本次链路监控自身检查恢复，业务健康仍由各项事实判断。", "无法确认{s}监控是否持续推进。", "检查监控采样和独立配置"},
	{"dependency.postgres", "中台任务与结果存储依赖异常，发布和回传可能受影响。", "本次中台数据库只读观测恢复。", "无法读取中台任务与结果状态，不能用零任务替代失败。", "检查数据库连接与查询超时"},
	{"dependency.redis", "消息交付依赖异常，任务通知是否到达尚需核对。", "本次消息队列观测恢复，任务完成仍需看业务回执。", "无法确认消息队列交付状态，不能据此认定通知丢失。", "恢复 Redis 只读采样"},
	{"host.monitor", "{s}的监控采样或通知可能中断，业务健康暂不能确认。", "{s}监控采样恢复推进，具体业务问题仍独立判断。", "无法读取{s}的监控自检。", "恢复监控快照与发送状态"},
	{"host.clock_sync", "主机时间尚未获同步确认，时间窗与信号时效判断存在偏差风险。", "主机重新报告时间同步；不等于测得精确外部时钟偏差。", "无法读取系统时间同步状态，不能认定已同步。", "检查系统时间同步服务"},
	{"host.memory", "可用内存余量偏低，构建与业务进程稳定性受到压力。", "本次可用内存采样回到阈值内，应继续观察并发任务压力。", "无法确认主机可用内存，不能将未知余量当作充足。", "检查内存占用与并发任务"},
	{"host.restart_storm", "{s}近期反复重启，业务处理可能被持续打断。", "本次窗口的服务重启次数回到阈值内。", "无法确认{s}近期重启频率。", "检查该服务退出原因"},
	{"host.service", "{s}未正常运行，对应业务产出可能停止。", "{s}已恢复到预期运行状态，在途任务仍需正常推进。", "无法确认{s}服务状态，不能据此认定业务已停止。", "检查该服务状态与退出记录"},
	{"host.application*", "{s}存活探测未通过，相关接口可能不可用。", "{s}存活探测恢复；业务 readiness 仍由独立检查判断。", "无法完成{s}存活探测，接口是否可用尚未确认。", "检查应用进程与监听接口"},
	{"host.*", "{s}可用容量或资源观测异常，文件写入与服务运行可能受影响。", "本次资源采样回到允许状态，仍需观察积压写入。", "无法读取{s}的资源状态，当前余量不能确认。", "检查对应资源和采集权限"},
}

func findCardCopy(id string) (cardCopy, bool) {
	for _, r := range cardCopies {
		if id == r.id || strings.HasSuffix(r.id, "*") && strings.HasPrefix(id, strings.TrimSuffix(r.id, "*")) {
			return r, true
		}
	}
	return cardCopy{}, false
}
func issueImpact(issue cardIssue, recovery bool) string {
	c := issue.alerts[0].Check
	subject := impactSubject(issue)
	if c.ID == "prediction.coverage" {
		if recovery {
			return subject + "的可用预测覆盖已恢复，是否开仓仍由策略和其他门禁决定。"
		}
		if c.Status == Unknown {
			return "无法确认" + subject + "的开仓输入，尚不能认定实际交易已停止。"
		}
		if c.Status == Critical && c.Evidence["effective_markets"] == "0" {
			return subject + "当前无法新开仓；平仓、撤单按原规则处理。"
		}
		return subject + "的开仓输入即将失效；若未补齐，将无法继续新开仓。"
	}
	if !recovery && c.Status != Unknown {
		if c.ID == "account.risk_state" && c.Status == Warning {
			return subject + "的对账结果接近过期，若未刷新将触发新订单门禁。"
		}
		if c.ID == "strategy.progress" && positiveEvidence(c, "missing_or_error_books") {
			return subject + "对应市场的策略输入不完整，需要先恢复异常盘口读取。"
		}
		if c.ID == "venue.execution_errors" {
			switch c.Evidence["sample_failure_code"] {
			case "CLOB_TRADING_PAUSED":
				return subject + "的最新异常样本被交易所暂停接单模式拒绝，需核对当前交易所状态。"
			case "CLOB_AUTH_FAILED", "KALSHI_AUTH_FAILED", "CLOB_INVALID_SIGNATURE":
				return subject + "最新异常样本的认证或签名未通过，相关提交可能被拒绝。"
			case "CLOB_INSUFFICIENT_BALANCE_ALLOWANCE":
				return subject + "最新异常样本提示余额或授权不足，相关买入可能受阻。"
			}
		}
		if c.ID == "decision.schedule" && c.Status == Warning {
			return "最近一次决策周期有错误，相关路由的策略或投递结果需要检查。"
		}
		if count, e := strconv.Atoi(c.Evidence["instances"]); strings.HasSuffix(c.ID, ".process") && e == nil && count > 1 {
			return subject + "存在多个实例，需核对是否属于同一实盘链路。"
		}
		if c.ID == "host.service" && (strings.Contains(c.Scope, "qa-live") || strings.Contains(c.Scope, "dataset-mask") || strings.Contains(c.Scope, "dataset-sanitize")) {
			return subject + "产出可能暂停，属于独立的 QA 或数据集处理链路。"
		}
	}
	if r, ok := findCardCopy(c.ID); ok {
		copy := r.impact
		if recovery {
			copy = r.recovered
		} else if c.Status == Unknown {
			copy = r.unknown
		}
		return strings.ReplaceAll(copy, "{s}", subject)
	}
	if recovery {
		return "该项采样检查恢复允许状态，具体业务进度仍以对应记录为准。"
	}
	if c.Status == Unknown {
		return "该阶段观测不完整，实际业务影响尚未确认。"
	}
	return "该阶段存在异常，需要结合完整诊断确认具体业务影响。"
}

func impactSubject(issue cardIssue) string {
	c := issue.alerts[0].Check
	if actors := issueActors(issue); actors != "" {
		return actors
	}
	if strings.HasPrefix(c.ID, "worker.predictor") {
		return "李昂预测服务"
	}
	if strings.HasPrefix(c.ID, "worker.uploader") {
		return "李昂回传服务"
	}
	if strings.HasPrefix(c.ID, "worker.builder") {
		return "Sandbox 构建服务"
	}
	if strings.HasPrefix(c.ID, "worker.qa") {
		return "QA 构建服务"
	}
	if strings.HasPrefix(c.ID, "queue.") {
		if strings.Contains(c.Scope, "sandboxbuild") {
			return "Sandbox 构建队列"
		}
		if strings.Contains(c.Scope, "sandboxready") {
			return "预测 Ready 队列"
		}
		if strings.Contains(c.Scope, "directprediction") {
			return "直接预测队列"
		}
		return "相关消息队列"
	}
	if strings.HasPrefix(c.ID, "host.") {
		labels := map[string]string{"pm-dataset-build-worker.service": "Sandbox 构建服务", "pm-dataset-mask-worker.service": "数据集脱敏服务", "pm-dataset-sanitize-worker.service": "数据集清洗服务", "qa-live-daemon-mengjunjie.service": "QA 生产服务", "prediction-infra.service": "数据中台", "trading-execution.service": "交易执行"}
		for key, label := range labels {
			if strings.Contains(c.Scope, key) {
				return label
			}
		}
		if strings.HasPrefix(c.Scope, "/") {
			return "目录 " + shortCard(c.Scope, 50)
		}
		return "主机"
	}
	if c.Scope == "POLYMARKET" || strings.HasPrefix(c.Scope, "POLYMARKET/") {
		if strings.HasPrefix(c.ID, "publication.") {
			return "Polymarket"
		}
		return "Polymarket 本批输入"
	}
	if c.Scope == "KALSHI" || strings.HasPrefix(c.Scope, "KALSHI/") {
		if strings.HasPrefix(c.ID, "publication.") {
			return "Kalshi"
		}
		return "Kalshi 本批输入"
	}
	if c.Scope == "prediction-infra" || c.Scope == "trading-execution" {
		return componentLabel(c.Scope)
	}
	if c.ID == "prediction.coverage" {
		return "所选路由"
	}
	return "相关链路"
}
func foregroundContext(issue cardIssue) string {
	c := issue.alerts[0].Check
	if c.ID == "prediction.coverage" {
		return issueScope(issue)
	}
	if c.ID == "prediction.venue" {
		parts := strings.Split(c.Scope, "/")
		if len(parts) >= 4 {
			return sourceChinese(parts[0]) + " · 模型 " + strings.Join(parts[2:len(parts)-1], "/")
		}
	}
	if strings.HasPrefix(c.ID, "sandbox.") || c.ID == "direct.result" {
		parts := strings.Split(c.Scope, "/")
		if len(parts) > 1 {
			return "模型 " + strings.Join(parts[1:], "/")
		}
	}
	return ""
}
func suggestedAction(c Check) string {
	if r, ok := findCardCopy(c.ID); ok {
		return r.action
	}
	return "查看该项只读诊断"
}

func keyMetrics(c Check, now time.Time) string {
	var out []string
	add := func(key, label string, zero bool) {
		if value := c.Evidence[key]; value != "" && (zero || !zeroMetric(value)) {
			out = append(out, label+" "+shortCard(value, 30))
		}
	}
	timeField := func(key, label string) {
		if value := cardTime(c.Evidence[key]); value != "" {
			out = append(out, label+" "+value)
		}
	}
	duration := func(key, label string) {
		if raw := c.Evidence[key]; raw != "" {
			if d, e := time.ParseDuration(raw); e == nil {
				out = append(out, label+" "+chineseDuration(d))
			}
		}
	}
	age := func(key, label string) {
		if t, e := time.Parse(time.RFC3339Nano, c.Evidence[key]); e == nil && !t.IsZero() && now.Sub(t) >= 0 {
			out = append(out, label+" "+chineseDuration(now.Sub(t)))
		}
	}
	recovery := c.Status == Healthy
	switch {
	case c.ID == "prediction.coverage" || c.ID == "prediction.venue":
		add("effective_markets", "可用预测市场", true)
		timeField("last_input_expires_at", "最晚失效")
		add("pending_direct_tasks", "待回传任务", false)
	case c.ID == "publication.schedule":
		timeField("last_published_at", "最近发布")
		age("last_published_at", "距今")
		duration("maximum_age", "允许间隔")
	case c.ID == "publication.dispatch":
		add("expected_members", "应预测市场", true)
		add("dispatched_members", "已登记市场", true)
	case c.ID == "publication.outbox":
		add("count", "采样未确认发布", false)
		age("oldest_created_at", "最长等待")
	case strings.HasPrefix(c.ID, "sandbox.") || c.ID == "direct.result":
		add("pending_stage_tasks", "本阶段待办", recovery)
		add("pending_tasks", "待回传任务", recovery)
		if c.ID == "direct.result" {
			add("expected_tasks", "观察窗期望任务", false)
		}
		if c.Evidence["eta_status"] == "estimated" {
			timeField("estimated_finish_at", "预计完成")
			timeField("earliest_input_deadline", "输入截止")
		} else {
			duration("oldest_task_age", "最长等待")
			timeField("earliest_input_deadline", "输入截止")
		}
	case strings.HasPrefix(c.ID, "queue."):
		if lag, err := strconv.ParseInt(c.Evidence["lag"], 10, 64); err == nil && lag >= 0 {
			add("lag", "待领取", recovery)
		}
		add("pending", "待确认", recovery)
		if c.Evidence["consumers"] == "0" {
			add("consumers", "登记消费者", true)
		}
	case c.ID == "account.cash":
		add("available_cash", "可用资金", true)
		add("reserved_cash", "预占资金", false)
	case c.ID == "account.risk_state":
		duration("age", "距上次对账")
		duration("max_state_age", "允许时效")
		timeField("last_completed_at", "最近成功")
	case c.ID == "account.reconciliation_issues":
		add("sampled_account_issues", "采样账户级问题", true)
	case c.ID == "market.reconciliation_issues":
		add("sampled_scoped_issues", "采样市场级问题", true)
		if v := reasonChinese(c.Evidence["sample_issue_type"]); v != "" {
			out = append(out, "样本类型 "+v)
		}
	case c.ID == "decision.schedule":
		timeField("last_started_at", "最近启动")
		timeField("next_due_at", "计划启动")
		add("skipped_boundaries", "本进程累计跳过", false)
	case c.ID == "strategy.progress":
		timeField("decision_at", "决策周期")
		add("missing_or_error_books", "异常盘口份数", recovery)
	case c.ID == "intent.delivery" || c.ID == "order.recovery" || c.ID == "fill.application":
		add("stalled_sample_rows", "采样超时项", true)
		age("oldest_observed_at", "最老项已等待")
		if v := stateChinese(c.Evidence["sample_state"]); v != "" {
			out = append(out, "样本状态 "+v)
		}
	case c.ID == "venue.execution_errors":
		add("sampled_orders", "近15分钟采样异常订单", true)
		if v := reasonChinese(c.Evidence["sample_failure_code"]); v != "" {
			out = append(out, "最新样本 "+v)
		}
		timeField("last_failure_at", "最近异常")
	case c.ID == "worker.predictor.execution":
		add("recent_failures", "近期失败", recovery)
		add("recent_error_retries", "异常重试", false)
		add("over_budget_running", "超预算在途", false)
	case c.ID == "worker.predictor.inputs":
		add("expired_inputs", "近期采样过期输入", true)
		add("recent_rows", "近期采样输入", false)
	case c.ID == "worker.predictor.discovery":
		add("recent_invalid_or_error", "近期采样异常候选", true)
	case strings.HasSuffix(c.ID, ".process") && strings.HasPrefix(c.ID, "worker."):
		add("instances", "进程实例", true)
	case c.ID == "worker.predictor.sources":
		for _, x := range []struct{ key, label string }{{"allowed", "已允许"}, {"expected", "应包含"}} {
			if v := c.Evidence[x.key]; v != "" {
				out = append(out, x.label+" "+sourceChinese(v))
			}
		}
	case c.ID == "worker.predictor.schedule":
		add("t0_window_bjt", "北京时间窗", true)
		if v := c.Evidence["max_input_age_hours"]; v != "" && v != "unknown" {
			out = append(out, "输入有效期 "+v+"小时")
		}
		duration("trading_lookback", "交易窗口")
	case c.ID == "worker.builder.state" || c.ID == "worker.qa.state":
		add("recent_sample_failures", "近期采样失败", true)
		add("sampled_rows", "采样任务", false)
	case c.ID == "worker.uploader.state":
		add("recent_errors", "近期采样回传异常", true)
		add("sampled_rows", "采样结果", false)
	case c.ID == "host.clock_sync":
		if v := c.Evidence["ntp_synchronized"]; v != "" {
			out = append(out, "时间同步 "+flagChinese(v, "已同步", "未同步"))
		}
		if v := c.Evidence["ntp_enabled"]; v != "" {
			out = append(out, "同步服务 "+flagChinese(v, "已启用", "未启用"))
		}
	case c.ID == "host.restart_storm":
		add("restarts_last_5m", "近5分钟重启", true)
	case c.ID == "host.service" || strings.HasPrefix(c.ID, "host.application"):
		if v := stateChinese(c.Evidence["ActiveState"]); v != "" {
			out = append(out, "服务状态 "+v)
		}
		add("NRestarts", "重启计数", false)
	case strings.HasPrefix(c.ID, "host."):
		if v := c.Evidence["used_percent"]; v != "" && v != "unknown" {
			out = append(out, "使用率 "+v+"%")
		}
		if v, e := strconv.ParseFloat(c.Evidence["available_kib"], 64); e == nil {
			out = append(out, fmt.Sprintf("可用内存 %.1f GiB", v/1024/1024))
		}
	case strings.HasPrefix(c.ID, "monitor."):
		timeField("last_failure_at", "最近发送失败")
		timeField("last_success_at", "最近送达")
	}
	if len(out) > 3 {
		out = out[:3]
	}
	return strings.Join(out, " · ")
}
func chineseDuration(d time.Duration) string {
	negative := d < 0
	if negative {
		d = -d
	}
	seconds := int64(d.Round(time.Second) / time.Second)
	prefix := ""
	if negative {
		prefix = "提前 "
	}
	if seconds >= 3600 {
		return fmt.Sprintf("%s%d小时%d分", prefix, seconds/3600, seconds%3600/60)
	}
	if seconds >= 60 {
		return fmt.Sprintf("%s%d分%d秒", prefix, seconds/60, seconds%60)
	}
	return fmt.Sprintf("%s%d秒", prefix, seconds)
}
func positiveEvidence(c Check, key string) bool {
	n, e := strconv.ParseFloat(c.Evidence[key], 64)
	return e == nil && n > 0
}
func sourceChinese(s string) string {
	return strings.NewReplacer("polymarket", "Polymarket", "POLYMARKET", "Polymarket", "kalshi", "Kalshi", "KALSHI", "Kalshi", ",", "、").Replace(s)
}
func flagChinese(s, yes, no string) string {
	switch s {
	case "yes", "true":
		return yes
	case "no", "false":
		return no
	}
	return "未知"
}
func stateChinese(s string) string {
	switch s {
	case "active":
		return "运行中"
	case "inactive", "dead":
		return "已停止"
	case "activating":
		return "启动中"
	case "failed":
		return "失败"
	case "UNKNOWN":
		return "结果未知"
	case "SUBMITTING":
		return "提交中"
	case "PENDING":
		return "待投递"
	case "CANCEL_PENDING":
		return "撤单待确认"
	case "MANUAL_REVIEW":
		return "待人工核查"
	case "CONFIRMED":
		return "已确认待应用"
	case "RECONCILING":
		return "对账中"
	}
	return ""
}
func reasonChinese(s string) string {
	switch s {
	case "CLOB_TRADING_PAUSED":
		return "交易所暂停接单"
	case "CLOB_AUTH_FAILED", "KALSHI_AUTH_FAILED":
		return "认证失败"
	case "CLOB_INVALID_SIGNATURE":
		return "签名无效"
	case "CLOB_INSUFFICIENT_BALANCE_ALLOWANCE":
		return "余额或授权不足"
	case "CLOB_RATE_LIMITED", "KALSHI_RATE_LIMITED":
		return "请求限流"
	case "CLOB_SERVER_ERROR", "KALSHI_SERVER_ERROR":
		return "服务端错误"
	case "KALSHI_TRANSPORT_FAILED":
		return "网络请求失败"
	case "POSITION_DRIFT":
		return "持仓差异"
	case "BALANCE_DRIFT":
		return "余额差异"
	case "SOURCE_UNAVAILABLE":
		return "权威数据不可用"
	case "ORDER_RECOVERY_STALLED":
		return "订单恢复停滞"
	}
	return ""
}

func specificProblemTitle(c Check) string {
	if c.Status == Unknown {
		return ""
	}
	switch {
	case c.ID == "host.memory":
		if c.Status == Healthy {
			return "可用内存回到阈值内"
		}
		return "可用内存余量偏低"
	case c.ID == "host.clock_sync":
		if c.Status == Healthy {
			return "主机时间同步已确认"
		}
		return "主机时间同步尚未确认"
	case c.ID == "venue.execution_errors" && c.Status == Healthy:
		return "交易所异常回到告警阈值内"
	case c.ID == "order.recovery" && c.Status == Healthy:
		return "订单恢复采样回到允许状态"
	case c.ID == "intent.delivery" && c.Status == Healthy:
		return "投递采样未再发现超时项"
	case c.ID == "fill.application" && c.Status == Healthy:
		return "本次采样未见超时成交入账项"
	case strings.HasPrefix(c.ID, "host.") && c.Evidence["used_percent"] != "":
		label := "磁盘空间"
		if strings.Contains(c.ID, "inode") || c.Evidence["metric"] == "inodes" {
			label = "文件节点"
		}
		if c.Status == Healthy {
			return label + "余量回到阈值内"
		}
		return label + "使用率偏高"
	case c.ID == "worker.builder.state" && positiveEvidence(c, "recent_sample_failures"):
		return "近期 Sandbox 构建采样出现失败"
	case c.ID == "worker.qa.state" && positiveEvidence(c, "recent_sample_failures"):
		return "近期 QA 构建采样出现失败"
	case c.ID == "worker.predictor.sources" && c.Status == Critical:
		return "预测来源准入不完整"
	case c.ID == "worker.predictor.schedule":
		return "输入准入时间窗与全天预期需要核对"
	case c.ID == "worker.predictor.execution" && c.Status != Healthy:
		return "预测执行出现失败、重试或超时"
	case c.ID == "host.service" && c.Status == Critical:
		return impactSubject(cardIssue{alerts: []Alert{{Check: c}}}) + "已停止或未加载"
	}
	return ""
}

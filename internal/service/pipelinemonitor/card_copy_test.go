package pipelinemonitor

import (
	"strings"
	"testing"
)

func TestKnownFamiliesShowOperationalImpactAndChineseEvidence(t *testing.T) {
	cases := []struct {
		id, scope string
		e         map[string]string
		want      []string
		recovery  string
	}{
		{"publication.schedule", "POLYMARKET", map[string]string{"last_published_at": "2026-10-30T02:00:00Z", "maximum_age": "4h"}, []string{"Polymarket", "最近发布", "4小时"}, "发布水位"},
		{"publication.dispatch", "KALSHI", map[string]string{"expected_members": "89", "dispatched_members": "0"}, []string{"Kalshi", "应预测市场 89", "已登记市场 0"}, "任务登记"},
		{"publication.outbox", "prediction-infra", map[string]string{"count": "9", "oldest_created_at": "2026-10-30T05:30:00Z"}, []string{"数据中台", "采样未确认发布 9", "1小时"}, "采样"},
		{"sandbox.original", "POLYMARKET/model", map[string]string{"pending_stage_tasks": "7", "oldest_task_age": "1h40m", "earliest_input_deadline": "2026-10-30T07:00:00Z"}, []string{"Polymarket", "本阶段待办 7", "1小时40分", "输入截止"}, "原始包阶段"},
		{"sandbox.masked", "KALSHI/model", map[string]string{"pending_stage_tasks": "8", "eta_status": "estimated", "estimated_finish_at": "2026-10-30T07:30:00Z", "earliest_input_deadline": "2026-10-30T07:00:00Z"}, []string{"Kalshi", "预计完成", "15:30", "输入截止"}, "脱敏交付"},
		{"sandbox.ready", "POLYMARKET/model", map[string]string{"pending_stage_tasks": "2", "oldest_task_age": "2h"}, []string{"回传任务通知", "本阶段待办 2"}, "Ready"},
		{"sandbox.result", "POLYMARKET/model", map[string]string{"pending_stage_tasks": "3", "oldest_task_age": "90m"}, []string{"模型结果", "本阶段待办 3"}, "回传阶段"},
		{"direct.result", "KALSHI/echo", map[string]string{"pending_tasks": "4", "expected_tasks": "12"}, []string{"直接预测", "待回传任务 4", "观察窗期望任务 12"}, "观察窗口"},
		{"queue.consumer", "prediction_pm_sandboxbuild/group", map[string]string{"lag": "7", "pending": "100", "consumers": "0"}, []string{"构建队列", "待领取 7", "待确认 100", "登记消费者 0"}, "ACK"},
		{"account.authorization", "kalshi:main", nil, []string{"Kalshi 主账户的新订单", "核对账户绑定、风控政策和控制项"}, "授权与控制"},
		{"account.cash", "wallet-6", map[string]string{"available_cash": "0", "reserved_cash": "12.5"}, []string{"钱包 6", "可用资金 0", "预占资金 12.5"}, "可用新买入资金"},
		{"account.risk_state", "wallet-6", map[string]string{"age": "12m", "max_state_age": "10m", "last_completed_at": "2026-10-30T06:18:00Z"}, []string{"钱包 6", "12分", "10分", "最近成功"}, "政策有效期"},
		{"account.reconciliation_issues", "wallet-6", map[string]string{"sampled_account_issues": "7", "sample_issue_id": "long-id-hidden"}, []string{"账户级", "采样账户级问题 7"}, "本次采样"},
		{"market.reconciliation_issues", "wallet-7", map[string]string{"sampled_scoped_issues": "11", "sample_issue_type": "POSITION_DRIFT", "sample_token_id": "long-token-hidden"}, []string{"市场或代币", "采样市场级问题 11", "持仓差异"}, "本次采样"},
		{"account.reconciliation", "POLYMARKET/main/model/strategy", nil, []string{"主钱包", "新订单", "查看该账户最新对账结果"}, "本次对账"},
		{"account.heartbeat", "POLYMARKET/main/model/strategy", nil, []string{"主钱包", "心跳", "撤单和查询"}, "心跳就绪"},
		{"decision.schedule", "engine", map[string]string{"last_started_at": "2026-10-30T00:21:00Z", "next_due_at": "2026-10-30T06:30:15Z", "skipped_boundaries": "2"}, []string{"08:21", "本进程累计跳过 2", "本轮决策"}, "已错过的周期"},
		{"strategy.progress", "wallet-6/model/strategy", map[string]string{"decision_at": "2026-10-30T00:20:00Z", "missing_or_error_books": "13", "cycle_id": "long-cycle-hidden"}, []string{"钱包 6", "对应市场", "异常盘口份数 13", "08:20"}, "当前策略周期"},
		{"intent.delivery", "wallet-6", map[string]string{"stalled_sample_rows": "7", "oldest_observed_at": "2026-10-30T06:05:00Z", "sample_state": "UNKNOWN", "sample_id": "long-order-hidden"}, []string{"钱包 6", "采样超时项 7", "25分", "结果未知", "不能直接重投"}, "本次投递采样"},
		{"order.recovery", "wallet-7", map[string]string{"stalled_sample_rows": "5", "sample_state": "MANUAL_REVIEW"}, []string{"钱包 7", "采样超时项 5", "待人工核查"}, "本次订单恢复采样"},
		{"fill.application", "wallet-6", map[string]string{"stalled_sample_rows": "4", "sample_state": "CONFIRMED"}, []string{"钱包 6", "已确认成交", "采样超时项 4", "已确认待应用"}, "本次采样"},
		{"venue.execution_errors", "kalshi:main", map[string]string{"sampled_orders": "7", "window": "15m", "sample_failure_code": "KALSHI_AUTH_FAILED", "last_failure_at": "2026-10-30T06:25:00Z"}, []string{"Kalshi 主账户", "最新异常样本", "近15分钟采样异常订单 7", "最新样本 认证失败"}, "阈值内"},
		{"worker.predictor.execution", "liang/live", map[string]string{"recent_failures": "2", "recent_error_retries": "3", "over_budget_running": "4"}, []string{"本批概率", "近期失败 2", "异常重试 3", "超预算在途 4"}, "本次执行采样"},
		{"worker.predictor.inputs", "liang/live", map[string]string{"expired_inputs": "6", "recent_rows": "30"}, []string{"将被跳过", "近期采样过期输入 6", "近期采样输入 30"}, "近期输入采样"},
		{"worker.predictor.sources", "liang/live", map[string]string{"allowed": "polymarket", "expected": "polymarket,kalshi"}, []string{"不会产生本轮结果", "已允许 Polymarket", "应包含 Polymarket、Kalshi"}, "来源准入"},
		{"worker.predictor.schedule", "liang/live", map[string]string{"t0_window_bjt": "00:05-01:00", "max_input_age_hours": "2", "trading_lookback": "3h"}, []string{"指定 t0", "北京时间窗 00:05-01:00", "输入有效期 2小时", "交易窗口 3小时"}, "准入时间窗"},
		{"worker.predictor.process", "liang/live", map[string]string{"instances": "0"}, []string{"李昂预测", "下一批概率", "进程实例 0"}, "预测进程"},
		{"worker.uploader.state", "liang/live", map[string]string{"recent_errors": "3", "sampled_rows": "1000"}, []string{"回传阻塞", "近期采样回传异常 3", "采样结果 1000"}, "本次采样"},
		{"worker.builder.state", "mjj/builder", map[string]string{"recent_sample_failures": "2", "sampled_rows": "1000"}, []string{"构建采样出现失败", "下一批预测输入", "近期采样失败 2"}, "本次构建采样"},
		{"worker.qa.state", "mjj/qa", map[string]string{"recent_sample_failures": "2"}, []string{"QA 构建采样出现失败", "QA 包更新", "近期采样失败 2"}, "本次 QA"},
		{"host.service", "216/pm-dataset-build-worker.service", map[string]string{"ActiveState": "inactive", "NRestarts": "2"}, []string{"Sandbox 构建服务", "已停止", "重启计数 2"}, "预期运行状态"},
		{"host.memory", "host", map[string]string{"used_percent": "91.50", "available_kib": "8388608"}, []string{"可用内存", "使用率 91.50%", "可用内存 8.0 GiB"}, "本次可用内存采样"},
		{"host.clock_sync", "host", map[string]string{"ntp_synchronized": "no", "ntp_enabled": "yes"}, []string{"时间同步 未同步", "同步服务 已启用"}, "重新报告时间同步"},
		{"host.restart_storm", "216/pm-dataset-build-worker.service", map[string]string{"restarts_last_5m": "5"}, []string{"Sandbox 构建服务", "反复重启", "近5分钟重启 5"}, "本次窗口"},
		{"monitor.delivery", "prediction-infra", map[string]string{"last_failure_at": "2026-10-30T06:25:00Z", "last_success_at": "2026-10-30T06:20:00Z"}, []string{"数据中台", "通知通道", "最近发送失败", "最近送达"}, "最近一次飞书通知"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			c := Check{ID: tc.id, Scope: tc.scope, Status: Critical, Summary: "该项异常", Evidence: tc.e}
			card, err := RenderCard("trading-execution", cardTestAt, []Alert{{Check: c}})
			if err != nil {
				t.Fatal(err)
			}
			front := frontText(t, card)
			for _, want := range tc.want {
				if !strings.Contains(front, want) {
					t.Fatalf("missing precise foreground %q: %s", want, front)
				}
			}
			for _, hidden := range []string{"long-id-hidden", "long-token-hidden", "long-order-hidden", "long-cycle-hidden", "sampled_rows", "source_model", "sample_failure_code"} {
				if strings.Contains(front, hidden) {
					t.Fatal("technical evidence leaked into foreground", hidden)
				}
			}
			c.Status = Healthy
			recovered, err := RenderCard("trading-execution", cardTestAt, []Alert{{Check: c, Recovery: true}})
			if err != nil {
				t.Fatal(err)
			}
			if text := frontText(t, recovered); !strings.Contains(text, tc.recovery) {
				t.Fatalf("recovery lost actual scope %q: %s", tc.recovery, text)
			}
		})
	}
}

func TestUnknownFamiliesDoNotClaimConfirmedInterruption(t *testing.T) {
	for _, id := range []string{"prediction.observation", "trading.database", "account.risk_state", "queue.observation", "worker.predictor.state", "host.filesystem", "monitor.configuration"} {
		c := Check{ID: id, Scope: "engine", Status: Unknown, Summary: "观测不可读"}
		card, err := RenderCard("trading-execution", cardTestAt, []Alert{{Check: c}})
		if err != nil {
			t.Fatal(err)
		}
		front := frontText(t, card)
		if strings.Contains(front, "当前无法新开仓") || strings.Contains(front, "新订单会被") || strings.Contains(front, "订单已经失败") {
			t.Fatal("unknown became business-failure assertion", front)
		}
		if !strings.Contains(front, "建议：") {
			t.Fatal("unknown with no evidence omitted useful observation action", front)
		}
	}
}

func TestVenueRecoveryPreservesBelowThresholdErrorsAndCashZeroIsMeaningful(t *testing.T) {
	c := Check{ID: "venue.execution_errors", Scope: "main", Status: Healthy, Summary: "近期执行未发现具名交易所基础故障", Evidence: map[string]string{"sampled_orders": "2", "sample_failure_code": "CLOB_SERVER_ERROR"}}
	card, err := RenderCard("trading-execution", cardTestAt, []Alert{{Check: c, Recovery: true}})
	if err != nil {
		t.Fatal(err)
	}
	front := frontText(t, card)
	if !strings.Contains(front, "阈值内") || !strings.Contains(front, "异常订单 2") || strings.Contains(front, "错误已清零") {
		t.Fatal("recovery misrepresented below-threshold sampling", front)
	}
}

func TestUnknownRedisLagSentinelIsNotShownAsNegativeTaskCount(t *testing.T) {
	c := Check{ID: "queue.consumer", Scope: "prediction_pm_sandboxbuild/group", Status: Unknown, Summary: "Redis 无法计算队列 lag", Evidence: map[string]string{"lag": "-1", "pending": "0"}}
	card, err := RenderCard("prediction-infra", cardTestAt, []Alert{{Check: c}})
	if err != nil {
		t.Fatal(err)
	}
	front := frontText(t, card)
	if strings.Contains(front, "待领取 -1") {
		t.Fatal("unknown Redis lag became a negative task count", front)
	}
}

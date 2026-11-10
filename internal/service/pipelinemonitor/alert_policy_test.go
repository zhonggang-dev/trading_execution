package pipelinemonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReminderBackoffSurvivesReloadAndFailedDelivery(t *testing.T) {
	now := cardTestAt
	calls, reject := 0, false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if reject {
			io.WriteString(w, `{"code":19000}`)
		} else {
			io.WriteString(w, `{"code":0}`)
		}
	}))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) { return nil, nil }))
	c := Check{ID: "sandbox.result", Scope: "KALSHI/Politics/model", Status: Warning, Summary: "结果尚未交付", Evidence: map[string]string{"business_date": "2026-10-30", "domain": "Politics", "round_index": "2"}}
	key := "test\x00" + c.ID + "\x00" + c.Scope
	send := func() { s.notify(context.Background(), Snapshot{SampledAt: now, Checks: []Check{c}}) }
	send()
	now = now.Add(cfg.Cooldown)
	send()
	if calls != 2 || s.states[key].RepeatCount != 2 {
		t.Fatal("first reminder did not use base cooldown", calls, s.states[key])
	}
	s = testService(t, cfg, &now, s.collector)
	now = now.Add(cfg.Cooldown)
	send()
	if calls != 2 {
		t.Fatal("restart lost longer reminder interval", calls)
	}
	now = now.Add(3 * cfg.Cooldown)
	send()
	if calls != 3 || s.states[key].RepeatCount != 3 {
		t.Fatal("second reminder did not use 4x cooldown", calls, s.states[key])
	}
	now = now.Add(15 * cfg.Cooldown)
	send()
	if calls != 3 {
		t.Fatal("reminder did not wait 16x cooldown", calls)
	}
	now = now.Add(cfg.Cooldown)
	reject = true
	before := s.states[key]
	send()
	if calls != 4 || s.states[key] != before {
		t.Fatal("failed reminder advanced successful state", calls, s.states[key])
	}
	reject = false
	send()
	if calls != 5 || s.states[key].RepeatCount != 3 {
		t.Fatal("failed reminder did not retry or backoff grew without bound", calls, s.states[key])
	}
	c.Status = Critical
	send()
	if calls != 6 || s.states[key].RepeatCount != 1 {
		t.Fatal("escalation did not immediately reset reminder interval", calls)
	}
	c.Status = Suppressed
	send()
	if calls != 6 || !s.states[key].Active {
		t.Fatal("suppressed invented recovery")
	}
	c.Status = Healthy
	send()
	send()
	if calls != 7 || s.states[key].Active {
		t.Fatal("recovery did not send once", calls)
	}
}

func TestDailyRoundIdentityStartsNewIncidentButCountsAndForcedRerunDoNot(t *testing.T) {
	now := cardTestAt
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; io.WriteString(w, `{"code":0}`) }))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) { return nil, nil }))
	c := Check{ID: "sandbox.masked", Scope: "POLYMARKET/World/Geopolitics/model/with/slash", Status: Critical, Evidence: map[string]string{"business_date": "2026-10-30", "domain": "World/Geopolitics", "round_index": "1", "generation_id": "3", "pending_stage_tasks": "90"}}
	send := func() { s.notify(context.Background(), Snapshot{SampledAt: now, Checks: []Check{c}}) }
	send()
	c.Evidence["pending_stage_tasks"] = "89"
	c.Evidence["generation_id"] = "4"
	c.Evidence["cohort_generations"] = "3,4"
	c.Evidence["estimated_finish_at"] = "2026-10-30T09:00:00Z"
	now = now.Add(time.Minute)
	send()
	if calls != 1 {
		t.Fatal("moving metrics or forced rerun caused new incident", calls)
	}
	for i := 1; i <= 8; i++ {
		c.Evidence["business_date"] = fmt.Sprintf("2026-11-%02d", i)
		now = now.Add(time.Minute)
		send()
	}
	if calls != 9 || len(s.states) != 1 {
		t.Fatal("new daily batch was muted or state grew per day", calls, len(s.states))
	}
	for _, state := range s.states {
		if state.RepeatCount != 1 || len(state.IncidentKey) != 64 {
			t.Fatal("new incident did not reset bounded state", state)
		}
	}
}

func TestLegacyAlertStateKeepsBaseCooldownUntilSuccessfulReminder(t *testing.T) {
	now := cardTestAt
	cfg := testConfig(t)
	key := "test\x00legacy\x00source"
	legacy := map[string]any{key: map[string]any{"level": Warning, "sent_at": now.Format(time.RFC3339Nano), "active": true}}
	data, _ := json.Marshal(legacy)
	if err := os.WriteFile(cfg.StatePath, data, 0600); err != nil {
		t.Fatal(err)
	}
	states, err := readState(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	old := states[key]
	if reminderCooldown(cfg.Cooldown, old.RepeatCount) != cfg.Cooldown {
		t.Fatal("legacy state was immediately given longer cooldown")
	}
	next := deliveredState(old, Check{ID: "legacy", Scope: "source", Status: Warning}, now.Add(cfg.Cooldown), false)
	if next.RepeatCount != 2 || reminderCooldown(cfg.Cooldown, next.RepeatCount) != 4*cfg.Cooldown {
		t.Fatal("legacy reminder did not adopt bounded backoff", next)
	}
	if reminderCooldown(time.Duration(1<<62), 3) <= 0 {
		t.Fatal("large configured cooldown overflowed")
	}
}

func TestLiveFailuresAreSelectedAndShownBeforeIndependentQAWork(t *testing.T) {
	now := cardTestAt
	var delivered map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p struct {
			Card map[string]any `json:"card"`
		}
		json.NewDecoder(r.Body).Decode(&p)
		delivered = p.Card
		io.WriteString(w, `{"code":0}`)
	}))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) { return nil, nil }))
	checks := make([]Check, 0, 15)
	for i := 0; i < 13; i++ {
		checks = append(checks, Check{ID: "host.service", Scope: fmt.Sprintf("216/pm-dataset-sanitize-worker@%d.service", i), Status: Warning, Summary: "旁路清洗实例停止"})
	}
	checks = append(checks, Check{ID: "intent.delivery", Scope: "main", Status: Warning, Summary: "真实订单待确认"}, Check{ID: "host.monitor", Scope: "prediction-infra", Status: Warning, Summary: "监控自身失效"})
	s.notify(context.Background(), Snapshot{SampledAt: now, Checks: checks})
	if len(s.states) != 12 || !s.states["test\x00intent.delivery\x00main"].Active || !s.states["test\x00host.monitor\x00prediction-infra"].Active {
		t.Fatal("side work crowded out live trading/monitoring failures", len(s.states))
	}
	front := frontText(t, delivered)
	if monitor, order, side := strings.Index(front, "监控自身失效"), strings.Index(front, "真实订单待确认"), strings.Index(front, "旁路清洗实例停止"); monitor < 0 || order <= monitor || side <= order {
		t.Fatal("card priority diverges from notification selection", front)
	}
	if !alertBefore(Check{ID: "worker.qa.state", Status: Critical}, Check{ID: "intent.delivery", Status: Warning}) {
		t.Fatal("business priority overrode severity")
	}
	if !reflect.DeepEqual(checks[0], Check{ID: "host.service", Scope: "216/pm-dataset-sanitize-worker@0.service", Status: Warning, Summary: "旁路清洗实例停止"}) {
		t.Fatal("selection mutated collector checks")
	}
}

func TestRoundCardHasUnambiguousVenueDomainBusinessDayAndModel(t *testing.T) {
	c := Check{ID: "sandbox.result", Scope: "POLYMARKET/World/Geopolitics/model/with/slash", Status: Warning, Summary: "概率回传尚未交付", Evidence: map[string]string{"source": "POLYMARKET", "domain": "World/Geopolitics", "business_date": "2026-11-10", "round_index": "1", "model": "model/with/slash", "pending_stage_tasks": "4"}}
	card, err := RenderCard("prediction-infra", cardTestAt, []Alert{{Check: c}})
	if err != nil {
		t.Fatal(err)
	}
	front := frontText(t, card)
	for _, want := range []string{"Polymarket · World/Geopolitics", "业务日 2026-11-10", "第1轮", "模型 model/with/slash", "本阶段待办 4"} {
		if !strings.Contains(front, want) {
			t.Fatal("missing round identity", want, front)
		}
	}
	c = Check{ID: "publication.schedule", Scope: "KALSHI/Politics", Status: Unknown, Summary: "本轮执行结果尚不可确认", Evidence: map[string]string{"source": "KALSHI", "domain": "Politics", "expected_business_date": "2026-11-10", "business_date": "2026-11-09", "round_index": "2", "scheduled_at": "2026-11-09T18:10:00Z", "deadline_at": "2026-11-09T18:40:00Z"}}
	card, err = RenderCard("prediction-infra", cardTestAt, []Alert{{Check: c}})
	if err != nil {
		t.Fatal(err)
	}
	front = frontText(t, card)
	for _, want := range []string{"Kalshi · Politics", "业务日 2026-11-10", "第2轮", "计划执行 11-10 02:10", "交付宽限至 11-10 02:40", "无法确认"} {
		if !strings.Contains(front, want) {
			t.Fatal("missing schedule identity or uncertainty", want, front)
		}
	}
	if strings.Contains(front, "已停止") || strings.Contains(front, "无法新开仓") {
		t.Fatal("unknown round was asserted to stop trading", front)
	}
}

func TestStaleWorkerObservationHighlightsLossOfFaultDetection(t *testing.T) {
	c := Check{ID: "worker.observation", Scope: "host", Status: Unknown, Summary: "Worker 采样已过时", Evidence: map[string]string{"sampled_at": cardTestAt.Add(-5 * 24 * time.Hour).Format(time.RFC3339)}}
	card, err := RenderCard("prediction-infra", cardTestAt, []Alert{{Check: c}})
	if err != nil {
		t.Fatal(err)
	}
	front := frontText(t, card)
	for _, want := range []string{"无法确认俊杰构建及李昂预测、回传服务的当前状态", "服务停止可能未被发现", "最近 Worker 采样", "已停更 120小时"} {
		if !strings.Contains(front, want) {
			t.Fatal("stale observation did not explain monitoring blindness", want, front)
		}
	}
	if strings.Contains(front, "服务已停止") {
		t.Fatal("stale sample was treated as current service failure", front)
	}
}

func TestRoundScheduleAndFailedExecutionLeadWithSpecificLane(t *testing.T) {
	c := Check{ID: "publication.schedule", Scope: "KALSHI/Politics", Status: Warning, Summary: "今日轮次尚未交付", Evidence: map[string]string{"source": "KALSHI", "domain": "Politics", "business_date": "2026-11-10", "round_index": "2"}}
	if title := problemTitle(c); title != "Kalshi · Politics｜今日轮次尚未交付" {
		t.Fatal("round title does not identify the missing lane", title)
	}
	c.ID = "worker.round.execution"
	c.Summary = "当日轮次明确执行记录"
	c.Evidence["outcome"] = "failed"
	c.Evidence["error_code"] = "UPSTREAM_RATE_LIMITED"
	c.Evidence["observed_at"] = cardTestAt.Format(time.RFC3339)
	card, err := RenderCard("prediction-infra", cardTestAt, []Alert{{Check: c}})
	if err != nil {
		t.Fatal(err)
	}
	front := frontText(t, card)
	for _, want := range []string{"Kalshi · Politics｜选盘上游请求被限流", "新一轮 Sandbox 与预测输入可能延迟", "失败原因 上游请求限流"} {
		if !strings.Contains(front, want) {
			t.Fatal("round execution failed without actionable focus", want, front)
		}
	}
	previous := alertIncident(c)
	c.Evidence["business_date"] = "2026-11-11"
	if previous == "" || previous == alertIncident(c) {
		t.Fatal("a fresh failed execution was hidden by previous day's cooldown")
	}
}

func TestMonitoringBlindnessOutranksSideWarningButNotConfirmedTradingFailure(t *testing.T) {
	blind := Check{ID: "worker.observation", Scope: "host", Status: Unknown, Summary: "Worker 采样失效"}
	side := Check{ID: "host.service", Scope: "216/pm-dataset-sanitize-worker@1.service", Status: Warning, Summary: "旁路清洗实例停止"}
	order := Check{ID: "order.recovery", Scope: "main", Status: Critical, Summary: "真实订单未完成确认"}
	if !alertBefore(blind, side) || alertBefore(side, blind) {
		t.Fatal("monitoring blindness hidden by side warning")
	}
	if !alertBefore(order, blind) || alertBefore(blind, order) {
		t.Fatal("unknown observation hid a critical trading failure")
	}
	known := Check{ID: "intent.delivery", Scope: "main", Status: Warning}
	if !alertBefore(known, blind) {
		t.Fatal("unknown observation hid a confirmed live warning")
	}
	side.Status = Critical
	if !alertBefore(side, blind) {
		t.Fatal("unknown observation overrode a critical status")
	}
	side.Status = Warning
	card, err := RenderCard("prediction-infra", cardTestAt, []Alert{{Check: side}, {Check: blind}, {Check: order}})
	if err != nil {
		t.Fatal(err)
	}
	front := frontText(t, card)
	if critical, observation, ancillary := strings.Index(front, order.Summary), strings.Index(front, blind.Summary), strings.Index(front, side.Summary); critical < 0 || observation <= critical || ancillary <= observation {
		t.Fatal("cross-severity selection and front-card focus differ", front)
	}
	all := []Check{blind, side, order, known, {ID: "host.disk", Status: Unknown}}
	for _, a := range all {
		for _, b := range all {
			for _, c := range all {
				if alertBefore(a, b) && alertBefore(b, c) && !alertBefore(a, c) {
					t.Fatal("non-transitive priority order", a, b, c)
				}
			}
		}
	}
}

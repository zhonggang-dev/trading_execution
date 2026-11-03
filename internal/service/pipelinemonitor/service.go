package pipelinemonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type BuildParams struct {
	Config     Config
	Component  string
	Collector  Collector
	HTTPClient *http.Client
	Now        func() time.Time
	Logger     *slog.Logger
}
type alertState struct {
	Level  string    `json:"level"`
	SentAt time.Time `json:"sent_at"`
	Active bool      `json:"active"`
}
type Service struct {
	config              Config
	component           string
	collector           Collector
	notifier            notifier
	now                 func() time.Time
	logger              *slog.Logger
	mu                  sync.RWMutex
	snapshot            Snapshot
	states              map[string]alertState
	stateError          bool
	persistError        bool
	lastDeliverySuccess time.Time
	lastDeliveryFailure time.Time
	deliveryFailure     string
	refreshMu           sync.Mutex
	lifecycleMu         sync.Mutex
	started             bool
	cancel              context.CancelFunc
	done                chan struct{}
}

func Build(p BuildParams) (*Service, error) {
	if err := p.Config.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(p.Component) == "" || p.Collector == nil {
		return nil, fmt.Errorf("pipeline monitor component and collector are required")
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.HTTPClient == nil {
		p.HTTPClient = &http.Client{Timeout: p.Config.HTTPTimeout}
	}
	s := &Service{config: p.Config, component: p.Component, collector: p.Collector, now: p.Now, logger: p.Logger, states: map[string]alertState{}, notifier: notifier{config: p.Config, client: p.HTTPClient}}
	s.snapshot = Snapshot{SchemaVersion: "trading.pipeline_monitor.v1", Component: p.Component, Checks: []Check{{ID: "monitor.lifecycle", Scope: p.Component, Status: Suppressed, Summary: "链路监控未启用"}}}
	if p.Config.Enabled {
		s.snapshot.Checks[0].Status = Unknown
		s.snapshot.Checks[0].Summary = "等待首次链路监控采样"
		s.snapshot.Stale = true
	}
	if p.Config.StatePath != "" {
		states, err := readState(p.Config.StatePath)
		if err != nil {
			s.stateError = true
		} else {
			s.states = states
		}
	}
	return s, nil
}

// ReportProblem exposes bootstrap-only monitor errors without affecting business readiness.
func (s *Service) ReportProblem(c Check) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Checks = []Check{c}
}

func (s *Service) Start() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.started || !s.config.Enabled {
		return nil
	}
	s.started = true
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		s.Refresh(ctx)
		ticker := time.NewTicker(s.config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.Refresh(ctx)
			}
		}
	}()
	return nil
}
func (s *Service) Stop(ctx context.Context) error {
	s.lifecycleMu.Lock()
	cancel, done := s.cancel, s.done
	s.lifecycleMu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Service) Snapshot(context.Context) (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := clone(s.snapshot)
	if s.config.Enabled && (out.SampledAt.IsZero() || s.now().UTC().Sub(out.SampledAt) > 2*s.config.Interval) {
		out.Stale = true
	}
	return out, nil
}
func (s *Service) Refresh(ctx context.Context) (Snapshot, error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if !s.config.Enabled {
		return s.Snapshot(ctx)
	}
	if s.stateError {
		if states, err := readState(s.config.StatePath); err == nil {
			s.states = states
			s.stateError = false
		}
	}
	if s.persistError && s.persist() == nil {
		s.persistError = false
	}
	now := s.now().UTC()
	sampleCtx, cancel := context.WithTimeout(ctx, s.config.CollectTimeout)
	checks, err := s.collector.Collect(sampleCtx, now)
	cancel()
	if err != nil {
		checks = append(checks, Check{ID: "monitor.collection", Scope: s.component, Status: Unknown, Summary: "链路观测读取失败", Details: "部分业务状态未知；未用零值替代读取失败"})
	}
	if len(checks) == 0 {
		checks = []Check{{ID: "monitor.collection", Scope: s.component, Status: Unknown, Summary: "链路采集未返回观测"}}
	}
	checks = append(checks, s.deliveryCheck())
	if s.stateError {
		checks = append(checks, Check{ID: "monitor.state", Scope: s.component, Status: Unknown, Summary: "告警状态文件不可用", Details: "未丢弃未知历史去重状态；请修复文件权限或损坏内容"})
	}
	if s.persistError {
		checks = append(checks, Check{ID: "monitor.persistence", Scope: s.component, Status: Unknown, Summary: "告警去重状态持久化失败", Details: "本进程仍保留发送记录；重启后可能重复提醒"})
	}
	seen := map[string]bool{}
	for i := range checks {
		c := &checks[i]
		key := c.ID + "\x00" + c.Scope
		if c.ID == "" || c.Scope == "" || !ValidStatus(c.Status) || seen[key] {
			checks = []Check{{ID: "monitor.collection", Scope: s.component, Status: Unknown, Summary: "链路观测格式不合法"}}
			break
		}
		seen[key] = true
	}
	sort.Slice(checks, func(i, j int) bool {
		if checks[i].ID == checks[j].ID {
			return checks[i].Scope < checks[j].Scope
		}
		return checks[i].ID < checks[j].ID
	})
	snapshot := Snapshot{SchemaVersion: "trading.pipeline_monitor.v1", Component: s.component, SampledAt: now, Checks: checks}
	s.mu.Lock()
	s.snapshot = clone(snapshot)
	s.mu.Unlock()
	s.notify(ctx, snapshot)
	finalSnapshot, _ := s.Snapshot(ctx)
	return finalSnapshot, err
}
func clone(s Snapshot) Snapshot {
	out := s
	out.Checks = make([]Check, len(s.Checks))
	for i, c := range s.Checks {
		out.Checks[i] = c
		if c.Evidence != nil {
			out.Checks[i].Evidence = make(map[string]string, len(c.Evidence))
			for k, v := range c.Evidence {
				out.Checks[i].Evidence[k] = v
			}
		}
	}
	return out
}
func rank(s string) int {
	switch s {
	case Critical:
		return 3
	case Warning:
		return 2
	case Unknown:
		return 1
	}
	return 0
}
func (s *Service) notify(ctx context.Context, snapshot Snapshot) {
	if !s.notifier.enabled() || s.stateError {
		return
	}
	type pending struct {
		key      string
		check    Check
		recovery bool
	}
	var selected []pending
	ordered := append([]Check(nil), snapshot.Checks...)
	sort.SliceStable(ordered, func(i, j int) bool { return rank(ordered[i].Status) > rank(ordered[j].Status) })
	for _, c := range ordered {
		key := s.component + "\x00" + c.ID + "\x00" + c.Scope
		old := s.states[key]
		recovery := c.Status == Healthy && old.Active
		if !recovery && (rank(c.Status) == 0 || (old.Active && rank(c.Status) <= rank(old.Level) && snapshot.SampledAt.Sub(old.SentAt) < s.config.Cooldown)) {
			continue
		}
		selected = append(selected, pending{key: key, check: c, recovery: recovery})
		if len(selected) == 12 {
			break
		}
	}
	if len(selected) == 0 {
		return
	}
	// Active faults and recoveries are independent deliveries. Persist accepted
	// Checks before sending the next card so a later failure cannot acknowledge it.
	var deliveryError error
	for _, recovery := range []bool{false, true} {
		var alerts []Alert
		var group []pending
		for _, item := range selected {
			if item.recovery == recovery {
				alerts = append(alerts, Alert{Check: item.check, Recovery: recovery})
				group = append(group, item)
			}
		}
		if len(group) == 0 {
			continue
		}
		card, err := RenderCard(s.component, snapshot.SampledAt, alerts)
		if err == nil {
			sendCtx, cancel := context.WithTimeout(ctx, s.config.HTTPTimeout)
			err = s.notifier.sendCard(sendCtx, card, snapshot.SampledAt)
			cancel()
		}
		if err != nil {
			deliveryError = err
			s.deliveryFailure = err.Error()
			s.lastDeliveryFailure = snapshot.SampledAt
			s.logger.Warn("pipeline monitor notification failed", "error", err)
			continue
		}
		s.lastDeliverySuccess = snapshot.SampledAt
		for _, item := range group {
			s.states[item.key] = alertState{Level: item.check.Status, SentAt: snapshot.SampledAt, Active: !item.recovery}
		}
		if err := s.persist(); err != nil {
			s.persistError = true
			s.logger.Error("pipeline monitor state persistence failed", "error", err)
			s.mu.Lock()
			s.snapshot.Checks = append(s.snapshot.Checks, Check{ID: "monitor.persistence", Scope: s.component, Status: Unknown, Summary: "告警去重状态持久化失败", Details: "本进程仍保留发送记录；重启后可能重复提醒"})
			s.mu.Unlock()
		}
	}
	if deliveryError == nil {
		s.deliveryFailure = ""
	}
	s.updateDeliveryObservation()
}
func readState(path string) (map[string]alertState, error) {
	states := map[string]alertState{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return states, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pipeline monitor state")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (2<<20)+1))
	if err != nil || len(data) > 2<<20 || json.Unmarshal(data, &states) != nil || states == nil {
		return nil, fmt.Errorf("invalid pipeline monitor state")
	}
	return states, nil
}
func (s *Service) persist() error {
	if s.config.StatePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.config.StatePath), 0700); err != nil {
		return fmt.Errorf("create pipeline monitor state directory")
	}
	f, err := os.CreateTemp(filepath.Dir(s.config.StatePath), ".pipeline-monitor-*")
	if err != nil {
		return fmt.Errorf("create pipeline monitor state")
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		err = json.NewEncoder(f).Encode(s.states)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write pipeline monitor state")
	}
	if err = os.Rename(name, s.config.StatePath); err != nil {
		return fmt.Errorf("replace pipeline monitor state")
	}
	return nil
}

// ReadWorkerSnapshot reads only a bounded local snapshot, never the private Worker databases.
func ReadWorkerSnapshot(path string, now time.Time, maxAge time.Duration) ([]Check, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read worker snapshot")
	}
	defer f.Close()
	var p struct {
		SchemaVersion string    `json:"schema_version"`
		SampledAt     time.Time `json:"sampled_at"`
		Checks        []Check   `json:"checks"`
	}
	data, err := io.ReadAll(io.LimitReader(f, (2<<20)+1))
	if err != nil || len(data) > 2<<20 || json.Unmarshal(data, &p) != nil || p.SchemaVersion != "trading.worker_snapshot.v1" || len(p.Checks) > 128 {
		return nil, fmt.Errorf("invalid worker snapshot")
	}
	if p.SampledAt.IsZero() || p.SampledAt.After(now.Add(time.Minute)) || now.Sub(p.SampledAt) > maxAge {
		return []Check{{ID: "worker.observation", Scope: "host", Status: Unknown, Summary: "Worker 采样已过时", Details: "旧快照不能代表当前服务和任务状态", Evidence: map[string]string{"sampled_at": p.SampledAt.Format(time.RFC3339)}}}, nil
	}
	seen := map[string]bool{}
	for _, c := range p.Checks {
		key := c.ID + "\x00" + c.Scope
		if c.ID == "" || c.Scope == "" || !ValidStatus(c.Status) || seen[key] {
			return nil, fmt.Errorf("invalid worker observation")
		}
		seen[key] = true
	}
	return p.Checks, nil
}

func (s *Service) deliveryCheck() Check {
	c := Check{ID: "monitor.delivery", Scope: s.component, Status: Suppressed, Summary: "告警通道已配置，尚无本进程成功送达记录", Details: "配置存在不等于消息已送达；等待首次实际通知验证", Evidence: map[string]string{"delivery_verified": "false"}}
	if !s.notifier.enabled() {
		c.Status = Unknown
		c.Summary = "链路告警发送通道未配置"
		c.Details = "只提供观测，不会发送飞书"
		return c
	}
	if !s.lastDeliverySuccess.IsZero() {
		c.Status = Healthy
		c.Summary = "最近一次飞书通知已成功送达"
		c.Details = "HTTP与飞书业务返回均成功；不是对未来送达的保证"
		c.Evidence["delivery_verified"] = "true"
		c.Evidence["last_success_at"] = s.lastDeliverySuccess.Format(time.RFC3339)
	}
	if !s.lastDeliveryFailure.IsZero() {
		c.Evidence["last_failure_at"] = s.lastDeliveryFailure.Format(time.RFC3339)
	}
	if s.deliveryFailure != "" {
		c.Status = Unknown
		c.Summary = "飞书通知发送失败"
		c.Details = s.deliveryFailure
		c.Evidence["delivery_verified"] = "false"
	}
	return c
}
func (s *Service) updateDeliveryObservation() {
	c := s.deliveryCheck()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, old := range s.snapshot.Checks {
		if old.ID == c.ID && old.Scope == c.Scope {
			s.snapshot.Checks[i] = c
			return
		}
	}
	s.snapshot.Checks = append(s.snapshot.Checks, c)
}

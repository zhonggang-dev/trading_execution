package pipelinemonitor

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	c, e := LoadConfig(func(string) string { return "" })
	if e != nil {
		t.Fatal(e)
	}
	c.Enabled = true
	c.StatePath = filepath.Join(t.TempDir(), "alerts.json")
	return c
}
func testService(t *testing.T, cfg Config, now *time.Time, collector Collector) *Service {
	t.Helper()
	s, e := Build(BuildParams{Config: cfg, Component: "test", Collector: collector, Now: func() time.Time { return *now }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestDurableCooldownEscalationRecoveryAndFailedSend(t *testing.T) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	status := Warning
	var calls atomic.Int64
	reject := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if reject {
			io.WriteString(w, `{"code":19000}`)
			return
		}
		io.WriteString(w, `{"code":0}`)
	}))
	defer server.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = server.URL
	collector := CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		return []Check{{ID: "work", Scope: "KALSHI/model", Status: status, Summary: "test"}}, nil
	})
	s := testService(t, cfg, &now, collector)
	refresh := func() {
		t.Helper()
		if _, e := s.Refresh(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	refresh()
	refresh()
	if calls.Load() != 1 {
		t.Fatalf("same-level cooldown calls=%d", calls.Load())
	}
	status = Critical
	refresh()
	if calls.Load() != 2 {
		t.Fatal("escalation not immediate")
	}
	s = testService(t, cfg, &now, collector)
	refresh()
	if calls.Load() != 2 {
		t.Fatal("restart lost durable cooldown")
	}
	status = Healthy
	refresh()
	refresh()
	if calls.Load() != 3 {
		t.Fatal("recovery should send exactly once")
	}
	status = Warning
	reject = true
	refresh()
	reject = false
	refresh()
	if calls.Load() != 5 {
		t.Fatal("failed notification advanced successful cooldown")
	}
	now = now.Add(cfg.Cooldown)
	refresh()
	if calls.Load() != 7 {
		t.Fatal("same fault did not remind after cooldown")
	}
	info, e := os.Stat(cfg.StatePath)
	if e != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("state file mode: %v %v", info, e)
	}
}
func TestNeverAlertedHealthyAndReadFailureDoNotInventRecovery(t *testing.T) {
	now := time.Now()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `{"code":0}`) }))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	fail := false
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		if fail {
			return nil, errors.New("sensitive DSN must not be surfaced")
		}
		return []Check{{ID: "normal", Scope: "POLYMARKET", Status: Suppressed, Summary: "本轮没有应预测任务"}}, nil
	}))
	s.Refresh(context.Background())
	if calls.Load() != 0 {
		t.Fatal("no workload generated a recovery notification")
	}
	fail = true
	snap, e := s.Refresh(context.Background())
	collectionUnknown := false
	for _, c := range snap.Checks {
		if c.ID == "monitor.collection" && c.Status == Unknown {
			collectionUnknown = true
		}
	}
	if e == nil || !collectionUnknown {
		t.Fatalf("error should be unknown: %+v %v", snap, e)
	}
	data, _ := json.Marshal(snap)
	if strings.Contains(string(data), "sensitive") {
		t.Fatal("raw collection error escaped to diagnostics")
	}
}

func TestDeliveryFailureIsVisibleAndOnlyActualSuccessClearsIt(t *testing.T) {
	now := time.Now()
	fail := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			io.WriteString(w, `{"code":19000}`)
		} else {
			io.WriteString(w, `{"code":0}`)
		}
	}))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		return []Check{{ID: "business", Scope: "source", Status: Critical}}, nil
	}))
	get := func(snap Snapshot) Check {
		for _, c := range snap.Checks {
			if c.ID == "monitor.delivery" {
				return c
			}
		}
		t.Fatal("delivery diagnostics missing")
		return Check{}
	}
	initial, _ := s.Snapshot(context.Background())
	_ = initial
	snap, _ := s.Refresh(context.Background())
	d := get(snap)
	if d.Status != Unknown || d.Evidence["delivery_verified"] != "false" || d.Evidence["last_failure_at"] == "" {
		t.Fatal("delivery failure hidden", d)
	}
	fail = false
	now = now.Add(time.Minute)
	snap, _ = s.Refresh(context.Background())
	d = get(snap)
	if d.Status != Healthy || d.Evidence["delivery_verified"] != "true" || d.Evidence["last_success_at"] == "" {
		t.Fatal("successful notification did not clear failure", d)
	}
}
func TestWebhookSignatureAndBusinessResponseValidation(t *testing.T) {
	now := time.Unix(1793318400, 0)
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		io.WriteString(w, `{"code":0}`)
	}))
	defer srv.Close()
	n := notifier{config: Config{WebhookURL: srv.URL, Secret: "test-secret"}, client: srv.Client()}
	if e := n.sendCard(context.Background(), testAlertCard(t, now), now); e != nil {
		t.Fatal(e)
	}
	mac := hmac.New(sha256.New, []byte("1793318400\ntest-secret"))
	want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if got["sign"] != want {
		t.Fatalf("signature=%v", got["sign"])
	}
	if got["msg_type"] != "interactive" || got["card"].(map[string]any)["schema"] != "2.0" {
		t.Fatal("webhook is not sending an interactive Card 2.0")
	}
	for _, body := range []string{`{}`, `{"code":1}`, `not-json`} {
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
		n.config.WebhookURL = srv2.URL
		if e := n.sendCard(context.Background(), testAlertCard(t, now), now); e == nil {
			t.Fatalf("accepted invalid response %s", body)
		}
		srv2.Close()
	}
}
func TestAppBotCachesTokenAndUsesChatIdentity(t *testing.T) {
	var tokens, messages atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "tenant_access_token") {
			tokens.Add(1)
			io.WriteString(w, `{"code":0,"tenant_access_token":"fake-token","expire":7200}`)
			return
		}
		messages.Add(1)
		if r.Header.Get("Authorization") != "Bearer fake-token" || r.URL.Query().Get("receive_id_type") != "chat_id" {
			t.Error("incorrect bot auth/routing")
		}
		var p map[string]string
		json.NewDecoder(r.Body).Decode(&p)
		if p["receive_id"] != "oc_test" {
			t.Error("incorrect chat")
		}
		var card map[string]any
		if p["msg_type"] != "interactive" || json.Unmarshal([]byte(p["content"]), &card) != nil || card["schema"] != "2.0" {
			t.Error("app bot content is not interactive Card 2.0 JSON")
		}
		io.WriteString(w, `{"code":0}`)
	}))
	defer srv.Close()
	n := notifier{config: Config{AppID: "cli_test", AppSecret: "test", ChatID: "oc_test"}, client: srv.Client(), apiBase: srv.URL}
	for range 2 {
		if e := n.sendCard(context.Background(), testAlertCard(t, time.Now()), time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	if tokens.Load() != 1 || messages.Load() != 2 {
		t.Fatalf("token/messages=%d/%d", tokens.Load(), messages.Load())
	}
}
func TestMissingTransportCorruptStateAndSnapshotStaleness(t *testing.T) {
	now := time.Now()
	cfg := testConfig(t)
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		return []Check{{ID: "db", Scope: "source", Status: Healthy, Summary: "ok"}}, nil
	}))
	snap, _ := s.Refresh(context.Background())
	if len(snap.Checks) != 2 || snap.Checks[1].Status != Unknown {
		t.Fatalf("unconfigured delivery hidden: %+v", snap)
	}
	now = now.Add(3 * cfg.Interval)
	snap, _ = s.Snapshot(context.Background())
	if !snap.Stale {
		t.Fatal("stale snapshot marked fresh")
	}
	if e := os.WriteFile(cfg.StatePath, []byte("broken"), 0600); e != nil {
		t.Fatal(e)
	}
	s = testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		return []Check{{ID: "db", Scope: "source", Status: Healthy}}, nil
	}))
	snap, _ = s.Refresh(context.Background())
	found := false
	for _, c := range snap.Checks {
		if c.ID == "monitor.state" && c.Status == Unknown {
			found = true
		}
	}
	if !found {
		t.Fatal("corrupt alert state hidden")
	}
}
func TestWorkerSnapshotIsBoundedFreshAndImmutable(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "worker.json")
	write := func(sample time.Time, version string) {
		t.Helper()
		p := map[string]any{"schema_version": version, "sampled_at": sample, "checks": []Check{{ID: "worker", Scope: "build", Status: Healthy}}}
		b, _ := json.Marshal(p)
		if e := os.WriteFile(path, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	write(now, "trading.worker_snapshot.v1")
	checks, e := ReadWorkerSnapshot(path, now, 2*time.Minute)
	if e != nil || len(checks) != 1 || checks[0].Status != Healthy {
		t.Fatal(checks, e)
	}
	write(now.Add(-3*time.Minute), "trading.worker_snapshot.v1")
	checks, e = ReadWorkerSnapshot(path, now, 2*time.Minute)
	if e != nil || checks[0].Status != Unknown {
		t.Fatal("stale worker accepted")
	}
	write(now, "unrecognised")
	if _, e = ReadWorkerSnapshot(path, now, 2*time.Minute); e == nil {
		t.Fatal("unknown schema accepted")
	}
}
func TestLoadConfigValidation(t *testing.T) {
	for _, env := range []map[string]string{{"PIPELINE_MONITOR_ENABLED": "garbage"}, {"PIPELINE_MONITOR_INTERVAL": "1ms"}, {"PIPELINE_MONITOR_FEISHU_APP_ID": "only-one-field"}, {"PIPELINE_MONITOR_ENABLED": "true", "PIPELINE_MONITOR_FEISHU_WEBHOOK_URL": "https://example.invalid"}} {
		if _, e := LoadConfig(func(k string) string { return env[k] }); e == nil {
			t.Fatalf("invalid config accepted: %v", env)
		}
	}
}

func TestSuppressedIsNotRecoveryAndBatchDoesNotFlood(t *testing.T) {
	now := time.Now()
	var calls atomic.Int64
	var message string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var p struct {
			Card json.RawMessage `json:"card"`
		}
		json.NewDecoder(r.Body).Decode(&p)
		message = string(p.Card)
		io.WriteString(w, `{"code":0}`)
	}))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	status := Critical
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		return []Check{{ID: "builder", Scope: "host", Status: status, Summary: "Builder停止"}, {ID: "forecast", Scope: "model", Status: status, Summary: "概率缺失"}}, nil
	}))
	s.Refresh(context.Background())
	if calls.Load() != 1 || !strings.Contains(message, "Builder停止") || !strings.Contains(message, "概率缺失") {
		t.Fatal("same sample should batch causal impact into one message")
	}
	status = Suppressed
	s.Refresh(context.Background())
	if calls.Load() != 1 {
		t.Fatal("planned suppression incorrectly announced recovery")
	}
	status = Healthy
	s.Refresh(context.Background())
	if calls.Load() != 2 {
		t.Fatal("explicit healthy recovery missing")
	}
}

func TestStateRepairDoesNotRequireBusinessRestart(t *testing.T) {
	now := time.Now()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `{"code":0}`) }))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	os.WriteFile(cfg.StatePath, []byte("broken"), 0600)
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		return []Check{{ID: "test", Scope: "host", Status: Critical, Summary: "stopped"}}, nil
	}))
	s.Refresh(context.Background())
	if calls.Load() != 0 {
		t.Fatal("corrupt durable state lost cooldown")
	}
	os.WriteFile(cfg.StatePath, []byte(`{}`), 0600)
	s.Refresh(context.Background())
	if calls.Load() != 1 {
		t.Fatal("repaired observer never resumed")
	}
}

func TestNotificationTimeoutDoesNotAdvanceState(t *testing.T) {
	now := time.Now()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		io.WriteString(w, `{"code":0}`)
	}))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	cfg.HTTPTimeout = 20 * time.Millisecond
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
		return []Check{{ID: "test", Scope: "host", Status: Critical}}, nil
	}))
	started := time.Now()
	s.Refresh(context.Background())
	if time.Since(started) > time.Second || len(s.states) != 0 {
		t.Fatal("timeout blocked monitor or advanced successful delivery")
	}
}

func testAlertCard(t *testing.T, at time.Time) map[string]any {
	t.Helper()
	card, err := RenderCard("test", at, []Alert{{Check: Check{ID: "test", Scope: "host", Status: Warning, Summary: "test"}}})
	if err != nil {
		t.Fatal(err)
	}
	return card
}

func TestPartialCardDeliveryOnlyAcknowledgesAndPersistsAcceptedLifecycle(t *testing.T) {
	for _, failAt := range []int64{1, 2} {
		t.Run(fmt.Sprintf("failed-card-%d", failAt), func(t *testing.T) {
			now := cardTestAt
			var calls atomic.Int64
			var colors []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var p struct {
					Type string         `json:"msg_type"`
					Card map[string]any `json:"card"`
				}
				json.NewDecoder(r.Body).Decode(&p)
				if p.Type != "interactive" {
					t.Error("non-card delivery")
				}
				colors = append(colors, p.Card["header"].(map[string]any)["template"].(string))
				if calls.Add(1) == failAt {
					io.WriteString(w, `{"code":19000}`)
				} else {
					io.WriteString(w, `{"code":0}`)
				}
			}))
			defer srv.Close()
			cfg := testConfig(t)
			cfg.WebhookURL = srv.URL
			s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) {
				return []Check{{ID: "new-fault", Scope: "source", Status: Critical, Summary: "新异常"}, {ID: "old-fault", Scope: "source", Status: Healthy, Summary: "旧异常恢复"}}, nil
			}))
			oldKey := "test\x00old-fault\x00source"
			newKey := "test\x00new-fault\x00source"
			old := alertState{Level: Critical, Active: true, SentAt: now.Add(-time.Hour)}
			s.states[oldKey] = old
			snap, err := s.Refresh(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 || !reflect.DeepEqual(colors, []string{"red", "green"}) {
				t.Fatal("active and recovery were not distinct cards", calls.Load(), colors)
			}
			disk, err := readState(cfg.StatePath)
			if err != nil {
				t.Fatal(err)
			}
			if failAt == 2 {
				if !disk[newKey].Active || !disk[newKey].SentAt.Equal(now) || disk[oldKey] != old {
					t.Fatal("failed recovery was acknowledged or accepted fault lost", disk)
				}
			} else {
				if _, exists := disk[newKey]; exists {
					t.Fatal("failed active card advanced cooldown")
				}
				if disk[oldKey].Active || !disk[oldKey].SentAt.Equal(now) {
					t.Fatal("accepted recovery was not persisted")
				}
			}
			for _, c := range snap.Checks {
				if c.ID == "monitor.delivery" && c.Status != Unknown {
					t.Fatal("partial send failure hidden by another card success", c)
				}
			}
			// Reload persisted state exactly as a process restart would: only the
			// unaccepted lifecycle may be retried inside its successful peer's cooldown.
			now = now.Add(time.Minute)
			reloaded := testService(t, cfg, &now, s.collector)
			reloaded.Refresh(context.Background())
			if calls.Load() != 3 {
				t.Fatal("restart retried already accepted peer", calls.Load())
			}
			if !reloaded.states[newKey].Active || reloaded.states[oldKey].Active {
				t.Fatal("failed lifecycle did not complete independently after restart")
			}
		})
	}
}

func TestGroupedCardKeepsEachWalletCooldownAndRecoveryIndependent(t *testing.T) {
	now := cardTestAt
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, `{"code":0}`) }))
	defer srv.Close()
	cfg := testConfig(t)
	cfg.WebhookURL = srv.URL
	checks := []Check{coverageAlert("main", "model").Check, coverageAlert("wallet-6", "model").Check, coverageAlert("wallet-7", "model").Check}
	s := testService(t, cfg, &now, CollectFunc(func(context.Context, time.Time) ([]Check, error) { return checks, nil }))
	s.Refresh(context.Background())
	if calls.Load() != 1 {
		t.Fatal("same-model wallet group did not send one card")
	}
	for _, c := range checks {
		if !s.states["test\x00"+c.ID+"\x00"+c.Scope].Active {
			t.Fatal("grouping lost independent cooldown", c.Scope)
		}
	}
	checks[0].Status = Healthy
	checks[0].Summary = "路由具有有效开仓概率"
	now = now.Add(time.Minute)
	s.Refresh(context.Background())
	if calls.Load() != 2 {
		t.Fatal("single wallet recovery was not independently notified")
	}
	if s.states["test\x00prediction.coverage\x00main/model/multfactor_v1"].Active || !s.states["test\x00prediction.coverage\x00wallet-6/model/multfactor_v1"].Active {
		t.Fatal("one wallet recovery incorrectly cleared another")
	}
}

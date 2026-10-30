package pipelinemonitor

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config is isolated from Redis-capacity and business execution configuration.
// Credentials never enter Snapshot or alert evidence.
type Config struct {
	Enabled            bool
	Interval           time.Duration
	Cooldown           time.Duration
	HTTPTimeout        time.Duration
	CollectTimeout     time.Duration
	StatePath          string
	WorkerSnapshotPath string
	WebhookURL         string
	Secret             string
	AppID              string
	AppSecret          string
	ChatID             string
}

// LoadConfig takes an explicit environment reader so callers/tests need not mutate process state.
func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{Interval: time.Minute, Cooldown: 15 * time.Minute, HTTPTimeout: 3 * time.Second, CollectTimeout: 10 * time.Second}
	if v := strings.TrimSpace(getenv("PIPELINE_MONITOR_ENABLED")); v != "" {
		b, e := strconv.ParseBool(v)
		if e != nil {
			return c, fmt.Errorf("PIPELINE_MONITOR_ENABLED must be boolean")
		}
		c.Enabled = b
	}
	for _, v := range []struct {
		name string
		dst  *time.Duration
	}{
		{"INTERVAL", &c.Interval}, {"ALERT_COOLDOWN", &c.Cooldown}, {"HTTP_TIMEOUT", &c.HTTPTimeout}, {"COLLECT_TIMEOUT", &c.CollectTimeout},
	} {
		if raw := strings.TrimSpace(getenv("PIPELINE_MONITOR_" + v.name)); raw != "" {
			d, e := time.ParseDuration(raw)
			if e != nil || d <= 0 {
				return c, fmt.Errorf("PIPELINE_MONITOR_%s must be a positive duration", v.name)
			}
			*v.dst = d
		}
	}
	c.StatePath = strings.TrimSpace(getenv("PIPELINE_MONITOR_STATE_PATH"))
	c.WorkerSnapshotPath = strings.TrimSpace(getenv("PIPELINE_MONITOR_WORKER_SNAPSHOT_PATH"))
	c.WebhookURL = strings.TrimSpace(getenv("PIPELINE_MONITOR_FEISHU_WEBHOOK_URL"))
	c.Secret = getenv("PIPELINE_MONITOR_FEISHU_SECRET")
	c.AppID = strings.TrimSpace(getenv("PIPELINE_MONITOR_FEISHU_APP_ID"))
	c.AppSecret = getenv("PIPELINE_MONITOR_FEISHU_APP_SECRET")
	c.ChatID = strings.TrimSpace(getenv("PIPELINE_MONITOR_FEISHU_CHAT_ID"))
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.Interval <= 0 || c.Cooldown <= 0 || c.HTTPTimeout <= 0 || c.CollectTimeout <= 0 {
		return fmt.Errorf("pipeline monitor durations must be positive")
	}
	if c.Interval < 5*time.Second || c.HTTPTimeout > 30*time.Second || c.CollectTimeout > time.Minute {
		return fmt.Errorf("pipeline monitor sampling budget is out of range")
	}
	if c.WebhookURL != "" {
		u, e := url.Parse(c.WebhookURL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("pipeline monitor webhook must be an absolute HTTP(S) URL")
		}
	}
	botFields := 0
	for _, s := range []string{c.AppID, c.AppSecret, c.ChatID} {
		if s != "" {
			botFields++
		}
	}
	if botFields != 0 && botFields != 3 {
		return fmt.Errorf("pipeline monitor app bot requires app ID, secret and chat ID")
	}
	if c.WebhookURL != "" && botFields != 0 {
		return fmt.Errorf("configure one pipeline monitor Feishu transport")
	}
	if c.Enabled && (c.WebhookURL != "" || botFields == 3) && c.StatePath == "" {
		return fmt.Errorf("PIPELINE_MONITOR_STATE_PATH is required for durable alert delivery")
	}
	return nil
}

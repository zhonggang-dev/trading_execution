package pipelinemonitor

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// FeishuTarget names one Feishu destination: a custom-bot webhook (optional
// signing secret) or an app bot posting to a chat. Exactly one form is used.
type FeishuTarget struct {
	WebhookURL string
	Secret     string
	AppID      string
	AppSecret  string
	ChatID     string
}

// FeishuSender reuses the pipeline monitor's delivery path for other read-only
// monitors so every Feishu card shares one signing and response contract.
type FeishuSender struct {
	n *notifier
}

// NewFeishuSender validates the target and returns a sender. Credentials stay
// inside the sender and never enter errors.
func NewFeishuSender(target FeishuTarget, client *http.Client) (*FeishuSender, error) {
	target.WebhookURL = strings.TrimSpace(target.WebhookURL)
	target.AppID = strings.TrimSpace(target.AppID)
	target.ChatID = strings.TrimSpace(target.ChatID)
	webhook := target.WebhookURL != ""
	bot := target.AppID != "" || target.AppSecret != "" || target.ChatID != ""
	switch {
	case webhook && bot:
		return nil, fmt.Errorf("Feishu target must be either a webhook or an app bot, not both")
	case webhook:
		u, err := url.Parse(target.WebhookURL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("Feishu webhook must be an absolute HTTP(S) URL")
		}
	case bot:
		if target.AppID == "" || target.AppSecret == "" || target.ChatID == "" {
			return nil, fmt.Errorf("Feishu app bot requires app id, app secret and chat id")
		}
	default:
		return nil, fmt.Errorf("Feishu target is not configured")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &FeishuSender{n: &notifier{config: Config{
		WebhookURL: target.WebhookURL, Secret: target.Secret,
		AppID: target.AppID, AppSecret: target.AppSecret, ChatID: target.ChatID,
	}, client: client}}, nil
}

// SendCard posts one interactive card.
func (s *FeishuSender) SendCard(ctx context.Context, card map[string]any, now time.Time) error {
	return s.n.sendCard(ctx, card, now)
}

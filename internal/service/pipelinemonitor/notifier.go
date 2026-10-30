package pipelinemonitor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type notifier struct {
	config     Config
	client     *http.Client
	token      string
	tokenUntil time.Time
	apiBase    string
}

func (n *notifier) enabled() bool { return n.config.WebhookURL != "" || n.config.AppID != "" }
func (n *notifier) send(ctx context.Context, text string, now time.Time) error {
	if !n.enabled() {
		return nil
	}
	if n.config.WebhookURL != "" {
		p := map[string]any{"msg_type": "text", "content": map[string]string{"text": text}}
		if n.config.Secret != "" {
			stamp := strconv.FormatInt(now.Unix(), 10)
			mac := hmac.New(sha256.New, []byte(stamp+"\n"+n.config.Secret))
			p["timestamp"] = stamp
			p["sign"] = base64.StdEncoding.EncodeToString(mac.Sum(nil))
		}
		return n.post(ctx, n.config.WebhookURL, "", p, nil)
	}
	base := n.apiBase
	if base == "" {
		base = "https://open.feishu.cn"
	}
	if n.token == "" || !now.Before(n.tokenUntil) {
		var r struct {
			Token  string `json:"tenant_access_token"`
			Expire int64  `json:"expire"`
		}
		if err := n.post(ctx, base+"/open-apis/auth/v3/tenant_access_token/internal", "", map[string]string{"app_id": n.config.AppID, "app_secret": n.config.AppSecret}, &r); err != nil {
			return err
		}
		if r.Token == "" || r.Expire <= 60 {
			return fmt.Errorf("Feishu bot authentication returned no usable token")
		}
		n.token = r.Token
		n.tokenUntil = now.Add(time.Duration(r.Expire-60) * time.Second)
	}
	content, _ := json.Marshal(map[string]string{"text": text})
	err := n.post(ctx, base+"/open-apis/im/v1/messages?receive_id_type=chat_id", n.token, map[string]string{"receive_id": n.config.ChatID, "msg_type": "text", "content": string(content)}, nil)
	if err != nil {
		n.token = ""
	}
	return err
}

func (n *notifier) post(ctx context.Context, target, token string, payload any, result any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode Feishu notification")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build Feishu request")
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := n.client.Do(request)
	if err != nil {
		return fmt.Errorf("Feishu notification transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Feishu notification HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("read Feishu response")
	}
	var envelope struct {
		Code *int `json:"code"`
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.Code == nil {
		return fmt.Errorf("Feishu response has no business result")
	}
	if *envelope.Code != 0 {
		return fmt.Errorf("Feishu notification rejected code %d", *envelope.Code)
	}
	if result != nil && json.Unmarshal(data, result) != nil {
		return fmt.Errorf("decode Feishu result")
	}
	return nil
}

package outboxdispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

// fakeOutbox 表示后端使用的 fakeOutbox 类型。
type fakeOutbox struct {
	events    []port.OutboxEvent
	published []string
	failed    []string
}

// Claim 模拟幂等认领并保存测试数据。
func (outbox *fakeOutbox) Claim(context.Context, int, time.Time) ([]port.OutboxEvent, error) {
	return append([]port.OutboxEvent(nil), outbox.events...), nil
}

// MarkPublished 记录模拟状态变更。
func (outbox *fakeOutbox) MarkPublished(_ context.Context, id string, _ time.Time) error {
	outbox.published = append(outbox.published, id)
	return nil
}

// MarkFailed 记录模拟状态变更。
func (outbox *fakeOutbox) MarkFailed(_ context.Context, id string, _ time.Time) error {
	outbox.failed = append(outbox.failed, id)
	return nil
}

// fakePublisher 表示后端使用的 fakePublisher 类型。
type fakePublisher struct{ failKey string }

// Publish 实现当前测试场景所需的辅助行为。
func (publisher fakePublisher) Publish(_ context.Context, _, key string, _ []byte) error {
	if key == publisher.failKey {
		return errors.New("broker unavailable")
	}
	return nil
}

// TestRunOncePublishesAndReschedulesIndependently 验证 Run Once Publishes And Reschedules Independently 场景下的行为。
func TestRunOncePublishesAndReschedulesIndependently(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{events: []port.OutboxEvent{
		{ID: "event-1", EventKey: "ok", Topic: "fills", Attempts: 1},
		{ID: "event-2", EventKey: "fail", Topic: "fills", Attempts: 2},
	}}
	service, err := New(Params{Outbox: outbox, Publisher: fakePublisher{failKey: "fail"}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.RunOnce(context.Background())
	if err == nil || result.Claimed != 2 || result.Published != 1 || result.Failed != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if len(outbox.published) != 1 || outbox.published[0] != "event-1" ||
		len(outbox.failed) != 1 || outbox.failed[0] != "event-2" {
		t.Fatalf("outbox published=%v failed=%v", outbox.published, outbox.failed)
	}
}

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

// OutboxParams 表示后端使用的 OutboxParams 类型。
type OutboxParams struct {
	DB         *sql.DB
	ClaimLease time.Duration
}

// Outbox 表示后端使用的 Outbox 类型。
type Outbox struct {
	db         *sql.DB
	claimLease time.Duration
}

var _ port.OutboxRepository = (*Outbox)(nil)

// NewOutbox 创建并初始化 Outbox。
func NewOutbox(params OutboxParams) (*Outbox, error) {
	if params.DB == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	if params.ClaimLease == 0 {
		params.ClaimLease = 30 * time.Second
	}
	if params.ClaimLease < time.Second || params.ClaimLease > 10*time.Minute {
		return nil, fmt.Errorf("outbox claim lease must be between 1 second and 10 minutes")
	}
	return &Outbox{db: params.DB, claimLease: params.ClaimLease}, nil
}

// Claim 认领一批当前可发布的 Outbox 事件。
func (outbox *Outbox) Claim(ctx context.Context, limit int, now time.Time) ([]port.OutboxEvent, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("outbox claim limit must be between 1 and 1000")
	}
	leaseUntil := now.UTC().Add(outbox.claimLease)
	rows, err := outbox.db.QueryContext(ctx, `
		WITH selected AS (
			SELECT outbox_event_id
			FROM execution_outbox
			WHERE status='PENDING' AND next_attempt_at <= $1
			ORDER BY next_attempt_at, created_at, outbox_event_id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE execution_outbox AS event
		SET attempts=event.attempts+1, next_attempt_at=$3
		FROM selected
		WHERE event.outbox_event_id=selected.outbox_event_id
		RETURNING event.outbox_event_id, event.topic, event.event_key,
		          event.aggregate_id, event.payload::text, event.attempts, event.created_at`,
		now.UTC(), limit, leaseUntil)
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()
	events := make([]port.OutboxEvent, 0, limit)
	for rows.Next() {
		var event port.OutboxEvent
		var payload string
		if err := rows.Scan(&event.ID, &event.Topic, &event.EventKey, &event.AggregateID,
			&payload, &event.Attempts, &event.CreatedAt); err != nil {
			return nil, err
		}
		event.Payload = []byte(payload)
		event.CreatedAt = event.CreatedAt.UTC()
		events = append(events, event)
	}
	return events, rows.Err()
}

// MarkPublished 将 Outbox 事件标记为已发布。
func (outbox *Outbox) MarkPublished(ctx context.Context, eventID string, publishedAt time.Time) error {
	result, err := outbox.db.ExecContext(ctx, `
		UPDATE execution_outbox
		SET status='PUBLISHED', published_at=COALESCE(published_at,$2)
		WHERE outbox_event_id=$1`, eventID, publishedAt.UTC())
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if !oneRow(result) {
		return fmt.Errorf("outbox event %q not found", eventID)
	}
	return nil
}

// MarkFailed 记录 Outbox 发布失败并设置下次重试时间。
func (outbox *Outbox) MarkFailed(ctx context.Context, eventID string, nextAttemptAt time.Time) error {
	result, err := outbox.db.ExecContext(ctx, `
		UPDATE execution_outbox SET next_attempt_at=$2
		WHERE outbox_event_id=$1 AND status='PENDING'`, eventID, nextAttemptAt.UTC())
	if err != nil {
		return fmt.Errorf("reschedule outbox event: %w", err)
	}
	if !oneRow(result) {
		return fmt.Errorf("pending outbox event %q not found", eventID)
	}
	return nil
}

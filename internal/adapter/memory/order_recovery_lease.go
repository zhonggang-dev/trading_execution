package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// OrderRecoveryLeaseStore is the in-process lease store for paper mode and
// tests. It applies the same rules as the PostgreSQL store but does not
// survive a restart, so live wiring must use the durable implementation.
type OrderRecoveryLeaseStore struct {
	mu     sync.Mutex
	leases map[string]domain.OrderRecoveryLease
}

var _ port.OrderRecoveryLeaseStore = (*OrderRecoveryLeaseStore)(nil)

// NewOrderRecoveryLeaseStore 创建并初始化内存 Order Recovery Lease Store。
func NewOrderRecoveryLeaseStore() *OrderRecoveryLeaseStore {
	return &OrderRecoveryLeaseStore{leases: make(map[string]domain.OrderRecoveryLease)}
}

// AcquireOrderRecoveryLease 按订单粒度获取恢复租约并校验快照版本。
func (store *OrderRecoveryLeaseStore) AcquireOrderRecoveryLease(_ context.Context, request domain.OrderRecoveryLeaseRequest) (domain.OrderRecoveryLease, error) {
	request.OrderID = strings.TrimSpace(request.OrderID)
	request.ExecutionAccountID = strings.TrimSpace(request.ExecutionAccountID)
	request.Holder = strings.TrimSpace(request.Holder)
	if request.OrderID == "" || request.ExecutionAccountID == "" || request.Holder == "" {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order id, execution account id, and holder are required")
	}
	if request.TTL <= 0 {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order recovery lease ttl must be positive")
	}
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expiresAt := now.Add(request.TTL)
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.leases[request.OrderID]
	if exists {
		if current.HeldAt(now, request.Holder) {
			return current, port.ErrOrderRecoveryLeaseHeld
		}
		if current.OrderRevision > request.OrderRevision {
			return current, port.ErrOrderRecoveryStaleView
		}
		if !request.BypassBackoff && !current.RetryDueAt(now) {
			return current, port.ErrOrderRecoveryBackoff
		}
	} else {
		current = domain.OrderRecoveryLease{OrderID: request.OrderID, ExecutionAccountID: request.ExecutionAccountID}
	}
	current.Holder = request.Holder
	current.OrderRevision = request.OrderRevision
	current.AcquiredAt = &now
	current.ExpiresAt = &expiresAt
	current.UpdatedAt = now
	store.leases[request.OrderID] = current
	return current, nil
}

// ReleaseOrderRecoveryLease 记录一次恢复结果并释放或删除订单租约。
func (store *OrderRecoveryLeaseStore) ReleaseOrderRecoveryLease(_ context.Context, release domain.OrderRecoveryRelease) (domain.OrderRecoveryLease, error) {
	release.OrderID = strings.TrimSpace(release.OrderID)
	release.Holder = strings.TrimSpace(release.Holder)
	if release.OrderID == "" || release.Holder == "" {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order id and holder are required")
	}
	now := release.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.leases[release.OrderID]
	if !exists {
		return domain.OrderRecoveryLease{}, fmt.Errorf("order recovery lease %s: %w", release.OrderID, port.ErrOrderNotFound)
	}
	if current.Holder != release.Holder {
		return current, port.ErrOrderRecoveryLeaseHeld
	}
	switch release.Kind {
	case domain.OrderRecoveryOutcomeResolved:
		delete(store.leases, release.OrderID)
		return domain.OrderRecoveryLease{OrderID: current.OrderID, ExecutionAccountID: current.ExecutionAccountID, OrderRevision: current.OrderRevision, UpdatedAt: now}, nil
	case domain.OrderRecoveryOutcomeWaiting, domain.OrderRecoveryOutcomeFailed:
		current.Holder = ""
		current.AcquiredAt = nil
		current.ExpiresAt = nil
		if release.Kind == domain.OrderRecoveryOutcomeFailed {
			current.Attempts++
		}
		if current.FirstPendingAt == nil {
			first := now
			current.FirstPendingAt = &first
		}
		last := now
		current.LastAttemptAt = &last
		current.NextRetryAt = nil
		if !release.NextRetryAt.IsZero() {
			next := release.NextRetryAt.UTC()
			current.NextRetryAt = &next
		}
		current.LastError = strings.TrimSpace(release.Error)
		if release.Escalate && current.EscalatedAt == nil {
			escalated := now
			current.EscalatedAt = &escalated
		}
		current.UpdatedAt = now
		store.leases[release.OrderID] = current
		return current, nil
	default:
		return current, fmt.Errorf("unsupported order recovery outcome %q", release.Kind)
	}
}

// Lease 返回订单当前租约记录，供测试和诊断读取。
func (store *OrderRecoveryLeaseStore) Lease(orderID string) (domain.OrderRecoveryLease, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, ok := store.leases[strings.TrimSpace(orderID)]
	return lease, ok
}

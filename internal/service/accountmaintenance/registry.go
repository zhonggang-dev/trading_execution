// Package accountmaintenance keeps the in-process list of execution accounts
// that must not take new orders because an account-local startup or runtime
// check failed. A maintenance account is not sent to the strategy, its new and
// recovered intents are held back, and manual placement is refused, while
// reconciliation, refresh, cancellation, and fill accounting keep running.
//
// The list is deliberately not persisted: a restart re-runs every check, and
// the operator-owned database pause stays a separate, durable control.
package accountmaintenance

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Reason codes. They appear in logs and the console runtime projection.
const (
	ReasonPreflightCLOBAuth               = "PREFLIGHT_CLOB_AUTH"
	ReasonPreflightFundingUnavailable     = "PREFLIGHT_FUNDING_UNAVAILABLE"
	ReasonPreflightAllowanceMissing       = "PREFLIGHT_ALLOWANCE_MISSING"
	ReasonPreflightEligibilityUnavailable = "PREFLIGHT_ELIGIBILITY_UNAVAILABLE"
	ReasonPreflightNotEligible            = "PREFLIGHT_NOT_ELIGIBLE"
	ReasonStartupReconciliationFailed     = "STARTUP_RECONCILIATION_FAILED"
	ReasonKalshiRouteUnavailable          = "KALSHI_ROUTE_UNAVAILABLE"
	ReasonKalshiMarketDataUnavailable     = "KALSHI_MARKET_DATA_UNAVAILABLE"
	// ReasonUnspecified replaces an empty reason so an entry is never invisible.
	ReasonUnspecified = "UNSPECIFIED"
)

const maxDetailBytes = 512

// IsPreflightReason reports whether a reason is cleared by a passing
// Polymarket account preflight.
func IsPreflightReason(reason string) bool {
	switch reason {
	case ReasonPreflightCLOBAuth, ReasonPreflightFundingUnavailable, ReasonPreflightAllowanceMissing,
		ReasonPreflightEligibilityUnavailable, ReasonPreflightNotEligible:
		return true
	default:
		return false
	}
}

// Entry is one active maintenance reason of one account.
type Entry struct {
	AccountID string
	Reason    string
	Detail    string
	Since     time.Time
}

// Registry is safe for concurrent use. A nil *Registry has no maintenance
// accounts, so optional callers need no nil checks.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]map[string]Entry
	now     func() time.Time
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{entries: make(map[string]map[string]Entry), now: time.Now}
}

// Enter records a reason for an account. It reports whether the reason is new
// for the account; repeating a reason only refreshes its detail and keeps the
// original Since.
func (registry *Registry) Enter(accountID, reason, detail string) bool {
	if registry == nil {
		return false
	}
	accountID, reason = strings.TrimSpace(accountID), strings.TrimSpace(reason)
	if accountID == "" {
		return false
	}
	if reason == "" {
		reason = ReasonUnspecified
	}
	detail = boundDetail(detail)
	registry.mu.Lock()
	defer registry.mu.Unlock()
	reasons := registry.entries[accountID]
	if reasons == nil {
		reasons = make(map[string]Entry, 1)
		registry.entries[accountID] = reasons
	}
	if existing, exists := reasons[reason]; exists {
		existing.Detail = detail
		reasons[reason] = existing
		return false
	}
	reasons[reason] = Entry{AccountID: accountID, Reason: reason, Detail: detail, Since: registry.now().UTC()}
	return true
}

// Exit clears one reason. It reports whether the reason was present.
func (registry *Registry) Exit(accountID, reason string) bool {
	if registry == nil {
		return false
	}
	accountID, reason = strings.TrimSpace(accountID), strings.TrimSpace(reason)
	registry.mu.Lock()
	defer registry.mu.Unlock()
	reasons := registry.entries[accountID]
	if _, exists := reasons[reason]; !exists {
		return false
	}
	delete(reasons, reason)
	if len(reasons) == 0 {
		delete(registry.entries, accountID)
	}
	return true
}

// MaintenanceReason reports whether the account is in maintenance and its
// sorted, comma-separated reason codes.
func (registry *Registry) MaintenanceReason(accountID string) (string, bool) {
	if registry == nil {
		return "", false
	}
	accountID = strings.TrimSpace(accountID)
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	reasons := registry.entries[accountID]
	if len(reasons) == 0 {
		return "", false
	}
	codes := make([]string, 0, len(reasons))
	for reason := range reasons {
		codes = append(codes, reason)
	}
	sort.Strings(codes)
	return strings.Join(codes, ","), true
}

// Entries returns the active reasons of one account, sorted by reason.
func (registry *Registry) Entries(accountID string) []Entry {
	if registry == nil {
		return nil
	}
	accountID = strings.TrimSpace(accountID)
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return sortedEntries(registry.entries[accountID])
}

// List returns every active reason, sorted by account then reason.
func (registry *Registry) List() []Entry {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	var result []Entry
	for _, reasons := range registry.entries {
		result = append(result, sortedEntries(reasons)...)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].AccountID != result[right].AccountID {
			return result[left].AccountID < result[right].AccountID
		}
		return result[left].Reason < result[right].Reason
	})
	return result
}

// AccountIDs returns the sorted accounts that currently are in maintenance.
func (registry *Registry) AccountIDs() []string {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	result := make([]string, 0, len(registry.entries))
	for accountID := range registry.entries {
		result = append(result, accountID)
	}
	sort.Strings(result)
	return result
}

func sortedEntries(reasons map[string]Entry) []Entry {
	result := make([]Entry, 0, len(reasons))
	for _, entry := range reasons {
		result = append(result, entry)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Reason < result[right].Reason })
	return result
}

// boundDetail keeps a log/console-friendly single line of bounded size. The
// detail is an error text that already contains no credential.
func boundDetail(detail string) string {
	detail = strings.Join(strings.Fields(detail), " ")
	if len(detail) <= maxDetailBytes {
		return detail
	}
	cut := maxDetailBytes
	for cut > 0 && !utf8Start(detail[cut]) {
		cut--
	}
	return detail[:cut] + "..."
}

func utf8Start(value byte) bool { return value&0xC0 != 0x80 }

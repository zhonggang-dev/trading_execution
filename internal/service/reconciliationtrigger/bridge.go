// Package reconciliationtrigger breaks the construction cycle between the
// execution service and the reconciliation runner without dropping early
// UNKNOWN/CANCEL_UNKNOWN triggers.
package reconciliationtrigger

import (
	"fmt"
	"strings"
	"sync"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const defaultMaximumPending = 10_000

type request struct {
	executionAccountID string
	trigger            domain.ReconciliationTrigger
	focusOrderID       string
}

// Bridge buffers and coalesces triggers until the fully constructed runner is
// bound. After Bind, calls pass straight through to the runner.
type Bridge struct {
	mu             sync.Mutex
	target         port.ReconciliationTriggerer
	pending        map[string]request
	maximumPending int
	dropped        uint64
}

// New creates an unbound trigger bridge.
func New(maximumPending int) (*Bridge, error) {
	if maximumPending == 0 {
		maximumPending = defaultMaximumPending
	}
	if maximumPending < 1 || maximumPending > 1_000_000 {
		return nil, fmt.Errorf("maximum pending reconciliation triggers must be between 1 and 1000000")
	}
	return &Bridge{pending: make(map[string]request), maximumPending: maximumPending}, nil
}

// Trigger stores a stable, coalesced request before Bind and delegates after
// Bind. A full pre-bind buffer drops only duplicate-compensable hints; the
// mandatory startup sweep remains the crash and overflow recovery path.
func (bridge *Bridge) Trigger(executionAccountID string, trigger domain.ReconciliationTrigger, focusOrderID string) {
	request := request{
		executionAccountID: strings.TrimSpace(executionAccountID),
		trigger:            trigger,
		focusOrderID:       strings.TrimSpace(focusOrderID),
	}
	if request.executionAccountID == "" {
		return
	}
	bridge.mu.Lock()
	if bridge.target != nil {
		target := bridge.target
		bridge.mu.Unlock()
		target.Trigger(request.executionAccountID, request.trigger, request.focusOrderID)
		return
	}
	key := request.executionAccountID + "\x00" + string(request.trigger) + "\x00" + request.focusOrderID
	if _, exists := bridge.pending[key]; exists {
		bridge.mu.Unlock()
		return
	}
	if len(bridge.pending) >= bridge.maximumPending {
		bridge.dropped++
		bridge.mu.Unlock()
		return
	}
	bridge.pending[key] = request
	bridge.mu.Unlock()
}

// Bind installs the runner exactly once and synchronously forwards every
// buffered request. The caller should Bind before accepting HTTP traffic.
func (bridge *Bridge) Bind(target port.ReconciliationTriggerer) error {
	if target == nil {
		return fmt.Errorf("reconciliation trigger target is required")
	}
	bridge.mu.Lock()
	if bridge.target != nil {
		bridge.mu.Unlock()
		return fmt.Errorf("reconciliation trigger bridge is already bound")
	}
	bridge.target = target
	pending := make([]request, 0, len(bridge.pending))
	for _, request := range bridge.pending {
		pending = append(pending, request)
	}
	bridge.pending = nil
	bridge.mu.Unlock()
	for _, request := range pending {
		target.Trigger(request.executionAccountID, request.trigger, request.focusOrderID)
	}
	return nil
}

// Dropped reports how many unique pre-bind hints exceeded the safety bound.
// Any non-zero value must keep live readiness false until a startup sweep has
// completed successfully.
func (bridge *Bridge) Dropped() uint64 {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return bridge.dropped
}

var _ port.ReconciliationTriggerer = (*Bridge)(nil)

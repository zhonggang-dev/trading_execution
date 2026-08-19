package reconciliationtrigger

import (
	"sync"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestBridgeForwardsBufferedAndLiveTriggers(t *testing.T) {
	bridge, err := New(10)
	if err != nil {
		t.Fatal(err)
	}
	bridge.Trigger("account-1", domain.ReconciliationTriggerOrderUnknown, "order-1")
	bridge.Trigger("account-1", domain.ReconciliationTriggerOrderUnknown, "order-1")
	target := &recordingTriggerer{}
	if err := bridge.Bind(target); err != nil {
		t.Fatal(err)
	}
	bridge.Trigger("account-1", domain.ReconciliationTriggerCancelUnknown, "order-2")
	requests := target.snapshot()
	if len(requests) != 2 {
		t.Fatalf("requests = %#v, want two unique triggers", requests)
	}
	if requests[0].orderID != "order-1" || requests[1].orderID != "order-2" {
		t.Fatalf("requests = %#v", requests)
	}
}

func TestBridgeBoundsPendingTriggersAndRejectsSecondBind(t *testing.T) {
	bridge, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	bridge.Trigger("account-1", domain.ReconciliationTriggerOrderUnknown, "order-1")
	bridge.Trigger("account-1", domain.ReconciliationTriggerOrderUnknown, "order-2")
	if bridge.Dropped() != 1 {
		t.Fatalf("Dropped() = %d, want 1", bridge.Dropped())
	}
	target := &recordingTriggerer{}
	if err := bridge.Bind(target); err != nil {
		t.Fatal(err)
	}
	if err := bridge.Bind(target); err == nil {
		t.Fatal("second Bind() error = nil")
	}
}

type recordedRequest struct {
	accountID string
	trigger   domain.ReconciliationTrigger
	orderID   string
}

type recordingTriggerer struct {
	mu       sync.Mutex
	requests []recordedRequest
}

func (triggerer *recordingTriggerer) Trigger(accountID string, trigger domain.ReconciliationTrigger, orderID string) {
	triggerer.mu.Lock()
	defer triggerer.mu.Unlock()
	triggerer.requests = append(triggerer.requests, recordedRequest{accountID: accountID, trigger: trigger, orderID: orderID})
}

func (triggerer *recordingTriggerer) snapshot() []recordedRequest {
	triggerer.mu.Lock()
	defer triggerer.mu.Unlock()
	return append([]recordedRequest(nil), triggerer.requests...)
}

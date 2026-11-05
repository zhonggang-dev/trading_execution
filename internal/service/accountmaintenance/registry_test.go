package accountmaintenance

import (
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegistryTracksReasonsPerAccount(t *testing.T) {
	registry := New()
	clock := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	registry.now = func() time.Time { return clock }
	if _, ok := registry.MaintenanceReason("main"); ok {
		t.Fatal("empty registry reported maintenance")
	}
	if !registry.Enter("main", ReasonPreflightCLOBAuth, "auth failed") {
		t.Fatal("first reason must be reported as new")
	}
	clock = clock.Add(time.Minute)
	if registry.Enter(" main ", ReasonPreflightCLOBAuth, "auth still failing") {
		t.Fatal("repeated reason must not be reported as new")
	}
	registry.Enter("main", ReasonStartupReconciliationFailed, "RUNNING lease")
	registry.Enter("kalshi:main", ReasonKalshiRouteUnavailable, "bad key")
	reason, ok := registry.MaintenanceReason("main")
	if !ok || reason != ReasonPreflightCLOBAuth+","+ReasonStartupReconciliationFailed {
		t.Fatalf("MaintenanceReason = %q, %v", reason, ok)
	}
	entries := registry.Entries("main")
	if len(entries) != 2 || entries[0].Detail != "auth still failing" ||
		!entries[0].Since.Equal(time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("entries = %#v, want refreshed detail and original since", entries)
	}
	if got := registry.AccountIDs(); !reflect.DeepEqual(got, []string{"kalshi:main", "main"}) {
		t.Fatalf("AccountIDs = %#v", got)
	}
	if list := registry.List(); len(list) != 3 || list[0].AccountID != "kalshi:main" {
		t.Fatalf("List = %#v", list)
	}
	if !registry.Exit("main", ReasonPreflightCLOBAuth) || registry.Exit("main", ReasonPreflightCLOBAuth) {
		t.Fatal("Exit must report presence exactly once")
	}
	if _, ok := registry.MaintenanceReason("main"); !ok {
		t.Fatal("account left maintenance while a reason remained")
	}
	registry.Exit("main", ReasonStartupReconciliationFailed)
	if _, ok := registry.MaintenanceReason("main"); ok {
		t.Fatal("account stayed in maintenance without reasons")
	}
	if registry.Enter("", ReasonPreflightCLOBAuth, "") {
		t.Fatal("empty account entered maintenance")
	}
}

func TestNilRegistryHasNoMaintenance(t *testing.T) {
	var registry *Registry
	if registry.Enter("main", ReasonPreflightCLOBAuth, "") || registry.Exit("main", ReasonPreflightCLOBAuth) {
		t.Fatal("nil registry changed state")
	}
	if _, ok := registry.MaintenanceReason("main"); ok || registry.List() != nil || registry.AccountIDs() != nil {
		t.Fatal("nil registry reported maintenance")
	}
}

func TestRegistryBoundsDetail(t *testing.T) {
	registry := New()
	registry.Enter("main", "", strings.Repeat("错误\n", 400))
	entries := registry.Entries("main")
	if len(entries) != 1 || entries[0].Reason != ReasonUnspecified {
		t.Fatalf("entries = %#v", entries)
	}
	if len(entries[0].Detail) > maxDetailBytes+3 || strings.Contains(entries[0].Detail, "\n") ||
		!strings.HasSuffix(entries[0].Detail, "...") {
		t.Fatalf("detail not bounded to one line: %q", entries[0].Detail)
	}
}

func TestRegistryIsSafeForConcurrentUse(t *testing.T) {
	registry := New()
	var workers sync.WaitGroup
	for index := 0; index < 16; index++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			for round := 0; round < 100; round++ {
				registry.Enter("main", ReasonPreflightCLOBAuth, "x")
				registry.MaintenanceReason("main")
				registry.List()
				if index%2 == 0 {
					registry.Exit("main", ReasonPreflightCLOBAuth)
				}
			}
		}(index)
	}
	workers.Wait()
}

func TestIsPreflightReason(t *testing.T) {
	for _, reason := range []string{ReasonPreflightCLOBAuth, ReasonPreflightFundingUnavailable,
		ReasonPreflightAllowanceMissing, ReasonPreflightEligibilityUnavailable, ReasonPreflightNotEligible} {
		if !IsPreflightReason(reason) {
			t.Fatalf("%s must be a preflight reason", reason)
		}
	}
	for _, reason := range []string{ReasonStartupReconciliationFailed, ReasonKalshiRouteUnavailable, ReasonKalshiMarketDataUnavailable} {
		if IsPreflightReason(reason) {
			t.Fatalf("%s must not be cleared by a preflight", reason)
		}
	}
}

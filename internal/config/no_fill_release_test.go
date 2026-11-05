package config

import (
	"testing"
	"time"
)

var noFillReleaseKeys = []string{
	"EXECUTION_SUBMIT_ATTEMPT_TIMEOUT", "ORDER_SUBMIT_IN_FLIGHT_GRACE",
	"POLYMARKET_IOC_ORDER_TYPE", "POLYMARKET_IOC_ORDER_LIFETIME",
	"ORDER_UNACCEPTED_AUTO_RESOLVE", "ORDER_UNACCEPTED_RESOLVE_GRACE",
}

func clearNoFillReleaseEnvironment(t *testing.T) {
	t.Helper()
	clearConfigEnvironment(t)
	for _, key := range noFillReleaseKeys {
		t.Setenv(key, "")
	}
}

func TestLoadSubmitAndNoFillReleaseDefaults(t *testing.T) {
	clearNoFillReleaseEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if config.Execution.SubmitAttemptTimeout != 60*time.Second || config.Execution.SubmitInFlightGrace != 90*time.Second {
		t.Fatalf("submit defaults = %s / %s", config.Execution.SubmitAttemptTimeout, config.Execution.SubmitInFlightGrace)
	}
	// GTD stays off and the automatic release only observes until switched on.
	if config.Polymarket.IOCOrderType != "GTC" || config.Polymarket.IOCOrderLifetime != 5*time.Minute ||
		config.Polymarket.UnacceptedAutoResolve != "shadow" || config.Polymarket.UnacceptedResolveGrace != 30*time.Minute {
		t.Fatalf("no-fill defaults = %#v", config.Polymarket)
	}
}

func TestLoadRejectsInvalidSubmitAndNoFillReleaseSettings(t *testing.T) {
	for key, value := range map[string]string{
		"POLYMARKET_IOC_ORDER_TYPE":        "FOK",
		"POLYMARKET_IOC_ORDER_LIFETIME":    "3m",
		"ORDER_UNACCEPTED_AUTO_RESOLVE":    "maybe",
		"ORDER_UNACCEPTED_RESOLVE_GRACE":   "10m",
		"EXECUTION_SUBMIT_ATTEMPT_TIMEOUT": "85s", // leaves no 10s margin below the 90s grace
		"ORDER_SUBMIT_IN_FLIGHT_GRACE":     "11m",
	} {
		t.Run(key, func(t *testing.T) {
			clearNoFillReleaseEnvironment(t)
			t.Setenv(key, value)
			if _, err := Load(); err == nil {
				t.Fatalf("%s=%s was accepted", key, value)
			}
		})
	}
	t.Run("GTD and on are accepted", func(t *testing.T) {
		clearNoFillReleaseEnvironment(t)
		t.Setenv("POLYMARKET_IOC_ORDER_TYPE", "gtd")
		t.Setenv("ORDER_UNACCEPTED_AUTO_RESOLVE", "ON")
		config, err := Load()
		if err != nil || config.Polymarket.IOCOrderType != "GTD" || config.Polymarket.UnacceptedAutoResolve != "on" {
			t.Fatalf("Load() = %#v, %v", config.Polymarket, err)
		}
	})
}

func TestLoadLiveSubmitTimeoutCoversTwoRequests(t *testing.T) {
	clearNoFillReleaseEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("POLYMARKET_REQUEST_TIMEOUT", "30s")
	t.Setenv("HTTP_WRITE_TIMEOUT", "2m")
	if _, err := Load(); err == nil {
		t.Fatal("a 60s submit timeout below 2x30s+10s was accepted in live mode")
	}
	t.Setenv("EXECUTION_SUBMIT_ATTEMPT_TIMEOUT", "70s")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() = %v", err)
	}
}

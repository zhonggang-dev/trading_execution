package config

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// delayedLaneAccounts is the production topology plus a new Kalshi account
// that, with wallet-1, is meant for the delayed lane.
const delayedLaneAccounts = `[
	{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"},
	{"execution_account_id":"wallet-1","market_source":"POLYMARKET","strategy_id":"delayed_lane_pending","mode":"ACTIVE"},
	{"execution_account_id":"wallet-6","market_source":"POLYMARKET","strategy_id":"multfactor_v1","mode":"PAUSED"},
	{"execution_account_id":"wallet-7","market_source":"POLYMARKET","strategy_id":"multfactor_v2","mode":"ACTIVE"},
	{"execution_account_id":"kalshi:main","market_source":"KALSHI","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"},
	{"execution_account_id":"kalshi:delayed","market_source":"KALSHI","strategy_id":"delayed_lane_pending","mode":"PAUSED"}
]`

const delayedLaneKalshiAccounts = `[
	{"execution_account_id":"kalshi:main","api_key_id":"placeholder-key-main","private_key_path":"/opt/trading-execution/secrets/placeholder-main.pem"},
	{"execution_account_id":"kalshi:delayed","api_key_id":"placeholder-key-delayed","private_key_path":"/opt/trading-execution/secrets/placeholder-delayed.pem"}
]`

func setDelayedLaneEnvironment(t *testing.T) {
	t.Helper()
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_ACCOUNTS_JSON", delayedLaneAccounts)
	t.Setenv("KALSHI_LIVE_ACCOUNTS_JSON", delayedLaneKalshiAccounts)
}

// TestLoadWithoutDelayedLaneKeepsEveryDerivedValue is AC1 at the config
// layer: without the new variables the Primary* helpers equal the original
// helpers, and no delayed lane exists.
func TestLoadWithoutDelayedLaneKeepsEveryDerivedValue(t *testing.T) {
	for name, accounts := range map[string]string{
		"production-equivalent": productionEquivalentAccounts,
		"delayed-candidates":    delayedLaneAccounts,
	} {
		t.Run(name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			t.Setenv("DECISION_CYCLE_ACCOUNTS_JSON", accounts)
			kalshi := productionEquivalentKalshiAccounts
			if name == "delayed-candidates" {
				kalshi = delayedLaneKalshiAccounts
			}
			t.Setenv("KALSHI_LIVE_ACCOUNTS_JSON", kalshi)
			config, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			cycle := config.DecisionCycle
			if cycle.HasDelayedLane() || len(cycle.DelayedAccountIDs) != 0 {
				t.Fatalf("delayed lane = %#v", cycle.DelayedAccountIDs)
			}
			if cycle.DelayedStartupDelay != 5*time.Minute+15*time.Second || cycle.DelayedTimeout != 4*time.Minute+30*time.Second {
				t.Fatalf("delayed defaults = %s/%s", cycle.DelayedStartupDelay, cycle.DelayedTimeout)
			}
			if !reflect.DeepEqual(cycle.PrimaryLane(), cycle) {
				t.Fatalf("PrimaryLane() differs from the configuration:\n%#v\n%#v", cycle.PrimaryLane(), cycle)
			}
			if !reflect.DeepEqual(cycle.PrimaryBindings(), cycle.Bindings()) ||
				!reflect.DeepEqual(cycle.PrimaryEntryDisabledAccounts(), cycle.EntryDisabledAccounts()) ||
				!reflect.DeepEqual(cycle.PrimaryStrategyDisabledAccounts(), cycle.StrategyDisabledAccounts()) {
				t.Fatal("Primary* helpers differ from the original helpers without a delayed lane")
			}
			if len(cycle.DelayedBindings()) != 0 || len(cycle.DelayedLane().Accounts) != 0 {
				t.Fatalf("delayed bindings = %#v", cycle.DelayedBindings())
			}
		})
	}
}

func TestLoadDelayedLaneSplitsAccounts(t *testing.T) {
	setDelayedLaneEnvironment(t)
	t.Setenv("DECISION_CYCLE_DELAYED_ACCOUNTS_JSON", ` [" wallet-1 ","kalshi:delayed"] `)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cycle := config.DecisionCycle
	if fmt.Sprint(cycle.DelayedAccountIDs) != "[wallet-1 kalshi:delayed]" {
		t.Fatalf("DelayedAccountIDs = %#v", cycle.DelayedAccountIDs)
	}
	ids := func(bindings []domain.StrategyExecutionBinding) string {
		result := make([]string, 0, len(bindings))
		for _, binding := range bindings {
			result = append(result, binding.ExecutionAccountID)
		}
		return strings.Join(result, ",")
	}
	if got := ids(cycle.PrimaryBindings()); got != "main,wallet-6,wallet-7,kalshi:main" {
		t.Fatalf("primary bindings = %s", got)
	}
	if got := ids(cycle.DelayedBindings()); got != "wallet-1,kalshi:delayed" {
		t.Fatalf("delayed bindings = %s", got)
	}
	if got := ids(cycle.Bindings()); got != "main,wallet-1,wallet-6,wallet-7,kalshi:main,kalshi:delayed" {
		t.Fatalf("all bindings = %s", got)
	}
	if fmt.Sprint(cycle.PrimaryStrategyDisabledAccounts()) != "[wallet-6]" ||
		fmt.Sprint(cycle.PrimaryEntryDisabledAccounts()) != "[wallet-6]" ||
		fmt.Sprint(cycle.DelayedStrategyDisabledAccounts()) != "[kalshi:delayed]" ||
		fmt.Sprint(cycle.DelayedEntryDisabledAccounts()) != "[kalshi:delayed]" {
		t.Fatalf("lane modes = %v %v %v %v", cycle.PrimaryStrategyDisabledAccounts(), cycle.PrimaryEntryDisabledAccounts(),
			cycle.DelayedStrategyDisabledAccounts(), cycle.DelayedEntryDisabledAccounts())
	}
	primary, delayed := cycle.PrimaryLane(), cycle.DelayedLane()
	if primary.StartupDelay != 15*time.Second || primary.Timeout != 8*time.Minute ||
		delayed.StartupDelay != 5*time.Minute+15*time.Second || delayed.Timeout != 4*time.Minute+30*time.Second {
		t.Fatalf("lane timing = primary %s/%s delayed %s/%s", primary.StartupDelay, primary.Timeout, delayed.StartupDelay, delayed.Timeout)
	}
	if primary.HasDelayedLane() || delayed.HasDelayedLane() {
		t.Fatal("a lane configuration still carries the delayed list")
	}
	// The whole account set (console, wallet file, authorizations) is unchanged.
	if len(cycle.Accounts) != 6 {
		t.Fatalf("accounts = %d", len(cycle.Accounts))
	}
}

// TestLoadDelayedLaneMayBeFullyPaused covers R4: every delayed account PAUSED
// still loads.
func TestLoadDelayedLaneMayBeFullyPaused(t *testing.T) {
	setDelayedLaneEnvironment(t)
	t.Setenv("DECISION_CYCLE_DELAYED_ACCOUNTS_JSON", `["kalshi:delayed","wallet-6"]`)
	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadDelayedLaneCustomTiming(t *testing.T) {
	setDelayedLaneEnvironment(t)
	t.Setenv("DECISION_CYCLE_DELAYED_ACCOUNTS_JSON", `["wallet-1"]`)
	t.Setenv("DECISION_CYCLE_DELAYED_STARTUP_DELAY", "4m")
	t.Setenv("DECISION_CYCLE_DELAYED_TIMEOUT", "5m")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.DecisionCycle.DelayedStartupDelay != 4*time.Minute || config.DecisionCycle.DelayedTimeout != 5*time.Minute {
		t.Fatalf("timing = %s/%s", config.DecisionCycle.DelayedStartupDelay, config.DecisionCycle.DelayedTimeout)
	}
}

func TestLoadRejectsInvalidDelayedLane(t *testing.T) {
	for _, test := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "not an array", env: map[string]string{"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `"wallet-1"`}, want: "DECISION_CYCLE_DELAYED_ACCOUNTS_JSON"},
		{name: "empty id", env: map[string]string{"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `[" "]`}, want: "account 0 is empty"},
		{name: "duplicate", env: map[string]string{"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1","wallet-1"]`}, want: `repeats account "wallet-1"`},
		{name: "unknown account", env: map[string]string{"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-9"]`}, want: `"wallet-9" is not in DECISION_CYCLE_ACCOUNTS_JSON`},
		{name: "primary lane left fully paused", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["main","wallet-1","wallet-7","kalshi:main"]`,
		}, want: "at least one account that is not PAUSED in the primary lane"},
		{name: "starts with the primary lane", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1"]`, "DECISION_CYCLE_DELAYED_STARTUP_DELAY": "15s",
		}, want: "DECISION_CYCLE_DELAYED_STARTUP_DELAY must be greater"},
		{name: "starts after the interval", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1"]`, "DECISION_CYCLE_DELAYED_STARTUP_DELAY": "10m",
		}, want: "DECISION_CYCLE_DELAYED_STARTUP_DELAY must be greater"},
		{name: "overlaps the next primary capture", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1"]`, "DECISION_CYCLE_DELAYED_TIMEOUT": "4m31s",
		}, want: "must not exceed DECISION_CYCLE_INTERVAL + DECISION_CYCLE_STARTUP_DELAY"},
		{name: "timeout not shorter than the interval", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1"]`, "DECISION_CYCLE_DELAYED_TIMEOUT": "10m",
		}, want: "DECISION_CYCLE_DELAYED_TIMEOUT must be positive"},
		{name: "malformed timeout", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1"]`, "DECISION_CYCLE_DELAYED_TIMEOUT": "soon",
		}, want: "DECISION_CYCLE_DELAYED_TIMEOUT"},
		{name: "strategy timeout not shorter", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1"]`, "DECISION_CYCLE_DELAYED_STARTUP_DELAY": "7m",
			"DECISION_CYCLE_DELAYED_TIMEOUT": "2m", "DECISION_CYCLE_STRATEGY_TIMEOUT": "2m",
		}, want: "DECISION_CYCLE_STRATEGY_TIMEOUT must be less than DECISION_CYCLE_DELAYED_TIMEOUT"},
		{name: "prediction timeout not shorter", env: map[string]string{
			"DECISION_CYCLE_DELAYED_ACCOUNTS_JSON": `["wallet-1"]`, "DECISION_CYCLE_DELAYED_STARTUP_DELAY": "7m",
			"DECISION_CYCLE_DELAYED_TIMEOUT": "2m", "DECISION_CYCLE_STRATEGY_TIMEOUT": "60s", "DECISION_CYCLE_PREDICTION_TIMEOUT": "2m",
		}, want: "DECISION_CYCLE_PREDICTION_TIMEOUT must be less than DECISION_CYCLE_DELAYED_TIMEOUT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setDelayedLaneEnvironment(t)
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}

// TestLoadIgnoresDelayedTimingWithoutList proves malformed-looking but
// parseable timing values change nothing while the list is empty.
func TestLoadIgnoresDelayedTimingWithoutList(t *testing.T) {
	setDelayedLaneEnvironment(t)
	t.Setenv("DECISION_CYCLE_DELAYED_STARTUP_DELAY", "1s")
	t.Setenv("DECISION_CYCLE_DELAYED_TIMEOUT", "9m")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v, timing must only be checked with a delayed list", err)
	}
}

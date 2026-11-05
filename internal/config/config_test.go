package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestLoadSafeDefaults 验证 Load Safe Defaults 场景下的行为。
func TestLoadSafeDefaults(t *testing.T) {
	clearConfigEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.Execution.Mode != "paper" || config.Execution.Venue != "polymarket-paper" {
		t.Fatalf("execution config = %#v", config.Execution)
	}
	if config.Execution.AllowMarketOrders {
		t.Fatal("market orders must be disabled by default")
	}
	if config.DecisionCycle.Enabled || config.DecisionCycle.OrderSubmissionEnabled || config.DecisionCycle.RequireCompleteModelCoverage {
		t.Fatalf("decision cycle safe defaults = %#v", config.DecisionCycle)
	}
	if config.Kalshi.MarketDataEnabled {
		t.Fatalf("Kalshi market data must be disabled without explicit credentials: %#v", config.Kalshi)
	}
	if len(config.DecisionCycle.Accounts) != 0 || len(config.Kalshi.LiveAccounts) != 0 {
		t.Fatalf("paper defaults must not configure accounts: %#v / %#v", config.DecisionCycle.Accounts, config.Kalshi.LiveAccounts)
	}
}

func TestLoadKalshiLedgerSyncSwitches(t *testing.T) {
	clearConfigEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.Kalshi.BalanceSyncEnabled || !config.Kalshi.PositionCheckEnabled {
		t.Fatalf("Kalshi ledger sync defaults = %#v, want both enabled", config.Kalshi)
	}
	t.Setenv("KALSHI_BALANCE_SYNC_ENABLED", "false")
	t.Setenv("KALSHI_POSITION_CHECK_ENABLED", "false")
	if config, err = Load(); err != nil || config.Kalshi.BalanceSyncEnabled || config.Kalshi.PositionCheckEnabled {
		t.Fatalf("disabled switches = %#v, %v", config.Kalshi, err)
	}
	t.Setenv("KALSHI_BALANCE_SYNC_ENABLED", "maybe")
	if _, err := Load(); err == nil {
		t.Fatal("an invalid KALSHI_BALANCE_SYNC_ENABLED was accepted")
	}
}

func TestLoadAutoRedeemMaxSubmitAttempts(t *testing.T) {
	clearConfigEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.Polymarket.AutoRedeemMaxSubmitAttempts != 3 {
		t.Fatalf("default max submit attempts = %d, want 3", config.Polymarket.AutoRedeemMaxSubmitAttempts)
	}
	t.Setenv("POLYMARKET_AUTO_REDEEM_MAX_SUBMIT_ATTEMPTS", "5")
	if config, err = Load(); err != nil || config.Polymarket.AutoRedeemMaxSubmitAttempts != 5 {
		t.Fatalf("configured max submit attempts = %d, %v, want 5", config.Polymarket.AutoRedeemMaxSubmitAttempts, err)
	}
	for _, value := range []string{"0", "11", "many"} {
		t.Setenv("POLYMARKET_AUTO_REDEEM_MAX_SUBMIT_ATTEMPTS", value)
		if _, err := Load(); err == nil {
			t.Fatalf("max submit attempts %q was accepted", value)
		}
	}
}

func TestLoadChainCashConfiguration(t *testing.T) {
	clearConfigEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(config.Polymarket.ChainCashRewardSenders) != 0 || len(config.Polymarket.ChainCashDepositSources) != 0 ||
		config.Polymarket.ChainCashMaxBlocksPerRun != 1500 || config.Polymarket.ChainCashLogChunkBlocks != 100 {
		t.Fatalf("chain cash defaults = %#v", config.Polymarket)
	}
	t.Setenv("CHAIN_CASH_REWARD_SENDERS", " 0x607C8C9866EF3B4665C5A384188706BE738D8BF8 ,")
	t.Setenv("CHAIN_CASH_DEPOSIT_SOURCES", "wallet-6=0x0AEFD80D00000000000000000000000000000001,wallet-7=0xc9ba353700000000000000000000000000000002")
	t.Setenv("CHAIN_CASH_MAX_BLOCKS_PER_RUN", "500")
	config, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if strings.Join(config.Polymarket.ChainCashRewardSenders, ",") != "0x607c8c9866ef3b4665c5a384188706be738d8bf8" ||
		config.Polymarket.ChainCashDepositSources["wallet-6"][0] != "0x0aefd80d00000000000000000000000000000001" ||
		config.Polymarket.ChainCashDepositSources["wallet-7"][0] != "0xc9ba353700000000000000000000000000000002" ||
		config.Polymarket.ChainCashMaxBlocksPerRun != 500 {
		t.Fatalf("chain cash config = %#v", config.Polymarket)
	}
	for name, values := range map[string]map[string]string{
		"reward sender":           {"CHAIN_CASH_REWARD_SENDERS": "0x1234"},
		"deposit source":          {"CHAIN_CASH_DEPOSIT_SOURCES": "0x0aefd80d00000000000000000000000000000001"},
		"chunk above 100":         {"CHAIN_CASH_LOG_CHUNK_BLOCKS": "101"},
		"blocks above 3000":       {"CHAIN_CASH_MAX_BLOCKS_PER_RUN": "3001"},
		"too many chunks per run": {"CHAIN_CASH_MAX_BLOCKS_PER_RUN": "1500", "CHAIN_CASH_LOG_CHUNK_BLOCKS": "10"},
	} {
		t.Run(name, func(t *testing.T) {
			clearConfigEnvironment(t)
			for key, value := range values {
				t.Setenv(key, value)
			}
			if _, err := Load(); err == nil {
				t.Fatalf("%v was accepted", values)
			}
		})
	}
	clearConfigEnvironment(t)
	t.Setenv("CHAIN_CASH_MAX_BLOCKS_PER_RUN", "3000")
	if config, err = Load(); err != nil || config.Polymarket.ChainCashMaxBlocksPerRun != 3000 {
		t.Fatalf("30 chunks per run rejected: %v", err)
	}
}

func TestLoadKalshiMarketDataRequiresCredentialFiles(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("KALSHI_MARKET_DATA_ENABLED", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH") {
		t.Fatalf("Load() error = %v", err)
	}
	t.Setenv("KALSHI_API_KEY_ID", "key-id")
	t.Setenv("KALSHI_PRIVATE_KEY_PATH", "/run/secrets/trading_execution/kalshi.pem")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.Kalshi.MarketDataEnabled || config.Kalshi.APIURL != "https://external-api.kalshi.com" {
		t.Fatalf("Kalshi config = %#v", config.Kalshi)
	}
}

func TestLoadIgnoresRetiredDecisionCycleMidPriceLookback(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_MID_PRICE_LOOKBACK", "not-a-duration")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v, want retired mid-price lookback to be ignored", err)
	}
}

func TestLoadAcceptsOrderSubmissionForWallet67LiveRelease(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "true")
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.OrderSubmissionEnabled {
		t.Fatal("live decision-cycle order submission was not enabled")
	}
}

func TestLoadRequiresDedicatedLiveJobToken(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "missing", value: "", want: "at least 32 bytes"},
		{name: "short", value: "short", want: "at least 32 bytes"},
		{name: "same as api", value: strings.Repeat("x", 32), want: "must be different"},
		{name: "same as readonly", value: strings.Repeat("r", 32), want: "must be different"},
		{name: "same as prediction", value: strings.Repeat("p", 32), want: "must be different"},
		{name: "same as strategy", value: strings.Repeat("s", 32), want: "must be different"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			t.Setenv("POSITION_EXIT_JOB_TOKEN", test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsDecisionSubmissionWithoutCompleteModelCoverage(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "REQUIRE_COMPLETE_MODEL_COVERAGE") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsDecisionCycleOnInsecureRemoteHTTP(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_PREDICTION_INFRA_URL", "http://prediction.example.invalid")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "HTTPS or loopback") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsDecisionCycleStartWindowAtCadence(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_MAX_START_LATENESS", "10m")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "MAX_START_LATENESS") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsDecisionSubmissionWithoutCycle(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "DECISION_CYCLE_ENABLED") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRequiresExplicitLiveConfirmation(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("EXECUTION_MODE", "live")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "POLYMARKET_LIVE_TRADING_ENABLED") {
		t.Fatalf("Load() error = %v, want explicit live confirmation", err)
	}
}

func TestLoadAcceptsCompleteLoopbackLiveConfiguration(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.Execution.Mode != "live" || !config.Polymarket.LiveTradingEnabled ||
		config.Polymarket.AccountsFile == "" || config.Polymarket.PolygonRPCURL == "" ||
		config.Polymarket.OrderFilledConfirmations != 64 {
		t.Fatalf("live config = %#v", config)
	}
}

func TestLoadPolymarketRequestLimiter(t *testing.T) {
	t.Run("unset leaves the adapter defaults in charge", func(t *testing.T) {
		clearConfigEnvironment(t)
		config, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if config.Polymarket.RequestsPerSecond != 0 || config.Polymarket.Burst != 0 {
			t.Fatalf("limiter = %d/%d, want 0/0 (adapter default)", config.Polymarket.RequestsPerSecond, config.Polymarket.Burst)
		}
	})
	t.Run("explicit values pass through", func(t *testing.T) {
		clearConfigEnvironment(t)
		t.Setenv("POLYMARKET_REQUESTS_PER_SECOND", "20")
		t.Setenv("POLYMARKET_BURST", "10")
		config, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if config.Polymarket.RequestsPerSecond != 20 || config.Polymarket.Burst != 10 {
			t.Fatalf("limiter = %d/%d, want 20/10", config.Polymarket.RequestsPerSecond, config.Polymarket.Burst)
		}
	})
	for _, test := range []struct{ key, value string }{
		{"POLYMARKET_REQUESTS_PER_SECOND", "fast"},
		{"POLYMARKET_REQUESTS_PER_SECOND", "0"},
		{"POLYMARKET_REQUESTS_PER_SECOND", "-1"},
		{"POLYMARKET_REQUESTS_PER_SECOND", "1001"},
		{"POLYMARKET_REQUESTS_PER_SECOND", "2.5"},
		{"POLYMARKET_BURST", "0"},
		{"POLYMARKET_BURST", "-3"},
		{"POLYMARKET_BURST", "1001"},
		{"POLYMARKET_BURST", "many"},
	} {
		t.Run("rejects "+test.key+"="+test.value, func(t *testing.T) {
			clearConfigEnvironment(t)
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.key) {
				t.Fatalf("Load() error = %v, want %s rejection", err, test.key)
			}
		})
	}
}

func TestLoadBoundsPolymarketRequestLimiterInLiveMode(t *testing.T) {
	for _, test := range []struct {
		name, rate, burst, wantError string
	}{
		{"at the venue ceiling", "40", "60", ""},
		{"production setting", "20", "10", ""},
		{"rate above ceiling", "41", "10", "POLYMARKET_REQUESTS_PER_SECOND"},
		{"burst above ceiling", "20", "61", "POLYMARKET_BURST"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			t.Setenv("POLYMARKET_REQUESTS_PER_SECOND", test.rate)
			t.Setenv("POLYMARKET_BURST", test.burst)
			_, err := Load()
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("Load() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Load() error = %v, want %s rejection", err, test.wantError)
			}
		})
	}
	t.Run("paper mode is not capped by the venue ceiling", func(t *testing.T) {
		clearConfigEnvironment(t)
		t.Setenv("POLYMARKET_REQUESTS_PER_SECOND", "500")
		t.Setenv("POLYMARKET_BURST", "500")
		if _, err := Load(); err != nil {
			t.Fatalf("Load() error = %v", err)
		}
	})
}

func TestLoadDecisionCycleDeliveryConcurrency(t *testing.T) {
	t.Run("defaults to sequential delivery", func(t *testing.T) {
		clearConfigEnvironment(t)
		config, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if config.DecisionCycle.DeliveryConcurrency != 1 {
			t.Fatalf("DeliveryConcurrency = %d, want 1", config.DecisionCycle.DeliveryConcurrency)
		}
	})
	for _, value := range []string{"1", "4", "8"} {
		t.Run("accepts "+value, func(t *testing.T) {
			clearConfigEnvironment(t)
			t.Setenv("DECISION_CYCLE_DELIVERY_CONCURRENCY", value)
			config, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got := strconv.Itoa(config.DecisionCycle.DeliveryConcurrency); got != value {
				t.Fatalf("DeliveryConcurrency = %s, want %s", got, value)
			}
		})
	}
	for _, value := range []string{"0", "9", "-1", "many", "2.5"} {
		t.Run("rejects "+value, func(t *testing.T) {
			clearConfigEnvironment(t)
			t.Setenv("DECISION_CYCLE_DELIVERY_CONCURRENCY", value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "DECISION_CYCLE_DELIVERY_CONCURRENCY") {
				t.Fatalf("Load() error = %v, want DECISION_CYCLE_DELIVERY_CONCURRENCY rejection", err)
			}
		})
	}
}

func TestLoadDecisionCycleCallTimeouts(t *testing.T) {
	t.Run("defaults keep an unchanged env file working", func(t *testing.T) {
		clearConfigEnvironment(t)
		setCompleteLiveEnvironment(t)
		setCompleteDecisionCycleEnvironment(t)
		config, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if config.DecisionCycle.StrategyTimeout != 90*time.Second || config.DecisionCycle.PredictionTimeout != 30*time.Second {
			t.Fatalf("timeouts = %s/%s, want 90s/30s", config.DecisionCycle.StrategyTimeout, config.DecisionCycle.PredictionTimeout)
		}
	})
	t.Run("custom values apply", func(t *testing.T) {
		clearConfigEnvironment(t)
		setCompleteLiveEnvironment(t)
		setCompleteDecisionCycleEnvironment(t)
		t.Setenv("DECISION_CYCLE_STRATEGY_TIMEOUT", "2m")
		t.Setenv("DECISION_CYCLE_PREDICTION_TIMEOUT", "10s")
		config, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if config.DecisionCycle.StrategyTimeout != 2*time.Minute || config.DecisionCycle.PredictionTimeout != 10*time.Second {
			t.Fatalf("timeouts = %s/%s", config.DecisionCycle.StrategyTimeout, config.DecisionCycle.PredictionTimeout)
		}
	})
	for _, key := range []string{"DECISION_CYCLE_STRATEGY_TIMEOUT", "DECISION_CYCLE_PREDICTION_TIMEOUT"} {
		for _, value := range []string{"0", "-1s", "8m", "9m", "soon"} {
			t.Run("rejects "+key+"="+value, func(t *testing.T) {
				clearConfigEnvironment(t)
				setCompleteLiveEnvironment(t)
				setCompleteDecisionCycleEnvironment(t)
				t.Setenv(key, value)
				if _, err := Load(); err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("Load() error = %v, want %s rejection", err, key)
				}
			})
		}
	}
}

func TestLoadDecisionCycleOrderReport(t *testing.T) {
	t.Run("defaults to disabled with a 24h lookback", func(t *testing.T) {
		clearConfigEnvironment(t)
		config, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if len(config.DecisionCycle.OrderReportStrategyIDs) != 0 || config.DecisionCycle.OrderReportLookback != 24*time.Hour {
			t.Fatalf("order report = %#v / %s", config.DecisionCycle.OrderReportStrategyIDs, config.DecisionCycle.OrderReportLookback)
		}
	})
	t.Run("reads strategy IDs and lookback", func(t *testing.T) {
		clearConfigEnvironment(t)
		t.Setenv("DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON", `["brier_mid_edge_once"]`)
		t.Setenv("DECISION_CYCLE_ORDER_REPORT_LOOKBACK", "72h")
		config, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if got := config.DecisionCycle.OrderReportStrategyIDs; len(got) != 1 || got[0] != "brier_mid_edge_once" ||
			config.DecisionCycle.OrderReportLookback != 72*time.Hour {
			t.Fatalf("order report = %#v / %s", got, config.DecisionCycle.OrderReportLookback)
		}
	})
	t.Run("rejects a non-array strategy list", func(t *testing.T) {
		clearConfigEnvironment(t)
		t.Setenv("DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON", `"brier_mid_edge_once"`)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON") {
			t.Fatalf("Load() error = %v", err)
		}
	})
	t.Run("rejects a malformed lookback", func(t *testing.T) {
		clearConfigEnvironment(t)
		t.Setenv("DECISION_CYCLE_ORDER_REPORT_LOOKBACK", "soon")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "DECISION_CYCLE_ORDER_REPORT_LOOKBACK") {
			t.Fatalf("Load() error = %v", err)
		}
	})
}

func TestLoadRejectsPublicLiveListener(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("HTTP_ADDRESS", "0.0.0.0:14000")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Load() error = %v, want public listener rejection", err)
	}
}

func TestLoadRejectsInsecureLiveDependencyURL(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("POLYMARKET_CLOB_URL", "http://clob.example.invalid")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("Load() error = %v, want insecure URL rejection", err)
	}
}

func TestLoadPinsCanonicalLiveCLOBOrigin(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("POLYMARKET_CLOB_URL", "https://attacker.example.invalid")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("Load() error = %v, want non-canonical CLOB rejection", err)
	}
}

func TestLoadRejectsShortLiveWriteTimeout(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("HTTP_WRITE_TIMEOUT", "10s")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "four times") {
		t.Fatalf("Load() error = %v, want short write-timeout rejection", err)
	}
}

// TestLoadRejectsSharedLiveOperationsToken 验证实盘只读令牌不能复用交易执行权限。
func TestLoadRejectsSharedLiveOperationsToken(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("LIVE_OPERATIONS_READ_ONLY_TOKEN", strings.Repeat("x", 32))
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must be different") {
		t.Fatalf("Load() error = %v, want token separation rejection", err)
	}
}

func TestLoadBoundsLiveOperationsInterval(t *testing.T) {
	tests := []struct {
		name      string
		interval  string
		maxAge    string
		wantError string
	}{
		{name: "thirty seconds", interval: "30s", maxAge: "90s"},
		{name: "upper bound", interval: "60s", maxAge: "90s"},
		{name: "above upper bound", interval: "61s", maxAge: "90s", wantError: "between 5s and 60s"},
		{name: "below lower bound", interval: "4s", maxAge: "30s", wantError: "between 5s and 60s"},
		{name: "max age not greater than interval", interval: "30s", maxAge: "30s", wantError: "MAX_SNAPSHOT_AGE must be greater"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			t.Setenv("LIVE_OPERATIONS_INTERVAL", test.interval)
			t.Setenv("LIVE_OPERATIONS_REFRESH_TIMEOUT", "3s")
			t.Setenv("LIVE_OPERATIONS_MAX_SNAPSHOT_AGE", test.maxAge)
			config, err := Load()
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("Load() error = %v", err)
				}
				if want, _ := time.ParseDuration(test.interval); config.LiveOperations.Interval != want {
					t.Fatalf("interval = %s, want %s", config.LiveOperations.Interval, test.interval)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Load() error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestLoadDoesNotRequireExecutionMonetaryCap(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("EXECUTION_MAX_ORDER_NOTIONAL", "")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() error = %v, monetary allocation belongs to the upstream strategy", err)
	}
}

// TestLoadRequiresProductionToken 验证 Load Requires Production Token 场景下的行为。
func TestLoadRequiresProductionToken(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("EXECUTION_API_TOKEN", "short")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("Load() error = %v, want token validation", err)
	}
}

// TestLoadRequiresProductionDatabase ensures non-local deployments cannot
// silently fall back to volatile order and reservation repositories.
func TestLoadRequiresProductionDatabase(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("EXECUTION_API_TOKEN", strings.Repeat("x", 32))
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "TRADING_EXECUTION_DATABASE_URL") {
		t.Fatalf("Load() error = %v, want database validation", err)
	}
}

func TestReconciliationScheduleOffset(t *testing.T) {
	t.Setenv("RECONCILIATION_SCHEDULE_OFFSET", "")
	if offset, err := reconciliationScheduleOffset(5 * time.Minute); err != nil || offset != nil {
		t.Fatalf("unset offset = %v, %v; want nil", offset, err)
	}
	t.Setenv("RECONCILIATION_SCHEDULE_OFFSET", "3m30s")
	if offset, err := reconciliationScheduleOffset(5 * time.Minute); err != nil || offset == nil || *offset != 210*time.Second {
		t.Fatalf("3m30s offset = %v, %v", offset, err)
	}
	for _, value := range []string{"5m", "-1s", "soon"} {
		t.Setenv("RECONCILIATION_SCHEDULE_OFFSET", value)
		if _, err := reconciliationScheduleOffset(5 * time.Minute); err == nil {
			t.Fatalf("offset %q accepted", value)
		}
	}
}

// productionEquivalentAccounts is the live topology of 2026-10-29 expressed in
// the account-row format: wallet-1 was entry- and strategy-disabled (PAUSED)
// and the Kalshi account becomes its own row instead of riding on main.
const productionEquivalentAccounts = `[
	{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"},
	{"execution_account_id":"wallet-1","market_source":"POLYMARKET","strategy_id":"strategy_pending_b","mode":"PAUSED"},
	{"execution_account_id":"wallet-6","market_source":"POLYMARKET","strategy_id":"strategy-v1","mode":"ACTIVE"},
	{"execution_account_id":"wallet-7","market_source":"POLYMARKET","strategy_id":"multfactor_v2","mode":"ACTIVE"},
	{"execution_account_id":"kalshi:main","market_source":"KALSHI","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"}
]`

const productionEquivalentKalshiAccounts = `[
	{"execution_account_id":"kalshi:main","api_key_id":"kalshi-key-main","private_key_path":"/opt/trading-execution/secrets/kalshi-main.pem"}
]`

func TestLoadDecisionAccountsDerivesTheProductionTopology(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_PREDICTION_MODEL_ID", " v4.1 flash ")
	t.Setenv("DECISION_CYCLE_ACCOUNTS_JSON", productionEquivalentAccounts)
	t.Setenv("KALSHI_LIVE_ACCOUNTS_JSON", productionEquivalentKalshiAccounts)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "true")
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cycle := config.DecisionCycle
	if cycle.PredictionModelID != "v4.1 flash" || cycle.PredictionSourceMode != domain.PredictionSourceModeSandbox {
		t.Fatalf("model = %q / %q", cycle.PredictionModelID, cycle.PredictionSourceMode)
	}
	want := []domain.StrategyExecutionBinding{
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "main", MarketSource: domain.MarketSourcePolymarket},
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: "strategy_pending_b", ExecutionAccountID: "wallet-1", MarketSource: domain.MarketSourcePolymarket},
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-6", MarketSource: domain.MarketSourcePolymarket},
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-7", MarketSource: domain.MarketSourcePolymarket},
		{PredictionModelID: "v4.1 flash", ModelID: "v4.1 flash", StrategyID: "brier_mid_edge_once", ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi},
	}
	if got := cycle.Bindings(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Bindings() =\n%#v\nwant\n%#v", got, want)
	}
	if got := cycle.EntryDisabledAccounts(); fmt.Sprint(got) != "[wallet-1]" {
		t.Fatalf("EntryDisabledAccounts() = %v", got)
	}
	if got := cycle.StrategyDisabledAccounts(); fmt.Sprint(got) != "[wallet-1]" {
		t.Fatalf("StrategyDisabledAccounts() = %v", got)
	}
	if got := cycle.PredictionSourceModes(); len(got) != 1 || got["v4.1 flash"] != domain.PredictionSourceModeSandbox {
		t.Fatalf("PredictionSourceModes() = %v", got)
	}
	if got := cycle.AccountsForSource(domain.MarketSourceKalshi); len(got) != 1 || got[0].ExecutionAccountID != "kalshi:main" {
		t.Fatalf("Kalshi accounts = %#v", got)
	}
	if got := config.Kalshi.LiveAccounts; len(got) != 1 || got[0].ExecutionAccountID != "kalshi:main" ||
		got[0].APIKeyID != "kalshi-key-main" || got[0].PrivateKeyPath != "/opt/trading-execution/secrets/kalshi-main.pem" {
		t.Fatalf("Kalshi live accounts = %#v", got)
	}
}

func TestLoadDecisionAccountsHaveNoFixedTopology(t *testing.T) {
	for name, accounts := range map[string]string{
		"single account": `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"}]`,
		"new names and one strategy on two wallets": `[
			{"execution_account_id":"wallet-8","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"},
			{"execution_account_id":"wallet-9","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"SELL_ONLY"}
		]`,
		"wallet-6 on another strategy": `[{"execution_account_id":"wallet-6","market_source":"POLYMARKET","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"}]`,
		"lower-case enums":             `[{"execution_account_id":"main","market_source":"polymarket","strategy_id":"brier_mid_edge_once","mode":"active"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			t.Setenv("DECISION_CYCLE_ACCOUNTS_JSON", accounts)
			if _, err := Load(); err != nil {
				t.Fatalf("Load() error = %v", err)
			}
		})
	}
}

func TestLoadAcceptsDirectPredictionSourceAndAnyModelName(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_PREDICTION_MODEL_ID", "kimi-k2")
	t.Setenv("DECISION_CYCLE_PREDICTION_SOURCE_MODE", "direct")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.DecisionCycle.PredictionModelID != "kimi-k2" || config.DecisionCycle.PredictionSourceMode != domain.PredictionSourceModeDirect {
		t.Fatalf("decision cycle = %#v", config.DecisionCycle)
	}
	for _, binding := range config.DecisionCycle.Bindings() {
		if binding.ModelID != "kimi-k2" || binding.PredictionModelID != "kimi-k2" {
			t.Fatalf("binding model = %#v", binding)
		}
	}
}

func TestLoadRejectsLegacyDecisionVariables(t *testing.T) {
	for _, name := range legacyDecisionVariables {
		t.Run(name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			// An empty line in an EnvironmentFile still defines the variable.
			t.Setenv(name, "")
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("Load() error = %v, want rejection naming %s", err, name)
			}
		})
	}
}

func TestLoadRejectsInvalidDecisionAccounts(t *testing.T) {
	const kalshiRow = `{"execution_account_id":"kalshi:main","market_source":"KALSHI","strategy_id":"brier_mid_edge_once","mode":"ACTIVE"}`
	for _, test := range []struct {
		name     string
		model    string
		mode     string
		accounts string
		kalshi   string
		want     string
	}{
		{name: "empty account list", accounts: `[]`, want: "at least one account"},
		{name: "missing account list", accounts: "", want: "at least one account"},
		{name: "not an array", accounts: `{"execution_account_id":"main"}`, want: "DECISION_CYCLE_ACCOUNTS_JSON"},
		{name: "trailing value", accounts: `[] []`, want: "exactly one JSON array"},
		{name: "unknown field", accounts: `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"s","mode":"ACTIVE","model_id":"x"}]`, want: "unknown field"},
		{name: "empty account id", accounts: `[{"execution_account_id":" ","market_source":"POLYMARKET","strategy_id":"s","mode":"ACTIVE"}]`, want: "execution_account_id is required"},
		{name: "duplicate account", accounts: `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"a","mode":"ACTIVE"},{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"b","mode":"ACTIVE"}]`, want: `repeats account "main"`},
		{name: "missing market source", accounts: `[{"execution_account_id":"main","strategy_id":"s","mode":"ACTIVE"}]`, want: "market_source must be POLYMARKET or KALSHI"},
		{name: "unknown market source", accounts: `[{"execution_account_id":"main","market_source":"MANIFOLD","strategy_id":"s","mode":"ACTIVE"}]`, want: "market_source must be POLYMARKET or KALSHI"},
		{name: "Kalshi account without prefix", accounts: `[{"execution_account_id":"main","market_source":"KALSHI","strategy_id":"s","mode":"ACTIVE"}]`, kalshi: `[]`, want: `must start with "kalshi:"`},
		{name: "Polymarket account with Kalshi prefix", accounts: `[{"execution_account_id":"kalshi:main","market_source":"POLYMARKET","strategy_id":"s","mode":"ACTIVE"}]`, want: "cannot use the"},
		{name: "missing mode", accounts: `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"s"}]`, want: "mode must be ACTIVE, SELL_ONLY or PAUSED"},
		{name: "unknown mode", accounts: `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"s","mode":"QUARANTINED"}]`, want: "mode must be ACTIVE, SELL_ONLY or PAUSED"},
		{name: "missing strategy", accounts: `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"","mode":"ACTIVE"}]`, want: "strategy_id"},
		{name: "unsafe strategy", accounts: `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"rm -rf","mode":"ACTIVE"}]`, want: "invalid format"},
		{name: "every account paused", accounts: `[{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"s","mode":"PAUSED"}]`, want: "cannot pause every account"},
		{name: "missing model", model: " ", want: "DECISION_CYCLE_PREDICTION_MODEL_ID must contain 1-200 bytes"},
		{name: "model too long", model: strings.Repeat("m", 201), want: "DECISION_CYCLE_PREDICTION_MODEL_ID must contain 1-200 bytes"},
		{name: "unknown source mode", mode: "REDIS", want: "DECISION_CYCLE_PREDICTION_SOURCE_MODE must be exactly DIRECT or SANDBOX"},
		{name: "Kalshi account without credential", accounts: `[` + kalshiRow + `]`, kalshi: `[]`, want: `KALSHI account "kalshi:main" has no credential`},
		{name: "credential for unknown account", kalshi: `[{"execution_account_id":"kalshi:wallet-7","api_key_id":"k","private_key_path":"/k.pem"}]`, want: `"kalshi:wallet-7" is not a KALSHI account`},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			if test.model != "" {
				t.Setenv("DECISION_CYCLE_PREDICTION_MODEL_ID", test.model)
			}
			if test.mode != "" {
				t.Setenv("DECISION_CYCLE_PREDICTION_SOURCE_MODE", test.mode)
			}
			if test.accounts != "" || test.name == "missing account list" {
				t.Setenv("DECISION_CYCLE_ACCOUNTS_JSON", test.accounts)
			}
			if test.kalshi != "" {
				t.Setenv("KALSHI_LIVE_ACCOUNTS_JSON", test.kalshi)
			}
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDecodeKalshiLiveAccountsRequiresIsolatedCredentials(t *testing.T) {
	accounts, err := decodeKalshiLiveAccounts(` [{"execution_account_id":" kalshi:main ","api_key_id":" key ","private_key_path":" /k.pem "}] `)
	if err != nil || len(accounts) != 1 || accounts[0].ExecutionAccountID != "kalshi:main" ||
		accounts[0].APIKeyID != "key" || accounts[0].PrivateKeyPath != "/k.pem" {
		t.Fatalf("decoded = %#v, %v", accounts, err)
	}
	for name, invalid := range map[string]string{
		"not an array":     `{"execution_account_id":"kalshi:main"}`,
		"incomplete":       `[{"execution_account_id":"kalshi:main","api_key_id":"key"}]`,
		"relative key":     `[{"execution_account_id":"kalshi:main","api_key_id":"key","private_key_path":"k.pem"}]`,
		"missing prefix":   `[{"execution_account_id":"main","api_key_id":"key","private_key_path":"/k.pem"}]`,
		"repeated account": `[{"execution_account_id":"kalshi:main","api_key_id":"a","private_key_path":"/a.pem"},{"execution_account_id":"kalshi:main","api_key_id":"b","private_key_path":"/b.pem"}]`,
		"shared key":       `[{"execution_account_id":"kalshi:a","api_key_id":"key","private_key_path":"/a.pem"},{"execution_account_id":"kalshi:b","api_key_id":"key","private_key_path":"/b.pem"}]`,
		"unknown field":    `[{"execution_account_id":"kalshi:main","api_key_id":"key","private_key_path":"/k.pem","model_id":"m"}]`,
	} {
		if _, err := decodeKalshiLiveAccounts(invalid); err == nil {
			t.Fatalf("%s: accepted %s", name, invalid)
		}
	}
}

func TestLoadLiveModeRequiresAccountsWhileSchedulerDisabled(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	unsetConfigEnvironment(t, "DECISION_CYCLE_ACCOUNTS_JSON")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "at least one account") {
		t.Fatalf("Load() error = %v, live startup derives authorization from the account rows", err)
	}
}

func TestLoadAcceptsGlobalSellOnlyGateInLiveMode(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED", "true")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.EntrySubmissionDisabled {
		t.Fatal("global sell-only gate was not loaded")
	}
}

// clearConfigEnvironment 实现当前测试场景所需的辅助行为。
func clearConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"APP_NAME", "APP_ENV", "LOG_LEVEL", "HTTP_ADDRESS", "HTTP_READ_HEADER_TIMEOUT",
		"HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT",
		"LIVE_OPERATIONS_READ_ONLY_TOKEN", "LIVE_OPERATIONS_INTERVAL", "LIVE_OPERATIONS_REFRESH_TIMEOUT",
		"LIVE_OPERATIONS_MAX_SNAPSHOT_AGE", "LIVE_OPERATIONS_EVENT_LIMIT",
		"DATABASE_CONNECT_TIMEOUT", "TRADING_EXECUTION_DATABASE_URL",
		"EXECUTION_API_TOKEN", "POSITION_EXIT_JOB_TOKEN", "EXECUTION_MODE", "EXECUTION_VENUE", "EXECUTION_ALLOW_MARKET_ORDERS",
		"EXECUTION_MAX_ORDER_NOTIONAL",
		"ORDER_COORDINATOR_INTERVAL", "ORDER_COORDINATOR_BATCH_SIZE",
		"POLYMARKET_LIVE_TRADING_ENABLED", "POLYMARKET_ACCOUNTS_FILE", "POLYMARKET_CLOB_URL",
		"POLYMARKET_GEOBLOCK_URL", "POLYMARKET_FRONTEND_ONLY_API_COUNTRIES", "POLYMARKET_GAMMA_URL",
		"POLYMARKET_DATA_API_URL", "POLYGON_RPC_URL", "POLYMARKET_REQUEST_TIMEOUT",
		"POLYMARKET_STARTUP_TIMEOUT", "POLYMARKET_MAX_CLOCK_SKEW", "POLYMARKET_HEARTBEAT_INTERVAL",
		"POLYMARKET_HEARTBEAT_CALL_TIMEOUT", "POLYMARKET_HEARTBEAT_STALE_AFTER",
		"RECONCILIATION_INTERVAL", "RECONCILIATION_TRADE_LOOKBACK",
		"RECONCILIATION_POSITION_EPSILON", "RECONCILIATION_BALANCE_EPSILON",
		"CANCEL_FILL_FINALITY_GRACE", "MAX_ORDER_RECONCILE_ATTEMPTS",
		"POLYMARKET_MAX_BUY_FEE_RATE_BPS", "POLYGON_ORDER_FILLED_CONFIRMATIONS",
		"POLYMARKET_REQUESTS_PER_SECOND", "POLYMARKET_BURST",
		"CHAIN_CASH_REWARD_SENDERS", "CHAIN_CASH_DEPOSIT_SOURCES", "CHAIN_CASH_MAX_BLOCKS_PER_RUN",
		"CHAIN_CASH_LOG_CHUNK_BLOCKS",
		"KALSHI_MARKET_DATA_ENABLED", "KALSHI_API_URL", "KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH", "KALSHI_REQUEST_TIMEOUT",
		"KALSHI_LIVE_ACCOUNTS_JSON", "KALSHI_SETTLEMENT_ENABLED", "KALSHI_SETTLEMENT_INTERVAL",
		"KALSHI_BALANCE_DRIFT_THRESHOLD", "KALSHI_BALANCE_SYNC_ENABLED", "KALSHI_POSITION_CHECK_ENABLED",
		"DECISION_CYCLE_ENABLED", "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED",
		"DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON", "DECISION_CYCLE_ORDER_REPORT_LOOKBACK",
		"DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE",
		"DECISION_CYCLE_PREDICTION_INFRA_URL", "DECISION_CYCLE_PREDICTION_INFRA_TOKEN",
		"DECISION_CYCLE_STRATEGY_URL", "DECISION_CYCLE_STRATEGY_TOKEN",
		"DECISION_CYCLE_INTERVAL", "DECISION_CYCLE_STARTUP_DELAY", "DECISION_CYCLE_MAX_START_LATENESS", "DECISION_CYCLE_TIMEOUT",
		"DECISION_CYCLE_STRATEGY_TIMEOUT", "DECISION_CYCLE_PREDICTION_TIMEOUT",
		"DECISION_CYCLE_PREDICTION_LOOKBACK", "DECISION_CYCLE_MID_PRICE_LOOKBACK",
		"DECISION_CYCLE_PREDICTION_MODEL_ID", "DECISION_CYCLE_PREDICTION_SOURCE_MODE", "DECISION_CYCLE_ACCOUNTS_JSON",
		"DECISION_CYCLE_DELIVERY_CONCURRENCY",
	} {
		t.Setenv(key, "")
	}
	// Replaced variables must be absent, not empty: an empty definition is
	// itself rejected at startup.
	unsetConfigEnvironment(t, legacyDecisionVariables...)
}

// unsetConfigEnvironment removes variables for one test and restores them
// afterwards.
func unsetConfigEnvironment(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func setCompleteDecisionCycleEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("DECISION_CYCLE_ENABLED", "true")
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "false")
	t.Setenv("DECISION_CYCLE_PREDICTION_INFRA_URL", "http://127.0.0.1:11000")
	t.Setenv("DECISION_CYCLE_PREDICTION_INFRA_TOKEN", strings.Repeat("p", 32))
	t.Setenv("DECISION_CYCLE_STRATEGY_URL", "http://127.0.0.1:8787")
	t.Setenv("DECISION_CYCLE_STRATEGY_TOKEN", strings.Repeat("s", 32))
	t.Setenv("DECISION_CYCLE_INTERVAL", "10m")
	t.Setenv("DECISION_CYCLE_STARTUP_DELAY", "15s")
	t.Setenv("DECISION_CYCLE_MAX_START_LATENESS", "30s")
	t.Setenv("DECISION_CYCLE_TIMEOUT", "8m")
	t.Setenv("DECISION_CYCLE_PREDICTION_LOOKBACK", "3h")
}

// setCompleteLiveEnvironment also sets the account rows: live startup needs
// them even while the decision cycle is disabled.
func setCompleteLiveEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "production")
	t.Setenv("HTTP_ADDRESS", "127.0.0.1:14000")
	t.Setenv("EXECUTION_API_TOKEN", strings.Repeat("x", 32))
	t.Setenv("POSITION_EXIT_JOB_TOKEN", strings.Repeat("j", 32))
	t.Setenv("LIVE_OPERATIONS_READ_ONLY_TOKEN", strings.Repeat("r", 32))
	t.Setenv("TRADING_EXECUTION_DATABASE_URL", "postgres://example.invalid/trading")
	t.Setenv("EXECUTION_MODE", "live")
	t.Setenv("EXECUTION_VENUE", "polymarket")
	t.Setenv("POLYMARKET_LIVE_TRADING_ENABLED", "true")
	t.Setenv("POLYMARKET_MAX_BUY_FEE_RATE_BPS", "10000")
	t.Setenv("POLYGON_ORDER_FILLED_CONFIRMATIONS", "64")
	t.Setenv("POLYMARKET_ACCOUNTS_FILE", "/run/secrets/trading_execution/wallets.json")
	t.Setenv("POLYGON_RPC_URL", "https://polygon-rpc.example.invalid")
	t.Setenv("DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED", "false")
	t.Setenv("DECISION_CYCLE_PREDICTION_MODEL_ID", "masked-source")
	t.Setenv("DECISION_CYCLE_ACCOUNTS_JSON", `[
		{"execution_account_id":"main","market_source":"POLYMARKET","strategy_id":"strategy_pending_a","mode":"SELL_ONLY"},
		{"execution_account_id":"wallet-1","market_source":"POLYMARKET","strategy_id":"strategy_pending_b","mode":"SELL_ONLY"},
		{"execution_account_id":"wallet-6","market_source":"POLYMARKET","strategy_id":"multfactor_v1","mode":"ACTIVE"},
		{"execution_account_id":"wallet-7","market_source":"POLYMARKET","strategy_id":"multfactor_v2","mode":"ACTIVE"}
	]`)
}

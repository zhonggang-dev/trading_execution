package config

import (
	"strings"
	"testing"
	"time"
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
}

func TestLoadAcceptsDecisionCycleWithSubmissionDisabled(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.Enabled || config.DecisionCycle.OrderSubmissionEnabled ||
		len(config.DecisionCycle.Bindings) != 1 || config.DecisionCycle.Interval != 10*time.Minute ||
		config.DecisionCycle.Bindings[0].PredictionModelID != "model-a" {
		t.Fatalf("decision cycle config = %#v", config.DecisionCycle)
	}
}

func TestLoadAcceptsFourWalletPredictionRoutes(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-2"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-3"}
	]`)
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.RequireCompleteModelCoverage || len(config.DecisionCycle.Bindings) != 4 ||
		config.DecisionCycle.Bindings[0].ExecutionAccountID != "main" ||
		config.DecisionCycle.Bindings[0].StrategyID != "multfactor_v2" ||
		config.DecisionCycle.Bindings[1].ExecutionAccountID != "wallet-1" ||
		config.DecisionCycle.Bindings[1].StrategyID != "multfactor_v1" ||
		config.DecisionCycle.Bindings[2].PredictionModelID != "gemini-3.6-flash" ||
		config.DecisionCycle.Bindings[2].ModelID != "gemini_masked" {
		t.Fatalf("decision routes = %#v", config.DecisionCycle.Bindings)
	}
}

func TestLoadRejectsPredictionModelRoutedToMultipleLogicalModels(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"producer-a","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"main"},
		{"prediction_model_id":"producer-a","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-2"}
	]`)
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "multiple logical models") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadAcceptsExplicitLiveDecisionSubmission(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-2"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-3"}
	]`)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "true")
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.OrderSubmissionEnabled {
		t.Fatal("decision-cycle order submission was not enabled")
	}
}

func TestLoadAcceptsSellOnlyDecisionSubmission(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED", "true")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.EntrySubmissionDisabled {
		t.Fatal("decision-cycle entry submission gate was not disabled")
	}
}

func TestLoadRejectsSingleBindingLiveDecisionSubmission(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "true")
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "2 models x 2 strategies x 4 accounts") {
		t.Fatalf("Load() error = %v", err)
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

func TestLoadRejectsDuplicateDecisionBinding(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"model_id":"model-a","strategy_id":"multfactor_v1","execution_account_id":"account-a"},
		{"model_id":"model-b","strategy_id":"multfactor_v2","execution_account_id":"account-a"}
	]`)
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "duplicate execution account") {
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

func TestLoadRequiresExplicitLiveRiskLimits(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	t.Setenv("EXECUTION_MAX_ORDER_NOTIONAL", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must be explicitly configured") {
		t.Fatalf("Load() error = %v, want explicit live risk limit", err)
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
		"EXECUTION_MAX_ORDER_SIZE", "EXECUTION_MAX_ORDER_NOTIONAL",
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
		"DECISION_CYCLE_ENABLED", "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED",
		"DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE",
		"DECISION_CYCLE_PREDICTION_INFRA_URL", "DECISION_CYCLE_PREDICTION_INFRA_TOKEN",
		"DECISION_CYCLE_STRATEGY_URL", "DECISION_CYCLE_STRATEGY_TOKEN",
		"DECISION_CYCLE_INTERVAL", "DECISION_CYCLE_STARTUP_DELAY", "DECISION_CYCLE_MAX_START_LATENESS", "DECISION_CYCLE_TIMEOUT",
		"DECISION_CYCLE_PREDICTION_LOOKBACK", "DECISION_CYCLE_MID_PRICE_LOOKBACK",
		"DECISION_CYCLE_BINDINGS_JSON",
	} {
		t.Setenv(key, "")
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
	t.Setenv("DECISION_CYCLE_MID_PRICE_LOOKBACK", "48h")
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[{"model_id":"model-a","strategy_id":"multfactor_v1","execution_account_id":"account-a"}]`)
}

func setCompleteLiveEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "production")
	t.Setenv("HTTP_ADDRESS", "127.0.0.1:14000")
	t.Setenv("EXECUTION_API_TOKEN", strings.Repeat("x", 32))
	t.Setenv("LIVE_OPERATIONS_READ_ONLY_TOKEN", strings.Repeat("r", 32))
	t.Setenv("TRADING_EXECUTION_DATABASE_URL", "postgres://example.invalid/trading")
	t.Setenv("EXECUTION_MODE", "live")
	t.Setenv("EXECUTION_VENUE", "polymarket")
	t.Setenv("EXECUTION_MAX_ORDER_SIZE", "10")
	t.Setenv("EXECUTION_MAX_ORDER_NOTIONAL", "10")
	t.Setenv("POLYMARKET_LIVE_TRADING_ENABLED", "true")
	t.Setenv("POLYMARKET_MAX_BUY_FEE_RATE_BPS", "10000")
	t.Setenv("POLYGON_ORDER_FILLED_CONFIRMATIONS", "64")
	t.Setenv("POLYMARKET_ACCOUNTS_FILE", "/run/secrets/trading_execution/wallets.json")
	t.Setenv("POLYGON_RPC_URL", "https://polygon-rpc.example.invalid")
}

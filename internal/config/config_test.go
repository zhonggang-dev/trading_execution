package config

import (
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

func TestLoadAcceptsDecisionCycleWithSubmissionDisabled(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.Enabled || config.DecisionCycle.OrderSubmissionEnabled ||
		len(config.DecisionCycle.Bindings) != 4 || config.DecisionCycle.Interval != 10*time.Minute ||
		config.DecisionCycle.Bindings[0].PredictionModelID != "echo-source" ||
		config.DecisionCycle.PredictionSourceModes["echo-source"] != domain.PredictionSourceModeDirect ||
		config.DecisionCycle.PredictionSourceModes["masked-source"] != domain.PredictionSourceModeSandbox {
		t.Fatalf("decision cycle config = %#v", config.DecisionCycle)
	}
}

func TestLoadRejectsInvalidDecisionPredictionSourceModes(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "missing configured model",
			value: `{"echo-source":"DIRECT"}`,
			want:  `missing configured prediction model "masked-source"`,
		},
		{
			name:  "extra model",
			value: `{"echo-source":"DIRECT","masked-source":"SANDBOX","other":"DIRECT"}`,
			want:  `contains unconfigured prediction model "other"`,
		},
		{
			name:  "unknown exact value",
			value: `{"echo-source":"direct","masked-source":"SANDBOX"}`,
			want:  "must be exactly DIRECT or SANDBOX",
		},
		{
			name:  "wrong echo release mode",
			value: `{"echo-source":"SANDBOX","masked-source":"SANDBOX"}`,
			want:  "logical model echo to use DIRECT",
		},
		{
			name:  "wrong masked release mode",
			value: `{"echo-source":"DIRECT","masked-source":"DIRECT"}`,
			want:  "logical model gemini_masked to use SANDBOX",
		},
		{
			name:  "duplicate model key",
			value: `{"echo-source":"DIRECT","echo-source":"DIRECT","masked-source":"SANDBOX"}`,
			want:  `duplicate prediction model "echo-source"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			t.Setenv("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON", test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadAcceptsFourWalletPredictionRoutes(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-producer-current","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
		{"prediction_model_id":"echo-producer-current","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
		{"prediction_model_id":"masked-producer-current","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-6"},
		{"prediction_model_id":"masked-producer-current","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7"}
	]`)
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	t.Setenv("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON", `{"echo-producer-current":"DIRECT","masked-producer-current":"SANDBOX"}`)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !config.DecisionCycle.RequireCompleteModelCoverage || len(config.DecisionCycle.Bindings) != 4 ||
		config.DecisionCycle.Bindings[0].ExecutionAccountID != "main" ||
		config.DecisionCycle.Bindings[0].StrategyID != "multfactor_v2" ||
		config.DecisionCycle.Bindings[1].ExecutionAccountID != "wallet-1" ||
		config.DecisionCycle.Bindings[1].StrategyID != "multfactor_v1" ||
		config.DecisionCycle.Bindings[2].PredictionModelID != "masked-producer-current" ||
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
		{"prediction_model_id":"producer-a","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"account-b"}
	]`)
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "multiple logical models") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadAcceptsExplicitShadowDecisionCycle(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-producer-current","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
		{"prediction_model_id":"echo-producer-current","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
		{"prediction_model_id":"masked-producer-current","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-6"},
		{"prediction_model_id":"masked-producer-current","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7"}
	]`)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "false")
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	t.Setenv("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON", `{"echo-producer-current":"DIRECT","masked-producer-current":"SANDBOX"}`)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.DecisionCycle.OrderSubmissionEnabled {
		t.Fatal("shadow decision-cycle order submission was enabled")
	}
}

func TestLoadRejectsRemappedFourWalletRoutes(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-7"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-6"}
	]`)
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must use execution account") {
		t.Fatalf("Load() error = %v, want exact-route rejection", err)
	}
}

func TestLoadRejectsRemappedRetainedWalletRoutes(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"wallet-1"},
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"main"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-6"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7"}
	]`)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "true")
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must use execution account") {
		t.Fatalf("Load() error = %v, want retained-wallet route rejection", err)
	}
}

func TestLoadRejectsRetiredWalletTopology(t *testing.T) {
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
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "wallet-6") {
		t.Fatalf("Load() error = %v, want retired-wallet rejection", err)
	}
}

func TestLoadAcceptsRequiredMainWallet1EntryDisabledAccounts(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
		{"prediction_model_id":"echo-producer-v7","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-6"},
		{"prediction_model_id":"gemini-3.6-flash","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7"}
	]`)
	t.Setenv("DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON", `[" wallet-1 ","main"]`)
	t.Setenv("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "false")
	t.Setenv("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", "true")
	t.Setenv("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON", `{"echo-producer-v7":"DIRECT","gemini-3.6-flash":"SANDBOX"}`)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(config.DecisionCycle.EntryDisabledAccounts) != 2 ||
		config.DecisionCycle.EntryDisabledAccounts[0] != "wallet-1" ||
		config.DecisionCycle.EntryDisabledAccounts[1] != "main" {
		t.Fatalf("entry-disabled accounts = %#v", config.DecisionCycle.EntryDisabledAccounts)
	}
}

func TestLoadRejectsInvalidDecisionSubmissionDisabledAccounts(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "not array", value: `"wallet-3"`, want: "cannot unmarshal"},
		{name: "trailing value", value: `["account-a"] []`, want: "exactly one JSON array"},
		{name: "empty", value: `[" "]`, want: "account 0 is empty"},
		{name: "duplicate", value: `["wallet-7","wallet-7"]`, want: "duplicate account"},
		{name: "unbound", value: `["wallet-3"]`, want: "not a configured binding"},
		{name: "wallet quarantine", value: `["wallet-6"]`, want: "requires DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON=[]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			t.Setenv("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON", test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsGlobalSellOnlyGateForWallet67ShadowRelease(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "requires DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED=false") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsInvalidEntryDisabledAccounts(t *testing.T) {
	for _, value := range []string{`[]`, `["main"]`, `["wallet-6","wallet-7"]`, `["main","main"]`} {
		t.Run(value, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			t.Setenv("DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON", value)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() accepted entry-disabled accounts %s", value)
			}
		})
	}
}

func TestDecodeKalshiLiveBindingsRequiresExactIsolatedRoutes(t *testing.T) {
	bindings, err := decodeKalshiLiveBindings(`[
		{"model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main","api_key_id":"echo-key","private_key_path":"/run/secrets/kalshi-echo.pem"},
		{"model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7","api_key_id":"gemini-key","private_key_path":"/run/secrets/kalshi-gemini.pem"}
	]`)
	if err != nil || len(bindings) != 2 {
		t.Fatalf("bindings=%#v err=%v", bindings, err)
	}
	if bindings[0].StrategyID != domain.StrategyIDMultfactorV2 || bindings[1].ExecutionAccountID != "wallet-7" {
		t.Fatalf("bindings=%#v", bindings)
	}
	for _, invalid := range []string{
		`[{"model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main","api_key_id":"same","private_key_path":"relative.pem"}]`,
		`[{"model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main","api_key_id":"same","private_key_path":"/a.pem"},{"model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7","api_key_id":"same","private_key_path":"/b.pem"}]`,
	} {
		if _, err := decodeKalshiLiveBindings(invalid); err == nil {
			t.Fatalf("invalid bindings accepted: %s", invalid)
		}
	}
}

func TestLoadKeepsCurrentLiveAccountEntryGateWhenSchedulerDisabled(t *testing.T) {
	for _, test := range []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "missing entry accounts", key: "DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON", value: "", want: "main and wallet-1"},
		{name: "wrong entry accounts", key: "DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON", value: `["wallet-6","wallet-7"]`, want: "main and wallet-1"},
		{name: "quarantined account", key: "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON", value: `["wallet-6"]`, want: "requires DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON=[]"},
		{name: "global sell only", key: "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED", value: "true", want: "requires DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED=false"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			setCompleteLiveEnvironment(t)
			setCompleteDecisionCycleEnvironment(t)
			t.Setenv("DECISION_CYCLE_ENABLED", "false")
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
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

func TestLoadRejectsSingleBindingLiveDecisionSubmission(t *testing.T) {
	clearConfigEnvironment(t)
	setCompleteLiveEnvironment(t)
	setCompleteDecisionCycleEnvironment(t)
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-source","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"}
	]`)
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
		"KALSHI_MARKET_DATA_ENABLED", "KALSHI_API_URL", "KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH", "KALSHI_REQUEST_TIMEOUT",
		"DECISION_CYCLE_ENABLED", "DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", "DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED",
		"DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE",
		"DECISION_CYCLE_PREDICTION_INFRA_URL", "DECISION_CYCLE_PREDICTION_INFRA_TOKEN",
		"DECISION_CYCLE_STRATEGY_URL", "DECISION_CYCLE_STRATEGY_TOKEN",
		"DECISION_CYCLE_INTERVAL", "DECISION_CYCLE_STARTUP_DELAY", "DECISION_CYCLE_MAX_START_LATENESS", "DECISION_CYCLE_TIMEOUT",
		"DECISION_CYCLE_PREDICTION_LOOKBACK", "DECISION_CYCLE_MID_PRICE_LOOKBACK",
		"DECISION_CYCLE_BINDINGS_JSON", "DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON",
		"DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON",
		"DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON",
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
	t.Setenv("DECISION_CYCLE_BINDINGS_JSON", `[
		{"prediction_model_id":"echo-source","model_id":"echo","strategy_id":"multfactor_v2","execution_account_id":"main"},
		{"prediction_model_id":"echo-source","model_id":"echo","strategy_id":"multfactor_v1","execution_account_id":"wallet-1"},
		{"prediction_model_id":"masked-source","model_id":"gemini_masked","strategy_id":"multfactor_v1","execution_account_id":"wallet-6"},
		{"prediction_model_id":"masked-source","model_id":"gemini_masked","strategy_id":"multfactor_v2","execution_account_id":"wallet-7"}
	]`)
	t.Setenv("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON", `[]`)
	t.Setenv("DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON", `["main","wallet-1"]`)
	t.Setenv("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON", `{"echo-source":"DIRECT","masked-source":"SANDBOX"}`)
}

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
	t.Setenv("DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON", `["main","wallet-1"]`)
	t.Setenv("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON", `[]`)
}

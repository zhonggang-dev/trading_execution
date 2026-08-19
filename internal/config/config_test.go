package config

import (
	"strings"
	"testing"
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
	} {
		t.Setenv(key, "")
	}
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

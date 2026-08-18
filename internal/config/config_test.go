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

// TestLoadRefusesLiveAdapterPlaceholder 验证 Load Refuses Live Adapter Placeholder 场景下的行为。
func TestLoadRefusesLiveAdapterPlaceholder(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("EXECUTION_MODE", "live")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "real venue adapter") {
		t.Fatalf("Load() error = %v, want live adapter refusal", err)
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
		"DATABASE_CONNECT_TIMEOUT", "TRADING_EXECUTION_DATABASE_URL",
		"EXECUTION_API_TOKEN", "EXECUTION_MODE", "EXECUTION_VENUE", "EXECUTION_ALLOW_MARKET_ORDERS",
		"EXECUTION_MAX_ORDER_SIZE", "EXECUTION_MAX_ORDER_NOTIONAL",
		"ORDER_COORDINATOR_INTERVAL", "ORDER_COORDINATOR_BATCH_SIZE",
	} {
		t.Setenv(key, "")
	}
}

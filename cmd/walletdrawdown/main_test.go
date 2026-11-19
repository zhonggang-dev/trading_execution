package main

import (
	"testing"
	"time"
)

func TestLoadConfigDefaultsAndOverrides(t *testing.T) {
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccountID != "main" || cfg.Threshold.FloatString(0) != "50" || cfg.RenotifyStep.FloatString(0) != "20" || cfg.Window != time.Hour || cfg.Slot != 10*time.Minute {
		t.Fatalf("defaults = %#v", cfg)
	}
	env := map[string]string{"WALLET_DRAWDOWN_THRESHOLD_USD": "75.5", "WALLET_DRAWDOWN_WINDOW": "90m", "WALLET_DRAWDOWN_SLOT": "5m"}
	cfg, err = loadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Threshold.FloatString(1) != "75.5" || cfg.Window != 90*time.Minute || cfg.Slot != 5*time.Minute {
		t.Fatalf("overrides = %#v", cfg)
	}
	for _, bad := range []map[string]string{{"WALLET_DRAWDOWN_THRESHOLD_USD": "abc"}, {"WALLET_DRAWDOWN_THRESHOLD_USD": "-1"}, {"WALLET_DRAWDOWN_WINDOW": "1m"}} {
		if _, err := loadConfig(func(k string) string { return bad[k] }); err == nil {
			t.Fatalf("config %v must fail", bad)
		}
	}
}

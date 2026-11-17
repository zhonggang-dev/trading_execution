package main

import (
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/config"
)

func TestBuildWalletPnLCorrector(t *testing.T) {
	var cfg config.Config
	if corrector, err := buildWalletPnLCorrector(cfg, nil); err != nil || corrector != nil {
		t.Fatalf("unset corrections = %#v, %v", corrector, err)
	}
	cfg.Polymarket.WalletPnLCorrectionsJSON = `[{"wallet_address":"bad"}]`
	if _, err := buildWalletPnLCorrector(cfg, nil); err == nil {
		t.Fatal("expected invalid corrections error")
	}
	cfg.Polymarket.WalletPnLCorrectionsJSON = `[{"wallet_address":"0x7b14065318fc2f00e2c971bd4cb3770eda7f6d17","token_id":"1","effective_at":"2026-08-07T00:00:00Z","phantom_shares":"1","missing_cash":"0"}]`
	cfg.Polymarket.OrderFilledConfirmations = 1
	if _, err := buildWalletPnLCorrector(cfg, nil); err == nil {
		t.Fatal("expected missing POLYGON_RPC_URL error")
	}
}

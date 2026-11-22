package main

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/service/walletpnl"
)

func TestBuildWalletPnLServiceExcludesKalshiExecutionAccounts(t *testing.T) {
	// No database or network request should be necessary to reject an account
	// from another venue. The database value is only used to construct a reader.
	live := &liveRuntime{
		activeAccounts:     []string{"portfolio-a", "kalshi:portfolio-b"},
		polymarketAccounts: []string{"portfolio-a"},
	}
	cfg := config.Config{Polymarket: config.Polymarket{
		DataAPIURL: "https://data.example.invalid", RequestTimeout: time.Second,
	}}
	service, err := buildWalletPnLService(new(sql.DB), cfg, live)
	if err != nil || service == nil {
		t.Fatalf("service=%v error=%v", service, err)
	}
	for _, accountID := range []string{"kalshi:portfolio-b", "unmanaged"} {
		if _, err := service.Query(context.Background(), walletpnl.Query{ExecutionAccountID: accountID, Interval: "1w", Fidelity: "3h"}); !errors.Is(err, walletpnl.ErrUnknownAccount) {
			t.Fatalf("account %q error=%v, want unmanaged", accountID, err)
		}
	}
	if !reflect.DeepEqual(live.activeAccounts, []string{"portfolio-a", "kalshi:portfolio-b"}) {
		t.Fatalf("execution accounts changed: %v", live.activeAccounts)
	}
}

func TestBuildWalletPnLServiceRequiresPolymarketAccounts(t *testing.T) {
	service, err := buildWalletPnLService(new(sql.DB), config.Config{}, &liveRuntime{activeAccounts: []string{"kalshi:portfolio-a"}})
	if err != nil || service != nil {
		t.Fatalf("Kalshi-only service=%v error=%v, want unconfigured", service, err)
	}
}

func TestBuildWalletPnLServiceAccountSince(t *testing.T) {
	live := &liveRuntime{polymarketAccounts: []string{"wallet-2", "wallet-3"}}
	cfg := config.Config{Polymarket: config.Polymarket{
		DataAPIURL: "https://data.example.invalid", RequestTimeout: time.Second,
		WalletPnLAccountSinceJSON: `{"wallet-2":"2026-11-20T12:34:14Z","wallet-3":"2026-11-20T12:34:14Z"}`,
	}}
	if service, err := buildWalletPnLService(new(sql.DB), cfg, live); err != nil || service == nil {
		t.Fatalf("valid since service=%v error=%v", service, err)
	}
	// 写错的起点只停用官方盈亏曲线，交易服务照常启动。
	for _, raw := range []string{`{"wallet-9":"2026-11-20T12:34:14Z"}`, `{"wallet-2":"yesterday"}`, `not json`} {
		cfg.Polymarket.WalletPnLAccountSinceJSON = raw
		if service, err := buildWalletPnLService(new(sql.DB), cfg, live); err != nil || service != nil {
			t.Fatalf("invalid since %q service=%v error=%v, want disabled", raw, service, err)
		}
	}
}

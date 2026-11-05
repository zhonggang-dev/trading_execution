package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshipnl"
)

func TestKalshiPnLCompositionIsOptionalAndManagedOnly(t *testing.T) {
	if s, e := buildKalshiPnLService(config.Config{}, nil); s != nil || e != nil {
		t.Fatalf("paper=%v %v", s, e)
	}
	live := &liveRuntime{kalshiAccounts: []string{"kalshi:main"}, activeAccounts: []string{"main", "kalshi:main"}}
	// Missing official identity must not inspect even an invalid key path.
	cfg := config.Config{Kalshi: config.Kalshi{LiveAccounts: []config.KalshiLiveAccount{{ExecutionAccountID: "kalshi:main", PrivateKeyPath: "/must/not/read"}, {ExecutionAccountID: "kalshi:other", OfficialUserID: "not-managed", PrivateKeyPath: "/must/not/read"}}}}
	s, e := buildKalshiPnLService(cfg, live)
	if e != nil {
		t.Fatal(e)
	}
	list, e := s.ListAccounts(context.Background())
	if e != nil || len(list.Accounts) != 0 {
		t.Fatalf("list=%v err=%v", list, e)
	}
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "key.pem")
	if e := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600); e != nil {
		t.Fatal(e)
	}
	cfg.Kalshi.LiveAccounts[0] = config.KalshiLiveAccount{ExecutionAccountID: "kalshi:main", APIKeyID: "key-id", PrivateKeyPath: path, OfficialUserID: "11111111-2222-3333-4444-555555555555"}
	s, e = buildKalshiPnLService(cfg, live)
	if e != nil {
		t.Fatal(e)
	}
	list, e = s.ListAccounts(context.Background())
	if e != nil || !reflect.DeepEqual(list.Accounts, []kalshipnl.Account{{ExecutionAccountID: "kalshi:main", MarketSource: "KALSHI"}}) {
		t.Fatalf("list=%v err=%v", list, e)
	}
	if !reflect.DeepEqual(live.activeAccounts, []string{"main", "kalshi:main"}) {
		t.Fatal("trading account set changed")
	}
	cfg.Kalshi.LiveAccounts[0].OfficialUserID = "../bad"
	if _, e := buildKalshiPnLService(cfg, live); e == nil {
		t.Fatal("bad official identity accepted")
	}
}

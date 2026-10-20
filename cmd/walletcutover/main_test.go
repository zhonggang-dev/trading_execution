package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletcutover"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestRunReplaysExpiredAppliedBatchBeforeRPCAndRejectsDifferentHash(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}

	adminConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	adminDB := stdlib.OpenDB(*adminConfig)
	t.Cleanup(func() { _ = adminDB.Close() })
	if err := adminDB.PingContext(context.Background()); err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	schema := fmt.Sprintf("walletcutover_cli_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() { _, _ = adminDB.Exec(`DROP SCHEMA ` + schema + ` CASCADE`) })

	testConfig := adminConfig.Copy()
	testConfig.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*testConfig)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE execution_wallet_cutover_batches (
			batch_id TEXT PRIMARY KEY,
			manifest_sha256 TEXT NOT NULL,
			actor TEXT NOT NULL,
			reason TEXT NOT NULL
		);
		CREATE TABLE execution_wallet_cutover_accounts (
			batch_id TEXT NOT NULL REFERENCES execution_wallet_cutover_batches(batch_id),
			execution_account_id TEXT NOT NULL,
			wallet_address TEXT NOT NULL,
			PRIMARY KEY (batch_id, execution_account_id)
		);
		CREATE TABLE execution_accounts (
			execution_account_id TEXT PRIMARY KEY,
			version BIGINT NOT NULL
		);`); err != nil {
		t.Fatalf("create replay fixtures: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	manifest := walletcutover.Manifest{
		SchemaVersion: walletcutover.ManifestSchemaVersion,
		BatchID:       "cli-expired-replay",
		ObservedAt:    now.Add(-2 * time.Hour),
		ExpiresAt:     now.Add(-time.Hour),
		EvidenceBlock: walletcutover.EvidenceBlock{ChainID: walletcutover.PolygonChainID, Number: 100, Hash: "0x" + strings.Repeat("ab", 32), Confirmations: 128},
		Actor:         "cli-test-operator",
		Reason:        "expired replay",
		Accounts: []walletcutover.AccountManifest{
			cliReplayAccount("main", "0x1111111111111111111111111111111111111111"),
			cliReplayAccount("wallet-1", "0x2222222222222222222222222222222222222222"),
		},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	digestHex := hex.EncodeToString(digest[:])
	if _, err := db.Exec(`
		INSERT INTO execution_wallet_cutover_batches(batch_id,manifest_sha256,actor,reason)
		VALUES ($1,$2,$3,$4)`, manifest.BatchID, digestHex, manifest.Actor, manifest.Reason); err != nil {
		t.Fatalf("seed applied batch: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_wallet_cutover_accounts(batch_id,execution_account_id,wallet_address)
		VALUES ($1,'main','0x1111111111111111111111111111111111111111'),
		       ($1,'wallet-1','0x2222222222222222222222222222222222222222')`, manifest.BatchID); err != nil {
		t.Fatalf("seed applied accounts: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_accounts(execution_account_id,version)
		VALUES ('main',99),('wallet-1',99)`); err != nil {
		t.Fatalf("seed applied batch: %v", err)
	}
	manifestPath := filepath.Join(t.TempDir(), "expired-replay.json")
	if err := os.WriteFile(manifestPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRADING_EXECUTION_DATABASE_URL", databaseURLWithSearchPath(databaseURL, schema))
	t.Setenv("POLYGON_RPC_URL", "")
	t.Setenv("POLYGON_ORDER_FILLED_CONFIRMATIONS", "")

	var replayOutput bytes.Buffer
	if err := run([]string{"--manifest", manifestPath}, &replayOutput); err != nil {
		t.Fatalf("run exact expired replay: %v", err)
	}
	var replay output
	if err := json.Unmarshal(replayOutput.Bytes(), &replay); err != nil {
		t.Fatalf("decode replay envelope: %v", err)
	}
	if replay.Mode != "applied-replay" || replay.Result == nil || !replay.Result.AlreadyApplied || replay.Result.ManifestSHA != digestHex {
		t.Fatalf("unexpected replay envelope: %#v", replay)
	}

	// Change the reviewed bytes under the same batch id. The CLI must reject the
	// different hash before it checks RPC configuration or attempts any write.
	manifest.Reason = "different reviewed reason"
	tampered, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"--manifest", manifestPath}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "different manifest SHA256") {
		t.Fatalf("different batch hash error = %v", err)
	}
}

func cliReplayAccount(id, wallet string) walletcutover.AccountManifest {
	return walletcutover.AccountManifest{
		ExecutionAccountID: id,
		WalletAddress:      wallet,
		BeforeVersion:      1,
		Ledger: walletcutover.BalanceSnapshot{
			Asset: "pUSD", TotalBalance: domain.Decimal("10"),
			AvailableBalance: domain.Decimal("10"), ReservedBalance: domain.Decimal("0"),
		},
		ChainBalance: domain.Decimal("10"),
		Assets:       []walletcutover.AssetManifest{},
	}
}

func databaseURLWithSearchPath(databaseURL, schema string) string {
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return databaseURL
	}
	config.RuntimeParams["search_path"] = schema
	return stdlib.RegisterConnConfig(config)
}

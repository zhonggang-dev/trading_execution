package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestWalletCutoverBaselinesKeepDisjointHistoryAndOneEffectiveSet(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0035_wallet_reuse_cutover.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("apply 0035: %v", err)
	}
	const accountID = "main"
	const wallet = "0x1111111111111111111111111111111111111111"
	if _, err := db.Exec(`
		INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance)
		VALUES ($1,$2,'pUSD',10,10,0)`, accountID, wallet); err != nil {
		t.Fatal(err)
	}

	// Preserve an older sealed baseline for a token that is absent from the
	// later wallet-cutover batch.
	baselineTx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baseline_items
			(baseline_id,execution_account_id,token_id,condition_id,outcome_index,outcome_name,neg_risk,shares)
		VALUES ('legacy-baseline',$1,'legacy-token','legacy-condition',0,'YES',false,2)`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baselines
			(baseline_id,execution_account_id,source,observed_at,evidence,actor,reason)
		VALUES ('legacy-baseline',$1,'POLYMARKET_DATA_API',$2,'{"wallet_address":"`+wallet+`"}'::jsonb,'operator','legacy evidence')`, accountID, time.Now().UTC().Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := baselineTx.Commit(); err != nil {
		t.Fatal(err)
	}

	insertCutoverEvidence := func(batchID, eventID, tokenID, conditionID, chainShares, managedShares string, observedAt time.Time) {
		t.Helper()
		if _, err := db.Exec(`
			INSERT INTO execution_account_events
				(account_event_id,execution_account_id,event_type,total_balance_delta,available_balance_delta,reserved_balance_delta,
				 total_balance_after,available_balance_after,reserved_balance_after,occurred_at)
			VALUES ($1,$2,'CUTOVER_ADJUSTMENT',0,0,0,10,10,0,$3)`, eventID, accountID, observedAt); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
			INSERT INTO execution_wallet_cutover_batches
				(batch_id,schema_version,manifest_sha256,evidence_block_number,evidence_block_hash,observed_at,expires_at,actor,reason,evidence)
			VALUES ($1,'trading.wallet_reuse_cutover.v1',$2,100,'0x`+repeatedHex("ab", 32)+`',$3,$4,'operator','cutover','{"manifest":true}'::jsonb)`, batchID, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", observedAt, observedAt.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
			INSERT INTO execution_wallet_cutover_accounts
				(batch_id,execution_account_id,wallet_address,expected_version,ledger_before,available_before,reserved_before,
				 chain_balance,adjustment_delta,ledger_after,account_event_id,evidence)
			VALUES ($1,$2,$3,1,10,10,0,10,0,10,$4,'{"account":true}'::jsonb)`, batchID, accountID, wallet, eventID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
			INSERT INTO execution_wallet_cutover_assets
				(batch_id,execution_account_id,token_id,condition_id,market_id,outcome_index,outcome_name,neg_risk,
				 chain_shares,managed_shares,managed_cost,after_cost,ownership,residual_reason,evidence)
			VALUES ($1,$2,$3,$4,'market',0,'YES',false,$5,$6,0,0,'EXTERNAL','retained evidence','{"asset":true}'::jsonb)`, batchID, accountID, tokenID, conditionID, chainShares, managedShares); err != nil {
			t.Fatal(err)
		}
	}
	insertCutoverEvidence("cutover-one", "cutover-event-one", "123", "cutover-condition", "3", "1", time.Now().UTC().Add(-2*time.Hour))
	insertCutoverEvidence("cutover-two", "cutover-event-two", "0", "chain-zero-condition", "4", "1", time.Now().UTC().Add(-time.Hour))

	repository, err := NewExternalPositionBaselineRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	baselines, err := repository.ListExternalPositionBaselines(context.Background(), accountID)
	if err != nil {
		t.Fatal(err)
	}
	if len(baselines) != 3 {
		t.Fatalf("effective baselines = %#v, want legacy, disjoint cutover and token 0", baselines)
	}
	seen := map[string]bool{}
	setID := baselines[0].BaselineID
	for _, baseline := range baselines {
		seen[baseline.TokenID] = true
		if baseline.BaselineID != setID {
			t.Fatalf("baseline set ids differ: %#v", baselines)
		}
	}
	for _, tokenID := range []string{"legacy-token", "123", "0"} {
		if !seen[tokenID] {
			t.Fatalf("missing effective baseline token %q: %#v", tokenID, baselines)
		}
	}
}

func TestWalletCutoverReadRoleCanReadEvidenceButCannotWriteOrApply(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0035_wallet_reuse_cutover.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("apply 0035: %v", err)
	}
	const accountID = "main"
	const wallet = "0x1111111111111111111111111111111111111111"
	if _, err := db.Exec(`
		INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance)
		VALUES ($1,$2,'pUSD',10,10,0)`, accountID, wallet); err != nil {
		t.Fatal(err)
	}
	baselineTx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baseline_items
			(baseline_id,execution_account_id,token_id,condition_id,outcome_index,outcome_name,neg_risk,shares)
		VALUES ('reader-legacy',$1,'1','reader-condition',0,'YES',false,2)`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := baselineTx.Exec(`
		INSERT INTO execution_external_position_baselines
			(baseline_id,execution_account_id,source,observed_at,evidence,actor,reason)
		VALUES ('reader-legacy',$1,'POLYMARKET_DATA_API',$2,$3::jsonb,'operator','reader test')`, accountID, time.Now().UTC().Add(-time.Hour), `{"wallet_address":"`+wallet+`"}`); err != nil {
		t.Fatal(err)
	}
	if err := baselineTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_account_events
			(account_event_id,execution_account_id,event_type,total_balance_delta,available_balance_delta,reserved_balance_delta,
			 total_balance_after,available_balance_after,reserved_balance_after,occurred_at)
		VALUES ('reader-cutover-event',$1,'CUTOVER_ADJUSTMENT',0,0,0,10,10,0,clock_timestamp())`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_wallet_cutover_batches
			(batch_id,schema_version,manifest_sha256,evidence_block_number,evidence_block_hash,observed_at,expires_at,actor,reason,evidence)
		VALUES ('reader-cutover','trading.wallet_reuse_cutover.v1',$1,100,$2,clock_timestamp(),clock_timestamp()+interval '1 hour','operator','reader test','{"manifest":true}'::jsonb)`, strings.Repeat("a", 64), "0x"+strings.Repeat("ab", 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_wallet_cutover_accounts
			(batch_id,execution_account_id,wallet_address,expected_version,ledger_before,available_before,reserved_before,
			 chain_balance,adjustment_delta,ledger_after,account_event_id,evidence)
		VALUES ('reader-cutover',$1,$2,1,10,10,0,10,0,10,'reader-cutover-event','{"account":true}'::jsonb)`, accountID, wallet); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_wallet_cutover_assets
			(batch_id,execution_account_id,token_id,condition_id,market_id,outcome_index,outcome_name,neg_risk,
			 chain_shares,managed_shares,managed_cost,after_cost,ownership,residual_reason,evidence)
		VALUES ('reader-cutover',$1,'0','reader-zero-condition','reader-market',0,'YES',false,2,0,0,0,'EXTERNAL','reader residual','{"asset":true}'::jsonb)`, accountID); err != nil {
		t.Fatal(err)
	}

	adminConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	role := fmt.Sprintf("walletcutover_reader_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'reader-password'`); err != nil {
		t.Fatalf("create reader role: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP ROLE ` + role) })
	if _, err := db.Exec(`GRANT USAGE ON SCHEMA ` + schema + ` TO ` + role); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`GRANT SELECT ON
		execution_wallet_cutover_batches, execution_wallet_cutover_accounts, execution_wallet_cutover_assets,
		execution_wallet_cutover_lot_adjustments, execution_external_position_baselines,
		execution_external_position_baseline_items, execution_external_position_dispositions,
		execution_accounts, execution_wallet_migrations TO ` + role); err != nil {
		t.Fatal(err)
	}
	readerConfig := adminConfig.Copy()
	readerConfig.User = role
	readerConfig.Password = "reader-password"
	readerConfig.RuntimeParams["search_path"] = schema
	readerDB := stdlib.OpenDB(*readerConfig)
	t.Cleanup(func() { _ = readerDB.Close() })
	if err := readerDB.PingContext(context.Background()); err != nil {
		t.Fatalf("connect reader role: %v", err)
	}
	repository, err := NewExternalPositionBaselineRepository(readerDB)
	if err != nil {
		t.Fatal(err)
	}
	baselines, err := repository.ListExternalPositionBaselines(context.Background(), accountID)
	if err != nil || len(baselines) != 2 {
		t.Fatalf("reader baseline read = %#v err=%v", baselines, err)
	}
	if _, err := readerDB.Exec(`INSERT INTO execution_wallet_cutover_batches(batch_id,schema_version,manifest_sha256,evidence_block_number,evidence_block_hash,observed_at,expires_at,actor,reason,evidence) VALUES ('denied','trading.wallet_reuse_cutover.v1',$1,1,$2,clock_timestamp(),clock_timestamp()+interval '1 hour','x','x','{}'::jsonb)`, strings.Repeat("b", 64), "0x"+strings.Repeat("cd", 32)); err == nil {
		t.Fatal("reader role INSERT unexpectedly succeeded")
	}
	if _, err := readerDB.Exec(`UPDATE execution_wallet_cutover_batches SET reason='mutated' WHERE batch_id='reader-cutover'`); err == nil {
		t.Fatal("reader role UPDATE unexpectedly succeeded")
	}
	var applyResult []byte
	if err := readerDB.QueryRow(`SELECT execution_apply_wallet_reuse_cutover('{}'::jsonb,'reader-cutover',$1,'operator','reader test')`, strings.Repeat("a", 64)).Scan(&applyResult); err == nil {
		t.Fatal("reader role EXECUTE unexpectedly succeeded")
	}
}

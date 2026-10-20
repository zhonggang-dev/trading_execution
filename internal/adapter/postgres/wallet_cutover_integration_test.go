package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletcutover"
)

type cutoverTestChain struct{ report walletcutover.ChainReport }

func (chain cutoverTestChain) ChainID(context.Context) (uint64, error) {
	return chain.report.ChainID, nil
}
func (chain cutoverTestChain) LatestBlock(context.Context) (uint64, error) {
	return chain.report.LatestBlock, nil
}
func (chain cutoverTestChain) BlockHeader(context.Context, uint64) (string, time.Time, error) {
	return chain.report.BlockHash, chain.report.BlockTime, nil
}
func (chain cutoverTestChain) BalanceAt(_ context.Context, wallet string, _ uint64) (domain.Decimal, error) {
	for _, account := range chain.report.Accounts {
		if account.WalletAddress == wallet {
			return account.CashBalance, nil
		}
	}
	return "", os.ErrNotExist
}
func (chain cutoverTestChain) TokenBalanceAt(_ context.Context, wallet, token string, _ uint64) (domain.Decimal, error) {
	for _, account := range chain.report.Accounts {
		if account.WalletAddress != wallet {
			continue
		}
		for _, asset := range account.Assets {
			if asset.TokenID == token {
				return asset.Shares, nil
			}
		}
	}
	return "0", nil
}

func TestWalletCutoverDryRunAndApplyAreAtomic(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}
	db := newIntegrationDatabase(t, databaseURL)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0035_wallet_reuse_cutover.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("apply 0035: %v", err)
	}

	for _, account := range []struct{ id, wallet, total string }{
		{"main", "0x1111111111111111111111111111111111111111", "10"},
		{"wallet-1", "0x2222222222222222222222222222222222222222", "20"},
	} {
		if _, err := db.Exec(`INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance) VALUES ($1,$2,'pUSD',$3::numeric,$3::numeric,0)`, account.id, account.wallet, account.total); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO execution_risk_controls(execution_account_id,control_scope,control_key,paused,reason) VALUES ($1,'ACCOUNT','',true,'cutover')`, account.id); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	manifest := walletcutover.Manifest{
		SchemaVersion: walletcutover.ManifestSchemaVersion, BatchID: "integration-wallet-cutover",
		ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Actor: "integration-operator", Reason: "atomic baseline",
		EvidenceBlock: walletcutover.EvidenceBlock{ChainID: 137, Number: 100, Hash: "0x" + repeatedHex("ab", 32), Confirmations: 128},
		Accounts: []walletcutover.AccountManifest{
			cutoverAccount("main", "0x1111111111111111111111111111111111111111", "10", "8"),
			cutoverAccount("wallet-1", "0x2222222222222222222222222222222222222222", "20", "19"),
		},
	}
	chain := cutoverTestChain{report: walletcutover.ChainReport{ChainID: 137, BlockNumber: 100, BlockHash: manifest.EvidenceBlock.Hash, BlockTime: manifest.ObservedAt, LatestBlock: 300,
		Accounts: []walletcutover.ChainAccountReport{{ExecutionAccountID: "main", WalletAddress: manifest.Accounts[0].WalletAddress, CashBalance: "8"}, {ExecutionAccountID: "wallet-1", WalletAddress: manifest.Accounts[1].WalletAddress, CashBalance: "19"}}}}
	store, err := NewWalletCutoverStore(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := walletcutover.New(walletcutover.Params{Chain: chain, Store: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	report, _, err := service.Audit(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	params := walletcutover.ApplyParams{Manifest: manifest, ManifestRaw: raw, ManifestSHA: hex.EncodeToString(digest[:]), Actor: manifest.Actor, Reason: manifest.Reason, Chain: report}
	if _, err := service.Apply(context.Background(), params); err != nil {
		t.Fatalf("service dry-run: %v", err)
	}
	var balance string
	if err := db.QueryRow(`SELECT total_balance::text FROM execution_accounts WHERE execution_account_id='main'`).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != "10" {
		t.Fatalf("dry-run changed main balance to %s", balance)
	}
	params.Commit = true
	result, err := service.Apply(context.Background(), params)
	if err != nil {
		t.Fatalf("service apply: %v", err)
	}
	if !result.Applied || result.AlreadyApplied {
		t.Fatalf("unexpected apply result: %#v", result)
	}
	if err := db.QueryRow(`SELECT total_balance::text FROM execution_accounts WHERE execution_account_id='main'`).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != "8" {
		t.Fatalf("main balance = %s, want 8", balance)
	}
	var eventType string
	if err := db.QueryRow(`SELECT event_type FROM execution_account_events WHERE execution_account_id='main'`).Scan(&eventType); err != nil {
		t.Fatal(err)
	}
	if eventType != "CUTOVER_ADJUSTMENT" {
		t.Fatalf("event type = %s", eventType)
	}
	if _, err := service.Apply(context.Background(), params); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
}

func TestWalletCutoverReducesOldManagedLotWithoutPnL(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}
	db := newIntegrationDatabase(t, databaseURL)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0035_wallet_reuse_cutover.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("apply 0035: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.Exec(`INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance) VALUES ('main','0x1111111111111111111111111111111111111111','pUSD',10,10,0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO execution_risk_controls(execution_account_id,control_scope,control_key,paused,reason) VALUES ('main','ACCOUNT','',true,'cutover')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance) VALUES ('wallet-1','0x2222222222222222222222222222222222222222','pUSD',0,0,0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO execution_risk_controls(execution_account_id,control_scope,control_key,paused,reason) VALUES ('wallet-1','ACCOUNT','',true,'cutover')`); err != nil {
		t.Fatal(err)
	}
	insertOpenLotFixtureNamedWithModel(t, db, "main", "123", "lot-old", "1", "echo")
	if _, err := db.Exec(`INSERT INTO execution_positions(execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,total_shares,available_shares,reserved_shares,cost_basis,average_cost_price,realized_pnl,market_value,unrealized_pnl,lifecycle_status) VALUES ('main','market-1','condition-1','123',0,'Yes',1,1,0,0.5,0.5,0,0,0,'OPEN')`); err != nil {
		t.Fatal(err)
	}
	manifest := walletcutover.Manifest{SchemaVersion: walletcutover.ManifestSchemaVersion, BatchID: "managed-lot-cutover", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Actor: "operator", Reason: "old lot exit evidence", EvidenceBlock: walletcutover.EvidenceBlock{ChainID: 137, Number: 100, Hash: "0x" + repeatedHex("ab", 32), Confirmations: 128}, Accounts: []walletcutover.AccountManifest{
		cutoverAccount("main", "0x1111111111111111111111111111111111111111", "10", "10"),
		cutoverAccount("wallet-1", "0x2222222222222222222222222222222222222222", "0", "0"),
	}}
	asset := walletcutover.AssetManifest{TokenID: "123", ConditionID: "condition-1", MarketID: "market-1", OutcomeName: "Yes", ChainShares: "0.5", ManagedShares: "1", ManagedCost: "0.5", AfterCost: "0.25", Ownership: "OLD_MANAGED", LotAdjustments: []walletcutover.LotAdjustment{{LotID: "lot-old", BeforeShares: "1", BeforeCost: "0.5", AfterShares: "0.5", AfterCost: "0.25", Reason: "verified external sell remainder"}}}
	manifest.Accounts[0].Assets = []walletcutover.AssetManifest{asset}
	chain := cutoverTestChain{report: walletcutover.ChainReport{ChainID: 137, BlockNumber: 100, BlockHash: manifest.EvidenceBlock.Hash, BlockTime: manifest.ObservedAt, LatestBlock: 300, Accounts: []walletcutover.ChainAccountReport{{ExecutionAccountID: "main", WalletAddress: manifest.Accounts[0].WalletAddress, CashBalance: "10", Assets: []walletcutover.ChainAssetReport{{TokenID: "123", Shares: "0.5"}}}, {ExecutionAccountID: "wallet-1", WalletAddress: manifest.Accounts[1].WalletAddress, CashBalance: "0"}}}}
	store, err := NewWalletCutoverStore(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := walletcutover.New(walletcutover.Params{Chain: chain, Store: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	report, _, err := service.Audit(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	_, err = service.Apply(context.Background(), walletcutover.ApplyParams{Manifest: manifest, ManifestRaw: raw, ManifestSHA: hex.EncodeToString(digest[:]), Actor: manifest.Actor, Reason: manifest.Reason, Chain: report, Commit: true})
	if err != nil {
		t.Fatalf("apply old managed lot adjustment: %v", err)
	}
	var shares, cost, status string
	if err := db.QueryRow(`SELECT remaining_shares::text, remaining_cost::text, status FROM position_lots WHERE lot_id='lot-old'`).Scan(&shares, &cost, &status); err != nil {
		t.Fatal(err)
	}
	if !sameNumeric(shares, "0.5") || !sameNumeric(cost, "0.25") || status != "OPEN" {
		t.Fatalf("lot=(%s,%s,%s)", shares, cost, status)
	}
	var eventType string
	var pnl string
	if err := db.QueryRow(`SELECT event_type, realized_pnl_delta::text FROM position_events WHERE event_type='CUTOVER_ADJUSTED'`).Scan(&eventType, &pnl); err != nil {
		t.Fatal(err)
	}
	if eventType != "CUTOVER_ADJUSTED" || !sameNumeric(pnl, "0") {
		t.Fatalf("event=(%s,%s)", eventType, pnl)
	}
}

func TestWalletCutoverRequiresEveryLocalAssetIncludingTokenZero(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}
	db := newIntegrationDatabase(t, databaseURL)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0035_wallet_reuse_cutover.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("apply 0035: %v", err)
	}
	for _, account := range []struct{ id, wallet string }{
		{"main", "0x1111111111111111111111111111111111111111"},
		{"wallet-1", "0x2222222222222222222222222222222222222222"},
	} {
		if _, err := db.Exec(`
			INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance)
			VALUES ($1,$2,'pUSD',10,10,0)`, account.id, account.wallet); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
			INSERT INTO execution_risk_controls(execution_account_id,control_scope,control_key,paused,reason)
			VALUES ($1,'ACCOUNT','',true,'cutover')`, account.id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO execution_positions(
			execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,
			total_shares,available_shares,reserved_shares,cost_basis,average_cost_price,
			realized_pnl,market_value,unrealized_pnl,is_dust,lifecycle_status)
		VALUES ('main','market-zero','condition-zero','0',0,'YES',1,1,0,0.5,0.5,0,0,0,false,'OPEN')`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	manifest := walletcutover.Manifest{
		SchemaVersion: walletcutover.ManifestSchemaVersion, BatchID: "coverage-token-zero",
		ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Actor: "operator", Reason: "coverage",
		EvidenceBlock: walletcutover.EvidenceBlock{ChainID: 137, Number: 100, Hash: "0x" + repeatedHex("ab", 32), Confirmations: 128},
		Accounts: []walletcutover.AccountManifest{
			cutoverAccount("main", "0x1111111111111111111111111111111111111111", "10", "10"),
			cutoverAccount("wallet-1", "0x2222222222222222222222222222222222222222", "10", "10"),
		},
	}
	chain := cutoverTestChain{report: walletcutover.ChainReport{
		ChainID: 137, BlockNumber: 100, BlockHash: manifest.EvidenceBlock.Hash,
		BlockTime: manifest.ObservedAt, LatestBlock: 300,
		Accounts: []walletcutover.ChainAccountReport{
			{ExecutionAccountID: "main", WalletAddress: manifest.Accounts[0].WalletAddress, CashBalance: "10"},
			{ExecutionAccountID: "wallet-1", WalletAddress: manifest.Accounts[1].WalletAddress, CashBalance: "10"},
		},
	}}
	store, err := NewWalletCutoverStore(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := walletcutover.New(walletcutover.Params{Chain: chain, Store: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Audit(context.Background(), manifest); err == nil {
		t.Fatal("Audit accepted a manifest omitting local token 0")
	}
}

func TestWalletCutoverResolvesOnlyExplicitCoveredIssues(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not configured")
	}
	db := newIntegrationDatabase(t, databaseURL)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0035_wallet_reuse_cutover.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatalf("apply 0035: %v", err)
	}
	for _, account := range []struct{ id, wallet string }{
		{"main", "0x1111111111111111111111111111111111111111"},
		{"wallet-1", "0x2222222222222222222222222222222222222222"},
	} {
		if _, err := db.Exec(`
			INSERT INTO execution_accounts(execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance)
			VALUES ($1,$2,'pUSD',10,10,0)`, account.id, account.wallet); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`
			INSERT INTO execution_risk_controls(execution_account_id,control_scope,control_key,paused,reason)
			VALUES ($1,'ACCOUNT','',true,'cutover')`, account.id); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.Exec(`
		INSERT INTO reconciliation_runs(run_id,execution_account_id,trigger,status,summary,error,started_at,completed_at)
		VALUES ('cutover-issue-run','main','ASSET_DRIFT','ATTENTION_REQUIRED','{}','',$1,$2)`, now.Add(-time.Hour), now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO reconciliation_issues(issue_id,run_id,fingerprint,execution_account_id,issue_type,resolution,status,token_id,source,details,observed_at)
		VALUES
		 ('cutover-balance-issue','cutover-issue-run','cutover-balance','main','BALANCE_DRIFT','MANUAL_REVIEW','OPEN','','EVM_ERC20_ETH_CALL','reviewed balance drift',$1),
		 ('cutover-unrelated-issue','cutover-issue-run','cutover-unrelated','main','EXTERNAL_TRADE','MANUAL_REVIEW','OPEN','other-token','POLYMARKET_DATA_API','must remain open',$1)`, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	manifest := walletcutover.Manifest{
		SchemaVersion: walletcutover.ManifestSchemaVersion, BatchID: "issue-cutover", ObservedAt: now.Add(-time.Minute),
		ExpiresAt: now.Add(time.Hour), Actor: "operator", Reason: "reviewed issue closure",
		EvidenceBlock: walletcutover.EvidenceBlock{ChainID: 137, Number: 100, Hash: "0x" + repeatedHex("ab", 32), Confirmations: 128},
		Accounts: []walletcutover.AccountManifest{
			cutoverAccount("main", "0x1111111111111111111111111111111111111111", "10", "10"),
			cutoverAccount("wallet-1", "0x2222222222222222222222222222222222222222", "10", "10"),
		},
	}
	manifest.Accounts[0].OpenIssueIDs = []string{"cutover-balance-issue"}
	chain := cutoverTestChain{report: walletcutover.ChainReport{
		ChainID: 137, BlockNumber: 100, BlockHash: manifest.EvidenceBlock.Hash, BlockTime: manifest.ObservedAt, LatestBlock: 300,
		Accounts: []walletcutover.ChainAccountReport{
			{ExecutionAccountID: "main", WalletAddress: manifest.Accounts[0].WalletAddress, CashBalance: "10"},
			{ExecutionAccountID: "wallet-1", WalletAddress: manifest.Accounts[1].WalletAddress, CashBalance: "10"},
		},
	}}
	store, err := NewWalletCutoverStore(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := walletcutover.New(walletcutover.Params{Chain: chain, Store: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	report, _, err := service.Audit(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if _, err := service.Apply(context.Background(), walletcutover.ApplyParams{
		Manifest: manifest, ManifestRaw: raw, ManifestSHA: hex.EncodeToString(digest[:]), Actor: manifest.Actor, Reason: manifest.Reason,
		Chain: report, Commit: true,
	}); err != nil {
		t.Fatalf("apply issue cutover: %v", err)
	}
	var status, details string
	if err := db.QueryRow(`SELECT status,details FROM reconciliation_issues WHERE issue_id='cutover-balance-issue'`).Scan(&status, &details); err != nil {
		t.Fatal(err)
	}
	if status != "RESOLVED" || !strings.Contains(details, "issue-cutover") {
		t.Fatalf("explicit issue=(%s,%s)", status, details)
	}
	if err := db.QueryRow(`SELECT status FROM reconciliation_issues WHERE issue_id='cutover-unrelated-issue'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "OPEN" {
		t.Fatalf("unrelated issue status=%s, want OPEN", status)
	}
}

func cutoverAccount(id, wallet, ledger, chain string) walletcutover.AccountManifest {
	return walletcutover.AccountManifest{ExecutionAccountID: id, WalletAddress: wallet, BeforeVersion: 1,
		Ledger:       walletcutover.BalanceSnapshot{Asset: "pUSD", TotalBalance: domain.Decimal(ledger), AvailableBalance: domain.Decimal(ledger), ReservedBalance: "0"},
		ChainBalance: domain.Decimal(chain), Assets: []walletcutover.AssetManifest{}}
}

func repeatedHex(value string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += value
	}
	return result
}

package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

type historicalVerifierFixture struct {
	sales map[string]RebasedSale
	bad   bool
}

func (f historicalVerifierFixture) ReadHistoricalSell(_ context.Context, r evmrpc.OrderFilledEvidenceRequest, collateral string) (evmrpc.HistoricalSellEvidence, error) {
	s, ok := f.sales[r.TransactionHash]
	if !ok || collateral != polymarket.PUSDAddress {
		return evmrpc.HistoricalSellEvidence{}, fmt.Errorf("unexpected evidence request")
	}
	units := func(a string) string {
		v, _ := new(big.Rat).SetString(a)
		return v.Mul(v, big.NewRat(1000000, 1)).Num().String()
	}
	confirmations := uint64(128)
	if f.bad {
		confirmations = 1
	}
	return evmrpc.HistoricalSellEvidence{Fill: evmrpc.OrderFilledEvidence{TransactionHash: s.TransactionHash, OrderHash: s.OrderHash, Maker: r.Maker, Side: evmrpc.OrderSideSell, TokenID: s.TokenID, LogIndex: s.FillLogIndex, BlockNumber: s.BlockNumber, BlockHash: s.BlockHash, Confirmations: confirmations, MakerAmountBaseUnits: units(s.TradeShares), TakerAmountBaseUnits: units(s.TradeGross), FeeBaseUnits: units(s.TradeFee)}, SoldAt: s.SoldAt, ReceiptSHA256: strings.Repeat("a", 64)}, nil
}
func historicalManifestFixture() RebasedSaleManifest {
	sold := time.Date(2026, 10, 20, 13, 14, 0, 0, time.UTC)
	return RebasedSaleManifest{Sales: []RebasedSale{
		{Account: "history-a", LotID: "history-lot-a", MarketID: "history-market-a", ConditionID: "0x" + strings.Repeat("2", 64), TokenID: "123", TransactionHash: "0x" + strings.Repeat("3", 64), OrderHash: "0x" + strings.Repeat("4", 64), BlockNumber: 100, BlockHash: "0x" + strings.Repeat("5", 64), FillLogIndex: 7, TradeShares: "0.89", TradeGross: "0.1157", TradeFee: "0", Shares: "0.14", Gross: "0.0182", Fee: "0", Principal: "0.02492", SoldAt: sold, RebaseEventID: "history-rebase-a", OfficialActivitySHA256: strings.Repeat("6", 64), OfficialActivityTimestamp: sold.Unix()},
		{Account: "history-b", LotID: "history-lot-b", MarketID: "history-market-b", ConditionID: "0x" + strings.Repeat("7", 64), TokenID: "456", TransactionHash: "0x" + strings.Repeat("8", 64), OrderHash: "0x" + strings.Repeat("9", 64), BlockNumber: 101, BlockHash: "0x" + strings.Repeat("a", 64), FillLogIndex: 8, TradeShares: "1", TradeGross: "0.87", TradeFee: "0", Shares: "1", Gross: "0.87", Fee: "0", Principal: "0.5526", SoldAt: sold.Add(9 * time.Second), RebaseEventID: "history-rebase-b", OfficialActivitySHA256: strings.Repeat("b", 64), OfficialActivityTimestamp: sold.Add(9 * time.Second).Unix()},
	}}
}
func seedHistoricalAdoption(t *testing.T, db *sql.DB, s RebasedSale, ordinal int) {
	t.Helper()
	wallet := "0x" + strings.Repeat(fmt.Sprint(ordinal), 40)
	insertAccount(t, db, s.Account, wallet, "100", "100", "0")
	run := func(tx *sql.Tx, q string, args ...any) {
		t.Helper()
		if _, err := tx.Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	run(tx, `INSERT INTO execution_strategy_bindings(model_id,strategy_id,execution_account_id,enabled) VALUES('echo-'||$1,'multfactor_v1',$1,true)`, s.Account)
	base, batch, adoption, disposition, event := "base-"+s.Account, "batch-"+s.Account, "adopt-"+s.Account, "disposition-"+s.Account, "event-"+s.Account
	opened, adopted := s.SoldAt.Add(-30*24*time.Hour), s.SoldAt.Add(-29*24*time.Hour)
	run(tx, `INSERT INTO execution_external_position_baseline_items(baseline_id,execution_account_id,token_id,condition_id,outcome_index,outcome_name,neg_risk,shares) VALUES($1,$2,$3,$4,1,'No',false,$5)`, base, s.Account, s.TokenID, s.ConditionID, s.Shares)
	run(tx, `INSERT INTO execution_external_position_baselines(baseline_id,execution_account_id,source,observed_at,evidence,actor,reason) VALUES($1,$2,'POLYMARKET_DATA_API',$3,'{"fixture":"historical"}','test','initial capture')`, base, s.Account, opened)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	run(tx, `INSERT INTO execution_external_position_dispositions(disposition_id,adjustment_batch_id,baseline_id,execution_account_id,condition_id,token_id,disposition_kind,transition_sequence,shares_before,shares_delta,shares_after,occurred_at,evidence,actor,reason) VALUES($1,$2,$3,$4,$5,$6,'ADOPTION',1,$7,$7,0,$8,'{"source":"fixture"}','test','adoption')`, disposition, batch, base, s.Account, s.ConditionID, s.TokenID, s.Shares, adopted)
	run(tx, `INSERT INTO execution_external_position_adoptions(external_adoption_id,adjustment_batch_id,disposition_id,baseline_id,execution_account_id,lot_id,position_event_id,market_id,condition_id,token_id,outcome_index,outcome_name,neg_risk,model_id,strategy_id,shares,remaining_cost,average_entry_price,opened_at,adopted_at,evidence,actor,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1,'No',false,'echo-'||$5,'multfactor_v1',$11,$12,$12::numeric/$11::numeric,$13,$14,'{"source":"fixture"}','test','adoption')`, adoption, batch, disposition, base, s.Account, s.LotID, event, s.MarketID, s.ConditionID, s.TokenID, s.Shares, s.Principal, opened, adopted)
	run(tx, `INSERT INTO execution_positions(execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,total_shares,available_shares,reserved_shares,cost_basis,average_cost_price,realized_pnl,market_value,unrealized_pnl,lifecycle_status) VALUES($1,$2,$3,$4,1,'No',$5,$5,0,$6,$6::numeric/$5::numeric,0,0,0,'OPEN')`, s.Account, s.MarketID, s.ConditionID, s.TokenID, s.Shares, s.Principal)
	run(tx, `INSERT INTO position_lots(lot_id,execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,neg_risk,model_id,strategy_id,opening_order_id,opening_fill_key,external_adoption_id,original_shares,remaining_shares,original_cost,remaining_cost,average_entry_price,status,opened_at) VALUES($1,$2,$3,$4,$5,1,'No',false,'echo-'||$2,'multfactor_v1',NULL,NULL,$6,$7,$7,$8,$8,$8::numeric/$7::numeric,'OPEN',$9)`, s.LotID, s.Account, s.MarketID, s.ConditionID, s.TokenID, adoption, s.Shares, s.Principal, opened)
	run(tx, `INSERT INTO position_events(position_event_id,event_type,execution_account_id,market_id,token_id,order_id,fill_key,external_adoption_id,model_id,strategy_id,shares_delta,cash_delta,cost_basis_delta,realized_pnl_delta,shares_after,cost_basis_after,average_cost_after,realized_pnl_after,unrealized_pnl_after,occurred_at) VALUES($1,'ADOPTED',$2,$3,$4,'','',$5,'echo-'||$2,'multfactor_v1',$6,0,$7,0,$6,$7,$7::numeric/$6::numeric,0,0,$8)`, event, s.Account, s.MarketID, s.TokenID, adoption, s.Shares, s.Principal, adopted)
	run(tx, `INSERT INTO execution_external_position_adjustment_batches(adjustment_batch_id,schema_version,observed_at,evidence,evidence_sha256,actor,reason) VALUES($1,'trading.external-position-adjustment.v1',$2,'{"source":"fixture"}',$3,'test','adoption')`, batch, adopted, strings.Repeat("a", 64))
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	closed := time.Date(s.SoldAt.Year(), s.SoldAt.Month(), s.SoldAt.Day(), 13, 18, 18, 0, time.UTC)
	run(tx, `UPDATE position_lots SET remaining_shares=0,remaining_cost=0,status='CLOSED',closed_at=$2 WHERE lot_id=$1`, s.LotID, closed)
	run(tx, `UPDATE execution_positions SET total_shares=0,available_shares=0,cost_basis=0,average_cost_price=0,lifecycle_status='CLOSED' WHERE execution_account_id=$1`, s.Account)
	run(tx, `INSERT INTO execution_account_events(account_event_id,execution_account_id,event_type,order_id,fill_key,total_balance_delta,available_balance_delta,reserved_balance_delta,total_balance_after,available_balance_after,reserved_balance_after,occurred_at) VALUES($1,$2,'MANUAL_REBASE','','',0,0,0,100,100,0,$3)`, s.RebaseEventID, s.Account, closed)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresIntegrationRebasedHistoricalSalesDryRunApplyAndRead(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("isolated PG required")
	}
	db := newIntegrationDatabase(t, url)
	m := historicalManifestFixture()
	var reviewedRaw []byte
	var err error
	if path := os.Getenv("REBASED_SALES_REHEARSAL_MANIFEST"); path != "" {
		reviewedRaw, err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		m, err = DecodeRebasedSaleManifest(reviewedRaw)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i, s := range m.Sales {
		seedHistoricalAdoption(t, db, s, i+1)
	}
	verifier := historicalVerifierFixture{sales: map[string]RebasedSale{}}
	for _, s := range m.Sales {
		verifier.sales[s.TransactionHash] = s
	}
	raw, _ := json.Marshal(m)
	if reviewedRaw != nil {
		raw = reviewedRaw
	}
	managed := []consolev2.ManagedAccount{}
	for _, s := range m.Sales {
		managed = append(managed, consolev2.ManagedAccount{ExecutionAccountID: s.Account, MarketSource: "POLYMARKET"})
	}
	reader := consolev2.New(db, consolev2.RuntimeConfig{ManagedAccounts: managed})
	before, err := reader.CohortMetricFacts(context.Background(), consolev2.CohortFilter{})
	if err != nil || len(before.UnavailableAccounts) != 2 {
		t.Fatal("missing history fixture", before, err)
	}
	report, err := RunRebasedHistoricalSales(context.Background(), db, verifier, raw, false, "", "test", "verified old rebase history")
	if err != nil || report.Applied || report.CashEffect != "0" || report.OriginalFactsBefore != report.OriginalFactsAfter {
		t.Fatal(report, err)
	}
	if v := scalar(t, db, `SELECT count(*)::text FROM historical_rebased_lot_sales`); v != "0" {
		t.Fatal("dry run committed", v)
	}
	if path := os.Getenv("REBASED_SALES_REHEARSAL_REPORT"); path != "" && reviewedRaw != nil {
		body, _ := json.MarshalIndent(map[string]any{"scope": "isolated PostgreSQL only; canonical production receipts were verified separately", "evidence_verifier": "fixture (no network in this database rehearsal)", "report": report}, "", "  ")
		if err = os.WriteFile(path, append(body, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}

	// A second-row SQL rejection must roll back the first valid insert.
	badManifest := m
	badManifest.Sales = append([]RebasedSale{}, m.Sales...)
	badManifest.Sales[1].RebaseEventID = badManifest.Sales[0].RebaseEventID
	badRaw, _ := json.Marshal(badManifest)
	if _, e := RunRebasedHistoricalSales(context.Background(), db, verifier, badRaw, true, fmt.Sprintf("%x", sha256.Sum256(badRaw)), "test", "wrong rebase"); e == nil {
		t.Fatal("wrong second-row rebase accepted")
	}
	if v := scalar(t, db, `SELECT count(*)::text FROM historical_rebased_lot_sales`); v != "0" {
		t.Fatal("rejected second row left first insert", v)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	bad := verifier
	bad.bad = true
	if _, err = RunRebasedHistoricalSales(context.Background(), db, bad, raw, true, hash, "test", "bad proof"); err == nil {
		t.Fatal("low-finality proof accepted")
	}
	report, err = RunRebasedHistoricalSales(context.Background(), db, verifier, raw, true, hash, "test", "verified old rebase history")
	if err != nil || !report.Applied || report.OriginalFactsBefore != report.OriginalFactsAfter {
		t.Fatal(report, err)
	}
	got, err := core.New(reader, nil).QueryCohort(context.Background(), consolev2.CohortFilter{})
	if err != nil || len(got.UnavailableAccounts) != 0 || len(got.Items) != 2 {
		t.Fatal(got, err)
	}
	for _, a := range got.Items {
		if a.AssetCount != 0 || a.ClosedAssetCount != 1 || a.MarketValue == nil || *a.MarketValue != "0" {
			t.Fatal(a)
		}
		expected := "-0.00672"
		if a.ExecutionAccountID == m.Sales[1].Account {
			expected = "0.3174"
		}
		assertConsoleAmount(t, a.RealizedPnL, expected)
		assertConsoleAmount(t, *a.TotalPnL, expected)
	}
	replay, err := RunRebasedHistoricalSales(context.Background(), db, verifier, raw, true, hash, "test", "replay")
	if err != nil || !replay.AlreadyApplied {
		t.Fatal("idempotency", replay, err)
	}
	changed := append([]byte("\n"), raw...)
	if _, err = RunRebasedHistoricalSales(context.Background(), db, verifier, changed, true, fmt.Sprintf("%x", sha256.Sum256(changed)), "test", "different batch"); err == nil {
		t.Fatal("conflicting provenance accepted")
	}
	for _, q := range []string{`UPDATE historical_rebased_lot_sales SET cash_effect=0`, `DELETE FROM historical_rebased_lot_sales`, `TRUNCATE historical_rebased_lot_sales`} {
		if _, err = db.Exec(q); err == nil {
			t.Fatal("append-only audit mutated", q)
		}
	}
	for _, s := range m.Sales {
		assertAccount(t, db, s.Account, "100", "100", "0")
	}
	early := consolev2.New(db, consolev2.RuntimeConfig{ManagedAccounts: managed, Now: func() time.Time { return m.Sales[0].SoldAt.Add(-time.Second) }})
	earlyFacts, e := early.CohortMetricFacts(context.Background(), consolev2.CohortFilter{})
	if e != nil || len(earlyFacts.UnavailableAccounts) != 2 {
		t.Fatal("future sale entered earlier observation", earlyFacts, e)
	}
	// Shadow only these relations to exercise post-repair duplicate evidence
	// without weakening any production constraint or touching the real audit.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, table := range []string{"execution_fills", "position_lot_closures"} {
		if _, e = db.Exec(`CREATE TEMP TABLE ` + table + ` AS SELECT * FROM ` + table + ` WITH NO DATA`); e != nil {
			t.Fatal(e)
		}
	}
	sale := m.Sales[0]
	if _, e = db.Exec(`INSERT INTO execution_fills(fill_key,execution_account_id,market_id,condition_id,token_id,side,status,shares,total_fee,applied_at) VALUES('duplicate',$1,$2,$3,$4,'SELL','CONFIRMED',$5,0,$6)`, sale.Account, sale.MarketID, sale.ConditionID, sale.TokenID, sale.Shares, sale.SoldAt); e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(`INSERT INTO position_lot_closures(closure_id,closing_fill_key,lot_id,closed_shares,allocated_net_proceeds,closed_at) VALUES('duplicate','duplicate',$1,$2,$3,$4)`, sale.LotID, sale.Shares, sale.Gross, sale.SoldAt); e != nil {
		t.Fatal(e)
	}
	if _, e = reader.CohortMetricFacts(context.Background(), consolev2.CohortFilter{}); e == nil {
		t.Fatal("later duplicate closure was hidden")
	}

}

func TestPostgresIntegrationHistoricalSaleRuntimeRoleIsReadOnly(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("isolated PG required")
	}
	admin, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err = admin.Exec(`DO $$ BEGIN IF to_regrole('trading_execution_app') IS NULL THEN CREATE ROLE trading_execution_app NOLOGIN; END IF; END $$`); err != nil {
		t.Fatal(err)
	}
	db := newIntegrationDatabase(t, url)
	var schema string
	if err = db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`GRANT USAGE ON SCHEMA ` + schema + ` TO trading_execution_app`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`SET LOCAL ROLE trading_execution_app`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = tx.QueryRow(`SELECT count(*) FROM historical_rebased_lot_sales`).Scan(&count); err != nil {
		t.Fatal("runtime cannot read audit", err)
	}
	_ = tx.Rollback()
	for _, q := range []string{`INSERT INTO historical_rebased_lot_sales DEFAULT VALUES`, `UPDATE historical_rebased_lot_sales SET cash_effect=0`, `DELETE FROM historical_rebased_lot_sales`, `TRUNCATE historical_rebased_lot_sales`} {
		tx, err = db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(`SET LOCAL ROLE trading_execution_app`); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(q)
		_ = tx.Rollback()
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatal("runtime write did not fail at privilege boundary", q, err)
		}
	}
}

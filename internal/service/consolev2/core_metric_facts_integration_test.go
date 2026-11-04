package consolev2

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func prepareCoreFacts(t *testing.T) performanceFixture {
	f := newPerformanceFixture(t)
	f.exec(t, `UPDATE execution_accounts SET wallet_address='0x1111111111111111111111111111111111111111'`)
	f.exec(t, `CREATE TEMP TABLE execution_positions AS SELECT * FROM execution_positions WITH NO DATA`)
	return f
}
func TestCoreMetricFactsSameAssetBasisAccountingAndSettlementEvidence(t *testing.T) {
	f := prepareCoreFacts(t)
	f.exec(t, `INSERT INTO execution_positions(execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,lifecycle_status,settlement_price,settlement_source,settled_at) VALUES('pm','market-p','condition-p','token-p',0,'Yes','SETTLED_PENDING_REDEEM',0,'POLYMARKET_DATA_API',$1)`, f.now.Add(-time.Hour))
	got, err := f.reader.CoreMetricFacts(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Accounts) != 3 {
		t.Fatal("managed registry", len(got.Accounts))
	}
	var pm CoreAccountFact
	for _, a := range got.Accounts {
		if a.Metrics.ExecutionAccountID == "pm" {
			pm = a
		}
	}
	if len(pm.Assets) != 1 || pm.Assets[0].RemainingShares != "5" || pm.Assets[0].RemainingCost != "1.3" || pm.Assets[0].SettlementPrice == nil || *pm.Assets[0].SettlementPrice != "0" {
		t.Fatalf("assets %+v", pm.Assets)
	}
	if pm.Metrics.PeakHoldingCost != "15" || pm.Metrics.CumulativeInvestedCost != "15.3" || pm.Metrics.RealizedPnL != "102" || pm.Metrics.RealizedDrawdown14D != "5" {
		t.Fatalf("ledger %+v", pm.Metrics)
	}
	f.exec(t, `UPDATE execution_positions SET condition_id='wrong'`)
	got, err = f.reader.CoreMetricFacts(context.Background(), "pm")
	if err != nil || got.Accounts[0].Assets[0].SettlementPrice != nil {
		t.Fatal("wrong settlement identity accepted", err)
	}
	f.exec(t, `UPDATE execution_positions SET condition_id='condition-p',settled_at=$1`, f.now.Add(time.Hour))
	got, err = f.reader.CoreMetricFacts(context.Background(), "pm")
	if err != nil || got.Accounts[0].Assets[0].SettlementPrice != nil {
		t.Fatal("future settlement accepted", err)
	}
}
func TestCoreMetricFactsRejectsSourceMismatchAndUnregisteredAccount(t *testing.T) {
	f := prepareCoreFacts(t)
	for _, id := range []string{"not-registered", "unmanaged", "unknown"} {
		if _, err := f.reader.CoreMetricFacts(context.Background(), id); !errors.Is(err, ErrInvalidPerformanceAccount) {
			t.Fatal(id, err)
		}
	}
	f.exec(t, `UPDATE execution_orders SET intent=jsonb_set(intent,'{market_source}','"KALSHI"') WHERE execution_account_id='pm'`)
	if _, err := f.reader.CoreMetricFacts(context.Background(), "pm"); !errors.Is(err, ErrUnavailable) {
		t.Fatal("source conflict", err)
	}
}
func TestCoreMetricFactsBoundedCompleteAssetsAndCancellation(t *testing.T) {
	f := prepareCoreFacts(t)
	f.exec(t, `INSERT INTO position_lots(lot_id,execution_account_id,market_id,condition_id,token_id,model_id,strategy_id,outcome_index,outcome_name,status,remaining_shares,remaining_cost,opened_at) SELECT 'many-'||i,'pm','m-'||i,'c-'||i,'t-'||i,'old','old',0,'Yes','OPEN',1,0.1,$1 FROM generate_series(1,$2) i`, f.now, MaxCoreMetricAssets)
	if _, err := f.reader.CoreMetricFacts(context.Background(), "pm"); !errors.Is(err, ErrUnavailable) {
		t.Fatal("asset cap was truncated instead of rejected", err)
	}
	f.exec(t, `DELETE FROM position_lots WHERE lot_id LIKE 'many-%'`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.reader.CoreMetricFacts(ctx, "pm"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel not propagated", err)
	}
	c, e := f.db.Conn(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var one int
	if e = c.QueryRowContext(context.Background(), `SELECT 1`).Scan(&one); e != nil || one != 1 {
		t.Fatal("connection not reusable", e)
	}
}
func TestCoreMetricFactsLargeHistoricalBudget(t *testing.T) {
	f := prepareCoreFacts(t)
	f.exec(t, `INSERT INTO position_events(position_event_id,execution_account_id,token_id,event_type,shares_delta,cost_basis_delta,realized_pnl_delta,occurred_at) SELECT 'core-history-'||i,'pm','closed','BOUGHT',0,0.001,0,$1 FROM generate_series(1,100000) i`, f.now.Add(-500*24*time.Hour))
	f.exec(t, `INSERT INTO position_lots(lot_id,execution_account_id,market_id,condition_id,token_id,model_id,strategy_id,outcome_index,outcome_name,status,remaining_shares,remaining_cost,opened_at) SELECT 'closed-'||i,'pm','closed','closed','closed','old','old',0,'Yes','CLOSED',0,0,$1 FROM generate_series(1,100000)i`, f.now.Add(-600*24*time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	got, err := f.reader.CoreMetricFacts(ctx, "pm")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Accounts[0].Assets) != 1 {
		t.Fatal("closed history leaked")
	}
	t.Log(fmt.Sprintf("100k events + 100k closed lots: %s", time.Since(started)))
}

// A volatile expression in the connection-local event view witnesses SQL
// execution before cancellation, rather than merely testing a cancelled caller.
func TestCoreMetricFactsRunningSQLCancellationReleasesConnection(t *testing.T) {
	f := prepareCoreFacts(t)
	f.exec(t, `ALTER TABLE position_events RENAME TO core_event_data`)
	f.exec(t, `CREATE FUNCTION pg_temp.core_slow_pnl(v numeric) RETURNS numeric LANGUAGE plpgsql VOLATILE AS $$BEGIN PERFORM pg_sleep(2);RETURN v;END$$`)
	f.exec(t, `CREATE TEMP VIEW position_events AS SELECT execution_account_id,position_event_id,occurred_at,event_type,cost_basis_delta,pg_temp.core_slow_pnl(realized_pnl_delta) AS realized_pnl_delta FROM core_event_data`)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := f.reader.CoreMetricFacts(ctx, "pm")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatal("running SQL cancellation", err, time.Since(started))
	}
	var one int
	if err = f.db.QueryRow(`SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatal("connection not reusable", err)
	}
}
func TestCoreMetricFactsPoolWaitCancellation(t *testing.T) {
	f := prepareCoreFacts(t)
	held, err := f.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = f.reader.CoreMetricFacts(ctx, "pm")
	held.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pool wait deadline", err)
	}
	var one int
	if err = f.db.QueryRow(`SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatal("pool not reusable", err)
	}
}

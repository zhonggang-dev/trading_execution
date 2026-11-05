package consolev2

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func cohortFixture(t *testing.T) performanceFixture {
	f := newPerformanceFixture(t)
	for _, table := range []string{"execution_fills", "position_lot_closures", "position_lot_redemptions", "polymarket_redemptions", "kalshi_settlement_lots", "kalshi_settlements", "managed_external_sells", "managed_external_sell_batches", "managed_external_sell_allocations", "execution_positions"} {
		f.exec(t, `CREATE TEMP TABLE `+table+` AS SELECT * FROM `+table+` WITH NO DATA`)
	}
	f.exec(t, `DELETE FROM position_lots`)
	f.exec(t, `DELETE FROM execution_orders`)
	f.exec(t, `DELETE FROM position_events`)
	return f
}
func (f performanceFixture) cohortLot(t *testing.T, id, shares, remaining, gross, fee string, at time.Time) {
	t.Helper()
	f.exec(t, `INSERT INTO execution_orders(order_id,execution_account_id,venue,market_id,token_id,intent) VALUES($1,'pm','POLYMARKET','m','t','{"condition_id":"c","metadata":{"market_question":"question"}}')`, id)
	f.exec(t, `INSERT INTO execution_fills(fill_key,venue,execution_account_id,order_id,market_id,condition_id,token_id,side,status,shares,gross_notional,total_fee,applied_at) VALUES($1,'POLYMARKET','pm',$1,'m','c','t','BUY','CONFIRMED',$2,$3,$4,$5)`, id, shares, gross, fee, at)
	f.exec(t, `INSERT INTO position_lots(lot_id,execution_account_id,market_id,condition_id,token_id,model_id,strategy_id,opening_order_id,opening_fill_key,outcome_index,outcome_name,status,original_shares,remaining_shares,original_cost,remaining_cost,opened_at) VALUES($1,'pm','m','c','t','old','old',$1,$1,0,'Yes',CASE WHEN $3::numeric=0 THEN 'CLOSED' ELSE 'OPEN' END,$2,$3,$4::numeric+$5::numeric,($4::numeric+$5::numeric)*$3::numeric/$2::numeric,$6)`, id, shares, remaining, gross, fee, at)
}
func (f performanceFixture) cohortSell(t *testing.T, lot, id, qty, gross, fee string, at time.Time) {
	t.Helper()
	f.exec(t, `INSERT INTO execution_fills(fill_key,venue,execution_account_id,order_id,market_id,condition_id,token_id,side,status,shares,gross_notional,total_fee,applied_at) VALUES($1,'POLYMARKET','pm',$2,'m','c','t','SELL','CONFIRMED',$3,$4,$5,$6)`, id, lot, qty, gross, fee, at)
	f.exec(t, `INSERT INTO position_lot_closures(closure_id,closing_fill_key,lot_id,closed_shares,allocated_net_proceeds,closed_at) VALUES($1,$1,$2,$3,$4::numeric-$5::numeric,$6)`, id, lot, qty, gross, fee, at)
}
func TestCohortMetricFactsOpeningBoundsDistinctPartialAndSource(t *testing.T) {
	f := cohortFixture(t)
	from, to := f.now.Add(-24*time.Hour), f.now.Add(-12*time.Hour)
	f.cohortLot(t, "before", "8", "8", "4", "0.08", from.Add(-time.Microsecond))
	f.cohortLot(t, "included", "10", "6", "4", "0.1", from)
	f.cohortLot(t, "fully-closed", "5", "0", "1", "0.05", from.Add(time.Hour))
	f.cohortLot(t, "at-end", "3", "3", "1.5", "0.03", to)
	f.cohortSell(t, "included", "sold", "4", "3", "0.1", f.now.Add(-time.Hour))
	f.cohortSell(t, "fully-closed", "sold-all", "5", "2", "0.05", f.now.Add(-time.Hour))
	f.exec(t, `INSERT INTO execution_positions(execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,lifecycle_status,settlement_price,settlement_source,settled_at) VALUES('pm','m','c','t',0,'Yes','SETTLED_PENDING_REDEEM',1,'official',$1)`, f.now.Add(-time.Hour))
	got, err := f.reader.CohortMetricFacts(context.Background(), CohortFilter{ExecutionAccountID: "pm", OpeningFrom: &from, OpeningTo: &to})
	if err != nil {
		t.Fatal(err)
	}
	c := got.Cohorts["pm"]
	if c.OpenedAssetCount != 1 || c.HoldingAssetCount != 1 || c.ClosedAssetCount != 0 {
		t.Fatal(c)
	}
	m := got.Accounts[0]
	if len(m.Assets) != 1 || m.Assets[0].SettlementPrice == nil || *m.Assets[0].SettlementPrice != "1" {
		t.Fatal(m.Assets)
	}
	performanceAmount(t, m.Assets[0].RemainingShares, "6")
	performanceAmount(t, m.Assets[0].RemainingCost, "2.4")
	performanceAmount(t, m.Metrics.RealizedPnL, "2.4")
	performanceAmount(t, m.Metrics.PeakHoldingCost, "5")
	performanceAmount(t, m.Metrics.CumulativeInvestedCost, "5")
	performanceAmount(t, c.BuyFees, "0.15")
	performanceAmount(t, c.SellFees, "0.15")
	f.exec(t, `UPDATE execution_fills SET token_id='other' WHERE fill_key='included'`)
	if _, err = f.reader.CohortMetricFacts(context.Background(), CohortFilter{ExecutionAccountID: "pm"}); !errors.Is(err, ErrUnavailable) {
		t.Fatal("identity conflict accepted", err)
	}
}
func TestCohortMetricFactsAllClosedZeroPayoutAndExternalSell(t *testing.T) {
	f := cohortFixture(t)
	at := f.now.Add(-time.Hour)
	f.cohortLot(t, "redeemed", "2", "0", "0.8", "0.02", at)
	f.cohortLot(t, "external", "4", "0", "2", "0.04", at)
	f.exec(t, `INSERT INTO polymarket_redemptions(execution_account_id,condition_id,status,confirmed_at,applied_at) VALUES('pm','c','APPLIED',$1,$1)`, f.now)
	f.exec(t, `INSERT INTO position_lot_redemptions(redemption_id,execution_account_id,condition_id,lot_id,redeemed_shares,allocated_payout,redeemed_at) VALUES('r','pm','c','redeemed',2,0,$1)`, f.now)
	f.exec(t, `INSERT INTO managed_external_sell_batches(batch_id,applied_at) VALUES('b',$1)`, f.now)
	f.exec(t, `INSERT INTO managed_external_sells(sell_id,batch_id,execution_account_id,condition_id,token_id,shares,total_fee,occurred_at) VALUES('s','b','pm','c','t',4,0.1,$1)`, f.now)
	f.exec(t, `INSERT INTO managed_external_sell_allocations(sell_id,lot_id,closed_shares,net_proceeds) VALUES('s','external',4,2.9)`)
	got, err := f.reader.CohortMetricFacts(context.Background(), CohortFilter{ExecutionAccountID: "pm"})
	if err != nil {
		t.Fatal(err)
	}
	c := got.Cohorts["pm"]
	if c.OpenedAssetCount != 1 || c.HoldingAssetCount != 0 || c.ClosedAssetCount != 1 {
		t.Fatal(c)
	}
	performanceAmount(t, got.Accounts[0].Metrics.RealizedPnL, "0.2")
	performanceAmount(t, c.BuyFees, "0.06")
	performanceAmount(t, c.SellFees, "0.1")
	if len(got.Accounts[0].Assets) != 0 {
		t.Fatal("closed priced as holding")
	}
	// Missing a zero-value settlement is still incomplete accounting.
	f.exec(t, `DELETE FROM position_lot_redemptions`)
	if _, err = f.reader.CohortMetricFacts(context.Background(), CohortFilter{ExecutionAccountID: "pm"}); !errors.Is(err, ErrUnavailable) {
		t.Fatal("missing zero flow accepted", err)
	}
}
func TestCohortMetricFactsEmptyScopeCancellationAndLargeHistory(t *testing.T) {
	f := cohortFixture(t)
	from, to := f.now.Add(-time.Hour), f.now
	got, err := f.reader.CohortMetricFacts(context.Background(), CohortFilter{ExecutionAccountID: "pm", OpeningFrom: &from, OpeningTo: &to})
	if err != nil {
		t.Fatal(err)
	}
	if got.Cohorts["pm"].MaxDrawdown == nil || *got.Cohorts["pm"].MaxDrawdown != "0" {
		t.Fatal(got.Cohorts)
	}
	f.cohortLot(t, "history", "1", "0", "0.5", "0.01", f.now.Add(-400*24*time.Hour))
	f.cohortSell(t, "history", "closed", "1", "0.6", "0.01", f.now.Add(-399*24*time.Hour))
	// 100k unrelated closed lots do not leak into a recent opening cohort.
	f.exec(t, `INSERT INTO position_lots(lot_id,execution_account_id,market_id,condition_id,token_id,outcome_index,outcome_name,status,original_shares,remaining_shares,original_cost,remaining_cost,opened_at) SELECT 'old-'||i,'unmanaged','m','c','t',0,'Yes','CLOSED',1,0,0.1,0,$1 FROM generate_series(1,100000)i`, f.now.Add(-500*24*time.Hour))
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = f.reader.CohortMetricFacts(ctx, CohortFilter{ExecutionAccountID: "pm", OpeningFrom: &from, OpeningTo: &to}); err != nil {
		t.Fatal(err)
	}
	t.Log(fmt.Sprintf("100k unrelated history: %s", time.Since(started)))
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err = f.reader.CohortMetricFacts(canceled, CohortFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var one int
	if err = f.db.QueryRow(`SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatal("connection not reusable", err)
	}
}

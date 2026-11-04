package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

// Real FillLedger and SettlementStore writes prove that the read model uses
// both netting legs and the one non-zero redemption event, without adding the
// account payout or the already-capitalized fees a second time.
func TestPostgresIntegrationConsoleV2AccountPerformanceKalshi(t *testing.T) {
	f := newKalshiNettingFixture(t, "kalshi:performance", "1000")
	f.buy("NET", "YES", "10", "0.3", "0.1")
	f.buy("NET", "YES", "8", "0.4", "0.1")
	f.buy("NET", "NO", "14", "0.5", "0.2")
	f.buy("PLAIN", "YES", "2", "0.6", "0.1")
	read := func() consolev2.AccountPerformance {
		t.Helper()
		r := consolev2.New(f.database, consolev2.RuntimeConfig{Now: func() time.Time { return f.clock }, ManagedAccounts: []consolev2.ManagedAccount{{ExecutionAccountID: f.account, MarketSource: "KALSHI"}}})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := r.AccountPerformance(ctx, f.account)
		if err != nil || len(result.Items) != 1 {
			t.Fatalf("performance=%#v err=%v", result, err)
		}
		return result.Items[0]
	}
	item := read()
	assertConsoleAmount(t, item.PeakHoldingCost, "13.6")
	assertConsoleAmount(t, item.CumulativeInvestedCost, "14.9")
	assertConsoleAmount(t, item.RealizedPnL, "2.05")
	assertConsoleAmount(t, item.CurrentCost, "2.95")
	assertConsoleAmount(t, item.RealizedDrawdown14D, "0")
	if item.AssetCount != 2 || item.PricedAssetCount != 0 || item.MarketValue != nil || item.UnrealizedPnL != nil || item.TotalPnL != nil || item.ReturnRate != nil {
		t.Fatalf("missing quotes invented an account value: %#v", item)
	}
	f.buy("ZERO", "YES", "2", "0.4", "0.1")
	f.buy("WIN", "NO", "3", "0.5", "0.1")
	f.buy("REVIEW", "YES", "1", "0.5", "0")
	store, err := NewKalshiSettlementStore(f.database, []string{f.account})
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []domain.KalshiSettlement{
		kalshiRecord("ZERO", "no", "2", "0", "0", "0"),
		kalshiRecord("WIN", "no", "0", "3", "3", "0"),
		kalshiRecord("REVIEW", "yes", "5", "0", "5", "1"),
	} {
		f.clock = f.clock.Add(24 * time.Hour)
		if _, err = store.ApplyKalshiSettlement(context.Background(), f.account, record, f.clock); err != nil {
			t.Fatal(err)
		}
		if _, err = store.ApplyKalshiSettlement(context.Background(), f.account, record, f.clock); err != nil {
			t.Fatal(err)
		}
	}
	item = read()
	assertConsoleAmount(t, item.PeakHoldingCost, "13.6")
	assertConsoleAmount(t, item.CumulativeInvestedCost, "17.9")
	assertConsoleAmount(t, item.RealizedPnL, "2.55")
	assertConsoleAmount(t, item.CurrentCost, "3.45")
	assertConsoleAmount(t, item.RealizedDrawdown14D, "0.9")
	if item.AssetCount != 3 {
		t.Fatalf("review or remaining netted asset disappeared: %#v", item)
	}
	// Seal a real immutable batch and real member rows under migration
	// constraints; the earlier adversarial quote tests use local shadow tables.
	tx, err := f.database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	decision := f.clock.Add(-time.Second)
	if _, err = tx.Exec(`INSERT INTO strategy_orderbook_snapshot_batches(decision_at,prediction_snapshot_id,snapshot_set_id,target_count,snapshot_count,ok_count,empty_count,missing_count,error_count,created_at) VALUES($1,'performance-prediction','performance-set',3,3,3,0,0,0,$1)`, decision); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO strategy_orderbook_snapshots(decision_at,market_source,market_id,condition_id,token_id,outcome_index,status,source_at,observed_at,best_bid,best_ask,depth_limit,bids,asks)
	 SELECT DISTINCT $1::timestamptz,'KALSHI',market_id,condition_id,token_id,outcome_index,'OK',$2::timestamptz,$3::timestamptz,0.4,0.6,1,'[{"price":"0.4","size":"1"}]'::jsonb,'[{"price":"0.6","size":"1"}]'::jsonb
	 FROM position_lots WHERE execution_account_id=$4 AND remaining_shares>0 AND status='OPEN'`, decision, f.clock.Add(-24*time.Hour), f.clock, f.account); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	item = read()
	if item.ValuationState != "complete" || item.PricedAssetCount != 3 || item.MarketValue == nil || item.TotalPnL == nil {
		t.Fatalf("real sealed quote batch %#v", item)
	}
	assertConsoleAmount(t, *item.MarketValue, "3.5")
	assertConsoleAmount(t, *item.UnrealizedPnL, "0.05")
	assertConsoleAmount(t, *item.TotalPnL, "2.6")
	raw, _ := json.Marshal(item)
	for _, secret := range []string{"wallet_address", "key-", "fill_key", "metadata"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("unsafe projection: %s", raw)
		}
	}
	// Complete pair netting leaves no assets. Zero current value is a real fact,
	// unlike the unavailable valuation on the earlier non-empty account.
	z := newKalshiNettingFixture(t, "kalshi:performance-flat", "100")
	z.buy("FLAT", "YES", "4", "0.3", "0.1")
	z.buy("FLAT", "NO", "4", "0.4", "0.1")
	rr := consolev2.New(z.database, consolev2.RuntimeConfig{Now: func() time.Time { return z.clock }, ManagedAccounts: []consolev2.ManagedAccount{{ExecutionAccountID: z.account, MarketSource: "KALSHI"}}})
	flat, err := rr.AccountPerformance(context.Background(), z.account)
	if err != nil || len(flat.Items) != 1 {
		t.Fatalf("flat=%#v %v", flat, err)
	}
	if v := flat.Items[0]; v.AssetCount != 0 || v.MarketValue == nil || v.UnrealizedPnL == nil || v.TotalPnL == nil || v.ReturnRate == nil || v.ValuationState != "complete" {
		t.Fatalf("flat value %#v", v)
	}
	assertConsoleAmount(t, *flat.Items[0].MarketValue, "0")
	assertConsoleAmount(t, *flat.Items[0].UnrealizedPnL, "0")
	assertConsoleAmount(t, *flat.Items[0].TotalPnL, "1")
}

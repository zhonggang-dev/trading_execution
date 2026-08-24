package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

func TestLiveWalletAccountingPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	insertAccount(t, db, "wallet-performance-a", "0xperformancea", "100", "100", "0")
	insertAccount(t, db, "wallet-performance-b", "0xperformanceb", "100", "100", "0")

	observedAt := time.Date(2026, 8, 23, 8, 0, 0, 0, time.UTC)
	insertLiveAccountingEvent(t, db, liveAccountingEvent{
		id: "event-a-1", accountID: "wallet-performance-a", eventType: "BOUGHT",
		costDelta: "10", realizedDelta: "0", sharesAfter: "10", costAfter: "10", occurredAt: observedAt.Add(-3 * time.Hour),
	})
	insertLiveAccountingEvent(t, db, liveAccountingEvent{
		id: "event-a-2", accountID: "wallet-performance-a", eventType: "SOLD",
		costDelta: "-4", realizedDelta: "2", sharesAfter: "6", costAfter: "6", occurredAt: observedAt.Add(-2 * time.Hour),
	})
	insertLiveAccountingEvent(t, db, liveAccountingEvent{
		id: "event-a-3", accountID: "wallet-performance-a", eventType: "BOUGHT",
		costDelta: "7", realizedDelta: "0", sharesAfter: "13", costAfter: "13", occurredAt: observedAt.Add(-time.Hour),
	})
	insertLiveAccountingEvent(t, db, liveAccountingEvent{
		id: "event-a-future", accountID: "wallet-performance-a", eventType: "BOUGHT",
		costDelta: "100", realizedDelta: "0", sharesAfter: "113", costAfter: "113", occurredAt: observedAt.Add(time.Hour),
	})

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	clause, args := liveOperationsAccountClause([]string{"wallet-performance-a", "wallet-performance-b"}, 1)
	items, err := loadLiveWalletAccounting(context.Background(), tx, clause, args, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("wallet accounting rows = %#v", items)
	}
	if items[0].ExecutionAccountID != "wallet-performance-a" || items[0].PeakCashUsed != "13" ||
		items[0].CumulativeInvestedCost != "17" || items[0].RealizedPnL != "2" {
		t.Fatalf("wallet-performance-a = %#v", items[0])
	}
	if items[1].ExecutionAccountID != "wallet-performance-b" || items[1].PeakCashUsed != "0" ||
		items[1].CumulativeInvestedCost != "0" || items[1].RealizedPnL != "0" {
		t.Fatalf("wallet-performance-b = %#v", items[1])
	}
}

type liveAccountingEvent struct {
	id            string
	accountID     string
	eventType     string
	costDelta     string
	realizedDelta string
	sharesAfter   string
	costAfter     string
	occurredAt    time.Time
}

func insertLiveAccountingEvent(t *testing.T, db *sql.DB, event liveAccountingEvent) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO position_events (
			position_event_id,event_type,execution_account_id,market_id,token_id,
			order_id,fill_key,model_id,strategy_id,shares_delta,cash_delta,
			cost_basis_delta,realized_pnl_delta,shares_after,cost_basis_after,
			average_cost_after,realized_pnl_after,unrealized_pnl_after,occurred_at
		) VALUES (
			$1,$2,$3,'market-performance','token-performance','','','model-performance','strategy-performance',
			0,0,$4::numeric,$5::numeric,$6::numeric,$7::numeric,
			CASE WHEN $6::numeric=0 THEN 0 ELSE $7::numeric/$6::numeric END,
			$5::numeric,0,$8
		)`, event.id, event.eventType, event.accountID, event.costDelta, event.realizedDelta,
		event.sharesAfter, event.costAfter, event.occurredAt.UTC())
	if err != nil {
		t.Fatalf("insert live accounting event %s: %v", event.id, err)
	}
}

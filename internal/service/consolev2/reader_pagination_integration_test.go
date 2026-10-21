package consolev2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// This integration regression is opt-in because it needs a migrated isolated
// PostgreSQL database. It deliberately uses the same test database gate as the
// existing console-v2 schema test and never creates a product-only switch.
func TestReaderPaginationSnapshotAndCancellation(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_CONSOLE_V2_TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	matrixAccount := "console-v2-page-matrix-" + suffix
	concurrentAccount := "console-v2-page-concurrent-" + suffix
	cleanup := func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_orders WHERE execution_account_id IN ($1,$2)`, matrixAccount, concurrentAccount)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_accounts WHERE execution_account_id IN ($1,$2)`, matrixAccount, concurrentAccount)
	}
	cleanup()
	defer cleanup()

	seedAccount := func(accountID string, walletSuffix int64) {
		t.Helper()
		wallet := fmt.Sprintf("0x%040x", walletSuffix)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO execution_accounts (execution_account_id, wallet_address, collateral_asset)
			VALUES ($1, $2, 'pUSD')`, accountID, wallet); err != nil {
			t.Fatal(err)
		}
	}
	seedAccount(matrixAccount, time.Now().UnixNano())
	seedAccount(concurrentAccount, time.Now().UnixNano()+1)

	seedOrder := func(accountID, orderID, status string) {
		t.Helper()
		intent := `{"model_id":"matrix","strategy_id":"matrix","market_source":"POLYMARKET","side":"BUY","price":"0.4","size":"1"}`
		if _, err := db.ExecContext(ctx, `
			INSERT INTO execution_orders (
				order_id, client_order_id, execution_account_id, venue, market_id, token_id,
				intent, status, revision, created_at, updated_at
			) VALUES ($1,$2,$3,'polymarket',$4,$5,$6::jsonb,$7,1,clock_timestamp(),clock_timestamp())`,
			orderID, orderID+"-client", accountID, orderID+"-market", orderID+"-token", intent, status); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 20; i++ {
		seedOrder(matrixAccount, fmt.Sprintf("page-live-%03d-%s", i, suffix), "LIVE")
	}
	for i := 0; i < 21; i++ {
		seedOrder(matrixAccount, fmt.Sprintf("page-partial-%03d-%s", i, suffix), "PARTIALLY_FILLED")
	}
	for i := 0; i < 101; i++ {
		seedOrder(matrixAccount, fmt.Sprintf("page-unknown-%03d-%s", i, suffix), "UNKNOWN")
	}
	seedOrder(matrixAccount, "page-one-"+suffix, "RECEIVED")

	reader := New(db, RuntimeConfig{Now: func() time.Time { return time.Now().UTC() }})
	checks := []struct {
		name   string
		status string
		want   int
	}{
		{name: "zero", status: "MANUAL_REVIEW", want: 0},
		{name: "one", status: "RECEIVED", want: 1},
		{name: "twenty", status: "LIVE", want: 20},
		{name: "twenty-one", status: "PARTIALLY_FILLED", want: 21},
		{name: "one-hundred-one", status: "UNKNOWN", want: 101},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			page, err := reader.Orders(ctx, ListFilter{Limit: 100, Status: check.status, ExecutionAccountID: matrixAccount})
			if err != nil {
				t.Fatal(err)
			}
			wantItems := check.want
			if wantItems > 100 {
				wantItems = 100
			}
			if page.Total != check.want || len(page.Items) != wantItems || page.HasMore != (check.want > 100) {
				t.Fatalf("page total=%d items=%d has_more=%v, want total=%d items=%d has_more=%v", page.Total, len(page.Items), page.HasMore, check.want, wantItems, check.want > 100)
			}
			if check.want == 101 {
				last, err := reader.Orders(ctx, ListFilter{Limit: 100, Offset: 100, Status: check.status, ExecutionAccountID: matrixAccount})
				if err != nil {
					t.Fatal(err)
				}
				if last.Total != 101 || len(last.Items) != 1 || last.HasMore {
					t.Fatalf("last page total=%d items=%d has_more=%v", last.Total, len(last.Items), last.HasMore)
				}
			}
		})
	}

	stopWriter := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; i < 80; i++ {
			orderID := fmt.Sprintf("page-flap-%03d-%s", i, suffix)
			_, _ = db.ExecContext(context.Background(), `
				INSERT INTO execution_orders (
					order_id, client_order_id, execution_account_id, venue, market_id, token_id,
					intent, status, revision, created_at, updated_at
				) VALUES ($1,$2,$3,'polymarket',$4,$5,'{"model_id":"matrix"}'::jsonb,'LIVE',1,clock_timestamp(),clock_timestamp())
				ON CONFLICT (order_id) DO NOTHING`, orderID, orderID+"-client", concurrentAccount, orderID+"-market", orderID+"-token")
			_, _ = db.ExecContext(context.Background(), `DELETE FROM execution_orders WHERE order_id=$1`, orderID)
			select {
			case <-stopWriter:
				return
			default:
			}
		}
	}()
	for i := 0; i < 30; i++ {
		page, err := reader.Orders(ctx, ListFilter{Limit: 100, ExecutionAccountID: concurrentAccount})
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != len(page.Items) || page.Total > 100 {
			t.Fatalf("concurrent snapshot total=%d items=%d", page.Total, len(page.Items))
		}
	}
	close(stopWriter)
	writer.Wait()

	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.ExecContext(ctx, `LOCK TABLE execution_orders IN ACCESS EXCLUSIVE MODE`); err != nil {
		_ = lockTx.Rollback()
		t.Fatal(err)
	}
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = reader.Orders(shortCtx, ListFilter{Limit: 100, ExecutionAccountID: matrixAccount})
	shortCancel()
	_ = lockTx.Rollback()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("locked count error=%v, want context deadline exceeded", err)
	}
	if inUse := db.Stats().InUse; inUse != 0 {
		t.Fatalf("connection leak after cancelled count: in_use=%d", inUse)
	}
}

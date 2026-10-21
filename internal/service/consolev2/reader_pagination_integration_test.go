package consolev2

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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
	cyclePrefix := "console-v2-page-cycle-" + suffix + "-"
	cleanup := func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM strategy_order_intent_deliveries WHERE cycle_id LIKE $1`, cyclePrefix+"%")
		_, _ = db.ExecContext(context.Background(), `DELETE FROM strategy_decision_runs WHERE cycle_id LIKE $1`, cyclePrefix+"%")
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
	largeSeed := make([]byte, 128*1024)
	if _, err := rand.Read(largeSeed); err != nil {
		t.Fatal(err)
	}
	largeBlob := hex.EncodeToString(largeSeed)
	largePayload := `{"blob":"` + largeBlob + `"}`

	seedCycle := func(index int, status string) string {
		t.Helper()
		cycleID := fmt.Sprintf("%s%02d", cyclePrefix, index)
		inputID := cycleID + "-input"
		decisionAt := time.Now().UTC().Add(-time.Duration(index) * time.Second)
		var outputPayload any
		var submissionEnabled any
		if status != "INPUT_RECORDED" {
			outputPayload = largePayload
			submissionEnabled = false
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO strategy_decision_runs (
				cycle_id, input_id, decision_at, model_id, strategy_id, execution_account_id,
				input_payload, output_payload, order_submission_enabled, created_at, updated_at, decided_at
			) VALUES ($1,$2,$3,'cycle-model','cycle-strategy',$4,$5::jsonb,$6::jsonb,$7,$3,$3,$8)`,
			cycleID, inputID, decisionAt, matrixAccount, largePayload, outputPayload, submissionEnabled,
			func() any {
				if status == "INPUT_RECORDED" {
					return nil
				}
				return decisionAt
			}()); err != nil {
			t.Fatal(err)
		}
		if status == "INPUT_RECORDED" {
			return cycleID
		}
		deliveryStatus := "SUBMITTED"
		if status == "DELIVERY_PENDING" {
			deliveryStatus = "PENDING"
		} else if status == "DELIVERY_ATTENTION" {
			deliveryStatus = "FAILED"
		}
		claimedAt, completedAt, orderID, orderStatus := any(decisionAt), any(decisionAt), any(cycleID+"-order"), any("UNKNOWN")
		if deliveryStatus == "PENDING" {
			claimedAt, completedAt, orderID, orderStatus = nil, nil, nil, nil
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO strategy_order_intent_deliveries (
				client_order_id, cycle_id, sequence_no, intent_payload, status,
				attempt_count, claimed_at, completed_at, order_id, order_status, created_at, updated_at
			) VALUES ($1,$2,0,'{}'::jsonb,$3,1,$4,$5,$6,$7,$8,$8)`,
			cycleID+"-delivery", cycleID, deliveryStatus, claimedAt, completedAt, orderID, orderStatus, decisionAt); err != nil {
			t.Fatal(err)
		}
		return cycleID
	}
	cycleIDs := map[string]string{
		"INPUT_RECORDED":     seedCycle(0, "INPUT_RECORDED"),
		"DELIVERY_PENDING":   seedCycle(1, "DELIVERY_PENDING"),
		"DELIVERY_ATTENTION": seedCycle(2, "DELIVERY_ATTENTION"),
		"OUTPUT_RECORDED":    seedCycle(3, "OUTPUT_RECORDED"),
	}
	_ = seedCycle(4, "INPUT_RECORDED")
	_ = seedCycle(5, "DELIVERY_PENDING")
	_ = seedCycle(6, "DELIVERY_ATTENTION")
	_ = seedCycle(7, "OUTPUT_RECORDED")
	var largePayloadRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM strategy_decision_runs
		WHERE cycle_id LIKE $1
		  AND pg_column_size(input_payload) > 100000
		  AND (output_payload IS NULL OR pg_column_size(output_payload) > 100000)`, cyclePrefix+"%").Scan(&largePayloadRows); err != nil {
		t.Fatal(err)
	}
	if largePayloadRows != 8 {
		t.Fatalf("large cycle payload rows=%d, want 8", largePayloadRows)
	}

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
	for _, status := range []string{"INPUT_RECORDED", "DELIVERY_PENDING", "DELIVERY_ATTENTION", "OUTPUT_RECORDED"} {
		page, err := reader.DecisionCycles(ctx, ListFilter{Limit: 1, Status: status, ExecutionAccountID: matrixAccount})
		if err != nil {
			t.Fatalf("cycle status %s: %v", status, err)
		}
		if page.Total != 2 || len(page.Items) != 1 || !page.HasMore {
			t.Fatalf("cycle status %s total=%d items=%d has_more=%v", status, page.Total, len(page.Items), page.HasMore)
		}
	}
	filteredCycles, err := reader.DecisionCycles(ctx, ListFilter{
		Limit: 1, Query: cycleIDs["DELIVERY_PENDING"], ModelID: "cycle-model",
		ExecutionAccountID: matrixAccount,
		From:               func() *time.Time { value := time.Now().UTC().Add(-2 * time.Minute); return &value }(),
		To:                 func() *time.Time { value := time.Now().UTC().Add(2 * time.Minute); return &value }(),
	})
	if err != nil || filteredCycles.Total != 1 || len(filteredCycles.Items) != 1 || filteredCycles.Items[0].Status != "DELIVERY_PENDING" {
		t.Fatalf("filtered cycles=%#v err=%v", filteredCycles, err)
	}
	emptyCycles, err := reader.DecisionCycles(ctx, ListFilter{Limit: 1, Query: "cycle-does-not-exist", ExecutionAccountID: matrixAccount})
	if err != nil || emptyCycles.Total != 0 || len(emptyCycles.Items) != 0 || emptyCycles.HasMore {
		t.Fatalf("empty cycles=%#v err=%v", emptyCycles, err)
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

	runtimeReader := New(db, RuntimeConfig{
		RunnerEnabled: true,
		Bindings:      []Binding{{ModelID: "matrix", StrategyID: "matrix", ExecutionAccountID: matrixAccount}},
		RunnerSnapshot: func() RunnerSnapshot {
			return RunnerSnapshot{LoopStarted: true, LoopRunning: true}
		},
		Now: func() time.Time { return time.Now().UTC() },
	})
	runtimeLockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeLockTx.ExecContext(ctx, `LOCK TABLE execution_orders IN ACCESS EXCLUSIVE MODE`); err != nil {
		_ = runtimeLockTx.Rollback()
		t.Fatal(err)
	}
	runtimeCtx, runtimeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	runtime, runtimeErr := runtimeReader.Runtime(runtimeCtx)
	runtimeCancel()
	_ = runtimeLockTx.Rollback()
	if runtimeErr != nil || runtime.AccountDataState != "unavailable" || len(runtime.Bindings) != 1 || !runtime.Runner.Running {
		t.Fatalf("runtime timeout err=%v state=%#v", runtimeErr, runtime)
	}

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

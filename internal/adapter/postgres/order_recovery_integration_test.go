package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// TestOrderRecoveryLeaseStorePostgresIntegration 验证订单级恢复租约在 PostgreSQL
// 中的互斥、版本校验、退避与升级持久化。
func TestOrderRecoveryLeaseStorePostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	insertAccount(t, db, "account-lease", "0xlease", "100", "100", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	order := integrationOrder("lease", "account-lease", "token-lease", domain.SideBuy, "10", "0.8")
	order.Status, order.CreatedAt, order.UpdatedAt, order.Revision = domain.OrderStatusReceived, now, now, 1
	if _, created, err := repository.Create(ctx, order); err != nil || !created {
		t.Fatalf("create order: created=%v err=%v", created, err)
	}
	store, err := NewOrderRecoveryLeaseStore(db)
	if err != nil {
		t.Fatal(err)
	}
	request := domain.OrderRecoveryLeaseRequest{OrderID: order.ID, ExecutionAccountID: "account-lease", Holder: "coordinator", OrderRevision: 1, Now: now, TTL: time.Minute}
	lease, err := store.AcquireOrderRecoveryLease(ctx, request)
	if err != nil || lease.Holder != "coordinator" || lease.ExpiresAt == nil {
		t.Fatalf("acquire: lease=%#v err=%v", lease, err)
	}
	other := request
	other.Holder = "reconciliation"
	if _, err := store.AcquireOrderRecoveryLease(ctx, other); !errors.Is(err, port.ErrOrderRecoveryLeaseHeld) {
		t.Fatalf("second holder error = %v, want held", err)
	}
	if _, err := store.ReleaseOrderRecoveryLease(ctx, domain.OrderRecoveryRelease{OrderID: order.ID, Holder: "reconciliation", Now: now, Kind: domain.OrderRecoveryOutcomeResolved}); !errors.Is(err, port.ErrOrderRecoveryLeaseHeld) {
		t.Fatalf("foreign release error = %v, want held", err)
	}
	released, err := store.ReleaseOrderRecoveryLease(ctx, domain.OrderRecoveryRelease{
		OrderID: order.ID, Holder: "coordinator", Now: now.Add(time.Second), Kind: domain.OrderRecoveryOutcomeFailed,
		Error: "clob 503", NextRetryAt: now.Add(31 * time.Second),
	})
	if err != nil || released.Holder != "" || released.Attempts != 1 || released.FirstPendingAt == nil || released.NextRetryAt == nil || released.LastError != "clob 503" {
		t.Fatalf("failed release: lease=%#v err=%v", released, err)
	}
	early := other
	early.Now = now.Add(10 * time.Second)
	if _, err := store.AcquireOrderRecoveryLease(ctx, early); !errors.Is(err, port.ErrOrderRecoveryBackoff) {
		t.Fatalf("early retry error = %v, want backoff", err)
	}
	early.BypassBackoff = true
	if _, err := store.AcquireOrderRecoveryLease(ctx, early); err != nil {
		t.Fatalf("focused bypass error = %v", err)
	}
	escalated, err := store.ReleaseOrderRecoveryLease(ctx, domain.OrderRecoveryRelease{
		OrderID: order.ID, Holder: "reconciliation", Now: now.Add(11 * time.Second), Kind: domain.OrderRecoveryOutcomeWaiting,
		NextRetryAt: now.Add(11 * time.Second), Escalate: true,
	})
	if err != nil || escalated.Attempts != 1 || !escalated.Escalated() || !escalated.FirstPendingAt.Equal(released.FirstPendingAt.UTC()) {
		t.Fatalf("waiting+escalate release: lease=%#v err=%v", escalated, err)
	}
	stale := request
	stale.Now = now.Add(time.Minute)
	stale.OrderRevision = 0
	if _, err := store.AcquireOrderRecoveryLease(ctx, stale); !errors.Is(err, port.ErrOrderRecoveryStaleView) {
		t.Fatalf("stale snapshot error = %v, want stale view", err)
	}
	fresh := request
	fresh.Now = now.Add(time.Minute)
	fresh.OrderRevision = 2
	if _, err := store.AcquireOrderRecoveryLease(ctx, fresh); err != nil {
		t.Fatalf("fresh acquire error = %v", err)
	}
	if _, err := store.ReleaseOrderRecoveryLease(ctx, domain.OrderRecoveryRelease{OrderID: order.ID, Holder: "coordinator", Now: now.Add(2 * time.Minute), Kind: domain.OrderRecoveryOutcomeResolved}); err != nil {
		t.Fatalf("resolved release error = %v", err)
	}
	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM order_recovery_leases WHERE order_id=$1`, order.ID).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("lease rows after resolve = %d err=%v, want 0", remaining, err)
	}
}

// TestScopedReconciliationIssuesGateOnlyTheAffectedMarketPostgresIntegration 验证
// Go 侧 live risk 授权与提交触发器都只按 impact_scope 拦截：账户级 issue 拒绝一切，
// 订单/token 级 issue 只拒绝同 token/condition/market，ATTENTION_REQUIRED 的完成扫描仍算新鲜。
func TestScopedReconciliationIssuesGateOnlyTheAffectedMarketPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	ctx := context.Background()
	for _, test := range []struct {
		name        string
		scope       string
		issueToken  string
		runStatus   string
		reserveCode string
	}{
		{name: "scoped-other-token-allowed", scope: "ORDER", issueToken: "token-other", runStatus: "ATTENTION_REQUIRED"},
		{name: "scoped-same-token-blocked", scope: "ORDER", issueToken: "token-scoped-same-token-blocked", runStatus: "ATTENTION_REQUIRED", reserveCode: "RISK_STATE_HAS_OPEN_ISSUES"},
		{name: "account-wide-blocked", scope: "ACCOUNT", issueToken: "token-other", runStatus: "ATTENTION_REQUIRED", reserveCode: "RISK_STATE_HAS_OPEN_ISSUES"},
		{name: "observation-only-allowed", scope: "NONE", issueToken: "token-observation-only-allowed", runStatus: "ATTENTION_REQUIRED"},
		{name: "failed-run-is-not-fresh", scope: "NONE", issueToken: "token-other", runStatus: "FAILED", reserveCode: "RISK_STATE_STALE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			accountID, tokenID := "account-"+test.name, "token-"+test.name
			insertAccount(t, db, accountID, "0x"+test.name, "100", "100", "0")
			provisionLiveRisk(t, db, liveRiskFixture{accountID: accountID, now: now, binding: true,
				maxOrder: "100", maxMarket: "100", maxStrategy: "100", maxWallet: "100", maxDaily: "100"})
			if _, err := db.Exec(`UPDATE reconciliation_runs SET status=$2 WHERE execution_account_id=$1`, accountID, test.runStatus); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`
				INSERT INTO reconciliation_issues (
					issue_id, run_id, fingerprint, execution_account_id, issue_type, resolution, status,
					order_id, market_id, condition_id, token_id, source, details, observed_at, impact_scope
				) VALUES ($1,$2,$3,$4,'SOURCE_UNAVAILABLE','RETRY_LATER','OPEN','order-x','market-x','condition-x',$5,'CLOB_ORDER','test',$6,$7)`,
				"issue-"+test.name, "risk-run-"+accountID, "fp-"+test.name, accountID, test.issueToken, now, test.scope); err != nil {
				t.Fatal(err)
			}
			order := liveIntegrationOrder(test.name, accountID, tokenID, "10", "0.5", now)
			repository, err := NewOrderRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			order.Status, order.CreatedAt, order.UpdatedAt, order.Revision = domain.OrderStatusReceived, now, now, 1
			stored, created, err := repository.Create(ctx, order)
			if err != nil || !created {
				t.Fatalf("create order: created=%v err=%v", created, err)
			}
			stored, event := applyIntegrationTransition(t, stored, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now, "")
			if err := repository.Transition(ctx, stored, event); err != nil {
				t.Fatal(err)
			}
			manager, err := NewReservationManager(ReservationManagerParams{DB: db, Now: func() time.Time { return now }, MaxBuyFeeRateBPS: "0"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = manager.Reserve(ctx, stored)
			if test.reserveCode != "" {
				assertRejectionCode(t, err, test.reserveCode)
				assertAccount(t, db, accountID, "100", "100", "0")
				return
			}
			if err != nil {
				t.Fatalf("reserve: %v", err)
			}
			stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusReserved, domain.TransitionTriggerReservation, now, "")
			if err := repository.Transition(ctx, stored, event); err != nil {
				t.Fatal(err)
			}
			attempt := domain.OrderAttempt{ID: "attempt-" + test.name, OrderID: stored.ID, Sequence: 1,
				Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeStarted, StartedAt: now}
			stored, event = applyIntegrationTransition(t, stored, domain.OrderStatusSubmitting, domain.TransitionTriggerSubmit, now, attempt.ID)
			if err := repository.StartAttempt(ctx, stored, event, attempt); err != nil {
				t.Fatalf("submit trigger rejected an intent outside the issue scope: %v", err)
			}
			// The trigger must still reject the same account once a same-market
			// scoped issue appears after the reservation.
			if _, err := db.Exec(`UPDATE reconciliation_issues SET token_id=$2 WHERE issue_id=$1`, "issue-"+test.name, tokenID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE reconciliation_issues SET impact_scope='TOKEN' WHERE issue_id=$1`, "issue-"+test.name); err != nil {
				t.Fatal(err)
			}
			second := liveIntegrationOrder(test.name+"-2", accountID, tokenID, "10", "0.5", now)
			second.Status, second.CreatedAt, second.UpdatedAt, second.Revision = domain.OrderStatusReceived, now, now, 1
			storedSecond, created, err := repository.Create(ctx, second)
			if err != nil || !created {
				t.Fatalf("create second order: created=%v err=%v", created, err)
			}
			storedSecond, event = applyIntegrationTransition(t, storedSecond, domain.OrderStatusValidating, domain.TransitionTriggerValidation, now, "")
			if err := repository.Transition(ctx, storedSecond, event); err != nil {
				t.Fatal(err)
			}
			_, err = manager.Reserve(ctx, storedSecond)
			assertRejectionCode(t, err, "RISK_STATE_HAS_OPEN_ISSUES")
		})
	}
}

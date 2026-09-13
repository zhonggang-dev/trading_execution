package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestPartialReconciliationClosesOnlyVerifiedScopesPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, dsn)
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	for index, tc := range []struct {
		name                              string
		verify, reproduce, manual, failed bool
		wantAccount                       bool
	}{
		{name: "recovered-source-and-unrelated-cancel", verify: true},
		{name: "unverified-source", wantAccount: true},
		{name: "reproduced-source", verify: true, reproduce: true, wantAccount: true},
		{name: "real-unattributed-balance-drift", verify: true, manual: true, wantAccount: true},
		{name: "failed-scan-cannot-resolve", verify: true, failed: true, wantAccount: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := fmt.Sprintf("resilience-%d", index)
			insertAccount(t, db, account, "0x"+account, "100", "90", "10")
			prior := startReconciliationFixtureRun(t, recorder, account, "prior-"+account, now.Add(-time.Hour))
			sourceIssue := domain.ReconciliationIssue{IssueID: "source-" + account, RunID: prior.RunID, Fingerprint: "source", ExecutionAccountID: account,
				Type: domain.ReconciliationIssueSourceUnavailable, Resolution: domain.ReconciliationResolutionRetry, Status: domain.ReconciliationIssueOpen,
				Source: "EXTERNAL_POSITIONS", Details: "position source timed out", ObservedAt: prior.StartedAt}
			if tc.manual {
				sourceIssue.Type = domain.ReconciliationIssueBalanceDrift
				sourceIssue.Resolution = domain.ReconciliationResolutionManual
				sourceIssue.Source = "EVM_ERC20_ETH_CALL"
				sourceIssue.LocalValue = "110"
				sourceIssue.RemoteValue = "100"
			}
			if err := recorder.RecordIssue(ctx, sourceIssue); err != nil {
				t.Fatal(err)
			}
			completeReconciliationFixtureRun(t, recorder, prior, domain.ReconciliationRunAttentionRequired, prior.StartedAt.Add(time.Second))
			current := startReconciliationFixtureRun(t, recorder, account, "current-"+account, now)
			pending := domain.ReconciliationIssue{IssueID: "cancel-" + account, RunID: current.RunID, Fingerprint: "cancel", ExecutionAccountID: account,
				Type: domain.ReconciliationIssueOrderRecoveryPending, Resolution: domain.ReconciliationResolutionRetry, Status: domain.ReconciliationIssueOpen,
				OrderID: "cancelled", MarketID: "stuck-market", ConditionID: "stuck-condition", TokenID: "stuck-token", Source: "ORDER_RECOVERY", Details: "cancellation finality unavailable", ObservedAt: now}
			if err := recorder.RecordIssue(ctx, pending); err != nil {
				t.Fatal(err)
			}
			if tc.reproduce {
				sourceIssue.IssueID = "repeat-" + account
				sourceIssue.RunID = current.RunID
				sourceIssue.ObservedAt = now
				if err := recorder.RecordIssue(ctx, sourceIssue); err != nil {
					t.Fatal(err)
				}
			}
			if tc.verify {
				current.VerifyReconciliation("source", "EXTERNAL_POSITIONS")
				current.VerifyReconciliation("balance", "")
			}
			current.Status = domain.ReconciliationRunAttentionRequired
			if tc.failed {
				current.Status = domain.ReconciliationRunFailed
			}
			completed := now.Add(time.Second)
			current.CompletedAt = &completed
			persisted, open, err := recorder.CompleteWithState(ctx, current)
			if err != nil {
				t.Fatal(err)
			}
			impact := domain.SummarizeReconciliationImpact(open)
			if impact.AccountWide != tc.wantAccount {
				t.Fatalf("impact=%#v open=%#v", impact, open)
			}
			if persisted.Summary["impact_account_wide"] != map[bool]int{true: 1, false: 0}[tc.wantAccount] {
				t.Fatalf("summary disagrees with persisted issues: %#v", persisted.Summary)
			}
			if len(impact.OrderIDs) != 1 || impact.OrderIDs[0] != "cancelled" {
				t.Fatalf("lost cancellation gate: %#v", impact)
			}
			var status string
			if err := db.QueryRow(`SELECT status FROM reconciliation_issues WHERE execution_account_id=$1 AND fingerprint='source'`, account).Scan(&status); err != nil {
				t.Fatal(err)
			}
			expected := "RESOLVED"
			if tc.wantAccount {
				expected = "OPEN"
			}
			if status != expected {
				t.Fatalf("source=%s want %s", status, expected)
			}
			assertAccount(t, db, account, "100", "90", "10")
			if !tc.wantAccount {
				for _, issue := range open {
					if issue.BlocksIntent(domain.OrderIntent{MarketID: "other-market", ConditionID: "other-condition", TokenID: "other-token"}) {
						t.Fatalf("unrelated placement blocked: %#v", issue)
					}
				}
				if !open[0].BlocksIntent(domain.OrderIntent{TokenID: "stuck-token"}) {
					t.Fatal("uncertain order lost its scoped protection")
				}
			}
			if _, _, err := recorder.CompleteWithState(ctx, current); err == nil {
				t.Fatal("duplicate completion succeeded")
			}
		})
	}
}

func TestRecoveryLeaseFirstAcquisitionRaceAndPendingSelectionPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, dsn)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	insertAccount(t, db, "race-account", "0xrace", "100", "100", "0")
	repo, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewOrderRecoveryLeaseStore(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a-delayed", "b-ready"} {
		order := integrationOrder(id, "race-account", id, domain.SideBuy, "5", "0.5")
		order.Status, order.Revision, order.CreatedAt, order.UpdatedAt = domain.OrderStatusReceived, 1, now.Add(-time.Hour), now.Add(-time.Hour)
		if _, _, err := repo.Create(ctx, order); err != nil {
			t.Fatal(err)
		}
	}
	delayed := integrationOrder("a-delayed", "race-account", "a-delayed", domain.SideBuy, "5", "0.5")
	gate := make(chan struct{})
	outcomes := make(chan struct {
		holder string
		err    error
	}, 16)
	var wg sync.WaitGroup
	for index := 0; index < 16; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			holder := fmt.Sprintf("worker-%d", index)
			_, err := store.AcquireOrderRecoveryLease(ctx, domain.OrderRecoveryLeaseRequest{OrderID: delayed.ID, ExecutionAccountID: "race-account", OrderRevision: 1, Holder: holder, Now: now, TTL: time.Minute})
			outcomes <- struct {
				holder string
				err    error
			}{holder, err}
		}()
	}
	close(gate)
	wg.Wait()
	close(outcomes)
	winner := ""
	success := 0
	for result := range outcomes {
		if result.err == nil {
			winner = result.holder
			success++
		} else if !errors.Is(result.err, port.ErrOrderRecoveryLeaseHeld) {
			t.Fatalf("unexpected race error=%v", result.err)
		}
	}
	if success != 1 {
		t.Fatalf("first acquisitions succeeded=%d", success)
	}
	// An active lease is excluded BEFORE LIMIT; it cannot hide the ready row.
	selected, err := repo.ListPending(ctx, now.Add(-time.Second), 1)
	if err != nil || len(selected) != 1 || selected[0].Intent.TokenID != "b-ready" {
		t.Fatalf("held selection=%#v err=%v", selected, err)
	}
	if _, err := store.ReleaseOrderRecoveryLease(ctx, domain.OrderRecoveryRelease{OrderID: delayed.ID, Holder: winner, Now: now, Kind: domain.OrderRecoveryOutcomeFailed, NextRetryAt: now.Add(time.Hour), Error: "404"}); err != nil {
		t.Fatal(err)
	}
	selected, err = repo.ListPending(ctx, now.Add(-time.Second), 1)
	if err != nil || len(selected) != 1 || selected[0].Intent.TokenID != "b-ready" {
		t.Fatalf("backoff selection=%#v err=%v", selected, err)
	}
	// The lease version can lag behind the order row; reject that stale view.
	if _, err := db.Exec(`UPDATE execution_orders SET revision=2 WHERE order_id=$1`, delayed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireOrderRecoveryLease(ctx, domain.OrderRecoveryLeaseRequest{OrderID: delayed.ID, ExecutionAccountID: "race-account", OrderRevision: 1, Holder: "stale", Now: now.Add(2 * time.Hour), TTL: time.Minute}); !errors.Is(err, port.ErrOrderRecoveryStaleView) {
		t.Fatalf("stale actual order accepted: %v", err)
	}
	request := domain.OrderRecoveryLeaseRequest{OrderID: delayed.ID, ExecutionAccountID: "race-account", OrderRevision: 2, Holder: "replacement", Now: now.Add(2 * time.Hour), TTL: time.Minute}
	if _, err := store.AcquireOrderRecoveryLease(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReleaseOrderRecoveryLease(ctx, domain.OrderRecoveryRelease{OrderID: delayed.ID, Holder: winner, Now: request.Now, Kind: domain.OrderRecoveryOutcomeResolved}); !errors.Is(err, port.ErrOrderRecoveryLeaseHeld) {
		t.Fatalf("old holder released replacement: %v", err)
	}
}

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestHistoricalIssueOrdersStayRecoverableUntilVerifiedPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, dsn)
	repository, _ := NewOrderRepository(db)
	recorder, _ := NewReconciliationRecorder(db)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, account := range []string{"history-account", "other-account"} {
		insertAccount(t, db, account, "0x"+account, "100", "100", "0")
	}
	prior := startReconciliationFixtureRun(t, recorder, "history-account", "history-prior", now.Add(-7*24*time.Hour))
	other := startReconciliationFixtureRun(t, recorder, "other-account", "other-prior", prior.StartedAt)
	for _, name := range []string{"retry", "stalled", "resolved", "manual", "foreign"} {
		account, run := "history-account", prior
		if name == "foreign" {
			account, run = "other-account", other
		}
		order := integrationOrder(name, account, "token-"+name, domain.SideBuy, "1", "0.5")
		order.Status = domain.OrderStatusFilled
		order.VenueOrderID = "venue-" + name
		order.CreatedAt, order.UpdatedAt = prior.StartedAt, prior.StartedAt
		insertReconciliationSelectionOrder(t, db, order)
		issue := domain.ReconciliationIssue{IssueID: "issue-" + name, RunID: run.RunID, Fingerprint: name,
			ExecutionAccountID: account, Type: domain.ReconciliationIssueSourceUnavailable,
			Resolution: domain.ReconciliationResolutionRetry, Status: domain.ReconciliationIssueOpen,
			OrderID: order.ID, TokenID: order.Intent.TokenID, Source: "CLOB_ORDER_TRADES", ObservedAt: prior.StartedAt}
		if name == "stalled" {
			issue.Type = domain.ReconciliationIssueOrderRecoveryStalled
			issue.Resolution = domain.ReconciliationResolutionManual
		}
		if name == "manual" {
			issue.Resolution = domain.ReconciliationResolutionManual
		}
		if name == "resolved" {
			issue.Status = domain.ReconciliationIssueResolved
			issue.ResolvedAt = &now
		}
		if err := recorder.RecordIssue(ctx, issue); err != nil {
			t.Fatal(err)
		}
		if name == "retry" {
			issue.IssueID = "issue-retry-again"
			issue.Fingerprint = "retry-again"
			if err := recorder.RecordIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
		}
	}
	ordinary, err := repository.ListForReconciliation(ctx, "history-account", now.Add(-48*time.Hour))
	if err != nil || len(ordinary) != 0 {
		t.Fatalf("historical fixture remained in normal scan: %v %v", ordinary, err)
	}
	orders, err := repository.ListWithOpenReconciliationIssues(ctx, "history-account")
	if err != nil || len(orders) != 2 || orders[0].ID != "order-retry" || orders[1].ID != "order-stalled" {
		t.Fatalf("recovery set crossed scope, duplicated or missed old orders: %v %v", orders, err)
	}
	completeReconciliationFixtureRun(t, recorder, prior, domain.ReconciliationRunAttentionRequired, prior.StartedAt.Add(time.Second))
	current := startReconciliationFixtureRun(t, recorder, "history-account", "history-current", now)
	current.VerifyReconciliation("order", "order-retry")
	completeReconciliationFixtureRun(t, recorder, current, domain.ReconciliationRunAttentionRequired, now.Add(time.Second))
	orders, err = repository.ListWithOpenReconciliationIssues(ctx, "history-account")
	if err != nil || len(orders) != 1 || orders[0].ID != "order-stalled" {
		t.Fatalf("verified orders kept retrying or unverified gate disappeared: %v %v", orders, err)
	}
}

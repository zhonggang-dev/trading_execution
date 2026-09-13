package reconciliation

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
)

func TestPartialRunVerifiesOnlyComparedAssets(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.refresher.errors = map[string]error{"order-unknown": errVenueDown}
	fixture.balance = "100"
	service := fixture.service(t, orderrecovery.Policy{})
	result := runScheduled(t, service)
	for _, key := range []string{"verified_position:token-1", "verified_order:order-unknown"} {
		if result.Run.Summary[key] != 0 {
			t.Fatalf("unresolved asset was declared verified: %s %#v", key, result.Run.Summary)
		}
	}
	for _, key := range []string{"verified_position:token-2", "verified_balance:", "verified_source:EXTERNAL_POSITIONS"} {
		if result.Run.Summary[key] != 1 {
			t.Fatalf("healthy independent check missing: %s %#v", key, result.Run.Summary)
		}
	}
	// A reservation-bounded cash explanation is not a verified balance match.
	fixture.balance = "99"
	result = runScheduled(t, service)
	if result.Run.Summary["verified_balance:"] != 0 {
		t.Fatal("bounded uncertainty was treated as verified cash")
	}
}

type completionStateRecorder struct {
	fakeRecorder
	open      []domain.ReconciliationIssue
	err       error
	recordErr error
}

func (recorder *completionStateRecorder) CompleteWithState(_ context.Context, run domain.ReconciliationRun) (domain.ReconciliationRun, []domain.ReconciliationIssue, error) {
	if len(recorder.open) > 0 && run.Status != domain.ReconciliationRunFailed {
		run.Status = domain.ReconciliationRunAttentionRequired
	}
	return run, recorder.open, recorder.err
}

func TestCompletionReportsHistoricalDurableGatesAndPersistenceFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		fixture := newRecoveryFixture()
		fixture.balance = "100"
		service := fixture.service(t, orderrecovery.Policy{})
		recorder := &completionStateRecorder{open: []domain.ReconciliationIssue{{IssueID: "historical-cash", RunID: "old-run",
			Type: domain.ReconciliationIssueBalanceDrift, Status: domain.ReconciliationIssueOpen, Resolution: domain.ReconciliationResolutionManual, Source: "CHAIN"}}}
		if fail {
			recorder.err = errors.New("commit failed")
		}
		service.recorder = recorder
		result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
		if fail {
			if !errors.Is(err, recorder.err) || result.Run.Status != domain.ReconciliationRunFailed {
				t.Fatalf("commit failure result=%#v err=%v", result, err)
			}
		} else if err != nil || !result.Impact.AccountWide || result.Run.Status != domain.ReconciliationRunAttentionRequired {
			t.Fatalf("historical gate disappeared from readiness: result=%#v err=%v", result, err)
		}
	}
}

func (recorder *completionStateRecorder) RecordIssue(ctx context.Context, issue domain.ReconciliationIssue) error {
	if recorder.recordErr != nil {
		return recorder.recordErr
	}
	return recorder.fakeRecorder.RecordIssue(ctx, issue)
}

func TestIssuePersistenceFailureCannotBecomeSuccessfulScan(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.balance = "100"
	fixture.refresher.errors = map[string]error{"order-unknown": errVenueDown}
	service := fixture.service(t, orderrecovery.Policy{})
	failure := errors.New("issue insert failed")
	service.recorder = &completionStateRecorder{recordErr: failure}
	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if !errors.Is(err, failure) || result.Run.Status != domain.ReconciliationRunFailed {
		t.Fatalf("unpersisted issue admitted placements: %#v %v", result, err)
	}
}

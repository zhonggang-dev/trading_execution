package autoredeem

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type fakeStore struct {
	events     []string
	applyErr   error
	retryTimes []time.Time
	// beginErr is returned by BeginRedemptionSubmission before any state change.
	beginErr error
	// failures is the durable submit-failure history; recordErr makes writing it fail.
	failures  domain.RedemptionSubmitFailures
	recordErr error
}

func (store *fakeStore) SyncPendingRedemptions(context.Context) error { return nil }
func (store *fakeStore) ListDueRedemptions(context.Context, int, time.Time) ([]domain.Redemption, error) {
	return nil, nil
}
func (store *fakeStore) BeginRedemptionSubmission(_ context.Context, _ domain.Redemption, kind domain.RedemptionSubmissionKind, _ time.Time) error {
	if store.beginErr != nil {
		return store.beginErr
	}
	store.events = append(store.events, "begin:"+string(kind))
	return nil
}
func (store *fakeStore) RecordRedemptionSubmitFailure(_ context.Context, _ domain.Redemption, failure domain.RedemptionSubmitFailure) (domain.RedemptionSubmitFailures, error) {
	if store.recordErr != nil {
		return domain.RedemptionSubmitFailures{}, store.recordErr
	}
	store.events = append(store.events, "failure:"+string(failure.Outcome))
	store.failures.Count++
	store.failures.LatestText = failure.ErrorText
	return store.failures, nil
}
func (store *fakeStore) RedemptionSubmitFailures(context.Context, domain.Redemption) (domain.RedemptionSubmitFailures, error) {
	return store.failures, nil
}
func (store *fakeStore) ResetRedemptionForResubmit(_ context.Context, _ domain.Redemption, reason string, _ time.Time) error {
	store.events = append(store.events, "resubmit:"+reason)
	return nil
}
func (store *fakeStore) RecordRedemptionSubmission(_ context.Context, _ domain.Redemption, submission domain.RedemptionSubmission, _, _ time.Time) error {
	store.events = append(store.events, "record:"+submission.Reference)
	return nil
}
func (store *fakeStore) ResetRedemptionReady(context.Context, domain.Redemption, time.Time) error {
	store.events = append(store.events, "ready")
	return nil
}
func (store *fakeStore) RecordRedemptionTransaction(_ context.Context, _ domain.Redemption, hash string, _ time.Time) error {
	store.events = append(store.events, "tx:"+hash)
	return nil
}
func (store *fakeStore) RecordRedemptionConfirmed(_ context.Context, _ domain.Redemption, receipt domain.RedemptionReceipt, _ time.Time) error {
	store.events = append(store.events, "confirmed:"+receipt.TransactionHash)
	return nil
}
func (store *fakeStore) ApplyRedemption(context.Context, domain.Redemption, time.Time) error {
	if store.applyErr != nil {
		return store.applyErr
	}
	store.events = append(store.events, "applied")
	return nil
}
func (store *fakeStore) RetryRedemption(_ context.Context, _ domain.Redemption, reason string, next time.Time) error {
	store.events = append(store.events, "retry:"+reason)
	store.retryTimes = append(store.retryTimes, next)
	return nil
}
func (store *fakeStore) ReviewRedemption(_ context.Context, _ domain.Redemption, reason string, _ time.Time) error {
	store.events = append(store.events, "review:"+reason)
	return nil
}

type fakeVenue struct {
	approved   bool
	submission domain.RedemptionSubmission
	store      *fakeStore
	submits    int
	submitErr  error
}

// classifiedError is a submit error carrying the venue's classification, like
// the one the Polymarket client returns.
type classifiedError struct {
	outcome domain.RedemptionSubmitOutcome
	status  int
	message string
}

func (failure *classifiedError) Error() string { return failure.message }
func (failure *classifiedError) SubmitOutcome() domain.RedemptionSubmitOutcome {
	return failure.outcome
}
func (failure *classifiedError) HTTPStatus() int { return failure.status }

func (venue *fakeVenue) RedemptionApproved(context.Context, string, bool) (bool, error) {
	return venue.approved, nil
}
func (venue *fakeVenue) SubmitRedemptionApproval(context.Context, string, bool) (domain.RedemptionSubmission, error) {
	return domain.RedemptionSubmission{}, fmt.Errorf("unexpected approval")
}
func (venue *fakeVenue) SubmitRedemption(context.Context, string, string, bool) (domain.RedemptionSubmission, error) {
	venue.submits++
	if venue.store == nil || len(venue.store.events) == 0 || venue.store.events[0] != "begin:REDEEM" {
		return domain.RedemptionSubmission{}, fmt.Errorf("venue mutation happened before durable submit intent")
	}
	venue.store.events = append(venue.store.events, "submit")
	if venue.submitErr != nil {
		return domain.RedemptionSubmission{}, venue.submitErr
	}
	return venue.submission, nil
}
func (venue *fakeVenue) ResolveRedemptionSubmission(context.Context, string, domain.RedemptionSubmission) (domain.RedemptionSubmission, error) {
	return venue.submission, nil
}

type fakeReceipts struct {
	receipt domain.RedemptionReceipt
	err     error
}

func (source fakeReceipts) ResolveRedemptionReceipt(context.Context, string, string, string, bool) (domain.RedemptionReceipt, error) {
	if source.err != nil {
		return domain.RedemptionReceipt{}, source.err
	}
	return source.receipt, nil
}

type fakeActivities struct{ activities []domain.RedeemActivity }

func (source fakeActivities) ListRedeemActivities(context.Context, string, string, time.Time) ([]domain.RedeemActivity, error) {
	return source.activities, nil
}

func newTestService(t *testing.T, store *fakeStore, venue *fakeVenue, receipts fakeReceipts, activities fakeActivities) *Service {
	t.Helper()
	service, err := New(Params{
		Store: store, Venue: venue, Receipts: receipts, Activities: activities,
		PollInterval: time.Minute, RetryInterval: 30 * time.Second,
		AmbiguityTimeout: 10 * time.Minute, BatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testRedemption(status domain.RedemptionStatus) domain.Redemption {
	return domain.Redemption{
		ExecutionAccountID: "wallet-6",
		ConditionID:        "0x9601280c3d5109783ba64644da25bdfdf120ce516a696f88a42f54ffd2ac761b",
		WalletAddress:      "0x635d25519789e40c3794d72a88cbb7f25ac443f8",
		NegRisk:            true, Status: status,
	}
}

func TestReadyPersistsSubmitIntentBeforeVenueMutation(t *testing.T) {
	store := &fakeStore{}
	venue := &fakeVenue{approved: true, store: store, submission: domain.RedemptionSubmission{
		Provider: "POLYMARKET_RELAYER", Reference: "relayer-1", State: domain.RedemptionSubmissionPending,
	}}
	service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
	if err := service.process(context.Background(), testRedemption(domain.RedemptionReady), time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
	want := []string{"begin:REDEEM", "submit", "record:relayer-1"}
	if fmt.Sprint(store.events) != fmt.Sprint(want) {
		t.Fatalf("event order = %v, want %v", store.events, want)
	}
}

func TestAmbiguousRedeemIsResubmittedOnlyAfterTimeoutWithoutEvidence(t *testing.T) {
	started := time.Unix(1000, 0).UTC()
	redemption := testRedemption(domain.RedemptionRedeemSubmitting)
	redemption.SubmittingAt = &started

	t.Run("before the timeout it only waits", func(t *testing.T) {
		store := &fakeStore{}
		venue := &fakeVenue{store: store}
		service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
		if err := service.process(context.Background(), redemption, started.Add(5*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if venue.submits != 0 || len(store.events) != 1 || firstEventKind(store) != "retry" {
			t.Fatalf("early recovery submits/events = %d/%v, want one retry", venue.submits, store.events)
		}
	})
	t.Run("after the timeout without evidence it resets for resubmission", func(t *testing.T) {
		store := &fakeStore{failures: domain.RedemptionSubmitFailures{Count: 1, LatestText: "relayer HTTP 502: bad gateway"}}
		venue := &fakeVenue{store: store}
		service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
		err := service.process(context.Background(), redemption, started.Add(11*time.Minute))
		if err == nil || venue.submits != 0 || len(store.events) != 1 || firstEventKind(store) != "resubmit" {
			t.Fatalf("timeout recovery error/submits/events = %v/%d/%v, want one resubmit reset", err, venue.submits, store.events)
		}
		if !strings.Contains(store.events[0], "relayer HTTP 502: bad gateway") {
			t.Fatalf("reset reason %q lost the raw error", store.events[0])
		}
	})
	t.Run("after the failure limit it goes to manual review with the raw error", func(t *testing.T) {
		store := &fakeStore{failures: domain.RedemptionSubmitFailures{Count: 3, LatestText: "relayer HTTP 502: bad gateway"}}
		service := newTestService(t, store, &fakeVenue{store: store}, fakeReceipts{}, fakeActivities{})
		err := service.process(context.Background(), redemption, started.Add(11*time.Minute))
		if err == nil || len(store.events) != 1 || firstEventKind(store) != "review" ||
			!strings.Contains(store.events[0], "relayer HTTP 502: bad gateway") {
			t.Fatalf("exhausted recovery error/events = %v/%v, want review with the raw error", err, store.events)
		}
	})
	t.Run("matching chain activity confirms instead of resubmitting", func(t *testing.T) {
		hash := "0x" + strings.Repeat("44", 32)
		store := &fakeStore{}
		venue := &fakeVenue{store: store}
		service := newTestService(t, store, venue,
			fakeReceipts{receipt: domain.RedemptionReceipt{TransactionHash: hash}},
			fakeActivities{activities: []domain.RedeemActivity{{TransactionHash: hash}}})
		if err := service.process(context.Background(), redemption, started.Add(11*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if venue.submits != 0 || len(store.events) != 1 || store.events[0] != "confirmed:"+hash {
			t.Fatalf("activity recovery submits/events = %d/%v, want confirmation only", venue.submits, store.events)
		}
	})
}

func TestApprovalSubmitOutcomeUnresolvedResubmitsWithinLimit(t *testing.T) {
	started := time.Unix(1000, 0).UTC()
	redemption := testRedemption(domain.RedemptionApprovalSubmitting)
	redemption.SubmittingAt = &started
	store := &fakeStore{failures: domain.RedemptionSubmitFailures{Count: 1, LatestText: "timeout"}}
	service := newTestService(t, store, &fakeVenue{store: store, approved: false}, fakeReceipts{}, fakeActivities{})
	if err := service.process(context.Background(), redemption, started.Add(11*time.Minute)); err == nil ||
		len(store.events) != 1 || firstEventKind(store) != "resubmit" {
		t.Fatalf("approval recovery error/events = %v/%v, want one resubmit reset", err, store.events)
	}
}

func TestWalletBusyDefersWithoutAnyWrite(t *testing.T) {
	store := &fakeStore{beginErr: fmt.Errorf("check: %w", port.ErrRedemptionWalletBusy)}
	venue := &fakeVenue{approved: true, store: store}
	service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
	if err := service.process(context.Background(), testRedemption(domain.RedemptionReady), time.Unix(1000, 0)); err != nil {
		t.Fatalf("a deferral is not a failure: %v", err)
	}
	if venue.submits != 0 || len(store.events) != 0 {
		t.Fatalf("deferred redemption submits/events = %d/%v, want none", venue.submits, store.events)
	}
}

func TestDefinitiveSubmitFailureIsRecordedAndResubmitted(t *testing.T) {
	for _, outcome := range []domain.RedemptionSubmitOutcome{domain.RedemptionSubmitRejected, domain.RedemptionSubmitNotSubmitted} {
		t.Run(string(outcome), func(t *testing.T) {
			store := &fakeStore{}
			venue := &fakeVenue{approved: true, store: store, submitErr: &classifiedError{
				outcome: outcome, status: 400, message: `relayer HTTP 400: {"error":"invalid nonce"}`,
			}}
			service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
			err := service.process(context.Background(), testRedemption(domain.RedemptionReady), time.Unix(1000, 0))
			if err == nil {
				t.Fatal("a failed submit must still surface as a sweep error")
			}
			if len(store.events) != 4 || store.events[2] != "failure:"+string(outcome) || !strings.HasPrefix(store.events[3], "resubmit:") ||
				!strings.Contains(store.events[3], "invalid nonce") {
				t.Fatalf("events = %v, want begin, submit, failure, resubmit with the raw error", store.events)
			}
		})
	}
}

func TestSubmitFailureLimitMovesToManualReviewWithRawError(t *testing.T) {
	store := &fakeStore{failures: domain.RedemptionSubmitFailures{Count: 2, LatestText: "earlier"}}
	venue := &fakeVenue{approved: true, store: store, submitErr: &classifiedError{
		outcome: domain.RedemptionSubmitRejected, status: 400, message: `relayer HTTP 400: {"error":"invalid nonce"}`,
	}}
	service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
	err := service.process(context.Background(), testRedemption(domain.RedemptionReady), time.Unix(1000, 0))
	last := store.events[len(store.events)-1]
	if err == nil || !strings.HasPrefix(last, "review:") || !strings.Contains(last, "invalid nonce") {
		t.Fatalf("limit error/events = %v/%v, want MANUAL_REVIEW carrying the raw error", err, store.events)
	}
}

func TestAmbiguousSubmitFailureIsRecordedButNeverResubmittedEarly(t *testing.T) {
	store := &fakeStore{}
	venue := &fakeVenue{approved: true, store: store, submitErr: errors.New("context deadline exceeded")}
	service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
	if err := service.process(context.Background(), testRedemption(domain.RedemptionReady), time.Unix(1000, 0)); err == nil {
		t.Fatal("ambiguous submit must surface as a sweep error")
	}
	if len(store.events) != 4 || store.events[2] != "failure:AMBIGUOUS" || !strings.HasPrefix(store.events[3], "retry:") {
		t.Fatalf("events = %v, want begin, submit, failure, retry (stay *_SUBMITTING)", store.events)
	}
}

func TestSubmitFailureWithoutDurableRecordNeverResubmits(t *testing.T) {
	store := &fakeStore{recordErr: errors.New("postgres unavailable")}
	venue := &fakeVenue{approved: true, store: store, submitErr: &classifiedError{
		outcome: domain.RedemptionSubmitRejected, status: 400, message: "relayer HTTP 400",
	}}
	service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
	if err := service.process(context.Background(), testRedemption(domain.RedemptionReady), time.Unix(1000, 0)); err == nil {
		t.Fatal("expected an error")
	}
	for _, event := range store.events {
		if strings.HasPrefix(event, "resubmit:") || strings.HasPrefix(event, "review:") {
			t.Fatalf("events = %v: without a durable failure count only evidence may move the redemption", store.events)
		}
	}
}

func TestMaxSubmitAttemptsIsValidated(t *testing.T) {
	for _, attempts := range []int{-1, 11} {
		_, err := New(Params{
			Store: &fakeStore{}, Venue: &fakeVenue{}, Receipts: fakeReceipts{}, Activities: fakeActivities{},
			PollInterval: time.Minute, RetryInterval: 30 * time.Second, AmbiguityTimeout: 10 * time.Minute,
			BatchSize: 10, MaxSubmitAttempts: attempts,
		})
		if err == nil {
			t.Fatalf("max submit attempts %d was accepted", attempts)
		}
	}
}

func TestSubmittedRedeemRequiresReceiptBeforeConfirmation(t *testing.T) {
	store := &fakeStore{}
	hash := "0x" + strings.Repeat("11", 32)
	venue := &fakeVenue{submission: domain.RedemptionSubmission{
		Provider: "POLYMARKET_RELAYER", Reference: "relayer-1", TransactionHash: hash,
		State: domain.RedemptionSubmissionConfirmed,
	}}
	redemption := testRedemption(domain.RedemptionRedeemSubmitted)
	redemption.SubmissionProvider = "POLYMARKET_RELAYER"
	redemption.SubmissionReference = "relayer-1"
	receipt := domain.RedemptionReceipt{
		TransactionHash: hash, WalletAddress: redemption.WalletAddress,
		ConditionID: redemption.ConditionID, EventType: "POSITIONS_REDEEMED",
		PayoutBaseUnits: "48000000", BlockNumber: 100, BlockHash: "0x" + strings.Repeat("22", 32),
		Confirmations: 64,
	}
	service := newTestService(t, store, venue, fakeReceipts{receipt: receipt}, fakeActivities{})
	if err := service.process(context.Background(), redemption, time.Unix(2000, 0)); err != nil {
		t.Fatal(err)
	}
	want := []string{"tx:" + hash, "confirmed:" + hash}
	if fmt.Sprint(store.events) != fmt.Sprint(want) {
		t.Fatalf("receipt event order = %v, want %v", store.events, want)
	}
}

func confirmedRedemption(confirmedAt time.Time) domain.Redemption {
	redemption := testRedemption(domain.RedemptionConfirmed)
	redemption.TransactionHash = "0x" + strings.Repeat("11", 32)
	redemption.PayoutBaseUnits = "3350000"
	redemption.ConfirmedAt = &confirmedAt
	return redemption
}

func firstEventKind(store *fakeStore) string {
	if len(store.events) == 0 {
		return ""
	}
	return strings.SplitN(store.events[0], ":", 2)[0]
}

func TestConfirmedApplyEvidenceMismatchMovesToManualReview(t *testing.T) {
	now := time.Unix(3000, 0).UTC()
	store := &fakeStore{applyErr: &domain.RedemptionEvidenceError{Reason: "redemption payout 3.35 does not equal managed binary payout 3.4"}}
	service := newTestService(t, store, &fakeVenue{}, fakeReceipts{}, fakeActivities{})
	err := service.process(context.Background(), confirmedRedemption(now.Add(-time.Second)), now)
	if err == nil || len(store.events) != 1 || firstEventKind(store) != "review" ||
		!strings.Contains(store.events[0], "does not equal managed binary payout") {
		t.Fatalf("permanent apply failure error/events = %v/%v, want one MANUAL_REVIEW", err, store.events)
	}
}

func TestConfirmedApplyTransientFailureKeepsRetryingPastAmbiguityTimeout(t *testing.T) {
	confirmedAt := time.Unix(3000, 0).UTC()
	// Retry interval 30s, ambiguity timeout 10m (newTestService).
	cases := []struct {
		age  time.Duration
		wait time.Duration
	}{
		{age: time.Minute, wait: 30 * time.Second},
		{age: 10 * time.Minute, wait: 150 * time.Second},
		{age: 12 * time.Minute, wait: 3 * time.Minute},
		{age: 20 * time.Minute, wait: 5 * time.Minute},
		{age: 6 * time.Hour, wait: 5 * time.Minute},
	}
	var logs strings.Builder
	store := &fakeStore{applyErr: errors.New("postgres connection reset")}
	service := newTestService(t, store, &fakeVenue{}, fakeReceipts{}, fakeActivities{})
	service.logger = slog.New(slog.NewTextHandler(&logs, nil))
	redemption := confirmedRedemption(confirmedAt)
	for index, tc := range cases {
		now := confirmedAt.Add(tc.age)
		err := service.process(context.Background(), redemption, now)
		if err == nil || len(store.events) != index+1 || !strings.HasPrefix(store.events[index], "retry:") {
			t.Fatalf("age %s error/events = %v/%v, want a retry and no MANUAL_REVIEW", tc.age, err, store.events)
		}
		if got := store.retryTimes[index].Sub(now); got != tc.wait {
			t.Fatalf("age %s next attempt in %s, want %s", tc.age, got, tc.wait)
		}
	}
	output := logs.String()
	if strings.Count(output, "level=ERROR") != len(cases)-1 {
		t.Fatalf("stalled apply ERROR logs = %q, want one per failure past the ambiguity timeout", output)
	}
	for _, field := range []string{
		"execution_account_id=wallet-6", "condition_id=" + redemption.ConditionID,
		"transaction_hash=" + redemption.TransactionHash, "failing_for=6h0m0s",
		`last_error="postgres connection reset"`,
	} {
		if !strings.Contains(output, field) {
			t.Fatalf("stalled apply log %q is missing %q", output, field)
		}
	}

	store.applyErr = nil
	if err := service.process(context.Background(), redemption, confirmedAt.Add(7*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if last := store.events[len(store.events)-1]; last != "applied" {
		t.Fatalf("recovered apply events = %v, want applied", store.events)
	}
	for _, event := range store.events {
		if strings.HasPrefix(event, "review:") {
			t.Fatalf("confirmed redemption moved to MANUAL_REVIEW on a transient failure: %v", store.events)
		}
	}
}

func TestConfirmedApplyEvidenceMismatchStillReviewsAfterAmbiguityTimeout(t *testing.T) {
	now := time.Unix(3000, 0).UTC()
	store := &fakeStore{applyErr: &domain.RedemptionEvidenceError{Reason: "redemption payout does not match chain"}}
	service := newTestService(t, store, &fakeVenue{}, fakeReceipts{}, fakeActivities{})
	err := service.process(context.Background(), confirmedRedemption(now.Add(-time.Hour)), now)
	if err == nil || len(store.events) != 1 || firstEventKind(store) != "review" {
		t.Fatalf("permanent apply failure error/events = %v/%v, want one MANUAL_REVIEW", err, store.events)
	}
}

func TestPendingSubmissionEscalatesAfterAmbiguityTimeout(t *testing.T) {
	now := time.Unix(4000, 0).UTC()
	hash := "0x" + strings.Repeat("33", 32)
	cases := map[string]struct {
		status   domain.RedemptionStatus
		venue    *fakeVenue
		receipts fakeReceipts
		hash     string
	}{
		"approval pending": {
			status: domain.RedemptionApprovalSubmitted,
			venue:  &fakeVenue{submission: domain.RedemptionSubmission{Provider: "POLYMARKET_RELAYER", Reference: "relayer-1", State: domain.RedemptionSubmissionPending}},
		},
		"redeem without transaction hash": {
			status: domain.RedemptionRedeemSubmitted,
			venue:  &fakeVenue{submission: domain.RedemptionSubmission{Provider: "POLYMARKET_RELAYER", Reference: "relayer-1", State: domain.RedemptionSubmissionPending}},
		},
		"redeem receipt not final": {
			status:   domain.RedemptionRedeemSubmitted,
			venue:    &fakeVenue{submission: domain.RedemptionSubmission{Provider: "POLYMARKET_RELAYER", Reference: "relayer-1", TransactionHash: hash, State: domain.RedemptionSubmissionPending}},
			receipts: fakeReceipts{err: errors.New("redemption receipt is pending or not finalized")},
			hash:     hash,
		},
	}
	for name, tc := range cases {
		for age, want := range map[time.Duration]string{time.Minute: "retry", 11 * time.Minute: "review"} {
			t.Run(fmt.Sprintf("%s after %s", name, age), func(t *testing.T) {
				store := &fakeStore{}
				service := newTestService(t, store, tc.venue, tc.receipts, fakeActivities{})
				redemption := testRedemption(tc.status)
				redemption.SubmissionProvider, redemption.SubmissionReference = "POLYMARKET_RELAYER", "relayer-1"
				redemption.TransactionHash = tc.hash
				submitting := now.Add(-age - time.Second)
				submitted := now.Add(-age)
				redemption.SubmittingAt, redemption.SubmittedAt = &submitting, &submitted
				err := service.process(context.Background(), redemption, now)
				if want == "review" && err == nil {
					t.Fatal("escalation returned nil error")
				}
				if len(store.events) == 0 || firstEventKind(store) != want {
					t.Fatalf("pending %s events = %v, want first event %s", name, store.events, want)
				}
				for _, event := range store.events {
					if strings.HasPrefix(event, "confirmed:") || event == "applied" {
						t.Fatalf("pending %s advanced without evidence: %v", name, store.events)
					}
				}
			})
		}
	}
}

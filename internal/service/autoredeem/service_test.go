package autoredeem

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type fakeStore struct {
	events   []string
	applyErr error
}

func (store *fakeStore) SyncPendingRedemptions(context.Context) error { return nil }
func (store *fakeStore) ListDueRedemptions(context.Context, int, time.Time) ([]domain.Redemption, error) {
	return nil, nil
}
func (store *fakeStore) BeginRedemptionSubmission(_ context.Context, _ domain.Redemption, kind domain.RedemptionSubmissionKind, _ time.Time) error {
	store.events = append(store.events, "begin:"+string(kind))
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
func (store *fakeStore) RetryRedemption(_ context.Context, _ domain.Redemption, reason string, _ time.Time) error {
	store.events = append(store.events, "retry:"+reason)
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
}

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

func TestAmbiguousRedeemIsNeverBlindlyResubmitted(t *testing.T) {
	store := &fakeStore{}
	venue := &fakeVenue{store: store}
	service := newTestService(t, store, venue, fakeReceipts{}, fakeActivities{})
	redemption := testRedemption(domain.RedemptionRedeemSubmitting)
	started := time.Unix(1000, 0).UTC()
	redemption.SubmittingAt = &started
	err := service.process(context.Background(), redemption, started.Add(11*time.Minute))
	if err == nil || venue.submits != 0 || len(store.events) != 1 || store.events[0][:7] != "review:" {
		t.Fatalf("ambiguous recovery error/submits/events = %v/%d/%v", err, venue.submits, store.events)
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

func TestConfirmedApplyTransientFailureRetriesUntilAmbiguityTimeout(t *testing.T) {
	now := time.Unix(3000, 0).UTC()
	for name, tc := range map[string]struct {
		confirmedAt time.Time
		want        string
	}{
		"recent confirmation retries": {confirmedAt: now.Add(-time.Minute), want: "retry"},
		"stale confirmation reviews":  {confirmedAt: now.Add(-11 * time.Minute), want: "review"},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{applyErr: errors.New("postgres connection reset")}
			service := newTestService(t, store, &fakeVenue{}, fakeReceipts{}, fakeActivities{})
			err := service.process(context.Background(), confirmedRedemption(tc.confirmedAt), now)
			if err == nil || len(store.events) != 1 || firstEventKind(store) != tc.want {
				t.Fatalf("transient apply failure error/events = %v/%v, want %s", err, store.events, tc.want)
			}
		})
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

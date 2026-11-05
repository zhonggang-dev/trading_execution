package nofillrelease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var (
	testSubmitAt  = time.Date(2026, 10, 26, 17, 20, 0, 0, time.UTC)
	testExpiresAt = testSubmitAt.Add(5 * time.Minute)
	testOrderHash = "0x" + strings.Repeat("e6", 32)
	testReadyAt   = testExpiresAt.Add(30*time.Minute + time.Second)
)

func eligibleFacts() domain.NoFillLocalFacts {
	expires := testExpiresAt
	completed := testSubmitAt.Add(2730 * time.Millisecond)
	readDone := testSubmitAt.Add(time.Minute)
	return domain.NoFillLocalFacts{
		Order: domain.Order{
			ID: "ord-e6578c7e", VenueOrderID: testOrderHash, Status: domain.OrderStatusUnknown,
			FilledSize: "0", FilledNotional: "0", TotalFees: "0", Revision: 7,
			FailureCode: "CLOB_ORDER_NOT_FOUND",
			CreatedAt:   testSubmitAt.Add(-time.Second), UpdatedAt: testSubmitAt.Add(time.Minute),
			Intent: domain.OrderIntent{
				Venue: "polymarket", ExecutionAccountID: "main", ClientOrderID: "client-1",
				ConditionID: "0xcondition", TokenID: "123", Side: domain.SideBuy, Size: "4", WorstPrice: "0.266",
			},
		},
		Attempts: []domain.OrderAttempt{
			{ID: "attempt:ord-e6578c7e:1", Sequence: 1, Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeUnknown,
				VenueOrderID: testOrderHash, HTTPStatus: 500, ErrorCode: "CLOB_SERVER_ERROR",
				ErrorMessage: "rpc error: code = DeadlineExceeded", StartedAt: testSubmitAt, CompletedAt: &completed, VenueExpiresAt: &expires},
			{ID: "attempt:ord-e6578c7e:2", Sequence: 2, Kind: domain.OrderAttemptReconcile, Outcome: domain.AttemptOutcomeUnknown,
				VenueOrderID: testOrderHash, VenueStatus: "CLOB_ORDER_NOT_FOUND", ErrorCode: "CLOB_ORDER_NOT_FOUND",
				StartedAt: testSubmitAt.Add(30 * time.Second), CompletedAt: &readDone},
		},
		EventStatuses: []domain.OrderStatus{
			domain.OrderStatusReceived, domain.OrderStatusValidating, domain.OrderStatusReserved, domain.OrderStatusSubmitting,
			domain.OrderStatusUnknown, domain.OrderStatusReconciling, domain.OrderStatusUnknown,
		},
		Reservation: &domain.AssetReservation{
			OrderID: "ord-e6578c7e", Side: domain.SideBuy, Status: domain.ReservationStatusReconciliationRequired,
			RemainingReservedBalance: "1.064", RemainingReservedShares: "0",
			SettledShares: "0", SettledNotional: "0", SettledFees: "0", Revision: 3,
		},
		OpenIssues: []domain.NoFillIssueRef{
			{IssueID: "issue-1", Type: domain.ReconciliationIssueSourceUnavailable, Source: "CLOB_ORDER", Resolution: domain.ReconciliationResolutionRetry},
			{IssueID: "issue-2", Type: domain.ReconciliationIssueOrderRecoveryPending, Source: "ORDER_RECOVERY", Resolution: domain.ReconciliationResolutionRetry},
			{IssueID: "issue-3", Type: domain.ReconciliationIssueOrderRecoveryStalled, Source: "ORDER_RECOVERY", Resolution: domain.ReconciliationResolutionManual},
		},
	}
}

type fakeStore struct {
	facts     domain.NoFillLocalFacts
	factsErr  error
	applyErr  error
	applied   []domain.NoFillResolution
	shadows   []domain.NoFillResolution
	hasShadow bool
}

func (store *fakeStore) Facts(context.Context, string) (domain.NoFillLocalFacts, error) {
	return store.facts, store.factsErr
}

func (store *fakeStore) Apply(_ context.Context, resolution domain.NoFillResolution) (domain.Order, error) {
	if store.applyErr != nil {
		return domain.Order{}, store.applyErr
	}
	store.applied = append(store.applied, resolution)
	order := store.facts.Order
	order.Status = domain.OrderStatusCancelled
	order.Revision += 2
	return order, nil
}

func (store *fakeStore) RecordShadow(_ context.Context, resolution domain.NoFillResolution) (bool, error) {
	store.shadows = append(store.shadows, resolution)
	return true, nil
}

func (store *fakeStore) HasShadow(context.Context, string) (bool, error) { return store.hasShadow, nil }

type fakeVenue struct {
	read      domain.NoFillOrderRead
	readErr   error
	trades    domain.NoFillTradeScan
	tradesErr error
	open      domain.NoFillOpenOrderScan
	openErr   error
	calls     int
}

func (venue *fakeVenue) ReadOrder(context.Context, domain.Order) (domain.NoFillOrderRead, error) {
	venue.calls++
	return venue.read, venue.readErr
}

func (venue *fakeVenue) TradeReferences(context.Context, domain.Order, time.Time) (domain.NoFillTradeScan, error) {
	venue.calls++
	return venue.trades, venue.tradesErr
}

func (venue *fakeVenue) OpenOrders(context.Context, domain.Order) (domain.NoFillOpenOrderScan, error) {
	venue.calls++
	return venue.open, venue.openErr
}

type fakeChain struct {
	evidence domain.NoFillChainEvidence
	err      error
	request  domain.NoFillChainScanRequest
	calls    int
}

func (chain *fakeChain) ScanOrderFilled(_ context.Context, request domain.NoFillChainScanRequest) (domain.NoFillChainEvidence, error) {
	chain.calls++
	chain.request = request
	return chain.evidence, chain.err
}

func cleanVenue() *fakeVenue {
	return &fakeVenue{
		read:   domain.NoFillOrderRead{NotFound: true, Code: "CLOB_ORDER_NOT_FOUND", ReadAt: testReadyAt},
		trades: domain.NoFillTradeScan{Market: "0xcondition", Scanned: 12, References: []domain.NoFillTradeReference{}},
		open:   domain.NoFillOpenOrderScan{Market: "0xcondition", TokenID: "123", Returned: 0},
	}
}

func cleanChain() *fakeChain {
	return &fakeChain{evidence: domain.NoFillChainEvidence{
		ChainID: 137, FromBlock: 100, ToBlock: 1500, ToBlockHashBefore: "0xaa", ToBlockHashAfter: "0xaa",
		Matches: []domain.NoFillChainMatch{},
		Controls: []domain.NoFillChainControl{
			{BlockNumber: 1500, Logs: 15}, {BlockNumber: 800, Logs: 14}, {BlockNumber: 100, Logs: 16},
		},
	}}
}

func newTestResolver(t *testing.T, store *fakeStore, venue *fakeVenue, chain *fakeChain, mode Mode, policy Policy, now time.Time) *Resolver {
	t.Helper()
	resolver, err := New(Params{
		Store: store, Venue: venue, Chain: chain, Mode: mode, Policy: policy,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func TestEvaluateResolvesOnlyWithCompleteEvidence(t *testing.T) {
	store := &fakeStore{facts: eligibleFacts()}
	venue, chain := cleanVenue(), cleanChain()
	resolver := newTestResolver(t, store, venue, chain, ModeOn, Policy{}, testReadyAt)
	evaluation, err := resolver.Evaluate(context.Background(), "ord-e6578c7e", EvaluateOptions{Mode: domain.NoFillModeAuto})
	if err != nil || evaluation.Decision.Kind != DecisionResolve {
		t.Fatalf("Evaluate() = %#v, %v", evaluation.Decision, err)
	}
	if !chain.request.FromNotAfter.Equal(testSubmitAt.Add(-time.Second).Add(-10*time.Minute)) ||
		!chain.request.ToNotBefore.Equal(testExpiresAt.Add(10*time.Minute)) || chain.request.MaxChunks != 30 ||
		chain.request.OrderHash != testOrderHash {
		t.Fatalf("chain request = %#v", chain.request)
	}
	digest := sha256.Sum256(evaluation.EvidenceJSON)
	if hex.EncodeToString(digest[:]) != evaluation.Digest {
		t.Fatal("evidence digest is not the sha256 of the stored document")
	}
	var decoded map[string]any
	if err := json.Unmarshal(evaluation.EvidenceJSON, &decoded); err != nil || decoded["version"] != float64(1) {
		t.Fatalf("evidence document = %s", evaluation.EvidenceJSON)
	}
	if evaluation.Evidence.Release.Balance != "1.064" || len(evaluation.Evidence.IssuesResolved) != 3 {
		t.Fatalf("release = %#v issues = %#v", evaluation.Evidence.Release, evaluation.Evidence.IssuesResolved)
	}
}

func TestEvaluateKeepsOrderFrozenWhenAnyConditionFails(t *testing.T) {
	type mutation func(*domain.NoFillLocalFacts, *fakeVenue, *fakeChain, *time.Time)
	cases := []struct {
		name   string
		mutate mutation
		want   DecisionKind
		remote bool // whether remote sources may be consulted
	}{
		{name: "before deadline", want: DecisionNotYet, mutate: func(_ *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, now *time.Time) {
			*now = testExpiresAt.Add(29 * time.Minute)
		}},
		{name: "no venue expiry (historical GTC)", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Attempts[0].VenueExpiresAt = nil
		}},
		{name: "partial fill on order", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Order.FilledSize = "1"
		}},
		{name: "pending fill row", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.FillCount = 1
		}},
		{name: "settled reservation", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Reservation.SettledShares = "1"
		}},
		{name: "ever acknowledged", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.EventStatuses = append(f.EventStatuses, domain.OrderStatusAcknowledged)
		}},
		{name: "a read once saw the order", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Attempts[1].Outcome = domain.AttemptOutcomeSucceeded
		}},
		{name: "unexpected read failure code", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Attempts[1].ErrorCode = "CLOB_FILL_DETAILS_UNAVAILABLE"
		}},
		{name: "cancel attempt", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Attempts = append(f.Attempts, domain.OrderAttempt{Kind: domain.OrderAttemptCancel, Outcome: domain.AttemptOutcomeUnknown})
		}},
		{name: "missed fill issue", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.OpenIssues = append(f.OpenIssues, domain.NoFillIssueRef{Type: domain.ReconciliationIssueMissedBuyFill, Source: "POLYMARKET_TRADES"})
		}},
		{name: "other source unavailable", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.OpenIssues = append(f.OpenIssues, domain.NoFillIssueRef{Type: domain.ReconciliationIssueSourceUnavailable, Source: "CLOB_ORDER_TRADES"})
		}},
		{name: "not UNKNOWN", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Order.Status = domain.OrderStatusCancelled
		}},
		{name: "kalshi", want: DecisionNotEligible, mutate: func(f *domain.NoFillLocalFacts, _ *fakeVenue, _ *fakeChain, _ *time.Time) {
			f.Order.Intent.Venue = "kalshi"
		}},
		{name: "order read failed", want: DecisionInconclusive, remote: true, mutate: func(_ *domain.NoFillLocalFacts, v *fakeVenue, _ *fakeChain, _ *time.Time) {
			v.readErr = errors.New("CLOB 502")
		}},
		{name: "order visible", want: DecisionNotEligible, remote: true, mutate: func(_ *domain.NoFillLocalFacts, v *fakeVenue, _ *fakeChain, _ *time.Time) {
			v.read.NotFound = false
		}},
		{name: "trades unavailable", want: DecisionInconclusive, remote: true, mutate: func(_ *domain.NoFillLocalFacts, v *fakeVenue, _ *fakeChain, _ *time.Time) {
			v.tradesErr = errors.New("CLOB 503")
		}},
		{name: "failed trade names the order", want: DecisionFillObserved, remote: true, mutate: func(_ *domain.NoFillLocalFacts, v *fakeVenue, _ *fakeChain, _ *time.Time) {
			v.trades.References = []domain.NoFillTradeReference{{TradeID: "t-1", Status: "FAILED", Role: "TAKER"}}
		}},
		{name: "open orders timeout", want: DecisionInconclusive, remote: true, mutate: func(_ *domain.NoFillLocalFacts, v *fakeVenue, _ *fakeChain, _ *time.Time) {
			v.openErr = context.DeadlineExceeded
		}},
		{name: "still open", want: DecisionNotEligible, remote: true, mutate: func(_ *domain.NoFillLocalFacts, v *fakeVenue, _ *fakeChain, _ *time.Time) {
			v.open.Contains = true
		}},
		{name: "rpc failure", want: DecisionInconclusive, remote: true, mutate: func(_ *domain.NoFillLocalFacts, _ *fakeVenue, c *fakeChain, _ *time.Time) {
			c.err = errors.New("RPC error -32000: unsupported block number")
		}},
		{name: "chain window not reached", want: DecisionNotYet, remote: true, mutate: func(_ *domain.NoFillLocalFacts, _ *fakeVenue, c *fakeChain, _ *time.Time) {
			c.err = fmt.Errorf("%w: lagging", domain.ErrNoFillChainWindowNotReached)
		}},
		{name: "OrderFilled on chain", want: DecisionFillObserved, remote: true, mutate: func(_ *domain.NoFillLocalFacts, _ *fakeVenue, c *fakeChain, _ *time.Time) {
			c.evidence.Matches = []domain.NoFillChainMatch{{TransactionHash: "0x1", BlockNumber: 900}}
		}},
		{name: "missing control", want: DecisionInconclusive, remote: true, mutate: func(_ *domain.NoFillLocalFacts, _ *fakeVenue, c *fakeChain, _ *time.Time) {
			c.evidence.Controls = c.evidence.Controls[:2]
		}},
		{name: "unstable range", want: DecisionInconclusive, remote: true, mutate: func(_ *domain.NoFillLocalFacts, _ *fakeVenue, c *fakeChain, _ *time.Time) {
			c.evidence.ToBlockHashAfter = "0xbb"
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			facts := eligibleFacts()
			venue, chain := cleanVenue(), cleanChain()
			now := testReadyAt
			testCase.mutate(&facts, venue, chain, &now)
			store := &fakeStore{facts: facts}
			resolver := newTestResolver(t, store, venue, chain, ModeOn, Policy{}, now)
			order, released, err := resolver.ResolveUnaccepted(context.Background(), facts.Order)
			if err != nil || released || order.Status != facts.Order.Status || len(store.applied) != 0 || len(store.shadows) != 0 {
				t.Fatalf("ResolveUnaccepted() = %s, released=%v, err=%v, applied=%d", order.Status, released, err, len(store.applied))
			}
			evaluation, err := resolver.Evaluate(context.Background(), facts.Order.ID, EvaluateOptions{Mode: domain.NoFillModeAuto})
			if err != nil || evaluation.Decision.Kind != testCase.want || evaluation.Evidence != nil {
				t.Fatalf("decision = %#v, %v; want %s without evidence", evaluation.Decision, err, testCase.want)
			}
			if !testCase.remote && (venue.calls != 0 || chain.calls != 0) {
				t.Fatalf("local rejection consulted remote sources: venue=%d chain=%d", venue.calls, chain.calls)
			}
		})
	}
}

func TestResolveUnacceptedShadowRecordsOnceAndChangesNothing(t *testing.T) {
	store := &fakeStore{facts: eligibleFacts()}
	venue, chain := cleanVenue(), cleanChain()
	resolver := newTestResolver(t, store, venue, chain, ModeShadow, Policy{}, testReadyAt)
	order, released, err := resolver.ResolveUnaccepted(context.Background(), store.facts.Order)
	if err != nil || released || order.Status != domain.OrderStatusUnknown || len(store.applied) != 0 || len(store.shadows) != 1 {
		t.Fatalf("shadow = %s released=%v err=%v applied=%d shadows=%d", order.Status, released, err, len(store.applied), len(store.shadows))
	}
	shadow := store.shadows[0]
	if shadow.Mode != domain.NoFillModeShadow || shadow.ReasonCode != domain.NoFillReleaseAutoReasonCode || shadow.ExpectedOrderRevision != 7 {
		t.Fatalf("shadow resolution = %#v", shadow)
	}
	store.hasShadow = true
	venueCalls := venue.calls
	if _, released, _ := resolver.ResolveUnaccepted(context.Background(), store.facts.Order); released || venue.calls != venueCalls || len(store.shadows) != 1 {
		t.Fatal("a recorded shadow order must not be evaluated again")
	}
}

func TestResolveUnacceptedOnReleasesThroughStore(t *testing.T) {
	store := &fakeStore{facts: eligibleFacts()}
	resolver := newTestResolver(t, store, cleanVenue(), cleanChain(), ModeOn, Policy{}, testReadyAt)
	order, released, err := resolver.ResolveUnaccepted(context.Background(), store.facts.Order)
	if err != nil || !released || order.Status != domain.OrderStatusCancelled || len(store.applied) != 1 {
		t.Fatalf("on = %s released=%v err=%v", order.Status, released, err)
	}
	resolution := store.applied[0]
	if resolution.Mode != domain.NoFillModeAuto || resolution.ReasonCode != domain.NoFillReleaseAutoReasonCode ||
		resolution.ExpectedOrderRevision != 7 || resolution.ExpectedReservationRevision != 3 || resolution.RequireLeaseFree ||
		resolution.AllowReadOnlyProgress || resolution.ExpectedReleaseBalance != "1.064" ||
		!strings.Contains(resolution.Summary, "digest="+resolution.EvidenceSHA256) {
		t.Fatalf("resolution = %#v", resolution)
	}
	store.applyErr = errors.New("revision changed")
	store.applied = nil
	if _, released, err := resolver.ResolveUnaccepted(context.Background(), store.facts.Order); released || err == nil {
		t.Fatal("a failed release transaction must report not released")
	}
}

func TestResolveUnacceptedOffDoesNothing(t *testing.T) {
	store := &fakeStore{facts: eligibleFacts()}
	venue := cleanVenue()
	resolver := newTestResolver(t, store, venue, cleanChain(), ModeOff, Policy{}, testReadyAt)
	if _, released, err := resolver.ResolveUnaccepted(context.Background(), store.facts.Order); released || err != nil || venue.calls != 0 {
		t.Fatal("mode off must not evaluate")
	}
}

func TestLegacyNoExpiryOnlyWithOperatorPolicy(t *testing.T) {
	facts := eligibleFacts()
	facts.Attempts[0].VenueExpiresAt = nil
	policy := Policy{AllowLegacyNoExpiry: true, LegacyMinAge: time.Hour, RequireLeaseFree: true}
	store := &fakeStore{facts: facts}
	chain := cleanChain()
	early := newTestResolver(t, store, cleanVenue(), chain, ModeOff, policy, testSubmitAt.Add(59*time.Minute))
	if evaluation, _ := early.Evaluate(context.Background(), facts.Order.ID, EvaluateOptions{Mode: domain.NoFillModeOperator, Actor: "op", Reason: "r"}); evaluation.Decision.Kind != DecisionNotYet {
		t.Fatalf("legacy before min age = %#v", evaluation.Decision)
	}
	late := newTestResolver(t, store, cleanVenue(), chain, ModeOff, policy, testSubmitAt.Add(62*time.Hour))
	evaluation, err := late.Evaluate(context.Background(), facts.Order.ID, EvaluateOptions{Mode: domain.NoFillModeOperator, Actor: "op", Reason: "r"})
	if err != nil || evaluation.Decision.Kind != DecisionResolve || evaluation.Evidence.TimeGate.Rule != "LEGACY_NO_EXPIRY_MIN_AGE" {
		t.Fatalf("legacy evaluation = %#v, %v", evaluation.Decision, err)
	}
	if _, err := late.Release(context.Background(), evaluation); err != nil {
		t.Fatal(err)
	}
	resolution := store.applied[0]
	if resolution.Mode != domain.NoFillModeOperator || resolution.ReasonCode != domain.NoFillReleaseOperatorReasonCode ||
		!resolution.RequireLeaseFree || !resolution.AllowReadOnlyProgress || resolution.Reason != "r" || resolution.ExpectedReleaseBalance != "1.064" {
		t.Fatalf("operator resolution = %#v", resolution)
	}
	store.facts.LeaseHeld = true
	if evaluation, _ := late.Evaluate(context.Background(), facts.Order.ID, EvaluateOptions{Mode: domain.NoFillModeOperator}); evaluation.Decision.Kind != DecisionNotYet {
		t.Fatalf("held lease = %#v; want NOT_YET", evaluation.Decision)
	}
}

func TestReleaseRefusesIncompleteEvaluation(t *testing.T) {
	store := &fakeStore{facts: eligibleFacts()}
	resolver := newTestResolver(t, store, cleanVenue(), cleanChain(), ModeOff, Policy{}, testReadyAt)
	if _, err := resolver.Release(context.Background(), Evaluation{Decision: Decision{Kind: DecisionInconclusive}}); err == nil {
		t.Fatal("Release() accepted an incomplete evaluation")
	}
	evaluation, _ := resolver.Evaluate(context.Background(), "x", EvaluateOptions{Mode: domain.NoFillModeShadow})
	if _, err := resolver.Release(context.Background(), evaluation); err == nil {
		t.Fatal("Release() accepted a shadow evaluation")
	}
}

func TestConfirmIDTracksWhatWouldChange(t *testing.T) {
	facts := eligibleFacts()
	base := ConfirmID(facts)
	if base != ConfirmID(eligibleFacts()) || len(base) != 64 {
		t.Fatal("confirm id is not stable")
	}
	facts.Order.Revision += 2
	facts.Reservation.Revision++
	if ConfirmID(facts) != base {
		t.Fatal("confirm id must survive further failed reads between dry run and apply")
	}
	facts = eligibleFacts()
	facts.Reservation.RemainingReservedBalance = "2"
	if ConfirmID(facts) == base {
		t.Fatal("confirm id ignores the released amount")
	}
}

func TestPolicyBounds(t *testing.T) {
	for _, policy := range []Policy{{Grace: 10 * time.Minute}, {Grace: 25 * time.Hour}, {LegacyMinAge: time.Minute}, {MaxChunks: -1}} {
		if _, err := policy.Normalize(); err == nil {
			t.Fatalf("Normalize(%#v) accepted an invalid policy", policy)
		}
	}
	if _, err := ParseMode("maybe"); err == nil {
		t.Fatal("ParseMode accepted an unknown mode")
	}
}

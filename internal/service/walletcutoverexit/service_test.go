package walletcutoverexit

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestParseManifestRejectsUnsafeAssetActions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	manifest.Actions[0].ExecutionAccountID = "wallet-6"
	data := mustJSON(t, manifest)
	if _, _, err := ParseManifest(data, now); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("ParseManifest error = %v, want wallet allowlist rejection", err)
	}
	manifest = testManifest(now)
	manifest.Actions[0].OrderType = domain.OrderTypeMarket
	if _, _, err := ParseManifest(mustJSON(t, manifest), now); err == nil || !strings.Contains(err.Error(), "LIMIT") {
		t.Fatalf("ParseManifest error = %v, want LIMIT rejection", err)
	}
	manifest = testRedemptionManifest(now)
	manifest.Redemptions[0].TokenIDs = []string{"1", "1"}
	manifest.Redemptions[0].Shares = []domain.Decimal{"1", "1"}
	manifest.Redemptions[0].OutcomeIndexes = []int{0, 1}
	if _, _, err := ParseManifest(mustJSON(t, manifest), now); err == nil || !strings.Contains(err.Error(), "duplicates token") {
		t.Fatalf("ParseManifest duplicate redemption token error = %v", err)
	}
}

func TestParseManifestRejectsDuplicateAndUnknownJSONFields(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	data := mustJSON(t, manifest)
	data = append(data[:len(data)-1], []byte(`,"manifest_id":"second"}`)...)
	if _, _, err := ParseManifest(data, now); err == nil || !strings.Contains(err.Error(), "duplicate JSON") {
		t.Fatalf("duplicate manifest error = %v", err)
	}
	data = append(mustJSON(t, manifest)[:len(mustJSON(t, manifest))-1], []byte(`,"unexpected":true}`)...)
	if _, _, err := ParseManifest(data, now); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown manifest error = %v", err)
	}
}

func TestAuditDoesNotNeedVenueOrJournalMutation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	data := mustJSON(t, manifest)
	parsed, digest, err := ParseManifest(data, now)
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryStore{}
	service, err := New(Params{Journal: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(context.Background(), parsed, digest, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != "dry-run" || len(result.Actions) != 1 || result.Actions[0].State != "WOULD_SELL" {
		t.Fatalf("audit result = %#v", result)
	}
	if store.saves != 0 {
		t.Fatalf("dry-run saved journal %d times", store.saves)
	}
}

func TestExecutePersistsPreparedBeforePlaceAndIsIdempotent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	digest := ManifestHash(mustJSON(t, manifest))
	store := &memoryStore{}
	venue := &fakePreparedVenue{store: store, result: port.VenueOrder{ID: "venue-1", State: port.VenueOrderFilled, FilledSize: "1"}}
	service, err := New(Params{Venue: venue, Preflight: fakePreflight{}, Journal: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "cutover"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Actions[0].State != JournalFilled || venue.prepareCalls != 1 || venue.placeCalls != 1 {
		t.Fatalf("result=%#v prepare=%d place=%d", result, venue.prepareCalls, venue.placeCalls)
	}
	if !venue.sawPreparedJournal {
		t.Fatal("PlacePrepared ran before PREPARED journal was durable")
	}
	result, err = service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "cutover"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Actions[0].State != JournalFilled || venue.placeCalls != 1 {
		t.Fatalf("idempotent rerun result=%#v place=%d", result, venue.placeCalls)
	}
}

func TestLocalGuardBlocksSigningAndPosting(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	digest := ManifestHash(mustJSON(t, manifest))
	venue := &fakePreparedVenue{}
	store := &memoryStore{}
	service, err := New(Params{Venue: venue, Preflight: fakePreflight{}, Guard: rejectingGuard{}, Journal: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "cutover"}); err == nil || venue.prepareCalls != 0 || venue.placeCalls != 0 {
		t.Fatalf("guard result err=%v prepare=%d place=%d", err, venue.prepareCalls, venue.placeCalls)
	}
}

func TestUnknownResultOnlyRecoversByGet(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	digest := ManifestHash(mustJSON(t, manifest))
	store := &memoryStore{}
	venue := &fakePreparedVenue{store: store, placeErr: errors.New("timeout")}
	service, err := New(Params{Venue: venue, Preflight: fakePreflight{}, Journal: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "cutover"}); err == nil {
		t.Fatal("unknown placement error = nil")
	}
	if venue.placeCalls != 1 {
		t.Fatalf("place calls = %d, want one", venue.placeCalls)
	}
	venue.getResult = port.VenueOrder{ID: "venue-1", State: port.VenueOrderFilled, FilledSize: "1"}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "recover"}); err != nil {
		t.Fatal(err)
	}
	if venue.placeCalls != 1 || venue.getCalls != 1 {
		t.Fatalf("recovery place=%d get=%d", venue.placeCalls, venue.getCalls)
	}
}

func TestPartialResultRequiresNewManifestOnRecovery(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	digest := ManifestHash(mustJSON(t, manifest))
	store := &memoryStore{}
	venue := &fakePreparedVenue{store: store, result: port.VenueOrder{
		ID: "venue-1", State: port.VenueOrderPartiallyFilled, FilledSize: "0.50",
	}}
	service, err := New(Params{Venue: venue, Preflight: fakePreflight{}, Journal: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "cutover"}); !errors.Is(err, ErrResidualShares) {
		t.Fatalf("initial partial result error = %v, want residual error", err)
	}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "recovery"}); !errors.Is(err, ErrResidualShares) {
		t.Fatalf("partial recovery error = %v, want residual error", err)
	}
	if venue.placeCalls != 1 || venue.getCalls != 0 {
		t.Fatalf("partial recovery place=%d get=%d, want no duplicate placement or lookup", venue.placeCalls, venue.getCalls)
	}
}

func TestExpiredAssetActionDoesNotPrepareOrPlace(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(now)
	manifest.Actions[0].ExpiresAt = now.Add(-time.Second)
	digest := ManifestHash(mustJSON(t, manifest))
	store := &memoryStore{}
	venue := &fakePreparedVenue{store: store, result: port.VenueOrder{ID: "venue-1", State: port.VenueOrderFilled, FilledSize: "1"}}
	clock := now
	service, err := New(Params{Venue: venue, Preflight: fakePreflight{}, Journal: store, Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	// An expired manifest cannot create a new execution action; recovery is the
	// only path that may load expired action bytes.
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "expired"}); err == nil {
		t.Fatal("expired action execution error = nil")
	}
	if venue.prepareCalls != 0 || venue.placeCalls != 0 {
		t.Fatalf("expired action prepare=%d place=%d, want no venue mutation", venue.prepareCalls, venue.placeCalls)
	}
}

func TestExpiredManifestCanRecoverExistingJournalButCannotSubmitNewAction(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Second)
	manifest := testManifest(base)
	digest := ManifestHash(mustJSON(t, manifest))
	store := &memoryStore{}
	venue := &fakePreparedVenue{store: store, placeErr: errors.New("timeout")}
	clock := base
	service, err := New(Params{Venue: venue, Preflight: fakePreflight{}, Journal: store, Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "cutover"}); err == nil {
		t.Fatal("initial unknown placement error = nil")
	}
	clock = base.Add(2 * time.Hour)
	venue.placeErr = nil
	venue.getResult = port.VenueOrder{ID: "venue-1", State: port.VenueOrderFilled, FilledSize: "1"}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "recovery"}); err != nil {
		t.Fatal(err)
	}
	if venue.placeCalls != 1 || venue.getCalls != 1 {
		t.Fatalf("expired recovery place=%d get=%d", venue.placeCalls, venue.getCalls)
	}
}

func TestFileJournalIsAtomicAndPrivate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.json")
	store, err := NewFileJournalStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(Journal{Version: 1, ManifestSHA256: strings.Repeat("a", 64), Entries: []JournalEntry{{ActionID: "a", Kind: assetActionKind, ManifestSHA256: strings.Repeat("a", 64), State: JournalPrepared}}}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %o, want 600", info.Mode().Perm())
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 1 {
		t.Fatalf("loaded journal = %#v err=%v", loaded, err)
	}
}

func TestFileJournalLockSerializesIndependentStores(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.json")
	first, err := NewFileJournalStore(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFileJournalStore(path)
	if err != nil {
		t.Fatal(err)
	}
	release, err := first.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Acquire(); !errors.Is(err, ErrJournalLocked) {
		t.Fatalf("second Acquire error = %v, want ErrJournalLocked", err)
	}
	release()
	releaseSecond, err := second.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond()
}

func TestRedemptionZeroPayoutRequiresReceiptAndRecoversWithoutResubmit(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testRedemptionManifest(now)
	digest := ManifestHash(mustJSON(t, manifest))
	store := &memoryStore{}
	venue := &fakeRedemptionVenue{}
	check := &fakeRedemptionCheck{}
	service, err := New(Params{Redemption: venue, RedemptionCheck: check, Journal: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "settled zero payout"}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("initial redemption error = %v, want recovery required", err)
	}
	if venue.submitCalls != 1 || check.verifyCalls != 0 {
		t.Fatalf("initial submit=%d verify=%d", venue.submitCalls, check.verifyCalls)
	}
	if _, err := service.Run(context.Background(), manifest, digest, RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "receipt recovery"}); err != nil {
		t.Fatal(err)
	}
	if venue.submitCalls != 1 || venue.resolveCalls != 1 || check.verifyCalls != 1 {
		t.Fatalf("recovery submit=%d resolve=%d verify=%d", venue.submitCalls, venue.resolveCalls, check.verifyCalls)
	}
}

func TestRedemptionSubmissionWithoutIdentityCannotBeCompletedOrResubmitted(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := testRedemptionManifest(now)
	digest := ManifestHash(mustJSON(t, manifest))
	store := &memoryStore{}
	venue := &fakeRedemptionVenue{missingIdentity: true}
	check := &fakeRedemptionCheck{}
	service, err := New(Params{Redemption: venue, RedemptionCheck: check, Journal: store, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	options := RunOptions{Execute: true, ConfirmSHA256: digest, Actor: "operator", Reason: "identity check"}
	if _, err := service.Run(context.Background(), manifest, digest, options); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("initial error = %v, want recovery required", err)
	}
	if _, err := service.Run(context.Background(), manifest, digest, options); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("recovery error = %v, want recovery required", err)
	}
	if venue.submitCalls != 1 {
		t.Fatalf("submit calls = %d, want one", venue.submitCalls)
	}
}

type memoryStore struct {
	value walletJournalValue
	saves int
}

type walletJournalValue = Journal

func (store *memoryStore) Load() (Journal, error) {
	if store.value.Version == 0 {
		store.value.Version = 1
	}
	return store.value, nil
}
func (store *memoryStore) Save(value Journal) error {
	store.value = value
	store.saves++
	return nil
}

type fakePreflight struct{}

func (fakePreflight) Check(context.Context, AssetAction) (Preflight, error) {
	return Preflight{WalletAddress: "0x1111111111111111111111111111111111111111", ActualShares: "1", TickSize: "0.01", NegRisk: false, Approved: true, Eligible: true, BestBid: "0.50", ObservedAt: time.Now().UTC()}, nil
}

type rejectingGuard struct{}

func (rejectingGuard) Check(context.Context, string) error {
	return errors.New("account is not paused")
}

type fakePlacement struct{ id string }

func (placement fakePlacement) ExpectedVenueOrderID() string { return placement.id }

type fakePreparedVenue struct {
	store              *memoryStore
	result             port.VenueOrder
	getResult          port.VenueOrder
	placeErr           error
	prepareCalls       int
	placeCalls         int
	getCalls           int
	sawPreparedJournal bool
}

func (*fakePreparedVenue) Name() string { return "fake" }
func (venue *fakePreparedVenue) PreparePlace(context.Context, domain.Order) (port.PreparedPlacement, error) {
	venue.prepareCalls++
	return fakePlacement{id: "venue-1"}, nil
}
func (venue *fakePreparedVenue) PlacePrepared(_ context.Context, _ domain.Order, _ port.PreparedPlacement) (port.VenueOrder, error) {
	venue.placeCalls++
	if venue.store != nil && len(venue.store.value.Entries) == 1 && venue.store.value.Entries[0].State == JournalPrepared {
		venue.sawPreparedJournal = true
	}
	if venue.placeErr != nil {
		return port.VenueOrder{}, venue.placeErr
	}
	return venue.result, nil
}
func (venue *fakePreparedVenue) Place(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, errors.New("direct Place must not be used")
}
func (*fakePreparedVenue) Cancel(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, errors.New("cancel not used")
}
func (venue *fakePreparedVenue) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	venue.getCalls++
	if venue.getResult.State == "" {
		return port.VenueOrder{}, errors.New("not found")
	}
	return venue.getResult, nil
}

type fakeRedemptionCheck struct{ verifyCalls int }

func (*fakeRedemptionCheck) Check(context.Context, RedemptionAction) (RedemptionPreflight, error) {
	return RedemptionPreflight{Settled: true, Approved: true, BalanceVerified: true, PayoutBaseUnits: "0"}, nil
}
func (check *fakeRedemptionCheck) VerifyReceipt(context.Context, RedemptionAction, domain.RedemptionSubmission) error {
	check.verifyCalls++
	return nil
}

type fakeRedemptionVenue struct {
	submitCalls     int
	resolveCalls    int
	missingIdentity bool
}

func (*fakeRedemptionVenue) RedemptionApproved(context.Context, string, bool) (bool, error) {
	return true, nil
}
func (*fakeRedemptionVenue) SubmitRedemptionApproval(context.Context, string, bool) (domain.RedemptionSubmission, error) {
	return domain.RedemptionSubmission{}, errors.New("approval must never be automatic")
}
func (venue *fakeRedemptionVenue) SubmitRedemption(context.Context, string, string, bool) (domain.RedemptionSubmission, error) {
	venue.submitCalls++
	hash := "0x" + strings.Repeat("c", 64)
	if venue.missingIdentity {
		return domain.RedemptionSubmission{State: domain.RedemptionSubmissionPending}, nil
	}
	return domain.RedemptionSubmission{Provider: "POLYGON_RPC", Reference: hash, TransactionHash: hash, State: domain.RedemptionSubmissionPending}, nil
}
func (venue *fakeRedemptionVenue) ResolveRedemptionSubmission(context.Context, string, domain.RedemptionSubmission) (domain.RedemptionSubmission, error) {
	venue.resolveCalls++
	hash := "0x" + strings.Repeat("c", 64)
	return domain.RedemptionSubmission{Provider: "POLYGON_RPC", Reference: hash, TransactionHash: hash, State: domain.RedemptionSubmissionConfirmed}, nil
}

func testManifest(now time.Time) Manifest {
	return Manifest{SchemaVersion: SchemaVersion, ManifestID: "cutover-1", Observed: Evidence{Block: 100, BlockHash: "0x" + strings.Repeat("a", 64), Observed: now}, ExpiresAt: now.Add(time.Hour), Actions: []AssetAction{{ActionID: "asset-1", ExecutionAccountID: "main", WalletAddress: "0x1111111111111111111111111111111111111111", MarketID: "market-1", ConditionID: "0x" + strings.Repeat("c", 64), OutcomeIndex: 0, OutcomeName: "Yes", TokenID: "1", NegRisk: false, Shares: "1", WorstPrice: "0.50", TickSize: "0.01", Side: domain.SideSell, OrderType: domain.OrderTypeLimit, TimeInForce: domain.TimeInForceFOK, ExpiresAt: now.Add(30 * time.Minute), LotID: "lot-1", Ownership: "managed"}}}
}

func testRedemptionManifest(now time.Time) Manifest {
	return Manifest{SchemaVersion: SchemaVersion, ManifestID: "redeem-1", Observed: Evidence{Block: 100, BlockHash: "0x" + strings.Repeat("a", 64), Observed: now}, ExpiresAt: now.Add(time.Hour), Redemptions: []RedemptionAction{{ActionID: "redeem-1", ExecutionAccountID: "main", WalletAddress: "0x1111111111111111111111111111111111111111", ConditionID: "0x" + strings.Repeat("b", 64), NegRisk: false, TokenIDs: []string{"1"}, Shares: []domain.Decimal{"1"}, OutcomeIndexes: []int{0}, PayoutBaseUnits: "0"}}}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

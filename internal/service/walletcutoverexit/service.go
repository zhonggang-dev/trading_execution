package walletcutoverexit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	assetActionKind      = "SELL"
	redemptionActionKind = "REDEEM"
	maxJournalErrorLen   = 512
)

var (
	ErrConfirmationRequired = errors.New("wallet exit execution requires exact manifest SHA256, actor, and reason")
	ErrRecoveryRequired     = errors.New("wallet exit action has an unknown or prepared venue result; reconcile by venue identity before retrying")
	ErrResidualShares       = errors.New("wallet exit left residual shares; create a separately reviewed manifest for the remainder")
	ErrRedemptionVerifier   = errors.New("redemption execution requires a settlement and payout verifier")
)

// Preflight is the read-only evidence captured immediately before signing.
// The source must read the actual CTF balance, exchange approval, open orders,
// eligibility, tick size, neg-risk bit and recent bid at one observation.
type Preflight struct {
	WalletAddress     string
	ActualShares      domain.Decimal
	TickSize          domain.Decimal
	NegRisk           bool
	Approved          bool
	Eligible          bool
	OpenOrderCount    int
	BestBid           domain.Decimal
	ObservedAt        time.Time
	ObservedBlock     uint64
	ObservedBlockHash string
	Nonce             string
}

type PreflightSource interface {
	Check(ctx context.Context, action AssetAction) (Preflight, error)
}

// RedemptionPreflight is the evidence required before invoking an already
// approved redemption adapter. PayoutBaseUnits may be "0" and is not inferred
// from a current-value endpoint.
type RedemptionPreflight struct {
	WalletAddress     string
	Settled           bool
	PayoutBaseUnits   string
	Approved          bool
	BalanceVerified   bool
	ObservedBlock     uint64
	ObservedBlockHash string
	ObservedAt        time.Time
}

type RedemptionSource interface {
	Check(ctx context.Context, action RedemptionAction) (RedemptionPreflight, error)
}

// RedemptionReceiptVerifier is an optional but required-for-completion hook
// for the execute path. It must verify a finalized receipt, condition payout
// and wallet/condition identity; a relayer "confirmed" state alone is not a
// redemption receipt and cannot close an action.
type RedemptionReceiptVerifier interface {
	VerifyReceipt(ctx context.Context, action RedemptionAction, submission domain.RedemptionSubmission) error
}

// Params contains only operator-scoped dependencies. In particular there is
// no OrderExecutor, reservation manager, fill ledger or account-wide gate:
// this path cannot masquerade as a strategy order.
type Params struct {
	Venue           port.PreparedVenue
	Preflight       PreflightSource
	Guard           ExecutionGuard
	Redemption      port.RedemptionVenue
	RedemptionCheck RedemptionSource
	Journal         JournalStore
	Now             func() time.Time
}

// ExecutionGuard is a read-only local safety boundary. Production adapters
// must prove selected accounts are paused and have no in-flight work before
// each new signed/posted action; it has no pause or mutation method.
type ExecutionGuard interface {
	Check(ctx context.Context, executionAccountID string) error
}

type Service struct {
	venue           port.PreparedVenue
	preflight       PreflightSource
	guard           ExecutionGuard
	redemption      port.RedemptionVenue
	redemptionCheck RedemptionSource
	journal         JournalStore
	now             func() time.Time
}

type RunOptions struct {
	Execute       bool
	ConfirmSHA256 string
	Actor         string
	Reason        string
}

type RunResult struct {
	Mode           string         `json:"mode"`
	ManifestID     string         `json:"manifest_id"`
	ManifestSHA256 string         `json:"manifest_sha256"`
	Actions        []ActionResult `json:"actions"`
}

type ActionResult struct {
	ActionID           string         `json:"action_id"`
	Kind               string         `json:"kind"`
	ExecutionAccountID string         `json:"execution_account_id"`
	ClientOrderID      string         `json:"client_order_id,omitempty"`
	State              string         `json:"state"`
	VenueOrderID       string         `json:"venue_order_id,omitempty"`
	FilledSize         domain.Decimal `json:"filled_size,omitempty"`
	RemainingSize      domain.Decimal `json:"remaining_size,omitempty"`
	Error              string         `json:"error,omitempty"`
}

func New(params Params) (*Service, error) {
	if params.Journal == nil {
		return nil, fmt.Errorf("wallet exit journal is required")
	}
	if params.Now == nil {
		params.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		venue: params.Venue, preflight: params.Preflight, guard: params.Guard, redemption: params.Redemption,
		redemptionCheck: params.RedemptionCheck, journal: params.Journal, now: params.Now,
	}, nil
}

// Audit performs manifest validation only. It intentionally does not create a
// journal, perform network reads, sign, place, approve or broadcast anything.
func (service *Service) Audit(_ context.Context, manifest Manifest, manifestSHA string) (RunResult, error) {
	now := service.now().UTC()
	if err := manifest.Validate(now); err != nil {
		return RunResult{}, err
	}
	if !validSHA(manifestSHA) {
		return RunResult{}, fmt.Errorf("manifest SHA256 is required")
	}
	result := RunResult{Mode: "dry-run", ManifestID: manifest.ManifestID, ManifestSHA256: manifestSHA,
		Actions: make([]ActionResult, 0, len(manifest.Actions)+len(manifest.Redemptions))}
	for _, action := range manifest.Actions {
		result.Actions = append(result.Actions, ActionResult{ActionID: action.ActionID, Kind: assetActionKind,
			ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: stableClientOrderID(manifestSHA, action.ActionID),
			State: "WOULD_SELL"})
	}
	for _, action := range manifest.Redemptions {
		result.Actions = append(result.Actions, ActionResult{ActionID: action.ActionID, Kind: redemptionActionKind,
			ExecutionAccountID: action.ExecutionAccountID, State: "WOULD_REDEEM"})
	}
	return result, nil
}

// Run executes only the explicit, reviewed manifest. The hash is the exact
// file hash calculated by ParseManifest; re-marshalling is never accepted as
// an approval substitute.
func (service *Service) Run(ctx context.Context, manifest Manifest, manifestSHA string, options RunOptions) (RunResult, error) {
	now := service.now().UTC()
	if !options.Execute {
		if err := manifest.Validate(now); err != nil {
			return RunResult{}, err
		}
	}
	if !validSHA(manifestSHA) {
		return RunResult{}, fmt.Errorf("manifest SHA256 is required")
	}
	if !options.Execute {
		return service.Audit(ctx, manifest, manifestSHA)
	}
	if locker, ok := service.journal.(JournalLocker); ok {
		release, lockErr := locker.Acquire()
		if lockErr != nil {
			return RunResult{}, lockErr
		}
		defer release()
	}
	if !strings.EqualFold(strings.TrimSpace(options.ConfirmSHA256), manifestSHA) || strings.TrimSpace(options.Actor) == "" || strings.TrimSpace(options.Reason) == "" {
		return RunResult{}, ErrConfirmationRequired
	}
	if service.venue == nil && len(manifest.Actions) > 0 {
		return RunResult{}, fmt.Errorf("SELL execution venue is required")
	}
	if service.preflight == nil && len(manifest.Actions) > 0 {
		return RunResult{}, fmt.Errorf("SELL read-only preflight source is required")
	}
	if len(manifest.Redemptions) > 0 && (service.redemption == nil || service.redemptionCheck == nil) {
		return RunResult{}, ErrRedemptionVerifier
	}
	journal, err := service.journal.Load()
	if err != nil {
		return RunResult{}, err
	}
	if journal.Version == 0 {
		journal.Version = journalVersion
	}
	if journal.ManifestSHA256 != "" && journal.ManifestSHA256 != manifestSHA {
		return RunResult{}, fmt.Errorf("journal belongs to manifest %s, not %s", journal.ManifestSHA256, manifestSHA)
	}
	journal.ManifestSHA256 = manifestSHA
	if journal.Entries == nil {
		journal.Entries = []JournalEntry{}
	}
	if options.Execute {
		allExisting := len(manifest.Actions)+len(manifest.Redemptions) > 0
		known := make(map[string]struct{}, len(journal.Entries))
		for _, entry := range journal.Entries {
			known[entry.ActionID] = struct{}{}
		}
		for _, action := range manifest.Actions {
			if _, exists := known[action.ActionID]; !exists {
				allExisting = false
				break
			}
		}
		if allExisting {
			for _, action := range manifest.Redemptions {
				if _, exists := known[action.ActionID]; !exists {
					allExisting = false
					break
				}
			}
		}
		if allExisting {
			if err := manifest.ValidateForRecovery(now); err != nil {
				return RunResult{}, err
			}
		} else if err := manifest.Validate(now); err != nil {
			return RunResult{}, err
		}
	}
	entryByID := make(map[string]*JournalEntry, len(journal.Entries))
	for i := range journal.Entries {
		entryByID[journal.Entries[i].ActionID] = &journal.Entries[i]
	}
	result := RunResult{Mode: "execute", ManifestID: manifest.ManifestID, ManifestSHA256: manifestSHA,
		Actions: make([]ActionResult, 0, len(manifest.Actions)+len(manifest.Redemptions))}
	var runErrs []error
	stopAfterAsset := false
	for _, action := range manifest.Actions {
		entry, actionResult, actionErr := service.runAssetAction(ctx, manifest, manifestSHA, action, entryByID, &journal, options)
		_ = entry
		result.Actions = append(result.Actions, actionResult)
		if actionErr != nil {
			runErrs = append(runErrs, fmt.Errorf("asset action %s: %w", action.ActionID, actionErr))
			if errors.Is(actionErr, ErrRecoveryRequired) || errors.Is(actionErr, ErrResidualShares) || errors.Is(actionErr, ErrRedemptionVerifier) {
				stopAfterAsset = true
				break
			}
		}
	}
	if stopAfterAsset {
		return result, errors.Join(runErrs...)
	}
	for _, action := range manifest.Redemptions {
		entry, actionResult, actionErr := service.runRedemptionAction(ctx, manifestSHA, action, entryByID, &journal, options)
		_ = entry
		result.Actions = append(result.Actions, actionResult)
		if actionErr != nil {
			runErrs = append(runErrs, fmt.Errorf("redemption action %s: %w", action.ActionID, actionErr))
			if errors.Is(actionErr, ErrRecoveryRequired) || errors.Is(actionErr, ErrRedemptionVerifier) {
				break
			}
		}
	}
	return result, errors.Join(runErrs...)
}

func (service *Service) runAssetAction(ctx context.Context, manifest Manifest, manifestSHA string, action AssetAction, entries map[string]*JournalEntry, journal *Journal, options RunOptions) (*JournalEntry, ActionResult, error) {
	clientID := stableClientOrderID(manifestSHA, action.ActionID)
	if existing := entries[action.ActionID]; existing != nil {
		if existing.ManifestSHA256 != manifestSHA || existing.ActionHash != actionHash(action) || existing.ClientOrderID != clientID {
			return existing, actionResultFromEntry(*existing, action), fmt.Errorf("journal action identity conflict")
		}
		if existing.State == JournalRejected || existing.State == JournalFilled || existing.State == JournalPartial || existing.State == JournalUnknown || existing.State == JournalPrepared || existing.State == JournalSubmitting || existing.State == JournalSubmitted {
			recoveryErr := service.recoverAssetAction(ctx, action, existing, journal)
			return existing, actionResultFromEntry(*existing, action), recoveryErr
		}
	}
	if !action.ExpiresAt.After(service.now().UTC()) {
		return nil, ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: clientID, State: "EXPIRED", Error: "asset action expired before preflight"}, fmt.Errorf("asset action %s expired before preflight", action.ActionID)
	}
	preflight, err := service.preflight.Check(ctx, action)
	if err != nil {
		return nil, ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: clientID, State: "PREFLIGHT_FAILED", Error: safeError(err)}, err
	}
	if err := validatePreflight(action, preflight); err != nil {
		return nil, ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: clientID, State: "PREFLIGHT_FAILED", Error: safeError(err)}, err
	}
	if service.guard != nil {
		if err := service.guard.Check(ctx, action.ExecutionAccountID); err != nil {
			return nil, ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: clientID, State: "LOCAL_GUARD_FAILED", Error: safeError(err)}, err
		}
	}
	order := buildExitOrder(manifest, manifestSHA, action, preflight)
	if err := order.Intent.Validate(); err != nil {
		return nil, ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: clientID, State: "ORDER_INVALID", Error: safeError(err)}, err
	}
	prepared, err := service.venue.PreparePlace(ctx, order)
	if err != nil {
		return nil, ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: clientID, State: "PREPARE_FAILED", Error: safeError(err)}, err
	}
	venueID := strings.TrimSpace(prepared.ExpectedVenueOrderID())
	if venueID == "" {
		return nil, ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: clientID, State: "PREPARE_FAILED", Error: "prepared venue omitted stable order id"}, fmt.Errorf("prepared venue omitted stable order id")
	}
	entry := JournalEntry{ActionID: action.ActionID, Kind: assetActionKind, ManifestSHA256: manifestSHA,
		ActionHash: actionHash(action), ExecutionAccountID: action.ExecutionAccountID, TokenID: action.TokenID,
		Nonce:         action.Nonce,
		ClientOrderID: clientID, ExpectedVenueID: venueID, State: JournalPrepared, Actor: options.Actor,
		Reason: options.Reason, UpdatedAt: service.now().UTC()}
	journal.Entries = append(journal.Entries, entry)
	entries[action.ActionID] = &journal.Entries[len(journal.Entries)-1]
	if err := service.journal.Save(*journal); err != nil {
		return entries[action.ActionID], actionResultFromEntry(entry, action), fmt.Errorf("persist prepared journal: %w", err)
	}
	if service.guard != nil {
		if err := service.guard.Check(ctx, action.ExecutionAccountID); err != nil {
			return entries[action.ActionID], actionResultFromEntry(*entries[action.ActionID], action), err
		}
	}
	if !action.ExpiresAt.After(service.now().UTC()) {
		return entries[action.ActionID], actionResultFromEntry(*entries[action.ActionID], action), fmt.Errorf("asset action %s expired before placement", action.ActionID)
	}
	order.VenueOrderID = venueID
	venueOrder, placeErr := service.venue.PlacePrepared(ctx, order, prepared)
	entryPtr := entries[action.ActionID]
	if placeErr != nil {
		entryPtr.State = JournalUnknown
		var venueErr *port.VenueError
		if errors.As(placeErr, &venueErr) && (venueErr.Kind == port.VenueErrorRejected || venueErr.Kind == port.VenueErrorInvalid) {
			entryPtr.State = JournalRejected
		}
		entryPtr.Error = safeError(placeErr)
		entryPtr.UpdatedAt = service.now().UTC()
		if saveErr := service.journal.Save(*journal); saveErr != nil {
			return entryPtr, actionResultFromEntry(*entryPtr, action), errors.Join(placeErr, fmt.Errorf("persist venue failure: %w", saveErr))
		}
		return entryPtr, actionResultFromEntry(*entryPtr, action), placeErr
	}
	updateEntryFromVenue(entryPtr, venueOrder, action.Shares)
	entryPtr.UpdatedAt = service.now().UTC()
	if err := service.journal.Save(*journal); err != nil {
		return entryPtr, actionResultFromEntry(*entryPtr, action), fmt.Errorf("persist venue result: %w", err)
	}
	if entryPtr.State == JournalPartial {
		return entryPtr, actionResultFromEntry(*entryPtr, action), ErrResidualShares
	}
	if entryPtr.State == JournalUnknown {
		return entryPtr, actionResultFromEntry(*entryPtr, action), ErrRecoveryRequired
	}
	return entryPtr, actionResultFromEntry(*entryPtr, action), nil
}

func (service *Service) recoverAssetAction(ctx context.Context, action AssetAction, entry *JournalEntry, journal *Journal) error {
	if entry.State == JournalRejected || entry.State == JournalFilled {
		return nil
	}
	if entry.State == JournalPartial {
		// A partial fill is a real residual. Do not silently report a rerun as
		// successful; the operator must review the remaining quantity and issue
		// a new manifest with its own price/expiry boundary.
		return ErrResidualShares
	}
	if entry.ExpectedVenueID == "" {
		return ErrRecoveryRequired
	}
	order := domain.Order{ID: entry.ClientOrderID, VenueOrderID: entry.ExpectedVenueID,
		Intent: domain.OrderIntent{ExecutionAccountID: action.ExecutionAccountID, ClientOrderID: entry.ClientOrderID,
			Venue: "polymarket", MarketID: action.MarketID, TokenID: action.TokenID, Side: domain.SideSell,
			Type: domain.OrderTypeLimit, Price: action.WorstPrice, Size: action.Shares,
			TimeInForce: action.TimeInForce, TargetLotID: targetLotID(action), MarketSource: domain.MarketSourcePolymarket},
		MarketValidation: &domain.MarketValidation{TokenID: action.TokenID, TickSize: action.TickSize, NegRisk: action.NegRisk}}
	venueOrder, err := service.venue.Get(ctx, order)
	if err != nil {
		entry.State = JournalUnknown
		entry.Error = safeError(err)
		entry.UpdatedAt = service.now().UTC()
		_ = service.journal.Save(*journal)
		return fmt.Errorf("recover venue order %s: %w", entry.ExpectedVenueID, ErrRecoveryRequired)
	}
	updateEntryFromVenue(entry, venueOrder, action.Shares)
	entry.UpdatedAt = service.now().UTC()
	if saveErr := service.journal.Save(*journal); saveErr != nil {
		return saveErr
	}
	if entry.State == JournalPartial {
		return ErrResidualShares
	}
	return nil
}

func (service *Service) runRedemptionAction(ctx context.Context, manifestSHA string, action RedemptionAction, entries map[string]*JournalEntry, journal *Journal, options RunOptions) (*JournalEntry, ActionResult, error) {
	if existing := entries[action.ActionID]; existing != nil {
		if existing.ManifestSHA256 != manifestSHA || existing.ActionHash != redemptionActionHash(action) {
			return existing, redemptionResultFromEntry(*existing, action), fmt.Errorf("journal redemption identity conflict")
		}
		if existing.State == JournalConfirmed || existing.State == JournalRejected {
			return existing, redemptionResultFromEntry(*existing, action), nil
		}
		if existing.State == JournalPrepared || existing.State == JournalSubmitting {
			return existing, redemptionResultFromEntry(*existing, action), ErrRecoveryRequired
		}
		if existing.ExpectedVenueID != "" || existing.TransactionHash != "" || existing.State == JournalSubmitted || existing.State == JournalUnknown {
			if existing.SubmissionProvider == "" || existing.ExpectedVenueID == "" {
				return existing, redemptionResultFromEntry(*existing, action), ErrRecoveryRequired
			}
			submission, resolveErr := service.redemption.ResolveRedemptionSubmission(ctx, action.ExecutionAccountID, domain.RedemptionSubmission{
				Provider: existing.SubmissionProvider, Reference: existing.ExpectedVenueID, TransactionHash: existing.TransactionHash,
				State: domain.RedemptionSubmissionPending,
			})
			if resolveErr != nil {
				existing.State = JournalUnknown
				existing.Error = safeError(resolveErr)
				existing.UpdatedAt = service.now().UTC()
				_ = service.journal.Save(*journal)
				return existing, redemptionResultFromEntry(*existing, action), ErrRecoveryRequired
			}
			existing.TransactionHash = firstNonEmpty(existing.TransactionHash, submission.TransactionHash)
			if submission.State == domain.RedemptionSubmissionFailed {
				existing.State = JournalRejected
				existing.Error = safeError(fmt.Errorf("redemption submission failed: %s", submission.FailureReason))
			} else if submission.State == domain.RedemptionSubmissionConfirmed {
				verifier, ok := service.redemptionCheck.(RedemptionReceiptVerifier)
				if !ok {
					return existing, redemptionResultFromEntry(*existing, action), ErrRedemptionVerifier
				}
				if verifyErr := verifier.VerifyReceipt(ctx, action, submission); verifyErr != nil {
					return existing, redemptionResultFromEntry(*existing, action), fmt.Errorf("verify redemption receipt: %w", verifyErr)
				}
				existing.State = JournalConfirmed
			} else {
				existing.State = JournalUnknown
			}
			existing.UpdatedAt = service.now().UTC()
			if saveErr := service.journal.Save(*journal); saveErr != nil {
				return existing, redemptionResultFromEntry(*existing, action), saveErr
			}
			if existing.State != JournalConfirmed {
				if existing.State == JournalRejected {
					return existing, redemptionResultFromEntry(*existing, action), fmt.Errorf("redemption submission was rejected")
				}
				return existing, redemptionResultFromEntry(*existing, action), ErrRecoveryRequired
			}
			return existing, redemptionResultFromEntry(*existing, action), nil
		}
	}
	if service.guard != nil {
		if err := service.guard.Check(ctx, action.ExecutionAccountID); err != nil {
			return nil, ActionResult{ActionID: action.ActionID, Kind: redemptionActionKind, ExecutionAccountID: action.ExecutionAccountID, State: "LOCAL_GUARD_FAILED", Error: safeError(err)}, err
		}
	}
	preflight, err := service.redemptionCheck.Check(ctx, action)
	if err != nil {
		return nil, ActionResult{ActionID: action.ActionID, Kind: redemptionActionKind, ExecutionAccountID: action.ExecutionAccountID, State: "PREFLIGHT_FAILED", Error: safeError(err)}, err
	}
	if !preflight.Settled || !preflight.Approved || !preflight.BalanceVerified || !equalBaseUnits(preflight.PayoutBaseUnits, action.PayoutBaseUnits) {
		return nil, ActionResult{ActionID: action.ActionID, Kind: redemptionActionKind, ExecutionAccountID: action.ExecutionAccountID, State: "PREFLIGHT_FAILED", Error: "settlement, payout, wallet balance, and exact adapter approval evidence are required"}, ErrRedemptionVerifier
	}
	entry := JournalEntry{ActionID: action.ActionID, Kind: redemptionActionKind, ManifestSHA256: manifestSHA,
		ActionHash: redemptionActionHash(action), ExecutionAccountID: action.ExecutionAccountID, ConditionID: action.ConditionID,
		State: JournalPrepared, Actor: options.Actor, Reason: options.Reason, UpdatedAt: service.now().UTC()}
	journal.Entries = append(journal.Entries, entry)
	entries[action.ActionID] = &journal.Entries[len(journal.Entries)-1]
	if err := service.journal.Save(*journal); err != nil {
		return entries[action.ActionID], redemptionResultFromEntry(entry, action), err
	}
	if service.guard != nil {
		if err := service.guard.Check(ctx, action.ExecutionAccountID); err != nil {
			return entries[action.ActionID], redemptionResultFromEntry(*entries[action.ActionID], action), err
		}
	}
	submission, submitErr := service.redemption.SubmitRedemption(ctx, action.ExecutionAccountID, action.ConditionID, action.NegRisk)
	entryPtr := entries[action.ActionID]
	if submitErr != nil {
		entryPtr.State = JournalUnknown
		var outcome interface {
			SubmitOutcome() domain.RedemptionSubmitOutcome
		}
		if errors.As(submitErr, &outcome) && outcome.SubmitOutcome() == domain.RedemptionSubmitRejected {
			entryPtr.State = JournalRejected
		}
		entryPtr.Error = safeError(submitErr)
		entryPtr.UpdatedAt = service.now().UTC()
		if saveErr := service.journal.Save(*journal); saveErr != nil {
			return entryPtr, redemptionResultFromEntry(*entryPtr, action), errors.Join(submitErr, fmt.Errorf("persist redemption failure: %w", saveErr))
		}
		return entryPtr, redemptionResultFromEntry(*entryPtr, action), submitErr
	}
	entryPtr.State = JournalSubmitted
	entryPtr.SubmissionProvider = strings.TrimSpace(submission.Provider)
	entryPtr.TransactionHash = strings.TrimSpace(submission.TransactionHash)
	entryPtr.ExpectedVenueID = strings.TrimSpace(submission.Reference)
	if submission.State != domain.RedemptionSubmissionFailed &&
		(entryPtr.SubmissionProvider == "" || entryPtr.ExpectedVenueID == "") {
		entryPtr.State = JournalUnknown
		entryPtr.Error = safeError(ErrRecoveryRequired)
	}
	if entryPtr.State != JournalUnknown {
		if submission.State == domain.RedemptionSubmissionFailed {
			entryPtr.State = JournalRejected
			entryPtr.Error = safeError(fmt.Errorf("redemption submission failed: %s", submission.FailureReason))
		} else if submission.State == domain.RedemptionSubmissionConfirmed {
			verifier, ok := service.redemptionCheck.(RedemptionReceiptVerifier)
			if !ok {
				entryPtr.Error = safeError(ErrRedemptionVerifier)
				entryPtr.UpdatedAt = service.now().UTC()
				_ = service.journal.Save(*journal)
				return entryPtr, redemptionResultFromEntry(*entryPtr, action), ErrRedemptionVerifier
			}
			if verifyErr := verifier.VerifyReceipt(ctx, action, submission); verifyErr != nil {
				entryPtr.Error = safeError(verifyErr)
				entryPtr.UpdatedAt = service.now().UTC()
				_ = service.journal.Save(*journal)
				return entryPtr, redemptionResultFromEntry(*entryPtr, action), fmt.Errorf("verify redemption receipt: %w", verifyErr)
			}
			entryPtr.State = JournalConfirmed
		} else {
			entryPtr.Error = safeError(ErrRecoveryRequired)
		}
	}
	entryPtr.UpdatedAt = service.now().UTC()
	if err := service.journal.Save(*journal); err != nil {
		return entryPtr, redemptionResultFromEntry(*entryPtr, action), err
	}
	if entryPtr.State != JournalConfirmed {
		if entryPtr.State == JournalRejected {
			return entryPtr, redemptionResultFromEntry(*entryPtr, action), fmt.Errorf("redemption submission was rejected")
		}
		return entryPtr, redemptionResultFromEntry(*entryPtr, action), ErrRecoveryRequired
	}
	return entryPtr, redemptionResultFromEntry(*entryPtr, action), nil
}

func validatePreflight(action AssetAction, preflight Preflight) error {
	if !strings.EqualFold(strings.TrimSpace(preflight.WalletAddress), strings.TrimSpace(action.WalletAddress)) {
		return fmt.Errorf("preflight wallet does not match manifest")
	}
	if !preflight.Approved || !preflight.Eligible {
		return fmt.Errorf("wallet is not approved and eligible for CLOB exit")
	}
	if preflight.OpenOrderCount != 0 {
		return fmt.Errorf("wallet has %d existing CLOB open orders; cancel/reconcile them before cutover exit", preflight.OpenOrderCount)
	}
	if !preflight.TickSize.Equal(action.TickSize) || preflight.NegRisk != action.NegRisk {
		return fmt.Errorf("live tick_size or neg_risk differs from manifest")
	}
	if comparison, err := preflight.ActualShares.Compare(action.Shares); err != nil || comparison < 0 {
		return fmt.Errorf("live CTF balance is below requested shares")
	}
	if preflight.BestBid.IsEmpty() {
		return fmt.Errorf("recent orderbook has no best bid")
	}
	if comparison, err := preflight.BestBid.Compare(action.WorstPrice); err != nil || comparison < 0 {
		return fmt.Errorf("worst_price is above the current best bid")
	}
	return nil
}

func buildExitOrder(manifest Manifest, manifestSHA string, action AssetAction, preflight Preflight) domain.Order {
	now := preflight.ObservedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	clientID := stableClientOrderID(manifestSHA, action.ActionID)
	targetLotID := targetLotID(action)
	return domain.Order{ID: clientID, Status: domain.OrderStatusReceived, CreatedAt: now, UpdatedAt: now,
		Intent: domain.OrderIntent{ModelID: "wallet_cutover", StrategyID: "wallet_reuse_exit", ExecutionAccountID: action.ExecutionAccountID,
			SignalID: "wallet-cutover:" + manifest.ManifestID + ":" + action.ActionID, ClientOrderID: clientID, Venue: "polymarket",
			MarketSource: domain.MarketSourcePolymarket, MarketID: action.MarketID, ConditionID: action.ConditionID,
			OutcomeIndex: &action.OutcomeIndex, OutcomeName: action.OutcomeName, TokenID: action.TokenID, TargetLotID: targetLotID,
			ExpectedNegRisk: boolPtr(action.NegRisk), Side: domain.SideSell, Type: domain.OrderTypeLimit, Price: action.WorstPrice,
			WorstPrice: action.WorstPrice, Size: action.Shares, TimeInForce: action.TimeInForce, ExpiresAt: &action.ExpiresAt,
			Metadata: map[string]string{"wallet_cutover_manifest_sha256": manifestSHA, "wallet_cutover_action_id": action.ActionID, "wallet_cutover_ownership": action.Ownership}},
		MarketValidation: &domain.MarketValidation{Mode: "WALLET_CUTOVER_EXIT", ValidatedAt: now, MarketObservedAt: now,
			LatestBookSourceAt: now, LatestBookObservedAt: now, OutcomeIndex: action.OutcomeIndex, OutcomeName: action.OutcomeName,
			TokenID: action.TokenID, NegRisk: action.NegRisk, TickSize: action.TickSize, BestBid: preflight.BestBid,
			WorstPrice: action.WorstPrice, BookStatus: domain.OrderBookStatusOK, ExecutableSize: action.Shares}}
}

func targetLotID(action AssetAction) string {
	if value := strings.TrimSpace(action.LotID); value != "" {
		return value
	}
	return "external-cutover:" + action.ActionID
}

func updateEntryFromVenue(entry *JournalEntry, venueOrder port.VenueOrder, requested domain.Decimal) {
	entry.ExpectedVenueID = firstNonEmpty(entry.ExpectedVenueID, venueOrder.ID)
	entry.VenueState = string(venueOrder.State)
	entry.FilledSize = venueOrder.FilledSize
	entry.RemainingSize = subtractDecimal(requested, venueOrder.FilledSize)
	filledComparison, filledErr := venueOrder.FilledSize.Compare(requested)
	filledPositive, positiveErr := venueOrder.FilledSize.Sign()
	if positiveErr == nil && filledPositive > 0 && filledErr == nil && filledComparison < 0 {
		entry.State = JournalPartial
		return
	}
	if filledErr == nil && filledComparison >= 0 {
		entry.State = JournalFilled
		return
	}
	switch venueOrder.State {
	case port.VenueOrderFilled:
		entry.State = JournalFilled
	case port.VenueOrderPartiallyFilled:
		entry.State = JournalPartial
	case port.VenueOrderRejected:
		entry.State = JournalRejected
	case port.VenueOrderUnknown:
		entry.State = JournalUnknown
	default:
		entry.State = JournalSubmitted
	}
}

func actionResultFromEntry(entry JournalEntry, action AssetAction) ActionResult {
	return ActionResult{ActionID: action.ActionID, Kind: assetActionKind, ExecutionAccountID: action.ExecutionAccountID,
		ClientOrderID: entry.ClientOrderID, State: entry.State, VenueOrderID: entry.ExpectedVenueID,
		FilledSize: entry.FilledSize, RemainingSize: entry.RemainingSize, Error: entry.Error}
}

func redemptionResultFromEntry(entry JournalEntry, action RedemptionAction) ActionResult {
	return ActionResult{ActionID: action.ActionID, Kind: redemptionActionKind, ExecutionAccountID: action.ExecutionAccountID,
		State: entry.State, VenueOrderID: entry.ExpectedVenueID, Error: entry.Error}
}

func stableClientOrderID(manifestSHA, actionID string) string {
	digest := sha256.Sum256([]byte(manifestSHA + "\x00" + actionID))
	return "wallet-cutover-" + hex.EncodeToString(digest[:16])
}

func actionHash(action AssetAction) string {
	data, _ := json.Marshal(action)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func redemptionActionHash(action RedemptionAction) string {
	data, _ := json.Marshal(action)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func subtractDecimal(left, right domain.Decimal) domain.Decimal {
	l, lok := new(big.Rat).SetString(strings.TrimSpace(left.String()))
	r, rok := new(big.Rat).SetString(strings.TrimSpace(right.String()))
	if !lok || !rok {
		return ""
	}
	result := new(big.Rat).Sub(l, r)
	if result.Sign() < 0 {
		return "0"
	}
	return domain.Decimal(result.FloatString(18)).Canonical()
}

func equalBaseUnits(left, right string) bool {
	leftValue, leftOK := new(big.Int).SetString(strings.TrimSpace(left), 10)
	rightValue, rightOK := new(big.Int).SetString(strings.TrimSpace(right), 10)
	return leftOK && rightOK && leftValue.Sign() >= 0 && rightValue.Sign() >= 0 && leftValue.Cmp(rightValue) == 0
}

func boolPtr(value bool) *bool { return &value }

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(err.Error())
	for _, marker := range []string{"?key=", "?secret=", "?passphrase=", "api_key=", "api_secret=", "private_key="} {
		if index := strings.Index(strings.ToLower(text), marker); index >= 0 {
			text = text[:index] + "?redacted"
		}
	}
	if len(text) > maxJournalErrorLen {
		return text[:maxJournalErrorLen]
	}
	return text
}

func validSHA(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

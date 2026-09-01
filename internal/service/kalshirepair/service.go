package kalshirepair

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const defaultFinalityGrace = 30 * time.Second

// LocalState is the exact local order/reservation pair eligible for repair.
// RepairFingerprint is populated only after this tool has already completed
// the same evidence-backed repair.
type LocalState struct {
	Order                    domain.Order
	ReservationStatus        domain.ReservationStatus
	RemainingReservedBalance domain.Decimal
	RemainingReservedShares  domain.Decimal
	RepairFingerprint        string
}

// Evidence is a normalized snapshot built from two independent authenticated
// Kalshi reads: lookup by client_order_id and the authoritative fills endpoint
// scoped to the returned order_id.
type Evidence struct {
	OrderID          string         `json:"order_id"`
	ClientOrderID    string         `json:"client_order_id"`
	MarketID         string         `json:"market_id"`
	OutcomeSide      string         `json:"outcome_side"`
	Action           string         `json:"action"`
	BookSide         string         `json:"book_side"`
	OrderType        string         `json:"order_type"`
	TimeInForce      string         `json:"time_in_force,omitempty"`
	OrderPrice       domain.Decimal `json:"order_price"`
	SelfTradePolicy  string         `json:"self_trade_prevention_type"`
	CancelOnPause    *bool          `json:"cancel_order_on_pause"`
	SubaccountNumber *int           `json:"subaccount_number"`
	Status           string         `json:"status"`
	FillCount        domain.Decimal `json:"fill_count"`
	RemainingCount   domain.Decimal `json:"remaining_count"`
	InitialCount     domain.Decimal `json:"initial_count"`
	FillIDs          []string       `json:"fill_ids"`
	LastUpdatedAt    time.Time      `json:"last_updated_at"`
	ObservedAt       time.Time      `json:"observed_at"`
	OrderQuerySource string         `json:"order_query_source"`
	FillQuerySource  string         `json:"fill_query_source"`
}

// Fingerprint excludes ObservedAt so a repeated authenticated read of the same
// immutable terminal venue evidence remains idempotent.
func (evidence Evidence) Fingerprint() (string, error) {
	payload, err := json.Marshal(struct {
		OrderID          string         `json:"order_id"`
		ClientOrderID    string         `json:"client_order_id"`
		MarketID         string         `json:"market_id"`
		OutcomeSide      string         `json:"outcome_side"`
		BookSide         string         `json:"book_side"`
		OrderType        string         `json:"order_type"`
		OrderPrice       domain.Decimal `json:"order_price"`
		SubaccountNumber *int           `json:"subaccount_number"`
		Status           string         `json:"status"`
		FillCount        domain.Decimal `json:"fill_count"`
		RemainingCount   domain.Decimal `json:"remaining_count"`
		InitialCount     domain.Decimal `json:"initial_count"`
		FillIDs          []string       `json:"fill_ids"`
		LastUpdatedAt    time.Time      `json:"last_updated_at"`
		OrderQuerySource string         `json:"order_query_source"`
		FillQuerySource  string         `json:"fill_query_source"`
	}{
		OrderID: strings.TrimSpace(evidence.OrderID), ClientOrderID: strings.TrimSpace(evidence.ClientOrderID),
		MarketID: strings.TrimSpace(evidence.MarketID), Status: strings.ToLower(strings.TrimSpace(evidence.Status)),
		OutcomeSide: strings.ToLower(strings.TrimSpace(evidence.OutcomeSide)),
		BookSide:    strings.ToLower(strings.TrimSpace(evidence.BookSide)), OrderType: strings.ToLower(strings.TrimSpace(evidence.OrderType)),
		OrderPrice:       evidence.OrderPrice,
		SubaccountNumber: evidence.SubaccountNumber,
		FillCount:        evidence.FillCount, RemainingCount: evidence.RemainingCount, InitialCount: evidence.InitialCount,
		FillIDs: append([]string(nil), evidence.FillIDs...), LastUpdatedAt: evidence.LastUpdatedAt.UTC(),
		OrderQuerySource: strings.TrimSpace(evidence.OrderQuerySource), FillQuerySource: strings.TrimSpace(evidence.FillQuerySource),
	})
	if err != nil {
		return "", fmt.Errorf("encode Kalshi repair evidence: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// EvidenceSource performs authenticated, read-only venue queries.
type EvidenceSource interface {
	Inspect(context.Context, domain.Order) (Evidence, error)
}

// Store owns the local authority. ApplyCancelled must re-check the exact
// account/order/status/reservation under one transaction before mutation.
type Store interface {
	Load(context.Context, string, string) (LocalState, error)
	ApplyCancelled(context.Context, ApplyParams) (bool, error)
}

type ApplyParams struct {
	ExecutionAccountID  string
	OrderID             string
	Evidence            Evidence
	EvidenceFingerprint string
}

type Params struct {
	Store         Store
	Evidence      EvidenceSource
	FinalityGrace time.Duration
	Now           func() time.Time
}

type Service struct {
	store         Store
	evidence      EvidenceSource
	finalityGrace time.Duration
	now           func() time.Time
}

func New(params Params) (*Service, error) {
	if params.Store == nil || params.Evidence == nil {
		return nil, fmt.Errorf("Kalshi repair store and evidence source are required")
	}
	if params.FinalityGrace == 0 {
		params.FinalityGrace = defaultFinalityGrace
	}
	if params.FinalityGrace < defaultFinalityGrace {
		return nil, fmt.Errorf("Kalshi repair finality grace must be at least %s", defaultFinalityGrace)
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{store: params.Store, evidence: params.Evidence, finalityGrace: params.FinalityGrace, now: params.Now}, nil
}

type Request struct {
	ExecutionAccountID string
	OrderID            string
	Apply              bool
	Confirmation       string
}

type Result struct {
	ExecutionAccountID  string         `json:"execution_account_id"`
	OrderID             string         `json:"order_id"`
	ClientOrderID       string         `json:"client_order_id"`
	AuthoritativeID     string         `json:"authoritative_order_id"`
	VenueStatus         string         `json:"venue_status"`
	VenueOutcomeSide    string         `json:"venue_outcome_side"`
	VenueAction         string         `json:"venue_action"`
	VenueBookSide       string         `json:"venue_book_side"`
	VenueOrderType      string         `json:"venue_order_type"`
	VenueTimeInForce    string         `json:"venue_time_in_force,omitempty"`
	VenueOrderPrice     domain.Decimal `json:"venue_order_price"`
	VenueFillCount      domain.Decimal `json:"venue_fill_count"`
	VenueRemainingCount domain.Decimal `json:"venue_remaining_count"`
	VenueFillIDs        []string       `json:"venue_fill_ids"`
	VenueLastUpdatedAt  time.Time      `json:"venue_last_updated_at"`
	EvidenceObservedAt  time.Time      `json:"evidence_observed_at"`
	EvidenceFingerprint string         `json:"evidence_sha256"`
	ReservedBalance     domain.Decimal `json:"remaining_reserved_balance"`
	ReservedShares      domain.Decimal `json:"remaining_reserved_shares"`
	DryRun              bool           `json:"dry_run"`
	Eligible            bool           `json:"eligible"`
	Applied             bool           `json:"applied"`
	AlreadyApplied      bool           `json:"already_applied"`
}

// Run never broad-scans. Both account and order are mandatory, and an apply
// additionally requires an exact "account/order" confirmation string.
func (service *Service) Run(ctx context.Context, request Request) (Result, error) {
	request.ExecutionAccountID = strings.TrimSpace(request.ExecutionAccountID)
	request.OrderID = strings.TrimSpace(request.OrderID)
	if request.ExecutionAccountID == "" || request.OrderID == "" {
		return Result{}, fmt.Errorf("execution account id and order id are required")
	}
	if request.Apply && strings.TrimSpace(request.Confirmation) != request.ExecutionAccountID+"/"+request.OrderID {
		return Result{}, fmt.Errorf("apply confirmation must equal execution-account-id/order-id")
	}
	local, err := service.store.Load(ctx, request.ExecutionAccountID, request.OrderID)
	if err != nil {
		return Result{}, err
	}
	if err := validateLocalState(local, request.ExecutionAccountID, request.OrderID); err != nil {
		return Result{}, err
	}
	evidence, err := service.evidence.Inspect(ctx, local.Order)
	if err != nil {
		return Result{}, fmt.Errorf("read authoritative Kalshi order/fill evidence: %w", err)
	}
	if err := service.validateEvidence(local.Order, evidence); err != nil {
		return Result{}, err
	}
	fingerprint, err := evidence.Fingerprint()
	if err != nil {
		return Result{}, err
	}
	result := Result{
		ExecutionAccountID: request.ExecutionAccountID, OrderID: request.OrderID,
		ClientOrderID: local.Order.Intent.ClientOrderID, AuthoritativeID: evidence.OrderID,
		VenueStatus: evidence.Status, VenueOutcomeSide: evidence.OutcomeSide,
		VenueAction: evidence.Action, VenueBookSide: evidence.BookSide,
		VenueOrderType: evidence.OrderType, VenueTimeInForce: evidence.TimeInForce,
		VenueOrderPrice: evidence.OrderPrice, VenueFillCount: evidence.FillCount,
		VenueRemainingCount: evidence.RemainingCount, VenueFillIDs: append([]string{}, evidence.FillIDs...),
		VenueLastUpdatedAt: evidence.LastUpdatedAt.UTC(), EvidenceObservedAt: evidence.ObservedAt.UTC(),
		EvidenceFingerprint: fingerprint,
		ReservedBalance:     local.RemainingReservedBalance, ReservedShares: local.RemainingReservedShares,
		DryRun: !request.Apply, Eligible: true,
	}
	if local.Order.Status == domain.OrderStatusCancelled {
		if local.ReservationStatus != domain.ReservationStatusReleased || local.RepairFingerprint != fingerprint {
			return Result{}, fmt.Errorf("cancelled local order is not the result of this exact evidence-backed repair")
		}
		result.AlreadyApplied = true
		return result, nil
	}
	if !request.Apply {
		return result, nil
	}
	applied, err := service.store.ApplyCancelled(ctx, ApplyParams{
		ExecutionAccountID: request.ExecutionAccountID, OrderID: request.OrderID,
		Evidence: evidence, EvidenceFingerprint: fingerprint,
	})
	if err != nil {
		return Result{}, fmt.Errorf("apply evidence-backed Kalshi cancellation repair: %w", err)
	}
	result.DryRun = false
	result.Applied = applied
	result.AlreadyApplied = !applied
	return result, nil
}

func validateLocalState(local LocalState, accountID, orderID string) error {
	order := local.Order
	if order.ID != orderID || order.Intent.ExecutionAccountID != accountID {
		return fmt.Errorf("local order does not match the explicit account/order scope")
	}
	if !strings.EqualFold(strings.TrimSpace(order.Intent.Venue), "kalshi") || order.Intent.MarketSource.Normalize() != domain.MarketSourceKalshi {
		return fmt.Errorf("repair supports Kalshi orders only")
	}
	if order.Status == domain.OrderStatusCancelled {
		return nil
	}
	if order.Status != domain.OrderStatusManualReview {
		return fmt.Errorf("order status %s is not MANUAL_REVIEW", order.Status)
	}
	if local.ReservationStatus != domain.ReservationStatusReconciliationRequired {
		return fmt.Errorf("reservation status %s is not RECONCILIATION_REQUIRED", local.ReservationStatus)
	}
	balanceSign, balanceErr := decimalSign(local.RemainingReservedBalance)
	sharesSign, sharesErr := decimalSign(local.RemainingReservedShares)
	if balanceErr != nil || sharesErr != nil {
		return fmt.Errorf("local reservation amounts are invalid")
	}
	if (order.Intent.Side == domain.SideBuy && (balanceSign <= 0 || sharesSign != 0)) ||
		(order.Intent.Side == domain.SideSell && (sharesSign <= 0 || balanceSign != 0)) {
		return fmt.Errorf("local reservation does not contain the expected unsettled collateral")
	}
	if sign, err := decimalSign(order.FilledSize); err != nil || sign != 0 {
		return fmt.Errorf("orders with local fills cannot use the no-fill repair")
	}
	return nil
}

func (service *Service) validateEvidence(order domain.Order, evidence Evidence) error {
	if strings.TrimSpace(evidence.OrderID) == "" || strings.TrimSpace(evidence.OrderID) == strings.TrimSpace(order.Intent.ClientOrderID) {
		return fmt.Errorf("Kalshi did not return a distinct authoritative order id")
	}
	if strings.TrimSpace(evidence.ClientOrderID) != strings.TrimSpace(order.Intent.ClientOrderID) ||
		strings.TrimSpace(evidence.MarketID) != strings.TrimSpace(order.Intent.MarketID) {
		return fmt.Errorf("Kalshi order identity does not match the local intent")
	}
	expectedType := strings.ToLower(strings.TrimSpace(string(order.Intent.Type)))
	expectedIdentity, err := domain.CanonicalKalshiOrderIdentity(order.Intent.Side, order.Intent.OutcomeID, order.Intent.WorstPrice)
	if err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(evidence.OutcomeSide)) != expectedIdentity.OutcomeSide ||
		strings.ToLower(strings.TrimSpace(evidence.BookSide)) != expectedIdentity.BookSide ||
		strings.ToLower(strings.TrimSpace(evidence.OrderType)) != expectedType {
		return fmt.Errorf("Kalshi canonical outcome/book-side/order-type does not match the local intent")
	}
	if order.Intent.Type != domain.OrderTypeLimit || order.Intent.TimeInForce != domain.TimeInForceFOK {
		return fmt.Errorf("legacy no-fill repair is restricted to LIMIT/FOK orders")
	}
	if remoteTIF := strings.ToLower(strings.TrimSpace(evidence.TimeInForce)); remoteTIF != "" && remoteTIF != "fill_or_kill" {
		return fmt.Errorf("Kalshi time_in_force does not match the local FOK intent")
	}
	if evidence.OrderPrice.IsEmpty() || !evidence.OrderPrice.Equal(expectedIdentity.OrderPrice) {
		return fmt.Errorf("Kalshi order price does not match the mapped local worst price")
	}
	if evidence.SubaccountNumber == nil || *evidence.SubaccountNumber != 0 {
		return fmt.Errorf("Kalshi subaccount identity does not match the submitted order")
	}
	status := strings.ToLower(strings.TrimSpace(evidence.Status))
	if status != "canceled" && status != "cancelled" {
		return fmt.Errorf("Kalshi order status %q is not a confirmed cancellation", evidence.Status)
	}
	for name, value := range map[string]domain.Decimal{
		"fill_count": evidence.FillCount, "remaining_count": evidence.RemainingCount,
	} {
		if value.IsEmpty() {
			return fmt.Errorf("Kalshi %s is missing", name)
		}
		if sign, err := value.Sign(); err != nil || sign != 0 {
			return fmt.Errorf("Kalshi %s must be zero", name)
		}
	}
	if evidence.FillIDs == nil {
		return fmt.Errorf("Kalshi fills query result is missing")
	}
	if len(evidence.FillIDs) != 0 {
		return fmt.Errorf("Kalshi fills endpoint returned %d fills; no-fill repair is forbidden", len(evidence.FillIDs))
	}
	if evidence.InitialCount.IsEmpty() || !evidence.InitialCount.Equal(order.Intent.Size) {
		return fmt.Errorf("Kalshi initial order size does not match the local intent")
	}
	if strings.TrimSpace(evidence.OrderQuerySource) != "KALSHI_ORDER_BY_CLIENT_THEN_ORDER_ID" ||
		strings.TrimSpace(evidence.FillQuerySource) != "KALSHI_FILLS_BY_ORDER_ID" {
		return fmt.Errorf("Kalshi order and fill query provenance is incomplete")
	}
	now := service.now().UTC()
	if evidence.ObservedAt.IsZero() || evidence.LastUpdatedAt.IsZero() || evidence.LastUpdatedAt.After(evidence.ObservedAt) || evidence.ObservedAt.After(now.Add(time.Second)) {
		return fmt.Errorf("Kalshi evidence timestamps are invalid")
	}
	if now.Sub(evidence.LastUpdatedAt.UTC()) < service.finalityGrace {
		return fmt.Errorf("Kalshi cancellation has not passed the fill finality grace")
	}
	return nil
}

func decimalSign(value domain.Decimal) (int, error) {
	if value.IsEmpty() {
		value = "0"
	}
	return value.Sign()
}

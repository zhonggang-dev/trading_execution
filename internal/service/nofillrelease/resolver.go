// Package nofillrelease decides whether an UNKNOWN Polymarket order was never
// accepted by the venue and, only then, releases its reservation.
//
// Principle: rather freeze too long than release wrongly. A release requires
// every source to agree at once: the time gate past the venue expiry plus a
// grace, a local ledger without any fill or acknowledgement, a venue that
// returns no order, no venue trade naming the order (any status), no open
// order, and no OrderFilled log for the order hash on either V2 exchange up to
// the confirmed head, with control queries proving the RPC backend serves that
// range. Any read failure keeps the order frozen for the next attempt.
package nofillrelease

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// Mode selects what the automatic path does with complete evidence.
type Mode string

const (
	// ModeOff disables the automatic evaluation.
	ModeOff Mode = "off"
	// ModeShadow records an audit row and logs "would release"; the order,
	// its reservation and its issues stay untouched.
	ModeShadow Mode = "shadow"
	// ModeOn cancels the order and releases its reservation.
	ModeOn Mode = "on"
)

// ParseMode validates a configured mode.
func ParseMode(value string) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(value))) {
	case ModeOff:
		return ModeOff, nil
	case ModeShadow:
		return ModeShadow, nil
	case ModeOn:
		return ModeOn, nil
	default:
		return "", fmt.Errorf("no-fill release mode must be off, shadow or on")
	}
}

// Store is the ledger side.
type Store interface {
	Facts(ctx context.Context, orderID string) (domain.NoFillLocalFacts, error)
	Apply(ctx context.Context, resolution domain.NoFillResolution) (domain.Order, error)
	RecordShadow(ctx context.Context, resolution domain.NoFillResolution) (bool, error)
	HasShadow(ctx context.Context, orderID string) (bool, error)
}

// VenueReader reads the venue evidence. It has no state-changing method.
type VenueReader interface {
	ReadOrder(ctx context.Context, order domain.Order) (domain.NoFillOrderRead, error)
	TradeReferences(ctx context.Context, order domain.Order, after time.Time) (domain.NoFillTradeScan, error)
	OpenOrders(ctx context.Context, order domain.Order) (domain.NoFillOpenOrderScan, error)
}

// ChainScanner scans OrderFilled logs by order hash.
type ChainScanner interface {
	ScanOrderFilled(ctx context.Context, request domain.NoFillChainScanRequest) (domain.NoFillChainEvidence, error)
}

// Params configures a Resolver.
type Params struct {
	Store  Store
	Venue  VenueReader
	Chain  ChainScanner
	Policy Policy
	// Mode applies to ResolveUnaccepted only; the operator path always
	// releases on complete evidence.
	Mode   Mode
	Actor  string
	Logger *slog.Logger
	Now    func() time.Time
}

// Resolver evaluates and applies no-fill releases.
type Resolver struct {
	store  Store
	venue  VenueReader
	chain  ChainScanner
	policy Policy
	mode   Mode
	actor  string
	logger *slog.Logger
	now    func() time.Time
}

var _ port.UnacceptedOrderResolver = (*Resolver)(nil)

// New validates dependencies and policy.
func New(params Params) (*Resolver, error) {
	if params.Store == nil || params.Venue == nil || params.Chain == nil {
		return nil, fmt.Errorf("no-fill release requires a store, a venue reader and a chain scanner")
	}
	policy, err := params.Policy.Normalize()
	if err != nil {
		return nil, err
	}
	if params.Mode == "" {
		params.Mode = ModeShadow
	}
	if _, err := ParseMode(string(params.Mode)); err != nil {
		return nil, err
	}
	if strings.TrimSpace(params.Actor) == "" {
		params.Actor = "trading_execution"
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Resolver{
		store: params.Store, venue: params.Venue, chain: params.Chain, policy: policy,
		mode: params.Mode, actor: strings.TrimSpace(params.Actor), logger: params.Logger, now: params.Now,
	}, nil
}

// Mode returns the configured automatic mode.
func (resolver *Resolver) Mode() Mode { return resolver.mode }

// Evaluation is the result of one evaluation.
type Evaluation struct {
	Decision  Decision
	Facts     domain.NoFillLocalFacts
	ConfirmID string
	// Evidence, EvidenceJSON and Digest are set only for DecisionResolve.
	Evidence     *Evidence
	EvidenceJSON []byte
	Digest       string
	mode         domain.NoFillResolutionMode
	reason       string
}

// EvaluateOptions names who evaluates and why.
type EvaluateOptions struct {
	Mode   domain.NoFillResolutionMode
	Actor  string
	Reason string
}

// Evaluate collects the evidence of one order and decides. It never writes.
// Cheap local checks run first; venue and chain reads only happen once the
// order is eligible and the time gate is open.
func (resolver *Resolver) Evaluate(ctx context.Context, orderID string, options EvaluateOptions) (Evaluation, error) {
	facts, err := resolver.store.Facts(ctx, orderID)
	if err != nil {
		return Evaluation{}, fmt.Errorf("read local no-fill facts: %w", err)
	}
	evaluation := Evaluation{Facts: facts, ConfirmID: ConfirmID(facts), mode: options.Mode, reason: strings.TrimSpace(options.Reason)}
	now := resolver.now().UTC()
	gate, local := checkLocal(facts, resolver.policy, now)
	if local.Kind != DecisionResolve {
		evaluation.Decision = local
		return evaluation, nil
	}
	var remote remoteResult
	remote.order, remote.orderErr = resolver.venue.ReadOrder(ctx, facts.Order)
	if remote.orderErr == nil && remote.order.NotFound {
		remote.trades, remote.tradesErr = resolver.venue.TradeReferences(ctx, facts.Order, gate.tradesAfter)
		if remote.tradesErr == nil && len(remote.trades.References) == 0 {
			remote.open, remote.openErr = resolver.venue.OpenOrders(ctx, facts.Order)
		}
	}
	if venue := decideVenue(remote); venue.Kind != DecisionResolve {
		evaluation.Decision = venue
		return evaluation, nil
	}
	remote.chain, remote.chainErr = resolver.chain.ScanOrderFilled(ctx, gate.scanRequest)
	remote.windowNotReached = errors.Is(remote.chainErr, domain.ErrNoFillChainWindowNotReached)
	if chain := decideChain(remote); chain.Kind != DecisionResolve {
		evaluation.Decision = chain
		return evaluation, nil
	}
	if err := ctx.Err(); err != nil {
		evaluation.Decision = decision(DecisionInconclusive, "evaluation interrupted: %v", err)
		return evaluation, nil
	}
	actor := strings.TrimSpace(options.Actor)
	if actor == "" {
		actor = resolver.actor
	}
	evidence := buildEvidence(facts, gate, remote, options.Mode, actor, evaluation.reason, now)
	encoded, digest, err := evidence.Encode()
	if err != nil {
		return Evaluation{}, err
	}
	evaluation.Decision = Decision{Kind: DecisionResolve, Reason: "every no-fill evidence source agrees"}
	evaluation.Evidence = &evidence
	evaluation.EvidenceJSON = encoded
	evaluation.Digest = digest
	return evaluation, nil
}

// Release cancels the order and releases its reservation in one transaction.
// It refuses any evaluation that is not DecisionResolve.
func (resolver *Resolver) Release(ctx context.Context, evaluation Evaluation) (domain.Order, error) {
	if evaluation.Decision.Kind != DecisionResolve || evaluation.Evidence == nil {
		return domain.Order{}, fmt.Errorf("no-fill release requires complete evidence (decision %s)", evaluation.Decision.Kind)
	}
	if evaluation.mode != domain.NoFillModeAuto && evaluation.mode != domain.NoFillModeOperator {
		return domain.Order{}, fmt.Errorf("no-fill release requires mode AUTO or OPERATOR")
	}
	return resolver.store.Apply(ctx, resolver.resolution(evaluation))
}

func (resolver *Resolver) resolution(evaluation Evaluation) domain.NoFillResolution {
	reasonCode := domain.NoFillReleaseAutoReasonCode
	operator := evaluation.mode == domain.NoFillModeOperator
	if operator {
		reasonCode = domain.NoFillReleaseOperatorReasonCode
	}
	reservationRevision := int64(0)
	if evaluation.Facts.Reservation != nil {
		reservationRevision = evaluation.Facts.Reservation.Revision
	}
	return domain.NoFillResolution{
		Mode: evaluation.mode, OrderID: evaluation.Facts.Order.ID,
		ExecutionAccountID: evaluation.Facts.Order.Intent.ExecutionAccountID,
		Actor:              evaluation.Evidence.Actor, Reason: evaluation.reason, ReasonCode: reasonCode,
		ExpectedOrderRevision:       evaluation.Facts.Order.Revision,
		ExpectedReservationRevision: reservationRevision,
		Evidence:                    evaluation.EvidenceJSON, EvidenceSHA256: evaluation.Digest,
		Summary:    evaluation.Evidence.Summary(evaluation.Digest),
		ResolvedAt: evaluation.Evidence.ResolvedAt, RequireLeaseFree: operator,
		AllowReadOnlyProgress:  operator,
		ExpectedReleaseBalance: evaluation.Evidence.Release.Balance,
		ExpectedReleaseShares:  evaluation.Evidence.Release.Shares,
	}
}

// ResolveUnaccepted is the automatic path called by Execution.Refresh after a
// venue read returned no order and fill synchronization succeeded. Only
// ModeOn may release; shadow mode records the evaluation once per order.
func (resolver *Resolver) ResolveUnaccepted(ctx context.Context, order domain.Order) (domain.Order, bool, error) {
	if resolver.mode == ModeOff {
		return order, false, nil
	}
	mode := domain.NoFillModeAuto
	if resolver.mode == ModeShadow {
		mode = domain.NoFillModeShadow
		// A shadow observation is recorded once; evaluating again only costs
		// venue and RPC requests.
		if recorded, err := resolver.store.HasShadow(ctx, order.ID); err != nil || recorded {
			return order, false, err
		}
	}
	evaluation, err := resolver.Evaluate(ctx, order.ID, EvaluateOptions{Mode: mode, Actor: resolver.actor})
	if err != nil {
		resolver.logger.Warn("no-fill release evaluation failed; order stays UNKNOWN and its reservation frozen",
			orderAttrs(order, "error", err.Error())...)
		return order, false, err
	}
	attrs := orderAttrs(order, "decision", string(evaluation.Decision.Kind), "reason", evaluation.Decision.Reason)
	switch evaluation.Decision.Kind {
	case DecisionResolve:
	case DecisionFillObserved:
		resolver.logger.Error("no-fill release found a fill naming the order; it must be recovered through fill synchronization", attrs...)
		return order, false, nil
	case DecisionInconclusive:
		resolver.logger.Warn("no-fill release evidence incomplete; order stays UNKNOWN and its reservation frozen", attrs...)
		return order, false, nil
	case DecisionNotEligible:
		if evaluation.Decision.routine {
			resolver.logger.Debug("no-fill release not applicable; order stays UNKNOWN", attrs...)
		} else {
			resolver.logger.Info("no-fill release not applicable; order stays UNKNOWN", attrs...)
		}
		return order, false, nil
	default:
		resolver.logger.Debug("no-fill release not due yet", attrs...)
		return order, false, nil
	}
	resolution := resolver.resolution(evaluation)
	attrs = append(attrs, "evidence_sha256", evaluation.Digest, "summary", resolution.Summary,
		"release_balance", evaluation.Evidence.Release.Balance.String(),
		"release_shares", evaluation.Evidence.Release.Shares.String())
	if mode == domain.NoFillModeShadow {
		if _, err := resolver.store.RecordShadow(ctx, resolution); err != nil {
			resolver.logger.Warn("no-fill release shadow record failed", append(attrs, "error", err.Error())...)
			return order, false, err
		}
		resolver.logger.Info("no-fill release (shadow): evidence complete, would release; nothing changed", attrs...)
		return order, false, nil
	}
	released, err := resolver.store.Apply(ctx, resolution)
	if err != nil {
		resolver.logger.Warn("no-fill release transaction failed; order stays UNKNOWN and its reservation frozen",
			append(attrs, "error", err.Error())...)
		return order, false, err
	}
	resolver.logger.Warn("no-fill release applied: order cancelled and reservation released", attrs...)
	return released, true, nil
}

func orderAttrs(order domain.Order, extra ...any) []any {
	return append([]any{
		"order_id", order.ID,
		"execution_account_id", order.Intent.ExecutionAccountID,
		"venue_order_id", order.VenueOrderID,
	}, extra...)
}

func buildEvidence(
	facts domain.NoFillLocalFacts,
	gate localGate,
	remote remoteResult,
	mode domain.NoFillResolutionMode,
	actor, reason string,
	now time.Time,
) Evidence {
	order := facts.Order
	submit := gate.submit
	return Evidence{
		Version: EvidenceVersion, Mode: string(mode), Actor: actor, Reason: reason,
		ResolvedAt: now, BinaryCommit: binaryCommit(),
		Order: EvidenceOrder{
			OrderID: order.ID, ExecutionAccountID: order.Intent.ExecutionAccountID,
			VenueOrderID: order.VenueOrderID, Side: order.Intent.Side, TokenID: order.Intent.TokenID,
			ConditionID: order.Intent.ConditionID, Size: order.Intent.Size, WorstPrice: order.Intent.WorstPrice,
			CreatedAt: order.CreatedAt.UTC(), Revision: order.Revision, ReservationRevision: gate.reservation.Revision,
		},
		Submit: EvidenceSubmit{
			AttemptID: submit.ID, StartedAt: submit.StartedAt.UTC(), CompletedAt: submit.CompletedAt,
			Outcome: string(submit.Outcome), HTTPStatus: submit.HTTPStatus, ErrorCode: submit.ErrorCode,
			ErrorMessage: submit.ErrorMessage, VenueExpiresAt: submit.VenueExpiresAt,
		},
		TimeGate:        gate.timeGate,
		VenueOrderReads: gate.reads,
		VenueOrder:      remote.order,
		VenueTrades:     remote.trades,
		VenueOpenOrders: remote.open,
		Chain:           remote.chain,
		Local: EvidenceLocal{
			FilledSize: order.FilledSize, Fills: facts.FillCount, FillEvents: facts.FillEventCount,
			EventStatuses: facts.EventStatuses, ReservationState: gate.reservation.Status,
			OpenIssues: facts.OpenIssues,
		},
		IssuesResolved: append([]string{}, gate.releasable...),
		Release:        gate.release,
	}
}

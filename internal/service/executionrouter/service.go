package executionrouter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/domain/orderstate"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

var ErrMaintenanceOnly = errors.New("Kalshi execution route is maintenance-only")

// ErrAccountMaintenance marks a Polymarket account held in maintenance. It
// wraps ErrMaintenanceOnly so existing callers treat both deferrals alike.
var ErrAccountMaintenance = fmt.Errorf("execution account is in maintenance: %w", ErrMaintenanceOnly)

// Maintenance reports execution accounts that must not take new orders. A
// maintenance account keeps refresh, cancellation, and fill accounting; only
// new submissions, strategy input, and pre-venue resumption are held back.
type Maintenance interface {
	MaintenanceReason(accountID string) (string, bool)
}

type Execution interface {
	Submit(context.Context, domain.OrderIntent) (port.OrderSubmitResult, error)
	Resume(context.Context, string) (domain.Order, error)
	Get(context.Context, string) (domain.Order, error)
	Refresh(context.Context, string) (domain.Order, error)
	Cancel(context.Context, string) (domain.Order, error)
	FinalizeCancellation(context.Context, string) (domain.Order, error)
	Events(context.Context, string) ([]domain.OrderEvent, error)
	Attempts(context.Context, string) ([]domain.OrderAttempt, error)
}

// Route is one Kalshi execution account. Kalshi accounts are first-class
// accounts (kalshi:<name>): they are not attached to a Polymarket wallet.
type Route struct {
	// AccountID is the Kalshi execution account, e.g. kalshi:main.
	AccountID string
	// StrategyID is the strategy configured for the account. Kalshi orders do
	// not pass the PostgreSQL strategy-binding gate, so the route is their
	// authorization: an intent for the account must carry this strategy.
	StrategyID string
	Execution  Execution
	// MaintenanceOnly keeps historical orders routable while preventing new
	// strategy deliveries after a degraded Kalshi startup preflight.
	MaintenanceOnly bool
}

type Service struct {
	repository  port.OrderRepository
	primary     Execution
	routes      map[string]Route
	maintenance Maintenance
}

func New(repository port.OrderRepository, primary Execution, routes []Route) (*Service, error) {
	return NewWithMaintenance(repository, primary, routes, nil)
}

// NewWithMaintenance adds a dynamic per-account maintenance list. A nil list
// keeps the exact behavior of New.
func NewWithMaintenance(repository port.OrderRepository, primary Execution, routes []Route,
	maintenance Maintenance) (*Service, error) {
	if repository == nil || primary == nil {
		return nil, fmt.Errorf("execution router requires repository and primary execution")
	}
	service := &Service{repository: repository, primary: primary, routes: make(map[string]Route, len(routes)),
		maintenance: maintenance}
	for _, route := range routes {
		route.AccountID = strings.TrimSpace(route.AccountID)
		route.StrategyID = domain.CanonicalStrategyID(route.StrategyID)
		if route.AccountID == "" || route.StrategyID == "" || route.Execution == nil {
			return nil, fmt.Errorf("Kalshi execution route is incomplete")
		}
		if !domain.IsKalshiExecutionAccount(route.AccountID) {
			return nil, fmt.Errorf("Kalshi execution route account %q must start with %q",
				route.AccountID, domain.KalshiExecutionAccountPrefix)
		}
		if _, exists := service.routes[route.AccountID]; exists {
			return nil, fmt.Errorf("duplicate Kalshi execution route")
		}
		service.routes[route.AccountID] = route
	}
	return service, nil
}

// inMaintenance reports the maintenance reason of an account, if any.
func (service *Service) inMaintenance(accountID string) (string, bool) {
	if service.maintenance == nil {
		return "", false
	}
	return service.maintenance.MaintenanceReason(strings.TrimSpace(accountID))
}

// kalshiRoute finds the route of a Kalshi intent. A durable delivery persisted
// before Kalshi accounts became first-class carries the Polymarket account it
// used to ride on (e.g. "main"); it is resolved to "kalshi:main" so the frozen
// intent can still be recovered exactly once.
func (service *Service) kalshiRoute(intent domain.OrderIntent) (Route, bool) {
	accountID := strings.TrimSpace(intent.ExecutionAccountID)
	route, ok := service.routes[accountID]
	if !ok && accountID != "" && !domain.IsKalshiExecutionAccount(accountID) {
		route, ok = service.routes[domain.KalshiExecutionAccountPrefix+accountID]
	}
	if !ok || domain.CanonicalStrategyID(intent.StrategyID) != route.StrategyID {
		return Route{}, false
	}
	return route, true
}

// KalshiEnabled reports whether a Kalshi intent has a tradable route.
func (service *Service) KalshiEnabled(intent domain.OrderIntent) bool {
	if intent.MarketSource.Normalize() != domain.MarketSourceKalshi {
		return false
	}
	route, ok := service.kalshiRoute(intent)
	if !ok || route.MaintenanceOnly {
		return false
	}
	_, maintenance := service.inMaintenance(route.AccountID)
	return !maintenance
}

// StrategyInputEnabled reports whether an account may be sent to the strategy
// this cycle. Polymarket accounts may unless they are in maintenance; a Kalshi
// account needs a tradable route for its configured strategy, so a strategy is
// never asked to decide for an account that cannot place the order.
func (service *Service) StrategyInputEnabled(binding domain.StrategyExecutionBinding) bool {
	binding = binding.Normalize()
	if _, maintenance := service.inMaintenance(binding.ExecutionAccountID); maintenance {
		return false
	}
	if binding.MarketSource != domain.MarketSourceKalshi {
		return true
	}
	route, ok := service.routes[binding.ExecutionAccountID]
	return ok && !route.MaintenanceOnly && route.StrategyID == binding.StrategyID
}

func (service *Service) Enabled(intent domain.OrderIntent) bool {
	if intent.MarketSource.Normalize() == domain.MarketSourcePolymarket {
		if domain.IsKalshiExecutionAccount(intent.ExecutionAccountID) {
			return false
		}
		_, maintenance := service.inMaintenance(intent.ExecutionAccountID)
		return !maintenance
	}
	return service.KalshiEnabled(intent)
}

func (service *Service) Submit(ctx context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	if intent.MarketSource.Normalize() != domain.MarketSourceKalshi {
		if domain.IsKalshiExecutionAccount(intent.ExecutionAccountID) {
			return port.OrderSubmitResult{}, fmt.Errorf("Polymarket intent cannot use Kalshi execution account %q", intent.ExecutionAccountID)
		}
		// Refused before the primary creates an order or reserves assets.
		if reason, maintenance := service.inMaintenance(intent.ExecutionAccountID); maintenance {
			return port.OrderSubmitResult{}, fmt.Errorf("%w: account %q (%s)", ErrAccountMaintenance,
				strings.TrimSpace(intent.ExecutionAccountID), reason)
		}
		return service.primary.Submit(ctx, intent)
	}
	route, ok := service.kalshiRoute(intent)
	if !ok || route.MaintenanceOnly {
		return port.OrderSubmitResult{}, fmt.Errorf("Kalshi live route is not enabled for account %q", intent.ExecutionAccountID)
	}
	if reason, maintenance := service.inMaintenance(route.AccountID); maintenance {
		return port.OrderSubmitResult{}, fmt.Errorf("Kalshi live route is in maintenance for account %q (%s)", route.AccountID, reason)
	}
	if intent.ExecutionAccountID != route.AccountID {
		// Legacy delivery: record which account it was persisted under.
		metadata := make(map[string]string, len(intent.Metadata)+1)
		for key, value := range intent.Metadata {
			metadata[key] = value
		}
		metadata["logical_execution_account_id"] = intent.ExecutionAccountID
		intent.Metadata = metadata
		intent.ExecutionAccountID = route.AccountID
	}
	return route.Execution.Submit(ctx, intent)
}

func (service *Service) executionForOrder(ctx context.Context, orderID string) (Execution, error) {
	order, err := service.repository.Get(ctx, strings.TrimSpace(orderID))
	if err != nil {
		return nil, err
	}
	if order.Intent.MarketSource.Normalize() != domain.MarketSourceKalshi {
		return service.primary, nil
	}
	if route, ok := service.routes[strings.TrimSpace(order.Intent.ExecutionAccountID)]; ok {
		return route.Execution, nil
	}
	return nil, fmt.Errorf("Kalshi order has no configured execution route")
}

func (service *Service) Get(ctx context.Context, id string) (domain.Order, error) {
	return service.repository.Get(ctx, strings.TrimSpace(id))
}
func (service *Service) Resume(ctx context.Context, id string) (domain.Order, error) {
	order, lookupErr := service.repository.Get(ctx, strings.TrimSpace(id))
	if lookupErr != nil {
		return domain.Order{}, lookupErr
	}
	accountID := strings.TrimSpace(order.Intent.ExecutionAccountID)
	if order.Intent.MarketSource.Normalize() == domain.MarketSourceKalshi {
		if route, ok := service.routes[accountID]; ok {
			if _, maintenance := service.inMaintenance(accountID); route.MaintenanceOnly || maintenance {
				return service.deferResume(ctx, order, "KALSHI_ROUTE_MAINTENANCE",
					"Kalshi preflight is unavailable; pre-venue recovery is deferred without submission", ErrMaintenanceOnly)
			}
		}
	} else if _, maintenance := service.inMaintenance(accountID); maintenance && !order.Terminal() {
		return service.deferResume(ctx, order, "ACCOUNT_MAINTENANCE",
			"execution account is in maintenance; pre-venue recovery is deferred without submission", ErrAccountMaintenance)
	}
	e, err := service.executionForOrder(ctx, id)
	if err != nil {
		return domain.Order{}, err
	}
	return e.Resume(ctx, id)
}

// deferResume records an audited self-transition instead of resuming a
// pre-venue order, so nothing is submitted while the account cannot trade.
func (service *Service) deferResume(ctx context.Context, order domain.Order, reasonCode, reason string,
	cause error) (domain.Order, error) {
	next, event, deferErr := orderstate.Apply(order, orderstate.Transition{
		EventID:    fmt.Sprintf("event:%s:%d", order.ID, order.Revision+1),
		To:         order.Status,
		Trigger:    domain.TransitionTriggerMaintenance,
		ReasonCode: reasonCode,
		Reason:     reason,
		At:         time.Now().UTC(),
	})
	if deferErr == nil {
		deferErr = service.repository.Transition(ctx, next, event)
	}
	if deferErr != nil {
		return order, errors.Join(cause, deferErr)
	}
	return next, cause
}

func (service *Service) Refresh(ctx context.Context, id string) (domain.Order, error) {
	e, err := service.executionForOrder(ctx, id)
	if err != nil {
		return domain.Order{}, err
	}
	return e.Refresh(ctx, id)
}
func (service *Service) Cancel(ctx context.Context, id string) (domain.Order, error) {
	e, err := service.executionForOrder(ctx, id)
	if err != nil {
		return domain.Order{}, err
	}
	return e.Cancel(ctx, id)
}
func (service *Service) FinalizeCancellation(ctx context.Context, id string) (domain.Order, error) {
	e, err := service.executionForOrder(ctx, id)
	if err != nil {
		return domain.Order{}, err
	}
	return e.FinalizeCancellation(ctx, id)
}
func (service *Service) Events(ctx context.Context, id string) ([]domain.OrderEvent, error) {
	e, err := service.executionForOrder(ctx, id)
	if err != nil {
		return nil, err
	}
	return e.Events(ctx, id)
}
func (service *Service) Attempts(ctx context.Context, id string) ([]domain.OrderAttempt, error) {
	e, err := service.executionForOrder(ctx, id)
	if err != nil {
		return nil, err
	}
	return e.Attempts(ctx, id)
}

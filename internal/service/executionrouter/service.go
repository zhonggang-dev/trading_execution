package executionrouter

import (
	"context"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

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

type Route struct {
	ModelID, StrategyID, LogicalAccountID, InternalAccountID string
	Execution                                                Execution
}

type Service struct {
	repository port.OrderRepository
	primary    Execution
	routes     map[string]Route
}

func New(repository port.OrderRepository, primary Execution, routes []Route) (*Service, error) {
	if repository == nil || primary == nil {
		return nil, fmt.Errorf("execution router requires repository and primary execution")
	}
	service := &Service{repository: repository, primary: primary, routes: make(map[string]Route, len(routes))}
	for _, route := range routes {
		route.ModelID = strings.TrimSpace(route.ModelID)
		route.StrategyID = domain.CanonicalStrategyID(route.StrategyID)
		route.LogicalAccountID = strings.TrimSpace(route.LogicalAccountID)
		route.InternalAccountID = strings.TrimSpace(route.InternalAccountID)
		if route.ModelID == "" || route.StrategyID == "" || route.LogicalAccountID == "" || route.InternalAccountID == "" || route.Execution == nil {
			return nil, fmt.Errorf("Kalshi execution route is incomplete")
		}
		key := routeKey(route.ModelID, route.StrategyID, route.LogicalAccountID)
		if _, exists := service.routes[key]; exists {
			return nil, fmt.Errorf("duplicate Kalshi execution route")
		}
		service.routes[key] = route
	}
	return service, nil
}

func (service *Service) KalshiEnabled(intent domain.OrderIntent) bool {
	_, ok := service.routes[routeKey(intent.ModelID, intent.StrategyID, intent.ExecutionAccountID)]
	return intent.MarketSource.Normalize() == domain.MarketSourceKalshi && ok
}

func (service *Service) Enabled(intent domain.OrderIntent) bool {
	if intent.MarketSource.Normalize() == domain.MarketSourcePolymarket {
		return true
	}
	return service.KalshiEnabled(intent)
}

func (service *Service) Submit(ctx context.Context, intent domain.OrderIntent) (port.OrderSubmitResult, error) {
	if intent.MarketSource.Normalize() != domain.MarketSourceKalshi {
		return service.primary.Submit(ctx, intent)
	}
	route, ok := service.routes[routeKey(intent.ModelID, intent.StrategyID, intent.ExecutionAccountID)]
	if !ok {
		return port.OrderSubmitResult{}, fmt.Errorf("Kalshi live route is not enabled for binding")
	}
	intent.ExecutionAccountID = route.InternalAccountID
	if intent.Metadata == nil {
		intent.Metadata = map[string]string{}
	}
	intent.Metadata["logical_execution_account_id"] = route.LogicalAccountID
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
	for _, route := range service.routes {
		if route.InternalAccountID == order.Intent.ExecutionAccountID {
			return route.Execution, nil
		}
	}
	return nil, fmt.Errorf("Kalshi order has no configured execution route")
}

func (service *Service) Get(ctx context.Context, id string) (domain.Order, error) {
	return service.repository.Get(ctx, strings.TrimSpace(id))
}
func (service *Service) Resume(ctx context.Context, id string) (domain.Order, error) {
	e, err := service.executionForOrder(ctx, id)
	if err != nil {
		return domain.Order{}, err
	}
	return e.Resume(ctx, id)
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

func routeKey(model, strategy, account string) string {
	return strings.TrimSpace(model) + "\x00" + domain.CanonicalStrategyID(strategy) + "\x00" + strings.TrimSpace(account)
}

package positionsource

import (
	"context"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type Route struct{ LogicalAccountID, InternalAccountID string }

type Router struct {
	source port.StrategyPositionSource
	routes map[string][]string
}

func New(source port.StrategyPositionSource, routes []Route) (*Router, error) {
	if source == nil {
		return nil, fmt.Errorf("position source is required")
	}
	router := &Router{source: source, routes: make(map[string][]string)}
	for _, route := range routes {
		logical, internal := strings.TrimSpace(route.LogicalAccountID), strings.TrimSpace(route.InternalAccountID)
		if logical == "" || internal == "" {
			return nil, fmt.Errorf("position route is incomplete")
		}
		router.routes[logical] = append(router.routes[logical], internal)
	}
	return router, nil
}

func (router *Router) ListOpenLots(ctx context.Context, binding domain.StrategyExecutionContext) ([]domain.PositionLot, error) {
	binding = binding.Normalize()
	logical := binding.ExecutionAccountID
	lots, err := router.source.ListOpenLots(ctx, binding)
	if err != nil {
		return nil, err
	}
	for _, internal := range router.routes[logical] {
		internalBinding := binding
		internalBinding.ExecutionAccountID = internal
		additional, err := router.source.ListOpenLots(ctx, internalBinding)
		if err != nil {
			return nil, err
		}
		for index := range additional {
			additional[index].ExecutionAccountID = logical
			additional[index].MarketSource = domain.MarketSourceKalshi
		}
		lots = append(lots, additional...)
	}
	return lots, nil
}

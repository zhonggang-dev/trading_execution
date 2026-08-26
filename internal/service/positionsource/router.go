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

func (router *Router) ListOpenLots(ctx context.Context, logical string) ([]domain.PositionLot, error) {
	logical = strings.TrimSpace(logical)
	lots, err := router.source.ListOpenLots(ctx, logical)
	if err != nil {
		return nil, err
	}
	for _, internal := range router.routes[logical] {
		additional, err := router.source.ListOpenLots(ctx, internal)
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

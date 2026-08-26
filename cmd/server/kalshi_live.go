package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/accountscope"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/executionrouter"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
	"github.com/UniPat-AI/trading_execution/internal/service/positionsource"
)

type kalshiComposition struct {
	execution      *executionrouter.Service
	positionSource port.StrategyPositionSource
	activeAccounts []string
}

func composeKalshiExecution(ctx context.Context, cfg config.Config, database *sql.DB, repository port.OrderRepository,
	guard port.Guard, ledger *postgresadapter.FillLedger, primary executionrouter.Execution, basePositions port.StrategyPositionSource,
	logger *slog.Logger) (kalshiComposition, error) {
	if len(cfg.Kalshi.LiveBindings) == 0 {
		router, err := executionrouter.New(repository, primary, nil)
		return kalshiComposition{execution: router, positionSource: basePositions}, err
	}
	if !cfg.Kalshi.MarketDataEnabled {
		return kalshiComposition{}, fmt.Errorf("Kalshi live bindings require KALSHI_MARKET_DATA_ENABLED=true")
	}
	routes := make([]executionrouter.Route, 0, len(cfg.Kalshi.LiveBindings))
	positionRoutes := make([]positionsource.Route, 0, len(cfg.Kalshi.LiveBindings))
	active := make([]string, 0, len(cfg.Kalshi.LiveBindings))
	for _, binding := range cfg.Kalshi.LiveBindings {
		if !configuredBinding(cfg.DecisionCycle.Bindings, binding) {
			return kalshiComposition{}, fmt.Errorf("Kalshi live route is not an exact decision binding")
		}
		built, err := buildKalshiLiveRoute(ctx, cfg, binding, database, repository, guard, ledger)
		if err != nil {
			// Kalshi is an additive venue. A credential/API failure downgrades only
			// this exact route to dry-run; it must never take Polymarket offline.
			logger.Error("Kalshi live binding disabled after preflight", "model_id", binding.ModelID,
				"strategy_id", binding.StrategyID, "execution_account_id", binding.ExecutionAccountID, "error", err)
			continue
		}
		routes = append(routes, built.route)
		positionRoutes = append(positionRoutes, built.positionRoute)
		active = append(active, built.internalAccountID)
		logger.Info("Kalshi live binding preflight passed", "model_id", binding.ModelID, "strategy_id", binding.StrategyID,
			"execution_account_id", binding.ExecutionAccountID, "internal_account_id", built.internalAccountID,
			"buy_funded", built.buyFunded)
	}
	router, err := executionrouter.New(repository, primary, routes)
	if err != nil {
		return kalshiComposition{}, err
	}
	positionRouter, err := positionsource.New(basePositions, positionRoutes)
	if err != nil {
		return kalshiComposition{}, err
	}
	return kalshiComposition{execution: router, positionSource: positionRouter, activeAccounts: active}, nil
}

type builtKalshiLiveRoute struct {
	route             executionrouter.Route
	positionRoute     positionsource.Route
	internalAccountID string
	buyFunded         bool
}

func buildKalshiLiveRoute(ctx context.Context, cfg config.Config, binding config.KalshiLiveBinding, database *sql.DB,
	repository port.OrderRepository, guard port.Guard, ledger *postgresadapter.FillLedger) (builtKalshiLiveRoute, error) {
	client, err := kalshi.NewClient(kalshi.ClientParams{BaseURL: cfg.Kalshi.APIURL, APIKeyID: binding.APIKeyID,
		PrivateKeyPath: binding.PrivateKeyPath, HTTPClient: noRedirectHTTPClient(cfg.Kalshi.RequestTimeout), LiveTradingEnabled: true})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	capabilities, err := client.ProbeCapabilities(ctx)
	if err != nil {
		return builtKalshiLiveRoute{}, fmt.Errorf("probe Kalshi capabilities: %w", err)
	}
	if !capabilities.Write {
		return builtKalshiLiveRoute{}, fmt.Errorf("Kalshi API key lacks write scope")
	}
	balance, err := client.GetBalance(ctx)
	if err != nil {
		return builtKalshiLiveRoute{}, fmt.Errorf("read Kalshi balance: %w", err)
	}
	availableBalance := balance.AvailableDollars()
	availableSign, signErr := availableBalance.Sign()
	if signErr != nil {
		return builtKalshiLiveRoute{}, fmt.Errorf("Kalshi available balance is invalid")
	}
	internalID := "kalshi:" + binding.ExecutionAccountID
	if err := syncKalshiExecutionAccount(ctx, database, internalID, binding.APIKeyID, availableBalance); err != nil {
		return builtKalshiLiveRoute{}, err
	}
	venue, err := kalshi.NewVenue(client)
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	books, err := kalshi.NewOrderBookSource(kalshi.OrderBookParams{Client: client})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	validator, err := kalshi.NewMarketValidator(books)
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	processor, err := fillprocessor.New(fillprocessor.Params{Orders: repository, Source: client, Ledger: ledger})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	reservations, err := postgresadapter.NewReservationManager(postgresadapter.ReservationManagerParams{DB: database, MaxBuyFeeRateBPS: cfg.Polymarket.MaxBuyFeeRateBPS})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	scope, err := accountscope.New([]string{internalID}, []string{internalID})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	service, err := execution.New(execution.Params{Repository: repository, Venue: venue, Guard: guard, MarketValidator: validator,
		Reservations: reservations, FillSynchronizer: processor, AuthoritativeFills: true, AccountScope: scope,
		RequirePreparedPlacement: true, MaxReconcileAttempts: cfg.Polymarket.MaxReconcileAttempts,
		CancelFillFinalityGrace: cfg.Polymarket.CancelFillFinalityGrace, EntrySubmissionDisabled: cfg.DecisionCycle.EntrySubmissionDisabled,
		EntryDisabledAccounts: internalEntryDisabledAccounts(binding.ExecutionAccountID, internalID, cfg.DecisionCycle.EntryDisabledAccounts)})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	return builtKalshiLiveRoute{
		route:             executionrouter.Route{ModelID: binding.ModelID, StrategyID: binding.StrategyID, LogicalAccountID: binding.ExecutionAccountID, InternalAccountID: internalID, Execution: service},
		positionRoute:     positionsource.Route{LogicalAccountID: binding.ExecutionAccountID, InternalAccountID: internalID},
		internalAccountID: internalID, buyFunded: availableSign > 0,
	}, nil
}

func internalEntryDisabledAccounts(logicalAccountID, internalAccountID string, disabled []string) []string {
	for _, accountID := range disabled {
		if strings.TrimSpace(accountID) == logicalAccountID {
			return []string{internalAccountID}
		}
	}
	return nil
}

func configuredBinding(bindings []domain.StrategyExecutionBinding, candidate config.KalshiLiveBinding) bool {
	for _, binding := range bindings {
		binding = binding.Normalize()
		if binding.ModelID == candidate.ModelID && binding.StrategyID == candidate.StrategyID && binding.ExecutionAccountID == candidate.ExecutionAccountID {
			return true
		}
	}
	return false
}

func syncKalshiExecutionAccount(ctx context.Context, database *sql.DB, accountID, apiKeyID string, balance domain.Decimal) error {
	if database == nil || strings.TrimSpace(balance.String()) == "" {
		return fmt.Errorf("Kalshi account balance is unavailable")
	}
	result, err := database.ExecContext(ctx, `
		INSERT INTO execution_accounts (execution_account_id,wallet_address,collateral_asset,total_balance,available_balance,reserved_balance,reconciled_at)
		VALUES ($1,$2,'USD',$3::numeric,$3::numeric,0,clock_timestamp())
		ON CONFLICT (execution_account_id) DO UPDATE SET total_balance=$3::numeric, available_balance=$3::numeric,
			reconciled_at=clock_timestamp(), updated_at=clock_timestamp(), version=execution_accounts.version+1
		WHERE execution_accounts.wallet_address=$2 AND execution_accounts.collateral_asset='USD' AND execution_accounts.reserved_balance=0`,
		accountID, "kalshi:"+apiKeyID, balance.String())
	if err != nil {
		return fmt.Errorf("sync Kalshi execution account: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("Kalshi account identity changed or has an active reservation")
	}
	return nil
}

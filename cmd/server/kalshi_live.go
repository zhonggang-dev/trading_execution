package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/accountscope"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/executionrouter"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshisettlement"
)

type kalshiComposition struct {
	execution *executionrouter.Service
	// activeAccounts are the Kalshi execution accounts with a route (tradable
	// or maintenance-only).
	activeAccounts []string
	// settlements apply Kalshi's own settlement records to the ledger of each
	// tradable route; the caller runs them for the life of the process.
	settlements []*kalshisettlement.Service
}

func composeKalshiExecution(ctx context.Context, cfg config.Config, database *sql.DB, repository port.OrderRepository,
	guard port.Guard, ledger *postgresadapter.FillLedger, primary executionrouter.Execution,
	logger *slog.Logger) (kalshiComposition, error) {
	if len(cfg.Kalshi.LiveAccounts) == 0 {
		router, err := executionrouter.New(repository, primary, nil)
		return kalshiComposition{execution: router}, err
	}
	if !cfg.Kalshi.MarketDataEnabled {
		return kalshiComposition{}, fmt.Errorf("Kalshi live accounts require KALSHI_MARKET_DATA_ENABLED=true")
	}
	routes := make([]executionrouter.Route, 0, len(cfg.Kalshi.LiveAccounts))
	active := make([]string, 0, len(cfg.Kalshi.LiveAccounts))
	settlements := make([]*kalshisettlement.Service, 0, len(cfg.Kalshi.LiveAccounts))
	for _, liveAccount := range cfg.Kalshi.LiveAccounts {
		account, ok := configuredKalshiAccount(cfg.DecisionCycle, liveAccount.ExecutionAccountID)
		if !ok {
			return kalshiComposition{}, fmt.Errorf("Kalshi live account %q is not a KALSHI decision account", liveAccount.ExecutionAccountID)
		}
		built, err := buildKalshiLiveRoute(ctx, cfg, account, liveAccount, database, repository, guard, ledger)
		if err != nil {
			needsMaintenance, maintenanceErr := kalshiAccountNeedsMaintenance(ctx, database, account.ExecutionAccountID)
			if maintenanceErr != nil {
				return kalshiComposition{}, fmt.Errorf("inspect failed Kalshi route maintenance state: %w", maintenanceErr)
			}
			if needsMaintenance {
				// Never start without the only route capable of recovering an owned
				// order or releasing protected assets. A bad local key/configuration
				// must be repaired instead of silently orphaning durable trading state.
				return kalshiComposition{}, fmt.Errorf("Kalshi account with unresolved protected assets cannot enter maintenance mode: %w", err)
			}
			// Kalshi is an additive venue. A credential/API failure downgrades only
			// an account with no durable work to dry-run; it must never take
			// Polymarket offline merely because a Kalshi account is unavailable.
			logger.Error("Kalshi live account disabled after preflight", "strategy_id", account.StrategyID,
				"execution_account_id", account.ExecutionAccountID, "error", err)
			continue
		}
		routes = append(routes, built.route)
		active = append(active, account.ExecutionAccountID)
		if built.settlement != nil {
			settlements = append(settlements, built.settlement)
		}
		logger.Info("Kalshi live account preflight passed", "strategy_id", account.StrategyID,
			"execution_account_id", account.ExecutionAccountID, "mode", string(account.Mode),
			"buy_funded", built.buyFunded, "maintenance_only", built.maintenanceOnly,
			"balance_sync_deferred", built.balanceSyncDeferred)
	}
	router, err := executionrouter.New(repository, primary, routes)
	if err != nil {
		return kalshiComposition{}, err
	}
	return kalshiComposition{execution: router, activeAccounts: active, settlements: settlements}, nil
}

type builtKalshiLiveRoute struct {
	route               executionrouter.Route
	buyFunded           bool
	settlement          *kalshisettlement.Service
	maintenanceOnly     bool
	balanceSyncDeferred bool
}

func buildKalshiLiveRoute(ctx context.Context, cfg config.Config, account config.DecisionAccount, credential config.KalshiLiveAccount,
	database *sql.DB, repository port.OrderRepository, guard port.Guard, ledger *postgresadapter.FillLedger) (builtKalshiLiveRoute, error) {
	client, err := kalshi.NewClient(kalshi.ClientParams{BaseURL: cfg.Kalshi.APIURL, APIKeyID: credential.APIKeyID,
		PrivateKeyPath: credential.PrivateKeyPath, HTTPClient: noRedirectHTTPClient(cfg.Kalshi.RequestTimeout), LiveTradingEnabled: true})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	internalID := account.ExecutionAccountID
	maintenanceOnly := false
	balanceSyncDeferred := false
	cashBalance := domain.Decimal("0")
	capabilities, preflightErr := retryKalshiRateLimit(ctx, sleepContext, client.ProbeCapabilities)
	if preflightErr == nil && !capabilities.Write {
		preflightErr = fmt.Errorf("Kalshi API key lacks write scope")
	}
	if preflightErr == nil {
		balanceSyncDeferred, cashBalance, preflightErr = postgresadapter.SyncKalshiExecutionAccount(
			ctx, database, internalID, credential.APIKeyID,
			func() (domain.Decimal, error) {
				balance, balanceErr := retryKalshiRateLimit(ctx, sleepContext, client.GetBalance)
				if balanceErr != nil {
					return "", fmt.Errorf("read Kalshi balance: %w", balanceErr)
				}
				return balance.AvailableDollars(), nil
			},
			func() ([]domain.KalshiSettlement, error) {
				return retryKalshiRateLimit(ctx, sleepContext, client.ListAllSettlements)
			},
		)
	}
	if preflightErr != nil {
		exists, accountErr := kalshiExecutionAccountExists(ctx, database, internalID, credential.APIKeyID)
		if accountErr != nil {
			return builtKalshiLiveRoute{}, accountErr
		}
		if !exists {
			return builtKalshiLiveRoute{}, fmt.Errorf("Kalshi remote preflight failed before the execution account was initialized: %w", preflightErr)
		}
		// Keep a maintenance route for refresh/fill/finality recovery. BUY is
		// disabled until a clean restart completes the remote preflight.
		maintenanceOnly = true
	}
	venue, err := kalshi.NewVenue(client)
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	books, err := kalshi.NewOrderBookSource(kalshi.OrderBookParams{Client: client})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	fees, err := kalshi.NewFeeScheduleSource(kalshi.FeeScheduleParams{Client: client})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	validator, err := kalshi.NewMarketValidator(books, fees)
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
	entryDisabledAccounts := accountEntryDisabled(internalID, cfg.DecisionCycle.EntryDisabledAccounts())
	service, err := execution.New(execution.Params{Repository: repository, Venue: venue, Guard: guard, MarketValidator: validator,
		Reservations: reservations, FillSynchronizer: processor, AuthoritativeFills: true, AccountScope: scope,
		RequirePreparedPlacement: true, MaxReconcileAttempts: cfg.Polymarket.MaxReconcileAttempts,
		CancelFillFinalityGrace: cfg.Polymarket.CancelFillFinalityGrace, EntrySubmissionDisabled: cfg.DecisionCycle.EntrySubmissionDisabled,
		EntryDisabledAccounts: entryDisabledAccounts})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	var settlement *kalshisettlement.Service
	// A route that failed its remote preflight is maintenance-only; its API
	// access is not trusted, so it does not settle until a clean restart.
	if cfg.Kalshi.SettlementEnabled && !maintenanceOnly {
		settlementStore, storeErr := postgresadapter.NewKalshiSettlementStore(database, []string{internalID})
		if storeErr != nil {
			return builtKalshiLiveRoute{}, storeErr
		}
		settlement, err = kalshisettlement.New(kalshisettlement.Params{
			ExecutionAccountID: internalID, Source: client, Store: settlementStore,
			Interval: cfg.Kalshi.SettlementInterval, BalanceDriftThreshold: cfg.Kalshi.BalanceDriftThreshold,
		})
		if err != nil {
			return builtKalshiLiveRoute{}, err
		}
	}
	cashSign, _ := cashBalance.Sign()
	return builtKalshiLiveRoute{
		route: executionrouter.Route{AccountID: internalID, StrategyID: account.StrategyID, Execution: service,
			MaintenanceOnly: maintenanceOnly},
		buyFunded: cashSign > 0, settlement: settlement,
		maintenanceOnly: maintenanceOnly, balanceSyncDeferred: balanceSyncDeferred,
	}, nil
}

// kalshiPreflightRetryDelays spaces the retries of a rate-limited preflight
// read. Kalshi throttles repeated balance reads from the same key inside a
// second, and the preflight reads the balance twice back to back.
var kalshiPreflightRetryDelays = []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}

// retryKalshiRateLimit repeats a read-only Kalshi request that was rejected
// with KALSHI_RATE_LIMITED. Any other error, and the last rate-limit error, is
// returned unchanged so the caller still downgrades the binding to maintenance.
func retryKalshiRateLimit[T any](ctx context.Context, wait func(context.Context, time.Duration) error,
	call func(context.Context) (T, error)) (T, error) {
	for attempt := 0; ; attempt++ {
		value, err := call(ctx)
		if err == nil || !kalshiRateLimited(err) || attempt >= len(kalshiPreflightRetryDelays) {
			return value, err
		}
		if waitErr := wait(ctx, kalshiPreflightRetryDelays[attempt]); waitErr != nil {
			return value, err
		}
	}
}

func kalshiRateLimited(err error) bool {
	var venueError *port.VenueError
	return errors.As(err, &venueError) && venueError.Code == "KALSHI_RATE_LIMITED"
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// accountEntryDisabled returns the account as the execution entry gate when
// its mode blocks BUY (SELL_ONLY or PAUSED).
func accountEntryDisabled(accountID string, disabled []string) []string {
	for _, disabledID := range disabled {
		if strings.TrimSpace(disabledID) == accountID {
			return []string{accountID}
		}
	}
	return nil
}

// configuredKalshiAccountIDs returns every KALSHI account row, whether or not
// its route passed the startup preflight.
func configuredKalshiAccountIDs(cycle config.DecisionCycle) []string {
	accounts := cycle.AccountsForSource(domain.MarketSourceKalshi)
	ids := make([]string, 0, len(accounts))
	for _, account := range accounts {
		ids = append(ids, account.ExecutionAccountID)
	}
	return ids
}

// configuredKalshiAccount finds the KALSHI decision account a credential
// belongs to.
func configuredKalshiAccount(cycle config.DecisionCycle, accountID string) (config.DecisionAccount, bool) {
	for _, account := range cycle.AccountsForSource(domain.MarketSourceKalshi) {
		if account.ExecutionAccountID == strings.TrimSpace(accountID) {
			return account, true
		}
	}
	return config.DecisionAccount{}, false
}

func kalshiExecutionAccountExists(ctx context.Context, database *sql.DB, accountID, apiKeyID string) (bool, error) {
	if database == nil {
		return false, nil
	}
	var exists bool
	if err := database.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM execution_accounts
			WHERE execution_account_id=$1 AND LOWER(wallet_address)=LOWER($2)
			  AND collateral_asset='USD'
		)`, accountID, "kalshi:"+apiKeyID).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect existing Kalshi execution account: %w", err)
	}
	return exists, nil
}

func kalshiAccountNeedsMaintenance(ctx context.Context, database *sql.DB, accountID string) (bool, error) {
	if database == nil {
		return false, nil
	}
	var required bool
	if err := database.QueryRowContext(ctx, `
		SELECT
			EXISTS (
				SELECT 1 FROM asset_reservations
				WHERE execution_account_id=$1
				  AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
			)
			OR EXISTS (
				SELECT 1 FROM execution_accounts
				WHERE execution_account_id=$1 AND reserved_balance > 0
			)`, accountID).Scan(&required); err != nil {
		return false, fmt.Errorf("inspect Kalshi maintenance protected assets: %w", err)
	}
	return required, nil
}

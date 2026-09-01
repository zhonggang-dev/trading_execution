package main

import (
	"context"
	"database/sql"
	"errors"
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
			needsMaintenance, maintenanceErr := kalshiAccountNeedsMaintenance(ctx, database, "kalshi:"+binding.ExecutionAccountID)
			if maintenanceErr != nil {
				return kalshiComposition{}, fmt.Errorf("inspect failed Kalshi route maintenance state: %w", maintenanceErr)
			}
			if needsMaintenance {
				// Never start without the only route capable of recovering an owned
				// order or releasing protected assets. A bad local key/configuration
				// must be repaired instead of silently orphaning durable trading state.
				return kalshiComposition{}, fmt.Errorf("Kalshi binding with unresolved protected assets cannot enter maintenance mode: %w", err)
			}
			// Kalshi is an additive venue. A credential/API failure downgrades only
			// a binding with no durable work to dry-run; it must never take
			// Polymarket offline merely because a new Kalshi account is unavailable.
			logger.Error("Kalshi live binding disabled after preflight", "model_id", binding.ModelID,
				"strategy_id", binding.StrategyID, "execution_account_id", binding.ExecutionAccountID, "error", err)
			continue
		}
		routes = append(routes, built.route)
		positionRoutes = append(positionRoutes, built.positionRoute)
		active = append(active, built.internalAccountID)
		logger.Info("Kalshi live binding preflight passed", "model_id", binding.ModelID, "strategy_id", binding.StrategyID,
			"execution_account_id", binding.ExecutionAccountID, "internal_account_id", built.internalAccountID,
			"buy_funded", built.buyFunded, "maintenance_only", built.maintenanceOnly,
			"balance_sync_deferred", built.balanceSyncDeferred)
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
	route               executionrouter.Route
	positionRoute       positionsource.Route
	internalAccountID   string
	buyFunded           bool
	maintenanceOnly     bool
	balanceSyncDeferred bool
}

func buildKalshiLiveRoute(ctx context.Context, cfg config.Config, binding config.KalshiLiveBinding, database *sql.DB,
	repository port.OrderRepository, guard port.Guard, ledger *postgresadapter.FillLedger) (builtKalshiLiveRoute, error) {
	client, err := kalshi.NewClient(kalshi.ClientParams{BaseURL: cfg.Kalshi.APIURL, APIKeyID: binding.APIKeyID,
		PrivateKeyPath: binding.PrivateKeyPath, HTTPClient: noRedirectHTTPClient(cfg.Kalshi.RequestTimeout), LiveTradingEnabled: true})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	internalID := "kalshi:" + binding.ExecutionAccountID
	maintenanceOnly := false
	balanceSyncDeferred := false
	cashBalance := domain.Decimal("0")
	capabilities, preflightErr := client.ProbeCapabilities(ctx)
	if preflightErr == nil && !capabilities.Write {
		preflightErr = fmt.Errorf("Kalshi API key lacks write scope")
	}
	if preflightErr == nil {
		balanceSyncDeferred, cashBalance, preflightErr = syncKalshiExecutionAccount(
			ctx, database, internalID, binding.APIKeyID,
			func() (domain.Decimal, error) {
				balance, balanceErr := client.GetBalance(ctx)
				if balanceErr != nil {
					return "", fmt.Errorf("read Kalshi balance: %w", balanceErr)
				}
				return balance.AvailableDollars(), nil
			},
		)
	}
	if preflightErr != nil {
		exists, accountErr := kalshiExecutionAccountExists(ctx, database, internalID, binding.APIKeyID)
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
	entryDisabledAccounts := internalEntryDisabledAccounts(binding.ExecutionAccountID, internalID, cfg.DecisionCycle.EntryDisabledAccounts)
	service, err := execution.New(execution.Params{Repository: repository, Venue: venue, Guard: guard, MarketValidator: validator,
		Reservations: reservations, FillSynchronizer: processor, AuthoritativeFills: true, AccountScope: scope,
		RequirePreparedPlacement: true, MaxReconcileAttempts: cfg.Polymarket.MaxReconcileAttempts,
		CancelFillFinalityGrace: cfg.Polymarket.CancelFillFinalityGrace, EntrySubmissionDisabled: cfg.DecisionCycle.EntrySubmissionDisabled,
		EntryDisabledAccounts: entryDisabledAccounts})
	if err != nil {
		return builtKalshiLiveRoute{}, err
	}
	cashSign, _ := cashBalance.Sign()
	return builtKalshiLiveRoute{
		route:             executionrouter.Route{ModelID: binding.ModelID, StrategyID: binding.StrategyID, LogicalAccountID: binding.ExecutionAccountID, InternalAccountID: internalID, Execution: service, MaintenanceOnly: maintenanceOnly},
		positionRoute:     positionsource.Route{LogicalAccountID: binding.ExecutionAccountID, InternalAccountID: internalID},
		internalAccountID: internalID, buyFunded: cashSign > 0,
		maintenanceOnly: maintenanceOnly, balanceSyncDeferred: balanceSyncDeferred,
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

// syncKalshiExecutionAccount updates an external cash baseline only when no
// owned order still has protected assets. Kalshi's balance endpoint reports
// currently available cash, which may already include a venue fill that the
// local ledger has not applied yet. Overwriting total_balance in that window
// would make the later fill debit/credit the same trade twice.
func syncKalshiExecutionAccount(
	ctx context.Context,
	database *sql.DB,
	accountID, apiKeyID string,
	readBalance func() (domain.Decimal, error),
) (bool, domain.Decimal, error) {
	if database == nil || readBalance == nil {
		return false, "", fmt.Errorf("Kalshi account balance source is unavailable")
	}
	tx, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, "", fmt.Errorf("begin Kalshi account sync: %w", err)
	}
	defer tx.Rollback()
	// Serialize account initialization and hold the same execution_accounts row
	// lock used by Reserve and FillLedger before taking the external balance
	// snapshot. Otherwise a concurrent fill could settle between GET /balance
	// and the local UPDATE, letting an older snapshot overwrite the newer ledger.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "kalshi-account-sync:"+accountID); err != nil {
		return false, "", fmt.Errorf("lock Kalshi account sync: %w", err)
	}

	walletAddress := ""
	collateralAsset := ""
	reservedBalance := "0"
	existing := true
	if err := tx.QueryRowContext(ctx, `
		SELECT wallet_address, collateral_asset, reserved_balance::text
		FROM execution_accounts WHERE execution_account_id=$1 FOR UPDATE`, accountID).
		Scan(&walletAddress, &collateralAsset, &reservedBalance); errors.Is(err, sql.ErrNoRows) {
		existing = false
	} else if err != nil {
		return false, "", fmt.Errorf("lock existing Kalshi execution account: %w", err)
	}
	if existing && (!strings.EqualFold(strings.TrimSpace(walletAddress), "kalshi:"+strings.TrimSpace(apiKeyID)) ||
		!strings.EqualFold(strings.TrimSpace(collateralAsset), "USD")) {
		return false, "", fmt.Errorf("Kalshi account identity changed")
	}
	var unresolved bool
	if existing {
		if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM asset_reservations
			WHERE execution_account_id=$1
			  AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')
		)`, accountID).Scan(&unresolved); err != nil {
			return false, "", fmt.Errorf("inspect unresolved Kalshi reservations: %w", err)
		}
	}
	reservedSign, reservedErr := domain.Decimal(reservedBalance).Sign()
	if reservedErr != nil {
		return false, "", fmt.Errorf("local Kalshi reserved balance is invalid")
	}
	balance, err := readBalance()
	if err != nil {
		return false, "", err
	}
	if sign, signErr := balance.Sign(); signErr != nil || sign < 0 {
		return false, "", fmt.Errorf("Kalshi cash balance is invalid")
	}
	if !existing {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO execution_accounts (
				execution_account_id,wallet_address,collateral_asset,
				total_balance,available_balance,reserved_balance,reconciled_at
			) VALUES ($1,$2,'USD',$3::numeric,$3::numeric,0,clock_timestamp())`,
			accountID, "kalshi:"+apiKeyID, balance.String()); err != nil {
			return false, "", fmt.Errorf("insert Kalshi execution account: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return false, "", fmt.Errorf("commit new Kalshi execution account: %w", err)
		}
		return false, balance, nil
	}
	if unresolved || reservedSign > 0 {
		if err := tx.Commit(); err != nil {
			return false, "", fmt.Errorf("commit deferred Kalshi account sync: %w", err)
		}
		return true, balance, nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_accounts
		SET total_balance=$2::numeric, available_balance=$2::numeric,
			reconciled_at=clock_timestamp(), updated_at=clock_timestamp(), version=version+1
		WHERE execution_account_id=$1`, accountID, balance.String()); err != nil {
		return false, "", fmt.Errorf("sync settled Kalshi execution account: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, "", fmt.Errorf("commit Kalshi execution account sync: %w", err)
	}
	return false, balance, nil
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

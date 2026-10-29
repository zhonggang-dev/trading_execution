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
		balanceSyncDeferred, cashBalance, preflightErr = syncKalshiExecutionAccount(
			ctx, database, internalID, credential.APIKeyID,
			func() (domain.Decimal, error) {
				balance, balanceErr := retryKalshiRateLimit(ctx, sleepContext, client.GetBalance)
				if balanceErr != nil {
					return "", fmt.Errorf("read Kalshi balance: %w", balanceErr)
				}
				return balance.AvailableDollars(), nil
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
	// The overwrite below replaces the local cash with the exchange balance, so
	// whatever the two disagreed by is written down before it disappears.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO execution_account_events (
			account_event_id, execution_account_id, event_type, total_balance_delta,
			available_balance_delta, reserved_balance_delta, total_balance_after,
			available_balance_after, reserved_balance_after, occurred_at
		)
		SELECT 'kalshi-balance-sync:'||execution_account_id||':'||to_char(clock_timestamp(),'YYYYMMDD"T"HH24MISSUS'),
		       execution_account_id, 'KALSHI_BALANCE_SYNC', $2::numeric - total_balance,
		       $2::numeric - available_balance, 0, $2::numeric, $2::numeric, reserved_balance, clock_timestamp()
		FROM execution_accounts
		WHERE execution_account_id=$1 AND (total_balance <> $2::numeric OR available_balance <> $2::numeric)`,
		accountID, balance.String()); err != nil {
		return false, "", fmt.Errorf("record Kalshi balance sync: %w", err)
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

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketgamma"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/accountscope"
	"github.com/UniPat-AI/trading_execution/internal/service/clobheartbeat"
	"github.com/UniPat-AI/trading_execution/internal/service/decisionrunner"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
	"github.com/UniPat-AI/trading_execution/internal/service/liveoperations"
	"github.com/UniPat-AI/trading_execution/internal/service/marketvalidation"
	"github.com/UniPat-AI/trading_execution/internal/service/readiness"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliationtrigger"
)

const polymarketCollateralAsset = "pUSD"

// liveRuntime 保存已经通过预检的实盘依赖图和后台服务。
type liveRuntime struct {
	repository     *postgresadapter.OrderRepository
	execution      *execution.Service
	reconciliation *reconciliation.Service
	readiness      *readiness.All
	heartbeat      *clobheartbeat.Service
	runner         *reconciliation.Runner
	operations     *liveoperations.Service
	decisionRunner *decisionrunner.Runner
	activeAccounts []string
}

// buildLiveRuntimeParams 收拢实盘依赖装配参数，避免长参数列表破坏函数声明可读性。
type buildLiveRuntimeParams struct {
	ctx               context.Context
	cfg               config.Config
	database          *sql.DB
	databaseReadiness *postgresadapter.HealthChecker
	guard             port.Guard
	logger            *slog.Logger
}

type liveAccountProbeClient interface {
	ProbeAccount(context.Context, string) (polymarket.AccountProbe, error)
	ProbeFunding(context.Context, string) (polymarket.FundingProbe, error)
}

type liveAccountPreflightResult struct {
	account     polymarket.AccountProbe
	funding     *polymarket.FundingProbe
	eligibility *polymarket.GeographicEligibility
}

// buildLiveRuntime 装配 Polymarket V2 实盘链路，并在开放 HTTP 前完成启动对账和首次只读快照。
func buildLiveRuntime(params buildLiveRuntimeParams) (*liveRuntime, error) {
	ctx, cfg := params.ctx, params.cfg
	database, databaseReadiness := params.database, params.databaseReadiness
	guard, logger := params.guard, params.logger
	startedAt := time.Now().UTC()
	if database == nil || databaseReadiness == nil {
		return nil, fmt.Errorf("live execution requires a ready PostgreSQL database")
	}
	if guard == nil {
		return nil, fmt.Errorf("live execution requires a hard risk guard")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{
		Path: cfg.Polymarket.AccountsFile,
		// Production startup must be read-only with respect to credentials. API
		// credentials have to be provisioned deliberately before this process.
		BootstrapMissingAPICredentials: false,
	})
	if err != nil {
		return nil, fmt.Errorf("load Polymarket live accounts: %w", err)
	}
	accountIDs := make([]string, 0, len(accounts))
	for _, account := range accounts {
		accountIDs = append(accountIDs, account.ExecutionAccountID)
	}
	if err := validateCurrentLiveWallet67Release(
		accountIDs, cfg.DecisionCycle.SubmissionDisabledAccounts,
	); err != nil {
		return nil, err
	}
	if cfg.DecisionCycle.Enabled {
		// This set equality must be proven before the first authenticated venue
		// read. A retired wallet left in the secret must never be recovered,
		// reconciled, or enrolled in a heartbeat by a shadow-mode process.
		if err := validateDecisionAccounts(cfg.DecisionCycle, accountIDs); err != nil {
			return nil, err
		}
	}
	reconciliationAccountIDs, quarantinedAccountIDs, err := partitionReconciliationAccounts(
		accountIDs,
		cfg.DecisionCycle.SubmissionDisabledAccounts,
	)
	if err != nil {
		return nil, err
	}
	quarantinedAccountSet := make(map[string]struct{}, len(quarantinedAccountIDs))
	for _, accountID := range quarantinedAccountIDs {
		quarantinedAccountSet[accountID] = struct{}{}
	}
	executionAccountScope, err := accountscope.New(reconciliationAccountIDs, accountIDs)
	if err != nil {
		return nil, fmt.Errorf("configure live execution account scope: %w", err)
	}
	activeAuthorizations, err := currentLiveWallet67Authorizations(reconciliationAccountIDs)
	if err != nil {
		return nil, err
	}
	if err := postgresadapter.CheckLiveActiveAccountAuthorization(
		ctx, database, activeAuthorizations,
	); err != nil {
		return nil, fmt.Errorf("verify active execution account authorization: %w", err)
	}
	provider, err := polymarket.NewStaticCredentialProvider(accounts)
	if err != nil {
		return nil, err
	}
	clobHTTP := noRedirectHTTPClient(cfg.Polymarket.RequestTimeout)
	orderFilledReader, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{
		RPCURL:                cfg.Polymarket.PolygonRPCURL,
		RequiredConfirmations: uint64(cfg.Polymarket.OrderFilledConfirmations),
		HTTPClient:            noRedirectHTTPClient(cfg.Polymarket.RequestTimeout),
		RequestTimeout:        cfg.Polymarket.RequestTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("configure Polygon OrderFilled evidence: %w", err)
	}
	feeEvidence, err := newPolygonFillFeeEvidence(orderFilledReader)
	if err != nil {
		return nil, err
	}
	tradingClient, err := polymarket.NewTradingClient(polymarket.TradingClientParams{
		BaseURL:        cfg.Polymarket.CLOBURL,
		HTTPClient:     clobHTTP,
		Credentials:    provider,
		FeeEvidence:    feeEvidence,
		RequestTimeout: cfg.Polymarket.RequestTimeout,
	})
	if err != nil {
		return nil, err
	}
	protocol, err := tradingClient.ProbeProtocol(ctx, cfg.Polymarket.MaxClockSkew)
	if err != nil {
		return nil, fmt.Errorf("Polymarket CLOB V2 protocol preflight: %w", err)
	}
	logger.Info("Polymarket CLOB V2 protocol preflight passed",
		"version", protocol.Version,
		"clock_skew_ms", protocol.ClockSkew.Milliseconds(),
	)

	geoblock, err := polymarket.NewGeoblockClient(polymarket.GeoblockClientParams{
		URL:        cfg.Polymarket.GeoblockURL,
		HTTPClient: noRedirectHTTPClient(cfg.Polymarket.RequestTimeout),
		Timeout:    cfg.Polymarket.RequestTimeout,
	})
	if err != nil {
		return nil, err
	}
	eligibility, err := polymarket.NewCLOBEligibilityChecker(polymarket.CLOBEligibilityCheckerParams{
		Geoblock:           geoblock,
		ClosedOnly:         tradingClient,
		APIExemptCountries: cfg.Polymarket.FrontendOnlyAPICountries,
	})
	if err != nil {
		return nil, err
	}

	expectedBindings := make([]postgresadapter.ExpectedExecutionAccount, 0, len(accounts))
	for _, account := range accounts {
		_, quarantined := quarantinedAccountSet[account.ExecutionAccountID]
		preflight, preflightErr := preflightLiveAccount(
			ctx, account.ExecutionAccountID, quarantined, tradingClient, eligibility,
		)
		if preflightErr != nil {
			return nil, preflightErr
		}
		expectedBindings = append(expectedBindings, postgresadapter.ExpectedExecutionAccount{
			ExecutionAccountID: account.ExecutionAccountID,
			WalletAddress:      preflight.account.FunderAddress,
		})
		if quarantined {
			logger.Warn("Polymarket quarantined account identity preflight passed",
				"execution_account_id", account.ExecutionAccountID,
				"open_orders", preflight.account.OpenOrderCount,
				"funding_and_placement_preflight_skipped", true,
			)
			continue
		}
		logger.Info("Polymarket live account preflight passed",
			"execution_account_id", account.ExecutionAccountID,
			"open_orders", preflight.account.OpenOrderCount,
			"required_v2_allowances_positive", preflight.funding.RequiredAllowancesPositive,
			"eligibility_reason", preflight.eligibility.Reason,
		)
	}
	if err := postgresadapter.CheckLiveAccountBindings(
		ctx, database, expectedBindings, polymarketCollateralAsset,
	); err != nil {
		return nil, fmt.Errorf("bind Polymarket wallets to PostgreSQL ledgers: %w", err)
	}
	if err := postgresadapter.CheckLiveLedgerBootstrap(ctx, database, expectedBindings); err != nil {
		return nil, fmt.Errorf("validate PostgreSQL live ledger bootstrap: %w", err)
	}
	var quarantineChecker *postgresadapter.ExecutionAccountQuarantineChecker
	if len(quarantinedAccountIDs) > 0 {
		quarantineChecker, err = postgresadapter.NewExecutionAccountQuarantineChecker(database, quarantinedAccountIDs)
		if err != nil {
			return nil, fmt.Errorf("configure execution account quarantine: %w", err)
		}
		if err := quarantineChecker.Check(ctx); err != nil {
			return nil, fmt.Errorf("verify execution account quarantine: %w", err)
		}
		logger.Warn("execution accounts quarantined from automatic reconciliation",
			"execution_account_ids", quarantinedAccountIDs,
			"active_reconciliation_accounts", len(reconciliationAccountIDs),
		)
	}

	repository, err := postgresadapter.NewOrderRepository(database)
	if err != nil {
		return nil, err
	}
	reservations, err := postgresadapter.NewReservationManager(postgresadapter.ReservationManagerParams{
		DB:               database,
		MaxBuyFeeRateBPS: cfg.Polymarket.MaxBuyFeeRateBPS,
	})
	if err != nil {
		return nil, err
	}
	fillLedger, err := postgresadapter.NewFillLedger(postgresadapter.FillLedgerParams{DB: database})
	if err != nil {
		return nil, err
	}
	fillProcessor, err := fillprocessor.New(fillprocessor.Params{
		Orders: repository,
		Source: tradingClient,
		Ledger: fillLedger,
	})
	if err != nil {
		return nil, err
	}
	triggerBridge, err := reconciliationtrigger.New(0)
	if err != nil {
		return nil, err
	}

	marketUniverse, err := polymarketgamma.New(polymarketgamma.Params{
		BaseURL:    cfg.Polymarket.GammaURL,
		HTTPClient: noRedirectHTTPClient(cfg.Polymarket.RequestTimeout),
		UserAgent:  cfg.App.Name,
	})
	if err != nil {
		return nil, err
	}
	orderBooks, err := polymarket.NewOrderBookSource(polymarket.OrderBookParams{
		BaseURL:    cfg.Polymarket.CLOBURL,
		HTTPClient: noRedirectHTTPClient(cfg.Polymarket.RequestTimeout),
	})
	if err != nil {
		return nil, err
	}
	marketValidator, err := marketvalidation.New(marketvalidation.Params{
		Universe:   marketUniverse,
		OrderBooks: orderBooks,
	})
	if err != nil {
		return nil, err
	}

	heartbeat, err := clobheartbeat.New(clobheartbeat.Params{
		Client:      tradingClient,
		Accounts:    reconciliationAccountIDs,
		Interval:    cfg.Polymarket.HeartbeatInterval,
		CallTimeout: cfg.Polymarket.HeartbeatCallTimeout,
		StaleAfter:  cfg.Polymarket.HeartbeatStaleAfter,
	})
	if err != nil {
		return nil, err
	}
	conditionalFundingVenue, err := polymarket.NewConditionalFundingVenue(tradingClient, tradingClient)
	if err != nil {
		return nil, err
	}
	eligibleVenue, err := polymarket.NewEligibilityVenue(conditionalFundingVenue, eligibility)
	if err != nil {
		return nil, err
	}
	guardedVenue, err := clobheartbeat.NewVenue(eligibleVenue, heartbeat)
	if err != nil {
		return nil, err
	}
	reconciliationVenue, err := newPlacementReadinessVenue(guardedVenue)
	if err != nil {
		return nil, err
	}

	executionService, err := execution.New(execution.Params{
		Repository:               repository,
		Venue:                    reconciliationVenue,
		Guard:                    guard,
		MarketValidator:          marketValidator,
		Reservations:             reservations,
		Reconciliation:           triggerBridge,
		FillSynchronizer:         fillProcessor,
		AuthoritativeFills:       true,
		CancelFillFinalityGrace:  cfg.Polymarket.CancelFillFinalityGrace,
		ImmediateCancelFinality:  false,
		MaxReconcileAttempts:     cfg.Polymarket.MaxReconcileAttempts,
		AccountScope:             executionAccountScope,
		RequirePreparedPlacement: true,
		EntrySubmissionDisabled:  cfg.DecisionCycle.EntrySubmissionDisabled,
		EntryDisabledAccounts:    cfg.DecisionCycle.EntryDisabledAccounts,
	})
	if err != nil {
		return nil, err
	}

	positionSource, err := polymarketdata.NewPositionClient(polymarketdata.PositionClientParams{
		BaseURL:    cfg.Polymarket.DataAPIURL,
		HTTPClient: noRedirectHTTPClient(cfg.Polymarket.RequestTimeout),
	})
	if err != nil {
		return nil, err
	}
	balanceSource, err := evmrpc.NewERC20BalanceClient(evmrpc.ERC20BalanceParams{
		RPCURL:       cfg.Polymarket.PolygonRPCURL,
		TokenAddress: polymarket.PUSDAddress,
		Asset:        polymarketCollateralAsset,
		Decimals:     6,
		HTTPClient:   noRedirectHTTPClient(cfg.Polymarket.RequestTimeout),
	})
	if err != nil {
		return nil, err
	}
	recorder, err := postgresadapter.NewReconciliationRecorder(database)
	if err != nil {
		return nil, err
	}
	positionBaselines, err := postgresadapter.NewExternalPositionBaselineRepository(database)
	if err != nil {
		return nil, err
	}
	reconciliationService, err := reconciliation.New(reconciliation.Params{
		Orders:                    repository,
		Venue:                     tradingClient,
		PositionSources:           []port.ExternalPositionSource{positionSource},
		PositionBaselines:         positionBaselines,
		PositionDispositionTrades: positionBaselines,
		BalanceSources:            []port.ExternalBalanceSource{balanceSource},
		Ledger:                    fillLedger,
		Fills:                     fillProcessor,
		OrderRefresher:            executionService,
		Recorder:                  recorder,
		TradeLookback:             cfg.Polymarket.ReconciliationLookback,
		PositionEpsilon:           cfg.Polymarket.PositionEpsilon,
		BalanceEpsilon:            cfg.Polymarket.BalanceEpsilon,
		AccountScope:              executionAccountScope,
	})
	if err != nil {
		return nil, err
	}
	runner, err := reconciliation.NewRunner(reconciliation.RunnerParams{
		Service:             reconciliationService,
		Accounts:            reconciliationAccountIDs,
		QuarantinedAccounts: quarantinedAccountIDs,
		Interval:            cfg.Polymarket.ReconciliationInterval,
		Logger:              logger,
	})
	if err != nil {
		return nil, err
	}
	placementReconciliationReadiness, err := newPlacementAccountReadiness(runner, quarantineChecker)
	if err != nil {
		return nil, err
	}
	if err := reconciliationVenue.Bind(placementReconciliationReadiness); err != nil {
		return nil, err
	}
	if err := triggerBridge.Bind(runner); err != nil {
		return nil, err
	}

	startupSweep := runner.Sweep(ctx, domain.ReconciliationTriggerStartup)
	if err := validateStartupReconciliation(startupSweep, len(reconciliationAccountIDs)); err != nil {
		return nil, err
	}
	if triggerBridge.Dropped() != 0 {
		return nil, fmt.Errorf("live startup dropped %d reconciliation trigger(s)", triggerBridge.Dropped())
	}
	liveOperationsRepository, err := postgresadapter.NewLiveOperationsRepository(database)
	if err != nil {
		return nil, err
	}
	operations, err := liveoperations.New(liveoperations.Params{
		Repository: liveOperationsRepository, Venue: tradingClient,
		PositionSource: positionSource, PositionBaselines: positionBaselines,
		BalanceSource: balanceSource, OrderBooks: orderBooks,
		Accounts: reconciliationAccountIDs, VenueName: "Polymarket CLOB", StartedAt: startedAt,
		RunID:    "live-" + startedAt.Format("20060102T150405.000000000Z"),
		Interval: cfg.LiveOperations.Interval, RefreshTimeout: cfg.LiveOperations.RefreshTimeout,
		MaxSnapshotAge: cfg.LiveOperations.MaxSnapshotAge, EventLimit: cfg.LiveOperations.EventLimit,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	if err := operations.Refresh(ctx); err != nil {
		return nil, fmt.Errorf("build initial live operations snapshot: %w", err)
	}
	decisionRunner, err := buildDecisionRunner(buildDecisionRunnerParams{
		cfg: cfg, database: database, positionSource: fillLedger, orderBooks: orderBooks,
		executor: executionService, accountIDs: accountIDs, logger: logger,
	})
	if err != nil {
		return nil, err
	}

	readinessChecks := []readiness.NamedChecker{
		readiness.NamedChecker{Name: "postgres", Checker: databaseReadiness},
		readiness.NamedChecker{Name: "reconciliation", Checker: runner},
		readiness.NamedChecker{Name: "clob_heartbeat", Checker: heartbeat},
	}
	if quarantineChecker != nil {
		readinessChecks = append(readinessChecks,
			readiness.NamedChecker{Name: "account_quarantine", Checker: quarantineChecker},
		)
	}
	if decisionRunner != nil {
		readinessChecks = append(readinessChecks, readiness.NamedChecker{Name: "decision_cycle", Checker: decisionRunner})
	}
	combinedReadiness, err := readiness.NewAll(readinessChecks...)
	if err != nil {
		return nil, err
	}
	// Activating the dead-man switch can cancel pre-existing orders, so it is
	// intentionally the final fallible startup action after reconciliation,
	// the initial operations snapshot, and decision-cycle composition.
	if err := heartbeat.Start(ctx); err != nil {
		return nil, fmt.Errorf("start Polymarket CLOB heartbeat: %w", err)
	}
	if err := heartbeat.Check(ctx); err != nil {
		return nil, fmt.Errorf("verify initial Polymarket CLOB heartbeat freshness: %w", err)
	}
	logger.Info("Polymarket live startup reconciliation passed",
		"accounts", len(reconciliationAccountIDs),
		"quarantined_accounts", len(quarantinedAccountIDs),
	)
	return &liveRuntime{
		repository: repository, execution: executionService,
		reconciliation: reconciliationService, readiness: combinedReadiness,
		heartbeat: heartbeat, runner: runner, operations: operations, decisionRunner: decisionRunner,
		activeAccounts: append([]string(nil), reconciliationAccountIDs...),
	}, nil
}

func validateCurrentLiveWallet67Release(configured, quarantined []string) error {
	if err := requireExactLiveAccountSet(
		"wallet file", configured, []string{"main", "wallet-1", "wallet-6", "wallet-7"},
	); err != nil {
		return err
	}
	if len(quarantined) != 0 {
		return fmt.Errorf("this release requires an empty submission-disabled account list")
	}
	return nil
}

func currentLiveWallet67Authorizations(
	activeAccountIDs []string,
) ([]postgresadapter.ExpectedActiveExecutionAccount, error) {
	routes := map[string]postgresadapter.ExpectedActiveExecutionAccount{
		"main": {
			ExecutionAccountID: "main",
			ModelID:            "echo",
			StrategyID:         domain.StrategyIDMultfactorV2,
		},
		"wallet-1": {
			ExecutionAccountID: "wallet-1",
			ModelID:            "echo",
			StrategyID:         domain.StrategyIDMultfactorV1,
		},
		"wallet-6": {
			ExecutionAccountID: "wallet-6",
			ModelID:            "gemini_masked",
			StrategyID:         domain.StrategyIDMultfactorV1,
		},
		"wallet-7": {
			ExecutionAccountID: "wallet-7",
			ModelID:            "gemini_masked",
			StrategyID:         domain.StrategyIDMultfactorV2,
		},
	}
	result := make([]postgresadapter.ExpectedActiveExecutionAccount, 0, len(activeAccountIDs))
	seen := make(map[string]struct{}, len(activeAccountIDs))
	for _, raw := range activeAccountIDs {
		accountID := strings.TrimSpace(raw)
		route, exists := routes[accountID]
		if !exists {
			return nil, fmt.Errorf("execution account %q has no current live route", accountID)
		}
		if _, duplicate := seen[accountID]; duplicate {
			return nil, fmt.Errorf("active execution account %q is duplicated", accountID)
		}
		seen[accountID] = struct{}{}
		result = append(result, route)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("current live release requires at least one active execution account")
	}
	return result, nil
}

func requireExactLiveAccountSet(label string, actual, expected []string) error {
	actualSet := make(map[string]struct{}, len(actual))
	for index, raw := range actual {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return fmt.Errorf("%s execution account %d is empty", label, index)
		}
		if _, duplicate := actualSet[accountID]; duplicate {
			return fmt.Errorf("%s contains duplicate execution account %q", label, accountID)
		}
		actualSet[accountID] = struct{}{}
	}
	expectedSet := make(map[string]struct{}, len(expected))
	for _, accountID := range expected {
		expectedSet[accountID] = struct{}{}
	}
	if len(actualSet) != len(expectedSet) {
		return fmt.Errorf("%s must contain exactly execution accounts %v", label, expected)
	}
	for accountID := range expectedSet {
		if _, exists := actualSet[accountID]; !exists {
			return fmt.Errorf("%s must contain exactly execution accounts %v", label, expected)
		}
	}
	return nil
}

func preflightLiveAccount(
	ctx context.Context,
	executionAccountID string,
	quarantined bool,
	client liveAccountProbeClient,
	eligibility polymarket.GeographicEligibilityChecker,
) (liveAccountPreflightResult, error) {
	probe, err := client.ProbeAccount(ctx, executionAccountID)
	if err != nil {
		return liveAccountPreflightResult{}, fmt.Errorf(
			"execution account %q authenticated CLOB preflight: %w", executionAccountID, err,
		)
	}
	result := liveAccountPreflightResult{account: probe}
	if quarantined {
		if probe.OpenOrderCount != 0 {
			return liveAccountPreflightResult{}, fmt.Errorf(
				"quarantined execution account %q has %d open CLOB order(s); cancel them before startup",
				executionAccountID, probe.OpenOrderCount,
			)
		}
		return result, nil
	}

	funding, err := client.ProbeFunding(ctx, executionAccountID)
	if err != nil {
		return liveAccountPreflightResult{}, fmt.Errorf(
			"execution account %q CLOB funding preflight: %w", executionAccountID, err,
		)
	}
	result.funding = &funding
	if !funding.CollateralBalancePositive {
		return liveAccountPreflightResult{}, fmt.Errorf(
			"execution account %q has no positive CLOB pUSD collateral balance", executionAccountID,
		)
	}
	if !funding.RequiredAllowancesPositive {
		return liveAccountPreflightResult{}, fmt.Errorf(
			"execution account %q is missing a positive pUSD allowance for a required CLOB V2 exchange", executionAccountID,
		)
	}
	placement, err := eligibility.Check(ctx, executionAccountID)
	if err != nil {
		return liveAccountPreflightResult{}, fmt.Errorf(
			"execution account %q placement eligibility preflight: %w", executionAccountID, err,
		)
	}
	result.eligibility = &placement
	if placement.Blocked {
		return liveAccountPreflightResult{}, fmt.Errorf(
			"execution account %q is not eligible to place CLOB orders (%s)", executionAccountID, placement.Reason,
		)
	}
	return result, nil
}

func partitionReconciliationAccounts(configured, quarantined []string) ([]string, []string, error) {
	configuredSet := make(map[string]struct{}, len(configured))
	active := make([]string, 0, len(configured))
	for index, raw := range configured {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return nil, nil, fmt.Errorf("configured execution account %d is empty", index)
		}
		if _, duplicate := configuredSet[accountID]; duplicate {
			return nil, nil, fmt.Errorf("configured execution account %q is duplicated", accountID)
		}
		configuredSet[accountID] = struct{}{}
	}
	quarantinedSet := make(map[string]struct{}, len(quarantined))
	normalizedQuarantined := make([]string, 0, len(quarantined))
	for index, raw := range quarantined {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return nil, nil, fmt.Errorf("quarantined execution account %d is empty", index)
		}
		if _, exists := configuredSet[accountID]; !exists {
			return nil, nil, fmt.Errorf("quarantined execution account %q is not configured", accountID)
		}
		if _, duplicate := quarantinedSet[accountID]; duplicate {
			return nil, nil, fmt.Errorf("quarantined execution account %q is duplicated", accountID)
		}
		quarantinedSet[accountID] = struct{}{}
		normalizedQuarantined = append(normalizedQuarantined, accountID)
	}
	for _, raw := range configured {
		accountID := strings.TrimSpace(raw)
		if _, excluded := quarantinedSet[accountID]; !excluded {
			active = append(active, accountID)
		}
	}
	if len(active) == 0 {
		return nil, nil, fmt.Errorf("account quarantine cannot exclude every configured reconciliation account")
	}
	return active, normalizedQuarantined, nil
}

// noRedirectHTTPClient 禁止把请求头或供应商 URL 中的凭证重放到重定向目标。
func noRedirectHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// validateStartupReconciliation confirms that every configured account was
// observed during startup. ATTENTION_REQUIRED is a durable, account-local
// state: global readiness remains degraded and CheckAccount blocks placement
// for that wallet, while unrelated reconciled wallets may continue trading.
func validateStartupReconciliation(result reconciliation.SweepResult, expectedAccounts int) error {
	if len(result.Runs) != expectedAccounts {
		return fmt.Errorf("live startup reconciliation returned %d account run(s), want %d", len(result.Runs), expectedAccounts)
	}
	if err := errors.Join(result.Errors...); err != nil {
		return fmt.Errorf("live startup reconciliation failed: %w", err)
	}
	for _, run := range result.Runs {
		switch run.Run.Status {
		case domain.ReconciliationRunCompleted, domain.ReconciliationRunAttentionRequired:
			continue
		default:
			return fmt.Errorf(
				"execution account %q startup reconciliation failed closed (status %s, issues %d)",
				run.Run.ExecutionAccountID, run.Run.Status, len(run.Issues),
			)
		}
	}
	return nil
}

// startBackground 启动 heartbeat、对账、只读快照和可选决策周期后台循环。
func (runtime *liveRuntime) startBackground(ctx context.Context, logger *slog.Logger) error {
	runnerReady := make(chan struct{})
	runnerErrors := make(chan error, 1)
	go func() {
		runnerErrors <- runtime.runner.RunAfterStartupReady(ctx, runnerReady)
	}()
	select {
	case <-runnerReady:
	case err := <-runnerErrors:
		return fmt.Errorf("start Polymarket reconciliation loop: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
	// Heartbeat has already completed its synchronous first beat during live
	// composition. Keep the dead-man loop alive before any optional decision
	// recovery can spend time reconciling durable intent deliveries.
	go func() {
		if err := runtime.heartbeat.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("Polymarket CLOB heartbeat loop stopped", "error", err)
		}
	}()
	var decisionErrors <-chan error
	if runtime.decisionRunner != nil {
		decisionReady := make(chan struct{})
		errorsChannel := make(chan error, 1)
		decisionErrors = errorsChannel
		go func() {
			errorsChannel <- runtime.decisionRunner.RunReady(ctx, decisionReady)
		}()
		select {
		case <-decisionReady:
		case err := <-errorsChannel:
			return fmt.Errorf("start decision cycle loop: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	go func() {
		if err := <-runnerErrors; err != nil && ctx.Err() == nil {
			logger.Error("Polymarket reconciliation loop stopped", "error", err)
		}
	}()
	go func() {
		if err := runtime.operations.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("live operations snapshot loop stopped", "error", err)
		}
	}()
	if decisionErrors != nil {
		go func() {
			if err := <-decisionErrors; err != nil && ctx.Err() == nil {
				logger.Error("decision cycle loop stopped", "error", err)
			}
		}()
	}
	return nil
}

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketgamma"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
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

	accountIDs := make([]string, 0, len(accounts))
	expectedBindings := make([]postgresadapter.ExpectedExecutionAccount, 0, len(accounts))
	for _, account := range accounts {
		probe, probeErr := tradingClient.ProbeAccount(ctx, account.ExecutionAccountID)
		if probeErr != nil {
			return nil, fmt.Errorf("execution account %q authenticated CLOB preflight: %w", account.ExecutionAccountID, probeErr)
		}
		funding, fundingErr := tradingClient.ProbeFunding(ctx, account.ExecutionAccountID)
		if fundingErr != nil {
			return nil, fmt.Errorf("execution account %q CLOB funding preflight: %w", account.ExecutionAccountID, fundingErr)
		}
		if !funding.CollateralBalancePositive {
			return nil, fmt.Errorf("execution account %q has no positive CLOB pUSD collateral balance", account.ExecutionAccountID)
		}
		if !funding.RequiredAllowancesPositive {
			return nil, fmt.Errorf("execution account %q is missing a positive pUSD allowance for a required CLOB V2 exchange", account.ExecutionAccountID)
		}
		placement, placementErr := eligibility.Check(ctx, account.ExecutionAccountID)
		if placementErr != nil {
			return nil, fmt.Errorf("execution account %q placement eligibility preflight: %w", account.ExecutionAccountID, placementErr)
		}
		if placement.Blocked {
			return nil, fmt.Errorf("execution account %q is not eligible to place CLOB orders (%s)", account.ExecutionAccountID, placement.Reason)
		}
		accountIDs = append(accountIDs, account.ExecutionAccountID)
		expectedBindings = append(expectedBindings, postgresadapter.ExpectedExecutionAccount{
			ExecutionAccountID: account.ExecutionAccountID,
			WalletAddress:      probe.FunderAddress,
		})
		logger.Info("Polymarket live account preflight passed",
			"execution_account_id", account.ExecutionAccountID,
			"open_orders", probe.OpenOrderCount,
			"required_v2_allowances_positive", funding.RequiredAllowancesPositive,
			"eligibility_reason", placement.Reason,
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
		Accounts:    accountIDs,
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
		RequirePreparedPlacement: true,
		EntrySubmissionDisabled:  cfg.DecisionCycle.EntrySubmissionDisabled,
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
	})
	if err != nil {
		return nil, err
	}
	runner, err := reconciliation.NewRunner(reconciliation.RunnerParams{
		Service:  reconciliationService,
		Accounts: accountIDs,
		Interval: cfg.Polymarket.ReconciliationInterval,
	})
	if err != nil {
		return nil, err
	}
	if err := reconciliationVenue.Bind(runner); err != nil {
		return nil, err
	}
	if err := triggerBridge.Bind(runner); err != nil {
		return nil, err
	}

	startupSweep := runner.Sweep(ctx, domain.ReconciliationTriggerStartup)
	if err := validateStartupReconciliation(startupSweep, len(accountIDs)); err != nil {
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
		Accounts: accountIDs, VenueName: "Polymarket CLOB", StartedAt: startedAt,
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
	logger.Info("Polymarket live startup reconciliation passed", "accounts", len(accountIDs))
	return &liveRuntime{
		repository: repository, execution: executionService,
		reconciliation: reconciliationService, readiness: combinedReadiness,
		heartbeat: heartbeat, runner: runner, operations: operations, decisionRunner: decisionRunner,
	}, nil
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

// validateStartupReconciliation 确认所有配置账户均成功完成启动对账。
func validateStartupReconciliation(result reconciliation.SweepResult, expectedAccounts int) error {
	if len(result.Runs) != expectedAccounts {
		return fmt.Errorf("live startup reconciliation returned %d account run(s), want %d", len(result.Runs), expectedAccounts)
	}
	if err := errors.Join(result.Errors...); err != nil {
		return fmt.Errorf("live startup reconciliation failed: %w", err)
	}
	for _, run := range result.Runs {
		if run.Run.Status != domain.ReconciliationRunCompleted {
			return fmt.Errorf(
				"execution account %q startup reconciliation requires attention (status %s, issues %d)",
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

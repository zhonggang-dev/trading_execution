package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/adapter/risk"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/edgedistribution"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/ordercoordinator"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
	"github.com/UniPat-AI/trading_execution/internal/service/tradehistory"
	"github.com/UniPat-AI/trading_execution/internal/transport/httpapi"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// main 启动当前命令并在失败时记录错误后退出。
func main() {
	if err := run(); err != nil {
		slog.Error("Trading Execution stopped", "error", err)
		os.Exit(1)
	}
}

// run 加载配置并装配依赖后运行支持优雅退出的 HTTP 服务。
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.App.LogLevel}))
	slog.SetDefault(logger)

	database, err := openDatabase(cfg)
	if err != nil {
		return err
	}
	if database != nil {
		defer database.Close()
	}

	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var databaseReadiness *postgresadapter.HealthChecker
	if database != nil {
		databaseReadiness, err = postgresadapter.NewHealthChecker(database)
		if err != nil {
			return err
		}
		readinessContext, cancel := context.WithTimeout(context.Background(), cfg.Database.ConnectTimeout)
		readinessErr := databaseReadiness.Check(readinessContext)
		cancel()
		if readinessErr != nil {
			return fmt.Errorf("database readiness check: %w", readinessErr)
		}
	}
	guard, err := risk.NewStaticGuard(risk.StaticGuardParams{
		AllowMarketOrders: cfg.Execution.AllowMarketOrders,
	})
	if err != nil {
		return err
	}

	var (
		repository            port.OrderRepository
		executionService      *execution.Service
		readiness             readinessChecker
		reconciliationService *reconciliation.Service
		live                  *liveRuntime
	)
	if cfg.Execution.Mode == "live" {
		startupContext, cancel := context.WithTimeout(rootContext, cfg.Polymarket.StartupTimeout)
		live, err = buildLiveRuntime(buildLiveRuntimeParams{
			ctx: startupContext, cfg: cfg, database: database,
			databaseReadiness: databaseReadiness, guard: guard, logger: logger,
		})
		cancel()
		if err != nil {
			return err
		}
		repository = live.repository
		executionService = live.execution
		readiness = live.readiness
		reconciliationService = live.reconciliation
	} else {
		repository, executionService, err = buildPaperRuntime(cfg, database, guard)
		if err != nil {
			return err
		}
		readiness = databaseReadiness
	}
	tradeHistoryService, err := buildTradeHistoryService(database)
	if err != nil {
		return err
	}
	edgeDistributionService, err := buildEdgeDistributionService(database)
	if err != nil {
		return err
	}
	httpParams := httpapi.Params{
		Service:          executionService,
		TradeHistory:     tradeHistoryService,
		EdgeDistribution: edgeDistributionService,
		Logger:           logger,
		APIToken:         cfg.HTTP.APIToken,
		JobToken:         cfg.HTTP.JobToken,
		ReadOnlyToken:    cfg.HTTP.ReadOnlyToken,
	}
	if readiness != nil {
		httpParams.Readiness = readiness
	}
	if reconciliationService != nil {
		httpParams.Reconciliation = reconciliationService
	}
	if live != nil {
		httpParams.LiveOperations = live.operations
	}
	httpAPI, err := httpapi.New(httpParams)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           httpAPI.Handler(),
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
	}

	if live != nil {
		if err := live.startBackground(rootContext, logger); err != nil {
			return err
		}
	}

	if database != nil {
		var coordinatorAccounts []string
		if live != nil {
			coordinatorAccounts = live.activeAccounts
		}
		coordinator, err := ordercoordinator.New(ordercoordinator.Params{
			Repository: repository, Execution: executionService,
			PollInterval: cfg.Execution.CoordinatorInterval,
			BatchSize:    cfg.Execution.CoordinatorBatchSize,
			Accounts:     coordinatorAccounts,
		})
		if err != nil {
			return err
		}
		go runOrderCoordinator(orderCoordinatorRunParams{
			ctx: rootContext, logger: logger, coordinator: coordinator,
			interval: cfg.Execution.CoordinatorInterval, continuous: cfg.Execution.Mode == "live",
		})
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("Trading Execution started",
			"app", cfg.App.Name,
			"environment", cfg.App.Env,
			"address", cfg.HTTP.Address,
			"mode", cfg.Execution.Mode,
			"venue", cfg.Execution.Venue,
		)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-rootContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return err
		}
		logger.Info("Trading Execution shutdown completed")
		return nil
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type readinessChecker interface {
	Check(context.Context) error
}

// buildPaperRuntime 装配确定性的模拟交易链路；配置数据库时仍持久化订单和预占。
func buildPaperRuntime(cfg config.Config, database *sql.DB, guard port.Guard) (port.OrderRepository, *execution.Service, error) {
	var repository port.OrderRepository = memory.NewOrderRepository()
	var reservations port.AssetReservationManager = paper.NewReservationManager()
	if database != nil {
		var err error
		repository, err = postgresadapter.NewOrderRepository(database)
		if err != nil {
			return nil, nil, err
		}
		reservations, err = postgresadapter.NewReservationManager(postgresadapter.ReservationManagerParams{
			DB: database,
			// 模拟成交没有交易所费用，因此保持上线前的零费率预占行为。
			MaxBuyFeeRateBPS: "0",
		})
		if err != nil {
			return nil, nil, err
		}
	}
	venue := paper.NewVenue(cfg.Execution.Venue)
	service, err := execution.New(execution.Params{
		Repository:              repository,
		Venue:                   venue,
		Guard:                   guard,
		MarketValidator:         paper.NewMarketValidator(),
		Reservations:            reservations,
		EntrySubmissionDisabled: cfg.DecisionCycle.EntrySubmissionDisabled,
		EntryDisabledAccounts:   cfg.DecisionCycle.EntryDisabledAccounts,
		// 模拟交易所同步返回结果，不会产生延迟出现的外部成交。
		ImmediateCancelFinality: true,
	})
	if err != nil {
		return nil, nil, err
	}
	return repository, service, nil
}

// openDatabase 打开并验证所有持久化仓储共享的 PostgreSQL 连接池。
func openDatabase(cfg config.Config) (*sql.DB, error) {
	if cfg.Database.URL == "" {
		return nil, nil
	}
	database, err := sql.Open("pgx", cfg.Database.URL)
	if err != nil {
		return nil, fmt.Errorf("open execution database: %w", err)
	}
	database.SetMaxOpenConns(16)
	database.SetMaxIdleConns(8)
	database.SetConnMaxLifetime(30 * time.Minute)
	database.SetConnMaxIdleTime(5 * time.Minute)
	connectContext, cancel := context.WithTimeout(context.Background(), cfg.Database.ConnectTimeout)
	defer cancel()
	if err := database.PingContext(connectContext); err != nil {
		database.Close()
		return nil, fmt.Errorf("connect execution database: %w", err)
	}
	return database, nil
}

// buildTradeHistoryService 使用订单和账本写入所依赖的同一权威数据库构建成交历史服务。
func buildTradeHistoryService(database *sql.DB) (*tradehistory.Service, error) {
	if database == nil {
		return tradehistory.New(memory.NewTradeHistoryRepository())
	}
	repository, err := postgresadapter.NewTradeHistoryRepository(database)
	if err != nil {
		return nil, err
	}
	service, err := tradehistory.New(repository)
	if err != nil {
		return nil, err
	}
	return service, nil
}

// buildEdgeDistributionService 使用执行审计的冻结策略输入构建 Edge 分布服务。
func buildEdgeDistributionService(database *sql.DB) (*edgedistribution.Service, error) {
	if database == nil {
		return nil, nil
	}
	repository, err := postgresadapter.NewEdgeDistributionRepository(database)
	if err != nil {
		return nil, err
	}
	return edgedistribution.New(repository, nil)
}

// orderCoordinatorRunParams 收拢订单协调器后台循环参数。
type orderCoordinatorRunParams struct {
	ctx         context.Context
	logger      *slog.Logger
	coordinator *ordercoordinator.Coordinator
	interval    time.Duration
	continuous  bool
}

// runOrderCoordinator 启动订单恢复扫描，并在实盘模式持续刷新开放或不确定订单。
func runOrderCoordinator(params orderCoordinatorRunParams) {
	ticker := time.NewTicker(params.interval)
	defer ticker.Stop()
	for {
		result := params.coordinator.Sweep(params.ctx)
		for _, err := range result.Errors {
			if !errors.Is(err, context.Canceled) {
				params.logger.Error("order coordinator sweep failed", "error", err)
			}
		}
		if !params.continuous {
			return
		}
		select {
		case <-params.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

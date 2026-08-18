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
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
	"github.com/UniPat-AI/trading_execution/internal/service/ordercoordinator"
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

	var repository port.OrderRepository = memory.NewOrderRepository()
	var reservations port.AssetReservationManager = paper.NewReservationManager()
	var readiness *postgresadapter.HealthChecker
	if database != nil {
		repository, err = postgresadapter.NewOrderRepository(database)
		if err != nil {
			return err
		}
		reservations, err = postgresadapter.NewReservationManager(postgresadapter.ReservationManagerParams{DB: database})
		if err != nil {
			return err
		}
		readiness, err = postgresadapter.NewHealthChecker(database)
		if err != nil {
			return err
		}
		readinessContext, cancel := context.WithTimeout(context.Background(), cfg.Database.ConnectTimeout)
		readinessErr := readiness.Check(readinessContext)
		cancel()
		if readinessErr != nil {
			return fmt.Errorf("database readiness check: %w", readinessErr)
		}
	}
	venue := paper.NewVenue(cfg.Execution.Venue)
	guard, err := risk.NewStaticGuard(risk.StaticGuardParams{
		AllowMarketOrders: cfg.Execution.AllowMarketOrders,
		MaxOrderSize:      cfg.Execution.MaxOrderSize,
		MaxOrderNotional:  cfg.Execution.MaxOrderNotional,
	})
	if err != nil {
		return err
	}
	executionService, err := execution.New(execution.Params{
		Repository:      repository,
		Venue:           venue,
		Guard:           guard,
		MarketValidator: paper.NewMarketValidator(),
		Reservations:    reservations,
		// The paper venue is synchronous and cannot produce late external fills.
		// Live venues must keep this false and wait for reconciliation finality.
		ImmediateCancelFinality: cfg.Execution.Mode == "paper",
	})
	if err != nil {
		return err
	}
	tradeHistoryService, err := buildTradeHistoryService(database)
	if err != nil {
		return err
	}
	httpParams := httpapi.Params{
		Service:      executionService,
		TradeHistory: tradeHistoryService,
		Logger:       logger,
		APIToken:     cfg.HTTP.APIToken,
		JobToken:     cfg.HTTP.JobToken,
	}
	if readiness != nil {
		httpParams.Readiness = readiness
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

	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if database != nil {
		coordinator, err := ordercoordinator.New(ordercoordinator.Params{
			Repository: repository, Execution: executionService,
			PollInterval: cfg.Execution.CoordinatorInterval,
			BatchSize:    cfg.Execution.CoordinatorBatchSize,
		})
		if err != nil {
			return err
		}
		go runOrderCoordinator(
			rootContext, logger, coordinator, cfg.Execution.CoordinatorInterval,
			cfg.Execution.Mode == "live",
		)
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

// openDatabase opens and validates the single PostgreSQL pool shared by all
// durable repositories. Production configuration validation requires it.
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

// buildTradeHistoryService uses the same authoritative database as order and
// accounting writes; local mode keeps the explicit empty in-memory view.
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

// runOrderCoordinator always performs one startup recovery scan. Live mode
// then continues refreshing ambiguous/open venue orders; paper mode stops
// after recovery because its deterministic venue has no external state to
// poll and repeated observations would only grow the audit log.
func runOrderCoordinator(
	ctx context.Context,
	logger *slog.Logger,
	coordinator *ordercoordinator.Coordinator,
	interval time.Duration,
	continuous bool,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result := coordinator.Sweep(ctx)
		for _, err := range result.Errors {
			if !errors.Is(err, context.Canceled) {
				logger.Error("order coordinator sweep failed", "error", err)
			}
		}
		if !continuous {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

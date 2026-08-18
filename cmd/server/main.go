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

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/adapter/paper"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/adapter/risk"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/service/execution"
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

	repository := memory.NewOrderRepository()
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
		Reservations:    paper.NewReservationManager(),
	})
	if err != nil {
		return err
	}
	tradeHistoryService, database, err := buildTradeHistoryService(cfg)
	if err != nil {
		return err
	}
	if database != nil {
		defer database.Close()
	}
	httpAPI, err := httpapi.New(httpapi.Params{
		Service:      executionService,
		TradeHistory: tradeHistoryService,
		Logger:       logger,
		APIToken:     cfg.HTTP.APIToken,
		JobToken:     cfg.HTTP.JobToken,
	})
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

	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
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

// buildTradeHistoryService 根据数据库配置装配纸交易或 PostgreSQL 交易历史服务。
func buildTradeHistoryService(cfg config.Config) (*tradehistory.Service, *sql.DB, error) {
	if cfg.Database.URL == "" {
		service, err := tradehistory.New(memory.NewTradeHistoryRepository())
		return service, nil, err
	}
	database, err := sql.Open("pgx", cfg.Database.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("open trade history database: %w", err)
	}
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(4)
	connectContext, cancel := context.WithTimeout(context.Background(), cfg.Database.ConnectTimeout)
	defer cancel()
	if err := database.PingContext(connectContext); err != nil {
		database.Close()
		return nil, nil, fmt.Errorf("connect trade history database: %w", err)
	}
	repository, err := postgresadapter.NewTradeHistoryRepository(database)
	if err != nil {
		database.Close()
		return nil, nil, err
	}
	service, err := tradehistory.New(repository)
	if err != nil {
		database.Close()
		return nil, nil, err
	}
	return service, database, nil
}

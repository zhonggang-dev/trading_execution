package main

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/adapter/predictioninfra"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	pm "github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
	"github.com/UniPat-AI/trading_execution/internal/service/tradingmonitor"
)

// buildPipelineMonitor only installs observation. It does not enter readiness
// or alter execution gates; a notifier or database observation failure cannot
// stop trading or cause an order to be retried.
func buildPipelineMonitor(cfg config.Config, db *sql.DB, live *liveRuntime, started time.Time, logger *slog.Logger) (*pm.Service, error) {
	if cfg.PipelineMonitorError != "" {
		return unavailablePipelineMonitor(logger)
	}
	if !cfg.PipelineMonitor.Enabled {
		return nil, nil
	}
	p := tradingmonitor.Params{
		Enabled: cfg.DecisionCycle.Enabled, SubmissionEnabled: cfg.DecisionCycle.OrderSubmissionEnabled,
		StartedAt: started, Interval: cfg.DecisionCycle.Interval, MaxStartLateness: cfg.DecisionCycle.MaxStartLateness,
		CycleTimeout: cfg.DecisionCycle.Timeout, PredictionLookback: cfg.DecisionCycle.PredictionLookback,
		SourceModes: cfg.DecisionCycle.PredictionSourceModes,
	}
	if p.Interval <= 0 {
		p.Interval = 10 * time.Minute
	}
	if p.MaxStartLateness <= 0 {
		p.MaxStartLateness = 30 * time.Second
	}
	if p.CycleTimeout <= 0 {
		p.CycleTimeout = 8 * time.Minute
	}
	if p.PredictionLookback <= 0 {
		p.PredictionLookback = 3 * time.Hour
	}
	for _, raw := range cfg.DecisionCycle.Bindings {
		b := raw.Normalize()
		item := tradingmonitor.Binding{StrategyExecutionBinding: b,
			SubmissionDisabled: containsMonitorAccount(cfg.DecisionCycle.SubmissionDisabledAccounts, b.ExecutionAccountID),
			EntryDisabled:      cfg.DecisionCycle.EntrySubmissionDisabled || containsMonitorAccount(cfg.DecisionCycle.EntryDisabledAccounts, b.ExecutionAccountID),
			StrategyDisabled:   containsMonitorAccount(cfg.DecisionCycle.StrategyDisabledAccounts, b.ExecutionAccountID),
		}
		if live != nil && cfg.DecisionCycle.KalshiStrategyInputEnabled {
			if route, ok := live.execution.(interface {
				KalshiInputEnabled(domain.StrategyExecutionContext) bool
			}); ok {
				item.KalshiEnabled = route.KalshiInputEnabled(domain.StrategyExecutionContext{ModelID: b.ModelID, StrategyID: b.StrategyID, ExecutionAccountID: b.ExecutionAccountID})
			}
		}
		p.Bindings = append(p.Bindings, item)
	}
	if live != nil {
		if live.decisionRunner != nil {
			p.RunnerSnapshot = live.decisionRunner.Snapshot
		}
		if live.heartbeat != nil {
			p.HeartbeatCheck = live.heartbeat.CheckAccount
		}
		if live.runner != nil {
			p.ReconciliationCheck = live.runner.CheckAccount
		}
	}
	if cfg.DecisionCycle.Enabled {
		source, err := predictioninfra.New(predictioninfra.Params{BaseURL: cfg.DecisionCycle.PredictionInfraBaseURL, BearerToken: cfg.DecisionCycle.PredictionInfraToken, HTTPClient: noRedirectHTTPClient(cfg.PipelineMonitor.HTTPTimeout)})
		if err != nil {
			return nil, err
		}
		p.Predictions = source
	}
	if db != nil {
		source, err := postgresadapter.NewPipelineMonitorSource(db, p.CycleTimeout)
		if err != nil {
			return nil, err
		}
		p.Evidence = source
	}
	collector, err := tradingmonitor.New(p)
	if err != nil {
		return nil, err
	}
	return pm.Build(pm.BuildParams{Config: cfg.PipelineMonitor, Component: "trading-execution", Collector: collector, Logger: logger, HTTPClient: noRedirectHTTPClient(cfg.PipelineMonitor.HTTPTimeout)})
}

func unavailablePipelineMonitor(logger *slog.Logger) (*pm.Service, error) {
	cfg, _ := pm.LoadConfig(func(string) string { return "" })
	s, err := pm.Build(pm.BuildParams{Config: cfg, Component: "trading-execution", Logger: logger, Collector: pm.CollectFunc(func(context.Context, time.Time) ([]pm.Check, error) { return nil, nil })})
	if err == nil {
		s.ReportProblem(pm.Check{ID: "monitor.configuration", Scope: "trading-execution", Status: pm.Unknown, Summary: "链路监控配置或装配不可用", Details: "监控故障不改变交易就绪状态；请检查监控独立配置"})
	}
	return s, err
}

func containsMonitorAccount(accounts []string, id string) bool {
	for _, a := range accounts {
		if a == id {
			return true
		}
	}
	return false
}

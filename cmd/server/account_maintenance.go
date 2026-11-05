package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
)

const (
	// accountMaintenanceProbeInterval is how often a maintenance account's
	// read-only preflight is re-run (design D1).
	accountMaintenanceProbeInterval = time.Minute
	// accountMaintenanceProbeTimeout bounds one account's preflight requests.
	accountMaintenanceProbeTimeout = 30 * time.Second
)

// enterAccountMaintenance records a reason and logs it once per new reason.
func enterAccountMaintenance(logger *slog.Logger, registry *accountmaintenance.Registry, accountID, reason, detail string) {
	if registry == nil {
		return
	}
	if registry.Enter(accountID, reason, detail) && logger != nil {
		logger.Warn("execution account entered maintenance",
			"execution_account_id", accountID, "reason", reason, "detail", detail)
	}
}

// exitAccountMaintenance clears a reason and logs when the account has no
// reason left.
func exitAccountMaintenance(logger *slog.Logger, registry *accountmaintenance.Registry, accountID, reason string) {
	if !registry.Exit(accountID, reason) || logger == nil {
		return
	}
	logger.Info("execution account maintenance reason cleared", "execution_account_id", accountID, "reason", reason)
	if _, still := registry.MaintenanceReason(accountID); !still {
		logger.Info("execution account left maintenance", "execution_account_id", accountID)
	}
}

type reconciliationResultSource interface {
	LastResult(accountID string) (reconciliation.Result, bool)
}

type accountMaintenanceProbeParams struct {
	registry       *accountmaintenance.Registry
	preflight      func(context.Context, string) (liveAccountPreflightResult, error)
	reconciliation reconciliationResultSource
	interval       time.Duration
	callTimeout    time.Duration
	logger         *slog.Logger
}

// accountMaintenanceProbe clears recovered Polymarket maintenance reasons. It
// performs only read-only work: the account preflight (open-order list,
// balance-allowance read, eligibility) and the reconciliation runner's
// in-memory latest result. Kalshi reasons are never cleared here; they need a
// restart after the credential or market-data configuration is repaired.
type accountMaintenanceProbe struct {
	registry       *accountmaintenance.Registry
	preflight      func(context.Context, string) (liveAccountPreflightResult, error)
	reconciliation reconciliationResultSource
	interval       time.Duration
	callTimeout    time.Duration
	logger         *slog.Logger
}

func newAccountMaintenanceProbe(params accountMaintenanceProbeParams) (*accountMaintenanceProbe, error) {
	if params.registry == nil || params.preflight == nil || params.reconciliation == nil {
		return nil, fmt.Errorf("account maintenance probe requires a registry, preflight, and reconciliation results")
	}
	if params.interval == 0 {
		params.interval = accountMaintenanceProbeInterval
	}
	if params.callTimeout == 0 {
		params.callTimeout = accountMaintenanceProbeTimeout
	}
	if params.interval <= 0 || params.callTimeout <= 0 {
		return nil, fmt.Errorf("account maintenance probe timing must be positive")
	}
	if params.logger == nil {
		params.logger = slog.Default()
	}
	return &accountMaintenanceProbe{
		registry: params.registry, preflight: params.preflight, reconciliation: params.reconciliation,
		interval: params.interval, callTimeout: params.callTimeout, logger: params.logger,
	}, nil
}

// Run probes every interval until ctx ends.
func (probe *accountMaintenanceProbe) Run(ctx context.Context) {
	ticker := time.NewTicker(probe.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probe.RunOnce(ctx)
		}
	}
}

// RunOnce evaluates every account currently in maintenance once.
func (probe *accountMaintenanceProbe) RunOnce(ctx context.Context) {
	for _, accountID := range probe.registry.AccountIDs() {
		if ctx.Err() != nil {
			return
		}
		var preflightReasons []string
		reconciliationFailed := false
		for _, entry := range probe.registry.Entries(accountID) {
			switch {
			case accountmaintenance.IsPreflightReason(entry.Reason):
				preflightReasons = append(preflightReasons, entry.Reason)
			case entry.Reason == accountmaintenance.ReasonStartupReconciliationFailed:
				reconciliationFailed = true
			}
		}
		if len(preflightReasons) > 0 {
			probe.probePreflight(ctx, accountID, preflightReasons)
		}
		if reconciliationFailed && probe.reconciliationRecovered(accountID) {
			exitAccountMaintenance(probe.logger, probe.registry, accountID, accountmaintenance.ReasonStartupReconciliationFailed)
		}
	}
}

func (probe *accountMaintenanceProbe) probePreflight(ctx context.Context, accountID string, current []string) {
	callCtx, cancel := context.WithTimeout(ctx, probe.callTimeout)
	_, err := probe.preflight(callCtx, accountID)
	cancel()
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		for _, reason := range current {
			exitAccountMaintenance(probe.logger, probe.registry, accountID, reason)
		}
		return
	}
	var accountErr *liveAccountPreflightError
	if !errors.As(err, &accountErr) {
		// Not an account-local preflight result; keep the existing reasons.
		probe.logger.Warn("execution account maintenance preflight failed",
			"execution_account_id", accountID, "error", err)
		return
	}
	// The failure may have moved to another stage (for example, the CLOB key
	// was repaired but an allowance is still missing): record the new reason
	// before clearing the old ones so the account never leaves maintenance.
	enterAccountMaintenance(probe.logger, probe.registry, accountID, accountErr.reason, err.Error())
	for _, reason := range current {
		if reason != accountErr.reason {
			exitAccountMaintenance(probe.logger, probe.registry, accountID, reason)
		}
	}
}

// reconciliationRecovered reports whether the latest reconciliation of the
// account finished. Account-wide issues and staleness remain gated by the
// existing placement readiness, exactly as for an account that passed startup
// with ATTENTION_REQUIRED.
func (probe *accountMaintenanceProbe) reconciliationRecovered(accountID string) bool {
	result, exists := probe.reconciliation.LastResult(accountID)
	if !exists || result.Run.CompletedAt == nil {
		return false
	}
	switch result.Run.Status {
	case domain.ReconciliationRunCompleted, domain.ReconciliationRunAttentionRequired:
		return true
	default:
		return false
	}
}

// Package kalshisettlement keeps the local ledger of one Kalshi account in step
// with the exchange. Kalshi credits winners and removes settled positions by
// itself, so there is nothing to submit: the service reads the exchange's
// settlement records, has the store apply those that agree exactly with the
// ledger, and reports the rest. It also watches the cash ledger against the
// exchange balance without ever rewriting it.
package kalshisettlement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

type Params struct {
	ExecutionAccountID    string
	Source                port.KalshiSettlementSource
	Store                 port.KalshiSettlementStore
	Interval              time.Duration
	BalanceDriftThreshold domain.Decimal
	Logger                *slog.Logger
	Now                   func() time.Time
}

type Service struct {
	account   string
	source    port.KalshiSettlementSource
	store     port.KalshiSettlementStore
	interval  time.Duration
	threshold domain.Decimal
	logger    *slog.Logger
	now       func() time.Time
}

func New(params Params) (*Service, error) {
	params.ExecutionAccountID = strings.TrimSpace(params.ExecutionAccountID)
	if !strings.HasPrefix(params.ExecutionAccountID, "kalshi:") {
		return nil, fmt.Errorf("Kalshi settlement account %q is not a Kalshi execution account", params.ExecutionAccountID)
	}
	if params.Source == nil || params.Store == nil {
		return nil, fmt.Errorf("Kalshi settlement requires an exchange source and a ledger store")
	}
	if params.Interval < 30*time.Second || params.Interval > time.Hour {
		return nil, fmt.Errorf("Kalshi settlement interval must be between 30s and 1h")
	}
	if sign, err := params.BalanceDriftThreshold.Sign(); err != nil || sign <= 0 {
		return nil, fmt.Errorf("Kalshi balance drift threshold must be positive")
	}
	if params.Logger == nil {
		params.Logger = slog.Default()
	}
	if params.Now == nil {
		params.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		account: params.ExecutionAccountID, source: params.Source, store: params.Store,
		interval: params.Interval, threshold: params.BalanceDriftThreshold,
		logger: params.Logger.With("execution_account_id", params.ExecutionAccountID), now: params.Now,
	}, nil
}

// Run sweeps once immediately and then on every interval until ctx ends.
func (service *Service) Run(ctx context.Context) error {
	if err := service.Sweep(ctx); err != nil && ctx.Err() == nil {
		service.logger.Error("initial Kalshi settlement sweep needs attention", "error", err)
	}
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := service.Sweep(ctx); err != nil && ctx.Err() == nil {
				service.logger.Error("Kalshi settlement sweep needs attention", "error", err)
			}
		}
	}
}

// Sweep applies every settled market the account still holds, then compares
// the cash ledger with the exchange. Settlement runs first so a payout that the
// exchange already credited is not reported as drift.
func (service *Service) Sweep(ctx context.Context) error {
	var failures []error
	tickers, err := service.store.OpenKalshiSettlementTickers(ctx, service.account)
	if err != nil {
		return fmt.Errorf("list open Kalshi markets: %w", err)
	}
	for _, ticker := range tickers {
		if err := service.settle(ctx, ticker); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", ticker, err))
		}
	}
	if err := service.checkBalance(ctx); err != nil {
		failures = append(failures, fmt.Errorf("balance check: %w", err))
	}
	return errors.Join(failures...)
}

func (service *Service) settle(ctx context.Context, ticker string) error {
	records, err := service.source.ListSettlements(ctx, ticker)
	if err != nil {
		return fmt.Errorf("read settlement: %w", err)
	}
	switch len(records) {
	case 0:
		return nil
	case 1:
	default:
		return fmt.Errorf("exchange returned %d settlement records for one market; none applied", len(records))
	}
	outcome, err := service.store.ApplyKalshiSettlement(ctx, service.account, records[0], service.now())
	if err != nil {
		return fmt.Errorf("apply settlement: %w", err)
	}
	switch outcome.Result {
	case domain.KalshiSettlementApplied:
		service.logger.Info("Kalshi settlement applied", "ticker", ticker, "result", records[0].MarketResult,
			"payout", outcome.Payout.String(), "realized_pnl", outcome.RealizedPnL.String())
	case domain.KalshiSettlementManualReview:
		service.logger.Error("Kalshi settlement held for manual review", "ticker", ticker, "reason", outcome.Reason)
	case domain.KalshiSettlementDeferred:
		service.logger.Info("Kalshi settlement deferred", "ticker", ticker, "reason", outcome.Reason)
	}
	return nil
}

func (service *Service) checkBalance(ctx context.Context) error {
	balance, err := service.source.AvailableBalance(ctx)
	if err != nil {
		return fmt.Errorf("read exchange balance: %w", err)
	}
	check, err := service.store.CheckKalshiBalance(ctx, service.account, balance, service.threshold, service.now())
	if err != nil {
		return err
	}
	switch {
	case check.Skipped:
	case check.Drifted:
		service.logger.Warn("Kalshi cash ledger differs from the exchange balance",
			"local_balance", check.LocalBalance.String(), "exchange_balance", check.ExchangeBalance.String(),
			"difference", check.Difference.String(), "threshold", service.threshold.String())
	}
	return nil
}

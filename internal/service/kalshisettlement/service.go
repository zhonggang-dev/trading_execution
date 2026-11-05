// Package kalshisettlement keeps the local ledger of one Kalshi account in step
// with the exchange. Kalshi credits winners and removes settled positions by
// itself, so there is nothing to submit: the service reads the exchange's
// settlement records, has the store apply those that agree exactly with the
// ledger, and reports the rest. It also compares the cash ledger with the
// exchange balance; with BalanceSyncEnabled it books a deposit or withdrawal
// in a verified quiet window (see postgres.SyncKalshiLedger) and checks the
// positions read-only, otherwise it only records drift.
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
	// BalanceSyncEnabled replaces the drift-only check with the running cash
	// sync. PositionCheckEnabled additionally reads the exchange positions in
	// a quiet window and requires them to match before cash is synced; it has
	// no effect without BalanceSyncEnabled. Both default to off, which is the
	// behaviour before migration 0045.
	BalanceSyncEnabled   bool
	PositionCheckEnabled bool
	Logger               *slog.Logger
	Now                  func() time.Time
}

type Service struct {
	account       string
	source        port.KalshiSettlementSource
	store         port.KalshiSettlementStore
	interval      time.Duration
	threshold     domain.Decimal
	syncEnabled   bool
	positionCheck bool
	logger        *slog.Logger
	now           func() time.Time
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
		syncEnabled: params.BalanceSyncEnabled, positionCheck: params.BalanceSyncEnabled && params.PositionCheckEnabled,
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
// the cash ledger with the exchange.
//
// Drift-only mode settles first so a payout the exchange already credited is
// not reported as drift. Sync mode reads the balance (and positions) before any
// settlement record instead: a sweep that then sees any settlement record, or
// fails to read one, does not compare at all, and an applied settlement changes
// the account version, which the locked sync rejects.
func (service *Service) Sweep(ctx context.Context) error {
	if !service.syncEnabled {
		return service.sweepDriftOnly(ctx)
	}
	var failures []error
	snapshot, snapshotErr := service.store.LedgerSnapshot(ctx, service.account)
	balance, balanceErr := service.source.AvailableBalance(ctx)
	observedAt := service.now()
	var positions []domain.KalshiMarketPosition
	positionsErr := ""
	if service.positionCheck && snapshotErr == nil && balanceErr == nil && snapshot.Quiet {
		read, err := service.source.ListPositions(ctx)
		if err != nil {
			positionsErr = err.Error()
			service.logger.Warn("Kalshi position check could not read the exchange positions", "error", err)
		} else {
			positions = read
			if positions == nil {
				positions = []domain.KalshiMarketPosition{}
			}
		}
	}
	tickers, err := service.store.OpenKalshiSettlementTickers(ctx, service.account)
	if err != nil {
		return fmt.Errorf("list open Kalshi markets: %w", err)
	}
	activity, readFailed := false, false
	for _, ticker := range tickers {
		seen, readErr, err := service.settleObserved(ctx, ticker)
		activity = activity || seen
		readFailed = readFailed || readErr
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", ticker, err))
		}
	}
	switch {
	case balanceErr != nil:
		failures = append(failures, fmt.Errorf("balance check: read exchange balance: %w", balanceErr))
	case snapshotErr != nil:
		failures = append(failures, fmt.Errorf("balance check: %w", snapshotErr))
	default:
		result, err := service.store.SyncKalshiLedger(ctx, service.account, domain.KalshiLedgerObservation{
			AccountVersionBefore: snapshot.AccountVersion, ExchangeBalance: balance,
			PositionCheckEnabled: service.positionCheck, Positions: positions, PositionsError: positionsErr,
			SettlementActivity: activity, SettlementReadFailed: readFailed,
			Threshold: service.threshold, ObservedAt: observedAt,
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("balance sync: %w", err))
		} else {
			service.logSync(result)
		}
	}
	return errors.Join(failures...)
}

func (service *Service) sweepDriftOnly(ctx context.Context) error {
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

// skipLogCounts are the consecutive-skip counts that are logged even when the
// reason did not change: the first, about an hour and about a day at 5m.
var skipLogCounts = map[int64]bool{1: true, 12: true, 288: true}

func (service *Service) logSync(result domain.KalshiLedgerSyncResult) {
	switch result.BalanceOutcome {
	case domain.KalshiBalanceSynced:
		service.logger.Info("Kalshi cash synced to the exchange balance", "delta", result.Difference.String(),
			"local_before", result.LocalBalance.String(), "exchange_balance", result.ExchangeBalance.String(),
			"event_id", result.SyncEventID, "first_observed_at", result.FirstObservedAt)
	case domain.KalshiBalanceSkipped:
		if result.SkipReason != result.PreviousSkipReason || skipLogCounts[result.ConsecutiveSkips] {
			service.logger.Info("Kalshi cash sync skipped", "skip_reason", result.SkipReason,
				"consecutive_skips", result.ConsecutiveSkips, "total_skips", result.TotalSkips)
		}
	}
	if result.Drifted && result.BalanceOutcome != domain.KalshiBalanceSynced {
		service.logger.Warn("Kalshi cash ledger differs from the exchange balance",
			"local_balance", result.LocalBalance.String(), "exchange_balance", result.ExchangeBalance.String(),
			"difference", result.Difference.String(), "threshold", service.threshold.String(),
			"outcome", result.BalanceOutcome)
	}
	if result.OpenPositionDrifts != result.PreviousOpenPositionDrifts {
		service.logger.Info("Kalshi position differences changed", "open_position_drifts", result.OpenPositionDrifts,
			"previous_open_position_drifts", result.PreviousOpenPositionDrifts, "positions_outcome", result.PositionsOutcome)
	}
}

// settleObserved settles one market and reports whether the exchange had a
// settlement record for it (seen) or could not be asked (readFailed).
func (service *Service) settleObserved(ctx context.Context, ticker string) (seen, readFailed bool, err error) {
	records, err := service.source.ListSettlements(ctx, ticker)
	if err != nil {
		return false, true, fmt.Errorf("read settlement: %w", err)
	}
	if len(records) == 0 {
		return false, false, nil
	}
	return true, false, service.applyRecords(ctx, ticker, records)
}

func (service *Service) settle(ctx context.Context, ticker string) error {
	records, err := service.source.ListSettlements(ctx, ticker)
	if err != nil {
		return fmt.Errorf("read settlement: %w", err)
	}
	if len(records) == 0 {
		return nil
	}
	return service.applyRecords(ctx, ticker, records)
}

func (service *Service) applyRecords(ctx context.Context, ticker string, records []domain.KalshiSettlement) error {
	if len(records) != 1 {
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

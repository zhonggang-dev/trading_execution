// Package walletdrawdown samples one Polymarket account's trading PnL on a fixed
// slot, measures the drop from the highest PnL inside a trailing window and
// notifies a Feishu chat when it exceeds a threshold.
package walletdrawdown

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
)

// Config holds the tunables; all of them come from the environment.
type Config struct {
	AccountID    string
	Threshold    *big.Rat
	RenotifyStep *big.Rat
	Window       time.Duration
	Slot         time.Duration
}

// Validate rejects configurations that would make the comparison meaningless.
func (c Config) Validate() error {
	if strings.TrimSpace(c.AccountID) == "" {
		return fmt.Errorf("drawdown account is required")
	}
	if c.Threshold == nil || c.Threshold.Sign() <= 0 {
		return fmt.Errorf("drawdown threshold must be positive")
	}
	if c.RenotifyStep == nil || c.RenotifyStep.Sign() <= 0 {
		return fmt.Errorf("drawdown renotify step must be positive")
	}
	if c.Slot < time.Minute || c.Slot > time.Hour {
		return fmt.Errorf("drawdown slot must be between 1m and 1h")
	}
	if c.Window < c.Slot || c.Window > 24*time.Hour {
		return fmt.Errorf("drawdown window must be at least one slot and at most 24h")
	}
	return nil
}

// PositionValue is one position as valued in a sample.
type PositionValue struct {
	TokenID     string         `json:"token_id"`
	ConditionID string         `json:"condition_id"`
	Title       string         `json:"title"`
	Outcome     string         `json:"outcome"`
	Shares      domain.Decimal `json:"shares"`
	Price       domain.Decimal `json:"price"`
	Value       string         `json:"value"`
}

// Sample is one persisted row.
type Sample struct {
	AccountID      string          `json:"execution_account_id"`
	SlotAt         time.Time       `json:"slot_at"`
	SampledAt      time.Time       `json:"sampled_at"`
	PositionsValue string          `json:"positions_value"`
	RealizedPnL    string          `json:"realized_pnl"`
	UnrealizedPnL  string          `json:"unrealized_pnl"`
	TotalPnL       string          `json:"total_pnl"`
	WindowMinutes  int             `json:"window_minutes"`
	PeakPnL        *string         `json:"window_peak_pnl,omitempty"`
	PeakAt         *time.Time      `json:"window_peak_at,omitempty"`
	Drawdown       *string         `json:"drawdown,omitempty"`
	Threshold      string          `json:"threshold_usd"`
	Exceeded       bool            `json:"exceeded"`
	Notified       bool            `json:"notified"`
	Positions      []PositionValue `json:"positions"`
}

// Store persists samples.
type Store interface {
	orderOperationStore
	LoadSample(ctx context.Context, accountID string, slot time.Time) (Sample, bool, error)
	// WindowSamples returns samples with sampled_at >= from and slot_at < before.
	WindowSamples(ctx context.Context, accountID string, from, before time.Time) ([]Sample, error)
	// LastNotifiedDrawdown returns the drawdown of the latest notified sample
	// before `before` that belongs to the current episode, i.e. after the latest
	// sample that was not exceeded. Nil means the episode has no notification.
	LastNotifiedDrawdown(ctx context.Context, accountID string, before time.Time) (*big.Rat, error)
	InsertSample(ctx context.Context, sample Sample) (bool, error)
	MarkNotified(ctx context.Context, accountID string, slot time.Time, notified bool, failure string) error
}

// PnLSource returns a marked snapshot of the account's trading ledger.
type PnLSource interface {
	Query(context.Context, string) (accountcoremetrics.Response, error)
}

// Notifier posts a card.
type Notifier interface {
	SendCard(ctx context.Context, card map[string]any, now time.Time) error
}

// Report is printed by the command after each run.
type Report struct {
	Outcome string       `json:"outcome"`
	Sample  *Sample      `json:"sample,omitempty"`
	Notify  string       `json:"notify"`
	Trade   *TradeReport `json:"trade,omitempty"`
}

// Run takes one sample, stores it and notifies when needed. A nil notifier or
// trader disables that action. A dry run suppresses all writes and actions.
func Run(ctx context.Context, cfg Config, store Store, src PnLSource, notifier Notifier, trader *Trader, now time.Time, dryRun bool) (Report, error) {
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	now = now.UTC()
	slot := now.Truncate(cfg.Slot)
	if existing, found, err := store.LoadSample(ctx, cfg.AccountID, slot); err != nil {
		return Report{}, err
	} else if found {
		report := Report{Outcome: "slot_already_sampled", Sample: &existing, Notify: "none"}
		if !existing.Exceeded || existing.Notified || dryRun || notifier == nil {
			return report, nil
		}
		// A previous attempt stored the sample but failed to deliver: retry.
		return deliver(ctx, cfg, store, notifier, existing, now, report)
	}
	sample, err := takeSample(ctx, cfg, src, slot, now)
	if err != nil {
		return Report{}, err
	}
	history, err := store.WindowSamples(ctx, cfg.AccountID, now.Add(-cfg.Window), slot)
	if err != nil {
		return Report{}, err
	}
	if err = evaluate(cfg, &sample, history); err != nil {
		return Report{}, err
	}
	report := Report{Outcome: "sampled", Sample: &sample, Notify: "none"}
	if dryRun {
		report.Outcome = "dry_run"
		return report, nil
	}
	inserted, err := store.InsertSample(ctx, sample)
	if err != nil {
		return Report{}, err
	}
	if !inserted {
		report.Outcome = "slot_already_sampled"
		return report, nil
	}
	var notifyErr, tradeErr error
	if sample.Exceeded && notifier != nil {
		report, notifyErr = deliverWithHistory(ctx, cfg, store, notifier, sample, history, now, report)
	}
	// Only the process that inserted the sample can start a trade. Re-running
	// a slot can retry Feishu delivery without submitting any new order.
	if trader != nil && startsTradeEpisode(sample, history) {
		trade, err := trader.buy(ctx, store, sample)
		report.Trade, tradeErr = &trade, err
	}
	return report, errors.Join(notifyErr, tradeErr)
}

func deliver(ctx context.Context, cfg Config, store Store, notifier Notifier, sample Sample, now time.Time, report Report) (Report, error) {
	history, err := store.WindowSamples(ctx, cfg.AccountID, sample.SampledAt.Add(-cfg.Window), sample.SlotAt)
	if err != nil {
		return report, err
	}
	return deliverWithHistory(ctx, cfg, store, notifier, sample, history, now, report)
}

func deliverWithHistory(ctx context.Context, cfg Config, store Store, notifier Notifier, sample Sample, history []Sample, now time.Time, report Report) (Report, error) {
	last, err := store.LastNotifiedDrawdown(ctx, cfg.AccountID, sample.SlotAt)
	if err != nil {
		return report, err
	}
	drawdown, err := ratOf(*sample.Drawdown)
	if err != nil {
		return report, err
	}
	if last != nil && drawdown.Cmp(new(big.Rat).Add(last, cfg.RenotifyStep)) < 0 {
		report.Notify = "suppressed_same_episode"
		return report, nil
	}
	card, err := RenderCard(cfg, sample, peakSample(sample, history), last != nil)
	if err != nil {
		return report, err
	}
	sendErr := notifier.SendCard(ctx, card, now)
	failure := ""
	if sendErr != nil {
		failure = sendErr.Error()
	}
	if err := store.MarkNotified(ctx, cfg.AccountID, sample.SlotAt, sendErr == nil, failure); err != nil {
		return report, err
	}
	if sendErr != nil {
		report.Notify = "failed"
		return report, fmt.Errorf("send drawdown notification: %w", sendErr)
	}
	report.Notify = "sent"
	sample.Notified = true
	report.Sample = &sample
	return report, nil
}

func takeSample(ctx context.Context, cfg Config, src PnLSource, slot, now time.Time) (Sample, error) {
	response, err := src.Query(ctx, cfg.AccountID)
	if err != nil {
		return Sample{}, fmt.Errorf("read trading PnL: %w", err)
	}
	if len(response.Items) != 1 || response.Items[0].ExecutionAccountID != cfg.AccountID {
		return Sample{}, fmt.Errorf("trading PnL account %s missing from snapshot", cfg.AccountID)
	}
	metrics := response.Items[0]
	if metrics.ValuationState != "complete" || metrics.TotalPnL == nil || metrics.UnrealizedPnL == nil || metrics.MarketValue == nil {
		return Sample{}, fmt.Errorf("trading PnL valuation incomplete for %s (%d of %d positions priced)", cfg.AccountID, metrics.PricedAssetCount, metrics.AssetCount)
	}
	values := make([]PositionValue, 0, len(metrics.ValuationAssets))
	for _, asset := range metrics.ValuationAssets {
		if asset.Price == nil || asset.MarketValue == nil {
			return Sample{}, fmt.Errorf("trading PnL missing valuation for token %s", asset.TokenID)
		}
		row := PositionValue{
			TokenID: asset.TokenID, ConditionID: asset.ConditionID,
			Shares: domain.Decimal(asset.RemainingShares), Price: domain.Decimal(*asset.Price), Value: *asset.MarketValue,
		}
		if asset.MarketQuestion != nil {
			row.Title = *asset.MarketQuestion
		}
		if asset.OutcomeName != nil {
			row.Outcome = *asset.OutcomeName
		}
		values = append(values, row)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].TokenID < values[j].TokenID })
	return Sample{
		AccountID: cfg.AccountID, SlotAt: slot, SampledAt: now,
		PositionsValue: *metrics.MarketValue, RealizedPnL: metrics.RealizedPnL, UnrealizedPnL: *metrics.UnrealizedPnL,
		TotalPnL: *metrics.TotalPnL, WindowMinutes: int(cfg.Window / time.Minute),
		Threshold: cfg.Threshold.FloatString(2), Positions: values,
	}, nil
}

// evaluate compares only PnL samples within the trailing time window.
func evaluate(cfg Config, sample *Sample, history []Sample) error {
	if len(history) == 0 {
		return nil
	}
	pnl, err := ratOf(sample.TotalPnL)
	if err != nil {
		return err
	}
	peak, peakAt := new(big.Rat).Set(pnl), sample.SampledAt
	for _, h := range history {
		value, err := ratOf(h.TotalPnL)
		if err != nil {
			return fmt.Errorf("history sample %s PnL: %w", h.SlotAt.Format(time.RFC3339), err)
		}
		if value.Cmp(peak) > 0 {
			peak, peakAt = value, h.SampledAt
		}
	}
	drawdown := new(big.Rat).Sub(peak, pnl)
	peakText, drawdownText := money(peak), money(drawdown)
	sample.PeakPnL, sample.PeakAt, sample.Drawdown = &peakText, &peakAt, &drawdownText
	sample.Exceeded = drawdown.Cmp(cfg.Threshold) > 0
	return nil
}

func peakSample(sample Sample, history []Sample) *Sample {
	if sample.PeakAt == nil {
		return nil
	}
	for i := range history {
		if history[i].SampledAt.Equal(*sample.PeakAt) {
			return &history[i]
		}
	}
	return nil
}

// Contributor is a position's mark-to-market change between the window peak
// and now, on the shares held now.
type Contributor struct {
	Title   string
	Outcome string
	Change  *big.Rat
	From    domain.Decimal
	To      domain.Decimal
}

// Contributors returns the largest losers among positions held at both times.
// Shares sold or bought in between move cash, not value, so only the price
// change on the current shares is attributed.
func Contributors(now Sample, peak *Sample, limit int) []Contributor {
	if peak == nil {
		return nil
	}
	before := map[string]PositionValue{}
	for _, p := range peak.Positions {
		before[p.TokenID] = p
	}
	result := []Contributor{}
	for _, p := range now.Positions {
		old, ok := before[p.TokenID]
		if !ok {
			continue
		}
		shares, e1 := ratOf(string(p.Shares))
		priceNow, e2 := ratOf(string(p.Price))
		priceThen, e3 := ratOf(string(old.Price))
		if e1 != nil || e2 != nil || e3 != nil {
			continue
		}
		change := new(big.Rat).Mul(shares, new(big.Rat).Sub(priceNow, priceThen))
		if change.Sign() < 0 {
			result = append(result, Contributor{Title: p.Title, Outcome: p.Outcome, Change: change, From: old.Price, To: p.Price})
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Change.Cmp(result[j].Change) < 0 })
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func ratOf(text string) (*big.Rat, error) {
	value, ok := new(big.Rat).SetString(strings.TrimSpace(text))
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", text)
	}
	return value, nil
}

// money keeps six decimals, the pUSD unit.
func money(value *big.Rat) string { return value.FloatString(6) }

// Package walletdrawdown samples one Polymarket wallet's equity on a fixed
// slot, measures the drop from the highest equity inside a trailing window and
// notifies a Feishu chat when it exceeds a threshold. It is read-only towards
// trading: it never places, cancels or pauses anything.
package walletdrawdown

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
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
	Redeemable  bool           `json:"redeemable"`
	ChainShares bool           `json:"chain_shares,omitempty"`
}

// Sample is one persisted row.
type Sample struct {
	AccountID      string          `json:"execution_account_id"`
	SlotAt         time.Time       `json:"slot_at"`
	SampledAt      time.Time       `json:"sampled_at"`
	CashPUSD       string          `json:"cash_pusd"`
	CashBlock      uint64          `json:"cash_block"`
	PositionsValue string          `json:"positions_value"`
	Equity         string          `json:"equity"`
	WindowMinutes  int             `json:"window_minutes"`
	PeakEquity     *string         `json:"window_peak_equity,omitempty"`
	PeakAt         *time.Time      `json:"window_peak_at,omitempty"`
	Drawdown       *string         `json:"drawdown,omitempty"`
	Threshold      string          `json:"threshold_usd"`
	Exceeded       bool            `json:"exceeded"`
	Notified       bool            `json:"notified"`
	Positions      []PositionValue `json:"positions"`
}

// Store persists samples.
type Store interface {
	WalletAddress(ctx context.Context, accountID string) (string, error)
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

// Sources reads the wallet's current state.
type Sources interface {
	Cash(ctx context.Context, wallet string) (domain.ExternalBalance, error)
	Positions(ctx context.Context, wallet string) ([]polymarketdata.PositionMark, error)
	ChainShares(ctx context.Context, wallet, tokenID string) (domain.Decimal, error)
}

// Notifier posts a card.
type Notifier interface {
	SendCard(ctx context.Context, card map[string]any, now time.Time) error
}

// Report is printed by the command after each run.
type Report struct {
	Outcome string  `json:"outcome"`
	Sample  *Sample `json:"sample,omitempty"`
	Notify  string  `json:"notify"`
}

// Run takes one sample, stores it and notifies when needed. A nil notifier or
// dryRun suppresses delivery; dryRun also suppresses every write.
func Run(ctx context.Context, cfg Config, store Store, src Sources, notifier Notifier, now time.Time, dryRun bool) (Report, error) {
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
	wallet, err := store.WalletAddress(ctx, cfg.AccountID)
	if err != nil {
		return Report{}, err
	}
	sample, err := takeSample(ctx, cfg, src, wallet, slot, now)
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
	if !sample.Exceeded || notifier == nil {
		return report, nil
	}
	return deliverWithHistory(ctx, cfg, store, notifier, sample, history, now, report)
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

func takeSample(ctx context.Context, cfg Config, src Sources, wallet string, slot, now time.Time) (Sample, error) {
	cash, err := src.Cash(ctx, wallet)
	if err != nil {
		return Sample{}, fmt.Errorf("read wallet cash: %w", err)
	}
	cashRat, err := ratOf(string(cash.Amount))
	if err != nil {
		return Sample{}, fmt.Errorf("wallet cash: %w", err)
	}
	marks, err := src.Positions(ctx, wallet)
	if err != nil {
		return Sample{}, fmt.Errorf("read wallet positions: %w", err)
	}
	total := new(big.Rat)
	values := make([]PositionValue, 0, len(marks))
	for _, mark := range marks {
		price, err := ratOf(string(mark.CurrentPrice))
		if err != nil {
			return Sample{}, fmt.Errorf("position %s price: %w", mark.TokenID, err)
		}
		shares := mark.Shares
		chain := false
		// A settled winner is worth its payout until redeemed; the indexer may
		// still list it after redemption already moved the cash on chain, so
		// the share count comes from chain to avoid counting it twice.
		if mark.Redeemable && price.Sign() > 0 {
			shares, err = src.ChainShares(ctx, wallet, mark.TokenID)
			if err != nil {
				return Sample{}, fmt.Errorf("read on-chain shares for %s: %w", mark.TokenID, err)
			}
			chain = true
		}
		sharesRat, err := ratOf(string(shares))
		if err != nil {
			return Sample{}, fmt.Errorf("position %s shares: %w", mark.TokenID, err)
		}
		value := new(big.Rat).Mul(sharesRat, price)
		total.Add(total, value)
		values = append(values, PositionValue{
			TokenID: mark.TokenID, ConditionID: mark.ConditionID, Title: mark.Title, Outcome: mark.Outcome,
			Shares: shares, Price: mark.CurrentPrice, Value: money(value), Redeemable: mark.Redeemable, ChainShares: chain,
		})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].TokenID < values[j].TokenID })
	return Sample{
		AccountID: cfg.AccountID, SlotAt: slot, SampledAt: now, CashPUSD: money(cashRat), CashBlock: cash.BlockNumber,
		PositionsValue: money(total), Equity: money(new(big.Rat).Add(cashRat, total)),
		WindowMinutes: int(cfg.Window / time.Minute), Threshold: cfg.Threshold.FloatString(2), Positions: values,
	}, nil
}

// evaluate sets the window peak, drawdown and exceeded flag. Without any
// earlier sample inside the window there is nothing to compare against.
func evaluate(cfg Config, sample *Sample, history []Sample) error {
	if len(history) == 0 {
		return nil
	}
	equity, err := ratOf(sample.Equity)
	if err != nil {
		return err
	}
	peak, peakAt := new(big.Rat).Set(equity), sample.SampledAt
	for _, h := range history {
		value, err := ratOf(h.Equity)
		if err != nil {
			return fmt.Errorf("history sample %s equity: %w", h.SlotAt.Format(time.RFC3339), err)
		}
		if value.Cmp(peak) > 0 {
			peak, peakAt = value, h.SampledAt
		}
	}
	drawdown := new(big.Rat).Sub(peak, equity)
	peakText, drawdownText := money(peak), money(drawdown)
	sample.PeakEquity, sample.PeakAt, sample.Drawdown = &peakText, &peakAt, &drawdownText
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

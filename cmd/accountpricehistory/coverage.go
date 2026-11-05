package main

import (
	"context"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

type assetCoverage struct {
	Asset          h.Asset   `json:"asset"`
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	SampledThrough time.Time `json:"sampled_through"`
	Available      int       `json:"available"`
	Missing        int       `json:"missing"`
}

// Include actual holding boundaries as well as UTC hours. An hour-only count
// could otherwise call a short holding complete despite lacking its entry mark.
func summarizeCoverage(ctx context.Context, target h.Window, series h.Series, observedAt time.Time) (assetCoverage, error) {
	end := target.To
	if completed := observedAt.UTC().Truncate(time.Hour); end.After(completed) {
		end = completed
	}
	item := assetCoverage{Asset: target.Asset, From: target.From, To: target.To, SampledThrough: end}
	if err := ctx.Err(); err != nil {
		return item, err
	}
	if end.Before(target.From) {
		return item, nil
	}
	check := func(at time.Time) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := h.PriceAt(series, at); ok {
			item.Available++
		} else {
			item.Missing++
		}
		return nil
	}
	if err := check(target.From); err != nil {
		return item, err
	}
	for at := target.From.UTC().Truncate(time.Hour).Add(time.Hour); at.Before(end); at = at.Add(time.Hour) {
		if err := check(at); err != nil {
			return item, err
		}
	}
	if end.After(target.From) {
		if err := check(end); err != nil {
			return item, err
		}
	}
	return item, nil
}

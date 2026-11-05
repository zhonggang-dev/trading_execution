package main

import (
	"context"
	"errors"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

func TestCoverageIncludesNonHourlyOpeningAndClosingBoundaries(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	series := h.Series{Points: []h.Point{{At: at.Add(time.Hour), Price: "0.5", ResolutionSeconds: 3600}}}
	w := h.Window{From: at.Add(30 * time.Minute), To: at.Add(90 * time.Minute)}
	got, err := summarizeCoverage(context.Background(), w, series, at.Add(4*time.Hour))
	if err != nil || got.Available != 2 || got.Missing != 1 {
		t.Fatal("missing entry price was hidden by the hour count", got, err)
	}
	w.From = at
	series.Points[0].At = at
	got, err = summarizeCoverage(context.Background(), w, series, at.Add(4*time.Hour))
	if err != nil || got.Available != 2 || got.Missing != 1 {
		t.Fatal("missing closing boundary was hidden by the hour count", got, err)
	}
}

func TestCoverageCountsAlignedBoundariesOnceAndStopsAtCompletedHour(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	series := h.Series{Points: []h.Point{
		{At: at, Price: "0.5", ResolutionSeconds: 3600},
		{At: at.Add(time.Hour), Price: "0.6", ResolutionSeconds: 3600},
		{At: at.Add(2 * time.Hour), Price: "0.7", ResolutionSeconds: 3600},
	}}
	w := h.Window{From: at, To: at.Add(3 * time.Hour)}
	got, err := summarizeCoverage(context.Background(), w, series, at.Add(2*time.Hour+17*time.Minute))
	if err != nil || got.Available != 3 || got.Missing != 0 || !got.SampledThrough.Equal(at.Add(2*time.Hour)) {
		t.Fatal("duplicate boundary or unfinished hour sampled", got, err)
	}
}

func TestCoverageCancellationDoesNotReturnSuccessfulCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	if _, err := summarizeCoverage(ctx, h.Window{From: at, To: at.Add(time.Hour)}, h.Series{}, at.Add(2*time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled coverage inspection succeeded", err)
	}
}

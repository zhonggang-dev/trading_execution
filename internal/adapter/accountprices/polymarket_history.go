package accountprices

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

type pmHistoryPoint struct {
	Timestamp  int64       `json:"timestamp"`
	Price      json.Number `json:"price"`
	Resolution int         `json:"resolution_seconds"`
}
type pmHistoryEnvelope struct {
	Data       []pmHistoryPoint `json:"data"`
	Pagination struct {
		HasMore bool    `json:"has_more"`
		Next    *string `json:"next_cursor"`
	} `json:"pagination"`
}

func appendPMHistoryPoints(w h.Window, ps []pmHistoryPoint, series *h.Series, boundary *time.Time, now time.Time) error {
	for _, p := range ps {
		if boundary != nil && time.Unix(p.Timestamp, 0).After(*boundary) {
			return fmt.Errorf("PM as_of returned future price")
		}
	}
	for _, p := range ps {
		if boundary == nil && p.Resolution == 0 {
			continue
		} // resolved window terminals may be clamped into the past
		price, priceErr := canonicalJSONPrice(p.Price)
		if priceErr != nil || p.Timestamp <= 0 || p.Resolution < 0 {
			return fmt.Errorf("invalid PM historical price")
		}
		at := time.Unix(p.Timestamp, 0).UTC()
		if at.After(w.To) {
			continue
		}
		source := at
		series.Points = append(series.Points, h.Point{At: at, Price: price, SourceAt: &source, FetchedAt: now, ResolutionSeconds: p.Resolution, Source: "PM_HISTORY"})
		if len(ps) > 10000 {
			return fmt.Errorf("PM history point budget exceeded")
		}
	}
	return nil
}

func (s *Source) pmHistory(ctx context.Context, w h.Window, series *h.Series) error {
	query := url.Values{"token_id": {w.Asset.TokenID}, "start": {strconv.FormatInt(w.From.Truncate(time.Hour).Add(-time.Hour).Unix(), 10)}, "end": {strconv.FormatInt(w.To.Unix()+1, 10)}, "bucket_seconds": {"3600"}, "limit": {"10000"}}
	cursors := map[string]bool{}
	initialPoints := len(series.Points)
	for page := 0; page < 100; page++ {
		var e pmHistoryEnvelope
		if err := s.get(ctx, s.p.PMBaseURL, "/v2/prices-history", query, &e); err != nil {
			return err
		}
		if e.Data == nil {
			return fmt.Errorf("PM history omitted data")
		}
		if !e.Pagination.HasMore && e.Pagination.Next != nil && *e.Pagination.Next != "" {
			return fmt.Errorf("PM history pagination inconsistent")
		}
		if err := appendPMHistoryPoints(w, e.Data, series, nil, s.p.Now().UTC()); err != nil {
			return err
		}
		if len(series.Points)-initialPoints > 10000 {
			return fmt.Errorf("PM history point budget exceeded")
		}
		if !e.Pagination.HasMore {
			break
		}
		if e.Pagination.Next == nil || *e.Pagination.Next == "" || cursors[*e.Pagination.Next] || page == 99 {
			return fmt.Errorf("PM history pagination incomplete")
		}
		cursors[*e.Pagination.Next] = true
		query.Set("cursor", *e.Pagination.Next)
	}

	return nil
}
func (s *Source) pmAsOf(ctx context.Context, w h.Window, at time.Time, series *h.Series) error {
	var e pmHistoryEnvelope
	q := url.Values{"token_id": {w.Asset.TokenID}, "as_of": {strconv.FormatInt(at.Unix(), 10)}}
	if err := s.get(ctx, s.p.PMBaseURL, "/v2/prices-history", q, &e); err != nil {
		return err
	}
	if e.Data == nil {
		return fmt.Errorf("PM boundary omitted data")
	}
	if e.Pagination.HasMore {
		return fmt.Errorf("PM boundary response incomplete")
	}
	return appendPMHistoryPoints(w, e.Data, series, &at, s.p.Now().UTC())
}

// Package accountpricehistory owns durable historical valuation inputs. It has
// no access to trading, balance, order or position mutation APIs.
package accountpricehistory

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
)

const FidelitySeconds = 3600
const MaxWindows = 10000
const MaxPoints = 2000000

type Asset struct {
	MarketSource string  `json:"market_source"`
	MarketID     string  `json:"market_id"`
	ConditionID  string  `json:"condition_id"`
	TokenID      string  `json:"token_id"`
	OutcomeIndex *int    `json:"outcome_index"`
	OutcomeName  *string `json:"outcome_name"`
}

func (a Asset) Key() string { b, _ := json.Marshal(a); return fmt.Sprintf("%x", sha256.Sum256(b)) }
func (a Asset) Validate() error {
	if (a.MarketSource != "POLYMARKET" && a.MarketSource != "KALSHI") || a.MarketID == "" || a.TokenID == "" {
		return fmt.Errorf("invalid history asset")
	}
	if a.MarketSource == "POLYMARKET" && a.ConditionID == "" {
		return fmt.Errorf("missing history condition")
	}
	return nil
}

type Window struct {
	Asset      Asset       `json:"asset"`
	From       time.Time   `json:"from"`
	To         time.Time   `json:"to"`
	RequiredAt []time.Time `json:"required_at,omitempty"`
	RefreshAt  *time.Time  `json:"refresh_at,omitempty"`
}
type Point struct {
	At                time.Time  `json:"at"`
	Price             string     `json:"price"`
	SourceAt          *time.Time `json:"source_at"`
	FetchedAt         time.Time  `json:"fetched_at"`
	ResolutionSeconds int        `json:"resolution_seconds"`
	Source            string     `json:"source"`
	ValidUntil        *time.Time `json:"valid_until"`
}
type Settlement struct {
	EffectiveAt     time.Time `json:"effective_at"`
	Price           string    `json:"price"`
	Source          string    `json:"source"`
	SourceReference string    `json:"source_reference"`
	FetchedAt       time.Time `json:"fetched_at"`
}
type Coverage struct {
	From      time.Time `json:"from"`
	To        time.Time `json:"to"`
	State     string    `json:"state"`
	Reason    string    `json:"reason"`
	FetchedAt time.Time `json:"fetched_at"`
}
type Series struct {
	Points      []Point      `json:"points"`
	Settlements []Settlement `json:"settlements"`
	Coverage    []Coverage   `json:"coverage"`
}
type Dataset struct {
	Series     map[string]Series `json:"series"`
	Version    string            `json:"version"`
	ObservedAt time.Time         `json:"observed_at"`
}
type Reader interface {
	Load(context.Context, []Window) (Dataset, error)
}
type Result struct {
	Window       Window `json:"window"`
	Series       Series `json:"series"`
	EvidenceHash string `json:"evidence_hash"`
}
type Source interface {
	Fetch(context.Context, Window) (Result, error)
}

// SettlementAwareSource optionally accepts already verified local/history facts.
// Implementations must still acquire genuine pre-settlement observations.
type SettlementAwareSource interface {
	FetchWithSettlements(context.Context, Window, []Settlement) (Result, error)
}

// SeriesAwareSource repairs only missing required samples using existing facts.
type SeriesAwareSource interface {
	FetchWithSeries(context.Context, Window, Series) (Result, error)
}
type Store interface {
	Reader
	Targets(context.Context, time.Time) ([]Window, error)
	Save(context.Context, Result) error
	Lock(context.Context) (func(), bool, error)
}

func ValidPrice(s string) bool {
	if s == "" || strings.TrimSpace(s) != s || strings.ContainsAny(s, "eE/") {
		return false
	}
	n, ok := new(big.Rat).SetString(s)
	return ok && n.Sign() >= 0 && n.Cmp(big.NewRat(1, 1)) <= 0
}

// PriceAt never looks ahead. A bounded extension must be supplied by verified
// no-trade evidence. Settlement inputs must already have passed identity and
// finalization checks in the source adapter.
func PriceAt(s Series, at time.Time) (Point, bool) {
	var settlement *Settlement
	for i := range s.Settlements {
		v := &s.Settlements[i]
		if !v.EffectiveAt.After(at) && ValidPrice(v.Price) && (settlement == nil || v.EffectiveAt.After(settlement.EffectiveAt)) {
			settlement = v
		}
	}
	if settlement != nil {
		t := settlement.EffectiveAt
		return Point{At: t, Price: settlement.Price, SourceAt: &t, FetchedAt: settlement.FetchedAt, ResolutionSeconds: 0, Source: settlement.Source}, true
	}
	// Durable readers and sources call SortSeries once per loaded series.
	i := sort.Search(len(s.Points), func(i int) bool { return s.Points[i].At.After(at) }) - 1
	for ; i >= 0; i-- {
		p := s.Points[i]
		if !ValidPrice(p.Price) || p.ResolutionSeconds < 0 || p.ResolutionSeconds > FidelitySeconds {
			continue
		}
		valid := at.Sub(p.At) <= time.Hour
		if p.ValidUntil != nil && !p.ValidUntil.Before(at) {
			valid = true
		}
		if valid {
			return p, true
		}
		return Point{}, false
	}
	return Point{}, false
}
func SortSeries(s *Series) {
	sort.Slice(s.Points, func(i, j int) bool {
		if s.Points[i].At.Equal(s.Points[j].At) {
			return s.Points[i].FetchedAt.Before(s.Points[j].FetchedAt)
		}
		return s.Points[i].At.Before(s.Points[j].At)
	})
	sort.Slice(s.Settlements, func(i, j int) bool { return s.Settlements[i].EffectiveAt.Before(s.Settlements[j].EffectiveAt) })
}

// MergeSettlements keeps one deterministic reference per effective time and
// rejects contradictory final payouts. Only already verified official binary
// facts belong here; their original observation timestamps remain unchanged.
func MergeSettlements(existing, additional []Settlement, asOf time.Time) ([]Settlement, error) {
	byTime := map[int64]Settlement{}
	payout := ""
	for _, batch := range [][]Settlement{existing, additional} {
		for _, v := range batch {
			if v.Source != "OFFICIAL_SETTLEMENT" || v.SourceReference == "" || v.EffectiveAt.IsZero() || !v.EffectiveAt.After(time.Unix(0, 0)) || v.EffectiveAt.After(asOf) || !ValidPrice(v.Price) {
				return nil, fmt.Errorf("invalid trusted historical settlement")
			}
			n, _ := new(big.Rat).SetString(v.Price)
			canonical := "0"
			if n.Cmp(big.NewRat(1, 1)) == 0 {
				canonical = "1"
			} else if n.Sign() != 0 {
				return nil, fmt.Errorf("non-binary trusted historical settlement")
			}
			if payout != "" && payout != canonical {
				return nil, fmt.Errorf("conflicting historical settlement evidence")
			}
			payout = canonical
			v.Price = canonical
			v.EffectiveAt = v.EffectiveAt.UTC()
			key := v.EffectiveAt.UnixNano()
			old, ok := byTime[key]
			if !ok || v.SourceReference < old.SourceReference || (v.SourceReference == old.SourceReference && v.FetchedAt.Before(old.FetchedAt)) {
				byTime[key] = v
			}
		}
	}
	result := make([]Settlement, 0, len(byTime))
	for _, v := range byTime {
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].EffectiveAt.Before(result[j].EffectiveAt) })
	return result, nil
}

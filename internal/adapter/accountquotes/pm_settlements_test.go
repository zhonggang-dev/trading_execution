package accountquotes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

const settledGamma = `[{"id":"M","conditionId":"C","closed":true,"umaResolutionStatus":"resolved","outcomes":"[\"Yes\",\"No\"]","clobTokenIds":"[\"yes-token\",\"no-token\"]","outcomePrices":"[\"0\",\"1\"]"}]`

func TestPMGammaConfirmedSettlementPrecedesExistingCurrentPriceAndBooks(t *testing.T) {
	now := time.Now().UTC()
	yes, no := target("a", "POLYMARKET", "yes-token", 0), target("a", "POLYMARKET", "no-token", 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/markets" || r.URL.Query().Get("closed") != "any" || r.URL.Query().Get("condition_ids") != "C" {
			t.Errorf("wrong settlement read %s", r.URL)
		}
		io.WriteString(w, settledGamma)
	}))
	defer server.Close()
	source := &Source{GammaURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return now }, Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		return []domain.ExternalPosition{{TokenID: "yes-token", ConditionID: "C", OutcomeIndex: yes.Asset.OutcomeIndex, OutcomeName: "Yes", CurrentPrice: "0.8", ObservedAt: now}, {TokenID: "no-token", ConditionID: "C", OutcomeIndex: no.Asset.OutcomeIndex, OutcomeName: "No", CurrentPrice: "0.2", ObservedAt: now}}, nil
	})}
	service := core.New(factsReaderFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) {
		return metricFacts(now, []core.Target{yes, no}), nil
	}), source)
	r, e := service.Query(context.Background(), "")
	if e != nil || len(r.Items) != 1 || r.Items[0].PricedAssetCount != 2 {
		t.Fatal("complete settlement", r, e)
	}
	for _, a := range r.Items[0].ValuationAssets {
		want := "0"
		if a.TokenID == "no-token" {
			want = "1"
		}
		if a.Price == nil || *a.Price != want || a.PriceSource == nil || *a.PriceSource != "OFFICIAL_SETTLEMENT" || a.SourceAt != nil {
			t.Fatalf("official terminal evidence lost %+v", a)
		}
	}
}
func TestPMGammaSettlementWithoutAnyAccountPosition(t *testing.T) {
	tar := target("a", "POLYMARKET", "yes-token", 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, settledGamma) }))
	defer server.Close()
	s := Source{GammaURL: server.URL, HTTPClient: server.Client(), Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) { return nil, nil })}
	q := s.Quotes(context.Background(), []core.Target{tar})[core.Key(tar)]
	if q.Price == nil || *q.Price != "0" || q.Source == nil || *q.Source != "OFFICIAL_SETTLEMENT" {
		t.Fatal("archived zero not priced", q)
	}
}
func TestPMGammaCannotTreatClosedOrAmbiguousIdentityAsFinalPrice(t *testing.T) {
	cases := map[string]string{"open": strings.Replace(settledGamma, `"closed":true`, `"closed":false`, 1), "proposed": strings.Replace(settledGamma, `"resolved"`, `"proposed"`, 1), "wrong-condition": strings.Replace(settledGamma, `"conditionId":"C"`, `"conditionId":"OTHER"`, 1), "wrong-market": strings.Replace(settledGamma, `"id":"M"`, `"id":"OTHER"`, 1), "wrong-token": strings.Replace(settledGamma, `yes-token`, `WRONG`, 1), "wrong-outcome": strings.Replace(settledGamma, `Yes`, `UP`, 1), "two-winners": strings.Replace(settledGamma, `\"0\",\"1\"`, `\"1\",\"1\"`, 1), "fractional": strings.Replace(settledGamma, `\"0\",\"1\"`, `\"0.5\",\"0.5\"`, 1), "duplicate": settledGamma[:len(settledGamma)-1] + "," + settledGamma[1:]}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
			defer server.Close()
			s := Source{GammaURL: server.URL, HTTPClient: server.Client()}
			tar := target("a", "POLYMARKET", "yes-token", 0)
			q := s.pmSettlements(context.Background(), []core.Target{tar}, nil)
			if len(q) != 0 {
				raw, _ := json.Marshal(q)
				t.Fatalf("unconfirmed/wrong final price %s", raw)
			}
		})
	}
}
func TestPMGammaSlowFailureDoesNotEraseSuccessfulCurrentPrice(t *testing.T) {
	now := time.Now().UTC()
	tar := target("a", "POLYMARKET", "yes-token", 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	source := &Source{GammaURL: server.URL, HTTPClient: server.Client(), Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		return []domain.ExternalPosition{{ConditionID: "C", TokenID: tar.Asset.TokenID, OutcomeIndex: tar.Asset.OutcomeIndex, OutcomeName: "Yes", CurrentPrice: "0.2", ObservedAt: now}}, nil
	})}
	service := core.New(factsReaderFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) {
		return metricFacts(now, []core.Target{tar}), nil
	}), source)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	r, e := service.Query(ctx, "")
	if e != nil || r.Items[0].PricedAssetCount != 1 || r.Items[0].ValuationAssets[0].PriceSource == nil || *r.Items[0].ValuationAssets[0].PriceSource != "PM_CURRENT_PRICE" {
		t.Fatal("Gamma failure erased known price", r, e)
	}
}

func TestPMKnownSettlementCannotBeOverwrittenByLateCurrentPriceAtBudget(t *testing.T) {
	now := time.Now().UTC()
	pm, k := target("a", "POLYMARKET", "yes-token", 0), kalshiTarget(0)
	facts := metricFacts(now, []core.Target{pm, k})
	settlementRead := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, settledGamma); close(settlementRead) }))
	defer server.Close()
	source := &Source{GammaURL: server.URL, HTTPClient: server.Client(), Positions: positionsFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
		<-settlementRead
		time.Sleep(50 * time.Millisecond)
		return []domain.ExternalPosition{{ConditionID: "C", TokenID: pm.Asset.TokenID, OutcomeIndex: pm.Asset.OutcomeIndex, OutcomeName: "Yes", CurrentPrice: "0.8", ObservedAt: now}}, nil
	}), Kalshi: marketsFunc(func(ctx context.Context, _ []string) ([]KalshiMarket, error) { <-ctx.Done(); return nil, ctx.Err() })}
	service := core.New(factsReaderFunc(func(context.Context, string) (consolev2.CoreMetricFacts, error) { return facts, nil }), source)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	r, e := service.Query(ctx, "")
	if e != nil || r.Items[0].MarketValue == nil {
		t.Fatal("completed settlement missing", e)
	}
	a := r.Items[0].ValuationAssets[0]
	if a.Price == nil || *a.Price != "0" || a.PriceSource == nil || *a.PriceSource != "OFFICIAL_SETTLEMENT" {
		t.Fatalf("late lower priority quote overwrote final price %+v", a)
	}
}

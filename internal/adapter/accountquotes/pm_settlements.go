package accountquotes

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	core "github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
)

// Gamma's final resolved outcome prices cover assets which the account-scoped
// positions endpoint no longer lists. This is only a public read, never a
// request to settle/redeem or alter the local ledger.
type gammaSettlement struct {
	ID          json.RawMessage `json:"id"`
	ConditionID string          `json:"conditionId"`
	Closed      *bool           `json:"closed"`
	Status      string          `json:"umaResolutionStatus"`
	Outcomes    json.RawMessage `json:"outcomes"`
	Tokens      json.RawMessage `json:"clobTokenIds"`
	Prices      json.RawMessage `json:"outcomePrices"`
}

func settlementStrings(raw json.RawMessage) ([]string, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	if raw[0] == '"' {
		var encoded string
		if json.Unmarshal(raw, &encoded) != nil {
			return nil, false
		}
		raw = []byte(encoded)
	}
	var values []string
	if json.Unmarshal(raw, &values) != nil {
		return nil, false
	}
	return values, true
}
func gammaID(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&number) == nil {
		return number.String()
	}
	return ""
}
func (s *Source) pmSettlements(ctx context.Context, targets []core.Target, emit func(map[string]core.Quote)) map[string]core.Quote {
	result := map[string]core.Quote{}
	if s.GammaURL == "" {
		return result
	}
	byCondition := map[string][]core.Target{}
	for _, t := range targets {
		if t.Asset.MarketSource == "POLYMARKET" {
			byCondition[t.Asset.ConditionID] = append(byCondition[t.Asset.ConditionID], t)
		}
	}
	conditions := make([]string, 0, len(byCondition))
	for condition := range byCondition {
		conditions = append(conditions, condition)
	}
	sort.Strings(conditions)
	for start := 0; start < len(conditions); start += 100 {
		end := start + 100
		if end > len(conditions) {
			end = len(conditions)
		}
		base, err := url.Parse(s.GammaURL)
		if err != nil || base.Host == "" {
			continue
		}
		endpoint := base.ResolveReference(&url.URL{Path: "/markets"})
		query := url.Values{"closed": {"any"}, "limit": {strconv.Itoa(end - start + 1)}}
		allowed := map[string]bool{}
		for _, condition := range conditions[start:end] {
			query.Add("condition_ids", condition)
			allowed[condition] = true
		}
		endpoint.RawQuery = query.Encode()
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Accept", "application/json")
		var markets []gammaSettlement
		err = s.read(req, &markets)
		cancel()
		if err != nil {
			continue
		}
		counts := map[string]int{}
		badBatch := false
		for _, m := range markets {
			counts[m.ConditionID]++
			if !allowed[m.ConditionID] {
				badBatch = true
			}
		}
		if badBatch {
			continue
		}
		observed := s.now()
		for _, m := range markets {
			if counts[m.ConditionID] != 1 || m.Closed == nil || !*m.Closed || !strings.EqualFold(strings.TrimSpace(m.Status), "resolved") {
				continue
			}
			outcomes, ok1 := settlementStrings(m.Outcomes)
			tokens, ok2 := settlementStrings(m.Tokens)
			prices, ok3 := settlementStrings(m.Prices)
			if !ok1 || !ok2 || !ok3 || len(tokens) != 2 || len(outcomes) != 2 || len(prices) != 2 || tokens[0] == "" || tokens[1] == "" || tokens[0] == tokens[1] || strings.EqualFold(outcomes[0], outcomes[1]) {
				continue
			}
			p0, e0 := price(prices[0])
			p1, e1 := price(prices[1])
			if e0 != nil || e1 != nil || !((p0 == "0" && p1 == "1") || (p0 == "1" && p1 == "0")) {
				continue
			}
			id := gammaID(m.ID)
			if id == "" {
				continue
			}
			for _, target := range byCondition[m.ConditionID] {
				a := target.Asset
				if a.MarketID != id || a.OutcomeIndex == nil || *a.OutcomeIndex < 0 || *a.OutcomeIndex > 1 {
					continue
				}
				i := *a.OutcomeIndex
				if a.TokenID != tokens[i] || !sameName(outcomes[i], a.OutcomeName) {
					continue
				}
				value := p0
				if i == 1 {
					value = p1
				}
				result[core.Key(target)] = core.Priced(value, "OFFICIAL_SETTLEMENT", observed, nil)
			}
		}
		if emit != nil && len(result) > 0 {
			emit(copyQuoteMap(result))
		}
	}
	return result
}

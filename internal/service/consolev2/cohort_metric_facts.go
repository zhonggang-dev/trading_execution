package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
)

const MaxCohortLots = 100000
const MaxCohortClosures = 200000

// CohortFilter selects opening lots, not the dates of subsequent cash flows.
// The opening interval is half-open; all subsequent accounting is read to now.
type CohortFilter struct {
	ExecutionAccountID string
	AccountScope       string
	OpeningFrom        *time.Time
	OpeningTo          *time.Time
}

type CohortAccountFact struct {
	OpenedAssetCount  int
	HoldingAssetCount int
	ClosedAssetCount  int
	BuyFees           string
	SellFees          string
	MaxDrawdown       *string
	DrawdownState     string
}
type CohortMetricFacts struct {
	CoreMetricFacts
	OpeningFrom *time.Time
	OpeningTo   *time.Time
	Cohorts     map[string]CohortAccountFact
}

func ValidateCohortFilter(f CohortFilter) error {
	if err := ValidatePerformanceAccountID(f.ExecutionAccountID); err != nil {
		return err
	}
	if err := ValidateAccountScope(f.ExecutionAccountID, f.AccountScope); err != nil {
		return err
	}
	if (f.OpeningFrom == nil) != (f.OpeningTo == nil) {
		return ErrInvalidPerformanceAccount
	}
	if f.OpeningFrom != nil && (!f.OpeningFrom.Before(*f.OpeningTo) || f.OpeningFrom.Year() < 1970 || f.OpeningTo.Year() > 9999) {
		return ErrInvalidPerformanceAccount
	}
	return nil
}

type cohortLot struct {
	id                                                 string
	asset                                              CoreAssetFact
	originalShares, remainingShares, principal, buyFee *big.Rat
	opened                                             time.Time
	closedShares, realized, sellFees                   *big.Rat
}
type cohortCashEvent struct {
	at          time.Time
	phase       int
	id, account string
	delta       *big.Rat
}

func cohortNumber(v string) (*big.Rat, error) {
	if v == "" || strings.ContainsAny(v, "eE/") {
		return nil, ErrUnavailable
	}
	n, ok := new(big.Rat).SetString(v)
	if !ok {
		return nil, ErrUnavailable
	}
	return n, nil
}
func cohortText(v *big.Rat) string {
	s := v.FloatString(24)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" || s == "-0" {
		return "0"
	}
	return s
}
func cohortIdentity(a CoreAssetFact) string {
	b, _ := json.Marshal([]any{a.ExecutionAccountID, a.MarketSource, a.MarketID, a.ConditionID, a.TokenID, a.OutcomeIndex, a.OutcomeName})
	return string(b)
}

// CohortMetricFacts uses immutable lot allocations to attribute both sides of
// Kalshi netting, ordinary sells, redemptions and external sells to the exact
// opening lots. It never rewrites the fee-inclusive funds ledger.
func (reader *Reader) CohortMetricFacts(ctx context.Context, filter CohortFilter) (CohortMetricFacts, error) {
	if err := ValidateCohortFilter(filter); err != nil {
		return CohortMetricFacts{}, err
	}
	tx, err := reader.beginReadTx(ctx, "cohort metric facts")
	if err != nil {
		return CohortMetricFacts{}, err
	}
	defer tx.Rollback()
	now := reader.now().UTC()
	accounts, err := reader.selectedManagedAccountsInTx(ctx, tx, filter.ExecutionAccountID, filter.AccountScope)
	if err != nil {
		return CohortMetricFacts{}, err
	}
	result := CohortMetricFacts{CoreMetricFacts: CoreMetricFacts{ObservedAt: now, Accounts: make([]CoreAccountFact, 0, len(accounts))}, OpeningFrom: filter.OpeningFrom, OpeningTo: filter.OpeningTo, Cohorts: map[string]CohortAccountFact{}}
	accountIndices := map[string]int{}
	for _, a := range accounts {
		f := CoreAccountFact{Metrics: AccountPerformance{ExecutionAccountID: a.ExecutionAccountID, MarketSource: a.MarketSource, CurrentCost: "0", PeakHoldingCost: "0", CumulativeInvestedCost: "0", RealizedPnL: "0", RealizedDrawdown14D: "0", DrawdownFrom: now, DrawdownTo: now}, Assets: []CoreAssetFact{}}
		if a.MarketSource == "POLYMARKET" {
			var address sql.NullString
			if err = tx.QueryRowContext(ctx, `SELECT LOWER(wallet_address) FROM execution_accounts WHERE execution_account_id=$1`, a.ExecutionAccountID).Scan(&address); err != nil {
				return CohortMetricFacts{}, dbUnavailable("cohort funding identity", err)
			}
			if address.Valid {
				f.FundingAddress = address.String
			}
		}
		accountIndices[a.ExecutionAccountID] = len(result.Accounts)
		result.Accounts = append(result.Accounts, f)
	}
	if len(accounts) == 0 {
		if err = tx.Commit(); err != nil {
			return CohortMetricFacts{}, dbUnavailable("commit cohort facts", err)
		}
		return result, nil
	}
	encoded, _ := json.Marshal(accounts)
	// Source/identity checks are the same as the current portfolio projection,
	// but closed lots are intentionally included here.
	base := `WITH configured AS(SELECT execution_account_id,market_source FROM jsonb_to_recordset($1::jsonb) AS a(execution_account_id text,market_source text)),
 selected_lots AS MATERIALIZED (
 SELECT l.*,` + portfolioSourceExpression + ` AS market_source,NULLIF(open.intent->'metadata'->>'market_question','') AS market_question,
 f.fill_key AS source_fill_key,f.execution_account_id AS fill_account,f.market_id AS fill_market,f.condition_id AS fill_condition,f.token_id AS fill_token,f.side AS fill_side,f.status AS fill_status,f.applied_at AS fill_applied_at,
 f.gross_notional,f.total_fee,f.order_id AS fill_order,f.shares AS fill_shares,f.venue AS fill_venue
 FROM position_lots l JOIN configured ON configured.execution_account_id=l.execution_account_id
 LEFT JOIN execution_orders open ON open.order_id=l.opening_order_id AND open.execution_account_id=l.execution_account_id AND open.market_id=l.market_id AND open.token_id=l.token_id AND COALESCE(open.intent->>'condition_id','')=l.condition_id
 LEFT JOIN execution_fills f ON f.fill_key=l.opening_fill_key
 LEFT JOIN execution_external_position_adoptions adoption ON adoption.external_adoption_id=l.external_adoption_id AND adoption.lot_id=l.lot_id AND adoption.execution_account_id=l.execution_account_id AND adoption.market_id=l.market_id AND adoption.condition_id=l.condition_id AND adoption.token_id=l.token_id AND adoption.outcome_index IS NOT DISTINCT FROM l.outcome_index AND adoption.outcome_name=l.outcome_name
 LEFT JOIN execution_external_position_baselines baseline ON baseline.baseline_id=adoption.baseline_id AND baseline.execution_account_id=adoption.execution_account_id
 LEFT JOIN execution_external_position_baseline_items baseline_item ON baseline_item.baseline_id=baseline.baseline_id AND baseline_item.execution_account_id=l.execution_account_id AND baseline_item.condition_id=l.condition_id AND baseline_item.token_id=l.token_id AND baseline_item.outcome_index IS NOT DISTINCT FROM l.outcome_index AND baseline_item.outcome_name=l.outcome_name
 WHERE l.opened_at<=$2 AND ($3::timestamptz IS NULL OR l.opened_at>=$3) AND ($4::timestamptz IS NULL OR l.opened_at<$4)
 )`
	args := []any{string(encoded), now, filter.OpeningFrom, filter.OpeningTo}
	q := base + fmt.Sprintf(` SELECT lot_id,execution_account_id,market_source,market_id,condition_id,token_id,outcome_index,NULLIF(outcome_name,''),market_question,original_shares::text,remaining_shares::text,
 CASE WHEN external_adoption_id IS NOT NULL THEN original_cost ELSE gross_notional END::text,
 CASE WHEN external_adoption_id IS NOT NULL THEN 0 ELSE total_fee END::text,original_cost::text,remaining_cost::text,opened_at,status,
 (external_adoption_id IS NOT NULL OR (source_fill_key IS NOT NULL AND fill_account=execution_account_id AND fill_market=market_id AND fill_condition=condition_id AND fill_token=token_id AND fill_order=opening_order_id AND fill_shares=original_shares AND UPPER(fill_venue)=market_source AND fill_side='BUY' AND fill_status='CONFIRMED' AND fill_applied_at<=$2)) AS valid_fill
 FROM selected_lots ORDER BY execution_account_id,opened_at,lot_id LIMIT %d`, MaxCohortLots+1)
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return CohortMetricFacts{}, dbUnavailable("cohort lots", err)
	}
	lots := map[string]*cohortLot{}
	events := []cohortCashEvent{}
	for rows.Next() {
		var l cohortLot
		var source sql.NullString
		var original, remaining, principal, buyFee, originalCost, remainingCost sql.NullString
		var status string
		var valid bool
		if err = rows.Scan(&l.id, &l.asset.ExecutionAccountID, &source, &l.asset.MarketID, &l.asset.ConditionID, &l.asset.TokenID, &l.asset.OutcomeIndex, &l.asset.OutcomeName, &l.asset.MarketQuestion, &original, &remaining, &principal, &buyFee, &originalCost, &remainingCost, &l.opened, &status, &valid); err != nil {
			rows.Close()
			return CohortMetricFacts{}, dbUnavailable("scan cohort lots", err)
		}
		idx, ok := accountIndices[l.asset.ExecutionAccountID]
		if !ok || !source.Valid || source.String != result.Accounts[idx].Metrics.MarketSource || !valid || !original.Valid || !remaining.Valid || !principal.Valid || !buyFee.Valid || !originalCost.Valid || !remainingCost.Valid {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
		l.asset.MarketSource = source.String
		values := []string{original.String, remaining.String, principal.String, buyFee.String, originalCost.String, remainingCost.String}
		ns := make([]*big.Rat, len(values))
		for i, v := range values {
			ns[i], err = cohortNumber(v)
			if err != nil || ns[i].Sign() < 0 {
				rows.Close()
				return CohortMetricFacts{}, ErrUnavailable
			}
		}
		if ns[0].Sign() <= 0 || ns[1].Cmp(ns[0]) > 0 || new(big.Rat).Add(ns[2], ns[3]).Cmp(ns[4]) != 0 || (ns[1].Sign() > 0 && (status != "OPEN" && status != "SETTLED_PENDING_REDEEM")) || (ns[1].Sign() == 0 && status != "CLOSED") {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
		l.originalShares, l.remainingShares, l.principal, l.buyFee = ns[0], ns[1], ns[2], ns[3]
		l.closedShares, l.realized, l.sellFees = new(big.Rat), new(big.Rat), new(big.Rat)
		l.asset.RemainingShares = remaining.String
		l.asset.RemainingCost = cohortText(new(big.Rat).Quo(new(big.Rat).Mul(l.principal, l.remainingShares), l.originalShares))
		l.asset.FirstOpenedAt, l.asset.LastOpenedAt = l.opened, l.opened
		l.asset.LotCount = 1
		lots[l.id] = &l
		events = append(events, cohortCashEvent{at: l.opened, phase: 0, id: l.id, account: l.asset.ExecutionAccountID, delta: new(big.Rat).Set(l.principal)})
		if len(lots) > MaxCohortLots {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
	}
	if err = queryRowsError(rows, "cohort lot rows"); err != nil {
		return CohortMetricFacts{}, err
	}
	// A selected cohort can contain only one leg of a paired netting. Validate
	// the complete exchange cash event and both legs before attributing just
	// the selected allocations. Reuse the ledger projection's exact evidence
	// contract rather than inventing a second definition of a valid pair.
	var invalidNetting bool
	q = base + `, selected_netting AS MATERIALIZED (
 SELECT DISTINCT f.fill_key FROM selected_lots l
 JOIN position_lot_closures c ON c.lot_id=l.lot_id
 JOIN execution_fills f ON f.fill_key=c.closing_fill_key
 WHERE f.side='BUY' AND c.closed_at<=$2
 ), valid_netting AS (SELECT f.fill_key` + nettingEvidenceFrom + `
 AND f.fill_key IN (SELECT fill_key FROM selected_netting))
 SELECT EXISTS(SELECT 1 FROM selected_netting n WHERE NOT EXISTS(SELECT 1 FROM valid_netting v WHERE v.fill_key=n.fill_key))`
	if err = tx.QueryRowContext(ctx, q, args...).Scan(&invalidNetting); err != nil {
		return CohortMetricFacts{}, dbUnavailable("validate cohort netting", err)
	}
	if invalidNetting {
		return CohortMetricFacts{}, ErrUnavailable
	}
	// Every disposition is tied to a selected opening lot. Fees on a reverse BUY
	// remain opening fees; only actual SELL cash flows restore a selling fee.
	q = base + `, flows AS (
 SELECT c.lot_id,'closure:'||c.closure_id AS flow_id,c.closed_shares AS shares,c.allocated_net_proceeds AS net,
 CASE WHEN f.side='SELL' THEN f.total_fee*c.closed_shares/f.shares ELSE 0 END AS sell_fee,c.closed_at AS at,
 (f.execution_account_id=l.execution_account_id AND f.market_id=l.market_id AND f.condition_id=l.condition_id AND (f.side='SELL' AND f.token_id=l.token_id OR f.side='BUY' AND l.market_source='KALSHI' AND c.closure_id='closure:netting:'||f.fill_key||':'||l.lot_id) AND f.status='CONFIRMED' AND f.applied_at<=$2) AS valid
 FROM selected_lots l JOIN position_lot_closures c ON c.lot_id=l.lot_id LEFT JOIN execution_fills f ON f.fill_key=c.closing_fill_key WHERE c.closed_at<=$2
 UNION ALL
 SELECT a.lot_id,'external:'||a.sell_id,a.closed_shares,a.net_proceeds,s.total_fee*a.closed_shares/s.shares,s.occurred_at,
 (s.execution_account_id=l.execution_account_id AND s.token_id=l.token_id AND s.condition_id=l.condition_id AND b.applied_at<=$2)
 FROM selected_lots l JOIN managed_external_sell_allocations a ON a.lot_id=l.lot_id JOIN managed_external_sells s ON s.sell_id=a.sell_id JOIN managed_external_sell_batches b ON b.batch_id=s.batch_id WHERE s.occurred_at<=$2
 UNION ALL
 SELECT r.lot_id,'redemption:'||r.redemption_id,r.redeemed_shares,r.allocated_payout,0,r.redeemed_at,
 (l.market_source='POLYMARKET' AND r.execution_account_id=l.execution_account_id AND r.condition_id=l.condition_id AND p.status='APPLIED' AND p.confirmed_at IS NOT NULL AND p.applied_at<=$2)
 FROM selected_lots l JOIN position_lot_redemptions r ON r.lot_id=l.lot_id LEFT JOIN polymarket_redemptions p ON p.execution_account_id=r.execution_account_id AND p.condition_id=r.condition_id WHERE r.redeemed_at<=$2
 UNION ALL
 SELECT r.lot_id,'kalshi:'||r.lot_id,r.settled_shares,r.allocated_payout,0,r.settled_at,
 (l.market_source='KALSHI' AND r.execution_account_id=l.execution_account_id AND r.ticker=l.market_id AND r.token_id=l.token_id AND p.status='APPLIED' AND p.applied_at<=$2)
 FROM selected_lots l JOIN kalshi_settlement_lots r ON r.lot_id=l.lot_id LEFT JOIN kalshi_settlements p ON p.execution_account_id=r.execution_account_id AND p.ticker=r.ticker WHERE r.settled_at<=$2
 ) SELECT lot_id,flow_id,shares::text,net::text,sell_fee::text,at,COALESCE(valid,FALSE) FROM flows ORDER BY at,flow_id,lot_id LIMIT ` + fmt.Sprint(MaxCohortClosures+1)
	rows, err = tx.QueryContext(ctx, q, args...)
	if err != nil {
		return CohortMetricFacts{}, dbUnavailable("cohort dispositions", err)
	}
	flowCount := 0
	for rows.Next() {
		var id, flow, shares, net, fee string
		var at time.Time
		var valid bool
		if err = rows.Scan(&id, &flow, &shares, &net, &fee, &at, &valid); err != nil {
			rows.Close()
			return CohortMetricFacts{}, dbUnavailable("scan cohort disposition", err)
		}
		flowCount++
		l, ok := lots[id]
		if flowCount > MaxCohortClosures || !ok || !valid || at.Before(l.opened) {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
		qty, e := cohortNumber(shares)
		if e != nil || qty.Sign() <= 0 {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
		cash, e := cohortNumber(net)
		if e != nil || cash.Sign() < 0 {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
		sellFee, e := cohortNumber(fee)
		if e != nil || sellFee.Sign() < 0 {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
		l.closedShares.Add(l.closedShares, qty)
		if l.closedShares.Cmp(l.originalShares) > 0 {
			rows.Close()
			return CohortMetricFacts{}, ErrUnavailable
		}
		cost := new(big.Rat).Quo(new(big.Rat).Mul(l.principal, qty), l.originalShares)
		l.realized.Add(l.realized, new(big.Rat).Sub(new(big.Rat).Add(cash, sellFee), cost))
		l.sellFees.Add(l.sellFees, sellFee)
		events = append(events, cohortCashEvent{at: at, phase: 1, id: flow + ":" + id, account: l.asset.ExecutionAccountID, delta: new(big.Rat).Neg(cost)})
	}
	if err = queryRowsError(rows, "cohort disposition rows"); err != nil {
		return CohortMetricFacts{}, err
	}
	// Aggregate after validating full lot conservation, never by token alone.
	assets := map[string]*CoreAssetFact{}
	assetHoldings := map[string]bool{}
	invested := map[string]*big.Rat{}
	realized := map[string]*big.Rat{}
	buyFees := map[string]*big.Rat{}
	sellFees := map[string]*big.Rat{}
	for _, a := range accounts {
		for _, m := range []map[string]*big.Rat{invested, realized, buyFees, sellFees} {
			m[a.ExecutionAccountID] = new(big.Rat)
		}
	}
	for _, l := range lots {
		if new(big.Rat).Add(l.remainingShares, l.closedShares).Cmp(l.originalShares) != 0 {
			return CohortMetricFacts{}, ErrUnavailable
		}
		id := l.asset.ExecutionAccountID
		invested[id].Add(invested[id], l.principal)
		realized[id].Add(realized[id], l.realized)
		buyFees[id].Add(buyFees[id], l.buyFee)
		sellFees[id].Add(sellFees[id], l.sellFees)
		key := cohortIdentity(l.asset)
		assetHoldings[key] = assetHoldings[key] || l.remainingShares.Sign() > 0
		if l.remainingShares.Sign() == 0 {
			continue
		}
		if current, ok := assets[key]; ok {
			q, _ := cohortNumber(current.RemainingShares)
			q.Add(q, l.remainingShares)
			current.RemainingShares = cohortText(q)
			cost, _ := cohortNumber(current.RemainingCost)
			next, _ := cohortNumber(l.asset.RemainingCost)
			cost.Add(cost, next)
			current.RemainingCost = cohortText(cost)
			current.LotCount++
			if l.opened.Before(current.FirstOpenedAt) {
				current.FirstOpenedAt = l.opened
			}
			if l.opened.After(current.LastOpenedAt) {
				current.LastOpenedAt = l.opened
			}
		} else {
			copy := l.asset
			assets[key] = &copy
		}
	}
	if len(assets) > MaxCoreMetricAssets {
		return CohortMetricFacts{}, ErrUnavailable
	}
	sort.Slice(events, func(i, j int) bool {
		if !events[i].at.Equal(events[j].at) {
			return events[i].at.Before(events[j].at)
		}
		if events[i].phase != events[j].phase {
			return events[i].phase < events[j].phase
		}
		return events[i].id < events[j].id
	})
	balances, peaks := map[string]*big.Rat{}, map[string]*big.Rat{}
	for _, a := range accounts {
		balances[a.ExecutionAccountID], peaks[a.ExecutionAccountID] = new(big.Rat), new(big.Rat)
	}
	for _, e := range events {
		b := balances[e.account]
		b.Add(b, e.delta)
		if b.Sign() < 0 {
			return CohortMetricFacts{}, ErrUnavailable
		}
		if b.Cmp(peaks[e.account]) > 0 {
			peaks[e.account].Set(b)
		}
	}
	for i := range result.Accounts {
		m := &result.Accounts[i].Metrics
		id := m.ExecutionAccountID
		m.PeakHoldingCost = cohortText(peaks[id])
		m.CumulativeInvestedCost = cohortText(invested[id])
		m.RealizedPnL = cohortText(realized[id])
		result.Cohorts[id] = CohortAccountFact{BuyFees: cohortText(buyFees[id]), SellFees: cohortText(sellFees[id]), DrawdownState: "history_unavailable"}
	}
	for key, holding := range assetHoldings {
		var tuple []any
		_ = json.Unmarshal([]byte(key), &tuple)
		id := tuple[0].(string)
		c := result.Cohorts[id]
		c.OpenedAssetCount++
		if holding {
			c.HoldingAssetCount++
		} else {
			c.ClosedAssetCount++
		}
		result.Cohorts[id] = c
	}
	// Fetch optional trusted settlement facts in one bounded set, never one query per asset.
	settlementTargets := make([]PortfolioHoldingIdentity, 0, len(assets))
	for _, a := range assets {
		if a.MarketSource == "POLYMARKET" {
			settlementTargets = append(settlementTargets, PortfolioHoldingIdentity{ExecutionAccountID: a.ExecutionAccountID, MarketSource: a.MarketSource, MarketID: a.MarketID, ConditionID: a.ConditionID, TokenID: a.TokenID, OutcomeIndex: a.OutcomeIndex, OutcomeName: a.OutcomeName})
		}
	}
	// The target JSON uses explicit field names to keep private facts out of the response.
	type settlementTarget struct {
		Account   string  `json:"account"`
		Market    string  `json:"market"`
		Condition string  `json:"condition"`
		Token     string  `json:"token"`
		Index     *int    `json:"index"`
		Name      *string `json:"name"`
	}
	targets := make([]settlementTarget, 0, len(settlementTargets))
	for _, a := range settlementTargets {
		targets = append(targets, settlementTarget{a.ExecutionAccountID, a.MarketID, a.ConditionID, a.TokenID, a.OutcomeIndex, a.OutcomeName})
	}
	if len(targets) > 0 {
		raw, _ := json.Marshal(targets)
		rows, err = tx.QueryContext(ctx, `SELECT p.execution_account_id,p.market_id,p.condition_id,p.token_id,p.outcome_index,NULLIF(p.outcome_name,''),p.settlement_price::text,p.settled_at
  FROM jsonb_to_recordset($1::jsonb) AS a(account text,market text,condition text,token text,index integer,name text)
  JOIN execution_positions p ON p.execution_account_id=a.account AND p.market_id=a.market AND p.condition_id=a.condition AND p.token_id=a.token AND p.outcome_index IS NOT DISTINCT FROM a.index AND NULLIF(p.outcome_name,'') IS NOT DISTINCT FROM a.name
  WHERE p.lifecycle_status='SETTLED_PENDING_REDEEM' AND p.settlement_source<>'' AND p.settled_at<=$2 AND p.settlement_price IN(0,1)`, string(raw), now)
		if err != nil {
			return CohortMetricFacts{}, dbUnavailable("cohort settlement prices", err)
		}
		for rows.Next() {
			var a CoreAssetFact
			var price string
			var at time.Time
			a.MarketSource = "POLYMARKET"
			if err = rows.Scan(&a.ExecutionAccountID, &a.MarketID, &a.ConditionID, &a.TokenID, &a.OutcomeIndex, &a.OutcomeName, &price, &at); err != nil {
				rows.Close()
				return CohortMetricFacts{}, dbUnavailable("scan cohort settlement price", err)
			}
			if target := assets[cohortIdentity(a)]; target != nil {
				v := at.UTC()
				target.SettlementPrice = &price
				target.SettlementAt = &v
			}
		}
		if err = queryRowsError(rows, "cohort settlement rows"); err != nil {
			return CohortMetricFacts{}, err
		}
	}
	for _, a := range assets {
		idx := accountIndices[a.ExecutionAccountID]
		result.Accounts[idx].Assets = append(result.Accounts[idx].Assets, *a)
	}
	for i := range result.Accounts {
		a := &result.Accounts[i]
		sort.Slice(a.Assets, func(i, j int) bool { return cohortIdentity(a.Assets[i]) < cohortIdentity(a.Assets[j]) })
		c := result.Cohorts[a.Metrics.ExecutionAccountID]
		if c.OpenedAssetCount == 0 {
			z := "0"
			c.MaxDrawdown = &z
			c.DrawdownState = "available"
			result.Cohorts[a.Metrics.ExecutionAccountID] = c
		}
	}
	if err = tx.Commit(); err != nil {
		return CohortMetricFacts{}, dbUnavailable("commit cohort facts", err)
	}
	return result, nil
}

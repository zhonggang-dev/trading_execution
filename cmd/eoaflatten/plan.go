package main

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type action string

const (
	actionSell   action = "SELL"
	actionRedeem action = "REDEEM"
	actionSkip   action = "SKIP"
)

const (
	onlyAll    = ""
	onlySell   = "sell"
	onlyRedeem = "redeem"
)

// shareScale is the CTF / pUSD base-unit scale (6 decimals).
var shareScale = big.NewInt(1_000_000)

// position is one Data API v2 row joined with its title.
type position struct {
	Title        string
	Outcome      string
	TokenID      string
	ConditionID  string
	Shares       domain.Decimal
	CurrentPrice domain.Decimal
	NegRisk      bool
	Redeemable   bool
}

// row is the plan and outcome for one wallet position.
type row struct {
	Pos    position
	Action action
	Reason string
	Notes  []string

	// ChainShares is ConditionalTokens.balanceOf in base units; nil if unread.
	ChainShares *big.Int
	// Sell fields are set only for SELL rows.
	Sell         *sellPlan
	SellNegRisk  bool
	TickSize     domain.Decimal
	MinOrderSize domain.Decimal

	Group *redeemGroup

	Result string
	Failed bool
}

func (r *row) skip(format string, args ...any) {
	r.Action = actionSkip
	r.Reason = fmt.Sprintf(format, args...)
	r.Sell = nil
}

// redeemGroup is one adapter redeemPositions call. The adapter redeems the
// caller's whole balance of both outcomes, so a condition is called once.
type redeemGroup struct {
	ConditionID string
	NegRisk     bool
	Rows        []*row
	Skip        string

	Adapter        string
	Collateral     string
	PositionIDs    [2]string
	Balances       [2]*big.Int
	Payout         polymarket.ConditionPayout
	ExpectedPayout *big.Int
	Simulation     string
	Approved       *bool

	Result string
	TxHash string
	Failed bool
}

func (g *redeemGroup) skip(format string, args ...any) {
	g.Skip = fmt.Sprintf(format, args...)
	for _, member := range g.Rows {
		member.skip("%s", g.Skip)
	}
}

// classify maps positions to SELL (unsettled) or REDEEM (settled, grouped by
// condition) and applies --only. It performs no I/O.
func classify(positions []position, only string) ([]*row, []*redeemGroup) {
	rows := make([]*row, 0, len(positions))
	groups := make([]*redeemGroup, 0)
	byCondition := make(map[string]*redeemGroup)
	for _, pos := range positions {
		current := &row{Pos: pos}
		rows = append(rows, current)
		if pos.Redeemable {
			if only == onlySell {
				current.skip("已结算待赎回，本次 --only sell 不处理")
				continue
			}
			current.Action = actionRedeem
			if isZeroDecimal(pos.CurrentPrice) {
				current.Notes = append(current.Notes, "价值为 0（输方），仍赎回以清掉显示")
			}
			key := strings.ToLower(strings.TrimSpace(pos.ConditionID))
			group, exists := byCondition[key]
			if !exists {
				group = &redeemGroup{ConditionID: key, NegRisk: pos.NegRisk}
				byCondition[key] = group
				groups = append(groups, group)
			} else if group.NegRisk != pos.NegRisk {
				group.Skip = "同一 condition 的 negative_risk 标记不一致，不赎回"
			}
			group.Rows = append(group.Rows, current)
			current.Group = group
			continue
		}
		if only == onlyRedeem {
			current.skip("未结算需卖出，本次 --only redeem 不处理")
			continue
		}
		current.Action = actionSell
	}
	for _, group := range groups {
		if group.Skip != "" {
			group.skip("%s", group.Skip)
		}
	}
	return rows, groups
}

// sellPlan is a FAK SELL at a fixed limit price.
type sellPlan struct {
	Price domain.Decimal
	Size  domain.Decimal
	// Dust is the remainder below the 0.01-share order precision (or removed
	// to satisfy FAK amount precision) that stays in the wallet.
	Dust *big.Rat
	// FillableShares and ExpectedProceeds walk the observed bids at or above
	// the limit price; fees are not deducted.
	FillableShares   *big.Rat
	ExpectedProceeds *big.Rat
}

// planSell sizes a FAK SELL of shares against the bid side. The limit is the
// best bid lowered by slippageTicks (never below one tick). Size is truncated
// to 0.01 and then lowered until price*size fits the FAK 4-decimal amount
// precision. The venue minimum order size is deliberately not enforced.
func planSell(shares *big.Rat, tickSize domain.Decimal, bids []domain.PriceLevel, slippageTicks int) (sellPlan, string) {
	tick, ok := decimalToRat(tickSize)
	if !ok || tick.Sign() <= 0 {
		return sellPlan{}, fmt.Sprintf("tick_size %q 无效", tickSize)
	}
	if shares == nil || shares.Sign() <= 0 {
		return sellPlan{}, "份额为 0"
	}
	hundred := big.NewRat(100, 1)
	size := floorRat(new(big.Rat).Mul(shares, hundred))
	size.Quo(size, hundred)
	if size.Sign() <= 0 {
		return sellPlan{}, fmt.Sprintf("份额 %s 不足 0.01，CLOB 无法下单（尘埃）", formatRat(shares, 6))
	}
	if len(bids) == 0 {
		return sellPlan{}, "订单簿没有买单"
	}
	best, ok := decimalToRat(bids[0].Price)
	if !ok || best.Sign() <= 0 {
		return sellPlan{}, "最优买价无效"
	}
	price := new(big.Rat).Sub(best, new(big.Rat).Mul(tick, big.NewRat(int64(slippageTicks), 1)))
	// Snap down onto the tick grid and never below one tick.
	steps := floorRat(new(big.Rat).Quo(price, tick))
	price = new(big.Rat).Mul(steps, tick)
	if price.Cmp(tick) < 0 {
		price = new(big.Rat).Set(tick)
	}
	if price.Cmp(new(big.Rat).Sub(big.NewRat(1, 1), tick)) > 0 {
		return sellPlan{}, "限价超出 [tick, 1-tick]"
	}
	cent := big.NewRat(1, 100)
	for ratDecimalPlaces(new(big.Rat).Mul(size, price)) > 4 {
		size.Sub(size, cent)
		if size.Sign() <= 0 {
			return sellPlan{}, fmt.Sprintf("限价 %s 下找不到满足 FAK 金额精度的份额", formatRat(price, tickPlaces(tick)))
		}
	}
	plan := sellPlan{
		Price:            domain.Decimal(formatRat(price, tickPlaces(tick))),
		Size:             domain.Decimal(formatRat(size, 2)),
		Dust:             new(big.Rat).Sub(shares, size),
		FillableShares:   new(big.Rat),
		ExpectedProceeds: new(big.Rat),
	}
	remaining := new(big.Rat).Set(size)
	for _, level := range bids {
		if remaining.Sign() <= 0 {
			break
		}
		levelPrice, okPrice := decimalToRat(level.Price)
		levelSize, okSize := decimalToRat(level.Size)
		if !okPrice || !okSize || levelPrice.Cmp(price) < 0 {
			break
		}
		take := levelSize
		if remaining.Cmp(levelSize) < 0 {
			take = remaining
		}
		plan.FillableShares.Add(plan.FillableShares, take)
		plan.ExpectedProceeds.Add(plan.ExpectedProceeds, new(big.Rat).Mul(take, levelPrice))
		remaining = new(big.Rat).Sub(remaining, take)
	}
	return plan, ""
}

// sellOperators returns the CTF operators a SELL on this market type needs:
// the CLOB V2 exchange, plus the legacy Neg Risk Adapter for neg-risk markets
// (Polymarket documents that the CLOB still checks that approval).
func sellOperators(negRisk bool) []string {
	if negRisk {
		return []string{strings.ToLower(polymarket.NegRiskExchangeV2Address), polymarket.NegRiskAdapterV1Address}
	}
	return []string{strings.ToLower(polymarket.StandardExchangeV2Address)}
}

// expectedRedemption is sum(balance_i * numerator_i) / denominator in base units.
func expectedRedemption(balances [2]*big.Int, payout polymarket.ConditionPayout) *big.Int {
	if !payout.Resolved() {
		return nil
	}
	total := new(big.Int)
	for index := range balances {
		if balances[index] == nil || payout.Numerators[index] == nil {
			continue
		}
		total.Add(total, new(big.Int).Mul(balances[index], payout.Numerators[index]))
	}
	return total.Quo(total, payout.Denominator)
}

func decimalToRat(value domain.Decimal) (*big.Rat, bool) {
	text := strings.TrimSpace(value.String())
	if text == "" || strings.ContainsAny(text, "/eE") {
		return nil, false
	}
	parsed, ok := new(big.Rat).SetString(text)
	return parsed, ok
}

func isZeroDecimal(value domain.Decimal) bool {
	parsed, ok := decimalToRat(value)
	return ok && parsed.Sign() == 0
}

func floorRat(value *big.Rat) *big.Rat {
	quotient := new(big.Int).Quo(value.Num(), value.Denom())
	if value.Sign() < 0 && new(big.Int).Mul(quotient, value.Denom()).Cmp(value.Num()) != 0 {
		quotient.Sub(quotient, big.NewInt(1))
	}
	return new(big.Rat).SetInt(quotient)
}

func ratDecimalPlaces(value *big.Rat) int {
	for places := 0; places <= 18; places++ {
		scaled := new(big.Rat).Mul(value, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(places)), nil)))
		if scaled.IsInt() {
			return places
		}
	}
	return 19
}

func tickPlaces(tick *big.Rat) int { return ratDecimalPlaces(tick) }

// formatRat renders value with at most places decimals, trimming zeros.
func formatRat(value *big.Rat, places int) string {
	if value == nil {
		return "?"
	}
	text := value.FloatString(places)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	if text == "-0" {
		return "0"
	}
	return text
}

func baseUnitsRat(value *big.Int) *big.Rat {
	if value == nil {
		return nil
	}
	return new(big.Rat).SetFrac(value, shareScale)
}

func formatBaseUnits(value *big.Int) string {
	if value == nil {
		return "?"
	}
	return formatRat(baseUnitsRat(value), 6)
}

func shortID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 16 {
		return value
	}
	return value[:6] + "…" + value[len(value)-6:]
}

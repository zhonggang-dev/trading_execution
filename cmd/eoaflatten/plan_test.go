package main

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestClassifySellsOpenRedeemsSettledAndGroupsByCondition(t *testing.T) {
	positions := []position{
		{TokenID: "1", ConditionID: "0xAA", Shares: "5", CurrentPrice: "0.4"},
		{TokenID: "2", ConditionID: "0xbb", Shares: "3", CurrentPrice: "1", Redeemable: true},
		{TokenID: "3", ConditionID: "0xBB", Shares: "2", CurrentPrice: "0", Redeemable: true},
		{TokenID: "4", ConditionID: "0xcc", Shares: "1", CurrentPrice: "1", Redeemable: true, NegRisk: true},
	}
	rows, groups := classify(positions, onlyAll)
	if rows[0].Action != actionSell {
		t.Fatalf("open position action = %s, want SELL", rows[0].Action)
	}
	for _, index := range []int{1, 2, 3} {
		if rows[index].Action != actionRedeem {
			t.Fatalf("settled position %d action = %s, want REDEEM", index, rows[index].Action)
		}
	}
	if len(groups) != 2 {
		t.Fatalf("redeem groups = %d, want 2 (YES/NO of one condition share a call)", len(groups))
	}
	if groups[0].ConditionID != "0xbb" || len(groups[0].Rows) != 2 || groups[0].NegRisk {
		t.Fatalf("first group = %+v", groups[0])
	}
	if !groups[1].NegRisk || len(groups[1].Rows) != 1 {
		t.Fatalf("neg-risk group = %+v", groups[1])
	}
	if len(rows[2].Notes) == 0 || !strings.Contains(rows[2].Notes[0], "价值为 0") {
		t.Fatalf("zero-value settled position must be flagged, notes = %v", rows[2].Notes)
	}
	if len(rows[1].Notes) != 0 {
		t.Fatalf("winning position must not be flagged as zero value: %v", rows[1].Notes)
	}
}

func TestClassifyOnlyFilters(t *testing.T) {
	positions := []position{
		{TokenID: "1", ConditionID: "0xaa", Shares: "5"},
		{TokenID: "2", ConditionID: "0xbb", Shares: "3", Redeemable: true},
	}
	rows, groups := classify(positions, onlySell)
	if rows[0].Action != actionSell || rows[1].Action != actionSkip || len(groups) != 0 {
		t.Fatalf("--only sell: actions %s/%s groups %d", rows[0].Action, rows[1].Action, len(groups))
	}
	rows, groups = classify(positions, onlyRedeem)
	if rows[0].Action != actionSkip || rows[1].Action != actionRedeem || len(groups) != 1 {
		t.Fatalf("--only redeem: actions %s/%s groups %d", rows[0].Action, rows[1].Action, len(groups))
	}
}

func TestClassifySkipsConditionWithConflictingNegRisk(t *testing.T) {
	rows, groups := classify([]position{
		{TokenID: "1", ConditionID: "0xaa", Shares: "1", Redeemable: true},
		{TokenID: "2", ConditionID: "0xaa", Shares: "1", Redeemable: true, NegRisk: true},
	}, onlyAll)
	if len(groups) != 1 || groups[0].Skip == "" {
		t.Fatalf("conflicting group must be skipped: %+v", groups)
	}
	for _, current := range rows {
		if current.Action != actionSkip {
			t.Fatalf("rows of a skipped group must be SKIP, got %s", current.Action)
		}
	}
}

func rat(t *testing.T, value string) *big.Rat {
	t.Helper()
	parsed, ok := new(big.Rat).SetString(value)
	if !ok {
		t.Fatalf("bad rat %q", value)
	}
	return parsed
}

func levels(pairs ...string) []domain.PriceLevel {
	result := make([]domain.PriceLevel, 0, len(pairs)/2)
	for index := 0; index+1 < len(pairs); index += 2 {
		result = append(result, domain.PriceLevel{Price: domain.Decimal(pairs[index]), Size: domain.Decimal(pairs[index+1])})
	}
	return result
}

func TestPlanSellUsesBestBidTruncatesAndWalksDepth(t *testing.T) {
	bids := levels("0.45", "5", "0.44", "10")
	plan, reason := planSell(rat(t, "6.930001"), "0.01", bids, 0)
	if reason != "" {
		t.Fatal(reason)
	}
	if plan.Price != "0.45" || plan.Size != "6.93" {
		t.Fatalf("price/size = %s/%s", plan.Price, plan.Size)
	}
	if formatRat(plan.FillableShares, 6) != "5" || formatRat(plan.ExpectedProceeds, 6) != "2.25" {
		t.Fatalf("fillable/proceeds = %s/%s", formatRat(plan.FillableShares, 6), formatRat(plan.ExpectedProceeds, 6))
	}
	if formatRat(plan.Dust, 6) != "0.000001" {
		t.Fatalf("dust = %s", formatRat(plan.Dust, 6))
	}

	plan, reason = planSell(rat(t, "6.93"), "0.01", bids, 1)
	if reason != "" {
		t.Fatal(reason)
	}
	if plan.Price != "0.44" || formatRat(plan.FillableShares, 6) != "6.93" || formatRat(plan.ExpectedProceeds, 6) != "3.0992" {
		t.Fatalf("slippage plan = %s fill %s proceeds %s", plan.Price, formatRat(plan.FillableShares, 6), formatRat(plan.ExpectedProceeds, 6))
	}
}

func TestPlanSellSizeBelowMinOrderSizeIsStillPlanned(t *testing.T) {
	plan, reason := planSell(rat(t, "1.98"), "0.01", levels("0.02", "100"), 0)
	if reason != "" || plan.Size != "1.98" || plan.Price != "0.02" {
		t.Fatalf("plan %+v reason %q", plan, reason)
	}
}

func TestPlanSellFitsFAKAmountPrecision(t *testing.T) {
	// 1.23 * 0.123 = 0.15129 has 5 decimals; FAK SELL needs <= 4.
	plan, reason := planSell(rat(t, "1.23"), "0.001", levels("0.123", "100"), 0)
	if reason != "" {
		t.Fatal(reason)
	}
	if plan.Size != "1.2" || formatRat(plan.Dust, 6) != "0.03" {
		t.Fatalf("size/dust = %s/%s", plan.Size, formatRat(plan.Dust, 6))
	}
	if ratDecimalPlaces(new(big.Rat).Mul(rat(t, plan.Size.String()), rat(t, plan.Price.String()))) > 4 {
		t.Fatalf("notional precision still > 4")
	}
}

func TestPlanSellSkips(t *testing.T) {
	cases := []struct {
		name   string
		shares string
		tick   domain.Decimal
		bids   []domain.PriceLevel
		want   string
	}{
		{"dust", "0.009", "0.01", levels("0.5", "1"), "不足 0.01"},
		{"no bids", "3", "0.01", nil, "没有买单"},
		{"bad tick", "3", "", levels("0.5", "1"), "tick_size"},
	}
	for _, testCase := range cases {
		_, reason := planSell(rat(t, testCase.shares), testCase.tick, testCase.bids, 0)
		if !strings.Contains(reason, testCase.want) {
			t.Fatalf("%s: reason %q, want %q", testCase.name, reason, testCase.want)
		}
	}
}

func TestPlanSellSlippageNeverGoesBelowOneTick(t *testing.T) {
	plan, reason := planSell(rat(t, "2"), "0.01", levels("0.02", "10"), 5)
	if reason != "" || plan.Price != "0.01" {
		t.Fatalf("price %s reason %q", plan.Price, reason)
	}
	if formatRat(plan.FillableShares, 6) != "2" {
		t.Fatalf("bid above limit must count as fillable, got %s", formatRat(plan.FillableShares, 6))
	}
}

func TestExpectedRedemption(t *testing.T) {
	payout := polymarket.ConditionPayout{Denominator: big.NewInt(1), Numerators: [2]*big.Int{big.NewInt(0), big.NewInt(1)}}
	got := expectedRedemption([2]*big.Int{big.NewInt(3_000_000), big.NewInt(1_980_000)}, payout)
	if formatBaseUnits(got) != "1.98" {
		t.Fatalf("payout = %s", formatBaseUnits(got))
	}
	half := polymarket.ConditionPayout{Denominator: big.NewInt(2), Numerators: [2]*big.Int{big.NewInt(1), big.NewInt(1)}}
	if got := expectedRedemption([2]*big.Int{big.NewInt(1_000_000), big.NewInt(0)}, half); formatBaseUnits(got) != "0.5" {
		t.Fatalf("50/50 payout = %s", formatBaseUnits(got))
	}
	if expectedRedemption([2]*big.Int{big.NewInt(1), big.NewInt(1)}, polymarket.ConditionPayout{Denominator: big.NewInt(0)}) != nil {
		t.Fatalf("unresolved condition must have no expected payout")
	}
}

func TestParseOptions(t *testing.T) {
	opts, err := parseOptions([]string{"--account", "wallet-2"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.execute || opts.only != onlyAll || opts.sellSlippageTicks != 0 || opts.confirmations != 3 || opts.txTimeout != 5*time.Minute {
		t.Fatalf("defaults = %+v (must be a dry run)", opts)
	}
	opts, err = parseOptions([]string{"--account=wallet-3", "--execute", "--only", "REDEEM", "--sell-slippage-ticks", "2"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.execute || opts.only != onlyRedeem || opts.sellSlippageTicks != 2 || opts.account != "wallet-3" {
		t.Fatalf("parsed = %+v", opts)
	}
	for _, arguments := range [][]string{
		{},
		{"--account", " "},
		{"--account", "wallet-2", "--only", "both"},
		{"--account", "wallet-2", "--sell-slippage-ticks", "-1"},
		{"--account", "wallet-2", "--sell-slippage-ticks", "21"},
		{"--account", "wallet-2", "extra"},
		{"--account", "wallet-2", "--private-key", "0xabc"},
		{"--account", "wallet-2", "--confirmations", "0"},
	} {
		if _, err := parseOptions(arguments); err == nil {
			t.Fatalf("arguments %v must be rejected", arguments)
		}
	}
}

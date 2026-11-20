package main

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const testWallet = "0x00000000000000000000000000000000000000e0"

// recorder collects every call; write calls are prefixed with "WRITE ".
type recorder struct{ calls []string }

func (rec *recorder) add(format string, args ...any) {
	rec.calls = append(rec.calls, fmt.Sprintf(format, args...))
}

func (rec *recorder) writes() []string {
	var result []string
	for _, call := range rec.calls {
		if strings.HasPrefix(call, "WRITE ") {
			result = append(result, call)
		}
	}
	return result
}

type fakePositions struct{ rows []domain.ExternalPosition }

func (fake fakePositions) ListExternalPositions(context.Context, string) ([]domain.ExternalPosition, error) {
	return fake.rows, nil
}

func (fake fakePositions) ListPositionMarks(context.Context, string) ([]polymarketdata.PositionMark, error) {
	marks := make([]polymarketdata.PositionMark, 0, len(fake.rows))
	for _, value := range fake.rows {
		marks = append(marks, polymarketdata.PositionMark{TokenID: value.TokenID, Title: "Title " + value.TokenID})
	}
	return marks, nil
}

type fakeBooks struct {
	bids map[string][]domain.PriceLevel
}

func (fake fakeBooks) Capture(_ context.Context, _ time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	books := make([]domain.OrderBookSnapshot, 0, len(targets))
	for _, target := range targets {
		books = append(books, domain.OrderBookSnapshot{
			TokenID: target.TokenID, Status: domain.OrderBookStatusEmpty, TickSize: "0.01", MinOrderSize: "5",
			Bids: fake.bids[target.TokenID],
		})
	}
	return books, nil
}

type fakeCLOB struct {
	rec        *recorder
	placeError map[string]error
}

func (fake *fakeCLOB) ProbeAccount(context.Context, string) (polymarket.AccountProbe, error) {
	return polymarket.AccountProbe{}, nil
}
func (fake *fakeCLOB) GetTickSize(context.Context, string) (domain.Decimal, error) {
	return "0.01", nil
}
func (fake *fakeCLOB) GetNegRisk(_ context.Context, tokenID string) (bool, error) {
	return tokenID == "neg-open", nil
}
func (fake *fakeCLOB) UpdateBalanceAllowance(_ context.Context, _ string, _ polymarket.BalanceAssetType, tokenID string) error {
	fake.rec.add("WRITE clob.refresh %s", tokenID)
	return nil
}
func (fake *fakeCLOB) Place(_ context.Context, order domain.Order) (port.VenueOrder, error) {
	fake.rec.add("WRITE clob.place %s %s %s@%s %s neg=%t", order.Intent.TokenID, order.Intent.Side, order.Intent.Size,
		order.Intent.Price, order.Intent.TimeInForce, order.MarketValidation.NegRisk)
	if err := fake.placeError[order.Intent.TokenID]; err != nil {
		return port.VenueOrder{}, err
	}
	return port.VenueOrder{ID: "0xorder-" + order.Intent.TokenID, State: port.VenueOrderFilled, FilledSize: order.Intent.Size}, nil
}

type fakeChain struct {
	rec             *recorder
	balances        map[string]int64
	adapterApproved map[bool]bool
	exchangeOK      bool
	// adapterTokens maps condition -> the two position ids the adapter derives.
	adapterTokens map[string][2]string
	resolved      bool
}

func (fake *fakeChain) NativeBalance(context.Context, string) (*big.Int, error) {
	return big.NewInt(5e17), nil
}
func (fake *fakeChain) CTFBalance(_ context.Context, _ string, tokenID string) (*big.Int, error) {
	fake.rec.add("chain.balance %s", tokenID)
	return big.NewInt(fake.balances[tokenID]), nil
}
func (fake *fakeChain) ConditionPayout(context.Context, string) (polymarket.ConditionPayout, error) {
	if !fake.resolved {
		return polymarket.ConditionPayout{Denominator: big.NewInt(0)}, nil
	}
	return polymarket.ConditionPayout{Denominator: big.NewInt(1), Numerators: [2]*big.Int{big.NewInt(1), big.NewInt(0)}}, nil
}
func (fake *fakeChain) AdapterPositions(_ context.Context, conditionID string, negRisk bool) (polymarket.AdapterPositions, error) {
	return polymarket.AdapterPositions{
		Adapter: polymarket.RedemptionAdapterAddress(negRisk), Collateral: "0xcollateral", PositionIDs: fake.adapterTokens[conditionID],
	}, nil
}
func (fake *fakeChain) ReadTradingApprovals(_ context.Context, _ string, approvals []polymarket.TradingApproval) ([]polymarket.TradingApprovalState, error) {
	states := make([]polymarket.TradingApprovalState, 0, len(approvals))
	for _, approval := range approvals {
		states = append(states, polymarket.TradingApprovalState{TradingApproval: approval, Satisfied: fake.exchangeOK})
	}
	return states, nil
}
func (fake *fakeChain) RedemptionApproved(_ context.Context, _ string, negRisk bool) (bool, error) {
	return fake.adapterApproved[negRisk], nil
}
func (fake *fakeChain) SimulateRedemption(_ context.Context, _ string, conditionID string, _ bool) error {
	fake.rec.add("chain.simulate %s", conditionID)
	return nil
}
func (fake *fakeChain) SubmitEOAOperatorApproval(_ context.Context, _ string, operator string) (domain.RedemptionSubmission, error) {
	fake.rec.add("WRITE chain.approveOperator %s", operator)
	return domain.RedemptionSubmission{Provider: "POLYGON_RPC", Reference: "0xapprove", TransactionHash: "0xapprove"}, nil
}
func (fake *fakeChain) SubmitRedemptionApproval(_ context.Context, _ string, negRisk bool) (domain.RedemptionSubmission, error) {
	fake.rec.add("WRITE chain.approveAdapter neg=%t", negRisk)
	fake.adapterApproved[negRisk] = true
	return domain.RedemptionSubmission{Provider: "POLYGON_RPC", Reference: "0xadapter", TransactionHash: "0xadapter"}, nil
}
func (fake *fakeChain) SubmitRedemption(_ context.Context, _ string, conditionID string, negRisk bool) (domain.RedemptionSubmission, error) {
	fake.rec.add("WRITE chain.redeem %s neg=%t", conditionID, negRisk)
	return domain.RedemptionSubmission{Provider: "POLYGON_RPC", Reference: "0xredeem", TransactionHash: "0xredeem"}, nil
}
func (fake *fakeChain) ResolveRedemptionSubmission(_ context.Context, _ string, submission domain.RedemptionSubmission) (domain.RedemptionSubmission, error) {
	fake.rec.add("chain.resolve %s", submission.TransactionHash)
	submission.State = domain.RedemptionSubmissionConfirmed
	return submission, nil
}

// fixture: one standard open position, one neg-risk open position, and one
// settled condition held on both outcomes plus a zero-value loser.
func newFixture(execute bool) (*runner, *recorder, *fakeCLOB, *fakeChain, *bytes.Buffer) {
	rec := &recorder{}
	condition := "0x" + strings.Repeat("ab", 32)
	positions := fakePositions{rows: []domain.ExternalPosition{
		{TokenID: "std-open", ConditionID: "0x01", OutcomeName: "Yes", Shares: "6.93", CurrentPrice: "0.4"},
		{TokenID: "neg-open", ConditionID: "0x02", OutcomeName: "No", Shares: "1.98", CurrentPrice: "0.1", NegRisk: true},
		{TokenID: "won", ConditionID: condition, OutcomeName: "Yes", Shares: "3", CurrentPrice: "1", Redeemable: true},
		{TokenID: "lost", ConditionID: condition, OutcomeName: "No", Shares: "2", CurrentPrice: "0", Redeemable: true},
	}}
	clob := &fakeCLOB{rec: rec, placeError: map[string]error{}}
	chain := &fakeChain{
		rec:             rec,
		balances:        map[string]int64{"std-open": 6_930_000, "neg-open": 1_980_000, "won": 3_000_000, "lost": 2_000_000},
		adapterApproved: map[bool]bool{},
		adapterTokens:   map[string][2]string{condition: {"won", "lost"}},
		resolved:        true,
	}
	books := fakeBooks{bids: map[string][]domain.PriceLevel{
		"std-open": levels("0.40", "100"),
		"neg-open": levels("0.10", "100"),
	}}
	out := &bytes.Buffer{}
	work := &runner{
		out: out, opts: options{account: "wallet-2", execute: execute, txTimeout: time.Minute},
		accountID: "wallet-2", wallet: testWallet,
		positions: positions, books: books, clob: clob, chain: chain,
		pollInterval: time.Millisecond, sleep: func(context.Context, time.Duration) error { return nil }, now: time.Now,
	}
	return work, rec, clob, chain, out
}

func TestDryRunNeverCallsWriteMethods(t *testing.T) {
	work, rec, _, _, out := newFixture(false)
	result, err := work.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if writes := rec.writes(); len(writes) != 0 {
		t.Fatalf("dry run called write methods: %v", writes)
	}
	if result.Sells != 2 || result.Redeems != 2 || len(result.Failures) != 0 {
		t.Fatalf("summary = %+v", result)
	}
	simulations := 0
	for _, call := range rec.calls {
		if strings.HasPrefix(call, "chain.simulate") {
			simulations++
		}
	}
	if simulations != 1 {
		t.Fatalf("one condition must be simulated exactly once, got %d", simulations)
	}
	text := out.String()
	for _, want := range []string{"[SELL] Title std-open", "[REDEEM] Title won", "价值为 0", "预计到手 3 pUSD", "未授权", "干跑"} {
		if !strings.Contains(text, want) {
			t.Fatalf("dry-run output lacks %q:\n%s", want, text)
		}
	}
}

func TestExecuteSellsFirstRedeemsEachConditionOnceAndDoesNotResubmitUnknown(t *testing.T) {
	work, rec, clob, _, out := newFixture(true)
	clob.placeError["neg-open"] = &port.VenueError{Kind: port.VenueErrorAmbiguous, Code: "CLOB_TRANSPORT_OUTCOME_UNKNOWN", VenueOrderID: "0xexpected"}
	result, err := work.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writes := rec.writes()
	want := []string{
		"WRITE chain.approveOperator " + strings.ToLower(polymarket.StandardExchangeV2Address),
		"WRITE chain.approveOperator " + strings.ToLower(polymarket.NegRiskExchangeV2Address),
		"WRITE chain.approveOperator " + polymarket.NegRiskAdapterV1Address,
		"WRITE clob.refresh std-open",
		"WRITE clob.place std-open SELL 6.93@0.4 FAK neg=false",
		"WRITE clob.refresh neg-open",
		"WRITE clob.place neg-open SELL 1.98@0.1 FAK neg=true",
		"WRITE chain.approveAdapter neg=false",
		"WRITE chain.redeem 0x" + strings.Repeat("ab", 32) + " neg=false",
	}
	if strings.Join(writes, "\n") != strings.Join(want, "\n") {
		t.Fatalf("write sequence:\n%s\nwant:\n%s", strings.Join(writes, "\n"), strings.Join(want, "\n"))
	}
	if len(result.Failures) != 1 || !strings.Contains(result.Failures[0], "UNKNOWN") || !strings.Contains(result.Failures[0], "0xexpected") {
		t.Fatalf("failures = %v", result.Failures)
	}
	if !strings.Contains(out.String(), "失败 1 项") {
		t.Fatalf("summary must list the failure:\n%s", out.String())
	}
}

func TestRedeemSkipsTokenTheAdapterCannotMove(t *testing.T) {
	work, rec, _, chain, out := newFixture(true)
	condition := "0x" + strings.Repeat("ab", 32)
	chain.adapterTokens[condition] = [2]string{"other-yes", "other-no"}
	work.opts.only = onlyRedeem
	result, err := work.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if writes := rec.writes(); len(writes) != 0 {
		t.Fatalf("mismatched adapter positions must not be redeemed: %v", writes)
	}
	if result.Redeems != 0 || !strings.Contains(out.String(), "不在 adapter 推导的 position id 中") {
		t.Fatalf("result %+v output:\n%s", result, out.String())
	}
}

func TestRedeemSkipsUnresolvedCondition(t *testing.T) {
	work, rec, _, chain, out := newFixture(true)
	chain.resolved = false
	work.opts.only = onlyRedeem
	if _, err := work.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if writes := rec.writes(); len(writes) != 0 {
		t.Fatalf("unresolved condition must not be redeemed: %v", writes)
	}
	if !strings.Contains(out.String(), "尚未结算") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestSellSkipsWhenChainBalanceIsZero(t *testing.T) {
	work, rec, _, chain, out := newFixture(true)
	chain.balances["std-open"] = 0
	chain.exchangeOK = true
	work.opts.only = onlySell
	if _, err := work.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, call := range rec.writes() {
		if strings.Contains(call, "std-open") {
			t.Fatalf("zero on-chain balance must not be sold: %s", call)
		}
	}
	if !strings.Contains(out.String(), "链上 CTF 余额为 0") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestWaitConfirmedTimesOutWithoutResubmitting(t *testing.T) {
	work, rec, _, _, _ := newFixture(true)
	pending := &pendingChain{fakeChain: work.chain.(*fakeChain)}
	work.chain = pending
	clock := time.Unix(0, 0)
	work.now = func() time.Time { return clock }
	work.sleep = func(context.Context, time.Duration) error { clock = clock.Add(20 * time.Second); return nil }
	_, err := work.waitConfirmed(context.Background(), domain.RedemptionSubmission{Provider: "POLYGON_RPC", Reference: "0xslow", TransactionHash: "0xslow"})
	if err == nil || !strings.Contains(err.Error(), "不要重发") {
		t.Fatalf("timeout error = %v", err)
	}
	if len(rec.writes()) != 0 {
		t.Fatalf("waiting must never submit: %v", rec.writes())
	}
}

type pendingChain struct{ *fakeChain }

func (pendingChain) ResolveRedemptionSubmission(_ context.Context, _ string, submission domain.RedemptionSubmission) (domain.RedemptionSubmission, error) {
	submission.State = domain.RedemptionSubmissionPending
	return submission, nil
}

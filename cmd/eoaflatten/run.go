package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// positionLister is the read-only Data API v2 positions source.
type positionLister interface {
	ListExternalPositions(ctx context.Context, walletAddress string) ([]domain.ExternalPosition, error)
	ListPositionMarks(ctx context.Context, walletAddress string) ([]polymarketdata.PositionMark, error)
}

// bookSource reads public CLOB order books.
type bookSource interface {
	Capture(ctx context.Context, at time.Time, targets []domain.BookTarget) ([]domain.OrderBookSnapshot, error)
}

// clobClient is the CLOB surface the tool uses. Place and
// UpdateBalanceAllowance are only called with --execute.
type clobClient interface {
	ProbeAccount(ctx context.Context, executionAccountID string) (polymarket.AccountProbe, error)
	GetTickSize(ctx context.Context, tokenID string) (domain.Decimal, error)
	GetNegRisk(ctx context.Context, tokenID string) (bool, error)
	UpdateBalanceAllowance(ctx context.Context, executionAccountID string, assetType polymarket.BalanceAssetType, tokenID string) error
	Place(ctx context.Context, order domain.Order) (port.VenueOrder, error)
}

// chainClient is the Polygon surface. The Submit* methods sign and broadcast
// and are only called with --execute.
type chainClient interface {
	NativeBalance(ctx context.Context, walletAddress string) (*big.Int, error)
	CTFBalance(ctx context.Context, walletAddress, tokenID string) (*big.Int, error)
	ConditionPayout(ctx context.Context, conditionID string) (polymarket.ConditionPayout, error)
	AdapterPositions(ctx context.Context, conditionID string, negRisk bool) (polymarket.AdapterPositions, error)
	ReadTradingApprovals(ctx context.Context, wallet string, approvals []polymarket.TradingApproval) ([]polymarket.TradingApprovalState, error)
	RedemptionApproved(ctx context.Context, walletAddress string, negRisk bool) (bool, error)
	SimulateRedemption(ctx context.Context, walletAddress, conditionID string, negRisk bool) error
	SubmitEOAOperatorApproval(ctx context.Context, executionAccountID, operator string) (domain.RedemptionSubmission, error)
	SubmitRedemptionApproval(ctx context.Context, executionAccountID string, negRisk bool) (domain.RedemptionSubmission, error)
	SubmitRedemption(ctx context.Context, executionAccountID, conditionID string, negRisk bool) (domain.RedemptionSubmission, error)
	ResolveRedemptionSubmission(ctx context.Context, executionAccountID string, submission domain.RedemptionSubmission) (domain.RedemptionSubmission, error)
}

type runner struct {
	out       io.Writer
	opts      options
	accountID string
	wallet    string
	// openOrders is the CLOB open-order count from the authenticated probe.
	openOrders int

	positions positionLister
	books     bookSource
	clob      clobClient
	chain     chainClient

	pollInterval time.Duration
	sleep        func(context.Context, time.Duration) error
	now          func() time.Time
}

// operatorApproval is one CTF setApprovalForAll the plan depends on.
type operatorApproval struct {
	Operator  string
	Label     string
	Satisfied bool
	ReadErr   string
}

type flattenPlan struct {
	Rows          []*row
	Groups        []*redeemGroup
	SellApprovals []*operatorApproval
	NativeBalance *big.Int
}

// summary is the final tally; Failures is non-empty only after --execute.
type summary struct {
	Sells, Redeems, Skips int
	Failures              []string
}

func (r *runner) run(ctx context.Context) (summary, error) {
	mode := "干跑（不下单、不发链上交易）"
	if r.opts.execute {
		mode = "执行（--execute）"
	}
	fmt.Fprintf(r.out, "账户 %s  EOA %s  模式：%s\n", r.accountID, r.wallet, mode)
	fmt.Fprintf(r.out, "CLOB 凭据可用；该钱包当前挂单 %d 个", r.openOrders)
	if r.openOrders > 0 {
		fmt.Fprint(r.out, "（挂单会占用份额，可能导致卖出因余额不足被拒；本工具不撤单）")
	}
	fmt.Fprintln(r.out)
	plan, err := r.buildPlan(ctx)
	if err != nil {
		return summary{}, err
	}
	r.printPlan(plan)
	result := summary{}
	for _, current := range plan.Rows {
		switch current.Action {
		case actionSell:
			result.Sells++
		case actionRedeem:
			result.Redeems++
		default:
			result.Skips++
		}
	}
	if !r.opts.execute {
		fmt.Fprintf(r.out, "\n汇总（干跑）：SELL %d 条，REDEEM %d 条（%d 次赎回调用），SKIP %d 条。加 --execute 才会真正执行。\n",
			result.Sells, result.Redeems, countActiveGroups(plan.Groups), result.Skips)
		return result, nil
	}
	result.Failures = r.execute(ctx, plan)
	r.printExecutionSummary(plan, result)
	return result, nil
}

func countActiveGroups(groups []*redeemGroup) int {
	count := 0
	for _, group := range groups {
		if group.Skip == "" {
			count++
		}
	}
	return count
}

// buildPlan reads positions, books and chain state. It never calls a write
// method of clobClient or chainClient.
func (r *runner) buildPlan(ctx context.Context) (*flattenPlan, error) {
	external, err := r.positions.ListExternalPositions(ctx, r.wallet)
	if err != nil {
		return nil, fmt.Errorf("读取 Data API v2 持仓: %w", err)
	}
	titles := map[string]string{}
	if marks, markErr := r.positions.ListPositionMarks(ctx, r.wallet); markErr != nil {
		fmt.Fprintf(r.out, "提示：读取持仓标题失败（%v），标题留空\n", markErr)
	} else {
		for _, mark := range marks {
			titles[mark.TokenID] = mark.Title
		}
	}
	positions := make([]position, 0, len(external))
	for _, value := range external {
		positions = append(positions, position{
			Title: titles[value.TokenID], Outcome: value.OutcomeName, TokenID: value.TokenID,
			ConditionID: value.ConditionID, Shares: value.Shares, CurrentPrice: value.CurrentPrice,
			NegRisk: value.NegRisk, Redeemable: value.Redeemable,
		})
	}
	sort.SliceStable(positions, func(i, j int) bool {
		if positions[i].Redeemable != positions[j].Redeemable {
			return !positions[i].Redeemable
		}
		return positions[i].ConditionID < positions[j].ConditionID
	})
	rows, groups := classify(positions, r.opts.only)
	plan := &flattenPlan{Rows: rows, Groups: groups}
	if balance, balanceErr := r.chain.NativeBalance(ctx, r.wallet); balanceErr == nil {
		plan.NativeBalance = balance
	} else {
		fmt.Fprintf(r.out, "提示：读取 POL 余额失败：%v\n", balanceErr)
	}
	for _, current := range rows {
		if current.Action == actionSell {
			r.planSellRow(ctx, current)
		}
	}
	plan.SellApprovals = r.readSellApprovals(ctx, rows)
	for _, group := range groups {
		if group.Skip == "" {
			r.planRedeemGroup(ctx, group)
		}
	}
	return plan, nil
}

func (r *runner) planSellRow(ctx context.Context, current *row) {
	shares, ok := decimalToRat(current.Pos.Shares)
	if !ok {
		current.skip("Data API 份额 %q 无法解析", current.Pos.Shares)
		return
	}
	if balance, err := r.chain.CTFBalance(ctx, r.wallet, current.Pos.TokenID); err != nil {
		current.Notes = append(current.Notes, fmt.Sprintf("链上余额读取失败（%v），按 Data API 份额计划", err))
	} else {
		current.ChainShares = balance
		if balance.Sign() == 0 {
			current.skip("链上 CTF 余额为 0（可能不是 CTF 仓位或已清），不处理")
			return
		}
		shares = baseUnitsRat(balance)
	}
	tick, err := r.clob.GetTickSize(ctx, current.Pos.TokenID)
	if err != nil {
		current.skip("读取 tick_size 失败：%v", err)
		return
	}
	negRisk, err := r.clob.GetNegRisk(ctx, current.Pos.TokenID)
	if err != nil {
		current.skip("读取 neg_risk 失败：%v", err)
		return
	}
	if negRisk != current.Pos.NegRisk {
		current.Notes = append(current.Notes, fmt.Sprintf("CLOB neg_risk=%t 与 Data API 不一致，以 CLOB 为准", negRisk))
	}
	current.TickSize, current.SellNegRisk = tick, negRisk
	books, err := r.books.Capture(ctx, r.now(), []domain.BookTarget{{
		MarketID: current.Pos.ConditionID, ConditionID: current.Pos.ConditionID, TokenID: current.Pos.TokenID,
	}})
	if err != nil || len(books) != 1 {
		current.skip("读取订单簿失败：%v", err)
		return
	}
	book := books[0]
	if book.Status == domain.OrderBookStatusError {
		current.skip("读取订单簿失败：%s", book.ErrorCode)
		return
	}
	if !book.TickSize.IsEmpty() && !book.TickSize.Equal(tick) {
		current.Notes = append(current.Notes, fmt.Sprintf("订单簿 tick %s 与 /tick-size %s 不一致，以 /tick-size 为准", book.TickSize, tick))
	}
	current.MinOrderSize = book.MinOrderSize
	plan, reason := planSell(shares, tick, book.Bids, r.opts.sellSlippageTicks)
	if reason != "" {
		current.skip("%s", reason)
		return
	}
	current.Sell = &plan
	if !book.MinOrderSize.IsEmpty() {
		if comparison, compareErr := plan.Size.Compare(book.MinOrderSize); compareErr == nil && comparison < 0 {
			current.Notes = append(current.Notes, fmt.Sprintf("小于 min_order_size %s，照常尝试", book.MinOrderSize))
		}
	}
	if plan.FillableShares.Cmp(mustRat(plan.Size)) < 0 {
		current.Notes = append(current.Notes, "盘口深度不足，FAK 只会部分成交")
	}
}

func (r *runner) readSellApprovals(ctx context.Context, rows []*row) []*operatorApproval {
	needed := map[string]string{}
	order := []string{}
	for _, current := range rows {
		if current.Action != actionSell {
			continue
		}
		for _, operator := range sellOperators(current.SellNegRisk) {
			if _, exists := needed[operator]; !exists {
				needed[operator] = operatorLabel(operator)
				order = append(order, operator)
			}
		}
	}
	if len(order) == 0 {
		return nil
	}
	requests := make([]polymarket.TradingApproval, 0, len(order))
	for _, operator := range order {
		requests = append(requests, polymarket.TradingApproval{Kind: polymarket.ApprovalCTFOperator, Spender: operator})
	}
	approvals := make([]*operatorApproval, 0, len(order))
	states, err := r.chain.ReadTradingApprovals(ctx, r.wallet, requests)
	for index, operator := range order {
		approval := &operatorApproval{Operator: operator, Label: needed[operator]}
		if err != nil {
			approval.ReadErr = err.Error()
		} else if index < len(states) {
			approval.Satisfied = states[index].Satisfied
		}
		approvals = append(approvals, approval)
	}
	return approvals
}

func (r *runner) planRedeemGroup(ctx context.Context, group *redeemGroup) {
	positions, err := r.chain.AdapterPositions(ctx, group.ConditionID, group.NegRisk)
	if err != nil {
		group.skip("读取 adapter 的 position id 失败：%v", err)
		return
	}
	group.Adapter, group.Collateral, group.PositionIDs = positions.Adapter, positions.Collateral, positions.PositionIDs
	for _, member := range group.Rows {
		if member.Pos.TokenID != positions.PositionIDs[0] && member.Pos.TokenID != positions.PositionIDs[1] {
			group.skip("token %s 不在 adapter 推导的 position id 中（抵押品 %s），adapter 赎不到它", shortID(member.Pos.TokenID), positions.Collateral)
			return
		}
	}
	payout, err := r.chain.ConditionPayout(ctx, group.ConditionID)
	if err != nil {
		group.skip("读取链上结算结果失败：%v", err)
		return
	}
	group.Payout = payout
	if !payout.Resolved() {
		group.skip("链上 condition 尚未结算（payoutDenominator=0），现在赎回会回滚")
		return
	}
	for index, tokenID := range positions.PositionIDs {
		balance, balanceErr := r.chain.CTFBalance(ctx, r.wallet, tokenID)
		if balanceErr != nil {
			group.skip("读取链上余额失败：%v", balanceErr)
			return
		}
		group.Balances[index] = balance
		for _, member := range group.Rows {
			if member.Pos.TokenID == tokenID {
				member.ChainShares = balance
			}
		}
	}
	if group.Balances[0].Sign() == 0 && group.Balances[1].Sign() == 0 {
		group.skip("链上两个 outcome 余额都为 0（可能已赎回），不再调用")
		return
	}
	group.ExpectedPayout = expectedRedemption(group.Balances, payout)
	approved, err := r.chain.RedemptionApproved(ctx, r.wallet, group.NegRisk)
	if err != nil {
		fmt.Fprintf(r.out, "提示：读取 adapter 授权失败（%s）：%v\n", shortID(group.ConditionID), err)
	} else {
		group.Approved = &approved
	}
	if simErr := r.chain.SimulateRedemption(ctx, r.wallet, group.ConditionID, group.NegRisk); simErr != nil {
		group.Simulation = "会回滚：" + simErr.Error()
		if group.Approved != nil && !*group.Approved {
			group.Simulation += "（尚未授权 adapter，预期如此；--execute 会先发授权，再在发送前重新模拟）"
		}
	} else {
		group.Simulation = "成功（不回滚）"
	}
}

func (r *runner) printPlan(plan *flattenPlan) {
	if plan.NativeBalance != nil {
		fmt.Fprintf(r.out, "POL 余额：%s（付 gas）\n", formatRat(new(big.Rat).SetFrac(plan.NativeBalance, big.NewInt(1e18)), 6))
	}
	fmt.Fprintf(r.out, "\n持仓 %d 条：\n", len(plan.Rows))
	for index, current := range plan.Rows {
		r.printRow(index+1, current)
	}
	if len(plan.SellApprovals) > 0 {
		fmt.Fprintln(r.out, "\n卖出所需的 CTF 授权（setApprovalForAll）：")
		for _, approval := range plan.SellApprovals {
			fmt.Fprintf(r.out, "  %s %s：%s\n", approval.Label, approval.Operator, approvalText(approval.Satisfied, approval.ReadErr))
		}
	}
	activeGroups := 0
	for _, group := range plan.Groups {
		if group.Skip != "" {
			continue
		}
		if activeGroups == 0 {
			fmt.Fprintln(r.out, "\n赎回调用（同一 condition 只调用一次）：")
		}
		activeGroups++
		fmt.Fprintf(r.out, "  condition %s  %s adapter %s  抵押品 %s\n", group.ConditionID, marketKind(group.NegRisk), group.Adapter, group.Collateral)
		fmt.Fprintf(r.out, "    链上余额 outcome1=%s outcome2=%s  结算 %s/%s,%s/%s  预计到手 %s pUSD\n",
			formatBaseUnits(group.Balances[0]), formatBaseUnits(group.Balances[1]),
			bigText(group.Payout.Numerators[0]), bigText(group.Payout.Denominator),
			bigText(group.Payout.Numerators[1]), bigText(group.Payout.Denominator),
			formatBaseUnits(group.ExpectedPayout))
		approved := "未知"
		if group.Approved != nil {
			approved = approvalText(*group.Approved, "")
		}
		fmt.Fprintf(r.out, "    adapter 授权：%s  模拟（eth_call from EOA）：%s\n", approved, group.Simulation)
	}
}

func (r *runner) printRow(index int, current *row) {
	pos := current.Pos
	title := pos.Title
	if title == "" {
		title = "（无标题）"
	}
	status := "OPEN"
	if pos.Redeemable {
		status = "REDEEMABLE"
	}
	shares := pos.Shares.String()
	if current.ChainShares != nil && formatBaseUnits(current.ChainShares) != pos.Shares.Canonical().String() {
		shares += "（链上 " + formatBaseUnits(current.ChainShares) + "）"
	}
	fmt.Fprintf(r.out, "%2d. [%s] %s | %s | token %s | condition %s | 份额 %s | %s | 现价 %s | %s\n",
		index, current.Action, title, pos.Outcome, shortID(pos.TokenID), pos.ConditionID, shares, status,
		pos.CurrentPrice, marketKind(pos.NegRisk))
	switch current.Action {
	case actionSell:
		plan := current.Sell
		fmt.Fprintf(r.out, "      计划：FAK 卖出 %s 股，限价 %s（tick %s，下调 %d tick）；盘口可成交 %s 股，预计到手 ≈ %s pUSD（未扣手续费）",
			plan.Size, plan.Price, current.TickSize, r.opts.sellSlippageTicks,
			formatRat(plan.FillableShares, 6), formatRat(plan.ExpectedProceeds, 6))
		if plan.Dust.Sign() > 0 {
			fmt.Fprintf(r.out, "；剩余 %s 股尘埃", formatRat(plan.Dust, 6))
		}
		fmt.Fprintln(r.out)
	case actionRedeem:
		fmt.Fprintf(r.out, "      计划：经 %s 赎回（见下方赎回调用）\n", marketKind(current.Group.NegRisk)+" adapter")
	default:
		fmt.Fprintf(r.out, "      跳过：%s\n", current.Reason)
	}
	for _, note := range current.Notes {
		fmt.Fprintf(r.out, "      注意：%s\n", note)
	}
}

// execute sells first, then redeems. Every item gets one attempt; a failure
// is recorded and the next item continues. Nothing is retried, and an order
// whose outcome is unknown is never resubmitted.
func (r *runner) execute(ctx context.Context, plan *flattenPlan) []string {
	var failures []string
	fail := func(format string, args ...any) {
		message := fmt.Sprintf(format, args...)
		failures = append(failures, message)
		fmt.Fprintf(r.out, "  失败：%s\n", message)
	}
	sells := make([]*row, 0)
	for _, current := range plan.Rows {
		if current.Action == actionSell {
			sells = append(sells, current)
		}
	}
	if len(sells) > 0 {
		fmt.Fprintln(r.out, "\n== 卖出 ==")
		blocked := map[string]string{}
		for _, approval := range plan.SellApprovals {
			if approval.Satisfied {
				continue
			}
			fmt.Fprintf(r.out, "授权 %s %s ...\n", approval.Label, approval.Operator)
			submission, err := r.chain.SubmitEOAOperatorApproval(ctx, r.accountID, approval.Operator)
			if err == nil {
				submission, err = r.waitConfirmed(ctx, submission)
			}
			if err != nil {
				blocked[approval.Operator] = err.Error()
				fail("授权 %s 失败：%v", approval.Label, err)
				continue
			}
			fmt.Fprintf(r.out, "  已确认 tx %s\n", submission.TransactionHash)
		}
		for index, current := range sells {
			r.executeSell(ctx, index, current, blocked, fail)
		}
	}
	groups := make([]*redeemGroup, 0)
	for _, group := range plan.Groups {
		if group.Skip == "" {
			groups = append(groups, group)
		}
	}
	if len(groups) > 0 {
		fmt.Fprintln(r.out, "\n== 赎回 ==")
		approvalFailed := map[bool]string{}
		for _, negRisk := range []bool{false, true} {
			if !needsAdapterApproval(groups, negRisk) {
				continue
			}
			approved, err := r.chain.RedemptionApproved(ctx, r.wallet, negRisk)
			if err == nil && approved {
				continue
			}
			label := marketKind(negRisk) + " adapter " + polymarket.RedemptionAdapterAddress(negRisk)
			fmt.Fprintf(r.out, "授权 %s ...\n", label)
			submission, submitErr := r.chain.SubmitRedemptionApproval(ctx, r.accountID, negRisk)
			if submitErr == nil {
				submission, submitErr = r.waitConfirmed(ctx, submission)
			}
			if submitErr != nil {
				approvalFailed[negRisk] = submitErr.Error()
				fail("授权 %s 失败：%v", label, submitErr)
				continue
			}
			fmt.Fprintf(r.out, "  已确认 tx %s\n", submission.TransactionHash)
		}
		for _, group := range groups {
			r.executeRedeem(ctx, group, approvalFailed, fail)
		}
	}
	return failures
}

func needsAdapterApproval(groups []*redeemGroup, negRisk bool) bool {
	for _, group := range groups {
		if group.NegRisk == negRisk {
			return true
		}
	}
	return false
}

func (r *runner) executeSell(ctx context.Context, index int, current *row, blocked map[string]string, fail func(string, ...any)) {
	label := fmt.Sprintf("卖出 %s %s（token %s）", titleOr(current.Pos), current.Pos.Outcome, shortID(current.Pos.TokenID))
	fmt.Fprintf(r.out, "%s：FAK %s 股 @ %s\n", label, current.Sell.Size, current.Sell.Price)
	for _, operator := range sellOperators(current.SellNegRisk) {
		if reason, isBlocked := blocked[operator]; isBlocked {
			current.Failed, current.Result = true, "授权失败未下单"
			fail("%s：所需授权 %s 失败（%s），未下单", label, operator, reason)
			return
		}
	}
	if err := r.clob.UpdateBalanceAllowance(ctx, r.accountID, polymarket.BalanceAssetConditional, current.Pos.TokenID); err != nil {
		fmt.Fprintf(r.out, "  提示：刷新 CLOB 余额/授权缓存失败（%v），照常下单\n", err)
	}
	orderID := fmt.Sprintf("eoaflatten-%s-%d-%d", r.accountID, r.now().UTC().UnixMilli(), index+1)
	order := domain.Order{
		ID: orderID,
		Intent: domain.OrderIntent{
			ExecutionAccountID: r.accountID, ClientOrderID: orderID, Venue: "polymarket",
			MarketID: current.Pos.ConditionID, ConditionID: current.Pos.ConditionID, TokenID: current.Pos.TokenID,
			OutcomeName: current.Pos.Outcome, Side: domain.SideSell, Type: domain.OrderTypeLimit,
			Price: current.Sell.Price, Size: current.Sell.Size, TimeInForce: domain.TimeInForceFAK,
		},
		MarketValidation: &domain.MarketValidation{
			Mode: "EOA_FLATTEN", ValidatedAt: r.now().UTC(), TokenID: current.Pos.TokenID,
			NegRisk: current.SellNegRisk, TickSize: current.TickSize, MinOrderSize: current.MinOrderSize,
			WorstPrice: current.Sell.Price,
		},
		Status: domain.OrderStatusSubmitting,
	}
	venueOrder, err := r.clob.Place(ctx, order)
	if err != nil {
		var venueError *port.VenueError
		if errors.As(err, &venueError) && venueError.Kind == port.VenueErrorAmbiguous {
			current.Failed, current.Result = true, "UNKNOWN"
			fail("%s：结果不明（UNKNOWN，%s），订单号 %s；不重发，请人工查该订单", label, venueError.Code, venueError.VenueOrderID)
			return
		}
		current.Failed, current.Result = true, "REJECTED"
		fail("%s：被拒 %v", label, err)
		return
	}
	current.Result = string(venueOrder.State)
	fmt.Fprintf(r.out, "  订单 %s 状态 %s 成交 %s 股 均价 %s\n", venueOrder.ID, venueOrder.State, venueOrder.FilledSize, decimalOr(venueOrder.AverageFillPrice, "?"))
	switch venueOrder.State {
	case port.VenueOrderFilled, port.VenueOrderPartiallyFilled:
	case port.VenueOrderUnknown:
		current.Failed = true
		fail("%s：订单 %s 状态 UNKNOWN（%s）；不重发，请人工查", label, venueOrder.ID, venueOrder.RawStatus)
	default:
		if sign, _ := venueOrder.FilledSize.Sign(); sign <= 0 {
			current.Failed = true
			fail("%s：订单 %s 状态 %s，未成交", label, venueOrder.ID, venueOrder.State)
		}
	}
}

func (r *runner) executeRedeem(ctx context.Context, group *redeemGroup, approvalFailed map[bool]string, fail func(string, ...any)) {
	label := fmt.Sprintf("赎回 condition %s（%s）", group.ConditionID, marketKind(group.NegRisk))
	fmt.Fprintf(r.out, "%s：预计 %s pUSD\n", label, formatBaseUnits(group.ExpectedPayout))
	if reason, failed := approvalFailed[group.NegRisk]; failed {
		group.Failed, group.Result = true, "授权失败未赎回"
		fail("%s：adapter 授权失败（%s），未发送", label, reason)
		return
	}
	submission, err := r.chain.SubmitRedemption(ctx, r.accountID, group.ConditionID, group.NegRisk)
	if err != nil {
		group.Failed, group.Result = true, "提交失败"
		fail("%s：提交失败 %v", label, err)
		return
	}
	group.TxHash = submission.TransactionHash
	fmt.Fprintf(r.out, "  已广播 tx %s，等待确认 ...\n", submission.TransactionHash)
	confirmed, err := r.waitConfirmed(ctx, submission)
	if err != nil {
		group.Failed, group.Result = true, "未确认"
		fail("%s：tx %s %v", label, submission.TransactionHash, err)
		return
	}
	group.Result = "已确认"
	fmt.Fprintf(r.out, "  已确认 tx %s\n", confirmed.TransactionHash)
	for index, tokenID := range group.PositionIDs {
		if balance, balanceErr := r.chain.CTFBalance(ctx, r.wallet, tokenID); balanceErr == nil && balance.Sign() != 0 {
			fmt.Fprintf(r.out, "  注意：确认后 outcome%d 链上余额仍为 %s\n", index+1, formatBaseUnits(balance))
		}
	}
}

// waitConfirmed polls the submission until it is confirmed, fails, or the
// per-transaction timeout passes. A timeout is a failure that must not be
// retried: the transaction may still be mined.
func (r *runner) waitConfirmed(ctx context.Context, submission domain.RedemptionSubmission) (domain.RedemptionSubmission, error) {
	deadline := r.now().Add(r.opts.txTimeout)
	for {
		resolved, err := r.chain.ResolveRedemptionSubmission(ctx, r.accountID, submission)
		if err == nil {
			switch resolved.State {
			case domain.RedemptionSubmissionConfirmed:
				return resolved, nil
			case domain.RedemptionSubmissionFailed:
				return resolved, fmt.Errorf("链上失败：%s（tx %s）", resolved.FailureReason, submission.TransactionHash)
			}
		}
		if !r.now().Before(deadline) {
			return submission, fmt.Errorf("等待 %s 仍未确认（tx %s 可能稍后上链，不要重发，请人工查）", r.opts.txTimeout, submission.TransactionHash)
		}
		if sleepErr := r.sleep(ctx, r.pollInterval); sleepErr != nil {
			return submission, fmt.Errorf("等待确认被中断（tx %s）：%w", submission.TransactionHash, sleepErr)
		}
	}
}

func (r *runner) printExecutionSummary(plan *flattenPlan, result summary) {
	fmt.Fprintln(r.out, "\n== 执行汇总 ==")
	for _, current := range plan.Rows {
		if current.Action == actionSell {
			fmt.Fprintf(r.out, "  SELL   %s %s：%s\n", titleOr(current.Pos), current.Pos.Outcome, current.Result)
		}
	}
	for _, group := range plan.Groups {
		if group.Skip == "" {
			fmt.Fprintf(r.out, "  REDEEM %s：%s %s\n", group.ConditionID, group.Result, group.TxHash)
		}
	}
	if len(result.Failures) == 0 {
		fmt.Fprintln(r.out, "全部成功。")
		return
	}
	fmt.Fprintf(r.out, "失败 %d 项：\n", len(result.Failures))
	for _, failure := range result.Failures {
		fmt.Fprintf(r.out, "  - %s\n", failure)
	}
}

func operatorLabel(operator string) string {
	switch strings.ToLower(operator) {
	case strings.ToLower(polymarket.StandardExchangeV2Address):
		return "CTF Exchange V2"
	case strings.ToLower(polymarket.NegRiskExchangeV2Address):
		return "Neg Risk CTF Exchange V2"
	case polymarket.NegRiskAdapterV1Address:
		return "Neg Risk Adapter (legacy)"
	default:
		return "operator"
	}
}

func approvalText(satisfied bool, readErr string) string {
	switch {
	case readErr != "":
		return "读取失败：" + readErr
	case satisfied:
		return "已授权"
	default:
		return "未授权（--execute 会先发 setApprovalForAll 交易）"
	}
}

func marketKind(negRisk bool) string {
	if negRisk {
		return "neg-risk"
	}
	return "standard"
}

func titleOr(pos position) string {
	if pos.Title != "" {
		return pos.Title
	}
	return pos.ConditionID
}

func bigText(value *big.Int) string {
	if value == nil {
		return "?"
	}
	return value.String()
}

func decimalOr(value domain.Decimal, fallback string) string {
	if value.IsEmpty() {
		return fallback
	}
	return value.String()
}

func mustRat(value domain.Decimal) *big.Rat {
	parsed, ok := decimalToRat(value)
	if !ok {
		return new(big.Rat)
	}
	return parsed
}

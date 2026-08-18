package reconciliation

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const defaultLookback = 48 * time.Hour

// OrderRefresher delegates all lifecycle changes to the existing audited
// state machine. Reconciliation never writes execution_orders directly.
type OrderRefresher interface {
	Refresh(context.Context, string) (domain.Order, error)
	FinalizeCancellation(context.Context, string) (domain.Order, error)
}

// Params 表示后端使用的 Params 类型。
type Params struct {
	Orders          port.ReconciliationOrderRepository
	Venue           port.VenueReconciliationSource
	PositionSources []port.ExternalPositionSource
	BalanceSources  []port.ExternalBalanceSource
	Ledger          port.ReconciliationLedger
	Fills           port.FillSynchronizer
	OrderRefresher  OrderRefresher
	Recorder        port.ReconciliationRecorder
	TradeLookback   time.Duration
	PositionEpsilon domain.Decimal
	BalanceEpsilon  domain.Decimal
	Now             func() time.Time
	NewID           func() string
}

// Service 表示后端使用的 Service 类型。
type Service struct {
	orders          port.ReconciliationOrderRepository
	venue           port.VenueReconciliationSource
	positionSources []port.ExternalPositionSource
	balanceSources  []port.ExternalBalanceSource
	ledger          port.ReconciliationLedger
	fills           port.FillSynchronizer
	orderRefresher  OrderRefresher
	recorder        port.ReconciliationRecorder
	tradeLookback   time.Duration
	positionEpsilon domain.Decimal
	balanceEpsilon  domain.Decimal
	now             func() time.Time
	newID           func() string
}

// Result 表示后端使用的 Result 类型。
type Result struct {
	Run    domain.ReconciliationRun     `json:"run"`
	Issues []domain.ReconciliationIssue `json:"issues"`
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.Orders == nil || params.Venue == nil || params.Ledger == nil || params.Fills == nil ||
		params.OrderRefresher == nil || params.Recorder == nil {
		return nil, fmt.Errorf("orders, venue, ledger, fills, order refresher, and recorder are required")
	}
	if len(params.PositionSources) == 0 || len(params.BalanceSources) == 0 {
		return nil, fmt.Errorf("at least one external position source and balance source are required")
	}
	for index, source := range params.PositionSources {
		if source == nil {
			return nil, fmt.Errorf("position source %d is nil", index)
		}
	}
	for index, source := range params.BalanceSources {
		if source == nil {
			return nil, fmt.Errorf("balance source %d is nil", index)
		}
	}
	if params.TradeLookback == 0 {
		params.TradeLookback = defaultLookback
	}
	if params.TradeLookback < time.Minute {
		return nil, fmt.Errorf("trade lookback must be at least one minute")
	}
	if params.PositionEpsilon.IsEmpty() {
		params.PositionEpsilon = "0.000001"
	}
	if params.BalanceEpsilon.IsEmpty() {
		params.BalanceEpsilon = "0.000001"
	}
	for name, epsilon := range map[string]domain.Decimal{
		"position epsilon": params.PositionEpsilon,
		"balance epsilon":  params.BalanceEpsilon,
	} {
		if sign, err := epsilon.Sign(); err != nil || sign < 0 {
			return nil, fmt.Errorf("%s must be a non-negative decimal", name)
		}
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.NewID == nil {
		params.NewID = newID
	}
	return &Service{
		orders: params.Orders, venue: params.Venue,
		positionSources: append([]port.ExternalPositionSource(nil), params.PositionSources...),
		balanceSources:  append([]port.ExternalBalanceSource(nil), params.BalanceSources...),
		ledger:          params.Ledger, fills: params.Fills, orderRefresher: params.OrderRefresher,
		recorder: params.Recorder, tradeLookback: params.TradeLookback,
		positionEpsilon: params.PositionEpsilon, balanceEpsilon: params.BalanceEpsilon,
		now: params.Now, newID: params.NewID,
	}, nil
}

// RunAccount 按证据优先顺序恢复成交并核对指定账户的订单、仓位和余额。
func (service *Service) RunAccount(
	ctx context.Context,
	executionAccountID string,
	trigger domain.ReconciliationTrigger,
	focusOrderID string,
) (Result, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	focusOrderID = strings.TrimSpace(focusOrderID)
	if executionAccountID == "" {
		return Result{}, fmt.Errorf("execution account id is required")
	}
	if !validTrigger(trigger) {
		return Result{}, fmt.Errorf("unsupported reconciliation trigger %q", trigger)
	}
	now := service.now().UTC()
	run, err := (domain.ReconciliationRunParams{
		RunID: service.newID(), ExecutionAccountID: executionAccountID,
		Trigger: trigger, FocusOrderID: focusOrderID, StartedAt: now,
	}).Build()
	if err != nil {
		return Result{}, err
	}
	if err := service.recorder.Start(ctx, run); err != nil {
		return Result{Run: run}, err
	}
	state := runState{service: service, run: &run, now: now}
	scanAfter := now.Add(-service.tradeLookback)
	if trigger == domain.ReconciliationTriggerStartup || trigger == domain.ReconciliationTriggerAssetDrift {
		// Startup and unexplained asset drift require a complete evidence scan.
		// Periodic and single-order runs use the overlap window for bounded QPS.
		scanAfter = time.Time{}
	}

	balance, err := service.ledger.GetBalance(ctx, executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_LEDGER", "read local execution account", err)
		return service.finish(ctx, state, errors.Join(err, errors.Join(state.errors...)))
	}
	orders, err := service.orders.ListForReconciliation(ctx, executionAccountID, scanAfter)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_ORDERS", "read local orders", err)
		return service.finish(ctx, state, errors.Join(err, errors.Join(state.errors...)))
	}
	state.run.Summary["local_orders"] = len(orders)

	openOrders, openErr := service.venue.ListReconciliationOpenOrders(ctx, executionAccountID)
	trades, tradesErr := service.venue.ListReconciliationTrades(ctx, executionAccountID, scanAfter)
	if openErr != nil {
		state.addInfrastructureIssue(ctx, "CLOB_OPEN_ORDERS", "read CLOB open orders", openErr)
	}
	if tradesErr != nil {
		state.addInfrastructureIssue(ctx, "CLOB_TRADES", "read Polymarket trades", tradesErr)
	}
	state.run.Summary["venue_open_orders"] = len(openOrders)
	state.run.Summary["venue_trades"] = len(trades)

	localByVenueID := make(map[string]domain.Order, len(orders))
	venueOrdersWithTrades := make(map[string]struct{})
	for _, order := range orders {
		if venueID := normalizedID(order.VenueOrderID); venueID != "" {
			localByVenueID[venueID] = order
		}
	}
	if openErr == nil {
		for _, remote := range openOrders {
			if _, owned := localByVenueID[normalizedID(remote.VenueOrderID)]; owned {
				continue
			}
			state.issue(ctx, domain.ReconciliationIssueParams{
				Type: domain.ReconciliationIssueExternalOrder, Resolution: domain.ReconciliationResolutionObserved,
				Status: domain.ReconciliationIssueOpen, VenueOrderID: remote.VenueOrderID,
				ConditionID: remote.ConditionID, TokenID: remote.TokenID, Source: "POLYMARKET_CLOB",
				Details: "wallet open order is not owned by this service; it is visible but will never be cancelled or modified",
			})
		}
	}
	if tradesErr == nil {
		for _, trade := range trades {
			for _, venueOrderID := range trade.OrderIDs {
				venueOrdersWithTrades[normalizedID(venueOrderID)] = struct{}{}
				if _, owned := localByVenueID[normalizedID(venueOrderID)]; owned {
					continue
				}
				state.issue(ctx, domain.ReconciliationIssueParams{
					Type: domain.ReconciliationIssueExternalTrade, Resolution: domain.ReconciliationResolutionManual,
					Status: domain.ReconciliationIssueOpen, VenueOrderID: venueOrderID,
					VenueTradeID: trade.VenueTradeID, ConditionID: trade.ConditionID, TokenID: trade.TokenID,
					Source:  "POLYMARKET_CLOB",
					Details: "wallet trade cannot be attributed to a local strategy order; do not manufacture a local fill",
				})
			}
		}
	}

	// Even if an account-level CLOB scan failed, per-order fill reads may still
	// succeed. They are safe read operations and fill identity is idempotent.
	for _, order := range prioritizeOrder(orders, focusOrderID) {
		if err := ctx.Err(); err != nil {
			state.errors = append(state.errors, err)
			break
		}
		if strings.TrimSpace(order.VenueOrderID) == "" {
			if order.Status == domain.OrderStatusSubmitting || order.Status == domain.OrderStatusUnknown ||
				order.Status == domain.OrderStatusReconciling || order.Status == domain.OrderStatusCancelPending {
				state.issue(ctx, domain.ReconciliationIssueParams{
					Type: domain.ReconciliationIssueSubmitUnconfirmed, Resolution: domain.ReconciliationResolutionManual,
					Status: domain.ReconciliationIssueOpen, OrderID: order.ID, MarketID: order.Intent.MarketID,
					ConditionID: order.Intent.ConditionID,
					TokenID:     order.Intent.TokenID, Source: "LOCAL_ORDER_STATE",
					Details: "mutating request outcome is ambiguous and no venue order id is known; automatic resubmission is forbidden",
				})
			}
			continue
		}
		_, tradeReferenced := venueOrdersWithTrades[normalizedID(order.VenueOrderID)]
		shouldSyncFills := !order.Terminal() || tradeReferenced || order.ID == focusOrderID
		// Cancellation finality requires positive evidence that all fills visible
		// for this order have been applied. A successful overlapping account trade
		// scan is enough when it did not reference the order; otherwise require the
		// order-specific fill read. Query failure must keep the reservation held.
		fillEvidenceComplete := tradesErr == nil && !tradeReferenced
		if shouldSyncFills {
			syncResult, syncErr := service.fills.SyncOrder(ctx, order.ID)
			if syncErr != nil {
				fillEvidenceComplete = false
				state.addOrderSourceIssue(ctx, order, "CLOB_ORDER_TRADES", "read/apply order fills", syncErr)
			} else {
				fillEvidenceComplete = true
				state.run.Summary["fill_observations"] += syncResult.Observed
				state.run.Summary["fills_applied"] += syncResult.Applied
				for _, application := range syncResult.Applications {
					if !application.Applied {
						continue
					}
					issueType := domain.ReconciliationIssueMissedBuyFill
					if application.Fill.Side == domain.SideSell {
						issueType = domain.ReconciliationIssueMissedSellFill
					}
					state.issue(ctx, domain.ReconciliationIssueParams{
						Type: issueType, Resolution: domain.ReconciliationResolutionAutomatic,
						Status: domain.ReconciliationIssueResolved, OrderID: order.ID,
						VenueOrderID: order.VenueOrderID, VenueTradeID: application.Fill.VenueFillID,
						MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID, TokenID: order.Intent.TokenID,
						RemoteValue: application.Fill.Shares, Source: "POLYMARKET_TRADES",
						Details: "confirmed venue fill was missing locally and was applied through the idempotent fill ledger",
					})
				}
			}
		}
		if order.Status == domain.OrderStatusCancelled {
			if !fillEvidenceComplete {
				// Without complete fill evidence, absence is not proof and the
				// cancellation reservation must remain protected.
				continue
			}
			state.finalizeCancellation(ctx, order)
			continue
		}
		if !orderNeedsRefresh(order) {
			continue
		}
		before := order.Status
		refreshed, refreshErr := service.orderRefresher.Refresh(ctx, order.ID)
		if refreshErr != nil {
			state.addOrderSourceIssue(ctx, order, "CLOB_ORDER", "refresh order state", refreshErr)
			continue
		}
		state.run.Summary["orders_refreshed"]++
		if before != domain.OrderStatusCancelled && refreshed.Status == domain.OrderStatusCancelled {
			state.issue(ctx, domain.ReconciliationIssueParams{
				Type: domain.ReconciliationIssueLocalOrderCancelled, Resolution: domain.ReconciliationResolutionAutomatic,
				Status: domain.ReconciliationIssueResolved, OrderID: order.ID, VenueOrderID: refreshed.VenueOrderID,
				MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID,
				TokenID: order.Intent.TokenID, Source: "POLYMARKET_CLOB",
				Details: "venue proved the order cancelled; unfilled reservation remains protected until the fill propagation grace completes",
			})
			if fillEvidenceComplete {
				state.finalizeCancellation(ctx, refreshed)
			}
		}
	}

	// Reload after fill application so position comparison uses the repaired
	// ledger, not the stale pre-reconciliation snapshot.
	localPositions, localPositionErr := service.ledger.ListPositions(ctx, executionAccountID)
	if localPositionErr != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_POSITIONS", "read local positions", localPositionErr)
	} else {
		state.run.Summary["local_positions"] = len(localPositions)
		positionSnapshots, conflict := state.readPositionConsensus(ctx, balance.WalletAddress)
		if !conflict {
			state.comparePositions(ctx, localPositions, positionSnapshots)
		}
	}

	reconciledBalance, localBalanceErr := service.ledger.GetBalance(ctx, executionAccountID)
	if localBalanceErr != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_LEDGER", "reload local balance after fill recovery", localBalanceErr)
	} else {
		balanceSnapshots, balanceConflict := state.readBalanceConsensus(
			ctx, reconciledBalance.WalletAddress, reconciledBalance.CollateralAsset,
		)
		if !balanceConflict && !within(reconciledBalance.TotalBalance, balanceSnapshots.Amount, service.balanceEpsilon) {
			state.issue(ctx, domain.ReconciliationIssueParams{
				Type: domain.ReconciliationIssueBalanceDrift, Resolution: domain.ReconciliationResolutionManual,
				Status: domain.ReconciliationIssueOpen, LocalValue: reconciledBalance.TotalBalance,
				RemoteValue: balanceSnapshots.Amount, Source: balanceSnapshots.Source,
				Details: "local total balance differs from the on-chain collateral balance; do not overwrite the ledger without attributable cash/fill/redeem evidence",
			})
		}
	}

	return service.finish(ctx, state, errors.Join(openErr, tradesErr, localPositionErr, localBalanceErr, errors.Join(state.errors...)))
}

// runState 表示后端使用的 runState 类型。
type runState struct {
	service *Service
	run     *domain.ReconciliationRun
	now     time.Time
	issues  []domain.ReconciliationIssue
	errors  []error
}

// issue 补全对账问题身份并通过参数构建器持久化到运行结果。
func (state *runState) issue(ctx context.Context, params domain.ReconciliationIssueParams) {
	issue := domain.ReconciliationIssue(params)
	issue.IssueID = state.service.newID()
	issue.RunID = state.run.RunID
	issue.ExecutionAccountID = state.run.ExecutionAccountID
	issue.ObservedAt = state.now
	if issue.Status == domain.ReconciliationIssueResolved {
		resolvedAt := state.now
		issue.ResolvedAt = &resolvedAt
	}
	issue.Fingerprint = issueFingerprint(issue)
	issue, err := domain.ReconciliationIssueParams(issue).Build()
	if err != nil {
		state.errors = append(state.errors, err)
		return
	}
	if err := state.service.recorder.RecordIssue(ctx, issue); err != nil {
		state.errors = append(state.errors, err)
	}
	state.issues = append(state.issues, issue)
	state.run.Summary["issues_total"]++
	state.run.Summary["issues_"+strings.ToLower(string(issue.Status))]++
	state.run.Summary["issues_"+strings.ToLower(string(issue.Resolution))]++
}

// addInfrastructureIssue 累加或记录 Infrastructure Issue。
func (state *runState) addInfrastructureIssue(ctx context.Context, source, operation string, err error) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueSourceUnavailable, Resolution: domain.ReconciliationResolutionRetry,
		Status: domain.ReconciliationIssueOpen, Source: source,
		Details: operation + " failed; the result is unknown, not empty: " + err.Error(),
	})
	state.errors = append(state.errors, err)
}

// addOrderSourceIssue 累加或记录 Order Source Issue。
func (state *runState) addOrderSourceIssue(ctx context.Context, order domain.Order, source, operation string, err error) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueSourceUnavailable, Resolution: domain.ReconciliationResolutionRetry,
		Status: domain.ReconciliationIssueOpen, OrderID: order.ID, VenueOrderID: order.VenueOrderID,
		MarketID: order.Intent.MarketID, ConditionID: order.Intent.ConditionID,
		TokenID: order.Intent.TokenID, Source: source,
		Details: operation + " failed; preserve state and reservations: " + err.Error(),
	})
	state.errors = append(state.errors, err)
}

// finalizeCancellation 在宽限期满足时完成已撤订单的最终预占释放。
func (state *runState) finalizeCancellation(ctx context.Context, order domain.Order) {
	_, err := state.service.orderRefresher.FinalizeCancellation(ctx, order.ID)
	if errors.Is(err, port.ErrCancelFinalityPending) {
		state.run.Summary["cancel_finality_pending"]++
		return
	}
	if err != nil {
		state.addOrderSourceIssue(ctx, order, "CANCEL_FINALITY", "release cancelled order reservation", err)
		return
	}
	state.run.Summary["cancellations_finalized"]++
}

// readPositionConsensus 读取并核对 Position Consensus。
func (state *runState) readPositionConsensus(ctx context.Context, wallet string) (positionSnapshot, bool) {
	snapshots := make([]positionSnapshot, 0, len(state.service.positionSources))
	for _, source := range state.service.positionSources {
		positions, err := source.ListExternalPositions(ctx, wallet)
		if err != nil {
			state.addInfrastructureIssue(ctx, "EXTERNAL_POSITIONS", "read external positions", err)
			continue
		}
		snapshot, err := makePositionSnapshot(positions)
		if err != nil {
			state.addInfrastructureIssue(ctx, "EXTERNAL_POSITIONS", "validate external positions", err)
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	if len(snapshots) != len(state.service.positionSources) {
		return positionSnapshot{}, true
	}
	for index := 1; index < len(snapshots); index++ {
		if !positionsAgree(snapshots[0], snapshots[index], state.service.positionEpsilon) {
			state.issue(ctx, domain.ReconciliationIssueParams{
				Type: domain.ReconciliationIssueSourceConflict, Resolution: domain.ReconciliationResolutionManual,
				Status: domain.ReconciliationIssueOpen, Source: snapshots[0].Source + "," + snapshots[index].Source,
				Details: "position sources disagree; all automatic position repairs are disabled for this run",
			})
			return positionSnapshot{}, true
		}
	}
	return snapshots[0], false
}

// readBalanceConsensus 读取并核对 Balance Consensus。
func (state *runState) readBalanceConsensus(ctx context.Context, wallet, asset string) (domain.ExternalBalance, bool) {
	snapshots := make([]domain.ExternalBalance, 0, len(state.service.balanceSources))
	for _, source := range state.service.balanceSources {
		balance, err := source.GetExternalBalance(ctx, wallet, asset)
		if err != nil {
			state.addInfrastructureIssue(ctx, "EXTERNAL_BALANCE", "read on-chain balance", err)
			continue
		}
		snapshots = append(snapshots, balance)
	}
	if len(snapshots) != len(state.service.balanceSources) {
		return domain.ExternalBalance{}, true
	}
	for index := 1; index < len(snapshots); index++ {
		if snapshots[0].Asset != snapshots[index].Asset ||
			!within(snapshots[0].Amount, snapshots[index].Amount, state.service.balanceEpsilon) {
			state.issue(ctx, domain.ReconciliationIssueParams{
				Type: domain.ReconciliationIssueSourceConflict, Resolution: domain.ReconciliationResolutionManual,
				Status: domain.ReconciliationIssueOpen, Source: snapshots[0].Source + "," + snapshots[index].Source,
				Details: "balance sources disagree; automatic balance repair is disabled",
			})
			return domain.ExternalBalance{}, true
		}
	}
	return snapshots[0], false
}

// comparePositions 比较 Positions。
func (state *runState) comparePositions(ctx context.Context, local []domain.Position, remote positionSnapshot) {
	localByToken := make(map[string]domain.Position, len(local))
	for _, position := range local {
		localByToken[position.TokenID] = position
	}
	for tokenID, external := range remote.Positions {
		position, exists := localByToken[tokenID]
		if !exists {
			state.issue(ctx, domain.ReconciliationIssueParams{
				Type: domain.ReconciliationIssuePhantomPosition, Resolution: domain.ReconciliationResolutionManual,
				Status: domain.ReconciliationIssueOpen, ConditionID: external.ConditionID, TokenID: tokenID,
				RemoteValue: external.Shares, Source: external.Source,
				Details: "wallet has shares without a local position or attributable local fill; manual/external/phantom trade must be identified",
			})
			continue
		}
		delete(localByToken, tokenID)
		sharesMatch := within(position.TotalShares, external.Shares, state.service.positionEpsilon)
		if external.Redeemable && sharesMatch && position.LifecycleStatus == domain.PositionLifecycleOpen {
			settled, err := state.service.ledger.MarkPositionSettled(ctx, state.run.ExecutionAccountID, tokenID,
				external.Source+":"+external.ConditionID, external.ObservedAt)
			if err != nil {
				state.addInfrastructureIssue(ctx, "POSTGRES_POSITIONS", "mark settled position "+tokenID, err)
			} else {
				state.issue(ctx, domain.ReconciliationIssueParams{
					Type: domain.ReconciliationIssuePositionSettled, Resolution: domain.ReconciliationResolutionAutomatic,
					Status: domain.ReconciliationIssueResolved, MarketID: settled.MarketID,
					ConditionID: settled.ConditionID, TokenID: tokenID,
					LocalValue: settled.TotalShares, RemoteValue: external.Shares, Source: external.Source,
					Details: "resolved market position was moved to SETTLED_PENDING_REDEEM; shares and cost basis were retained until a confirmed redeem receipt",
				})
			}
		}
		if !sharesMatch {
			state.issue(ctx, domain.ReconciliationIssueParams{
				Type: domain.ReconciliationIssuePositionDrift, Resolution: domain.ReconciliationResolutionManual,
				Status: domain.ReconciliationIssueOpen, MarketID: position.MarketID,
				ConditionID: position.ConditionID, TokenID: tokenID,
				LocalValue: position.TotalShares, RemoteValue: external.Shares, Source: external.Source,
				Details: "position quantity still differs after confirmed-fill recovery; do not guess an accounting event",
			})
		}
	}
	for tokenID, position := range localByToken {
		if sign, err := position.TotalShares.Sign(); err == nil && sign == 0 {
			continue
		}
		state.issue(ctx, domain.ReconciliationIssueParams{
			Type: domain.ReconciliationIssuePositionDrift, Resolution: domain.ReconciliationResolutionManual,
			Status: domain.ReconciliationIssueOpen, MarketID: position.MarketID,
			ConditionID: position.ConditionID, TokenID: tokenID,
			LocalValue: position.TotalShares, RemoteValue: "0", Source: remote.Source,
			Details: "local shares are absent from the external position snapshot; settlement, redeem, sell, or stale source cannot be inferred safely",
		})
	}
}

// positionSnapshot 表示后端使用的 positionSnapshot 类型。
type positionSnapshot struct {
	Source    string
	Positions map[string]domain.ExternalPosition
}

// makePositionSnapshot 汇总并构建 Position Snapshot。
func makePositionSnapshot(positions []domain.ExternalPosition) (positionSnapshot, error) {
	result := positionSnapshot{Positions: make(map[string]domain.ExternalPosition)}
	for _, position := range positions {
		tokenID := strings.TrimSpace(position.TokenID)
		if tokenID == "" || strings.TrimSpace(position.Source) == "" {
			return positionSnapshot{}, fmt.Errorf("external position identity and source are required")
		}
		if _, exists := result.Positions[tokenID]; exists {
			return positionSnapshot{}, fmt.Errorf("external position source returned duplicate token %s", tokenID)
		}
		if result.Source == "" {
			result.Source = position.Source
		} else if result.Source != position.Source {
			return positionSnapshot{}, fmt.Errorf("one external snapshot contains mixed sources")
		}
		result.Positions[tokenID] = position
	}
	if result.Source == "" {
		result.Source = "EXTERNAL_POSITION_SOURCE"
	}
	return result, nil
}

// positionsAgree 判断当前业务条件是否成立。
func positionsAgree(left, right positionSnapshot, epsilon domain.Decimal) bool {
	if len(left.Positions) != len(right.Positions) {
		return false
	}
	for tokenID, leftPosition := range left.Positions {
		rightPosition, found := right.Positions[tokenID]
		if !found || leftPosition.Redeemable != rightPosition.Redeemable ||
			leftPosition.ConditionID != rightPosition.ConditionID ||
			!within(leftPosition.Shares, rightPosition.Shares, epsilon) {
			return false
		}
	}
	return true
}

// finish 完成并持久化 对应数据。
func (service *Service) finish(ctx context.Context, state runState, cause error) (Result, error) {
	completedAt := service.now().UTC()
	state.run.CompletedAt = &completedAt
	state.run.Status = domain.ReconciliationRunCompleted
	for _, issue := range state.issues {
		if issue.Status == domain.ReconciliationIssueOpen {
			state.run.Status = domain.ReconciliationRunAttentionRequired
			break
		}
	}
	if cause != nil {
		state.run.Error = cause.Error()
		// Infrastructure uncertainty is not a successful reconciliation. It is
		// still ATTENTION_REQUIRED when useful comparisons completed; FAILED is
		// reserved for a run that could not even establish local authority.
		if state.run.Summary["local_orders"] == 0 && state.run.Summary["local_positions"] == 0 {
			state.run.Status = domain.ReconciliationRunFailed
		}
	}
	completeErr := service.recorder.Complete(ctx, *state.run)
	return Result{Run: *state.run, Issues: state.issues}, errors.Join(cause, completeErr)
}

// validTrigger 判断当前业务条件是否成立。
func validTrigger(trigger domain.ReconciliationTrigger) bool {
	switch trigger {
	case domain.ReconciliationTriggerStartup, domain.ReconciliationTriggerScheduled,
		domain.ReconciliationTriggerOrderUnknown, domain.ReconciliationTriggerCancelUnknown,
		domain.ReconciliationTriggerAssetDrift:
		return true
	default:
		return false
	}
}

// orderNeedsRefresh 判断当前业务条件是否成立。
func orderNeedsRefresh(order domain.Order) bool {
	switch order.Status {
	case domain.OrderStatusSubmitting, domain.OrderStatusAcknowledged, domain.OrderStatusLive,
		domain.OrderStatusPartiallyFilled, domain.OrderStatusUnknown,
		domain.OrderStatusCancelPending, domain.OrderStatusReconciling:
		return strings.TrimSpace(order.VenueOrderID) != ""
	default:
		return false
	}
}

// prioritizeOrder 按关注标识优先排列 Order。
func prioritizeOrder(orders []domain.Order, focus string) []domain.Order {
	result := append([]domain.Order(nil), orders...)
	if focus == "" {
		return result
	}
	sort.SliceStable(result, func(left, right int) bool {
		return result[left].ID == focus && result[right].ID != focus
	})
	return result
}

// normalizedID 规范化 d 标识 的字段和表示。
func normalizedID(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

// within 判断两个值的差是否处于 对应数据。
func within(left, right, epsilon domain.Decimal) bool {
	leftRat, leftOK := new(big.Rat).SetString(left.String())
	rightRat, rightOK := new(big.Rat).SetString(right.String())
	epsilonRat, epsilonOK := new(big.Rat).SetString(epsilon.String())
	if !leftOK || !rightOK || !epsilonOK || epsilonRat.Sign() < 0 {
		return false
	}
	difference := new(big.Rat).Sub(leftRat, rightRat)
	if difference.Sign() < 0 {
		difference.Neg(difference)
	}
	return difference.Cmp(epsilonRat) <= 0
}

// issueFingerprint 根据稳定业务身份生成幂等标识。
func issueFingerprint(issue domain.ReconciliationIssue) string {
	parts := []string{
		string(issue.Type), issue.ExecutionAccountID, issue.OrderID, issue.VenueOrderID,
		issue.VenueTradeID, issue.MarketID, issue.ConditionID, issue.TokenID, issue.Source,
		issue.LocalValue.String(), issue.RemoteValue.String(),
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

// newID 生成随机且稳定格式的对账运行标识。
func newID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err == nil {
		return "recon:" + hex.EncodeToString(value)
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return "recon:" + hex.EncodeToString(digest[:16])
}

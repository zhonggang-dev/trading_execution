package reconciliation

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// positionSnapshot 保存一个外部来源按 token 聚合后的持仓快照。
type positionSnapshot struct {
	Source    string
	Positions map[string]domain.ExternalPosition
}

// compareExternalPositionParams 收拢一个外部持仓与本地持仓索引的比较参数。
type compareExternalPositionParams struct {
	tokenID      string
	external     domain.ExternalPosition
	localByToken map[string]domain.Position
}

// settlePositionParams 收拢推进持仓结算生命周期所需的证据。
type settlePositionParams struct {
	tokenID     string
	position    domain.Position
	external    domain.ExternalPosition
	sharesMatch bool
}

// missingExternalPositionParams 收拢记录外部缺失持仓所需的信息。
type missingExternalPositionParams struct {
	tokenID        string
	position       domain.Position
	externalSource string
}

// reconcilePositions 在成交补录后重新读取并核对本地与外部持仓。
func (state *runState) reconcilePositions(ctx context.Context, executionAccountID, walletAddress string) error {
	positions, err := state.service.ledger.ListPositions(ctx, executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_POSITIONS", "read local positions", err)
		return err
	}
	state.run.Summary["local_positions"] = len(positions)
	external, conflict := state.readPositionConsensus(ctx, walletAddress)
	if conflict {
		return nil
	}
	state.comparePositions(ctx, positions, external)
	return nil
}

// reconcileBalance 在成交补录后重新读取并核对本地与链上余额。
func (state *runState) reconcileBalance(ctx context.Context, executionAccountID string) error {
	balance, err := state.service.ledger.GetBalance(ctx, executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_LEDGER", "reload local balance after fill recovery", err)
		return err
	}
	external, conflict := state.readBalanceConsensus(ctx, balance.WalletAddress, balance.CollateralAsset)
	if conflict || within(balance.TotalBalance, external.Amount, state.service.balanceEpsilon) {
		return nil
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueBalanceDrift, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, LocalValue: balance.TotalBalance,
		RemoteValue: external.Amount, Source: external.Source,
		Details: "local total balance differs from the on-chain collateral balance; do not overwrite the ledger without attributable cash/fill/redeem evidence",
	})
	return nil
}

// readPositionConsensus 读取多个外部持仓源并确认它们是否一致。
func (state *runState) readPositionConsensus(ctx context.Context, wallet string) (positionSnapshot, bool) {
	snapshots := make([]positionSnapshot, 0, len(state.service.positionSources))
	for _, source := range state.service.positionSources {
		snapshot, ok := state.readPositionSnapshot(ctx, source, wallet)
		if ok {
			snapshots = append(snapshots, snapshot)
		}
	}
	if len(snapshots) != len(state.service.positionSources) {
		return positionSnapshot{}, true
	}
	return state.requirePositionConsensus(ctx, snapshots)
}

// readPositionSnapshot 读取并规范化单个外部持仓来源。
func (state *runState) readPositionSnapshot(ctx context.Context, source port.ExternalPositionSource, wallet string) (positionSnapshot, bool) {
	positions, err := source.ListExternalPositions(ctx, wallet)
	if err != nil {
		state.addInfrastructureIssue(ctx, "EXTERNAL_POSITIONS", "read external positions", err)
		return positionSnapshot{}, false
	}
	snapshot, err := makePositionSnapshot(positions)
	if err != nil {
		state.addInfrastructureIssue(ctx, "EXTERNAL_POSITIONS", "validate external positions", err)
		return positionSnapshot{}, false
	}
	return snapshot, true
}

// requirePositionConsensus 检查全部外部持仓快照是否在容差内一致。
func (state *runState) requirePositionConsensus(ctx context.Context, snapshots []positionSnapshot) (positionSnapshot, bool) {
	for index := 1; index < len(snapshots); index++ {
		if positionsAgree(snapshots[0], snapshots[index], state.service.positionEpsilon) {
			continue
		}
		state.issue(ctx, domain.ReconciliationIssueParams{
			Type: domain.ReconciliationIssueSourceConflict, Resolution: domain.ReconciliationResolutionManual,
			Status: domain.ReconciliationIssueOpen, Source: snapshots[0].Source + "," + snapshots[index].Source,
			Details: "position sources disagree; all automatic position repairs are disabled for this run",
		})
		return positionSnapshot{}, true
	}
	return snapshots[0], false
}

// readBalanceConsensus 读取多个外部余额源并确认它们是否一致。
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
	return state.requireBalanceConsensus(ctx, snapshots)
}

// requireBalanceConsensus 检查全部外部余额快照是否在容差内一致。
func (state *runState) requireBalanceConsensus(ctx context.Context, snapshots []domain.ExternalBalance) (domain.ExternalBalance, bool) {
	for index := 1; index < len(snapshots); index++ {
		if snapshots[0].Asset == snapshots[index].Asset &&
			within(snapshots[0].Amount, snapshots[index].Amount, state.service.balanceEpsilon) {
			continue
		}
		state.issue(ctx, domain.ReconciliationIssueParams{
			Type: domain.ReconciliationIssueSourceConflict, Resolution: domain.ReconciliationResolutionManual,
			Status: domain.ReconciliationIssueOpen, Source: snapshots[0].Source + "," + snapshots[index].Source,
			Details: "balance sources disagree; automatic balance repair is disabled",
		})
		return domain.ExternalBalance{}, true
	}
	return snapshots[0], false
}

// comparePositions 比较本地权威持仓与已经达成共识的外部快照。
func (state *runState) comparePositions(ctx context.Context, local []domain.Position, remote positionSnapshot) {
	localByToken := make(map[string]domain.Position, len(local))
	for _, position := range local {
		localByToken[position.TokenID] = position
	}
	for tokenID, external := range remote.Positions {
		state.compareExternalPosition(ctx, compareExternalPositionParams{tokenID: tokenID, external: external, localByToken: localByToken})
	}
	for tokenID, position := range localByToken {
		state.recordMissingExternalPosition(ctx, missingExternalPositionParams{tokenID: tokenID, position: position, externalSource: remote.Source})
	}
}

// compareExternalPosition 比较一个外部持仓与对应的本地权威快照。
func (state *runState) compareExternalPosition(ctx context.Context, params compareExternalPositionParams) {
	position, exists := params.localByToken[params.tokenID]
	if !exists {
		state.recordPhantomPosition(ctx, params.tokenID, params.external)
		return
	}
	delete(params.localByToken, params.tokenID)

	sharesMatch := within(position.TotalShares, params.external.Shares, state.service.positionEpsilon)
	state.settlePositionIfNeeded(ctx, settlePositionParams{tokenID: params.tokenID, position: position, external: params.external, sharesMatch: sharesMatch})
	if sharesMatch {
		return
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssuePositionDrift, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, MarketID: position.MarketID,
		ConditionID: position.ConditionID, TokenID: params.tokenID,
		LocalValue: position.TotalShares, RemoteValue: params.external.Shares, Source: params.external.Source,
		Details: "position quantity still differs after confirmed-fill recovery; do not guess an accounting event",
	})
}

// recordPhantomPosition 记录外部存在但本地没有可归因账本事件的持仓。
func (state *runState) recordPhantomPosition(ctx context.Context, tokenID string, external domain.ExternalPosition) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssuePhantomPosition, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, ConditionID: external.ConditionID, TokenID: tokenID,
		RemoteValue: external.Shares, Source: external.Source,
		Details: "wallet has shares without a local position or attributable local fill; manual/external/phantom trade must be identified",
	})
}

// settlePositionIfNeeded 在数量一致且外部明确可赎回时推进本地持仓生命周期。
func (state *runState) settlePositionIfNeeded(ctx context.Context, params settlePositionParams) {
	if !params.external.Redeemable || !params.sharesMatch || params.position.LifecycleStatus != domain.PositionLifecycleOpen {
		return
	}
	settled, err := state.service.ledger.MarkPositionSettled(
		ctx,
		state.run.ExecutionAccountID,
		params.tokenID,
		params.external.Source+":"+params.external.ConditionID,
		params.external.ObservedAt,
	)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_POSITIONS", "mark settled position "+params.tokenID, err)
		return
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssuePositionSettled, Resolution: domain.ReconciliationResolutionAutomatic,
		Status: domain.ReconciliationIssueResolved, MarketID: settled.MarketID,
		ConditionID: settled.ConditionID, TokenID: params.tokenID,
		LocalValue: settled.TotalShares, RemoteValue: params.external.Shares, Source: params.external.Source,
		Details: "resolved market position was moved to SETTLED_PENDING_REDEEM; shares and cost basis were retained until a confirmed redeem receipt",
	})
}

// recordMissingExternalPosition 记录本地存在但外部持仓快照缺失的非零仓位。
func (state *runState) recordMissingExternalPosition(ctx context.Context, params missingExternalPositionParams) {
	if sign, err := params.position.TotalShares.Sign(); err == nil && sign == 0 {
		return
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssuePositionDrift, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, MarketID: params.position.MarketID,
		ConditionID: params.position.ConditionID, TokenID: params.tokenID,
		LocalValue: params.position.TotalShares, RemoteValue: "0", Source: params.externalSource,
		Details: "local shares are absent from the external position snapshot; settlement, redeem, sell, or stale source cannot be inferred safely",
	})
}

// makePositionSnapshot 汇总并构建一个外部来源的持仓快照。
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

// positionsAgree 判断两个外部持仓快照是否在容差内一致。
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

// within 判断两个十进制值的绝对差是否处于容差内。
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

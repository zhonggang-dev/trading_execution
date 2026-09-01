package reconciliation

import (
	"context"
	"encoding/json"
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

// positionBaselineSnapshot contains immutable shares outside this service's
// ownership. These values remain separate from the managed position ledger.
type positionBaselineSnapshot struct {
	BaselineID string
	Positions  map[string]domain.ExternalPositionBaseline
}

// compareExternalPositionParams 收拢一个外部持仓与本地持仓索引的比较参数。
type compareExternalPositionParams struct {
	tokenID      string
	external     domain.ExternalPosition
	localByToken map[string]domain.Position
}

// compareBaselinedPositionParams collects the three independently owned views
// required to compare a token that existed before account cutover.
type compareBaselinedPositionParams struct {
	tokenID  string
	external domain.ExternalPosition
	baseline domain.ExternalPositionBaseline
	local    domain.Position
	hasLocal bool
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

// baselineDriftParams preserves both the immutable expected total and the
// externally observed total for manual review.
type baselineDriftParams struct {
	baseline      domain.ExternalPositionBaseline
	localExpected domain.Decimal
	remoteValue   domain.Decimal
	source        string
	details       string
}

const polymarketDataAPIShareEpsilon = domain.Decimal("0.001")

// reconcilePositions 在成交补录后重新读取并核对本地与外部持仓。
func (state *runState) reconcilePositions(ctx context.Context, executionAccountID, walletAddress string) error {
	positions, err := state.service.ledger.ListPositions(ctx, executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_POSITIONS", "read local positions", err)
		return err
	}
	state.run.Summary["local_positions"] = len(positions)
	baselineValues, err := state.service.positionBaselines.ListExternalPositionBaselines(ctx, executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_EXTERNAL_POSITION_BASELINES", "read external position ownership baseline", err)
		return err
	}
	baselines, err := makePositionBaselineSnapshot(executionAccountID, baselineValues)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_EXTERNAL_POSITION_BASELINES", "validate external position ownership baseline", err)
		return err
	}
	state.run.Summary["unmanaged_position_baselines"] = len(baselines.Positions)
	external, conflict := state.readPositionConsensus(ctx, walletAddress)
	if conflict {
		return nil
	}
	state.comparePositions(ctx, positions, baselines, external)
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
func (state *runState) comparePositions(
	ctx context.Context,
	local []domain.Position,
	baselines positionBaselineSnapshot,
	remote positionSnapshot,
) {
	localByToken := make(map[string]domain.Position, len(local))
	for _, position := range local {
		localByToken[position.TokenID] = position
	}
	baselineByToken := make(map[string]domain.ExternalPositionBaseline, len(baselines.Positions))
	for tokenID, baseline := range baselines.Positions {
		baselineByToken[tokenID] = baseline
	}
	for tokenID, external := range remote.Positions {
		if baseline, exists := baselineByToken[tokenID]; exists {
			position, hasLocal := localByToken[tokenID]
			delete(baselineByToken, tokenID)
			delete(localByToken, tokenID)
			state.compareBaselinedPosition(ctx, compareBaselinedPositionParams{
				tokenID: tokenID, external: external, baseline: baseline,
				local: position, hasLocal: hasLocal,
			})
			continue
		}
		state.compareExternalPosition(ctx, compareExternalPositionParams{tokenID: tokenID, external: external, localByToken: localByToken})
	}
	for tokenID, baseline := range baselineByToken {
		position, hasLocal := localByToken[tokenID]
		delete(localByToken, tokenID)
		expected := baseline.Shares
		if hasLocal {
			var err error
			expected, err = addDecimals(expected, position.TotalShares)
			if err != nil {
				state.addInfrastructureIssue(ctx, "POSTGRES_POSITIONS", "sum managed and unmanaged position shares", err)
				continue
			}
		}
		state.recordBaselineDrift(ctx, baselineDriftParams{
			baseline: baseline, localExpected: expected, remoteValue: "0", source: baseline.Source,
			details: "the immutable unmanaged baseline token is absent from the external snapshot; the baseline is never reduced or imported into the managed ledger",
		})
	}
	for tokenID, position := range localByToken {
		state.recordMissingExternalPosition(ctx, missingExternalPositionParams{tokenID: tokenID, position: position, externalSource: remote.Source})
	}
}

// compareBaselinedPosition verifies remote_total = immutable unmanaged shares
// + managed local shares. It never materializes the unmanaged shares locally.
func (state *runState) compareBaselinedPosition(ctx context.Context, params compareBaselinedPositionParams) {
	expected := params.baseline.Shares
	if params.hasLocal {
		var err error
		expected, err = addDecimals(expected, params.local.TotalShares)
		if err != nil {
			state.addInfrastructureIssue(ctx, "POSTGRES_POSITIONS", "sum managed and unmanaged position shares", err)
			return
		}
	}
	identityMatches := baselineMatchesExternal(params.baseline, params.external)
	if params.hasLocal {
		identityMatches = identityMatches && baselineMatchesLocal(params.baseline, params.local)
	}
	// Baseline evidence is exact and immutable. Unlike ordinary live-source
	// comparison, no epsilon may absorb a change to an unmanaged cutover amount.
	if !identityMatches || !expected.Equal(params.external.Shares) {
		details := "remote position does not equal the immutable unmanaged baseline plus the managed ledger; do not adjust the baseline or invent a managed position"
		if !identityMatches {
			details = "remote, unmanaged baseline, and managed ledger position identities do not match exactly; do not merge or relabel the token"
		}
		state.recordBaselineDrift(ctx, baselineDriftParams{
			baseline: params.baseline, localExpected: expected, remoteValue: params.external.Shares,
			source: params.external.Source, details: details,
		})
		return
	}
	if !params.hasLocal {
		return
	}
	managedExternal := params.external
	managedExternal.Shares = params.local.TotalShares
	state.settlePositionIfNeeded(ctx, settlePositionParams{
		tokenID: params.tokenID, position: params.local, external: managedExternal, sharesMatch: true,
	})
}

// compareExternalPosition 比较一个外部持仓与对应的本地权威快照。
func (state *runState) compareExternalPosition(ctx context.Context, params compareExternalPositionParams) {
	position, exists := params.localByToken[params.tokenID]
	if !exists {
		state.recordPhantomPosition(ctx, params.tokenID, params.external)
		return
	}
	delete(params.localByToken, params.tokenID)

	if !managedPositionMatchesExternal(state.run.ExecutionAccountID, position, params.external) {
		state.issue(ctx, domain.ReconciliationIssueParams{
			Type: domain.ReconciliationIssuePositionDrift, Resolution: domain.ReconciliationResolutionManual,
			Status: domain.ReconciliationIssueOpen, MarketID: position.MarketID,
			ConditionID: position.ConditionID, TokenID: params.tokenID,
			LocalValue: position.TotalShares, RemoteValue: params.external.Shares, Source: params.external.Source,
			Details: "managed ledger and remote position identities do not match exactly; do not merge, relabel, or settle the token",
		})
		return
	}
	sharesMatch := managedPositionSharesMatch(position.TotalShares, params.external, state.service.positionEpsilon)
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

func managedPositionSharesMatch(local domain.Decimal, external domain.ExternalPosition, configured domain.Decimal) bool {
	tolerance := configured
	// Polymarket's Data API position snapshot reports outcome shares at
	// millishare precision even though confirmed OrderFilled events and the
	// ERC-1155 ledger retain six decimals. Bound only this read-model source to
	// one displayed quantum; the fill ledger remains exact and authoritative.
	if strings.EqualFold(strings.TrimSpace(external.Source), "POLYMARKET_DATA_API") {
		comparison, err := tolerance.Compare(polymarketDataAPIShareEpsilon)
		if err != nil || comparison < 0 {
			tolerance = polymarketDataAPIShareEpsilon
		}
	}
	return within(local, external.Shares, tolerance)
}

func managedPositionMatchesExternal(executionAccountID string, position domain.Position, external domain.ExternalPosition) bool {
	return position.ExecutionAccountID == executionAccountID &&
		position.ConditionID != "" && position.ConditionID == external.ConditionID &&
		position.TokenID != "" && position.TokenID == external.TokenID &&
		position.OutcomeName != "" && position.OutcomeName == external.OutcomeName &&
		outcomeIndexesEqual(position.OutcomeIndex, external.OutcomeIndex)
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

// recordBaselineDrift records immutable ownership evidence drift without
// changing either the baseline or the managed position ledger.
func (state *runState) recordBaselineDrift(ctx context.Context, params baselineDriftParams) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueExternalPositionBaselineDrift, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, ConditionID: params.baseline.ConditionID,
		TokenID: params.baseline.TokenID, LocalValue: params.localExpected,
		RemoteValue: params.remoteValue, Source: params.source, Details: params.details,
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
		params.external.CurrentPrice,
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

// makePositionBaselineSnapshot validates the baseline again at the service
// boundary so a corrupt or alternate adapter cannot silently weaken cutover
// ownership. Every item in one account must belong to the same immutable set.
func makePositionBaselineSnapshot(
	executionAccountID string,
	values []domain.ExternalPositionBaseline,
) (positionBaselineSnapshot, error) {
	result := positionBaselineSnapshot{Positions: make(map[string]domain.ExternalPositionBaseline)}
	for _, baseline := range values {
		baseline.BaselineID = strings.TrimSpace(baseline.BaselineID)
		baseline.ExecutionAccountID = strings.TrimSpace(baseline.ExecutionAccountID)
		baseline.ConditionID = strings.TrimSpace(baseline.ConditionID)
		baseline.TokenID = strings.TrimSpace(baseline.TokenID)
		baseline.OutcomeName = strings.TrimSpace(baseline.OutcomeName)
		baseline.Source = strings.TrimSpace(baseline.Source)
		baseline.Actor = strings.TrimSpace(baseline.Actor)
		baseline.Reason = strings.TrimSpace(baseline.Reason)
		if baseline.BaselineID == "" || baseline.ExecutionAccountID != executionAccountID ||
			baseline.ConditionID == "" || baseline.TokenID == "" || baseline.OutcomeName == "" ||
			baseline.Source == "" || baseline.Actor == "" || baseline.Reason == "" || baseline.ObservedAt.IsZero() {
			return positionBaselineSnapshot{}, fmt.Errorf("external position baseline has incomplete or mismatched identity for token %q", baseline.TokenID)
		}
		if result.BaselineID == "" {
			result.BaselineID = baseline.BaselineID
		} else if result.BaselineID != baseline.BaselineID {
			return positionBaselineSnapshot{}, fmt.Errorf("execution account has multiple external position baseline sets")
		}
		if baseline.OutcomeIndex != nil && *baseline.OutcomeIndex < 0 {
			return positionBaselineSnapshot{}, fmt.Errorf("external position baseline token %q has invalid outcome index", baseline.TokenID)
		}
		if sign, err := baseline.Shares.Sign(); err != nil || sign <= 0 {
			return positionBaselineSnapshot{}, fmt.Errorf("external position baseline token %q must have positive shares", baseline.TokenID)
		}
		var evidence map[string]any
		if err := json.Unmarshal(baseline.Evidence, &evidence); err != nil || len(evidence) == 0 {
			return positionBaselineSnapshot{}, fmt.Errorf("external position baseline token %q must have non-empty object evidence", baseline.TokenID)
		}
		if _, exists := result.Positions[baseline.TokenID]; exists {
			return positionBaselineSnapshot{}, fmt.Errorf("external position baseline contains duplicate token %q", baseline.TokenID)
		}
		result.Positions[baseline.TokenID] = baseline
	}
	return result, nil
}

func baselineMatchesExternal(baseline domain.ExternalPositionBaseline, external domain.ExternalPosition) bool {
	return baseline.ConditionID == strings.TrimSpace(external.ConditionID) &&
		baseline.TokenID == strings.TrimSpace(external.TokenID) &&
		baseline.OutcomeName == strings.TrimSpace(external.OutcomeName) &&
		baseline.NegRisk == external.NegRisk &&
		!external.ObservedAt.IsZero() && !external.ObservedAt.Before(baseline.ObservedAt) &&
		outcomeIndexesEqual(baseline.OutcomeIndex, external.OutcomeIndex)
}

func baselineMatchesLocal(baseline domain.ExternalPositionBaseline, local domain.Position) bool {
	return baseline.ConditionID == strings.TrimSpace(local.ConditionID) &&
		baseline.TokenID == strings.TrimSpace(local.TokenID) &&
		baseline.OutcomeName == strings.TrimSpace(local.OutcomeName) &&
		outcomeIndexesEqual(baseline.OutcomeIndex, local.OutcomeIndex)
}

func outcomeIndexesEqual(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// addDecimals performs exact base-10 addition and emits a Decimal rather than
// a rational fraction. The maximum input scale is sufficient for an exact sum.
func addDecimals(left, right domain.Decimal) (domain.Decimal, error) {
	leftValue, leftOK := new(big.Rat).SetString(strings.TrimSpace(left.String()))
	rightValue, rightOK := new(big.Rat).SetString(strings.TrimSpace(right.String()))
	if !leftOK || !rightOK || strings.ContainsAny(left.String()+right.String(), "/eE") {
		return "", fmt.Errorf("invalid decimal operands %q and %q", left, right)
	}
	scale := decimalScale(left.String())
	if rightScale := decimalScale(right.String()); rightScale > scale {
		scale = rightScale
	}
	return domain.ParseDecimal(new(big.Rat).Add(leftValue, rightValue).FloatString(scale))
}

func decimalScale(value string) int {
	value = strings.TrimSpace(value)
	point := strings.IndexByte(value, '.')
	if point < 0 {
		return 0
	}
	return len(value) - point - 1
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
			leftPosition.OutcomeName != rightPosition.OutcomeName ||
			leftPosition.NegRisk != rightPosition.NegRisk ||
			!outcomeIndexesEqual(leftPosition.OutcomeIndex, rightPosition.OutcomeIndex) ||
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

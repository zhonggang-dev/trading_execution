package reconciliation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// defaultFillFinalityMaxAge bounds the expected wait for the configured Polygon
// confirmation depth. Sixty-four Polygon blocks land in roughly two to three
// minutes; a CLOB-confirmed fill still unfinalized half an hour after matching
// means the transaction was dropped, re-included elsewhere, or the RPC head is
// stuck, none of which is an ordinary propagation wait.
const defaultFillFinalityMaxAge = 30 * time.Minute

// finalityPendingFills is the per-run view of fills whose OrderFilled receipt
// is canonical and exactly verified but not yet deep enough to be applied.
// The chain already reflects them while the ledger still shows the pre-fill
// balance and position. Rather than explaining an aggregate difference, every
// pending fill contributes its exact signed share and cash delta, so a real
// discrepancy of coincidentally similar size is still reported.
type finalityPendingFills struct {
	fills         []domain.Fill
	sharesByToken map[string]domain.Decimal
	cashDelta     domain.Decimal
}

// loadFinalityPendingFills reads the account's finality-pending fills before
// any position or balance comparison and escalates fills that have waited too
// long. A read failure is infrastructure uncertainty (RETRY_LATER), never a
// reason to fall back to recording manual drift.
func (state *runState) loadFinalityPendingFills(ctx context.Context, executionAccountID string) error {
	state.finality = finalityPendingFills{sharesByToken: make(map[string]domain.Decimal), cashDelta: "0"}
	fills, err := state.service.ledger.ListFinalityPendingFills(ctx, executionAccountID)
	if err != nil {
		state.addInfrastructureIssue(ctx, "POSTGRES_FILLS", "read finality-pending fills", err)
		return err
	}
	for _, fill := range fills {
		fill = fill.Normalize()
		if fill.ExecutionAccountID != executionAccountID || !isFinalityPendingFill(fill) {
			err := fmt.Errorf("finality-pending fill %q has invalid identity, status, or evidence", fill.Key)
			state.addInfrastructureIssue(ctx, "POSTGRES_FILLS", "validate finality-pending fills", err)
			return err
		}
		signedShares := fill.Shares
		if fill.Side == domain.SideSell {
			signedShares = domain.Decimal("-" + strings.TrimPrefix(fill.Shares.String(), "-"))
		}
		tokenShares, exists := state.finality.sharesByToken[fill.TokenID]
		if !exists {
			tokenShares = "0"
		}
		if tokenShares, err = addDecimals(tokenShares, signedShares); err != nil {
			state.addInfrastructureIssue(ctx, "POSTGRES_FILLS", "sum finality-pending fill shares", err)
			return err
		}
		state.finality.sharesByToken[fill.TokenID] = tokenShares
		if state.finality.cashDelta, err = addDecimals(state.finality.cashDelta, fill.NetCashDelta); err != nil {
			state.addInfrastructureIssue(ctx, "POSTGRES_FILLS", "sum finality-pending fill cash", err)
			return err
		}
		state.finality.fills = append(state.finality.fills, fill)
		state.recordStalledFinalityPendingFill(ctx, fill)
	}
	state.run.Summary["finality_pending_fills"] = len(state.finality.fills)
	return nil
}

// isFinalityPendingFill reports whether a fill observation carries verified
// on-chain settlement evidence while its status is still short of CONFIRMED.
func isFinalityPendingFill(fill domain.Fill) bool {
	return fill.AppliedAt == nil && fill.SettlementEvidence != nil &&
		fill.FeeSource == domain.FeeSourcePolygonV2OrderFilled &&
		fill.Status != domain.FillStatusConfirmed && fill.Status != domain.FillStatusFailed &&
		fill.SettlementEvidence.BlockNumber > 0 && fill.SettlementEvidence.Confirmations > 0
}

// recordStalledFinalityPendingFill escalates a fill that matched longer ago
// than the configured maximum age but still lacks finality. Confirmations only
// stop growing when the transaction was dropped or re-included by a
// reorganization, or the RPC head is stuck; each is a manual gate.
func (state *runState) recordStalledFinalityPendingFill(ctx context.Context, fill domain.Fill) {
	age := state.now.Sub(fill.MatchedAt)
	if age <= state.service.fillFinalityMaxAge {
		return
	}
	state.run.Summary["finality_pending_fills_stalled"]++
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueFillFinalityStalled, Resolution: domain.ReconciliationResolutionManual,
		Status: domain.ReconciliationIssueOpen, OrderID: fill.OrderID, VenueOrderID: fill.VenueOrderID,
		VenueTradeID: fill.VenueFillID, MarketID: fill.MarketID, ConditionID: fill.ConditionID,
		TokenID: fill.TokenID, LocalValue: fill.Shares, Source: "POLYGON_RECEIPT",
		Details: fmt.Sprintf(
			"CLOB-confirmed fill matched %s ago and its receipt in block %d (%s) still has %d confirmations; "+
				"verify the transaction is canonical before applying or discarding it",
			age.Truncate(time.Second), fill.SettlementEvidence.BlockNumber,
			fill.SettlementEvidence.TransactionHash, fill.SettlementEvidence.Confirmations,
		),
	})
}

// balanceExplainedByFinalityPendingFills reports whether the local total
// balance plus the exact net cash of every finality-pending fill equals the
// on-chain balance. In-flight redemptions may overlap the same window, so the
// projected balance is also offered to the redemption explanation.
func (state *runState) balanceExplainedByFinalityPendingFills(local, external domain.Decimal) bool {
	if len(state.finality.fills) == 0 {
		return false
	}
	projected, err := addDecimals(local, state.finality.cashDelta)
	if err != nil {
		return false
	}
	if !within(projected, external, state.service.balanceEpsilon) &&
		!state.balanceExplainedByRedemptions(projected, external) {
		return false
	}
	state.run.Summary["balance_explained_by_finality_pending_fills"]++
	return true
}

// positionExplainedByFinalityPendingFills reports whether the local shares
// plus the signed finality-pending share delta of that token equal the
// external quantity. Data API snapshots keep millishare precision, so the same
// bounded tolerance as the ordinary comparison applies.
func (state *runState) positionExplainedByFinalityPendingFills(tokenID string, local, external domain.Decimal) bool {
	delta, exists := state.finality.sharesByToken[strings.TrimSpace(tokenID)]
	if !exists {
		return false
	}
	projected, err := addDecimals(local, delta)
	if err != nil {
		return false
	}
	tolerance := state.service.positionEpsilon
	if comparison, err := tolerance.Compare(polymarketDataAPIShareEpsilon); err != nil || comparison < 0 {
		tolerance = polymarketDataAPIShareEpsilon
	}
	if !within(projected, external, tolerance) {
		return false
	}
	state.run.Summary["positions_explained_by_finality_pending_fills"]++
	return true
}

// baselinedPositionExplainedByFinalityPendingFills is the exact variant used
// for tokens with an immutable unmanaged baseline: no epsilon is allowed, only
// the precise signed delta of finality-pending managed fills.
func (state *runState) baselinedPositionExplainedByFinalityPendingFills(tokenID string, expected, external domain.Decimal) bool {
	delta, exists := state.finality.sharesByToken[strings.TrimSpace(tokenID)]
	if !exists {
		return false
	}
	projected, err := addDecimals(expected, delta)
	if err != nil || !projected.Equal(external) {
		return false
	}
	state.run.Summary["positions_explained_by_finality_pending_fills"]++
	return true
}

// absentLocalPositionIsFinalityPending reports whether an external position
// with no managed local counterpart is exactly the shares minted by
// finality-pending BUY fills of that token.
func (state *runState) absentLocalPositionIsFinalityPending(tokenID string, external domain.ExternalPosition) bool {
	return state.positionExplainedByFinalityPendingFills(tokenID, "0", external.Shares)
}

package reconciliation

import (
	"context"
	"fmt"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/chaincash"
)

// chainCashSource names the pUSD Transfer log source in issues and in the
// run's verified-source summary.
const chainCashSource = "EVM_ERC20_TRANSFER_LOGS"

// syncChainCash brings the account's on-chain pUSD transfer ledger up to the
// balance snapshot block. A disabled ledger (no cursor) returns a zero result
// and the legacy comparison applies unchanged. Any failure, including a
// ledger that is still catching up, is recorded as SOURCE_UNAVAILABLE and the
// balance comparison is skipped: unknown transfers are never read as "none".
func (state *runState) syncChainCash(
	ctx context.Context, executionAccountID, walletAddress string, external domain.ExternalBalance,
) (chaincash.SyncResult, error) {
	if state.service.chainCash == nil {
		return chaincash.SyncResult{}, nil
	}
	result, err := state.service.chainCash.Sync(ctx, chaincash.SyncParams{
		ExecutionAccountID: executionAccountID, WalletAddress: walletAddress, SnapshotBlock: external.BlockNumber,
	})
	if err != nil {
		state.addInfrastructureIssue(ctx, chainCashSource, "synchronize the on-chain pUSD transfer ledger", err)
		return result, err
	}
	if !result.Enabled {
		return result, nil
	}
	state.run.Summary["chain_cash_transfers_persisted"] += result.Persisted
	state.run.Summary["chain_cash_transfers_credited"] += len(result.Credited)
	state.run.Summary["chain_cash_transfers_reattributed"] += result.Reattributed
	for _, transfer := range result.Credited {
		state.service.logger.Info("chain cash transfer credited",
			"execution_account_id", executionAccountID, "classification", transfer.Classification,
			"amount", transfer.Amount, "counterparty", transfer.Counterparty,
			"transaction_hash", transfer.TransactionHash, "log_index", transfer.LogIndex,
			"block_number", transfer.BlockNumber, "account_event_id", transfer.AccountEventID)
	}
	for _, transfer := range result.Unreported {
		state.recordUnattributedTransfer(ctx, transfer)
	}
	if !result.CaughtUp {
		err := fmt.Errorf("chain cash ledger is catching up: persisted through block %d, confirmed head %d",
			result.ProcessedBlock, result.ConfirmedBlock)
		state.addInfrastructureIssue(ctx, chainCashSource, "catch up the on-chain pUSD transfer ledger", err)
		return result, err
	}
	state.run.VerifyReconciliation("source", chainCashSource)
	return result, nil
}

// recordUnattributedTransfer writes the one audit issue of an unattributed
// transfer. Unknown incoming cash is observation only; unknown outgoing cash
// blocks the whole account and is never closed automatically.
func (state *runState) recordUnattributedTransfer(ctx context.Context, transfer domain.ChainCashTransfer) {
	params := domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueUnattributedCashIn, Resolution: domain.ReconciliationResolutionObserved,
		Status: domain.ReconciliationIssueOpen, VenueTradeID: transfer.Reference(), RemoteValue: transfer.Amount,
		RemoteBlockNumber: transfer.BlockNumber, Source: chainCashSource,
		Details: fmt.Sprintf(
			"confirmed pUSD transfer of %s from %s (transaction %s, log %d) is not a venue trade, an own redemption, "+
				"a configured reward sender, or a configured deposit source; it is recorded for audit, never credited, "+
				"and does not block trading", transfer.Amount, transfer.Counterparty, transfer.TransactionHash, transfer.LogIndex),
	}
	if transfer.Classification == domain.ChainCashUnattributedOut {
		params.Type = domain.ReconciliationIssueUnattributedCashOut
		params.Resolution = domain.ReconciliationResolutionManual
		params.Details = fmt.Sprintf(
			"confirmed pUSD transfer of %s to %s (transaction %s, log %d) left the wallet outside any venue trade; "+
				"the account stays blocked until an operator identifies the outgoing cash",
			transfer.Amount, transfer.Counterparty, transfer.TransactionHash, transfer.LogIndex)
	}
	state.issue(ctx, params)
}

// recordChainCashBalanceDrift reports a balance that the ledger plus every
// known transfer still cannot explain. Less money on-chain than expected is
// an account-wide manual gate; more money is observation only.
func (state *runState) recordChainCashBalanceDrift(
	ctx context.Context, result chaincash.SyncResult, expected domain.Decimal, external domain.ExternalBalance,
) {
	resolution := domain.ReconciliationResolutionManual
	details := "on-chain pUSD balance is below the ledger plus every known transfer; do not overwrite the ledger without attributable cash/fill/redeem evidence"
	if comparison, err := external.Amount.Compare(expected); err == nil && comparison > 0 {
		resolution = domain.ReconciliationResolutionObserved
		details = "on-chain pUSD balance exceeds the ledger plus every known transfer; the surplus is not credited and does not block trading"
	}
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueBalanceDrift, Resolution: resolution, Status: domain.ReconciliationIssueOpen,
		LocalValue: expected, RemoteValue: external.Amount, RemoteBlockNumber: external.BlockNumber, Source: external.Source,
		Details: fmt.Sprintf("%s (ledger total %s, persisted transfer offset %s, unconfirmed transfer offset %s, chain cash block %d)",
			details, result.LedgerTotal, result.PersistedOffset, result.PendingOffset, result.ProcessedBlock),
	})
}

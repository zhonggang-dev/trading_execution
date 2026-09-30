// Package chaincash keeps an append-only ledger of on-chain pUSD transfers for
// opted-in execution accounts and credits attributable external cash (platform
// rewards and self-funded deposits) exactly once. It never decides whether the
// account is safe to trade; reconciliation consumes the returned offset.
package chaincash

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// defaultMaxBlocksPerRun bounds one run to 15 chunks of 100 blocks, i.e. 60
// eth_getLogs calls (four per chunk), well inside the per-account timeout.
const defaultMaxBlocksPerRun = uint64(1500)

// LogSource reads the pUSD Transfer logs of one wallet together with the venue
// OrderFilled logs naming it, over the same inclusive block range. A failure
// of either read fails the whole call.
type LogSource interface {
	ReadWalletLogs(ctx context.Context, walletAddress string, fromBlock, toBlock uint64) (domain.ChainCashLogs, error)
	RequireBlock(ctx context.Context, blockNumber uint64) error
}

// Store persists the chain cash ledger. ApplyChainCashRange runs in one
// transaction: lock the account, persist transfers, credit REWARD/DEPOSIT,
// advance the cursor, and read the ledger total and persisted offset.
type Store interface {
	GetChainCashCursor(ctx context.Context, executionAccountID string) (domain.ChainCashCursor, bool, error)
	ListRedemptionTransactionHashes(ctx context.Context, executionAccountID string, transactionHashes []string) (map[string]struct{}, error)
	ApplyChainCashRange(ctx context.Context, value domain.ChainCashRange) (domain.ChainCashRangeResult, error)
}

// Params configures the chain cash service. Address lists are validated and
// normalized to lowercase; DepositSources maps an account to its funding EOAs.
type Params struct {
	Logs              LogSource
	Store             Store
	ExchangeAddresses []string
	RewardSenders     []string
	DepositSources    map[string][]string
	Confirmations     uint64
	MaxBlocksPerRun   uint64
	Now               func() time.Time
}

// Service synchronizes the chain cash ledger for one account per call.
type Service struct {
	logs            LogSource
	store           Store
	exchanges       map[string]struct{}
	rewardSenders   map[string]struct{}
	depositSources  map[string]map[string]struct{}
	confirmations   uint64
	maxBlocksPerRun uint64
	now             func() time.Time
}

// New validates the dependencies and address lists.
func New(params Params) (*Service, error) {
	if params.Logs == nil || params.Store == nil {
		return nil, fmt.Errorf("chain cash log source and store are required")
	}
	if params.Confirmations == 0 {
		return nil, fmt.Errorf("chain cash confirmations must be positive")
	}
	if params.MaxBlocksPerRun == 0 {
		params.MaxBlocksPerRun = defaultMaxBlocksPerRun
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	exchanges, err := addressSet(params.ExchangeAddresses)
	if err != nil || len(exchanges) == 0 {
		return nil, fmt.Errorf("chain cash exchange addresses are invalid or empty")
	}
	rewards, err := addressSet(params.RewardSenders)
	if err != nil {
		return nil, fmt.Errorf("chain cash reward senders: %w", err)
	}
	deposits := make(map[string]map[string]struct{}, len(params.DepositSources))
	for accountID, addresses := range params.DepositSources {
		accountID = strings.TrimSpace(accountID)
		set, err := addressSet(addresses)
		if accountID == "" || err != nil {
			return nil, fmt.Errorf("chain cash deposit sources for %q are invalid", accountID)
		}
		deposits[accountID] = set
	}
	return &Service{
		logs: params.Logs, store: params.Store, exchanges: exchanges, rewardSenders: rewards,
		depositSources: deposits, confirmations: params.Confirmations,
		maxBlocksPerRun: params.MaxBlocksPerRun, now: params.Now,
	}, nil
}

// SyncParams identifies the account and the block-bound balance snapshot.
type SyncParams struct {
	ExecutionAccountID string
	WalletAddress      string
	SnapshotBlock      uint64
}

// SyncResult is the chain cash view at SnapshotBlock.
type SyncResult struct {
	// Enabled is false when the account has no cursor; nothing else is set.
	Enabled bool
	// CaughtUp is false while the confirmed range exceeded the per-run budget.
	// The persisted progress is kept, but the balance cannot be verified yet.
	CaughtUp        bool
	ProcessedBlock  uint64
	ConfirmedBlock  uint64
	LedgerTotal     domain.Decimal
	PersistedOffset domain.Decimal
	PendingOffset   domain.Decimal
	// Offset = PersistedOffset + PendingOffset; the expected on-chain balance
	// at SnapshotBlock is LedgerTotal + Offset (before the legacy explanations).
	Offset       domain.Decimal
	Persisted    int
	Credited     []domain.ChainCashTransfer
	Reattributed int
	Unreported   []domain.ChainCashTransfer
}

// Sync persists confirmed transfers up to SnapshotBlock-Confirmations, credits
// rewards and deposits, and returns the offset between the ledger and the
// chain at SnapshotBlock. Any read or write failure returns an error and
// leaves the cursor unchanged.
func (service *Service) Sync(ctx context.Context, params SyncParams) (SyncResult, error) {
	accountID := strings.TrimSpace(params.ExecutionAccountID)
	cursor, found, err := service.store.GetChainCashCursor(ctx, accountID)
	if err != nil {
		return SyncResult{}, fmt.Errorf("read chain cash cursor: %w", err)
	}
	if !found {
		return SyncResult{}, nil
	}
	wallet := strings.ToLower(strings.TrimSpace(params.WalletAddress))
	if cursor.ExecutionAccountID != accountID || cursor.WalletAddress != wallet {
		return SyncResult{Enabled: true}, fmt.Errorf("chain cash cursor wallet %s does not match account wallet %s", cursor.WalletAddress, wallet)
	}
	snapshot := params.SnapshotBlock
	if snapshot == 0 || snapshot <= service.confirmations {
		return SyncResult{Enabled: true}, fmt.Errorf("balance snapshot block %d cannot bound the chain cash ledger", snapshot)
	}
	if snapshot < cursor.ProcessedBlock {
		return SyncResult{Enabled: true}, fmt.Errorf(
			"balance snapshot block %d is behind persisted chain cash block %d", snapshot, cursor.ProcessedBlock)
	}
	// Confirm the backend serves the snapshot block before trusting any
	// eth_getLogs answer up to it; a lagging node would return an empty range.
	if err := service.logs.RequireBlock(ctx, snapshot); err != nil {
		return SyncResult{Enabled: true}, err
	}
	confirmed := snapshot - service.confirmations
	target := cursor.ProcessedBlock
	caughtUp := true
	if confirmed > cursor.ProcessedBlock {
		target = confirmed
		if confirmed-cursor.ProcessedBlock > service.maxBlocksPerRun {
			target = cursor.ProcessedBlock + service.maxBlocksPerRun
			caughtUp = false
		}
	}
	var transfers []domain.ChainCashTransfer
	if target > cursor.ProcessedBlock {
		if transfers, err = service.readClassified(ctx, accountID, wallet, cursor.ProcessedBlock+1, target); err != nil {
			return SyncResult{Enabled: true}, err
		}
	}
	// (target, SnapshotBlock] is not final yet. Its transfers are read into
	// memory only: never persisted or credited, so a reorganization cannot
	// leave a durable trace. Both ranges are read before anything is written,
	// so a failed read never moves the cursor.
	var pending []domain.ChainCashTransfer
	if caughtUp && snapshot > target {
		if pending, err = service.readClassified(ctx, accountID, wallet, target+1, snapshot); err != nil {
			return SyncResult{Enabled: true}, err
		}
	}
	applied, err := service.store.ApplyChainCashRange(ctx, domain.ChainCashRange{
		ExecutionAccountID: accountID, FromBlock: cursor.ProcessedBlock, ToBlock: target,
		Transfers: transfers, AppliedAt: service.now().UTC(),
	})
	if err != nil {
		return SyncResult{Enabled: true}, fmt.Errorf("apply chain cash range: %w", err)
	}
	result := SyncResult{
		Enabled: true, CaughtUp: caughtUp, ProcessedBlock: applied.ProcessedBlock, ConfirmedBlock: confirmed,
		LedgerTotal: applied.LedgerTotal, PersistedOffset: applied.PersistedOffset, PendingOffset: "0",
		Persisted: len(transfers), Credited: applied.Credited, Reattributed: applied.Reattributed,
		Unreported: applied.Unreported,
	}
	// Unconfirmed cash owned by the fill and redemption ledgers is excluded:
	// their own pending-finality explanations cover that window. An
	// UNATTRIBUTED_OUT never explains a deficit, confirmed or not: the ledger
	// was not debited, so the account stays gated by BALANCE_DRIFT until an
	// operator repairs the ledger, independent of the outflow's own issue.
	counted := make([]domain.ChainCashTransfer, 0, len(pending))
	for _, transfer := range pending {
		if !transfer.Classification.OwnedByOtherLedger() && transfer.Classification != domain.ChainCashUnattributedOut {
			counted = append(counted, transfer)
		}
	}
	if result.PendingOffset, err = SumSigned(counted); err != nil {
		return result, err
	}
	if result.Offset, err = AddDecimals(result.PersistedOffset, result.PendingOffset); err != nil {
		return result, err
	}
	return result, nil
}

func (service *Service) readClassified(
	ctx context.Context, accountID, wallet string, fromBlock, toBlock uint64,
) ([]domain.ChainCashTransfer, error) {
	logs, err := service.logs.ReadWalletLogs(ctx, wallet, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}
	fills, err := service.fillTransactions(logs.Fills, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}
	transfers := logs.Transfers
	if err := service.classify(ctx, accountID, transfers, fills); err != nil {
		return nil, err
	}
	return transfers, nil
}

// ReadConfirmedTransfers reads and classifies every transfer of the wallet in
// the inclusive range [fromBlock, toBlock] without persisting anything. The
// caller is responsible for the range being final; cmd/chaincashinit uses it
// to catch up an account offline before its cursor becomes visible.
func (service *Service) ReadConfirmedTransfers(
	ctx context.Context, executionAccountID, walletAddress string, fromBlock, toBlock uint64,
) ([]domain.ChainCashTransfer, error) {
	accountID := strings.TrimSpace(executionAccountID)
	wallet, err := normalizeAddress(walletAddress)
	if err != nil || accountID == "" {
		return nil, fmt.Errorf("chain cash account and wallet are required")
	}
	return service.readClassified(ctx, accountID, wallet, fromBlock, toBlock)
}

// fillTransactions returns the block hash of every transaction in the range
// that emitted an OrderFilled from a configured exchange naming the wallet.
// A fill from any other contract is ignored: only the venue exchanges prove
// that a transaction is trade settlement.
func (service *Service) fillTransactions(fills []domain.ChainCashFill, fromBlock, toBlock uint64) (map[string]string, error) {
	result := make(map[string]string, len(fills))
	for _, fill := range fills {
		if err := fill.Validate(); err != nil {
			return nil, err
		}
		if _, exchange := service.exchanges[fill.Exchange]; !exchange {
			continue
		}
		if fill.BlockNumber < fromBlock || fill.BlockNumber > toBlock {
			return nil, fmt.Errorf("OrderFilled %s:%d at block %d is outside [%d, %d]",
				fill.TransactionHash, fill.LogIndex, fill.BlockNumber, fromBlock, toBlock)
		}
		if blockHash, seen := result[fill.TransactionHash]; seen && blockHash != fill.BlockHash {
			return nil, fmt.Errorf("OrderFilled logs of transaction %s disagree on the block hash", fill.TransactionHash)
		}
		result[fill.TransactionHash] = fill.BlockHash
	}
	return result, nil
}

// classify attributes every transfer in place. Rules are ordered exactly as in
// the design: venue trade (exchange counterparty, or a transaction carrying a
// wallet OrderFilled), own redemption mint, reward sender, account deposit
// source, then unattributed by direction. fills maps each fill transaction to
// its block hash.
func (service *Service) classify(
	ctx context.Context, accountID string, transfers []domain.ChainCashTransfer, fills map[string]string,
) error {
	hashes := make([]string, 0)
	for index := range transfers {
		transfer := &transfers[index]
		transfer.ExecutionAccountID = accountID
		if err := transfer.Validate(); err != nil {
			return err
		}
		// The Transfer and OrderFilled reads are separate calls; a block hash
		// mismatch for one transaction means they saw different forks.
		if blockHash, fill := fills[transfer.TransactionHash]; fill && blockHash != transfer.BlockHash {
			return fmt.Errorf("transfer %s and its OrderFilled logs disagree on the block hash", transfer.Reference())
		}
		if transfer.Direction == domain.ChainCashIn && transfer.Counterparty == domain.ChainCashZeroAddress {
			hashes = append(hashes, transfer.TransactionHash)
		}
	}
	redemptions := map[string]struct{}{}
	if len(hashes) > 0 {
		var err error
		redemptions, err = service.store.ListRedemptionTransactionHashes(ctx, accountID, hashes)
		if err != nil {
			return fmt.Errorf("read redemption transaction hashes: %w", err)
		}
	}
	for index := range transfers {
		transfer := &transfers[index]
		transfer.Classification = service.classifyOne(accountID, *transfer, fills, redemptions)
	}
	return nil
}

// classifyOne attributes one transfer. TRADE covers both venue settlement
// shapes in either direction: cash exchanged with an exchange contract, and
// peer-to-peer V2 settlement where pUSD moves directly between the wallet and
// a counterparty maker (or to the fee collector) inside a transaction whose
// exchange OrderFilled names the wallet. A settlement is never matched to an
// individual fill, because the fill ledger owns that cash and an unbooked
// external trade still surfaces in the balance comparison (BUY: less cash,
// account gate; SELL: more cash, observation plus market-scoped position
// drift).
func (service *Service) classifyOne(
	accountID string, transfer domain.ChainCashTransfer, fills map[string]string, redemptions map[string]struct{},
) domain.ChainCashClassification {
	if _, exchange := service.exchanges[transfer.Counterparty]; exchange {
		return domain.ChainCashTrade
	}
	if _, fill := fills[transfer.TransactionHash]; fill {
		return domain.ChainCashTrade
	}
	if transfer.Direction == domain.ChainCashOut {
		return domain.ChainCashUnattributedOut
	}
	if transfer.Counterparty == domain.ChainCashZeroAddress {
		// A redemption pays out by minting pUSD to the wallet. Only a mint in
		// a transaction this account recorded as its own redemption qualifies.
		if _, redeemed := redemptions[transfer.TransactionHash]; redeemed {
			return domain.ChainCashRedemption
		}
		return domain.ChainCashUnattributedIn
	}
	if _, reward := service.rewardSenders[transfer.Counterparty]; reward {
		return domain.ChainCashReward
	}
	if _, deposit := service.depositSources[accountID][transfer.Counterparty]; deposit {
		return domain.ChainCashDeposit
	}
	return domain.ChainCashUnattributedIn
}

// SumSigned returns the exact net balance effect of the given transfers.
func SumSigned(transfers []domain.ChainCashTransfer) (domain.Decimal, error) {
	total := new(big.Rat)
	for _, transfer := range transfers {
		if err := addRat(total, transfer.SignedAmount()); err != nil {
			return "", err
		}
	}
	return ratDecimal(total), nil
}

// AddDecimals returns the exact sum of pUSD amounts.
func AddDecimals(values ...domain.Decimal) (domain.Decimal, error) {
	total := new(big.Rat)
	for _, value := range values {
		if err := addRat(total, value); err != nil {
			return "", err
		}
	}
	return ratDecimal(total), nil
}

// ParseAddressList parses a comma-separated address list, lowercasing and
// validating each 20-byte address. Empty input yields an empty list.
func ParseAddressList(value string) ([]string, error) {
	result := make([]string, 0)
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		address, err := normalizeAddress(part)
		if err != nil {
			return nil, err
		}
		result = append(result, address)
	}
	return result, nil
}

// ParseDepositSources parses "account=0x...,account=0x..." into a map from
// account to funding addresses. An account may be listed more than once.
func ParseDepositSources(value string) (map[string][]string, error) {
	result := make(map[string][]string)
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		accountID, address, found := strings.Cut(part, "=")
		accountID = strings.TrimSpace(accountID)
		if !found || accountID == "" {
			return nil, fmt.Errorf("deposit source %q must be account=0xaddress", part)
		}
		normalized, err := normalizeAddress(address)
		if err != nil {
			return nil, fmt.Errorf("deposit source for %s: %w", accountID, err)
		}
		result[accountID] = append(result[accountID], normalized)
	}
	return result, nil
}

func addressSet(values []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		address, err := normalizeAddress(value)
		if err != nil {
			return nil, err
		}
		result[address] = struct{}{}
	}
	return result, nil
}

func normalizeAddress(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 42 || !strings.HasPrefix(value, "0x") {
		return "", fmt.Errorf("address %q must contain 20 hexadecimal bytes", value)
	}
	for _, character := range value[2:] {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return "", fmt.Errorf("address %q must contain 20 hexadecimal bytes", value)
		}
	}
	if value == domain.ChainCashZeroAddress {
		return "", fmt.Errorf("the zero address cannot be configured")
	}
	return value, nil
}

func addRat(total *big.Rat, value domain.Decimal) error {
	parsed, ok := new(big.Rat).SetString(strings.TrimSpace(value.String()))
	if !ok || strings.ContainsAny(value.String(), "/eE") {
		return fmt.Errorf("invalid decimal %q", value)
	}
	total.Add(total, parsed)
	return nil
}

// ratDecimal renders a pUSD amount (at most six decimals) exactly.
func ratDecimal(value *big.Rat) domain.Decimal {
	text := value.FloatString(6)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	if text == "-0" || text == "" {
		text = "0"
	}
	return domain.Decimal(text)
}

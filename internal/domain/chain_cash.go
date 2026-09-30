package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ChainCashDirection is the direction of one pUSD Transfer relative to the
// execution wallet.
type ChainCashDirection string

const (
	ChainCashIn  ChainCashDirection = "IN"
	ChainCashOut ChainCashDirection = "OUT"
)

// ChainCashClassification attributes one on-chain pUSD Transfer. Only REWARD
// and DEPOSIT are credited by the chain cash ledger; TRADE and REDEMPTION cash
// is owned by the fill and redemption ledgers; UNATTRIBUTED_* is audit only.
type ChainCashClassification string

const (
	ChainCashTrade           ChainCashClassification = "TRADE"
	ChainCashRedemption      ChainCashClassification = "REDEMPTION"
	ChainCashReward          ChainCashClassification = "REWARD"
	ChainCashDeposit         ChainCashClassification = "DEPOSIT"
	ChainCashUnattributedIn  ChainCashClassification = "UNATTRIBUTED_IN"
	ChainCashUnattributedOut ChainCashClassification = "UNATTRIBUTED_OUT"
)

// Credited reports whether the chain cash ledger itself credits this class.
func (classification ChainCashClassification) Credited() bool {
	return classification == ChainCashReward || classification == ChainCashDeposit
}

// OwnedByOtherLedger reports whether another ledger (fills or redemptions)
// already accounts for the cash of this class, including its timing windows.
func (classification ChainCashClassification) OwnedByOtherLedger() bool {
	return classification == ChainCashTrade || classification == ChainCashRedemption
}

// ChainCashTransfer is one ERC20 Transfer log that moved pUSD into or out of
// an execution wallet. Amount is always positive; Direction carries the sign.
type ChainCashTransfer struct {
	ExecutionAccountID string                  `json:"execution_account_id"`
	TransactionHash    string                  `json:"transaction_hash"`
	LogIndex           uint64                  `json:"log_index"`
	BlockNumber        uint64                  `json:"block_number"`
	BlockHash          string                  `json:"block_hash"`
	Direction          ChainCashDirection      `json:"direction"`
	Counterparty       string                  `json:"counterparty"`
	Amount             Decimal                 `json:"amount"`
	Classification     ChainCashClassification `json:"classification,omitempty"`
	AccountEventID     string                  `json:"account_event_id,omitempty"`
	ObservedAt         time.Time               `json:"observed_at"`
}

// Reference is the stable per-log identity used by audit issues.
func (transfer ChainCashTransfer) Reference() string {
	return transfer.TransactionHash + ":" + strconv.FormatUint(transfer.LogIndex, 10)
}

// SignedAmount returns the balance effect on the execution wallet.
func (transfer ChainCashTransfer) SignedAmount() Decimal {
	amount := strings.TrimPrefix(strings.TrimSpace(transfer.Amount.String()), "-")
	if transfer.Direction == ChainCashOut {
		return Decimal("-" + amount)
	}
	return Decimal(amount)
}

// ChainCashAccountEventID returns the deterministic ledger event identity of a
// credit. The account is part of the identity because one Transfer log between
// two managed wallets is stored once per account.
func ChainCashAccountEventID(transfer ChainCashTransfer) string {
	return "account-chain-cash:" + transfer.ExecutionAccountID + ":" + transfer.Reference()
}

// Validate checks the identity and shape of a transfer read from a log source.
func (transfer ChainCashTransfer) Validate() error {
	if !isLowerHex(transfer.TransactionHash, 32) || !isLowerHex(transfer.BlockHash, 32) {
		return fmt.Errorf("chain cash transfer hashes must be lowercase 32-byte hex")
	}
	if !isLowerHex(transfer.Counterparty, 20) {
		return fmt.Errorf("chain cash transfer counterparty must be a lowercase address")
	}
	if transfer.BlockNumber == 0 {
		return fmt.Errorf("chain cash transfer block number is required")
	}
	if transfer.Direction != ChainCashIn && transfer.Direction != ChainCashOut {
		return fmt.Errorf("chain cash transfer direction %q is invalid", transfer.Direction)
	}
	if sign, err := transfer.Amount.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("chain cash transfer amount must be positive")
	}
	return nil
}

// ChainCashFill is one Polymarket V2 OrderFilled log that names the execution
// wallet as maker or taker. It is settlement evidence only: V2 settles many
// fills peer-to-peer (wallet -> counterparty maker, wallet -> fee collector),
// so every pUSD Transfer in a transaction that carries such a log is venue
// trade cash owned by the fill ledger, whatever its counterparty.
type ChainCashFill struct {
	TransactionHash string `json:"transaction_hash"`
	LogIndex        uint64 `json:"log_index"`
	BlockNumber     uint64 `json:"block_number"`
	BlockHash       string `json:"block_hash"`
	Exchange        string `json:"exchange"`
	Maker           string `json:"maker"`
	Taker           string `json:"taker"`
}

// Validate checks the identity and shape of a fill read from a log source.
func (fill ChainCashFill) Validate() error {
	if !isLowerHex(fill.TransactionHash, 32) || !isLowerHex(fill.BlockHash, 32) {
		return fmt.Errorf("chain cash fill hashes must be lowercase 32-byte hex")
	}
	if !isLowerHex(fill.Exchange, 20) || !isLowerHex(fill.Maker, 20) || !isLowerHex(fill.Taker, 20) {
		return fmt.Errorf("chain cash fill exchange, maker, and taker must be lowercase addresses")
	}
	if fill.BlockNumber == 0 {
		return fmt.Errorf("chain cash fill block number is required")
	}
	return nil
}

// ChainCashLogs is everything one wallet log read returns for an inclusive
// block range: every pUSD Transfer into or out of the wallet, and every venue
// OrderFilled naming the wallet. Both are read in the same chunks, so a
// failure of either fails the whole read.
type ChainCashLogs struct {
	Transfers []ChainCashTransfer
	Fills     []ChainCashFill
}

// ChainCashCursor is the opt-in and progress row of one account. Accounts
// without a cursor keep the legacy balance comparison.
type ChainCashCursor struct {
	ExecutionAccountID string    `json:"execution_account_id"`
	WalletAddress      string    `json:"wallet_address"`
	TokenAddress       string    `json:"token_address"`
	StartBlock         uint64    `json:"start_block"`
	StartBalance       Decimal   `json:"start_balance"`
	ProcessedBlock     uint64    `json:"processed_block"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// ChainCashRange persists every classified confirmed transfer in
// (FromBlock, ToBlock] and advances the cursor from FromBlock to ToBlock.
type ChainCashRange struct {
	ExecutionAccountID string
	FromBlock          uint64
	ToBlock            uint64
	Transfers          []ChainCashTransfer
	AppliedAt          time.Time
}

// ChainCashRangeResult is read inside the same transaction that applied the
// range, so LedgerTotal and PersistedOffset describe one consistent instant.
type ChainCashRangeResult struct {
	ProcessedBlock uint64
	Credited       []ChainCashTransfer
	Reattributed   int
	LedgerTotal    Decimal
	// PersistedOffset = uncredited REWARD/DEPOSIT + UNATTRIBUTED_IN (never UNATTRIBUTED_OUT).
	PersistedOffset Decimal
	// Unreported lists unattributed transfers without an audit issue yet.
	Unreported []ChainCashTransfer
}

// ChainCashZeroAddress is the ERC20 mint/burn counterparty.
const ChainCashZeroAddress = "0x0000000000000000000000000000000000000000"

func isLowerHex(value string, byteLength int) bool {
	if len(value) != 2+2*byteLength || !strings.HasPrefix(value, "0x") {
		return false
	}
	for _, character := range value[2:] {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

package evmrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// MaxTransferLogChunkBlocks is the largest inclusive block span of a single
// eth_getLogs call. The production dRPC plan rejects spans above 101 blocks.
const MaxTransferLogChunkBlocks = uint64(100)

// ERC20TransferLogParams configures a read-only ERC20 Transfer log reader.
// ExchangeAddresses are the venue contracts whose OrderFilled logs mark a
// transaction as trade settlement.
type ERC20TransferLogParams struct {
	RPCURL            string
	TokenAddress      string
	ExchangeAddresses []string
	Decimals          int
	ChunkBlocks       uint64
	HTTPClient        *http.Client
	RequestTimeout    time.Duration
	ReadAttempts      int
	Now               func() time.Time
}

// ERC20TransferLogReader lists every Transfer of one ERC20 token into or out
// of a wallet, together with every venue OrderFilled naming the wallet as
// maker or taker. It reuses the bounded JSON-RPC transport and retry rules of
// the OrderFilled reader, but never consults its latest-block high-water mark:
// the caller supplies explicit block ranges.
type ERC20TransferLogReader struct {
	rpc         *OrderFilledEvidenceReader
	token       string
	exchanges   []string
	exchangeSet map[string]struct{}
	decimals    int
	chunkBlocks uint64
	now         func() time.Time
}

// NewERC20TransferLogReader validates the log reader configuration.
func NewERC20TransferLogReader(params ERC20TransferLogParams) (*ERC20TransferLogReader, error) {
	token, err := normalizedAddress(params.TokenAddress)
	if err != nil {
		return nil, fmt.Errorf("ERC20 token address: %w", err)
	}
	if params.Decimals < 0 || params.Decimals > 36 {
		return nil, fmt.Errorf("ERC20 decimals are invalid")
	}
	exchanges := make([]string, 0, len(params.ExchangeAddresses))
	exchangeSet := make(map[string]struct{}, len(params.ExchangeAddresses))
	for _, value := range params.ExchangeAddresses {
		exchange, err := normalizedAddress(value)
		if err != nil {
			return nil, fmt.Errorf("exchange address: %w", err)
		}
		if _, duplicate := exchangeSet[exchange]; !duplicate {
			exchangeSet[exchange] = struct{}{}
			exchanges = append(exchanges, exchange)
		}
	}
	if len(exchanges) == 0 {
		return nil, fmt.Errorf("at least one exchange address is required")
	}
	if params.ChunkBlocks == 0 {
		params.ChunkBlocks = MaxTransferLogChunkBlocks
	}
	if params.ChunkBlocks > MaxTransferLogChunkBlocks {
		return nil, fmt.Errorf("transfer log chunk must not exceed %d blocks", MaxTransferLogChunkBlocks)
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	rpc, err := NewOrderFilledEvidenceReader(OrderFilledEvidenceParams{
		RPCURL: params.RPCURL, RequiredConfirmations: 1, HTTPClient: params.HTTPClient,
		RequestTimeout: params.RequestTimeout, ReadAttempts: params.ReadAttempts,
	})
	if err != nil {
		return nil, err
	}
	return &ERC20TransferLogReader{
		rpc: rpc, token: token, exchanges: exchanges, exchangeSet: exchangeSet,
		decimals: params.Decimals, chunkBlocks: params.ChunkBlocks, now: params.Now,
	}, nil
}

// fillRole is the OrderFilled topic position that names the wallet.
type fillRole int

const (
	fillRoleMaker fillRole = 2
	fillRoleTaker fillRole = 3
)

// ReadWalletLogs returns, for the inclusive range [fromBlock, toBlock]:
//   - every non-zero Transfer of the token touching wallet, ordered by block
//     and log index (a wallet-to-itself transfer does not change the balance
//     and is omitted);
//   - every OrderFilled emitted by a configured exchange that names wallet as
//     maker or taker.
//
// Each chunk of at most MaxTransferLogChunkBlocks blocks issues four
// eth_getLogs calls: Transfer IN, Transfer OUT, OrderFilled as maker, and
// OrderFilled as taker. Any failed call fails the whole read: a partial result
// is never returned.
func (reader *ERC20TransferLogReader) ReadWalletLogs(
	ctx context.Context, walletAddress string, fromBlock, toBlock uint64,
) (domain.ChainCashLogs, error) {
	wallet, err := normalizedAddress(walletAddress)
	if err != nil {
		return domain.ChainCashLogs{}, fmt.Errorf("wallet address: %w", err)
	}
	if fromBlock == 0 || toBlock < fromBlock {
		return domain.ChainCashLogs{}, fmt.Errorf("transfer log range [%d, %d] is invalid", fromBlock, toBlock)
	}
	walletTopic := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(wallet, "0x")
	transfers := make([]domain.ChainCashTransfer, 0)
	fills := make([]domain.ChainCashFill, 0)
	seen := make(map[string]struct{})
	seenFills := make(map[string]struct{})
	observedAt := reader.now().UTC()
	for start := fromBlock; start <= toBlock; {
		end := toBlock
		if end-start >= reader.chunkBlocks {
			end = start + reader.chunkBlocks - 1
		}
		for _, direction := range []domain.ChainCashDirection{domain.ChainCashIn, domain.ChainCashOut} {
			topics := []any{transferTopic, nil, walletTopic}
			if direction == domain.ChainCashOut {
				topics = []any{transferTopic, walletTopic, nil}
			}
			logs, err := reader.getLogs(ctx, reader.token, start, end, topics)
			if err != nil {
				return domain.ChainCashLogs{}, fmt.Errorf("read %s transfer logs [%d, %d]: %w", direction, start, end, err)
			}
			for _, entry := range logs {
				transfer, skip, err := reader.decodeTransfer(entry, wallet, direction, start, end)
				if err != nil {
					return domain.ChainCashLogs{}, fmt.Errorf("transfer log [%d, %d]: %w", start, end, err)
				}
				if skip {
					continue
				}
				key := transfer.Reference()
				if _, duplicate := seen[key]; duplicate {
					return domain.ChainCashLogs{}, fmt.Errorf("transfer log %s was returned twice", key)
				}
				seen[key] = struct{}{}
				transfer.ObservedAt = observedAt
				transfers = append(transfers, transfer)
			}
		}
		for _, role := range []fillRole{fillRoleMaker, fillRoleTaker} {
			topics := []any{orderFilledV2Topic, nil, walletTopic}
			if role == fillRoleTaker {
				topics = []any{orderFilledV2Topic, nil, nil, walletTopic}
			}
			logs, err := reader.getLogs(ctx, reader.exchanges, start, end, topics)
			if err != nil {
				return domain.ChainCashLogs{}, fmt.Errorf("read OrderFilled logs [%d, %d]: %w", start, end, err)
			}
			for _, entry := range logs {
				fill, err := reader.decodeFill(entry, wallet, role, start, end)
				if err != nil {
					return domain.ChainCashLogs{}, fmt.Errorf("OrderFilled log [%d, %d]: %w", start, end, err)
				}
				// A self-matched fill names the wallet as maker and taker
				// and is legitimately returned by both queries.
				key := fill.TransactionHash + ":" + strconv.FormatUint(fill.LogIndex, 10)
				if _, duplicate := seenFills[key]; duplicate {
					continue
				}
				seenFills[key] = struct{}{}
				fills = append(fills, fill)
			}
		}
		if end == toBlock {
			break
		}
		start = end + 1
	}
	sort.Slice(transfers, func(left, right int) bool {
		if transfers[left].BlockNumber != transfers[right].BlockNumber {
			return transfers[left].BlockNumber < transfers[right].BlockNumber
		}
		return transfers[left].LogIndex < transfers[right].LogIndex
	})
	sort.Slice(fills, func(left, right int) bool {
		if fills[left].BlockNumber != fills[right].BlockNumber {
			return fills[left].BlockNumber < fills[right].BlockNumber
		}
		return fills[left].LogIndex < fills[right].LogIndex
	})
	return domain.ChainCashLogs{Transfers: transfers, Fills: fills}, nil
}

// RequireBlock fails unless the RPC backend already serves the given block.
// A load-balanced backend that lags the balance snapshot would otherwise
// answer eth_getLogs with an incomplete, empty-looking range.
func (reader *ERC20TransferLogReader) RequireBlock(ctx context.Context, blockNumber uint64) error {
	_, _, err := reader.BlockHeader(ctx, blockNumber)
	return err
}

// BlockHeader returns the canonical hash and timestamp of one block.
func (reader *ERC20TransferLogReader) BlockHeader(ctx context.Context, blockNumber uint64) (string, time.Time, error) {
	raw, err := reader.rpc.rpcCall(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", blockNumber), false})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read block %d: %w", blockNumber, err)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", time.Time{}, fmt.Errorf("RPC backend does not serve block %d yet", blockNumber)
	}
	var header struct {
		Number    string `json:"number"`
		Hash      string `json:"hash"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return "", time.Time{}, fmt.Errorf("decode block %d: %w", blockNumber, err)
	}
	number, err := parseCanonicalQuantity(header.Number, "block number")
	if err != nil || number != blockNumber {
		return "", time.Time{}, fmt.Errorf("RPC returned block %q for requested block %d", header.Number, blockNumber)
	}
	hash, err := normalizedFixedHex(header.Hash, 32, "block hash")
	if err != nil {
		return "", time.Time{}, err
	}
	timestamp, err := parseCanonicalQuantity(header.Timestamp, "block timestamp")
	if err != nil {
		return "", time.Time{}, err
	}
	return hash, time.Unix(int64(timestamp), 0).UTC(), nil
}

// LatestBlock returns the current head of the RPC backend.
func (reader *ERC20TransferLogReader) LatestBlock(ctx context.Context) (uint64, error) {
	return reader.rpc.getLatestBlock(ctx)
}

// ChainID returns the chain identifier of the RPC backend.
func (reader *ERC20TransferLogReader) ChainID(ctx context.Context) (uint64, error) {
	return reader.rpc.getChainID(ctx)
}

// BalanceAt reads balanceOf(wallet) of the configured token at an exact block.
func (reader *ERC20TransferLogReader) BalanceAt(ctx context.Context, walletAddress string, blockNumber uint64) (domain.Decimal, error) {
	if reader.decimals != 6 {
		return "", fmt.Errorf("balance reads support six-decimal tokens only")
	}
	value, err := reader.rpc.ReadTokenBalance(ctx, reader.token, walletAddress, "", fmt.Sprintf("0x%x", blockNumber))
	if err != nil {
		return "", err
	}
	return domain.ParseDecimal(value)
}

// getLogs issues one eth_getLogs call. address is one contract or a list of
// contracts; the span is never wider than MaxTransferLogChunkBlocks.
func (reader *ERC20TransferLogReader) getLogs(
	ctx context.Context, address any, fromBlock, toBlock uint64, topics []any,
) ([]rpcLog, error) {
	if toBlock < fromBlock || toBlock-fromBlock+1 > MaxTransferLogChunkBlocks {
		return nil, fmt.Errorf("log chunk [%d, %d] exceeds %d blocks", fromBlock, toBlock, MaxTransferLogChunkBlocks)
	}
	raw, err := reader.rpc.rpcCall(ctx, "eth_getLogs", []any{map[string]any{
		"address":   address,
		"fromBlock": fmt.Sprintf("0x%x", fromBlock),
		"toBlock":   fmt.Sprintf("0x%x", toBlock),
		"topics":    topics,
	}})
	if err != nil {
		return nil, err
	}
	var logs []rpcLog
	if err := json.Unmarshal(raw, &logs); err != nil {
		return nil, fmt.Errorf("decode eth_getLogs result: %w", err)
	}
	return logs, nil
}

// decodeTransfer validates one log against the query that produced it. A
// removed log, a foreign contract, an out-of-range block, or a topic that does
// not name the wallet on the queried side fails the read.
func (reader *ERC20TransferLogReader) decodeTransfer(
	entry rpcLog, wallet string, direction domain.ChainCashDirection, fromBlock, toBlock uint64,
) (domain.ChainCashTransfer, bool, error) {
	address, err := normalizedAddress(entry.Address)
	if err != nil || address != reader.token {
		return domain.ChainCashTransfer{}, false, fmt.Errorf("log address %q is not the configured token", entry.Address)
	}
	// eth_getLogs only reports removed=true to reorg-aware subscriptions; an
	// omitted flag on a plain range query is a canonical log.
	if entry.Removed != nil && *entry.Removed {
		return domain.ChainCashTransfer{}, false, fmt.Errorf("log was removed by a reorganization")
	}
	if len(entry.Topics) != 3 || !equalFixedHex(entry.Topics[0], transferTopic, 32) {
		return domain.ChainCashTransfer{}, false, fmt.Errorf("log is not an ERC20 Transfer with three topics")
	}
	from, err := addressFromTopic(entry.Topics[1], "transfer sender")
	if err != nil {
		return domain.ChainCashTransfer{}, false, err
	}
	to, err := addressFromTopic(entry.Topics[2], "transfer recipient")
	if err != nil {
		return domain.ChainCashTransfer{}, false, err
	}
	counterparty := from
	if direction == domain.ChainCashIn && to != wallet || direction == domain.ChainCashOut && from != wallet {
		return domain.ChainCashTransfer{}, false, fmt.Errorf("log does not name the wallet on the queried side")
	}
	if direction == domain.ChainCashOut {
		counterparty = to
	}
	blockNumber, err := parseCanonicalQuantity(entry.BlockNumber, "log block number")
	if err != nil {
		return domain.ChainCashTransfer{}, false, err
	}
	if blockNumber < fromBlock || blockNumber > toBlock {
		return domain.ChainCashTransfer{}, false, fmt.Errorf("log block %d is outside [%d, %d]", blockNumber, fromBlock, toBlock)
	}
	transactionHash, err := normalizedFixedHex(entry.TransactionHash, 32, "log transaction hash")
	if err != nil {
		return domain.ChainCashTransfer{}, false, err
	}
	blockHash, err := normalizedFixedHex(entry.BlockHash, 32, "log block hash")
	if err != nil {
		return domain.ChainCashTransfer{}, false, err
	}
	logIndex, err := parseCanonicalQuantity(entry.LogIndex, "log index")
	if err != nil {
		return domain.ChainCashTransfer{}, false, err
	}
	data, err := decodeFixedHex(entry.Data, 32, "transfer amount")
	if err != nil {
		return domain.ChainCashTransfer{}, false, err
	}
	units := new(big.Int).SetBytes(data)
	if units.Sign() == 0 || from == to {
		// Zero-value and self transfers never change the wallet balance.
		return domain.ChainCashTransfer{}, true, nil
	}
	transfer := domain.ChainCashTransfer{
		TransactionHash: transactionHash, LogIndex: logIndex, BlockNumber: blockNumber, BlockHash: blockHash,
		Direction: direction, Counterparty: counterparty, Amount: domain.Decimal(formatUnits(units, reader.decimals)),
	}
	return transfer, false, transfer.Validate()
}

// decodeFill validates one OrderFilled log against the query that produced
// it. A removed log, a contract outside the exchange set, a malformed topic
// list, an out-of-range block, or a log that does not name the wallet in the
// queried role fails the read.
func (reader *ERC20TransferLogReader) decodeFill(
	entry rpcLog, wallet string, role fillRole, fromBlock, toBlock uint64,
) (domain.ChainCashFill, error) {
	exchange, err := normalizedAddress(entry.Address)
	if err != nil {
		return domain.ChainCashFill{}, fmt.Errorf("log address %q is not a configured exchange", entry.Address)
	}
	if _, known := reader.exchangeSet[exchange]; !known {
		return domain.ChainCashFill{}, fmt.Errorf("log address %q is not a configured exchange", entry.Address)
	}
	if entry.Removed != nil && *entry.Removed {
		return domain.ChainCashFill{}, fmt.Errorf("log was removed by a reorganization")
	}
	if len(entry.Topics) != 4 || !equalFixedHex(entry.Topics[0], orderFilledV2Topic, 32) {
		return domain.ChainCashFill{}, fmt.Errorf("log is not a V2 OrderFilled with four topics")
	}
	maker, err := addressFromTopic(entry.Topics[2], "fill maker")
	if err != nil {
		return domain.ChainCashFill{}, err
	}
	taker, err := addressFromTopic(entry.Topics[3], "fill taker")
	if err != nil {
		return domain.ChainCashFill{}, err
	}
	if role == fillRoleMaker && maker != wallet || role == fillRoleTaker && taker != wallet {
		return domain.ChainCashFill{}, fmt.Errorf("log does not name the wallet in the queried role")
	}
	blockNumber, err := parseCanonicalQuantity(entry.BlockNumber, "log block number")
	if err != nil {
		return domain.ChainCashFill{}, err
	}
	if blockNumber < fromBlock || blockNumber > toBlock {
		return domain.ChainCashFill{}, fmt.Errorf("log block %d is outside [%d, %d]", blockNumber, fromBlock, toBlock)
	}
	transactionHash, err := normalizedFixedHex(entry.TransactionHash, 32, "log transaction hash")
	if err != nil {
		return domain.ChainCashFill{}, err
	}
	blockHash, err := normalizedFixedHex(entry.BlockHash, 32, "log block hash")
	if err != nil {
		return domain.ChainCashFill{}, err
	}
	logIndex, err := parseCanonicalQuantity(entry.LogIndex, "log index")
	if err != nil {
		return domain.ChainCashFill{}, err
	}
	fill := domain.ChainCashFill{
		TransactionHash: transactionHash, LogIndex: logIndex, BlockNumber: blockNumber, BlockHash: blockHash,
		Exchange: exchange, Maker: maker, Taker: taker,
	}
	return fill, fill.Validate()
}

package evmrpc

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// ErrOrderFilledScanWindowNotReached reports that the newest confirmed block
// is still older than the scan must reach. Nothing is wrong; retry later.
var ErrOrderFilledScanWindowNotReached = domain.ErrNoFillChainWindowNotReached

const (
	// orderFilledScanBlockSeconds under-estimates Polygon's ~2s block time so
	// the estimated first block is never too late; the header check corrects it.
	orderFilledScanBlockSeconds = 1.5
	orderFilledScanMaxStepBacks = 10
)

// OrderFilledScanParams configures the read-only OrderFilled-by-hash scanner.
type OrderFilledScanParams struct {
	RPCURL                string
	RequiredConfirmations uint64
	HTTPClient            *http.Client
	RequestTimeout        time.Duration
	ReadAttempts          int
	ChunkBlocks           uint64
	Now                   func() time.Time
}

// OrderFilledScanner proves that no Polymarket V2 exchange emitted an
// OrderFilled event for one order hash in a confirmed block range. Every
// failure, incomplete read, head regression or reorganization is an error;
// the caller must treat an error as "unknown", never as "no fill".
type OrderFilledScanner struct {
	rpc           *OrderFilledEvidenceReader
	confirmations uint64
	chunkBlocks   uint64
	rpcHost       string
	exchanges     []string
	now           func() time.Time
}

// NewOrderFilledScanner validates the scanner configuration.
func NewOrderFilledScanner(params OrderFilledScanParams) (*OrderFilledScanner, error) {
	if params.RequiredConfirmations == 0 {
		return nil, fmt.Errorf("required confirmations must be positive")
	}
	if params.ChunkBlocks == 0 {
		params.ChunkBlocks = MaxTransferLogChunkBlocks
	}
	if params.ChunkBlocks > MaxTransferLogChunkBlocks {
		return nil, fmt.Errorf("OrderFilled scan chunk must not exceed %d blocks", MaxTransferLogChunkBlocks)
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
	host := ""
	if parsed, parseErr := url.Parse(strings.TrimSpace(params.RPCURL)); parseErr == nil {
		// Only the host: provider keys live in the path or the query string.
		host = parsed.Hostname()
	}
	return &OrderFilledScanner{
		rpc: rpc, confirmations: params.RequiredConfirmations, chunkBlocks: params.ChunkBlocks,
		rpcHost: host, now: params.Now,
		exchanges: []string{PolygonCTFExchangeV2Address, PolygonNegRiskCTFExchangeV2Address},
	}, nil
}

// ScanOrderFilled scans [first block not newer than FromNotAfter, head minus
// confirmations] for OrderFilled(orderHash) on both V2 exchanges. Matches are
// returned as evidence, not as an error.
func (scanner *OrderFilledScanner) ScanOrderFilled(
	ctx context.Context, request domain.NoFillChainScanRequest,
) (domain.NoFillChainEvidence, error) {
	orderHash, err := normalizedFixedHex(request.OrderHash, 32, "order hash")
	if err != nil {
		return domain.NoFillChainEvidence{}, err
	}
	if request.FromNotAfter.IsZero() || request.ToNotBefore.IsZero() || request.FromNotAfter.After(request.ToNotBefore) {
		return domain.NoFillChainEvidence{}, fmt.Errorf("OrderFilled scan window is invalid")
	}
	if request.MaxChunks <= 0 {
		return domain.NoFillChainEvidence{}, fmt.Errorf("OrderFilled scan requires a positive chunk budget")
	}
	evidence := domain.NoFillChainEvidence{
		Exchanges: append([]string(nil), scanner.exchanges...), Topic0: orderFilledV2Topic, Topic1: orderHash,
		Confirmations: scanner.confirmations, ChunkBlocks: scanner.chunkBlocks, RPCHost: scanner.rpcHost,
		Matches: []domain.NoFillChainMatch{}, Controls: []domain.NoFillChainControl{},
	}
	chainID, err := scanner.rpc.getChainID(ctx)
	if err != nil {
		return domain.NoFillChainEvidence{}, err
	}
	if chainID != polygonChainID {
		return domain.NoFillChainEvidence{}, fmt.Errorf("RPC chain ID is %d; require Polygon chain ID %d", chainID, polygonChainID)
	}
	evidence.ChainID = chainID
	head, err := scanner.stableHead(ctx)
	if err != nil {
		return domain.NoFillChainEvidence{}, err
	}
	evidence.HeadBefore = head
	if head <= scanner.confirmations {
		return domain.NoFillChainEvidence{}, fmt.Errorf("chain head %d is not past %d confirmations", head, scanner.confirmations)
	}
	toBlock := head - scanner.confirmations
	toHash, toTime, err := rpcBlockHeader(ctx, scanner.rpc, toBlock)
	if err != nil {
		return domain.NoFillChainEvidence{}, err
	}
	if toTime.Before(request.ToNotBefore.UTC()) {
		return domain.NoFillChainEvidence{}, fmt.Errorf("%w: block %d at %s, need %s",
			ErrOrderFilledScanWindowNotReached, toBlock, toTime.Format(time.RFC3339), request.ToNotBefore.UTC().Format(time.RFC3339))
	}
	evidence.ToBlock, evidence.ToBlockTime, evidence.ToBlockHashBefore = toBlock, toTime, toHash
	fromBlock, fromTime, err := scanner.firstBlockNotAfter(ctx, toBlock, toTime, request.FromNotAfter.UTC())
	if err != nil {
		return domain.NoFillChainEvidence{}, err
	}
	evidence.FromBlock, evidence.FromBlockTime = fromBlock, fromTime
	span := toBlock - fromBlock + 1
	chunks := int((span + scanner.chunkBlocks - 1) / scanner.chunkBlocks)
	if chunks > request.MaxChunks {
		return domain.NoFillChainEvidence{}, fmt.Errorf("OrderFilled scan [%d, %d] needs %d chunks; budget is %d",
			fromBlock, toBlock, chunks, request.MaxChunks)
	}
	evidence.Chunks = chunks

	seen := make(map[string]struct{})
	for start := fromBlock; start <= toBlock; {
		if err := ctx.Err(); err != nil {
			return domain.NoFillChainEvidence{}, err
		}
		end := toBlock
		if end-start >= scanner.chunkBlocks {
			end = start + scanner.chunkBlocks - 1
		}
		logs, err := rpcGetLogs(ctx, scanner.rpc, scanner.exchanges, start, end, []any{orderFilledV2Topic, orderHash})
		if err != nil {
			return domain.NoFillChainEvidence{}, fmt.Errorf("read OrderFilled logs [%d, %d]: %w", start, end, err)
		}
		for _, entry := range logs {
			match, err := scanner.decodeMatch(entry, orderHash, start, end)
			if err != nil {
				return domain.NoFillChainEvidence{}, fmt.Errorf("OrderFilled log [%d, %d]: %w", start, end, err)
			}
			key := fmt.Sprintf("%s:%d", match.TransactionHash, match.LogIndex)
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			evidence.Matches = append(evidence.Matches, match)
		}
		if end == toBlock {
			break
		}
		start = end + 1
	}

	// The scanned range must still be canonical and still confirmed.
	afterHash, _, err := rpcBlockHeader(ctx, scanner.rpc, toBlock)
	if err != nil {
		return domain.NoFillChainEvidence{}, err
	}
	if afterHash != toHash {
		return domain.NoFillChainEvidence{}, fmt.Errorf("block %d hash changed during the scan; possible reorganization", toBlock)
	}
	evidence.ToBlockHashAfter = afterHash
	headAfter, err := scanner.rpc.getLatestBlock(ctx)
	if err != nil {
		return domain.NoFillChainEvidence{}, err
	}
	if headAfter < toBlock || headAfter-toBlock < scanner.confirmations {
		return domain.NoFillChainEvidence{}, fmt.Errorf("chain head %d fell below %d confirmations of block %d", headAfter, scanner.confirmations, toBlock)
	}
	evidence.HeadAfter = headAfter
	evidence.ReadAt = scanner.now().UTC()
	if len(evidence.Matches) > 0 {
		return evidence, nil
	}
	// Negative evidence is only trusted when the same backend demonstrably
	// returns OrderFilled logs for the scanned range (a lagging node answers
	// an unsynced range with an empty array).
	middle := fromBlock + (toBlock-fromBlock)/2
	for _, sample := range []uint64{toBlock, middle, fromBlock} {
		control, err := scanner.control(ctx, sample)
		if err != nil {
			return domain.NoFillChainEvidence{}, err
		}
		evidence.Controls = append(evidence.Controls, control)
	}
	return evidence, nil
}

// stableHead reads the head twice and returns the lower value; a fall larger
// than a lagging backend explains is an error.
func (scanner *OrderFilledScanner) stableHead(ctx context.Context) (uint64, error) {
	first, err := scanner.rpc.getLatestBlock(ctx)
	if err != nil {
		return 0, err
	}
	second, err := scanner.rpc.getLatestBlock(ctx)
	if err != nil {
		return 0, err
	}
	if second < first && first-second > maxTolerableLatestBlockLag {
		return 0, fmt.Errorf("RPC latest block fell from %d to %d; possible reorganization or stale backend", first, second)
	}
	return min(first, second), nil
}

// firstBlockNotAfter finds a block whose timestamp is not later than limit.
func (scanner *OrderFilledScanner) firstBlockNotAfter(
	ctx context.Context, toBlock uint64, toTime, limit time.Time,
) (uint64, time.Time, error) {
	candidate := toBlock
	candidateTime := toTime
	for step := 0; step <= orderFilledScanMaxStepBacks; step++ {
		if !candidateTime.After(limit) {
			return candidate, candidateTime, nil
		}
		if step == orderFilledScanMaxStepBacks {
			break
		}
		back := uint64(candidateTime.Sub(limit).Seconds()/orderFilledScanBlockSeconds) + 1
		if back >= candidate {
			return 0, time.Time{}, fmt.Errorf("OrderFilled scan start precedes the genesis block")
		}
		candidate -= back
		_, headerTime, err := rpcBlockHeader(ctx, scanner.rpc, candidate)
		if err != nil {
			return 0, time.Time{}, err
		}
		candidateTime = headerTime
	}
	return 0, time.Time{}, fmt.Errorf("could not locate a block at or before %s", limit.Format(time.RFC3339))
}

// control proves the backend serves OrderFilled logs for one sampled block.
// An empty answer fails the scan: the two V2 exchanges emit fills in almost
// every block, so an empty sample is more likely a backend that has not
// indexed the range than a quiet block. Substituting a neighbouring block
// would forgive exactly that anomaly; the next evaluation samples again.
func (scanner *OrderFilledScanner) control(ctx context.Context, block uint64) (domain.NoFillChainControl, error) {
	logs, err := rpcGetLogs(ctx, scanner.rpc, scanner.exchanges, block, block, []any{orderFilledV2Topic})
	if err != nil {
		return domain.NoFillChainControl{}, fmt.Errorf("read OrderFilled control block %d: %w", block, err)
	}
	for _, entry := range logs {
		if err := scanner.validateControlLog(entry, block); err != nil {
			return domain.NoFillChainControl{}, fmt.Errorf("OrderFilled control block %d: %w", block, err)
		}
	}
	if len(logs) == 0 {
		return domain.NoFillChainControl{}, fmt.Errorf("RPC returned no OrderFilled log for control block %d; negative evidence is not trusted", block)
	}
	return domain.NoFillChainControl{BlockNumber: block, Logs: len(logs)}, nil
}

func (scanner *OrderFilledScanner) isExchange(address string) bool {
	for _, exchange := range scanner.exchanges {
		if exchange == address {
			return true
		}
	}
	return false
}

func (scanner *OrderFilledScanner) validateControlLog(entry rpcLog, block uint64) error {
	address, err := normalizedAddress(entry.Address)
	if err != nil || !scanner.isExchange(address) {
		return fmt.Errorf("log address %q is not a V2 exchange", entry.Address)
	}
	if entry.Removed != nil && *entry.Removed {
		return fmt.Errorf("log was removed by a reorganization")
	}
	if len(entry.Topics) != 4 || !equalFixedHex(entry.Topics[0], orderFilledV2Topic, 32) {
		return fmt.Errorf("log is not a V2 OrderFilled with four topics")
	}
	number, err := parseCanonicalQuantity(entry.BlockNumber, "log block number")
	if err != nil || number != block {
		return fmt.Errorf("log block %q is not the queried block %d", entry.BlockNumber, block)
	}
	return nil
}

func (scanner *OrderFilledScanner) decodeMatch(entry rpcLog, orderHash string, fromBlock, toBlock uint64) (domain.NoFillChainMatch, error) {
	address, err := normalizedAddress(entry.Address)
	if err != nil || !scanner.isExchange(address) {
		return domain.NoFillChainMatch{}, fmt.Errorf("log address %q is not a V2 exchange", entry.Address)
	}
	if entry.Removed != nil && *entry.Removed {
		return domain.NoFillChainMatch{}, fmt.Errorf("log was removed by a reorganization")
	}
	if len(entry.Topics) != 4 || !equalFixedHex(entry.Topics[0], orderFilledV2Topic, 32) {
		return domain.NoFillChainMatch{}, fmt.Errorf("log is not a V2 OrderFilled with four topics")
	}
	if !equalFixedHex(entry.Topics[1], orderHash, 32) {
		return domain.NoFillChainMatch{}, fmt.Errorf("log order hash does not match the queried order")
	}
	blockNumber, err := parseCanonicalQuantity(entry.BlockNumber, "log block number")
	if err != nil {
		return domain.NoFillChainMatch{}, err
	}
	if blockNumber < fromBlock || blockNumber > toBlock {
		return domain.NoFillChainMatch{}, fmt.Errorf("log block %d is outside [%d, %d]", blockNumber, fromBlock, toBlock)
	}
	transactionHash, err := normalizedFixedHex(entry.TransactionHash, 32, "log transaction hash")
	if err != nil {
		return domain.NoFillChainMatch{}, err
	}
	logIndex, err := parseCanonicalQuantity(entry.LogIndex, "log index")
	if err != nil {
		return domain.NoFillChainMatch{}, err
	}
	return domain.NoFillChainMatch{
		TransactionHash: transactionHash, LogIndex: logIndex, BlockNumber: blockNumber, Exchange: address,
	}, nil
}

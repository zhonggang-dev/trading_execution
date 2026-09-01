package evmrpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"golang.org/x/crypto/sha3"
)

const (
	PolymarketConditionalTokensAddress = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
	PolymarketCollateralAdapterAddress = "0xada100db00ca00073811820692005400218fce1f"
	PolymarketNegRiskAdapterAddress    = "0xada2005600dec949baf300f4c6120000bdb6eaab"
)

var ErrRedemptionReceiptPending = errors.New("redemption receipt is pending or not finalized")

type RedemptionPermanentError struct{ Reason string }

func (failure *RedemptionPermanentError) Error() string   { return failure.Reason }
func (failure *RedemptionPermanentError) Permanent() bool { return true }

type RedemptionEvidenceParams struct {
	RPCURL                string
	RequiredConfirmations uint64
	HTTPClient            *http.Client
}

// RedemptionEvidenceReader validates the exact adapter event and a stable,
// canonical Polygon receipt. A Data API disappearance is never accepted as
// accounting evidence.
type RedemptionEvidenceReader struct {
	rpcURL                *url.URL
	requiredConfirmations uint64
	httpClient            *http.Client
	mu                    sync.Mutex
	nextID                uint64
}

var _ port.RedemptionReceiptSource = (*RedemptionEvidenceReader)(nil)

func NewRedemptionEvidenceReader(params RedemptionEvidenceParams) (*RedemptionEvidenceReader, error) {
	rpcURL, err := url.Parse(strings.TrimSpace(params.RPCURL))
	if err != nil || rpcURL.Scheme == "" || rpcURL.Host == "" || (rpcURL.Scheme != "http" && rpcURL.Scheme != "https") {
		return nil, fmt.Errorf("Polygon RPC URL is invalid")
	}
	if params.RequiredConfirmations == 0 || params.RequiredConfirmations > 10_000 {
		return nil, fmt.Errorf("redemption confirmations must be between 1 and 10000")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &RedemptionEvidenceReader{
		rpcURL: rpcURL, requiredConfirmations: params.RequiredConfirmations,
		httpClient: params.HTTPClient,
	}, nil
}

func RedemptionAdapterAddress(negRisk bool) string {
	if negRisk {
		return PolymarketNegRiskAdapterAddress
	}
	return PolymarketCollateralAdapterAddress
}

func (reader *RedemptionEvidenceReader) ResolveRedemptionReceipt(
	ctx context.Context,
	transactionHash, walletAddress, conditionID string,
	negRisk bool,
) (domain.RedemptionReceipt, error) {
	txHash, err := normalizedHash(transactionHash, "transaction hash")
	if err != nil {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: err.Error()}
	}
	wallet, err := normalizedAddress(walletAddress)
	if err != nil {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: "wallet address: " + err.Error()}
	}
	condition, err := normalizedHash(conditionID, "condition id")
	if err != nil {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: err.Error()}
	}
	adapter := RedemptionAdapterAddress(negRisk)

	first, err := reader.readAndMatch(ctx, txHash, wallet, condition, adapter)
	if err != nil {
		return domain.RedemptionReceipt{}, err
	}
	firstLatest, err := reader.blockNumber(ctx)
	if err != nil {
		return domain.RedemptionReceipt{}, err
	}
	if firstLatest < first.BlockNumber || firstLatest-first.BlockNumber+1 < reader.requiredConfirmations {
		return domain.RedemptionReceipt{}, fmt.Errorf("%w: have %d confirmations", ErrRedemptionReceiptPending, confirmations(firstLatest, first.BlockNumber))
	}
	if err := reader.requireCanonicalBlock(ctx, first.BlockNumber, first.BlockHash); err != nil {
		return domain.RedemptionReceipt{}, err
	}

	second, err := reader.readAndMatch(ctx, txHash, wallet, condition, adapter)
	if err != nil {
		return domain.RedemptionReceipt{}, err
	}
	secondLatest, err := reader.blockNumber(ctx)
	if err != nil {
		return domain.RedemptionReceipt{}, err
	}
	if first.TransactionHash != second.TransactionHash || first.EventType != second.EventType ||
		first.PayoutBaseUnits != second.PayoutBaseUnits || first.BlockNumber != second.BlockNumber || first.BlockHash != second.BlockHash {
		return domain.RedemptionReceipt{}, fmt.Errorf("%w: receipt changed across reads", ErrRedemptionReceiptPending)
	}
	if secondLatest < second.BlockNumber || secondLatest-second.BlockNumber+1 < reader.requiredConfirmations {
		return domain.RedemptionReceipt{}, fmt.Errorf("%w: confirmation depth changed", ErrRedemptionReceiptPending)
	}
	if err := reader.requireCanonicalBlock(ctx, second.BlockNumber, second.BlockHash); err != nil {
		return domain.RedemptionReceipt{}, err
	}
	second.Confirmations = secondLatest - second.BlockNumber + 1
	return second, nil
}

type redemptionRPCReceipt struct {
	TransactionHash string             `json:"transactionHash"`
	BlockHash       string             `json:"blockHash"`
	BlockNumber     string             `json:"blockNumber"`
	Status          string             `json:"status"`
	Logs            []redemptionRPCLog `json:"logs"`
}

type redemptionRPCLog struct {
	Address         string   `json:"address"`
	Topics          []string `json:"topics"`
	Data            string   `json:"data"`
	TransactionHash string   `json:"transactionHash"`
	BlockHash       string   `json:"blockHash"`
	BlockNumber     string   `json:"blockNumber"`
	Removed         *bool    `json:"removed"`
}

func (reader *RedemptionEvidenceReader) readAndMatch(
	ctx context.Context, txHash, wallet, condition, adapter string,
) (domain.RedemptionReceipt, error) {
	result, err := reader.call(ctx, "eth_getTransactionReceipt", []any{txHash})
	if err != nil {
		return domain.RedemptionReceipt{}, err
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return domain.RedemptionReceipt{}, ErrRedemptionReceiptPending
	}
	var receipt redemptionRPCReceipt
	if json.Unmarshal(result, &receipt) != nil {
		return domain.RedemptionReceipt{}, fmt.Errorf("decode redemption receipt")
	}
	return matchRedemptionReceipt(receipt, txHash, wallet, condition, adapter)
}

func matchRedemptionReceipt(
	receipt redemptionRPCReceipt, txHash, wallet, condition, adapter string,
) (domain.RedemptionReceipt, error) {
	receiptHash, err := normalizedHash(receipt.TransactionHash, "receipt transaction hash")
	if err != nil || receiptHash != txHash {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: "receipt transaction hash mismatch"}
	}
	status, err := quantityUint64(receipt.Status, "receipt status")
	if err != nil || status > 1 {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: "receipt status is invalid"}
	}
	if status == 0 {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: "redemption transaction reverted"}
	}
	blockNumber, err := quantityUint64(receipt.BlockNumber, "receipt block number")
	if err != nil || blockNumber == 0 {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: "receipt block number is invalid"}
	}
	blockHash, err := normalizedHash(receipt.BlockHash, "receipt block hash")
	if err != nil || strings.Trim(blockHash[2:], "0") == "" {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: "receipt block hash is invalid"}
	}
	walletTopic := "0x" + strings.Repeat("0", 24) + wallet[2:]
	eventTopic := eventTopic("PositionsRedeemed(address,bytes32,uint256[],uint256)")
	var matches []domain.RedemptionReceipt
	for _, entry := range receipt.Logs {
		address, addressErr := normalizedAddress(entry.Address)
		if addressErr != nil || address != adapter || len(entry.Topics) != 3 ||
			!strings.EqualFold(entry.Topics[0], eventTopic) ||
			!strings.EqualFold(entry.Topics[1], walletTopic) ||
			!strings.EqualFold(entry.Topics[2], condition) {
			continue
		}
		if entry.Removed == nil || *entry.Removed {
			return domain.RedemptionReceipt{}, fmt.Errorf("%w: matching redemption log was removed or incomplete", ErrRedemptionReceiptPending)
		}
		logTx, logErr := normalizedHash(entry.TransactionHash, "log transaction hash")
		logBlockHash, blockErr := normalizedHash(entry.BlockHash, "log block hash")
		logBlockNumber, numberErr := quantityUint64(entry.BlockNumber, "log block number")
		payout, payoutErr := decodePositionsRedeemedData(entry.Data)
		if logErr != nil || blockErr != nil || numberErr != nil || payoutErr != nil ||
			logTx != txHash || logBlockHash != blockHash || logBlockNumber != blockNumber {
			return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: "matching redemption log identity or data is invalid"}
		}
		matches = append(matches, domain.RedemptionReceipt{
			TransactionHash: txHash, WalletAddress: wallet, ConditionID: condition,
			EventType: "POSITIONS_REDEEMED", PayoutBaseUnits: payout.String(),
			BlockNumber: blockNumber, BlockHash: blockHash,
		})
	}
	if len(matches) != 1 {
		return domain.RedemptionReceipt{}, &RedemptionPermanentError{Reason: fmt.Sprintf("receipt contains %d exact PositionsRedeemed events", len(matches))}
	}
	return matches[0], nil
}

func decodePositionsRedeemedData(value string) (*big.Int, error) {
	raw := strings.TrimPrefix(strings.TrimSpace(value), "0x")
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) < 96 || len(decoded)%32 != 0 {
		return nil, fmt.Errorf("PositionsRedeemed data is invalid")
	}
	offset := new(big.Int).SetBytes(decoded[:32])
	if !offset.IsUint64() || offset.Uint64() != 64 {
		return nil, fmt.Errorf("PositionsRedeemed amounts offset is invalid")
	}
	length := new(big.Int).SetBytes(decoded[64:96])
	if !length.IsUint64() || length.Uint64() != 2 || len(decoded) != 96+2*32 {
		return nil, fmt.Errorf("PositionsRedeemed amounts must contain two outcomes")
	}
	return new(big.Int).SetBytes(decoded[32:64]), nil
}

func eventTopic(signature string) string {
	hash := sha3.NewLegacyKeccak256()
	_, _ = hash.Write([]byte(signature))
	return "0x" + hex.EncodeToString(hash.Sum(nil))
}

func (reader *RedemptionEvidenceReader) blockNumber(ctx context.Context) (uint64, error) {
	result, err := reader.call(ctx, "eth_blockNumber", nil)
	if err != nil {
		return 0, err
	}
	var value string
	if json.Unmarshal(result, &value) != nil {
		return 0, fmt.Errorf("latest block response is invalid")
	}
	return quantityUint64(value, "latest block number")
}

func (reader *RedemptionEvidenceReader) requireCanonicalBlock(ctx context.Context, number uint64, expectedHash string) error {
	result, err := reader.call(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", number), false})
	if err != nil {
		return err
	}
	var block struct {
		Hash string `json:"hash"`
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) || json.Unmarshal(result, &block) != nil {
		return fmt.Errorf("%w: receipt block is unavailable", ErrRedemptionReceiptPending)
	}
	hash, err := normalizedHash(block.Hash, "canonical block hash")
	if err != nil || hash != expectedHash {
		return fmt.Errorf("%w: receipt block is not canonical", ErrRedemptionReceiptPending)
	}
	return nil
}

func (reader *RedemptionEvidenceReader) call(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	reader.mu.Lock()
	reader.nextID++
	id := reader.nextID
	reader.mu.Unlock()
	payload, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, reader.rpcURL.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "trading-execution/redemption-evidence")
	response, err := reader.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%s transport: %w", method, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", method, err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s HTTP %d", method, response.StatusCode)
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      uint64          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil || envelope.JSONRPC != "2.0" || envelope.ID != id {
		return nil, fmt.Errorf("%s response envelope is invalid", method)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("%s RPC error %d: %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 {
		return nil, fmt.Errorf("%s response has no result", method)
	}
	return envelope.Result, nil
}

func confirmations(latest, receipt uint64) uint64 {
	if latest < receipt {
		return 0
	}
	return latest - receipt + 1
}

func normalizedHash(value, label string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	raw := strings.TrimPrefix(value, "0x")
	if len(raw) != 64 {
		return "", fmt.Errorf("%s must contain 32 bytes", label)
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return "", fmt.Errorf("%s is not hexadecimal", label)
	}
	return "0x" + raw, nil
}

func quantityUint64(value, label string) (uint64, error) {
	if !strings.HasPrefix(value, "0x") || len(value) < 3 || (len(value) > 3 && value[2] == '0') {
		return 0, fmt.Errorf("%s is not a canonical quantity", label)
	}
	parsed, err := strconv.ParseUint(value[2:], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is invalid", label)
	}
	return parsed, nil
}

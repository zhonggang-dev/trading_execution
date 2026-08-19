package evmrpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// PolygonCTFExchangeV2Address is Polymarket's production standard-market V2 exchange.
	PolygonCTFExchangeV2Address = "0xe111180000d2663c0091e4f400237545b87b996b"
	// PolygonNegRiskCTFExchangeV2Address is Polymarket's production neg-risk V2 exchange.
	PolygonNegRiskCTFExchangeV2Address = "0xe2222d279d744050d28e00520010520000310f59"

	orderFilledV2Signature = "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
	orderFilledV2Topic     = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
	polygonChainID         = uint64(137)
	defaultRPCReadAttempts = 3
	maximumRPCReadAttempts = 5
	defaultRPCBodyLimit    = int64(1 << 20)
	maximumRPCBodyLimit    = int64(16 << 20)
	defaultRPCReadTimeout  = 10 * time.Second
)

// OrderSide is the maker order side encoded by the V2 OrderFilled event.
type OrderSide uint8

const (
	OrderSideBuy  OrderSide = 0
	OrderSideSell OrderSide = 1
)

// OrderFilledEvidenceParams configures a read-only V2 OrderFilled evidence reader.
type OrderFilledEvidenceParams struct {
	RPCURL                string
	RequiredConfirmations uint64
	HTTPClient            *http.Client
	RequestTimeout        time.Duration
	MaxResponseBytes      int64
	ReadAttempts          int
}

// OrderFilledEvidenceRequest identifies one exact maker fill in a transaction receipt.
type OrderFilledEvidenceRequest struct {
	TransactionHash string
	OrderHash       string
	Maker           string
	Side            OrderSide
	TokenID         string
}

// OrderFilledEvidence is canonical, confirmed evidence emitted by a Polymarket V2 exchange.
// Integer amounts are returned as base-unit decimal strings so no precision is lost.
type OrderFilledEvidence struct {
	TransactionHash      string
	LogIndex             uint64
	BlockNumber          uint64
	BlockHash            string
	ExchangeAddress      string
	OrderHash            string
	Maker                string
	Taker                string
	Side                 OrderSide
	TokenID              string
	MakerAmountBaseUnits string
	TakerAmountBaseUnits string
	FeeBaseUnits         string
	Builder              string
	Metadata             string
	Confirmations        uint64
}

// OrderFilledEvidenceReader reads receipts only. It never signs or sends transactions.
type OrderFilledEvidenceReader struct {
	rpcURL                *url.URL
	requiredConfirmations uint64
	httpClient            *http.Client
	requestTimeout        time.Duration
	maxResponseBytes      int64
	readAttempts          int

	mu                 sync.Mutex
	nextRequestID      uint64
	highestLatestBlock uint64
	hasLatestBlock     bool
}

// NewOrderFilledEvidenceReader validates configuration for the read-only evidence reader.
func NewOrderFilledEvidenceReader(params OrderFilledEvidenceParams) (*OrderFilledEvidenceReader, error) {
	rpcURL, err := url.Parse(strings.TrimSpace(params.RPCURL))
	if err != nil || rpcURL.Scheme == "" || rpcURL.Host == "" {
		return nil, fmt.Errorf("EVM RPC URL is invalid")
	}
	if rpcURL.Scheme != "http" && rpcURL.Scheme != "https" {
		return nil, fmt.Errorf("EVM RPC URL scheme must be HTTP or HTTPS")
	}
	if rpcURL.User != nil || rpcURL.Fragment != "" {
		return nil, fmt.Errorf("EVM RPC URL must not contain userinfo or a fragment")
	}
	if params.RequiredConfirmations == 0 {
		return nil, fmt.Errorf("required confirmations must be positive")
	}
	if params.RequestTimeout < 0 || params.MaxResponseBytes < 0 || params.ReadAttempts < 0 {
		return nil, fmt.Errorf("RPC read limits must not be negative")
	}
	if params.RequestTimeout == 0 {
		params.RequestTimeout = defaultRPCReadTimeout
	}
	if params.MaxResponseBytes == 0 {
		params.MaxResponseBytes = defaultRPCBodyLimit
	}
	if params.ReadAttempts == 0 {
		params.ReadAttempts = defaultRPCReadAttempts
	}
	if params.MaxResponseBytes > maximumRPCBodyLimit {
		return nil, fmt.Errorf("RPC response limit must not exceed %d bytes", maximumRPCBodyLimit)
	}
	if params.ReadAttempts > maximumRPCReadAttempts {
		return nil, fmt.Errorf("RPC read attempts must not exceed %d", maximumRPCReadAttempts)
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: params.RequestTimeout}
	}
	return &OrderFilledEvidenceReader{
		rpcURL: rpcURL, requiredConfirmations: params.RequiredConfirmations,
		httpClient: params.HTTPClient, requestTimeout: params.RequestTimeout,
		maxResponseBytes: params.MaxResponseBytes, readAttempts: params.ReadAttempts,
	}, nil
}

// Read resolves one exact, successful and sufficiently confirmed OrderFilled event.
func (reader *OrderFilledEvidenceReader) Read(
	ctx context.Context,
	request OrderFilledEvidenceRequest,
) (OrderFilledEvidence, error) {
	expected, err := validateEvidenceRequest(request)
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	chainID, err := reader.getChainID(ctx)
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	if chainID != polygonChainID {
		return OrderFilledEvidence{}, fmt.Errorf(
			"RPC chain ID is %d; require Polygon chain ID %d", chainID, polygonChainID,
		)
	}

	firstReceipt, err := reader.getTransactionReceipt(ctx, expected.transactionHash)
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	firstEvidence, err := matchReceipt(firstReceipt, expected)
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	firstLatest, err := reader.getLatestBlock(ctx)
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	if err := reader.observeLatestBlock(firstLatest); err != nil {
		return OrderFilledEvidence{}, err
	}
	if firstLatest < firstEvidence.BlockNumber {
		return OrderFilledEvidence{}, fmt.Errorf(
			"RPC latest block %d is behind receipt block %d", firstLatest, firstEvidence.BlockNumber,
		)
	}

	// Read both values again. A changed receipt or a falling head is a reorg/load-balanced
	// RPC risk and must never be accepted as fee evidence.
	secondReceipt, err := reader.getTransactionReceipt(ctx, expected.transactionHash)
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	secondEvidence, err := matchReceipt(secondReceipt, expected)
	if err != nil {
		return OrderFilledEvidence{}, fmt.Errorf("receipt was not stable across reads: %w", err)
	}
	if !sameEvidence(firstEvidence, secondEvidence) {
		return OrderFilledEvidence{}, fmt.Errorf("receipt changed across reads; possible reorganization")
	}
	secondLatest, err := reader.getLatestBlock(ctx)
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	if secondLatest < firstLatest {
		return OrderFilledEvidence{}, fmt.Errorf(
			"RPC latest block fell from %d to %d; possible reorganization or stale backend",
			firstLatest, secondLatest,
		)
	}
	if err := reader.observeLatestBlock(secondLatest); err != nil {
		return OrderFilledEvidence{}, err
	}
	if secondLatest < secondEvidence.BlockNumber {
		return OrderFilledEvidence{}, fmt.Errorf(
			"RPC latest block %d is behind receipt block %d", secondLatest, secondEvidence.BlockNumber,
		)
	}
	if secondLatest-secondEvidence.BlockNumber == math.MaxUint64 {
		return OrderFilledEvidence{}, fmt.Errorf("confirmation count overflows uint64")
	}
	confirmations := secondLatest - secondEvidence.BlockNumber + 1
	if confirmations < reader.requiredConfirmations {
		return OrderFilledEvidence{}, fmt.Errorf(
			"fill has %d confirmations; require at least %d", confirmations, reader.requiredConfirmations,
		)
	}

	secondEvidence.Confirmations = confirmations
	return secondEvidence, nil
}

func (reader *OrderFilledEvidenceReader) observeLatestBlock(latest uint64) error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.hasLatestBlock && latest < reader.highestLatestBlock {
		return fmt.Errorf(
			"RPC latest block regressed from previously observed %d to %d",
			reader.highestLatestBlock, latest,
		)
	}
	if !reader.hasLatestBlock || latest > reader.highestLatestBlock {
		reader.highestLatestBlock = latest
	}
	reader.hasLatestBlock = true
	return nil
}

type validatedEvidenceRequest struct {
	transactionHash string
	orderHash       string
	maker           string
	side            OrderSide
	tokenID         *big.Int
}

func validateEvidenceRequest(request OrderFilledEvidenceRequest) (validatedEvidenceRequest, error) {
	transactionHash, err := normalizedFixedHex(request.TransactionHash, 32, "transaction hash")
	if err != nil {
		return validatedEvidenceRequest{}, err
	}
	orderHash, err := normalizedFixedHex(request.OrderHash, 32, "order hash")
	if err != nil {
		return validatedEvidenceRequest{}, err
	}
	maker, err := normalizedAddress(request.Maker)
	if err != nil {
		return validatedEvidenceRequest{}, fmt.Errorf("maker: %w", err)
	}
	if request.Side != OrderSideBuy && request.Side != OrderSideSell {
		return validatedEvidenceRequest{}, fmt.Errorf("side must be BUY (0) or SELL (1)")
	}
	tokenID, err := canonicalUint256Decimal(request.TokenID, "token ID")
	if err != nil {
		return validatedEvidenceRequest{}, err
	}
	return validatedEvidenceRequest{
		transactionHash: transactionHash, orderHash: orderHash, maker: maker,
		side: request.Side, tokenID: tokenID,
	}, nil
}

type rpcReceipt struct {
	TransactionHash string   `json:"transactionHash"`
	BlockHash       string   `json:"blockHash"`
	BlockNumber     string   `json:"blockNumber"`
	Status          string   `json:"status"`
	Logs            []rpcLog `json:"logs"`
}

type rpcLog struct {
	Address         string   `json:"address"`
	Topics          []string `json:"topics"`
	Data            string   `json:"data"`
	BlockNumber     string   `json:"blockNumber"`
	TransactionHash string   `json:"transactionHash"`
	BlockHash       string   `json:"blockHash"`
	LogIndex        string   `json:"logIndex"`
	Removed         *bool    `json:"removed"`
}

func (reader *OrderFilledEvidenceReader) getTransactionReceipt(
	ctx context.Context,
	transactionHash string,
) (rpcReceipt, error) {
	result, err := reader.rpcCall(ctx, "eth_getTransactionReceipt", []any{transactionHash})
	if err != nil {
		return rpcReceipt{}, fmt.Errorf("read transaction receipt: %w", err)
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return rpcReceipt{}, fmt.Errorf("transaction receipt is pending or unavailable")
	}
	var receipt rpcReceipt
	if err := json.Unmarshal(result, &receipt); err != nil {
		return rpcReceipt{}, fmt.Errorf("decode transaction receipt: %w", err)
	}
	return receipt, nil
}

func (reader *OrderFilledEvidenceReader) getLatestBlock(ctx context.Context) (uint64, error) {
	result, err := reader.rpcCall(ctx, "eth_blockNumber", []any{})
	if err != nil {
		return 0, fmt.Errorf("read latest block: %w", err)
	}
	var encoded string
	if err := json.Unmarshal(result, &encoded); err != nil {
		return 0, fmt.Errorf("decode latest block: %w", err)
	}
	value, err := parseCanonicalQuantity(encoded, "latest block")
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (reader *OrderFilledEvidenceReader) getChainID(ctx context.Context) (uint64, error) {
	result, err := reader.rpcCall(ctx, "eth_chainId", []any{})
	if err != nil {
		return 0, fmt.Errorf("read chain ID: %w", err)
	}
	var encoded string
	if err := json.Unmarshal(result, &encoded); err != nil {
		return 0, fmt.Errorf("decode chain ID: %w", err)
	}
	value, err := parseCanonicalQuantity(encoded, "chain ID")
	if err != nil {
		return 0, err
	}
	return value, nil
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type rpcEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type retryableRPCError struct{ cause error }

func (err retryableRPCError) Error() string { return err.cause.Error() }
func (err retryableRPCError) Unwrap() error { return err.cause }

func (reader *OrderFilledEvidenceReader) rpcCall(
	ctx context.Context,
	method string,
	params []any,
) (json.RawMessage, error) {
	requestID := reader.allocateRequestID()
	payload, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  []any  `json:"params"`
	}{JSONRPC: "2.0", ID: requestID, Method: method, Params: params})
	if err != nil {
		return nil, fmt.Errorf("encode RPC request: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= reader.readAttempts; attempt++ {
		result, callErr := reader.rpcCallOnce(ctx, requestID, payload)
		if callErr == nil {
			return result, nil
		}
		lastErr = callErr
		var retryable retryableRPCError
		if !errors.As(callErr, &retryable) || ctx.Err() != nil {
			return nil, callErr
		}
	}
	return nil, fmt.Errorf("RPC read failed after %d attempts: %w", reader.readAttempts, lastErr)
}

func (reader *OrderFilledEvidenceReader) rpcCallOnce(
	ctx context.Context,
	requestID uint64,
	payload []byte,
) (json.RawMessage, error) {
	requestContext, cancel := context.WithTimeout(ctx, reader.requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestContext, http.MethodPost, reader.rpcURL.String(), bytes.NewReader(payload),
	)
	if err != nil {
		return nil, fmt.Errorf("create RPC request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := reader.httpClient.Do(request)
	if err != nil {
		if requestContext.Err() != nil {
			return nil, fmt.Errorf("RPC request timed out or was cancelled: %w", requestContext.Err())
		}
		return nil, retryableRPCError{cause: fmt.Errorf("RPC transport: %w", err)}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, reader.maxResponseBytes+1))
	if err != nil {
		return nil, retryableRPCError{cause: fmt.Errorf("read RPC response: %w", err)}
	}
	if int64(len(body)) > reader.maxResponseBytes {
		return nil, fmt.Errorf("RPC response exceeds %d bytes", reader.maxResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		httpErr := fmt.Errorf("RPC HTTP status %d", response.StatusCode)
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests ||
			response.StatusCode >= http.StatusInternalServerError {
			return nil, retryableRPCError{cause: httpErr}
		}
		return nil, httpErr
	}
	var envelope rpcEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode RPC envelope: %w", err)
	}
	if envelope.JSONRPC != "2.0" {
		return nil, fmt.Errorf("RPC response has invalid jsonrpc version")
	}
	if string(bytes.TrimSpace(envelope.ID)) != strconv.FormatUint(requestID, 10) {
		return nil, fmt.Errorf("RPC response ID does not match request")
	}
	if envelope.Error != nil {
		if strings.TrimSpace(envelope.Error.Message) == "" {
			return nil, fmt.Errorf("RPC error %d has no message", envelope.Error.Code)
		}
		return nil, fmt.Errorf("RPC error %d: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 {
		return nil, fmt.Errorf("RPC response has neither result nor error")
	}
	return envelope.Result, nil
}

func (reader *OrderFilledEvidenceReader) allocateRequestID() uint64 {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.nextRequestID++
	return reader.nextRequestID
}

func matchReceipt(receipt rpcReceipt, expected validatedEvidenceRequest) (OrderFilledEvidence, error) {
	receiptTransactionHash, err := normalizedFixedHex(receipt.TransactionHash, 32, "receipt transaction hash")
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	if receiptTransactionHash != expected.transactionHash {
		return OrderFilledEvidence{}, fmt.Errorf("receipt transaction hash does not match requested transaction")
	}
	blockHash, err := normalizedFixedHex(receipt.BlockHash, 32, "receipt block hash")
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	if isZeroFixedHex(blockHash) {
		return OrderFilledEvidence{}, fmt.Errorf("receipt block hash must not be zero")
	}
	blockNumber, err := parseCanonicalQuantity(receipt.BlockNumber, "receipt block number")
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	switch receipt.Status {
	case "0x1":
	case "0x0":
		return OrderFilledEvidence{}, fmt.Errorf("transaction reverted")
	default:
		return OrderFilledEvidence{}, fmt.Errorf("receipt status is not canonical success or revert")
	}

	matches := make([]OrderFilledEvidence, 0, 1)
	for index, logEntry := range receipt.Logs {
		logAddress, logIndex, err := validateLogBasics(
			logEntry, receiptTransactionHash, blockHash, blockNumber, index,
		)
		if err != nil {
			return OrderFilledEvidence{}, err
		}
		if !isExpectedV2Exchange(logAddress) || len(logEntry.Topics) == 0 ||
			!equalFixedHex(logEntry.Topics[0], orderFilledV2Topic, 32) {
			continue
		}
		decoded, err := decodeOrderFilledV2(logEntry, logAddress, logIndex, blockNumber, blockHash, receiptTransactionHash)
		if err != nil {
			return OrderFilledEvidence{}, fmt.Errorf("decode OrderFilled log %d: %w", index, err)
		}
		if decoded.OrderHash != expected.orderHash || decoded.Maker != expected.maker ||
			decoded.Side != expected.side || decoded.TokenID != expected.tokenID.String() {
			continue
		}
		matches = append(matches, decoded)
	}
	if len(matches) == 0 {
		return OrderFilledEvidence{}, fmt.Errorf("receipt contains no exact expected V2 OrderFilled event")
	}
	if len(matches) > 1 {
		return OrderFilledEvidence{}, fmt.Errorf("receipt contains %d exact expected V2 OrderFilled events", len(matches))
	}
	return matches[0], nil
}

func validateLogBasics(
	logEntry rpcLog,
	receiptTransactionHash, receiptBlockHash string,
	receiptBlockNumber uint64,
	position int,
) (string, uint64, error) {
	address, err := normalizedAddress(logEntry.Address)
	if err != nil {
		return "", 0, fmt.Errorf("receipt log %d address: %w", position, err)
	}
	transactionHash, err := normalizedFixedHex(logEntry.TransactionHash, 32, "log transaction hash")
	if err != nil {
		return "", 0, fmt.Errorf("receipt log %d: %w", position, err)
	}
	if transactionHash != receiptTransactionHash {
		return "", 0, fmt.Errorf("receipt log %d transaction hash mismatch", position)
	}
	blockHash, err := normalizedFixedHex(logEntry.BlockHash, 32, "log block hash")
	if err != nil {
		return "", 0, fmt.Errorf("receipt log %d: %w", position, err)
	}
	if blockHash != receiptBlockHash {
		return "", 0, fmt.Errorf("receipt log %d block hash mismatch", position)
	}
	blockNumber, err := parseCanonicalQuantity(logEntry.BlockNumber, "log block number")
	if err != nil {
		return "", 0, fmt.Errorf("receipt log %d: %w", position, err)
	}
	if blockNumber != receiptBlockNumber {
		return "", 0, fmt.Errorf("receipt log %d block number mismatch", position)
	}
	logIndex, err := parseCanonicalQuantity(logEntry.LogIndex, "log index")
	if err != nil {
		return "", 0, fmt.Errorf("receipt log %d: %w", position, err)
	}
	if logEntry.Removed == nil {
		return "", 0, fmt.Errorf("receipt log %d omitted removed flag", position)
	}
	if *logEntry.Removed {
		return "", 0, fmt.Errorf("receipt log %d was removed by a reorganization", position)
	}
	return address, logIndex, nil
}

func decodeOrderFilledV2(
	logEntry rpcLog,
	exchangeAddress string,
	logIndex, blockNumber uint64,
	blockHash, transactionHash string,
) (OrderFilledEvidence, error) {
	if len(logEntry.Topics) != 4 {
		return OrderFilledEvidence{}, fmt.Errorf("event must contain exactly four topics")
	}
	if !equalFixedHex(logEntry.Topics[0], orderFilledV2Topic, 32) {
		return OrderFilledEvidence{}, fmt.Errorf("event signature topic mismatch")
	}
	orderHash, err := normalizedFixedHex(logEntry.Topics[1], 32, "event order hash")
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	maker, err := addressFromTopic(logEntry.Topics[2], "event maker")
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	taker, err := addressFromTopic(logEntry.Topics[3], "event taker")
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	data, err := decodeFixedHex(logEntry.Data, 7*32, "event data")
	if err != nil {
		return OrderFilledEvidence{}, err
	}
	words := make([][]byte, 7)
	for index := range words {
		words[index] = data[index*32 : (index+1)*32]
	}
	for _, value := range words[0][:31] {
		if value != 0 {
			return OrderFilledEvidence{}, fmt.Errorf("side word is not a canonical uint8")
		}
	}
	side := OrderSide(words[0][31])
	if side != OrderSideBuy && side != OrderSideSell {
		return OrderFilledEvidence{}, fmt.Errorf("side is neither BUY nor SELL")
	}
	return OrderFilledEvidence{
		TransactionHash:      transactionHash,
		LogIndex:             logIndex,
		BlockNumber:          blockNumber,
		BlockHash:            blockHash,
		ExchangeAddress:      exchangeAddress,
		OrderHash:            orderHash,
		Maker:                maker,
		Taker:                taker,
		Side:                 side,
		TokenID:              new(big.Int).SetBytes(words[1]).String(),
		MakerAmountBaseUnits: new(big.Int).SetBytes(words[2]).String(),
		TakerAmountBaseUnits: new(big.Int).SetBytes(words[3]).String(),
		FeeBaseUnits:         new(big.Int).SetBytes(words[4]).String(),
		Builder:              "0x" + hex.EncodeToString(words[5]),
		Metadata:             "0x" + hex.EncodeToString(words[6]),
	}, nil
}

func sameEvidence(left, right OrderFilledEvidence) bool {
	left.Confirmations = 0
	right.Confirmations = 0
	return left == right
}

func isExpectedV2Exchange(address string) bool {
	return address == PolygonCTFExchangeV2Address || address == PolygonNegRiskCTFExchangeV2Address
}

func addressFromTopic(value, field string) (string, error) {
	decoded, err := decodeFixedHex(value, 32, field)
	if err != nil {
		return "", err
	}
	for _, prefix := range decoded[:12] {
		if prefix != 0 {
			return "", fmt.Errorf("%s is not a canonical indexed address", field)
		}
	}
	return "0x" + hex.EncodeToString(decoded[12:]), nil
}

func canonicalUint256Decimal(value, field string) (*big.Int, error) {
	value = strings.TrimSpace(value)
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return nil, fmt.Errorf("%s must be a canonical unsigned decimal integer", field)
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return nil, fmt.Errorf("%s must be a canonical unsigned decimal integer", field)
		}
	}
	parsed := new(big.Int)
	if _, ok := parsed.SetString(value, 10); !ok || parsed.Sign() < 0 || parsed.BitLen() > 256 {
		return nil, fmt.Errorf("%s exceeds uint256", field)
	}
	return parsed, nil
}

func normalizedFixedHex(value string, byteLength int, field string) (string, error) {
	decoded, err := decodeFixedHex(value, byteLength, field)
	if err != nil {
		return "", err
	}
	return "0x" + hex.EncodeToString(decoded), nil
}

func decodeFixedHex(value string, byteLength int, field string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "0x") || len(value) != 2+byteLength*2 {
		return nil, fmt.Errorf("%s must be canonical 0x-prefixed %d-byte data", field, byteLength)
	}
	decoded, err := hex.DecodeString(value[2:])
	if err != nil {
		return nil, fmt.Errorf("%s is not hexadecimal", field)
	}
	return decoded, nil
}

func equalFixedHex(value, expected string, byteLength int) bool {
	normalized, err := normalizedFixedHex(value, byteLength, "hex value")
	return err == nil && normalized == expected
}

func parseCanonicalQuantity(value, field string) (uint64, error) {
	if !strings.HasPrefix(value, "0x") || len(value) < 3 || (len(value) > 3 && value[2] == '0') {
		return 0, fmt.Errorf("%s is not a canonical hexadecimal quantity", field)
	}
	for _, character := range value[2:] {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return 0, fmt.Errorf("%s is not a canonical hexadecimal quantity", field)
		}
	}
	parsed, err := strconv.ParseUint(value[2:], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("%s exceeds uint64", field)
	}
	return parsed, nil
}

func isZeroFixedHex(value string) bool {
	for _, character := range strings.TrimPrefix(value, "0x") {
		if character != '0' {
			return false
		}
	}
	return true
}

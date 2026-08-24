package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type rpcClient struct {
	url        *url.URL
	httpClient *http.Client
	timeout    time.Duration

	mu     sync.Mutex
	nextID uint64
}

type rpcCallError struct {
	method    string
	ambiguous bool
	cause     error
}

func (rpcError *rpcCallError) Error() string { return rpcError.cause.Error() }
func (rpcError *rpcCallError) Unwrap() error { return rpcError.cause }

func newRPCClient(rawURL string, httpClient *http.Client, requestTimeout time.Duration) (*rpcClient, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("Polygon RPC URL is invalid")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("Polygon RPC URL must not contain userinfo or a fragment")
	}
	if parsed.Scheme != "https" && !isLoopbackHost(parsed.Hostname()) {
		return nil, fmt.Errorf("non-loopback Polygon RPC URL must use HTTPS")
	}
	if requestTimeout <= 0 {
		requestTimeout = defaultRequestTimeout
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: requestTimeout}
	}
	httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return fmt.Errorf("Polygon RPC redirects are disabled")
	}
	return &rpcClient{url: parsed, httpClient: httpClient, timeout: requestTimeout}, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func (client *rpcClient) Call(ctx context.Context, method string, params []any) ([]byte, error) {
	client.mu.Lock()
	client.nextID++
	requestID := client.nextID
	client.mu.Unlock()
	payload, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  []any  `json:"params"`
	}{JSONRPC: "2.0", ID: requestID, Method: method, Params: params})
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", method, err)
	}
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, client.url.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", method, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		ambiguous := method == "eth_sendRawTransaction"
		return nil, &rpcCallError{method: method, ambiguous: ambiguous, cause: fmt.Errorf("%s transport: %w", method, err)}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumRPCResponseBytes+1))
	if err != nil {
		return nil, &rpcCallError{
			method: method, ambiguous: method == "eth_sendRawTransaction",
			cause: fmt.Errorf("read %s response: %w", method, err),
		}
	}
	if int64(len(body)) > maximumRPCResponseBytes {
		return nil, fmt.Errorf("%s response exceeds %d bytes", method, maximumRPCResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		return nil, &rpcCallError{
			method: method, ambiguous: method == "eth_sendRawTransaction",
			cause: fmt.Errorf("%s HTTP status %d", method, response.StatusCode),
		}
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, &rpcCallError{
			method: method, ambiguous: method == "eth_sendRawTransaction",
			cause: fmt.Errorf("decode %s response: %w", method, err),
		}
	}
	if envelope.JSONRPC != "2.0" || string(bytes.TrimSpace(envelope.ID)) != strconv.FormatUint(requestID, 10) {
		return nil, &rpcCallError{
			method: method, ambiguous: method == "eth_sendRawTransaction",
			cause: fmt.Errorf("%s response envelope identity is invalid", method),
		}
	}
	if envelope.Error != nil {
		if strings.TrimSpace(envelope.Error.Message) == "" {
			return nil, fmt.Errorf("%s RPC error %d has no message", method, envelope.Error.Code)
		}
		return nil, fmt.Errorf("%s RPC error %d: %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 {
		return nil, fmt.Errorf("%s response has neither result nor error", method)
	}
	return append([]byte(nil), envelope.Result...), nil
}

func readChainID(ctx context.Context, rpc rpcCaller) error {
	value, err := rpcQuantityUint64(ctx, rpc, "eth_chainId", nil, "chain ID")
	if err != nil {
		return err
	}
	if value != polygonChainID {
		return fmt.Errorf("RPC chain ID is %d; require Polygon %d", value, polygonChainID)
	}
	return nil
}

func readNonce(ctx context.Context, rpc rpcCaller, owner string) (uint64, error) {
	latest, err := rpcQuantityUint64(ctx, rpc, "eth_getTransactionCount", []any{owner, "latest"}, "latest nonce")
	if err != nil {
		return 0, err
	}
	pending, err := rpcQuantityUint64(ctx, rpc, "eth_getTransactionCount", []any{owner, "pending"}, "pending nonce")
	if err != nil {
		return 0, err
	}
	if latest != pending {
		return 0, fmt.Errorf("account %s has pending nonce %d but latest nonce %d", owner, pending, latest)
	}
	return latest, nil
}

func readBalance(ctx context.Context, rpc rpcCaller, address, blockTag string) (*big.Int, error) {
	return rpcQuantityBig(ctx, rpc, "eth_getBalance", []any{address, blockTag}, "native balance")
}

func requireEOARecipient(ctx context.Context, rpc rpcCaller, recipient string) error {
	result, err := rpc.Call(ctx, "eth_getCode", []any{recipient, "latest"})
	if err != nil {
		return fmt.Errorf("read recipient code: %w", err)
	}
	data, err := parseRPCDataVariable(result, "recipient code")
	if err != nil {
		return err
	}
	if len(data) != 0 {
		return fmt.Errorf("recipient %s has contract code; require EOA", recipient)
	}
	return nil
}

func nativeFundingCall(owner, recipient string) map[string]string {
	return map[string]string{
		"from": owner, "to": recipient, "value": quantityHexBig(fundingAmountWei), "data": "0x",
	}
}

func simulateFunding(ctx context.Context, rpc rpcCaller, owner, recipient string) error {
	call := nativeFundingCall(owner, recipient)
	result, err := rpc.Call(ctx, "eth_call", []any{call, "pending"})
	if err != nil {
		return fmt.Errorf("simulate native funding: %w", err)
	}
	decoded, err := parseRPCDataVariable(result, "funding simulation result")
	if err != nil {
		return err
	}
	if len(decoded) != 0 {
		return fmt.Errorf("native funding simulation returned non-empty data")
	}
	return nil
}

func estimateFundingGas(ctx context.Context, rpc rpcCaller, owner, recipient string) (uint64, error) {
	call := nativeFundingCall(owner, recipient)
	estimate, err := rpcQuantityUint64(ctx, rpc, "eth_estimateGas", []any{call, "pending"}, "funding gas estimate")
	if err != nil {
		return 0, err
	}
	if estimate < minimumFundingGasEstimate || estimate > maximumFundingGasLimit {
		return 0, fmt.Errorf("funding gas estimate %d is outside [%d,%d]", estimate, minimumFundingGasEstimate, maximumFundingGasLimit)
	}
	if estimate > (^uint64(0))/fundingGasNumerator {
		return 0, fmt.Errorf("funding gas margin overflows")
	}
	gasLimit := (estimate*fundingGasNumerator + fundingGasDenominator - 1) / fundingGasDenominator
	if gasLimit > maximumFundingGasLimit {
		return 0, fmt.Errorf("funding gas limit %d exceeds cap %d", gasLimit, maximumFundingGasLimit)
	}
	return gasLimit, nil
}

func readFees(ctx context.Context, rpc rpcCaller) (*big.Int, *big.Int, error) {
	priority, err := rpcQuantityBig(ctx, rpc, "eth_maxPriorityFeePerGas", nil, "max priority fee")
	if err != nil {
		return nil, nil, err
	}
	blockResult, err := rpc.Call(ctx, "eth_getBlockByNumber", []any{"pending", false})
	if err != nil {
		return nil, nil, fmt.Errorf("read pending block fee: %w", err)
	}
	var block struct {
		BaseFeePerGas string `json:"baseFeePerGas"`
	}
	if bytes.Equal(bytes.TrimSpace(blockResult), []byte("null")) || json.Unmarshal(blockResult, &block) != nil {
		return nil, nil, fmt.Errorf("pending block response is invalid")
	}
	baseFee, err := parseQuantityBig(block.BaseFeePerGas, "pending base fee")
	if err != nil {
		return nil, nil, err
	}
	if priority.Sign() <= 0 || priority.Cmp(new(big.Int).SetUint64(maximumPriorityFeeWei)) > 0 {
		return nil, nil, fmt.Errorf("priority fee %s is zero or exceeds cap", priority)
	}
	if baseFee.Sign() <= 0 || baseFee.Cmp(new(big.Int).SetUint64(maximumBaseFeeWei)) > 0 {
		return nil, nil, fmt.Errorf("base fee %s is zero or exceeds cap", baseFee)
	}
	maxFee := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), priority)
	if maxFee.Cmp(new(big.Int).SetUint64(maximumMaxFeePerGasWei)) > 0 {
		return nil, nil, fmt.Errorf("computed max fee %s exceeds cap", maxFee)
	}
	return priority, maxFee, nil
}

func requireNativeFundingBudget(ctx context.Context, rpc rpcCaller, owner string, required *big.Int) error {
	if required == nil || required.Sign() <= 0 || required.BitLen() > 256 {
		return fmt.Errorf("account %s required native funding budget is invalid", owner)
	}
	balance, err := readBalance(ctx, rpc, owner, "pending")
	if err != nil {
		return err
	}
	if balance.Cmp(required) < 0 {
		return fmt.Errorf("account %s native balance %s is below required fixed value plus maximum gas budget %s", owner, balance, required)
	}
	return nil
}

func sendRawTransaction(ctx context.Context, rpc rpcCaller, signed signedFundingTransaction) error {
	result, err := rpc.Call(ctx, "eth_sendRawTransaction", []any{signed.raw})
	if err != nil {
		return err
	}
	var transactionHash string
	if err := json.Unmarshal(result, &transactionHash); err != nil {
		return fmt.Errorf("decode send transaction hash: %w", err)
	}
	normalized, err := normalizeFixedHex(transactionHash, 32, "sent transaction hash")
	if err != nil {
		return err
	}
	if normalized != signed.hash {
		return fmt.Errorf("RPC returned transaction hash %s; locally signed hash is %s", normalized, signed.hash)
	}
	return nil
}

type rpcTransaction struct {
	Hash                 string `json:"hash"`
	ChainID              string `json:"chainId"`
	Type                 string `json:"type"`
	From                 string `json:"from"`
	To                   string `json:"to"`
	Nonce                string `json:"nonce"`
	Gas                  string `json:"gas"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         string `json:"maxFeePerGas"`
	Value                string `json:"value"`
	Input                string `json:"input"`
	BlockHash            string `json:"blockHash"`
	BlockNumber          string `json:"blockNumber"`
}

func readTransaction(ctx context.Context, rpc rpcCaller, hash string) (*rpcTransaction, error) {
	result, err := rpc.Call(ctx, "eth_getTransactionByHash", []any{hash})
	if err != nil {
		return nil, err
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return nil, nil
	}
	var transaction rpcTransaction
	if err := json.Unmarshal(result, &transaction); err != nil {
		return nil, fmt.Errorf("decode transaction: %w", err)
	}
	return &transaction, nil
}

func validateRPCTransaction(actual rpcTransaction, expected journalEntry) error {
	hash, err := normalizeFixedHex(actual.Hash, 32, "transaction hash")
	if err != nil || hash != expected.TransactionHash {
		return fmt.Errorf("RPC transaction hash mismatch")
	}
	chainID, err := parseQuantityUint64(actual.ChainID, "transaction chain ID")
	if err != nil || chainID != polygonChainID {
		return fmt.Errorf("RPC transaction chain ID mismatch")
	}
	typeID, err := parseQuantityUint64(actual.Type, "transaction type")
	if err != nil || typeID != 2 {
		return fmt.Errorf("RPC transaction is not EIP-1559 type 2")
	}
	from, err := normalizeAddress(actual.From)
	if err != nil || from != expected.Source {
		return fmt.Errorf("RPC transaction sender mismatch")
	}
	to, err := normalizeAddress(actual.To)
	if err != nil || to != expected.Recipient {
		return fmt.Errorf("RPC transaction destination mismatch")
	}
	nonce, err := parseQuantityUint64(actual.Nonce, "transaction nonce")
	if err != nil || nonce != expected.Nonce {
		return fmt.Errorf("RPC transaction nonce mismatch")
	}
	gas, err := parseQuantityUint64(actual.Gas, "transaction gas")
	if err != nil || gas != expected.GasLimit {
		return fmt.Errorf("RPC transaction gas mismatch")
	}
	priority, err := parseQuantityBig(actual.MaxPriorityFeePerGas, "transaction priority fee")
	if err != nil || priority.String() != expected.MaxPriorityFeePerGas {
		return fmt.Errorf("RPC transaction priority fee mismatch")
	}
	maxFee, err := parseQuantityBig(actual.MaxFeePerGas, "transaction max fee")
	if err != nil || maxFee.String() != expected.MaxFeePerGas {
		return fmt.Errorf("RPC transaction max fee mismatch")
	}
	value, err := parseQuantityBig(actual.Value, "transaction value")
	if err != nil || value.Cmp(fundingAmountWei) != 0 {
		return fmt.Errorf("RPC transaction value mismatch")
	}
	data, err := normalizeHexData(actual.Input, "transaction input")
	if err != nil || data != "0x" || data != expected.Input {
		return fmt.Errorf("RPC transaction input mismatch")
	}
	return nil
}

type rpcReceipt struct {
	TransactionHash   string   `json:"transactionHash"`
	BlockHash         string   `json:"blockHash"`
	BlockNumber       string   `json:"blockNumber"`
	From              string   `json:"from"`
	To                string   `json:"to"`
	Type              string   `json:"type"`
	Status            string   `json:"status"`
	GasUsed           string   `json:"gasUsed"`
	EffectiveGasPrice string   `json:"effectiveGasPrice"`
	Logs              []rpcLog `json:"logs"`
}

type rpcLog struct {
	Address         string   `json:"address"`
	Topics          []string `json:"topics"`
	Data            string   `json:"data"`
	BlockHash       string   `json:"blockHash"`
	BlockNumber     string   `json:"blockNumber"`
	TransactionHash string   `json:"transactionHash"`
	LogIndex        string   `json:"logIndex"`
	Removed         *bool    `json:"removed"`
}

func readReceipt(ctx context.Context, rpc rpcCaller, hash string) (*rpcReceipt, error) {
	result, err := rpc.Call(ctx, "eth_getTransactionReceipt", []any{hash})
	if err != nil {
		return nil, err
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return nil, nil
	}
	var receipt rpcReceipt
	if err := json.Unmarshal(result, &receipt); err != nil {
		return nil, fmt.Errorf("decode transaction receipt: %w", err)
	}
	return &receipt, nil
}

type validatedReceipt struct {
	blockNumber       uint64
	blockHash         string
	gasUsed           uint64
	effectiveGasPrice string
}

const (
	polygonNativeTokenAddress  = "0x0000000000000000000000000000000000001010"
	polygonLogTransferTopic    = "0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4"
	polygonLogFeeTransferTopic = "0x4dfe1bbbcf077ddc3e01291eea2d5c70c2b422b415d95645b9adcfd678cb1d63"
)

func topicAddress(topic, field string) (string, error) {
	normalized, err := normalizeFixedHex(topic, 32, field)
	if err != nil || normalized[2:26] != strings.Repeat("0", 24) {
		return "", fmt.Errorf("%s is not a canonical address topic", field)
	}
	return normalizeAddress("0x" + normalized[26:])
}

func polygonLogWords(data, field string) ([5]*big.Int, error) {
	var result [5]*big.Int
	normalized, err := normalizeHexData(data, field)
	if err != nil || len(normalized) != 2+5*64 {
		return result, fmt.Errorf("%s must contain exactly five uint256 words", field)
	}
	for index := range result {
		word := normalized[2+index*64 : 2+(index+1)*64]
		value, ok := new(big.Int).SetString(word, 16)
		if !ok {
			return result, fmt.Errorf("%s word %d is invalid", field, index)
		}
		result[index] = value
	}
	return result, nil
}

func validatePolygonFundingLogs(
	logs []rpcLog,
	receiptBlockHash string,
	receiptBlockNumber uint64,
	gasUsed uint64,
	effectiveGasPrice *big.Int,
	expected journalEntry,
) error {
	if len(logs) != 2 {
		return fmt.Errorf("Polygon native funding receipt must contain exactly two system logs")
	}
	logIndexes := [2]uint64{}
	words := [2][5]*big.Int{}
	for index, item := range logs {
		address, err := normalizeAddress(item.Address)
		if err != nil || address != polygonNativeTokenAddress {
			return fmt.Errorf("Polygon native funding log %d has an unexpected address", index)
		}
		if item.Removed == nil || *item.Removed {
			return fmt.Errorf("Polygon native funding log %d is removed or lacks an explicit removed flag", index)
		}
		blockHash, err := normalizeFixedHex(item.BlockHash, 32, "Polygon funding log block hash")
		if err != nil || blockHash != receiptBlockHash {
			return fmt.Errorf("Polygon native funding log %d block hash mismatch", index)
		}
		blockNumber, err := parseQuantityUint64(item.BlockNumber, "Polygon funding log block number")
		if err != nil || blockNumber != receiptBlockNumber {
			return fmt.Errorf("Polygon native funding log %d block number mismatch", index)
		}
		transactionHash, err := normalizeFixedHex(item.TransactionHash, 32, "Polygon funding log transaction hash")
		if err != nil || transactionHash != expected.TransactionHash {
			return fmt.Errorf("Polygon native funding log %d transaction hash mismatch", index)
		}
		logIndex, err := parseQuantityUint64(item.LogIndex, "Polygon funding log index")
		if err != nil {
			return fmt.Errorf("Polygon native funding log %d has an invalid log index", index)
		}
		logIndexes[index] = logIndex
		if len(item.Topics) != 4 {
			return fmt.Errorf("Polygon native funding log %d must contain exactly four topics", index)
		}
		topic0, err := normalizeFixedHex(item.Topics[0], 32, "Polygon funding log signature")
		if err != nil {
			return fmt.Errorf("Polygon native funding log %d has an invalid signature", index)
		}
		wantSignature := polygonLogTransferTopic
		if index == 1 {
			wantSignature = polygonLogFeeTransferTopic
		}
		if topic0 != wantSignature {
			return fmt.Errorf("Polygon native funding log %d has an unexpected signature", index)
		}
		token, err := topicAddress(item.Topics[1], "Polygon funding log token")
		if err != nil || token != polygonNativeTokenAddress {
			return fmt.Errorf("Polygon native funding log %d token mismatch", index)
		}
		source, err := topicAddress(item.Topics[2], "Polygon funding log source")
		if err != nil || source != expected.Source {
			return fmt.Errorf("Polygon native funding log %d source mismatch", index)
		}
		recipient, err := topicAddress(item.Topics[3], "Polygon funding log recipient")
		if err != nil || isZeroHex(recipient) {
			return fmt.Errorf("Polygon native funding log %d recipient is invalid", index)
		}
		if index == 0 && recipient != expected.Recipient {
			return fmt.Errorf("Polygon native value log recipient mismatch")
		}
		if index == 1 && (recipient == expected.Source || recipient == expected.Recipient || recipient == polygonNativeTokenAddress) {
			return fmt.Errorf("Polygon native fee log recipient is not an independent fee recipient")
		}
		words[index], err = polygonLogWords(item.Data, fmt.Sprintf("Polygon funding log %d data", index))
		if err != nil {
			return err
		}
	}
	if logIndexes[0] == ^uint64(0) || logIndexes[1] != logIndexes[0]+1 {
		return fmt.Errorf("Polygon native funding log indexes are not consecutive")
	}
	valueWords := words[0]
	if valueWords[0].Cmp(fundingAmountWei) != 0 ||
		new(big.Int).Sub(valueWords[1], valueWords[3]).Cmp(fundingAmountWei) != 0 ||
		new(big.Int).Sub(valueWords[4], valueWords[2]).Cmp(fundingAmountWei) != 0 {
		return fmt.Errorf("Polygon native value log does not encode the fixed balance transfer")
	}
	feeWords := words[1]
	if feeWords[0].Sign() <= 0 || gasUsed == 0 {
		return fmt.Errorf("Polygon native fee log amount is not positive")
	}
	gasUsedBig := new(big.Int).SetUint64(gasUsed)
	feePerGas := new(big.Int)
	remainder := new(big.Int)
	feePerGas.QuoRem(feeWords[0], gasUsedBig, remainder)
	totalGasCost := new(big.Int).Mul(gasUsedBig, effectiveGasPrice)
	if remainder.Sign() != 0 || feePerGas.Sign() <= 0 || feeWords[0].Cmp(totalGasCost) > 0 {
		return fmt.Errorf("Polygon native fee log amount is inconsistent with the signed EIP-1559 fee")
	}
	return nil
}

func validateFundingReceipt(receipt rpcReceipt, expected journalEntry) (validatedReceipt, error) {
	hash, err := normalizeFixedHex(receipt.TransactionHash, 32, "receipt transaction hash")
	if err != nil || hash != expected.TransactionHash {
		return validatedReceipt{}, fmt.Errorf("receipt transaction hash mismatch")
	}
	blockHash, err := normalizeFixedHex(receipt.BlockHash, 32, "receipt block hash")
	if err != nil || isZeroHex(blockHash) {
		return validatedReceipt{}, fmt.Errorf("receipt block hash is invalid")
	}
	blockNumber, err := parseQuantityUint64(receipt.BlockNumber, "receipt block number")
	if err != nil || blockNumber == 0 {
		return validatedReceipt{}, fmt.Errorf("receipt block number is invalid")
	}
	from, err := normalizeAddress(receipt.From)
	if err != nil || from != expected.Source {
		return validatedReceipt{}, fmt.Errorf("receipt sender mismatch")
	}
	to, err := normalizeAddress(receipt.To)
	if err != nil || to != expected.Recipient {
		return validatedReceipt{}, fmt.Errorf("receipt destination mismatch")
	}
	typeID, err := parseQuantityUint64(receipt.Type, "receipt type")
	if err != nil || typeID != 2 {
		return validatedReceipt{}, fmt.Errorf("receipt is not EIP-1559 type 2")
	}
	status, err := parseQuantityUint64(receipt.Status, "receipt status")
	if err != nil || status != 1 {
		return validatedReceipt{}, fmt.Errorf("native funding transaction reverted or has invalid status")
	}
	gasUsed, err := parseQuantityUint64(receipt.GasUsed, "receipt gas used")
	if err != nil || gasUsed == 0 || gasUsed > expected.GasLimit {
		return validatedReceipt{}, fmt.Errorf("receipt gas used is zero or exceeds signed gas limit")
	}
	effectiveGasPrice, err := parseQuantityBig(receipt.EffectiveGasPrice, "receipt effective gas price")
	maxFee, maxFeeOK := new(big.Int).SetString(expected.MaxFeePerGas, 10)
	if err != nil || !maxFeeOK || effectiveGasPrice.Sign() <= 0 || effectiveGasPrice.Cmp(maxFee) > 0 {
		return validatedReceipt{}, fmt.Errorf("receipt effective gas price is zero or exceeds signed max fee")
	}
	if err := validatePolygonFundingLogs(
		receipt.Logs, blockHash, blockNumber, gasUsed, effectiveGasPrice, expected,
	); err != nil {
		return validatedReceipt{}, err
	}
	return validatedReceipt{
		blockNumber: blockNumber, blockHash: blockHash, gasUsed: gasUsed,
		effectiveGasPrice: effectiveGasPrice.String(),
	}, nil
}

func readLatestBlock(ctx context.Context, rpc rpcCaller) (uint64, error) {
	return rpcQuantityUint64(ctx, rpc, "eth_blockNumber", nil, "latest block")
}

func readBlockHash(ctx context.Context, rpc rpcCaller, blockNumber uint64) (string, error) {
	result, err := rpc.Call(ctx, "eth_getBlockByNumber", []any{quantityHex(blockNumber), false})
	if err != nil {
		return "", err
	}
	var block struct {
		Number string `json:"number"`
		Hash   string `json:"hash"`
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) || json.Unmarshal(result, &block) != nil {
		return "", fmt.Errorf("confirmed block response is invalid")
	}
	actualNumber, err := parseQuantityUint64(block.Number, "confirmed block number")
	if err != nil || actualNumber != blockNumber {
		return "", fmt.Errorf("confirmed block number mismatch")
	}
	return normalizeFixedHex(block.Hash, 32, "confirmed block hash")
}

func rpcQuantityUint64(ctx context.Context, rpc rpcCaller, method string, params []any, field string) (uint64, error) {
	value, err := rpcQuantityBig(ctx, rpc, method, params, field)
	if err != nil {
		return 0, err
	}
	if !value.IsUint64() {
		return 0, fmt.Errorf("%s exceeds uint64", field)
	}
	return value.Uint64(), nil
}

func rpcQuantityBig(ctx context.Context, rpc rpcCaller, method string, params []any, field string) (*big.Int, error) {
	result, err := rpc.Call(ctx, method, params)
	if err != nil {
		return nil, err
	}
	var encoded string
	if err := json.Unmarshal(result, &encoded); err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	return parseQuantityBig(encoded, field)
}

func parseQuantityUint64(value, field string) (uint64, error) {
	parsed, err := parseQuantityBig(value, field)
	if err != nil {
		return 0, err
	}
	if !parsed.IsUint64() {
		return 0, fmt.Errorf("%s exceeds uint64", field)
	}
	return parsed.Uint64(), nil
}

func parseQuantityBig(value, field string) (*big.Int, error) {
	if !strings.HasPrefix(value, "0x") || len(value) < 3 || (len(value) > 3 && value[2] == '0') {
		return nil, fmt.Errorf("%s is not a canonical hexadecimal quantity", field)
	}
	for _, character := range value[2:] {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return nil, fmt.Errorf("%s is not a canonical hexadecimal quantity", field)
		}
	}
	parsed, ok := new(big.Int).SetString(value[2:], 16)
	if !ok || parsed.Sign() < 0 || parsed.BitLen() > 256 {
		return nil, fmt.Errorf("%s exceeds uint256", field)
	}
	return parsed, nil
}

func parseRPCData(result []byte, exactBytes int, field string) ([]byte, error) {
	var encoded string
	if err := json.Unmarshal(result, &encoded); err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	normalized, err := normalizeFixedHex(encoded, exactBytes, field)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(normalized[2:])
}

func parseRPCDataVariable(result []byte, field string) ([]byte, error) {
	var encoded string
	if err := json.Unmarshal(result, &encoded); err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	normalized, err := normalizeHexData(encoded, field)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(normalized[2:])
}

func normalizeFixedHex(value string, exactBytes int, field string) (string, error) {
	if !strings.HasPrefix(value, "0x") || len(value) != 2+exactBytes*2 {
		return "", fmt.Errorf("%s must be canonical 0x-prefixed %d-byte data", field, exactBytes)
	}
	decoded, err := hex.DecodeString(value[2:])
	if err != nil {
		return "", fmt.Errorf("%s is not hexadecimal", field)
	}
	return "0x" + hex.EncodeToString(decoded), nil
}

func normalizeHexData(value, field string) (string, error) {
	if !strings.HasPrefix(value, "0x") || len(value)%2 != 0 {
		return "", fmt.Errorf("%s is not canonical hex data", field)
	}
	decoded, err := hex.DecodeString(value[2:])
	if err != nil {
		return "", fmt.Errorf("%s is not hexadecimal", field)
	}
	return "0x" + hex.EncodeToString(decoded), nil
}

func addressTopic(address string) (string, error) {
	decoded, err := addressBytes(address)
	if err != nil {
		return "", err
	}
	word := make([]byte, 32)
	copy(word[12:], decoded)
	return "0x" + hex.EncodeToString(word), nil
}

func quantityHex(value uint64) string { return fmt.Sprintf("0x%x", value) }

func quantityHexBig(value *big.Int) string {
	if value == nil || value.Sign() < 0 {
		panic("negative or nil quantity")
	}
	return "0x" + value.Text(16)
}

func isZeroHex(value string) bool {
	for _, character := range strings.TrimPrefix(value, "0x") {
		if character != '0' {
			return false
		}
	}
	return true
}

func mustDecodeHex(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return decoded
}

func isAmbiguousSendError(err error) bool {
	var rpcError *rpcCallError
	return errors.As(err, &rpcError) && rpcError.ambiguous
}

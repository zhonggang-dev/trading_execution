package evmrpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/sha3"
)

const (
	testTransactionHash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testOrderHash       = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testMaker           = "0x1111111111111111111111111111111111111111"
	testTaker           = "0x2222222222222222222222222222222222222222"
	testBlockHash       = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testBuilder         = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	testMetadata        = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	testTokenID         = "123456789012345678901234567890"
)

type scriptedRPCTransport struct {
	mu        sync.Mutex
	responses []scriptedRPCResponse
	calls     int
}

type scriptedRPCResponse struct {
	method string
	result json.RawMessage
	status int
	body   string
	err    error
}

func (transport *scriptedRPCTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.calls++
	if len(transport.responses) == 0 {
		return nil, fmt.Errorf("unexpected RPC call")
	}
	response := transport.responses[0]
	transport.responses = transport.responses[1:]
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	var rpcRequest struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
	}
	if err := json.Unmarshal(body, &rpcRequest); err != nil {
		return nil, err
	}
	if rpcRequest.JSONRPC != "2.0" || rpcRequest.Method != response.method {
		return nil, fmt.Errorf("RPC request = %s", body)
	}
	if response.err != nil {
		return nil, response.err
	}
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	responseBody := response.body
	if responseBody == "" {
		responseBody = fmt.Sprintf(
			`{"jsonrpc":"2.0","id":%s,"result":%s}`,
			bytesAsString(rpcRequest.ID), bytesAsString(response.result),
		)
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(responseBody)),
		Request:    request,
	}, nil
}

func bytesAsString(value []byte) string { return string(value) }

func TestOrderFilledEvidenceReaderReadsBuyAndSellEvidence(t *testing.T) {
	tests := []struct {
		name              string
		side              OrderSide
		exchange          string
		fee               string
		makerAmount       string
		takerAmount       string
		wantConfirmations uint64
	}{
		{
			name: "BUY standard exchange with fee", side: OrderSideBuy,
			exchange: PolygonCTFExchangeV2Address, fee: "7000",
			makerAmount: "2500000", takerAmount: "5000000", wantConfirmations: 6,
		},
		{
			name: "SELL neg-risk exchange with evidenced zero fee", side: OrderSideSell,
			exchange: PolygonNegRiskCTFExchangeV2Address, fee: "0",
			makerAmount: "5000000", takerAmount: "2500000", wantConfirmations: 6,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receipt := validReceipt(test.exchange, test.side, testTokenID, test.makerAmount, test.takerAmount, test.fee)
			reader, transport := readerForSequence(t, 4,
				rpcResult("eth_getTransactionReceipt", receipt),
				rpcResult("eth_blockNumber", json.RawMessage(`"0x69"`)),
				rpcResult("eth_getTransactionReceipt", receipt),
				rpcResult("eth_blockNumber", json.RawMessage(`"0x69"`)),
			)
			evidence, err := reader.Read(context.Background(), validEvidenceRequest(test.side))
			if err != nil {
				t.Fatal(err)
			}
			if transport.calls != 5 {
				t.Fatalf("RPC calls = %d, want 5", transport.calls)
			}
			if evidence.TransactionHash != testTransactionHash || evidence.LogIndex != 3 ||
				evidence.BlockNumber != 100 || evidence.BlockHash != testBlockHash ||
				evidence.ExchangeAddress != test.exchange || evidence.OrderHash != testOrderHash ||
				evidence.Maker != testMaker || evidence.Taker != testTaker || evidence.Side != test.side ||
				evidence.TokenID != testTokenID || evidence.MakerAmountBaseUnits != test.makerAmount ||
				evidence.TakerAmountBaseUnits != test.takerAmount || evidence.FeeBaseUnits != test.fee ||
				evidence.Builder != testBuilder || evidence.Metadata != testMetadata ||
				evidence.Confirmations != test.wantConfirmations {
				t.Fatalf("evidence = %#v", evidence)
			}
		})
	}
}

func TestOrderFilledEvidenceReaderRejectsNonMatchingEvent(t *testing.T) {
	tests := []struct {
		name    string
		receipt json.RawMessage
		request OrderFilledEvidenceRequest
	}{
		{
			name:    "wrong contract",
			receipt: validReceipt("0x3333333333333333333333333333333333333333", OrderSideBuy, testTokenID, "1", "2", "0"),
			request: validEvidenceRequest(OrderSideBuy),
		},
		{
			name:    "wrong order",
			receipt: validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0"),
			request: replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) {
				value.OrderHash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			}),
		},
		{
			name:    "wrong maker",
			receipt: validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0"),
			request: replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) {
				value.Maker = "0x4444444444444444444444444444444444444444"
			}),
		},
		{
			name:    "wrong side",
			receipt: validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0"),
			request: validEvidenceRequest(OrderSideSell),
		},
		{
			name:    "wrong token",
			receipt: validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0"),
			request: replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) {
				value.TokenID = "999"
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, _ := readerForSequence(t, 4, rpcResult("eth_getTransactionReceipt", test.receipt))
			if _, err := reader.Read(context.Background(), test.request); err == nil ||
				!strings.Contains(err.Error(), "no exact expected") {
				t.Fatalf("Read() error = %v", err)
			}
		})
	}
}

func TestOrderFilledEvidenceReaderRejectsDuplicateExactLogs(t *testing.T) {
	receipt := receiptObject(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
	secondLog := cloneMap(receipt["logs"].([]any)[0].(map[string]any))
	secondLog["logIndex"] = "0x4"
	receipt["logs"] = append(receipt["logs"].([]any), secondLog)
	encoded, _ := json.Marshal(receipt)
	reader, _ := readerForSequence(t, 4, rpcResult("eth_getTransactionReceipt", encoded))
	if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
		!strings.Contains(err.Error(), "2 exact expected") {
		t.Fatalf("Read() error = %v", err)
	}
}

func TestOrderFilledEvidenceReaderRejectsRevertedAndPendingTransactions(t *testing.T) {
	reverted := receiptObject(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
	reverted["status"] = "0x0"
	revertedJSON, _ := json.Marshal(reverted)
	tests := []struct {
		name      string
		result    json.RawMessage
		wantError string
	}{
		{name: "reverted", result: revertedJSON, wantError: "transaction reverted"},
		{name: "pending", result: json.RawMessage(`null`), wantError: "pending or unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, _ := readerForSequence(t, 4, rpcResult("eth_getTransactionReceipt", test.result))
			if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
				!strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Read() error = %v", err)
			}
		})
	}
}

func TestOrderFilledEvidenceReaderRequiresConfirmationsAndStableChainHead(t *testing.T) {
	receipt := validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
	tests := []struct {
		name          string
		confirmations uint64
		firstLatest   string
		secondLatest  string
		wantError     string
	}{
		{name: "insufficient confirmations", confirmations: 5, firstLatest: "0x66", secondLatest: "0x66", wantError: "has 3 confirmations"},
		{name: "latest below receipt", confirmations: 1, firstLatest: "0x63", secondLatest: "0x63", wantError: "behind receipt"},
		{name: "latest falls between reads", confirmations: 1, firstLatest: "0x65", secondLatest: "0x64", wantError: "latest block fell"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, _ := readerForSequence(t, test.confirmations,
				rpcResult("eth_getTransactionReceipt", receipt),
				rpcResult("eth_blockNumber", quotedResult(test.firstLatest)),
				rpcResult("eth_getTransactionReceipt", receipt),
				rpcResult("eth_blockNumber", quotedResult(test.secondLatest)),
			)
			if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
				!strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Read() error = %v", err)
			}
		})
	}
}

func TestOrderFilledEvidenceReaderRejectsLatestBlockFallbackAcrossReads(t *testing.T) {
	receipt := validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
	reader, _ := readerForSequence(t, 1,
		rpcResult("eth_getTransactionReceipt", receipt),
		rpcResult("eth_blockNumber", json.RawMessage(`"0x69"`)),
		rpcResult("eth_getTransactionReceipt", receipt),
		rpcResult("eth_blockNumber", json.RawMessage(`"0x69"`)),
		rpcResult("eth_chainId", json.RawMessage(`"0x89"`)),
		rpcResult("eth_getTransactionReceipt", receipt),
		rpcResult("eth_blockNumber", json.RawMessage(`"0x68"`)),
	)
	if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
		!strings.Contains(err.Error(), "regressed from previously observed 105 to 104") {
		t.Fatalf("second Read() error = %v", err)
	}
}

func TestOrderFilledEvidenceReaderRejectsChangedReceipt(t *testing.T) {
	first := validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
	second := receiptObject(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
	second["blockHash"] = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	second["logs"].([]any)[0].(map[string]any)["blockHash"] = second["blockHash"]
	secondJSON, _ := json.Marshal(second)
	reader, _ := readerForSequence(t, 1,
		rpcResult("eth_getTransactionReceipt", first),
		rpcResult("eth_blockNumber", json.RawMessage(`"0x69"`)),
		rpcResult("eth_getTransactionReceipt", secondJSON),
	)
	if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
		!strings.Contains(err.Error(), "receipt changed") {
		t.Fatalf("Read() error = %v", err)
	}
}

func TestOrderFilledEvidenceReaderRequiresPolygonChainID(t *testing.T) {
	tests := []struct {
		name      string
		result    json.RawMessage
		wantError string
	}{
		{name: "wrong chain", result: json.RawMessage(`"0x1"`), wantError: "require Polygon chain ID 137"},
		{name: "non-canonical chain ID", result: json.RawMessage(`"0x089"`), wantError: "chain ID is not a canonical"},
		{name: "malformed chain ID", result: json.RawMessage(`"polygon"`), wantError: "chain ID is not a canonical"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, transport := readerForRawSequence(t, 1,
				rpcResult("eth_chainId", test.result),
			)
			if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
				!strings.Contains(err.Error(), test.wantError) || transport.calls != 1 {
				t.Fatalf("Read() error = %v, calls=%d", err, transport.calls)
			}
		})
	}
}

func TestOrderFilledEvidenceReaderRejectsMalformedReceipt(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(map[string]any)
		wantError string
	}{
		{
			name: "non-canonical status", wantError: "status is not canonical",
			mutate: func(receipt map[string]any) { receipt["status"] = "0x01" },
		},
		{
			name: "non-canonical block quantity", wantError: "canonical hexadecimal quantity",
			mutate: func(receipt map[string]any) { receipt["blockNumber"] = "0x064" },
		},
		{
			name: "removed log", wantError: "removed by a reorganization",
			mutate: func(receipt map[string]any) { receipt["logs"].([]any)[0].(map[string]any)["removed"] = true },
		},
		{
			name: "maker topic has dirty high bytes", wantError: "canonical indexed address",
			mutate: func(receipt map[string]any) {
				log := receipt["logs"].([]any)[0].(map[string]any)
				topics := log["topics"].([]string)
				topics[2] = "0x01" + topics[2][4:]
			},
		},
		{
			name: "side has dirty high bytes", wantError: "canonical uint8",
			mutate: func(receipt map[string]any) {
				log := receipt["logs"].([]any)[0].(map[string]any)
				log["data"] = "0x01" + log["data"].(string)[4:]
			},
		},
		{
			name: "short event data", wantError: "224-byte data",
			mutate: func(receipt map[string]any) { receipt["logs"].([]any)[0].(map[string]any)["data"] = "0x00" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receipt := receiptObject(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
			test.mutate(receipt)
			encoded, _ := json.Marshal(receipt)
			reader, _ := readerForSequence(t, 1, rpcResult("eth_getTransactionReceipt", encoded))
			if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
				!strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Read() error = %v", err)
			}
		})
	}
}

func TestOrderFilledEvidenceReaderValidatesRequestAndConfiguration(t *testing.T) {
	if _, err := NewOrderFilledEvidenceReader(OrderFilledEvidenceParams{RPCURL: "https://rpc.example"}); err == nil ||
		!strings.Contains(err.Error(), "confirmations must be positive") {
		t.Fatalf("constructor error = %v", err)
	}
	reader, _ := readerForSequence(t, 1)
	tests := []OrderFilledEvidenceRequest{
		replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) { value.TransactionHash = "0x01" }),
		replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) { value.OrderHash = "not-hex" }),
		replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) { value.Maker = "0x01" }),
		replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) { value.Side = 2 }),
		replaceRequest(validEvidenceRequest(OrderSideBuy), func(value *OrderFilledEvidenceRequest) { value.TokenID = "01" }),
	}
	for _, request := range tests {
		if _, err := reader.Read(context.Background(), request); err == nil {
			t.Fatalf("Read(%#v) unexpectedly succeeded", request)
		}
	}
}

func TestOrderFilledEvidenceReaderHonorsStrictRPCErrorAndResponseLimit(t *testing.T) {
	t.Run("JSON-RPC error is not treated as a result", func(t *testing.T) {
		reader, transport := readerForSequence(t, 1, scriptedRPCResponse{
			method: "eth_getTransactionReceipt",
			body:   `{"jsonrpc":"2.0","id":2,"result":null,"error":{"code":-32000,"message":"backend rejected"}}`,
		})
		if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
			!strings.Contains(err.Error(), "RPC error -32000") || transport.calls != 2 {
			t.Fatalf("Read() error = %v, calls=%d", err, transport.calls)
		}
	})
	t.Run("oversized response fails closed", func(t *testing.T) {
		transport := &scriptedRPCTransport{responses: []scriptedRPCResponse{{
			method: "eth_chainId", body: strings.Repeat("x", 65),
		}}}
		reader, err := NewOrderFilledEvidenceReader(OrderFilledEvidenceParams{
			RPCURL: "https://rpc.example", RequiredConfirmations: 1,
			HTTPClient: &http.Client{Transport: transport}, MaxResponseBytes: 64,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err == nil ||
			!strings.Contains(err.Error(), "exceeds 64 bytes") {
			t.Fatalf("Read() error = %v", err)
		}
	})
}

func TestOrderFilledEvidenceReaderRetriesOnlyFiniteTransientReads(t *testing.T) {
	receipt := validReceipt(PolygonCTFExchangeV2Address, OrderSideBuy, testTokenID, "1", "2", "0")
	reader, transport := readerForSequence(t, 1,
		scriptedRPCResponse{method: "eth_getTransactionReceipt", status: http.StatusServiceUnavailable, body: "unavailable"},
		rpcResult("eth_getTransactionReceipt", receipt),
		rpcResult("eth_blockNumber", json.RawMessage(`"0x69"`)),
		rpcResult("eth_getTransactionReceipt", receipt),
		rpcResult("eth_blockNumber", json.RawMessage(`"0x69"`)),
	)
	if _, err := reader.Read(context.Background(), validEvidenceRequest(OrderSideBuy)); err != nil {
		t.Fatal(err)
	}
	if transport.calls != 6 {
		t.Fatalf("calls = %d, want 6", transport.calls)
	}
}

func readerForSequence(
	t *testing.T,
	confirmations uint64,
	responses ...scriptedRPCResponse,
) (*OrderFilledEvidenceReader, *scriptedRPCTransport) {
	t.Helper()
	responses = append([]scriptedRPCResponse{
		rpcResult("eth_chainId", json.RawMessage(`"0x89"`)),
	}, responses...)
	return readerForRawSequence(t, confirmations, responses...)
}

func readerForRawSequence(
	t *testing.T,
	confirmations uint64,
	responses ...scriptedRPCResponse,
) (*OrderFilledEvidenceReader, *scriptedRPCTransport) {
	t.Helper()
	transport := &scriptedRPCTransport{responses: responses}
	reader, err := NewOrderFilledEvidenceReader(OrderFilledEvidenceParams{
		RPCURL: "https://rpc.example", RequiredConfirmations: confirmations,
		HTTPClient: &http.Client{Transport: transport}, RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader, transport
}

func rpcResult(method string, result json.RawMessage) scriptedRPCResponse {
	return scriptedRPCResponse{method: method, result: result}
}

func quotedResult(value string) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

func validEvidenceRequest(side OrderSide) OrderFilledEvidenceRequest {
	return OrderFilledEvidenceRequest{
		TransactionHash: testTransactionHash,
		OrderHash:       testOrderHash,
		Maker:           testMaker,
		Side:            side,
		TokenID:         testTokenID,
	}
}

func replaceRequest(
	request OrderFilledEvidenceRequest,
	update func(*OrderFilledEvidenceRequest),
) OrderFilledEvidenceRequest {
	update(&request)
	return request
}

func validReceipt(
	exchange string,
	side OrderSide,
	tokenID, makerAmount, takerAmount, fee string,
) json.RawMessage {
	encoded, _ := json.Marshal(receiptObject(exchange, side, tokenID, makerAmount, takerAmount, fee))
	return encoded
}

func receiptObject(
	exchange string,
	side OrderSide,
	tokenID, makerAmount, takerAmount, fee string,
) map[string]any {
	return map[string]any{
		"transactionHash": testTransactionHash,
		"blockHash":       testBlockHash,
		"blockNumber":     "0x64",
		"status":          "0x1",
		"logs": []any{map[string]any{
			"address":         exchange,
			"topics":          []string{orderFilledV2Topic, testOrderHash, addressTopic(testMaker), addressTopic(testTaker)},
			"data":            eventData(side, tokenID, makerAmount, takerAmount, fee, testBuilder, testMetadata),
			"blockNumber":     "0x64",
			"transactionHash": testTransactionHash,
			"blockHash":       testBlockHash,
			"logIndex":        "0x3",
			"removed":         false,
		}},
	}
}

func eventData(
	side OrderSide,
	tokenID, makerAmount, takerAmount, fee, builder, metadata string,
) string {
	return "0x" +
		uint256Word(new(big.Int).SetUint64(uint64(side))) +
		uint256Word(mustBigInt(tokenID)) +
		uint256Word(mustBigInt(makerAmount)) +
		uint256Word(mustBigInt(takerAmount)) +
		uint256Word(mustBigInt(fee)) +
		strings.TrimPrefix(builder, "0x") +
		strings.TrimPrefix(metadata, "0x")
}

func addressTopic(address string) string {
	return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(address, "0x")
}

func uint256Word(value *big.Int) string {
	return fmt.Sprintf("%064x", value)
}

func mustBigInt(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok {
		panic("invalid test integer")
	}
	return parsed
}

func cloneMap(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func TestOrderFilledV2TopicMatchesOfficialSignature(t *testing.T) {
	decoded, err := hex.DecodeString(strings.TrimPrefix(orderFilledV2Topic, "0x"))
	if err != nil || len(decoded) != 32 {
		t.Fatalf("topic = %q, err = %v", orderFilledV2Topic, err)
	}
	hasher := sha3.NewLegacyKeccak256()
	_, _ = hasher.Write([]byte(orderFilledV2Signature))
	computed := "0x" + hex.EncodeToString(hasher.Sum(nil))
	if orderFilledV2Topic != computed {
		t.Fatalf("pinned topic = %s, computed topic = %s", orderFilledV2Topic, computed)
	}
}

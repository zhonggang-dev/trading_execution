package evmrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	transferTestToken  = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
	transferTestWallet = "0x1111111111111111111111111111111111111111"
	transferTestOther  = "0x2222222222222222222222222222222222222222"
)

// getLogsQuery names the four eth_getLogs queries issued per chunk.
type getLogsQuery string

const (
	queryTransferIn  getLogsQuery = "TRANSFER_IN"
	queryTransferOut getLogsQuery = "TRANSFER_OUT"
	queryFillMaker   getLogsQuery = "FILL_MAKER"
	queryFillTaker   getLogsQuery = "FILL_TAKER"
)

var chunkQueryOrder = []getLogsQuery{queryTransferIn, queryTransferOut, queryFillMaker, queryFillTaker}

type getLogsCall struct {
	from, to  uint64
	direction domain.ChainCashDirection
	query     getLogsQuery
}

type fakeTransferRPC struct {
	mu       sync.Mutex
	calls    []getLogsCall
	logs     func(call getLogsCall) []map[string]any
	status   func(attempt int) int
	attempts int
	block    any
}

func (fake *fakeTransferRPC) handler(t *testing.T) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			ID     uint64            `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		fake.mu.Lock()
		fake.attempts++
		attempt := fake.attempts
		fake.mu.Unlock()
		if fake.status != nil {
			if status := fake.status(attempt); status != http.StatusOK {
				writer.WriteHeader(status)
				return
			}
		}
		var result any
		switch body.Method {
		case "eth_getLogs":
			var filter struct {
				Address   json.RawMessage `json:"address"`
				FromBlock string          `json:"fromBlock"`
				ToBlock   string          `json:"toBlock"`
				Topics    []any           `json:"topics"`
			}
			if err := json.Unmarshal(body.Params[0], &filter); err != nil {
				t.Errorf("decode filter: %v", err)
				return
			}
			from, _ := strconv.ParseUint(strings.TrimPrefix(filter.FromBlock, "0x"), 16, 64)
			to, _ := strconv.ParseUint(strings.TrimPrefix(filter.ToBlock, "0x"), 16, 64)
			call := getLogsCall{from: from, to: to}
			walletTopic := topicAddress(transferTestWallet)
			switch {
			case len(filter.Topics) > 0 && filter.Topics[0] == transferTopic:
				var address string
				if err := json.Unmarshal(filter.Address, &address); err != nil || address != transferTestToken || len(filter.Topics) != 3 {
					t.Errorf("unexpected transfer filter %s %#v", filter.Address, filter.Topics)
				}
				call.direction, call.query = domain.ChainCashIn, queryTransferIn
				if filter.Topics[1] != nil {
					call.direction, call.query = domain.ChainCashOut, queryTransferOut
					if filter.Topics[1] != walletTopic || filter.Topics[2] != nil {
						t.Errorf("OUT query must name the sender and leave the recipient open: %#v", filter.Topics)
					}
				} else if filter.Topics[2] != walletTopic {
					t.Errorf("IN query must name the recipient: %#v", filter.Topics)
				}
			case len(filter.Topics) > 0 && filter.Topics[0] == orderFilledV2Topic:
				var addresses []string
				if err := json.Unmarshal(filter.Address, &addresses); err != nil || len(addresses) != 2 ||
					addresses[0] != PolygonCTFExchangeV2Address || addresses[1] != PolygonNegRiskCTFExchangeV2Address {
					t.Errorf("OrderFilled query must target both V2 exchanges: %s", filter.Address)
				}
				switch {
				case len(filter.Topics) == 3 && filter.Topics[1] == nil && filter.Topics[2] == walletTopic:
					call.query = queryFillMaker
				case len(filter.Topics) == 4 && filter.Topics[1] == nil && filter.Topics[2] == nil && filter.Topics[3] == walletTopic:
					call.query = queryFillTaker
				default:
					t.Errorf("unexpected OrderFilled topics %#v", filter.Topics)
				}
			default:
				t.Errorf("unexpected filter topics %#v", filter.Topics)
			}
			fake.mu.Lock()
			fake.calls = append(fake.calls, call)
			fake.mu.Unlock()
			logs := []map[string]any{}
			if fake.logs != nil {
				if returned := fake.logs(call); returned != nil {
					logs = returned
				}
			}
			result = logs
		case "eth_getBlockByNumber":
			result = fake.block
		default:
			t.Errorf("unexpected method %s", body.Method)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"jsonrpc": "2.0", "id": body.ID, "result": result})
	}
}

func topicAddress(address string) string {
	return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(address, "0x")
}

func transferLog(block, logIndex uint64, from, to string, units int64) map[string]any {
	return map[string]any{
		"address":         transferTestToken,
		"topics":          []string{transferTopic, topicAddress(from), topicAddress(to)},
		"data":            fmt.Sprintf("0x%064x", units),
		"blockNumber":     fmt.Sprintf("0x%x", block),
		"transactionHash": fmt.Sprintf("0x%064x", block*1000+logIndex),
		"blockHash":       fmt.Sprintf("0x%064x", block),
		"logIndex":        fmt.Sprintf("0x%x", logIndex),
	}
}

// orderFilledLog is a V2 OrderFilled emitted by exchange in the transaction
// that transferLog(block, txLogIndex, ...) would produce.
func orderFilledLog(exchange string, block, txLogIndex, logIndex uint64, maker, taker string) map[string]any {
	return map[string]any{
		"address":         exchange,
		"topics":          []string{orderFilledV2Topic, "0x" + strings.Repeat("0a", 32), topicAddress(maker), topicAddress(taker)},
		"data":            "0x" + strings.Repeat("0", 64*7),
		"blockNumber":     fmt.Sprintf("0x%x", block),
		"transactionHash": fmt.Sprintf("0x%064x", block*1000+txLogIndex),
		"blockHash":       fmt.Sprintf("0x%064x", block),
		"logIndex":        fmt.Sprintf("0x%x", logIndex),
	}
}

var transferTestExchanges = []string{PolygonCTFExchangeV2Address, strings.ToUpper(PolygonNegRiskCTFExchangeV2Address[:2]) + PolygonNegRiskCTFExchangeV2Address[2:]}

func newTransferTestReader(t *testing.T, fake *fakeTransferRPC, chunk uint64) *ERC20TransferLogReader {
	t.Helper()
	server := httptest.NewServer(fake.handler(t))
	t.Cleanup(server.Close)
	reader, err := NewERC20TransferLogReader(ERC20TransferLogParams{
		RPCURL: server.URL, TokenAddress: transferTestToken, ExchangeAddresses: transferTestExchanges,
		Decimals: 6, ChunkBlocks: chunk, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestERC20TransferLogReaderChunksEveryQueryToAtMostOneHundredBlocks(t *testing.T) {
	for _, test := range []struct {
		name     string
		from, to uint64
		chunks   [][2]uint64
	}{
		{name: "single block", from: 7, to: 7, chunks: [][2]uint64{{7, 7}}},
		{name: "exactly one hundred", from: 1, to: 100, chunks: [][2]uint64{{1, 100}}},
		{name: "one hundred and one", from: 1, to: 101, chunks: [][2]uint64{{1, 100}, {101, 101}}},
		{name: "two and a half chunks", from: 94681378, to: 94681627, chunks: [][2]uint64{
			{94681378, 94681477}, {94681478, 94681577}, {94681578, 94681627},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeTransferRPC{}
			reader := newTransferTestReader(t, fake, 0)
			if _, err := reader.ReadWalletLogs(context.Background(), transferTestWallet, test.from, test.to); err != nil {
				t.Fatal(err)
			}
			perChunk := len(chunkQueryOrder)
			if len(fake.calls) != perChunk*len(test.chunks) {
				t.Fatalf("calls = %#v, want %d chunks with four queries each", fake.calls, len(test.chunks))
			}
			for index, call := range fake.calls {
				if call.to < call.from || call.to-call.from+1 > MaxTransferLogChunkBlocks {
					t.Fatalf("eth_getLogs span [%d, %d] exceeds %d blocks", call.from, call.to, MaxTransferLogChunkBlocks)
				}
				want := test.chunks[index/perChunk]
				if call.from != want[0] || call.to != want[1] {
					t.Fatalf("call %d = [%d, %d], want %v", index, call.from, call.to, want)
				}
				if call.query != chunkQueryOrder[index%perChunk] {
					t.Fatalf("call %d query = %s, want %s", index, call.query, chunkQueryOrder[index%perChunk])
				}
			}
		})
	}
}

func TestERC20TransferLogReaderRejectsChunksAboveOneHundredBlocks(t *testing.T) {
	if _, err := NewERC20TransferLogReader(ERC20TransferLogParams{
		RPCURL: "https://rpc.example", TokenAddress: transferTestToken, ExchangeAddresses: transferTestExchanges,
		Decimals: 6, ChunkBlocks: 101,
	}); err == nil {
		t.Fatal("101-block chunk accepted")
	}
	for name, exchanges := range map[string][]string{"no exchanges": nil, "malformed exchange": {"0x12"}} {
		if _, err := NewERC20TransferLogReader(ERC20TransferLogParams{
			RPCURL: "https://rpc.example", TokenAddress: transferTestToken, ExchangeAddresses: exchanges, Decimals: 6,
		}); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	reader, err := NewERC20TransferLogReader(ERC20TransferLogParams{
		RPCURL: "https://rpc.example", TokenAddress: transferTestToken, ExchangeAddresses: transferTestExchanges, Decimals: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.getLogs(context.Background(), reader.token, 1, 101, nil); err == nil {
		t.Fatal("getLogs accepted a 101-block span")
	}
	if _, err := reader.getLogs(context.Background(), reader.exchanges, 1, 101, nil); err == nil {
		t.Fatal("OrderFilled getLogs accepted a 101-block span")
	}
}

func TestERC20TransferLogReaderDecodesBothDirectionsInChainOrder(t *testing.T) {
	fake := &fakeTransferRPC{logs: func(call getLogsCall) []map[string]any {
		if call.query == queryFillMaker || call.query == queryFillTaker {
			return nil
		}
		if call.direction == domain.ChainCashIn {
			return []map[string]any{
				transferLog(12, 3, transferTestOther, transferTestWallet, 1_500),
				// A self transfer never changes the balance and is dropped.
				transferLog(12, 5, transferTestWallet, transferTestWallet, 9_000_000),
			}
		}
		return []map[string]any{
			transferLog(11, 0, transferTestWallet, transferTestOther, 8_800_000),
			transferLog(12, 5, transferTestWallet, transferTestWallet, 9_000_000),
			// Zero-value transfers never change the balance and are dropped.
			transferLog(12, 7, transferTestWallet, transferTestOther, 0),
		}
	}}
	reader := newTransferTestReader(t, fake, 0)
	logs, err := reader.ReadWalletLogs(context.Background(), strings.ToUpper(transferTestWallet[:2])+transferTestWallet[2:], 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	transfers := logs.Transfers
	if len(transfers) != 2 {
		t.Fatalf("transfers = %#v", transfers)
	}
	out, in := transfers[0], transfers[1]
	if out.Direction != domain.ChainCashOut || out.Counterparty != transferTestOther || out.Amount != "8.8" ||
		out.BlockNumber != 11 || out.LogIndex != 0 {
		t.Fatalf("out = %#v", out)
	}
	if in.Direction != domain.ChainCashIn || in.Counterparty != transferTestOther || in.Amount != "0.0015" ||
		in.BlockNumber != 12 || in.LogIndex != 3 || in.ObservedAt.IsZero() {
		t.Fatalf("in = %#v", in)
	}
}

func TestERC20TransferLogReaderFailsOnInvalidLogs(t *testing.T) {
	removed := true
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "removed", mutate: func(log map[string]any) { log["removed"] = removed }},
		{name: "foreign contract", mutate: func(log map[string]any) { log["address"] = transferTestOther }},
		{name: "two topics", mutate: func(log map[string]any) {
			log["topics"] = []string{transferTopic, topicAddress(transferTestOther)}
		}},
		{name: "not a transfer", mutate: func(log map[string]any) {
			log["topics"] = []string{"0x" + strings.Repeat("ab", 32), topicAddress(transferTestOther), topicAddress(transferTestWallet)}
		}},
		{name: "block outside the queried chunk", mutate: func(log map[string]any) { log["blockNumber"] = "0x65" }},
		{name: "wallet not on the queried side", mutate: func(log map[string]any) {
			log["topics"] = []string{transferTopic, topicAddress(transferTestWallet), topicAddress(transferTestOther)}
		}},
		{name: "malformed amount", mutate: func(log map[string]any) { log["data"] = "0x01" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeTransferRPC{logs: func(call getLogsCall) []map[string]any {
				if call.query != queryTransferIn {
					return nil
				}
				log := transferLog(12, 1, transferTestOther, transferTestWallet, 1_000_000)
				test.mutate(log)
				return []map[string]any{log}
			}}
			reader := newTransferTestReader(t, fake, 0)
			logs, err := reader.ReadWalletLogs(context.Background(), transferTestWallet, 10, 20)
			if err == nil || logs.Transfers != nil || logs.Fills != nil {
				t.Fatalf("invalid log accepted: %#v %v", logs, err)
			}
		})
	}
}

func TestERC20TransferLogReaderRetriesTransientStatusAndFailsWholeRead(t *testing.T) {
	fake := &fakeTransferRPC{status: func(attempt int) int {
		if attempt == 1 {
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	}}
	reader := newTransferTestReader(t, fake, 0)
	if _, err := reader.ReadWalletLogs(context.Background(), transferTestWallet, 1, 10); err != nil {
		t.Fatalf("429 was not retried: %v", err)
	}
	if fake.attempts != 5 {
		t.Fatalf("attempts = %d, want 1 retried + 4 queries", fake.attempts)
	}

	// The second chunk keeps failing: no partial result is returned.
	failing := &fakeTransferRPC{}
	failing.status = func(int) int {
		failing.mu.Lock()
		defer failing.mu.Unlock()
		if len(failing.calls) >= len(chunkQueryOrder) {
			return http.StatusBadGateway
		}
		return http.StatusOK
	}
	failing.logs = func(call getLogsCall) []map[string]any {
		if call.query == queryTransferIn {
			return []map[string]any{transferLog(5, 1, transferTestOther, transferTestWallet, 1)}
		}
		return nil
	}
	reader = newTransferTestReader(t, failing, 0)
	logs, err := reader.ReadWalletLogs(context.Background(), transferTestWallet, 1, 150)
	if err == nil || logs.Transfers != nil {
		t.Fatalf("partial read accepted: %#v %v", logs, err)
	}
}

func TestERC20TransferLogReaderRequireBlock(t *testing.T) {
	fake := &fakeTransferRPC{block: nil}
	reader := newTransferTestReader(t, fake, 0)
	if err := reader.RequireBlock(context.Background(), 200); err == nil || !strings.Contains(err.Error(), "does not serve block 200") {
		t.Fatalf("missing block error = %v", err)
	}
	fake.block = map[string]string{"number": "0xc8", "hash": "0x" + strings.Repeat("ab", 32), "timestamp": "0x66f9e5d5"}
	if err := reader.RequireBlock(context.Background(), 200); err != nil {
		t.Fatal(err)
	}
	hash, at, err := reader.BlockHeader(context.Background(), 200)
	if err != nil || hash != "0x"+strings.Repeat("ab", 32) || at.Unix() != 0x66f9e5d5 {
		t.Fatalf("header = %s %s %v", hash, at, err)
	}
	fake.block = map[string]string{"number": "0xc7", "hash": "0x" + strings.Repeat("ab", 32), "timestamp": "0x1"}
	if err := reader.RequireBlock(context.Background(), 200); err == nil {
		t.Fatal("mismatched block number accepted")
	}
}

func TestERC20TransferLogReaderReturnsWalletOrderFilledEvidence(t *testing.T) {
	// A peer-to-peer V2 settlement: the wallet pays a maker directly, and the
	// exchange emits OrderFilled naming the wallet (as maker in one log and
	// as taker in another). A self-matched fill is returned by both queries
	// and kept once.
	fake := &fakeTransferRPC{logs: func(call getLogsCall) []map[string]any {
		switch call.query {
		case queryTransferOut:
			return []map[string]any{transferLog(12, 1, transferTestWallet, transferTestOther, 4_807_640)}
		case queryFillMaker:
			return []map[string]any{
				orderFilledLog(PolygonCTFExchangeV2Address, 12, 1, 2, transferTestWallet, transferTestOther),
				orderFilledLog(PolygonNegRiskCTFExchangeV2Address, 15, 0, 9, transferTestWallet, transferTestWallet),
			}
		case queryFillTaker:
			return []map[string]any{
				orderFilledLog(PolygonNegRiskCTFExchangeV2Address, 14, 0, 4, transferTestOther, transferTestWallet),
				orderFilledLog(PolygonNegRiskCTFExchangeV2Address, 15, 0, 9, transferTestWallet, transferTestWallet),
			}
		}
		return nil
	}}
	reader := newTransferTestReader(t, fake, 0)
	logs, err := reader.ReadWalletLogs(context.Background(), transferTestWallet, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs.Transfers) != 1 || len(logs.Fills) != 3 {
		t.Fatalf("logs = %#v", logs)
	}
	maker, taker, self := logs.Fills[0], logs.Fills[1], logs.Fills[2]
	if maker.TransactionHash != logs.Transfers[0].TransactionHash || maker.Exchange != PolygonCTFExchangeV2Address ||
		maker.Maker != transferTestWallet || maker.Taker != transferTestOther || maker.BlockHash != logs.Transfers[0].BlockHash {
		t.Fatalf("maker fill = %#v", maker)
	}
	if taker.Exchange != PolygonNegRiskCTFExchangeV2Address || taker.Taker != transferTestWallet || taker.LogIndex != 4 {
		t.Fatalf("taker fill = %#v", taker)
	}
	if self.BlockNumber != 15 || self.Maker != transferTestWallet || self.Taker != transferTestWallet {
		t.Fatalf("self fill = %#v", self)
	}
}

func TestERC20TransferLogReaderFailsOnInvalidOrderFilledLogs(t *testing.T) {
	removed := true
	for _, test := range []struct {
		name   string
		query  getLogsQuery
		mutate func(map[string]any)
	}{
		{name: "removed", query: queryFillMaker, mutate: func(log map[string]any) { log["removed"] = removed }},
		{name: "not a configured exchange", query: queryFillMaker, mutate: func(log map[string]any) { log["address"] = transferTestOther }},
		{name: "three topics", query: queryFillMaker, mutate: func(log map[string]any) {
			log["topics"] = log["topics"].([]string)[:3]
		}},
		{name: "not an OrderFilled", query: queryFillMaker, mutate: func(log map[string]any) {
			topics := append([]string(nil), log["topics"].([]string)...)
			topics[0] = transferTopic
			log["topics"] = topics
		}},
		{name: "block outside the queried chunk", query: queryFillMaker, mutate: func(log map[string]any) { log["blockNumber"] = "0x65" }},
		{name: "wallet is not the maker", query: queryFillMaker, mutate: func(log map[string]any) {
			topics := append([]string(nil), log["topics"].([]string)...)
			topics[2] = topicAddress(transferTestOther)
			log["topics"] = topics
		}},
		{name: "wallet is not the taker", query: queryFillTaker, mutate: func(map[string]any) {}},
		{name: "malformed transaction hash", query: queryFillTaker, mutate: func(log map[string]any) { log["transactionHash"] = "0x12" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeTransferRPC{logs: func(call getLogsCall) []map[string]any {
				if call.query != test.query {
					return nil
				}
				// Maker-shaped log: wallet is the maker, a stranger the taker.
				log := orderFilledLog(PolygonCTFExchangeV2Address, 12, 1, 2, transferTestWallet, transferTestOther)
				test.mutate(log)
				return []map[string]any{log}
			}}
			reader := newTransferTestReader(t, fake, 0)
			logs, err := reader.ReadWalletLogs(context.Background(), transferTestWallet, 10, 20)
			if err == nil || logs.Transfers != nil || logs.Fills != nil {
				t.Fatalf("invalid OrderFilled log accepted: %#v %v", logs, err)
			}
		})
	}
}

func TestERC20TransferLogReaderFailsWholeReadWhenOrderFilledQueryFails(t *testing.T) {
	// Transfers read fine but the taker OrderFilled query keeps failing: the
	// read must fail rather than return transfers without their fill evidence.
	fake := &fakeTransferRPC{}
	fake.status = func(int) int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if len(fake.calls) >= 3 {
			return http.StatusBadGateway
		}
		return http.StatusOK
	}
	fake.logs = func(call getLogsCall) []map[string]any {
		if call.query == queryTransferOut {
			return []map[string]any{transferLog(12, 1, transferTestWallet, transferTestOther, 1_000_000)}
		}
		return nil
	}
	reader := newTransferTestReader(t, fake, 0)
	logs, err := reader.ReadWalletLogs(context.Background(), transferTestWallet, 10, 20)
	if err == nil || !strings.Contains(err.Error(), "OrderFilled") || logs.Transfers != nil {
		t.Fatalf("read without fill evidence accepted: %#v %v", logs, err)
	}
}

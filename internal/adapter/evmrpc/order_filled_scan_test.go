package evmrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const scanTestOrderHash = "0xabababababababababababababababababababababababababababababababab"

var scanTestBase = time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)

// fakeScanRPC simulates a Polygon backend: block n is mined at base + 2n s.
type fakeScanRPC struct {
	mu            sync.Mutex
	heads         []uint64 // successive eth_blockNumber answers; the last repeats
	headCalls     int
	chainID       uint64
	matches       map[uint64]int // block -> logs naming the order
	emptyBlocks   map[uint64]bool
	hashChangeAt  int // header call count after which hashes change (0 = never)
	headerCalls   int
	getLogsErr    func(from, to uint64, topics []any) *rpcError
	getLogsSpans  [][2]uint64
	maxSpan       uint64
	orderQueries  int
	orderSpans    [][2]uint64
	controlBlocks []uint64
	removedMatch  bool
}

func (fake *fakeScanRPC) handler(t *testing.T) http.HandlerFunc {
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
		defer fake.mu.Unlock()
		var result any
		var rpcErr *rpcError
		switch body.Method {
		case "eth_chainId":
			result = fmt.Sprintf("0x%x", fake.chainID)
		case "eth_blockNumber":
			index := fake.headCalls
			if index >= len(fake.heads) {
				index = len(fake.heads) - 1
			}
			fake.headCalls++
			result = fmt.Sprintf("0x%x", fake.heads[index])
		case "eth_getBlockByNumber":
			var encoded string
			_ = json.Unmarshal(body.Params[0], &encoded)
			number, _ := strconv.ParseUint(strings.TrimPrefix(encoded, "0x"), 16, 64)
			fake.headerCalls++
			salt := uint64(0)
			if fake.hashChangeAt > 0 && fake.headerCalls > fake.hashChangeAt {
				salt = 1
			}
			result = map[string]any{
				"number":    fmt.Sprintf("0x%x", number),
				"hash":      fmt.Sprintf("0x%064x", number*10+salt),
				"timestamp": fmt.Sprintf("0x%x", scanTestBase.Add(time.Duration(number)*2*time.Second).Unix()),
			}
		case "eth_getLogs":
			var filter struct {
				Address   []string `json:"address"`
				FromBlock string   `json:"fromBlock"`
				ToBlock   string   `json:"toBlock"`
				Topics    []any    `json:"topics"`
			}
			if err := json.Unmarshal(body.Params[0], &filter); err != nil {
				t.Errorf("decode filter: %v", err)
				return
			}
			if len(filter.Address) != 2 || filter.Address[0] != PolygonCTFExchangeV2Address || filter.Address[1] != PolygonNegRiskCTFExchangeV2Address {
				t.Errorf("OrderFilled scan must target both V2 exchanges: %#v", filter.Address)
			}
			from, _ := strconv.ParseUint(strings.TrimPrefix(filter.FromBlock, "0x"), 16, 64)
			to, _ := strconv.ParseUint(strings.TrimPrefix(filter.ToBlock, "0x"), 16, 64)
			if span := to - from + 1; span > fake.maxSpan {
				fake.maxSpan = span
			}
			fake.getLogsSpans = append(fake.getLogsSpans, [2]uint64{from, to})
			if fake.getLogsErr != nil {
				if injected := fake.getLogsErr(from, to, filter.Topics); injected != nil {
					rpcErr = injected
					break
				}
			}
			logs := []map[string]any{}
			if len(filter.Topics) == 2 {
				fake.orderQueries++
				fake.orderSpans = append(fake.orderSpans, [2]uint64{from, to})
				if filter.Topics[0] != orderFilledV2Topic || filter.Topics[1] != scanTestOrderHash {
					t.Errorf("order query topics = %#v", filter.Topics)
				}
				for block := from; block <= to; block++ {
					for index := 0; index < fake.matches[block]; index++ {
						logs = append(logs, scanTestLog(block, index, scanTestOrderHash, fake.removedMatch))
					}
				}
			} else {
				if len(filter.Topics) != 1 || filter.Topics[0] != orderFilledV2Topic || from != to {
					t.Errorf("control query = %#v [%d, %d]", filter.Topics, from, to)
				}
				fake.controlBlocks = append(fake.controlBlocks, from)
				if !fake.emptyBlocks[from] {
					logs = append(logs, scanTestLog(from, 0, "0x"+strings.Repeat("cd", 32), false))
				}
			}
			result = logs
		default:
			t.Errorf("unexpected method %s", body.Method)
		}
		response := map[string]any{"jsonrpc": "2.0", "id": body.ID}
		if rpcErr != nil {
			response["error"] = rpcErr
		} else {
			response["result"] = result
		}
		_ = json.NewEncoder(writer).Encode(response)
	}
}

func scanTestLog(block uint64, index int, orderHash string, removed bool) map[string]any {
	return map[string]any{
		"address":         PolygonNegRiskCTFExchangeV2Address,
		"topics":          []string{orderFilledV2Topic, orderHash, topicAddress(transferTestWallet), topicAddress(transferTestOther)},
		"data":            "0x",
		"blockNumber":     fmt.Sprintf("0x%x", block),
		"transactionHash": fmt.Sprintf("0x%064x", block*100+uint64(index)),
		"blockHash":       fmt.Sprintf("0x%064x", block*10),
		"logIndex":        fmt.Sprintf("0x%x", index+1),
		"removed":         removed,
	}
}

// blockTime returns the timestamp of block n in the fake chain.
func blockTime(number uint64) time.Time {
	return scanTestBase.Add(time.Duration(number) * 2 * time.Second)
}

func newScanTest(t *testing.T, fake *fakeScanRPC) *OrderFilledScanner {
	t.Helper()
	if fake.chainID == 0 {
		fake.chainID = 137
	}
	server := httptest.NewServer(fake.handler(t))
	t.Cleanup(server.Close)
	scanner, err := NewOrderFilledScanner(OrderFilledScanParams{
		RPCURL: server.URL + "/rpc?dkey=secret", RequiredConfirmations: 64, ReadAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return scanner
}

// scanRequest asks for the window [block fromBlock, block toBlock] by time.
func scanRequest(fromBlock, toBlock uint64, maxChunks int) domain.NoFillChainScanRequest {
	return domain.NoFillChainScanRequest{
		OrderHash: scanTestOrderHash, FromNotAfter: blockTime(fromBlock), ToNotBefore: blockTime(toBlock), MaxChunks: maxChunks,
	}
}

func TestOrderFilledScanProvesAbsenceWithControls(t *testing.T) {
	// head 5064 -> confirmed toBlock 5000; the scan must start at or before block 3800.
	fake := &fakeScanRPC{heads: []uint64{5064}}
	scanner := newScanTest(t, fake)
	evidence, err := scanner.ScanOrderFilled(context.Background(), scanRequest(3800, 4900, 30))
	if err != nil {
		t.Fatalf("ScanOrderFilled() error = %v", err)
	}
	if evidence.ToBlock != 5000 || evidence.FromBlock > 3800 || len(evidence.Matches) != 0 || len(evidence.Controls) != 3 {
		t.Fatalf("evidence = %#v", evidence)
	}
	if evidence.ToBlockHashBefore != evidence.ToBlockHashAfter || evidence.RPCHost != "127.0.0.1" {
		t.Fatalf("evidence identity = %#v", evidence)
	}
	if fake.maxSpan > MaxTransferLogChunkBlocks {
		t.Fatalf("eth_getLogs span %d exceeds %d", fake.maxSpan, MaxTransferLogChunkBlocks)
	}
	wantChunks := int((evidence.ToBlock - evidence.FromBlock + MaxTransferLogChunkBlocks) / MaxTransferLogChunkBlocks)
	if fake.orderQueries != wantChunks || evidence.Chunks != wantChunks {
		t.Fatalf("order queries = %d, chunks = %d, want %d", fake.orderQueries, evidence.Chunks, wantChunks)
	}
}

func TestOrderFilledScanChunkBoundaries(t *testing.T) {
	for _, span := range []uint64{1, 100, 101, 250} {
		t.Run(strconv.FormatUint(span, 10), func(t *testing.T) {
			fake := &fakeScanRPC{heads: []uint64{1064}}
			scanner := newScanTest(t, fake)
			// Ask for a start exactly span-1 blocks before the confirmed head.
			from := 1000 - span + 1
			evidence, err := scanner.ScanOrderFilled(context.Background(), scanRequest(from, 1000, 30))
			if err != nil {
				t.Fatal(err)
			}
			next := evidence.FromBlock
			for _, call := range fake.orderSpans {
				if call[0] != next || call[1]-call[0]+1 > 100 {
					t.Fatalf("order query spans = %v; not contiguous chunks of at most 100", fake.orderSpans)
				}
				next = call[1] + 1
			}
			wantChunks := int((evidence.ToBlock - evidence.FromBlock + 100) / 100)
			if evidence.FromBlock > from || next != evidence.ToBlock+1 || len(fake.orderSpans) != wantChunks {
				t.Fatalf("scan [%d, %d] spans %v, want %d chunks from at most %d", evidence.FromBlock, evidence.ToBlock, fake.orderSpans, wantChunks, from)
			}
		})
	}
}

func TestOrderFilledScanReportsMatchesAsEvidence(t *testing.T) {
	fake := &fakeScanRPC{heads: []uint64{5064}, matches: map[uint64]int{4500: 1}}
	scanner := newScanTest(t, fake)
	evidence, err := scanner.ScanOrderFilled(context.Background(), scanRequest(3800, 4900, 30))
	if err != nil || len(evidence.Matches) != 1 || evidence.Matches[0].BlockNumber != 4500 {
		t.Fatalf("ScanOrderFilled() = %#v, %v; want the matching log", evidence.Matches, err)
	}
}

func TestOrderFilledScanFailsClosed(t *testing.T) {
	cases := []struct {
		name    string
		fake    *fakeScanRPC
		request domain.NoFillChainScanRequest
		want    string
	}{
		{
			name: "unsupported block number", request: scanRequest(3800, 4900, 30),
			fake: &fakeScanRPC{heads: []uint64{5064}, getLogsErr: func(from, _ uint64, _ []any) *rpcError {
				if from > 4000 {
					return &rpcError{Code: -32000, Message: "unsupported block number"}
				}
				return nil
			}},
			want: "unsupported block number",
		},
		{name: "wrong chain", request: scanRequest(3800, 4900, 30), fake: &fakeScanRPC{heads: []uint64{5064}, chainID: 1}, want: "chain ID"},
		{name: "head fell", request: scanRequest(3800, 4900, 30), fake: &fakeScanRPC{heads: []uint64{5064, 5040}}, want: "fell"},
		{name: "hash changed during scan", request: scanRequest(3800, 4900, 30), fake: &fakeScanRPC{heads: []uint64{5064}, hashChangeAt: 2}, want: "hash changed"},
		{name: "head below confirmations after scan", request: scanRequest(3800, 4900, 30), fake: &fakeScanRPC{heads: []uint64{5064, 5064, 5050}}, want: "fell below"},
		{name: "control block empty", request: scanRequest(3800, 4900, 30), fake: &fakeScanRPC{heads: []uint64{5064}, emptyBlocks: map[uint64]bool{5000: true, 4999: true, 4998: true}}, want: "negative evidence is not trusted"},
		{name: "chunk budget", request: scanRequest(3800, 4900, 5), fake: &fakeScanRPC{heads: []uint64{5064}}, want: "budget"},
		{name: "removed log", request: scanRequest(3800, 4900, 30), fake: &fakeScanRPC{heads: []uint64{5064}, matches: map[uint64]int{4500: 1}, removedMatch: true}, want: "removed"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			scanner := newScanTest(t, testCase.fake)
			_, err := scanner.ScanOrderFilled(context.Background(), testCase.request)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("ScanOrderFilled() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

// TestOrderFilledScanEmptyControlFailsClosed: an empty sampled control block
// is not replaced by a neighbour; the negative evidence is refused.
func TestOrderFilledScanEmptyControlFailsClosed(t *testing.T) {
	fake := &fakeScanRPC{heads: []uint64{5064}, emptyBlocks: map[uint64]bool{5000: true}}
	scanner := newScanTest(t, fake)
	_, err := scanner.ScanOrderFilled(context.Background(), scanRequest(3800, 4900, 30))
	if err == nil || !strings.Contains(err.Error(), "control block 5000") {
		t.Fatalf("ScanOrderFilled() error = %v, want the empty control block to fail the scan", err)
	}
}

func TestOrderFilledScanWindowNotReached(t *testing.T) {
	fake := &fakeScanRPC{heads: []uint64{5064}}
	scanner := newScanTest(t, fake)
	_, err := scanner.ScanOrderFilled(context.Background(), scanRequest(3800, 5100, 30))
	if !errors.Is(err, domain.ErrNoFillChainWindowNotReached) {
		t.Fatalf("ScanOrderFilled() error = %v, want window-not-reached", err)
	}
}

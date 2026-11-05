package evmrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func historicalReceipt(t *testing.T, cash, ctf int) json.RawMessage {
	t.Helper()
	object := receiptObject(PolygonCTFExchangeV2Address, OrderSideSell, testTokenID, "3000000", "1800000", "10000")
	logs := object["logs"].([]any)
	log := func(address string, topics []string, data string, index string) map[string]any {
		return map[string]any{"address": address, "topics": topics, "data": data, "transactionHash": testTransactionHash, "blockHash": testBlockHash, "blockNumber": "0x64", "logIndex": index, "removed": false}
	}
	logs = append(logs, log(testTaker, []string{transferTopic, addressTopic(testTaker), addressTopic(testMaker)}, fmt.Sprintf("0x%064x", cash), "0x4"))
	token := mustBigInt(testTokenID)
	logs = append(logs, log(PolymarketConditionalTokensAddress, []string{"0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62", addressTopic(testTaker), addressTopic(testMaker), addressTopic(testTaker)}, "0x"+uint256Word(token)+fmt.Sprintf("%064x", ctf), "0x5"))
	object["logs"] = logs
	raw, _ := json.Marshal(object)
	return raw
}
func TestHistoricalSellOneCanonicalPayloadProvesCashCTFAndTime(t *testing.T) {
	valid := historicalReceipt(t, 1790000, 3000000)
	for _, test := range []struct {
		name      string
		final     json.RawMessage
		blockHash string
		want      bool
	}{
		{"complete", valid, testBlockHash, true},
		{"same fill but final cash changed", historicalReceipt(t, 0, 3000000), testBlockHash, false},
		{"same fill but final CTF changed", historicalReceipt(t, 1790000, 2999999), testBlockHash, false},
		{"noncanonical block", valid, "0x" + strings.Repeat("b", 64), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			block, _ := json.Marshal(map[string]any{"hash": test.blockHash, "number": "0x64", "timestamp": "0x68f63800"})
			r, _ := readerForSequence(t, 128, rpcResult("eth_getTransactionReceipt", valid), rpcResult("eth_blockNumber", quotedResult("0x200")), rpcResult("eth_getTransactionReceipt", valid), rpcResult("eth_blockNumber", quotedResult("0x200")), rpcResult("eth_getTransactionReceipt", valid), rpcResult("eth_getTransactionReceipt", test.final), rpcResult("eth_getBlockByNumber", block))
			got, err := r.ReadHistoricalSell(context.Background(), validEvidenceRequest(OrderSideSell), testTaker)
			if (err == nil) != test.want {
				t.Fatalf("wanted success %v, got %v", test.want, err)
			}
			if test.want && (got.ReceiptSHA256 == "" || got.SoldAt.Unix() != 0x68f63800 || got.Fill.FeeBaseUnits != "10000") {
				t.Fatal(got)
			}
		})
	}
}
func TestHistoricalCTFBatchBounds(t *testing.T) {
	for _, data := range [][]byte{nil, make([]byte, 64), make([]byte, 65)} {
		if _, _, err := historicalBatch(data); err == nil {
			t.Fatal("malformed CTF batch accepted")
		}
	}
	data := make([]byte, 192)
	data[31] = 64
	data[63] = 128
	data[95] = 1
	data[127] = 7
	data[159] = 1
	data[191] = 9
	ids, values, err := historicalBatch(data)
	if err != nil || len(ids) != 1 || ids[0].String() != "7" || values[0].String() != "9" {
		t.Fatal(ids, values, err)
	}
}

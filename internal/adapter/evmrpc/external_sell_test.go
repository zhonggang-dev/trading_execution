package evmrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestExternalSellRequiresExactActualCollateral(t *testing.T) {
	for _, amount := range []int{1790000, 1800000, 0} {
		t.Run(fmt.Sprint(amount), func(t *testing.T) {
			object := receiptObject(PolygonCTFExchangeV2Address, OrderSideSell, testTokenID, "3000000", "1800000", "10000")
			object["logs"] = append(object["logs"].([]any), map[string]any{"address": testTaker, "topics": []string{transferTopic, addressTopic(testTaker), addressTopic(testMaker)}, "data": fmt.Sprintf("0x%064x", amount), "transactionHash": testTransactionHash, "blockNumber": "0x64", "blockHash": testBlockHash, "logIndex": "0x2", "removed": false})
			raw, _ := json.Marshal(object)
			r, _ := readerForSequence(t, 1, rpcResult("eth_getTransactionReceipt", raw), rpcResult("eth_blockNumber", quotedResult("0x100")), rpcResult("eth_getTransactionReceipt", raw), rpcResult("eth_blockNumber", quotedResult("0x100")), rpcResult("eth_getTransactionReceipt", raw))
			_, err := r.ReadExternalSell(context.Background(), validEvidenceRequest(OrderSideSell), testTaker)
			if (err == nil) != (amount == 1790000) {
				t.Fatalf("cash %d: %v", amount, err)
			}
		})
	}
}
func TestKnownTokenBalancePreservesDust(t *testing.T) {
	r, _ := readerForSequence(t, 1, rpcResult("eth_call", quotedResult(fmt.Sprintf("0x%064x", 6664))))
	balance, err := r.GetKnownPositionBalance(context.Background(), testMaker, "123")
	if err != nil || balance.String() != "0.006664" {
		t.Fatalf("balance %s %v", balance, err)
	}
	r, _ = readerForRawSequence(t, 1, rpcResult("eth_call", quotedResult("0x"+strings.Repeat("0", 63))))
	if _, err = r.ReadTokenBalance(context.Background(), testTaker, testMaker, "123", "latest"); err == nil {
		t.Fatal("accepted malformed ABI result")
	}
}

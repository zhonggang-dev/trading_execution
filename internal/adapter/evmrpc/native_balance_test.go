package evmrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestReadOnlyNativeAndConditionPayoutEvidence(t *testing.T) {
	var calls int
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		body, _ := io.ReadAll(request.Body)
		var envelope struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		result := `"0x0"`
		switch envelope.Method {
		case "eth_getBalance":
			result = `"0x8ac7230489e80000"`
		case "eth_getBlockByNumber":
			result = `{"hash":"0x` + strings.Repeat("ab", 32) + `","timestamp":"0x64"}`
		case "eth_call":
			if calls == 3 {
				result = `"0x0000000000000000000000000000000000000000000000000000000000000001"` // payout denominator
			} else {
				result = `"0x0000000000000000000000000000000000000000000000000000000000000000"` // both outcome numerators: zero payout is valid
			}
		default:
			t.Fatalf("unexpected RPC method %q", envelope.Method)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%s}`, envelope.ID, result))), Request: request}, nil
	})}
	reader, err := NewOrderFilledEvidenceReader(OrderFilledEvidenceParams{RPCURL: "https://rpc.example", RequiredConfirmations: 1, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	wallet := "0x1111111111111111111111111111111111111111"
	if balance, err := reader.NativeBalanceAt(context.Background(), wallet, "0x64"); err != nil || !balance.Equal("10") {
		t.Fatalf("NativeBalanceAt = %s, %v", balance, err)
	}
	if hash, _, err := reader.BlockHeader(context.Background(), 100); err != nil || hash != "0x"+strings.Repeat("ab", 32) {
		t.Fatalf("BlockHeader = %s, %v", hash, err)
	}
	payout, err := reader.ReadConditionPayout(context.Background(), "0x"+strings.Repeat("11", 32), "0x64")
	if err != nil || payout.Denominator.Sign() != 1 || payout.Numerators[0].Sign() != 0 || payout.Numerators[1].Sign() != 0 {
		t.Fatalf("ReadConditionPayout = %#v, %v", payout, err)
	}
}

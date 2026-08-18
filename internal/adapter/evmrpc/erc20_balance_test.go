package evmrpc

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// roundTripFunc 表示后端使用的 roundTripFunc 类型。
type roundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip 实现当前测试场景所需的辅助行为。
func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// TestERC20BalanceClientReadsExactBaseUnits 验证 ERC 20 Balance Client Reads Exact Base Units 场景下的行为。
func TestERC20BalanceClientReadsExactBaseUnits(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	wallet := "0x1111111111111111111111111111111111111111"
	token := "0x2222222222222222222222222222222222222222"
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(request.Body)
		if !strings.Contains(string(body), `"method":"eth_call"`) ||
			!strings.Contains(string(body), "0x70a08231"+strings.Repeat("0", 24)+strings.TrimPrefix(wallet, "0x")) ||
			!strings.Contains(string(body), token) {
			t.Fatalf("RPC body = %s", body)
		}
		return &http.Response{
			StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":"0x75bcd15"}`)),
			Request: request,
		}, nil
	})}
	client, err := NewERC20BalanceClient(ERC20BalanceParams{
		RPCURL: "https://rpc.example", TokenAddress: token, Asset: "USDC", Decimals: 6,
		HTTPClient: httpClient, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	balance, err := client.GetExternalBalance(context.Background(), wallet, "USDC")
	if err != nil {
		t.Fatal(err)
	}
	if balance.Amount != "123.456789" || balance.Asset != "USDC" || balance.ObservedAt != now {
		t.Fatalf("balance = %#v", balance)
	}
}

// TestERC20BalanceClientFailsAfterReadRetriesInsteadOfReturningZero 验证 ERC 20 Balance Client Fails After Read Retries Instead Of Returning Zero 场景下的行为。
func TestERC20BalanceClientFailsAfterReadRetriesInsteadOfReturningZero(t *testing.T) {
	calls := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("unavailable")), Request: request,
		}, nil
	})}
	client, err := NewERC20BalanceClient(ERC20BalanceParams{
		RPCURL: "https://rpc.example", TokenAddress: "0x2222222222222222222222222222222222222222",
		Asset: "USDC", Decimals: 6, HTTPClient: httpClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	balance, err := client.GetExternalBalance(context.Background(), "0x1111111111111111111111111111111111111111", "USDC")
	if err == nil || !balance.Amount.IsEmpty() || calls != 3 {
		t.Fatalf("GetExternalBalance() = %#v, %v, calls=%d; want unknown after 3 read retries", balance, err, calls)
	}
}

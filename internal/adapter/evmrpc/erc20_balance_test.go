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
	calls := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		body, _ := io.ReadAll(request.Body)
		if calls == 1 {
			if !strings.Contains(string(body), `"method":"eth_blockNumber"`) {
				t.Fatalf("block RPC body = %s", body)
			}
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`)),
				Request: request,
			}, nil
		}
		if !strings.Contains(string(body), `"method":"eth_call"`) ||
			!strings.Contains(string(body), "0x70a08231"+strings.Repeat("0", 24)+strings.TrimPrefix(wallet, "0x")) ||
			!strings.Contains(string(body), token) || !strings.Contains(string(body), `"0x64"`) {
			t.Fatalf("balance RPC body = %s", body)
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
	if balance.Amount != "123.456789" || balance.Asset != "USDC" || balance.BlockNumber != 100 || balance.ObservedAt != now || calls != 2 {
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

const unknownBlockBody = `{"id":1,"jsonrpc":"2.0","error":{"message":"Unknown block","code":26}}`

// unknownBlockClient answers eth_blockNumber normally and fails the first
// unknownCalls eth_call requests with the load-balancer "Unknown block" error.
func unknownBlockClient(t *testing.T, unknownCalls int, ethCalls *int) *ERC20BalanceClient {
	t.Helper()
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(request.Body)
		if strings.Contains(string(body), `"method":"eth_blockNumber"`) {
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`)),
				Request: request,
			}, nil
		}
		*ethCalls++
		if *ethCalls <= unknownCalls {
			return &http.Response{
				StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader(unknownBlockBody)), Request: request,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":"0x75bcd15"}`)),
			Request: request,
		}, nil
	})}
	client, err := NewERC20BalanceClient(ERC20BalanceParams{
		RPCURL: "https://rpc.example", TokenAddress: "0x2222222222222222222222222222222222222222",
		Asset: "USDC", Decimals: 6, HTTPClient: httpClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestERC20BalanceClientWaitsForLaggingNodeToSeeSnapshotBlock 验证节点落后时等待而不是立刻判定余额源不可用。
func TestERC20BalanceClientWaitsForLaggingNodeToSeeSnapshotBlock(t *testing.T) {
	ethCalls := 0
	client := unknownBlockClient(t, 4, &ethCalls)
	var delays []time.Duration
	client.sleep = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}
	balance, err := client.GetExternalBalance(context.Background(), "0x1111111111111111111111111111111111111111", "USDC")
	if err != nil || balance.Amount != "123.456789" || balance.BlockNumber != 100 || ethCalls != 5 {
		t.Fatalf("GetExternalBalance() = %#v, %v, eth_call=%d; want recovery on the 5th attempt", balance, err, ethCalls)
	}
	if len(delays) != 4 || delays[0] != rpcUnknownBlockRetryDelay || delays[3] != 4*rpcUnknownBlockRetryDelay {
		t.Fatalf("delays = %v; want growing waits between attempts", delays)
	}
}

// TestERC20BalanceClientStillFailsWhenBlockStaysUnknown 验证区块始终不可见时仍返回未知结果而不是零。
func TestERC20BalanceClientStillFailsWhenBlockStaysUnknown(t *testing.T) {
	ethCalls := 0
	client := unknownBlockClient(t, 1000, &ethCalls)
	client.sleep = func(context.Context, time.Duration) error { return nil }
	balance, err := client.GetExternalBalance(context.Background(), "0x1111111111111111111111111111111111111111", "USDC")
	if err == nil || !balance.Amount.IsEmpty() || ethCalls != rpcUnknownBlockAttempts ||
		!strings.Contains(err.Error(), "Unknown block") {
		t.Fatalf("GetExternalBalance() = %#v, %v, eth_call=%d; want failure after %d attempts", balance, err, ethCalls, rpcUnknownBlockAttempts)
	}
}

// TestERC20BalanceClientStopsWaitingWhenContextEnds 验证等待期间上下文结束会立即停止重试。
func TestERC20BalanceClientStopsWaitingWhenContextEnds(t *testing.T) {
	ethCalls := 0
	client := unknownBlockClient(t, 1000, &ethCalls)
	client.sleep = func(ctx context.Context, delay time.Duration) error { return context.Canceled }
	_, err := client.GetExternalBalance(context.Background(), "0x1111111111111111111111111111111111111111", "USDC")
	if err == nil || ethCalls != 1 {
		t.Fatalf("GetExternalBalance() error = %v, eth_call=%d; want one attempt then stop", err, ethCalls)
	}
}

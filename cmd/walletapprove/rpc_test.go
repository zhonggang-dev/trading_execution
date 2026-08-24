package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

type dryRunRPC struct {
	sendCalls    int
	allowanceHex string
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestRPCClientMarksOnlyRawSendTransportAsAmbiguous(t *testing.T) {
	client, err := newRPCClient(
		"http://127.0.0.1:8545",
		&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("connection reset")
		})},
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, sendErr := client.Call(context.Background(), "eth_sendRawTransaction", []any{"0x02"})
	if sendErr == nil || !isAmbiguousSendError(sendErr) {
		t.Fatalf("send error = %v, want ambiguous", sendErr)
	}
	_, readErr := client.Call(context.Background(), "eth_chainId", nil)
	if readErr == nil || isAmbiguousSendError(readErr) {
		t.Fatalf("read error = %v, want non-ambiguous", readErr)
	}
}

func (rpc *dryRunRPC) Call(_ context.Context, method string, params []any) ([]byte, error) {
	switch method {
	case "eth_chainId":
		return []byte(`"0x89"`), nil
	case "eth_getTransactionCount":
		return []byte(`"0x5"`), nil
	case "eth_call":
		call, ok := params[0].(map[string]string)
		if !ok {
			return nil, fmt.Errorf("unexpected eth_call params %#v", params)
		}
		data := strings.TrimPrefix(call["data"], "0x")
		switch {
		case strings.HasPrefix(data, "dd62ed3e"):
			if rpc.allowanceHex != "" {
				return json.Marshal(rpc.allowanceHex)
			}
			return json.Marshal("0x" + strings.Repeat("0", 64))
		case strings.HasPrefix(data, "095ea7b3"):
			return json.Marshal("0x" + strings.Repeat("0", 63) + "1")
		default:
			return nil, fmt.Errorf("unexpected eth_call data %s", data)
		}
	case "eth_estimateGas":
		return []byte(`"0xc350"`), nil // 50,000; planned gas is 60,000.
	case "eth_maxPriorityFeePerGas":
		return []byte(`"0x3b9aca00"`), nil // 1 gwei.
	case "eth_getBlockByNumber":
		return []byte(`{"baseFeePerGas":"0x3b9aca00"}`), nil
	case "eth_getBalance":
		return []byte(`"0xde0b6b3a7640000"`), nil
	case "eth_sendRawTransaction":
		rpc.sendCalls++
		return nil, fmt.Errorf("dry-run attempted broadcast")
	default:
		return nil, fmt.Errorf("unexpected RPC method %s", method)
	}
}

type failSigner struct {
	address string
	calls   int
}

func (signer *failSigner) Address() string { return signer.address }
func (signer *failSigner) SignDigest(context.Context, []byte) ([]byte, error) {
	signer.calls++
	return nil, fmt.Errorf("dry-run attempted signing")
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }
func (fixedClock) Sleep(context.Context, time.Duration) error {
	return fmt.Errorf("unexpected sleep")
}

func TestDryRunCoversOnlyFixedTargetsWithoutSigningOrSending(t *testing.T) {
	rpc := &dryRunRPC{}
	wallet6Signer := &failSigner{address: wallet6ExpectedAddress}
	wallet7Signer := &failSigner{address: wallet7ExpectedAddress}
	accounts := map[string]approvalAccount{
		wallet6ExecutionAccountID: {executionAccountID: wallet6ExecutionAccountID, address: wallet6ExpectedAddress, signer: wallet6Signer},
		wallet7ExecutionAccountID: {executionAccountID: wallet7ExecutionAccountID, address: wallet7ExpectedAddress, signer: wallet7Signer},
	}
	result, err := runApprovals(context.Background(), approvalRunParams{
		rpc: rpc, accounts: accounts, clock: fixedClock{now: time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)},
		execute: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.DryRun || result.ChainID != polygonChainID || result.TokenAddress != polygonPUSDAddress ||
		result.AmountBaseUnits != approvalAmount.String() || result.RequiredConfirmations != requiredConfirmations || len(result.Targets) != 4 {
		t.Fatalf("dry-run result = %#v", result)
	}
	for index, target := range result.Targets {
		fixed := fixedApprovalTargets[index]
		if target.ExecutionAccountID != fixed.executionAccountID || target.Owner != fixed.expectedAddress ||
			target.Spender != fixed.spender || !target.NeedsTransaction || target.GasLimit != 60_000 ||
			target.TransactionHash != "" || target.State != journalStatePending {
			t.Fatalf("target %d = %#v", index, target)
		}
		wantNonce := uint64(5 + index%2)
		if target.Nonce != wantNonce {
			t.Fatalf("target %d nonce = %d, want %d", index, target.Nonce, wantNonce)
		}
	}
	if wallet6Signer.calls != 0 || wallet7Signer.calls != 0 || rpc.sendCalls != 0 {
		t.Fatalf("dry-run signer/send calls = %d/%d/%d", wallet6Signer.calls, wallet7Signer.calls, rpc.sendCalls)
	}
}

func TestDryRunRejectsNonZeroAllowanceThatIsNotExact(t *testing.T) {
	rpc := &dryRunRPC{allowanceHex: "0x" + strings.Repeat("0", 63) + "1"}
	wallet6Signer := &failSigner{address: wallet6ExpectedAddress}
	wallet7Signer := &failSigner{address: wallet7ExpectedAddress}
	_, err := runApprovals(context.Background(), approvalRunParams{
		rpc: rpc,
		accounts: map[string]approvalAccount{
			wallet6ExecutionAccountID: {executionAccountID: wallet6ExecutionAccountID, address: wallet6ExpectedAddress, signer: wallet6Signer},
			wallet7ExecutionAccountID: {executionAccountID: wallet7ExecutionAccountID, address: wallet7ExpectedAddress, signer: wallet7Signer},
		},
		clock:   fixedClock{now: time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)},
		execute: false,
	})
	if err == nil || !strings.Contains(err.Error(), "non-zero and differs from required exact allowance 200000000") {
		t.Fatalf("non-exact allowance error = %v", err)
	}
	if wallet6Signer.calls != 0 || wallet7Signer.calls != 0 || rpc.sendCalls != 0 {
		t.Fatalf("rejected dry-run signer/send calls = %d/%d/%d", wallet6Signer.calls, wallet7Signer.calls, rpc.sendCalls)
	}
}

func TestValidateApprovalReceiptRequiresOneExactLog(t *testing.T) {
	removed := false
	transactionHash := "0x" + strings.Repeat("11", 32)
	blockHash := "0x" + strings.Repeat("22", 32)
	eventTopic := "0x" + hex.EncodeToString(keccak256([]byte("Approval(address,address,uint256)")))
	ownerTopic, _ := addressTopic(wallet6ExpectedAddress)
	spenderTopic, _ := addressTopic(standardExchangeV2Address)
	entry := journalEntry{
		ExecutionAccountID: wallet6ExecutionAccountID,
		Owner:              wallet6ExpectedAddress,
		Spender:            standardExchangeV2Address,
		GasLimit:           60_000,
		MaxFeePerGas:       "100000000000",
		TransactionHash:    transactionHash,
	}
	logEntry := rpcLog{
		Address:         polygonPUSDAddress,
		Topics:          []string{eventTopic, ownerTopic, spenderTopic},
		Data:            fmt.Sprintf("0x%064x", approvalAmount),
		BlockHash:       blockHash,
		BlockNumber:     "0x64",
		TransactionHash: transactionHash,
		LogIndex:        "0x0",
		Removed:         &removed,
	}
	receipt := rpcReceipt{
		TransactionHash:   transactionHash,
		BlockHash:         blockHash,
		BlockNumber:       "0x64",
		From:              wallet6ExpectedAddress,
		To:                polygonPUSDAddress,
		Type:              "0x2",
		Status:            "0x1",
		GasUsed:           "0xc350",
		EffectiveGasPrice: "0x3b9aca00",
		Logs:              []rpcLog{logEntry},
	}
	validated, err := validateApprovalReceipt(receipt, entry)
	if err != nil {
		t.Fatal(err)
	}
	if validated.blockNumber != 100 || validated.blockHash != blockHash || validated.gasUsed != 50_000 {
		t.Fatalf("validated receipt = %#v", validated)
	}
	receipt.Logs = append(receipt.Logs, logEntry)
	if _, err := validateApprovalReceipt(receipt, entry); err == nil || !strings.Contains(err.Error(), "require 1") {
		t.Fatalf("duplicate-log error = %v", err)
	}
}

type finalityRPC struct {
	receipt rpcReceipt
	latest  string
}

func (rpc finalityRPC) Call(_ context.Context, method string, params []any) ([]byte, error) {
	switch method {
	case "eth_getTransactionReceipt":
		return json.Marshal(rpc.receipt)
	case "eth_blockNumber":
		return json.Marshal(rpc.latest)
	case "eth_getBlockByNumber":
		return json.Marshal(map[string]string{"number": rpc.receipt.BlockNumber, "hash": rpc.receipt.BlockHash})
	case "eth_call":
		return json.Marshal(fmt.Sprintf("0x%064x", approvalAmount))
	default:
		return nil, fmt.Errorf("unexpected finality RPC method %s with %#v", method, params)
	}
}

func TestWaitForFinalReceiptRequires64StableConfirmationsAndAllowance(t *testing.T) {
	entry, receipt := approvalReceiptFixture(t)
	params := approvalRunParams{
		rpc:          finalityRPC{receipt: receipt, latest: "0xa3"}, // block 163 => 64 confirmations for block 100.
		clock:        fixedClock{now: time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)},
		pollInterval: time.Millisecond,
	}
	confirmed, err := waitForFinalReceipt(context.Background(), params, entry)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.confirmations != 64 || confirmed.receipt.blockNumber != 100 {
		t.Fatalf("final receipt = %#v", confirmed)
	}
	params.rpc = finalityRPC{receipt: receipt, latest: "0xa2"} // 63 confirmations.
	if _, err := waitForFinalReceipt(context.Background(), params, entry); err == nil || !strings.Contains(err.Error(), "unexpected sleep") {
		t.Fatalf("63-confirmation error = %v", err)
	}
}

func approvalReceiptFixture(t *testing.T) (journalEntry, rpcReceipt) {
	t.Helper()
	removed := false
	transactionHash := "0x" + strings.Repeat("33", 32)
	blockHash := "0x" + strings.Repeat("44", 32)
	eventTopic := "0x" + hex.EncodeToString(keccak256([]byte("Approval(address,address,uint256)")))
	ownerTopic, err := addressTopic(wallet7ExpectedAddress)
	if err != nil {
		t.Fatal(err)
	}
	spenderTopic, err := addressTopic(negRiskExchangeV2Address)
	if err != nil {
		t.Fatal(err)
	}
	entry := journalEntry{
		ExecutionAccountID: wallet7ExecutionAccountID,
		Owner:              wallet7ExpectedAddress,
		Spender:            negRiskExchangeV2Address,
		GasLimit:           60_000,
		MaxFeePerGas:       "100000000000",
		TransactionHash:    transactionHash,
	}
	receipt := rpcReceipt{
		TransactionHash:   transactionHash,
		BlockHash:         blockHash,
		BlockNumber:       "0x64",
		From:              wallet7ExpectedAddress,
		To:                polygonPUSDAddress,
		Type:              "0x2",
		Status:            "0x1",
		GasUsed:           "0xc350",
		EffectiveGasPrice: "0x3b9aca00",
		Logs: []rpcLog{{
			Address:         polygonPUSDAddress,
			Topics:          []string{eventTopic, ownerTopic, spenderTopic},
			Data:            fmt.Sprintf("0x%064x", approvalAmount),
			BlockHash:       blockHash,
			BlockNumber:     "0x64",
			TransactionHash: transactionHash,
			LogIndex:        "0x1",
			Removed:         &removed,
		}},
	}
	return entry, receipt
}

func TestCanonicalQuantityRejectsLeadingZeroAndUppercase(t *testing.T) {
	for _, value := range []string{"0x00", "0xA", "10", "0x"} {
		if _, err := parseQuantityBig(value, "test"); err == nil {
			t.Fatalf("parseQuantityBig(%q) succeeded", value)
		}
	}
	if value, err := parseQuantityBig("0x0", "test"); err != nil || value.Sign() != 0 {
		t.Fatalf("zero quantity = %v / %v", value, err)
	}
}

type queuedRPC struct {
	results map[string][][]byte
}

func (rpc *queuedRPC) Call(_ context.Context, method string, _ []any) ([]byte, error) {
	queue := rpc.results[method]
	if len(queue) == 0 {
		return nil, fmt.Errorf("unexpected RPC call %s", method)
	}
	rpc.results[method] = queue[1:]
	return queue[0], nil
}

func TestNonceSimulationAndFeeInputsFailClosed(t *testing.T) {
	nonces := &queuedRPC{results: map[string][][]byte{
		"eth_getTransactionCount": {[]byte(`"0x7"`), []byte(`"0x8"`)},
	}}
	if _, err := readNonce(context.Background(), nonces, wallet6ExpectedAddress); err == nil || !strings.Contains(err.Error(), "pending nonce") {
		t.Fatalf("nonce mismatch error = %v", err)
	}

	simulation := &queuedRPC{results: map[string][][]byte{
		"eth_call": {[]byte(`"0x0000000000000000000000000000000000000000000000000000000000000000"`)},
	}}
	if err := simulateApproval(context.Background(), simulation, wallet6ExpectedAddress, standardExchangeV2Address); err == nil ||
		!strings.Contains(err.Error(), "did not return true") {
		t.Fatalf("false simulation error = %v", err)
	}

	fees := &queuedRPC{results: map[string][][]byte{
		"eth_maxPriorityFeePerGas": {[]byte(`"0x174876e801"`)}, // 100 gwei + 1.
		"eth_getBlockByNumber":     {[]byte(`{"baseFeePerGas":"0x1"}`)},
	}}
	if _, _, err := readFees(context.Background(), fees); err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("fee cap error = %v", err)
	}
}

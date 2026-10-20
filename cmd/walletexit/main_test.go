package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletcutover"
	walletcutoverexit "github.com/UniPat-AI/trading_execution/internal/service/walletcutoverexit"
	"golang.org/x/crypto/sha3"
)

func TestConditionPayoutBaseUnits(t *testing.T) {
	action := walletcutoverexit.RedemptionAction{Shares: []domain.Decimal{"1", "2"}, OutcomeIndexes: []int{0, 1}}
	payout := evmrpc.ConditionPayout{Denominator: big.NewInt(2), Numerators: [2]*big.Int{big.NewInt(1), big.NewInt(0)}}
	value, err := conditionPayoutBaseUnits(action, payout)
	if err != nil || value != "500000" {
		t.Fatalf("conditionPayoutBaseUnits() = %q, %v; want 500000", value, err)
	}
}

func TestRunReadOnlyCLIPathDoesNotBuildExternalClients(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := walletcutoverexit.Manifest{
		SchemaVersion: walletcutoverexit.SchemaVersion,
		ManifestID:    "cli-read-only",
		Observed: walletcutoverexit.Evidence{
			Block: 100, BlockHash: "0x" + strings.Repeat("a", 64), Observed: now,
		},
		ExpiresAt: now.Add(time.Hour),
		Actions: []walletcutoverexit.AssetAction{{
			ActionID: "asset-1", ExecutionAccountID: "main", WalletAddress: "0x" + strings.Repeat("1", 40),
			MarketID: "market-1", ConditionID: "0x" + strings.Repeat("c", 64), OutcomeIndex: 0,
			OutcomeName: "Yes", TokenID: "1", Shares: "1", WorstPrice: "0.50", TickSize: "0.01",
			Side: domain.SideSell, OrderType: domain.OrderTypeLimit, TimeInForce: domain.TimeInForceFOK,
			ExpiresAt: now.Add(30 * time.Minute), LotID: "lot-1", Ownership: "managed",
		}},
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := run(context.Background(), commandOptions{manifest: path})
	if err != nil {
		t.Fatal(err)
	}
	if result.Mode != "dry-run" || len(result.Actions) != 1 || result.Actions[0].State != "WOULD_SELL" {
		t.Fatalf("read-only CLI result = %#v", result)
	}
}

func TestRunExecuteSellAssemblesRealCLOBRPCAndJournalRecovery(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	manifest := cliSellManifest(now)
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(t.TempDir(), "sell-manifest.json")
	if err := os.WriteFile(manifestPath, manifestBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	accountsPath := filepath.Join(t.TempDir(), "accounts.json")
	accountsJSON := `{"accounts":[{"execution_account_id":"main","funder_address":"0x7e5f4552091a69125d5dfcb7b8c2659029395bdf","private_key":"0000000000000000000000000000000000000000000000000000000000000001","signature_type":0,"api_key":"key","api_secret":"c2VjcmV0","api_passphrase":"passphrase"}]}`
	if err := os.WriteFile(accountsPath, []byte(accountsJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(t.TempDir(), "wallet-exit-journal.json")
	var postCount atomic.Int32
	var sawSignedSell atomic.Bool
	var failFirstPost atomic.Bool
	var postedOrderID atomic.Value
	var tradeOrderID atomic.Value
	var tradeRequestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/" && request.Method == http.MethodPost {
			handleSellRPC(writer, request)
			return
		}
		switch request.URL.Path {
		case "/version":
			writeJSONTest(writer, map[string]any{"version": 2})
		case "/data/orders":
			writeJSONTest(writer, map[string]any{"data": []any{}, "next_cursor": "LTE="})
		case "/auth/ban-status/closed-only":
			writeJSONTest(writer, map[string]any{"closed_only": false})
		case "/balance-allowance":
			writeJSONTest(writer, map[string]any{
				"balance":    "1000000",
				"allowances": map[string]string{strings.ToLower("0xE111180000d2663C0091e4f400237545B87B996B"): "1000000"},
			})
		case "/tick-size":
			writeJSONTest(writer, map[string]any{"minimum_tick_size": "0.01"})
		case "/neg-risk":
			writeJSONTest(writer, map[string]any{"neg_risk": false})
		case "/book":
			writeJSONTest(writer, map[string]any{"timestamp": now.Format(time.RFC3339), "tick_size": "0.01", "min_order_size": "0.01", "bids": []map[string]string{{"price": "0.50", "size": "10"}}, "asks": []map[string]string{{"price": "0.51", "size": "10"}}})
		case "/order":
			postCount.Add(1)
			var payload struct {
				Order struct {
					Salt          json.Number `json:"salt"`
					Maker         string      `json:"maker"`
					Signer        string      `json:"signer"`
					TokenID       string      `json:"tokenId"`
					MakerAmount   string      `json:"makerAmount"`
					TakerAmount   string      `json:"takerAmount"`
					Side          string      `json:"side"`
					SignatureType uint8       `json:"signatureType"`
					Timestamp     string      `json:"timestamp"`
					Metadata      string      `json:"metadata"`
					Builder       string      `json:"builder"`
				} `json:"order"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.Order.Side != "SELL" || payload.Order.Maker != "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf" || payload.Order.TokenID != "123" {
				t.Fatalf("signed order identity = %#v", payload.Order)
			}
			sawSignedSell.Store(true)
			orderID, digestErr := cliSignedOrderID(payload.Order)
			if digestErr != nil {
				t.Fatal(digestErr)
			}
			postedOrderID.Store(orderID)
			if failFirstPost.CompareAndSwap(true, false) {
				// Close the response after the signed POST bytes have arrived. The
				// client must journal UNKNOWN and recover by GET instead of signing
				// another order on the next CLI invocation.
				hijacker, ok := writer.(http.Hijacker)
				if !ok {
					t.Fatal("test server does not support hijacking")
				}
				connection, _, hijackErr := hijacker.Hijack()
				if hijackErr != nil {
					t.Fatal(hijackErr)
				}
				_ = connection.Close()
				return
			}
			writeJSONTest(writer, map[string]any{"success": true, "orderID": orderID, "status": "matched"})
		default:
			if strings.HasPrefix(request.URL.Path, "/data/order/") {
				orderID := strings.TrimPrefix(request.URL.Path, "/data/order/")
				postedOrderID.Store(orderID)
				writeJSONTest(writer, map[string]any{"id": orderID, "status": "MATCHED", "market": manifest.Actions[0].ConditionID, "asset_id": manifest.Actions[0].TokenID, "side": "SELL", "original_size": "1.000000", "size_matched": "1.000000", "price": "0.50", "outcome": "Yes", "order_type": "FOK", "maker_address": "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf", "associate_trades": []string{"trade-1"}})
				return
			}
			if request.URL.Path == "/data/trades" {
				orderID, _ := postedOrderID.Load().(string)
				tradeOrderID.Store(orderID)
				tradeRequestCount.Add(1)
				writeJSONTest(writer, []map[string]any{{"id": "trade-1", "taker_order_id": orderID, "market": manifest.Actions[0].ConditionID, "asset_id": manifest.Actions[0].TokenID, "size": "1", "price": "0.50", "status": "CONFIRMED", "trader_side": "TAKER", "maker_address": "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"}})
				return
			}
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	previousSnapshotStoreFactory := walletExitSnapshotStoreFactory
	defer func() { walletExitSnapshotStoreFactory = previousSnapshotStoreFactory }()
	walletExitSnapshotStoreFactory = func(*sql.DB) (walletExitSnapshotStore, error) { return cliGuardStore{}, nil }
	previousFreezeCheck := walletExitProcessFreezeCheck
	defer func() { walletExitProcessFreezeCheck = previousFreezeCheck }()
	walletExitProcessFreezeCheck = func(context.Context) error { return nil }
	options := commandOptions{manifest: manifestPath, journal: journalPath, accountsFile: accountsPath, clobURL: server.URL, rpcURL: server.URL, databaseURL: "postgresql://wallet-exit-test", execute: true, confirm: walletcutoverexit.ManifestHash(manifestBytes), actor: "operator", reason: "cli execute test", timeout: 5 * time.Second}
	failFirstPost.Store(true)
	firstResult, firstErr := run(context.Background(), options)
	if firstErr == nil || firstResult.Actions[0].State != walletcutoverexit.JournalUnknown {
		t.Fatalf("first execute result/error = %#v / %v, want UNKNOWN", firstResult, firstErr)
	}
	secondResult, secondErr := run(context.Background(), options)
	if secondErr != nil || secondResult.Actions[0].State != walletcutoverexit.JournalFilled {
		tradeID, _ := tradeOrderID.Load().(string)
		t.Fatalf("recovery result/error = %#v / %v, want FILLED (trade_requests=%d trade_order=%s)", secondResult, secondErr, tradeRequestCount.Load(), tradeID)
	}
	if postCount.Load() != 1 || !sawSignedSell.Load() {
		t.Fatalf("POST count/signed sell = %d/%v, want one signed SELL POST", postCount.Load(), sawSignedSell.Load())
	}
	journalBytes, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	var journal walletcutoverexit.Journal
	if err := json.Unmarshal(journalBytes, &journal); err != nil || len(journal.Entries) != 1 || journal.Entries[0].State != walletcutoverexit.JournalFilled {
		t.Fatalf("journal = %s, error = %v", journalBytes, err)
	}
}

func TestRunExecuteZeroPayoutRedemptionUsesReceiptVerifier(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	funder := "0x6F908B3B67b9F9C40413775fFf48b286aeF9a081"
	condition := "0x" + strings.Repeat("b", 64)
	manifest := walletcutoverexit.Manifest{
		SchemaVersion: walletcutoverexit.SchemaVersion,
		ManifestID:    "cli-zero-redeem",
		Observed:      walletcutoverexit.Evidence{Block: 100, BlockHash: "0x" + strings.Repeat("a", 64), Observed: now},
		ExpiresAt:     now.Add(time.Hour),
		Redemptions: []walletcutoverexit.RedemptionAction{{
			ActionID: "redeem-1", ExecutionAccountID: "main", WalletAddress: funder, ConditionID: condition,
			TokenIDs: []string{"123"}, Shares: []domain.Decimal{"1"}, OutcomeIndexes: []int{0}, PayoutBaseUnits: "0",
		}},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(t.TempDir(), "redeem-manifest.json")
	if err := os.WriteFile(manifestPath, manifestBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	accountsPath := filepath.Join(t.TempDir(), "accounts.json")
	accountsJSON := `{"accounts":[{"execution_account_id":"main","funder_address":"` + funder + `","private_key":"0000000000000000000000000000000000000000000000000000000000000001","signature_type":3,"api_key":"key","api_secret":"c2VjcmV0","api_passphrase":"passphrase","relayer_api_key":"relayer-key","relayer_api_key_address":"0x0000000000000000000000000000000000005678"}]}`
	if err := os.WriteFile(accountsPath, []byte(accountsJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(t.TempDir(), "redeem-journal.json")
	var submitCount atomic.Int32
	var receiptCount atomic.Int32
	txHash := "0x" + strings.Repeat("d", 64)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/" && request.Method == http.MethodPost {
			handleRedeemRPC(writer, request, condition, funder, txHash, &receiptCount)
			return
		}
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/v1/account/transactions/params":
			writeJSONTest(writer, map[string]any{"address": "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf", "nonce": "7"})
		case request.Method == http.MethodPost && request.URL.Path == "/submit":
			submitCount.Add(1)
			writeJSONTest(writer, map[string]any{"state": "PENDING", "transactionID": "tx-1", "transactionHash": txHash})
		case request.Method == http.MethodGet && request.URL.Path == "/v1/account/transactions/tx-1":
			writeJSONTest(writer, map[string]any{"state": "STATE_CONFIRMED", "transaction_id": "tx-1", "transaction_hash": txHash})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	previousTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	defer func() { http.DefaultTransport = previousTransport }()
	previousRelayer := os.Getenv("POLYMARKET_RELAYER_URL")
	if err := os.Setenv("POLYMARKET_RELAYER_URL", server.URL); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Setenv("POLYMARKET_RELAYER_URL", previousRelayer) }()
	previousSnapshotStoreFactory := walletExitSnapshotStoreFactory
	defer func() { walletExitSnapshotStoreFactory = previousSnapshotStoreFactory }()
	walletExitSnapshotStoreFactory = func(*sql.DB) (walletExitSnapshotStore, error) { return cliGuardStore{}, nil }
	previousFreezeCheck := walletExitProcessFreezeCheck
	defer func() { walletExitProcessFreezeCheck = previousFreezeCheck }()
	walletExitProcessFreezeCheck = func(context.Context) error { return nil }
	options := commandOptions{manifest: manifestPath, journal: journalPath, accountsFile: accountsPath, clobURL: server.URL, rpcURL: server.URL, databaseURL: "postgresql://wallet-exit-test", execute: true, confirm: walletcutoverexit.ManifestHash(manifestBytes), actor: "operator", reason: "zero payout", timeout: 5 * time.Second}
	firstResult, firstErr := run(context.Background(), options)
	if firstErr == nil || firstResult.Actions[0].State != walletcutoverexit.JournalSubmitted {
		t.Fatalf("first redemption result/error = %#v / %v, want SUBMITTED recovery state", firstResult, firstErr)
	}
	secondResult, secondErr := run(context.Background(), options)
	if secondErr != nil || secondResult.Actions[0].State != walletcutoverexit.JournalConfirmed {
		t.Fatalf("redemption recovery result/error = %#v / %v, want CONFIRMED", secondResult, secondErr)
	}
	if submitCount.Load() != 1 || receiptCount.Load() == 0 {
		t.Fatalf("relayer submits/receipt reads = %d/%d, want one submit and receipt verification", submitCount.Load(), receiptCount.Load())
	}
}

func TestConditionPayoutBaseUnitsAllowsZeroPayout(t *testing.T) {
	action := walletcutoverexit.RedemptionAction{Shares: []domain.Decimal{"1"}, OutcomeIndexes: []int{0}}
	payout := evmrpc.ConditionPayout{Denominator: big.NewInt(1), Numerators: [2]*big.Int{big.NewInt(0), big.NewInt(0)}}
	value, err := conditionPayoutBaseUnits(action, payout)
	if err != nil || value != "0" {
		t.Fatalf("conditionPayoutBaseUnits() = %q, %v; want 0", value, err)
	}
}

func TestConditionPayoutBaseUnitsRejectsFractionalBaseUnits(t *testing.T) {
	action := walletcutoverexit.RedemptionAction{Shares: []domain.Decimal{"0.000001"}, OutcomeIndexes: []int{0}}
	payout := evmrpc.ConditionPayout{Denominator: big.NewInt(3), Numerators: [2]*big.Int{big.NewInt(1), big.NewInt(0)}}
	if _, err := conditionPayoutBaseUnits(action, payout); err == nil || !strings.Contains(err.Error(), "not an integer") {
		t.Fatalf("conditionPayoutBaseUnits() error = %v, want fractional-base-unit rejection", err)
	}
}

type cliGuardStore struct{}

func (cliGuardStore) Snapshot(context.Context, []string) ([]walletcutover.AccountState, error) {
	return []walletcutover.AccountState{
		{ExecutionAccountID: "main", WalletAddress: "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf", CollateralAsset: walletcutover.PUSDAsset, Version: 1, Paused: true},
		{ExecutionAccountID: "wallet-1", WalletAddress: "0x1111111111111111111111111111111111111111", CollateralAsset: walletcutover.PUSDAsset, Version: 1, Paused: true},
	}, nil
}

func cliSellManifest(now time.Time) walletcutoverexit.Manifest {
	return walletcutoverexit.Manifest{
		SchemaVersion: walletcutoverexit.SchemaVersion,
		ManifestID:    "cli-sell-execute",
		Observed: walletcutoverexit.Evidence{
			Block: 100, BlockHash: "0x" + strings.Repeat("a", 64), Observed: now,
		},
		ExpiresAt: now.Add(time.Hour),
		Actions: []walletcutoverexit.AssetAction{{
			ActionID: "sell-1", ExecutionAccountID: "main", WalletAddress: "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf",
			MarketID: "market-1", ConditionID: "0x" + strings.Repeat("c", 64), OutcomeIndex: 0, OutcomeName: "Yes",
			TokenID: "123", Shares: "1", WorstPrice: "0.50", TickSize: "0.01", Side: domain.SideSell,
			OrderType: domain.OrderTypeLimit, TimeInForce: domain.TimeInForceFOK, ExpiresAt: now.Add(30 * time.Minute),
			LotID: "lot-1", Ownership: "managed",
		}},
	}
}

func handleSellRPC(writer http.ResponseWriter, request *http.Request) {
	var envelope struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	result := any(nil)
	switch envelope.Method {
	case "eth_blockNumber":
		result = "0x12c"
	case "eth_chainId":
		result = "0x89"
	case "eth_getBlockByNumber":
		result = map[string]any{"hash": "0x" + strings.Repeat("a", 64), "timestamp": "0x64"}
	case "eth_getBalance":
		result = "0xde0b6b3a7640000"
	case "eth_call":
		result = "0x" + fmt.Sprintf("%064x", 1_000_000)
	default:
		http.Error(writer, "unexpected RPC method "+envelope.Method, http.StatusBadRequest)
		return
	}
	if len(envelope.ID) == 0 {
		envelope.ID = json.RawMessage("1")
	}
	payload := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(envelope.ID), "result": result}
	writeJSONTest(writer, payload)
}

func handleRedeemRPC(writer http.ResponseWriter, request *http.Request, condition, wallet, txHash string, receiptCount *atomic.Int32) {
	var envelope struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	result := any(nil)
	switch envelope.Method {
	case "eth_blockNumber":
		result = "0x12c"
	case "eth_getBlockByNumber":
		result = map[string]any{"hash": "0x" + strings.Repeat("a", 64), "timestamp": "0x64"}
	case "eth_getBalance":
		result = "0xde0b6b3a7640000"
	case "eth_getTransactionReceipt":
		receiptCount.Add(1)
		result = zeroPayoutReceipt(condition, wallet, txHash)
	case "eth_call":
		data := ""
		if len(envelope.Params) > 0 {
			var call map[string]any
			if err := json.Unmarshal(envelope.Params[0], &call); err == nil {
				data, _ = call["data"].(string)
			}
		}
		switch {
		case strings.HasPrefix(data, "0xe985e9c5"):
			result = "0x" + fmt.Sprintf("%064x", 1)
		case strings.HasPrefix(data, "0x"+testSelector("payoutDenominator(bytes32)")):
			result = "0x" + fmt.Sprintf("%064x", 1)
		case strings.HasPrefix(data, "0x"+testSelector("payoutNumerators(bytes32,uint256)")):
			result = "0x" + fmt.Sprintf("%064x", 0)
		case strings.HasPrefix(data, "0x00fdd58e"):
			result = "0x" + fmt.Sprintf("%064x", 1_000_000)
		default:
			result = "0x" + fmt.Sprintf("%064x", 0)
		}
	default:
		http.Error(writer, "unexpected RPC method "+envelope.Method, http.StatusBadRequest)
		return
	}
	if len(envelope.ID) == 0 {
		envelope.ID = json.RawMessage("1")
	}
	writeJSONTest(writer, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(envelope.ID), "result": result})
}

func testSelector(signature string) string {
	hash := sha3.NewLegacyKeccak256()
	_, _ = hash.Write([]byte(signature))
	return hex.EncodeToString(hash.Sum(nil)[:4])
}

func zeroPayoutReceipt(condition, wallet, txHash string) map[string]any {
	event := sha3.NewLegacyKeccak256()
	_, _ = event.Write([]byte("PositionsRedeemed(address,bytes32,uint256[],uint256)"))
	topic := "0x" + hex.EncodeToString(event.Sum(nil))
	walletWord := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(strings.ToLower(wallet), "0x")
	amountsData := "0x" + fmt.Sprintf("%064x%064x%064x%064x%064x", 64, 0, 2, 0, 0)
	return map[string]any{
		"transactionHash": txHash,
		"blockHash":       "0x" + strings.Repeat("a", 64),
		"blockNumber":     "0x64",
		"status":          "0x1",
		"logs": []map[string]any{{
			"address":         "0xada100db00ca00073811820692005400218fce1f",
			"topics":          []string{topic, walletWord, strings.ToLower(condition)},
			"data":            amountsData,
			"removed":         false,
			"transactionHash": txHash,
			"blockHash":       "0x" + strings.Repeat("a", 64),
			"blockNumber":     "0x64",
		}},
	}
}

func writeJSONTest(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}

func cliSignedOrderID(order struct {
	Salt          json.Number `json:"salt"`
	Maker         string      `json:"maker"`
	Signer        string      `json:"signer"`
	TokenID       string      `json:"tokenId"`
	MakerAmount   string      `json:"makerAmount"`
	TakerAmount   string      `json:"takerAmount"`
	Side          string      `json:"side"`
	SignatureType uint8       `json:"signatureType"`
	Timestamp     string      `json:"timestamp"`
	Metadata      string      `json:"metadata"`
	Builder       string      `json:"builder"`
}) (string, error) {
	salt, ok := new(big.Int).SetString(order.Salt.String(), 10)
	if !ok {
		return "", fmt.Errorf("invalid salt")
	}
	timestamp, err := strconv.ParseInt(order.Timestamp, 10, 64)
	if err != nil {
		return "", err
	}
	side := big.NewInt(0)
	if order.Side == "SELL" {
		side.SetInt64(1)
	}
	wordAddress := func(value string) ([]byte, error) {
		raw, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(value), "0x"))
		if err != nil || len(raw) != 20 {
			return nil, fmt.Errorf("invalid address")
		}
		word := make([]byte, 32)
		copy(word[12:], raw)
		return word, nil
	}
	wordBytes32 := func(value string) ([]byte, error) {
		raw, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(value), "0x"))
		if err != nil || len(raw) != 32 {
			return nil, fmt.Errorf("invalid bytes32")
		}
		return raw, nil
	}
	wordUint := func(value *big.Int) []byte {
		word := make([]byte, 32)
		bytes := value.Bytes()
		copy(word[len(word)-len(bytes):], bytes)
		return word
	}
	keccak := func(parts ...[]byte) []byte {
		hash := sha3.NewLegacyKeccak256()
		for _, part := range parts {
			_, _ = hash.Write(part)
		}
		return hash.Sum(nil)
	}
	verifyingContract, err := wordAddress("0xE111180000d2663C0091e4f400237545B87B996B")
	if err != nil {
		return "", err
	}
	maker, err := wordAddress(order.Maker)
	if err != nil {
		return "", err
	}
	signer, err := wordAddress(order.Signer)
	if err != nil {
		return "", err
	}
	tokenID, ok := new(big.Int).SetString(order.TokenID, 10)
	if !ok {
		return "", fmt.Errorf("invalid token id")
	}
	makerAmount, ok := new(big.Int).SetString(order.MakerAmount, 10)
	if !ok {
		return "", fmt.Errorf("invalid maker amount")
	}
	takerAmount, ok := new(big.Int).SetString(order.TakerAmount, 10)
	if !ok {
		return "", fmt.Errorf("invalid taker amount")
	}
	metadata, err := wordBytes32(order.Metadata)
	if err != nil {
		return "", err
	}
	builder, err := wordBytes32(order.Builder)
	if err != nil {
		return "", err
	}
	const domainType = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
	const orderType = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)"
	domainSeparator := keccak(keccak([]byte(domainType)), keccak([]byte("Polymarket CTF Exchange")), keccak([]byte("2")), wordUint(big.NewInt(137)), verifyingContract)
	orderHash := keccak(keccak([]byte(orderType)), wordUint(salt), maker, signer, wordUint(tokenID), wordUint(makerAmount), wordUint(takerAmount), wordUint(side), wordUint(new(big.Int).SetUint64(uint64(order.SignatureType))), wordUint(big.NewInt(timestamp)), metadata, builder)
	digest := keccak([]byte{0x19, 0x01}, domainSeparator, orderHash)
	return "0x" + hex.EncodeToString(digest), nil
}

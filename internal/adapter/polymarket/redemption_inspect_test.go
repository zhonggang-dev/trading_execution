package polymarket

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type rpcRequest struct {
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// newInspectTestClient serves eth_call through answer(to, data) and records
// every request.
func newInspectTestClient(t *testing.T, account TradingAccount, answer func(to, data string) string) (*RedemptionClient, *[]rpcRequest) {
	t.Helper()
	var mu sync.Mutex
	requests := []rpcRequest{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests = append(requests, request)
		mu.Unlock()
		result := `"0x"`
		if request.Method == "eth_call" {
			var params []json.RawMessage
			_ = json.Unmarshal(request.Params, &params)
			var call map[string]string
			_ = json.Unmarshal(params[0], &call)
			result = `"` + answer(call["to"], call["data"]) + `"`
		}
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, request.ID, result)
	}))
	t.Cleanup(server.Close)
	client, err := NewRedemptionClient(RedemptionClientParams{
		RPCURL: server.URL, HTTPClient: server.Client(), RequestTimeout: time.Second,
		RequiredConfirmations: 1, Credentials: singleAccountProvider{account: account},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, &requests
}

func wordHex(value *big.Int) string { return "0x" + hex.EncodeToString(uint256Word(value)) }

func testEOAAccount(t *testing.T) TradingAccount {
	t.Helper()
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	return TradingAccount{ExecutionAccountID: "wallet-2", FunderAddress: signer.Address(), SignatureType: SignatureTypeEOA, Signer: signer}
}

func TestSimulateRedemptionCallsExactRedeemDataFromWallet(t *testing.T) {
	condition := "0x9601280c3d5109783ba64644da25bdfdf120ce516a696f88a42f54ffd2ac761b"
	client, requests := newInspectTestClient(t, testEOAAccount(t), func(string, string) string { return "0x" })
	wallet := "0x00000000000000000000000000000000000000E0"
	if err := client.SimulateRedemption(context.Background(), wallet, condition, true); err != nil {
		t.Fatal(err)
	}
	want, _ := redeemPositionsCall(negRiskCollateralAdapter, condition)
	if len(*requests) != 1 || (*requests)[0].Method != "eth_call" {
		t.Fatalf("requests = %+v", *requests)
	}
	var params []json.RawMessage
	_ = json.Unmarshal((*requests)[0].Params, &params)
	var call map[string]string
	_ = json.Unmarshal(params[0], &call)
	if call["from"] != strings.ToLower(wallet) || call["to"] != negRiskCollateralAdapter || call["data"] != want.Data || string(params[1]) != `"latest"` {
		t.Fatalf("simulation call = %v block %s", call, params[1])
	}
}

func TestAdapterPositionsReadsAdapterCollateralAndCTFIds(t *testing.T) {
	usdce := "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
	wcol := "0x3a3bd7bb9528e159577f7c2e685cc81a765002e2"
	selector := func(signature string) string { return "0x" + hex.EncodeToString(abiSelector(signature)) }
	answer := func(to, data string) string {
		switch {
		case to == standardCollateralAdapter && data == selector("USDCE()"):
			return "0x" + strings.Repeat("0", 24) + usdce[2:]
		case to == negRiskCollateralAdapter && data == selector("WRAPPED_COLLATERAL()"):
			return "0x" + strings.Repeat("0", 24) + wcol[2:]
		case to == conditionalTokensAddress && strings.HasPrefix(data, selector("getCollectionId(bytes32,bytes32,uint256)")):
			indexSet := new(big.Int).SetBytes(mustHex(data[len(data)-64:]))
			return wordHex(new(big.Int).Add(big.NewInt(1000), indexSet))
		case to == conditionalTokensAddress && strings.HasPrefix(data, selector("getPositionId(address,bytes32)")):
			collateral := data[10+24 : 10+64]
			collection := new(big.Int).SetBytes(mustHex(data[len(data)-64:]))
			offset := int64(0)
			if collateral == wcol[2:] {
				offset = 5000
			}
			return wordHex(new(big.Int).Add(big.NewInt(offset), collection))
		}
		t.Fatalf("unexpected eth_call to %s data %s", to, data)
		return ""
	}
	client, _ := newInspectTestClient(t, testEOAAccount(t), answer)
	condition := "0x" + strings.Repeat("11", 32)
	standard, err := client.AdapterPositions(context.Background(), condition, false)
	if err != nil {
		t.Fatal(err)
	}
	if standard.Adapter != standardCollateralAdapter || standard.Collateral != usdce || standard.PositionIDs != [2]string{"1001", "1002"} {
		t.Fatalf("standard = %+v", standard)
	}
	negRisk, err := client.AdapterPositions(context.Background(), condition, true)
	if err != nil {
		t.Fatal(err)
	}
	if negRisk.Adapter != negRiskCollateralAdapter || negRisk.Collateral != wcol || negRisk.PositionIDs != [2]string{"6001", "6002"} {
		t.Fatalf("neg risk = %+v", negRisk)
	}
}

func TestConditionPayoutSkipsNumeratorsWhenUnresolved(t *testing.T) {
	client, requests := newInspectTestClient(t, testEOAAccount(t), func(string, string) string { return wordHex(big.NewInt(0)) })
	payout, err := client.ConditionPayout(context.Background(), "0x"+strings.Repeat("22", 32))
	if err != nil {
		t.Fatal(err)
	}
	if payout.Resolved() || len(*requests) != 1 {
		t.Fatalf("payout %+v after %d requests", payout, len(*requests))
	}
}

func TestSubmitEOAOperatorApprovalRejectsBeforeAnyRPC(t *testing.T) {
	client, requests := newInspectTestClient(t, testEOAAccount(t), func(string, string) string { return "0x" })
	if _, err := client.SubmitEOAOperatorApproval(context.Background(), "wallet-2", "0x000000000000000000000000000000000000dead"); err == nil {
		t.Fatal("non-allowlisted operator must be rejected")
	}
	deposit := testEOAAccount(t)
	deposit.SignatureType = SignatureTypePolyEIP1271
	depositClient, depositRequests := newInspectTestClient(t, deposit, func(string, string) string { return "0x" })
	if _, err := depositClient.SubmitEOAOperatorApproval(context.Background(), "wallet-2", polygonExchangeV2); err == nil {
		t.Fatal("non-EOA account must be rejected")
	}
	if len(*requests) != 0 || len(*depositRequests) != 0 {
		t.Fatalf("rejections must not reach the RPC")
	}
}

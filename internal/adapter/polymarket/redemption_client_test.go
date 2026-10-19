package polymarket

import (
	"context"
	"encoding/hex"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type singleAccountProvider struct{ account TradingAccount }

func (provider singleAccountProvider) Account(context.Context, string) (TradingAccount, error) {
	return provider.account, nil
}

// newRelayerTestClient points a redemption client at a fake HTTPS relayer.
// submits counts the POST /submit requests that reached it.
func newRelayerTestClient(t *testing.T, nonceStatus int, submit http.HandlerFunc) (*RedemptionClient, *atomic.Int32) {
	t.Helper()
	var submits atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/account/transactions/params":
			if nonceStatus != http.StatusOK {
				http.Error(w, "nonce unavailable", nonceStatus)
				return
			}
			_, _ = w.Write([]byte(`{"address":"0x1","nonce":"7"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/submit":
			submits.Add(1)
			submit(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewRedemptionClient(RedemptionClientParams{
		RelayerURL: server.URL, RPCURL: server.URL, HTTPClient: server.Client(),
		RequestTimeout: 200 * time.Millisecond, RequiredConfirmations: 64,
		Credentials: singleAccountProvider{account: TradingAccount{
			ExecutionAccountID: "wallet-7", FunderAddress: "0x0000000000000000000000000000000000001234",
			SignatureType: SignatureTypePolyEIP1271, Signer: signer,
			Relayer: RelayerCredentials{Key: "key", Address: "0x0000000000000000000000000000000000005678"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, &submits
}

func TestDepositWalletSubmitFailuresAreClassifiedByWhatTheRelayerDid(t *testing.T) {
	conditionID := "0x" + strings.Repeat("ab", 32)
	cases := []struct {
		name        string
		nonceStatus int
		submit      http.HandlerFunc
		wantOutcome domain.RedemptionSubmitOutcome
		wantStatus  int
		wantText    string
		wantSubmits int32
	}{
		{
			name: "400 is an explicit rejection", nonceStatus: http.StatusOK,
			submit: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, `{"error":"invalid nonce: expected 8"}`, http.StatusBadRequest)
			},
			wantOutcome: domain.RedemptionSubmitRejected, wantStatus: 400, wantText: "invalid nonce: expected 8", wantSubmits: 1,
		},
		{
			name: "429 is an explicit rejection", nonceStatus: http.StatusOK,
			submit:      func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "slow down", http.StatusTooManyRequests) },
			wantOutcome: domain.RedemptionSubmitRejected, wantStatus: 429, wantText: "slow down", wantSubmits: 1,
		},
		{
			name: "408 is a timeout, not a verdict", nonceStatus: http.StatusOK,
			submit:      func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "timeout", http.StatusRequestTimeout) },
			wantOutcome: domain.RedemptionSubmitAmbiguous, wantStatus: 408, wantSubmits: 1,
		},
		{
			name: "500 may follow an accepted job", nonceStatus: http.StatusOK,
			submit:      func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", http.StatusInternalServerError) },
			wantOutcome: domain.RedemptionSubmitAmbiguous, wantStatus: 500, wantSubmits: 1,
		},
		{
			name: "an unreadable success body is ambiguous", nonceStatus: http.StatusOK,
			submit:      func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("not json")) },
			wantOutcome: domain.RedemptionSubmitAmbiguous, wantSubmits: 1,
		},
		{
			name: "a client timeout is ambiguous", nonceStatus: http.StatusOK,
			submit: func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-time.After(500 * time.Millisecond):
				}
			},
			wantOutcome: domain.RedemptionSubmitAmbiguous, wantSubmits: 1,
		},
		{
			name: "a failed nonce fetch sent nothing", nonceStatus: http.StatusInternalServerError,
			submit:      func(http.ResponseWriter, *http.Request) {},
			wantOutcome: domain.RedemptionSubmitNotSubmitted, wantStatus: 500, wantText: "nonce unavailable", wantSubmits: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, submits := newRelayerTestClient(t, tc.nonceStatus, tc.submit)
			_, err := client.SubmitRedemption(context.Background(), "wallet-7", conditionID, false)
			if err == nil {
				t.Fatal("expected a submit error")
			}
			outcome, status := domain.SubmitOutcomeOf(err)
			if outcome != tc.wantOutcome || status != tc.wantStatus {
				t.Fatalf("outcome/status = %s/%d, want %s/%d (err: %v)", outcome, status, tc.wantOutcome, tc.wantStatus, err)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q lost the raw relayer text %q", err, tc.wantText)
			}
			if submits.Load() != tc.wantSubmits {
				t.Fatalf("POST /submit count = %d, want %d", submits.Load(), tc.wantSubmits)
			}
		})
	}
}

func TestRelayerRejectionKeepsTheFullBody(t *testing.T) {
	body := strings.Repeat("x", 1500)
	client, _ := newRelayerTestClient(t, http.StatusOK, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, body, http.StatusBadRequest)
	})
	_, err := client.SubmitRedemption(context.Background(), "wallet-7", "0x"+strings.Repeat("ab", 32), false)
	if err == nil || !strings.Contains(err.Error(), body) {
		t.Fatalf("a 1500-byte rejection body was truncated: %v", err)
	}
}

func TestDepositWalletBatchDigestMatchesEthAccountVector(t *testing.T) {
	call, err := setApprovalForAllCall(standardCollateralAdapter)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := depositWalletBatchDigest(
		"0x0000000000000000000000000000000000001234",
		big.NewInt(3), big.NewInt(1_000_000), []redemptionCall{call},
	)
	if err != nil {
		t.Fatal(err)
	}
	const expectedDigest = "32faa893843148413d267d1d3232ec96398e008cc377bad9fcf5d2c20eceecbb"
	if hex.EncodeToString(digest) != expectedDigest {
		t.Fatalf("Deposit Wallet digest = %x, want %s", digest, expectedDigest)
	}
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signer.SignDigest(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	const expectedSignature = "83d1f5c583bc772fc81f27c988e3664f7ee5646f448185f3d05ff186f50b3ea95658ae88db5e43db48082687a99760b5f907ab9fe2ed3ef75448b6da405f2a731b"
	if hex.EncodeToString(signature) != expectedSignature {
		t.Fatalf("Deposit Wallet signature = %x, want %s", signature, expectedSignature)
	}
}

func TestRedeemPositionsCallTargetsExactAdapterAndBinaryPartition(t *testing.T) {
	condition := "0x9601280c3d5109783ba64644da25bdfdf120ce516a696f88a42f54ffd2ac761b"
	call, err := redeemPositionsCall(negRiskCollateralAdapter, condition)
	if err != nil {
		t.Fatal(err)
	}
	if call.Target != negRiskCollateralAdapter || call.Value != "0" {
		t.Fatalf("redeem target/value = %s/%s", call.Target, call.Value)
	}
	data, err := decodeVariableHex(call.Data, "redeem data")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 4+7*32 {
		t.Fatalf("redeem calldata length = %d", len(data))
	}
	wantSelector := keccak256([]byte("redeemPositions(address,bytes32,bytes32,uint256[])"))[:4]
	if hex.EncodeToString(data[:4]) != hex.EncodeToString(wantSelector) {
		t.Fatalf("redeem selector = %x", data[:4])
	}
	if new(big.Int).SetBytes(data[4+3*32:4+4*32]).Uint64() != 128 ||
		new(big.Int).SetBytes(data[4+4*32:4+5*32]).Uint64() != 2 ||
		new(big.Int).SetBytes(data[4+5*32:4+6*32]).Uint64() != 1 ||
		new(big.Int).SetBytes(data[4+6*32:]).Uint64() != 2 {
		t.Fatalf("redeem binary index set encoding is invalid")
	}
}

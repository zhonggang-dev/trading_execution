package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTradingApprovalCallEncoding(t *testing.T) {
	const spender = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"
	pusd, err := tradingApprovalCall(TradingApproval{Kind: ApprovalPUSDAllowance, Spender: spender})
	if err != nil {
		t.Fatal(err)
	}
	wantPUSD := "0x095ea7b3" + strings.Repeat("0", 24) + spender[2:] + strings.Repeat("f", 64)
	if pusd.Target != strings.ToLower(PUSDAddress) || pusd.Value != "0" || pusd.Data != wantPUSD {
		t.Fatalf("pUSD approve call = %#v, want data %s", pusd, wantPUSD)
	}
	ctf, err := tradingApprovalCall(TradingApproval{Kind: ApprovalCTFOperator, Spender: spender})
	if err != nil {
		t.Fatal(err)
	}
	wantCTF := "0xa22cb465" + strings.Repeat("0", 24) + spender[2:] + strings.Repeat("0", 63) + "1"
	if ctf.Target != conditionalTokensAddress || ctf.Data != wantCTF {
		t.Fatalf("CTF setApprovalForAll call = %#v, want data %s", ctf, wantCTF)
	}
	if _, err := tradingApprovalCall(TradingApproval{Kind: "OTHER", Spender: spender}); err == nil {
		t.Fatal("unsupported approval kind was accepted")
	}
	if _, err := tradingApprovalCall(TradingApproval{Kind: ApprovalPUSDAllowance, Spender: "0x1234"}); err == nil {
		t.Fatal("invalid spender was accepted")
	}
}

// unusedCredentials satisfies the constructor; reading approvals never signs.
type unusedCredentials struct{}

func (unusedCredentials) Account(context.Context, string) (TradingAccount, error) {
	return TradingAccount{}, errors.New("credentials are not used when reading approvals")
}

func TestReadTradingApprovalsRequiresMaxAllowanceAndTrueOperator(t *testing.T) {
	const wallet = "0x635d25519789e40c3794d72a88cbb7f25ac443f8"
	const spender = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"
	responses := map[string]string{
		"pusd_max":  "0x" + strings.Repeat("f", 64),
		"pusd_part": "0x" + strings.Repeat("0", 56) + "0bebc200",
		"pusd_zero": "0x" + strings.Repeat("0", 64),
		"ctf_true":  "0x" + strings.Repeat("0", 63) + "1",
		"ctf_false": "0x" + strings.Repeat("0", 64),
	}
	var current string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var request struct {
			ID     json.RawMessage   `json:"id"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.Unmarshal(body, &request)
		var call struct{ To, Data string }
		_ = json.Unmarshal(request.Params[0], &call)
		wantSelector := map[string]string{PUSDAddress: "0xdd62ed3e", conditionalTokensAddress: "0xe985e9c5"}[call.To]
		if wantSelector == "" {
			wantSelector = map[string]string{strings.ToLower(PUSDAddress): "0xdd62ed3e"}[call.To]
		}
		if !strings.HasPrefix(call.Data, wantSelector) || !strings.Contains(call.Data, wallet[2:]) || !strings.Contains(call.Data, spender[2:]) {
			t.Errorf("unexpected eth_call %#v", call)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": responses[current]})
	}))
	defer server.Close()
	provider := unusedCredentials{}
	client, err := NewRedemptionClient(RedemptionClientParams{
		RPCURL: server.URL, HTTPClient: server.Client(), Credentials: provider, RequiredConfirmations: 1,
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		kind     TradingApprovalKind
		response string
		want     bool
	}{
		{"allowance max", ApprovalPUSDAllowance, "pusd_max", true},
		{"allowance partial", ApprovalPUSDAllowance, "pusd_part", false},
		{"allowance zero", ApprovalPUSDAllowance, "pusd_zero", false},
		{"operator approved", ApprovalCTFOperator, "ctf_true", true},
		{"operator not approved", ApprovalCTFOperator, "ctf_false", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			current = tc.response
			states, err := client.ReadTradingApprovals(t.Context(), wallet, []TradingApproval{{Kind: tc.kind, Spender: spender}})
			if err != nil {
				t.Fatal(err)
			}
			if len(states) != 1 || states[0].Satisfied != tc.want {
				t.Fatalf("state = %#v, want satisfied=%v", states, tc.want)
			}
		})
	}
}

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletpnl"
)

type walletPnLTestAddresses struct {
	values map[string]string
	err    error
}

func (reader walletPnLTestAddresses) WalletAddress(_ context.Context, accountID string) (string, error) {
	if reader.err != nil {
		return "", reader.err
	}
	address, ok := reader.values[accountID]
	if !ok {
		return "", errors.New("unknown account")
	}
	return address, nil
}

type walletPnLTestSource struct{ err error }

func (source walletPnLTestSource) Series(_ context.Context, _ string, _ string, _ string) (polymarketdata.PnLSeries, error) {
	if source.err != nil {
		return polymarketdata.PnLSeries{}, source.err
	}
	return polymarketdata.PnLSeries{Points: []polymarketdata.PnLPoint{{Timestamp: 1, TradePnL: domain.Decimal("1.25")}}}, nil
}

func newWalletPnLHTTPTestServer(t *testing.T, source walletPnLTestSource) *Server {
	t.Helper()
	addresses := walletPnLTestAddresses{values: map[string]string{
		"main":     "0x1111111111111111111111111111111111111111",
		"wallet-1": "0x2222222222222222222222222222222222222222",
		"wallet-6": "0x6666666666666666666666666666666666666666",
		"wallet-7": "0x7777777777777777777777777777777777777777",
	}}
	service, err := walletpnl.New(walletpnl.Params{
		Addresses: addresses, Source: source,
		Accounts: []string{"main", "wallet-1", "wallet-6", "wallet-7"},
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Params{
		Service: baseExecutionService(t), WalletPnL: service,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIToken: "execution-secret", ReadOnlyToken: "readonly-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestWalletPnLEndpointAuthAndFourManagedAccounts(t *testing.T) {
	server := newWalletPnLHTTPTestServer(t, walletPnLTestSource{})
	if response := performRequest(t, server, http.MethodGet, "/api/v1/wallet-pnl?execution_account_id=main&interval=1w&fidelity=3h", "", ""); response.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d", response.Code)
	}
	if response := performRequest(t, server, http.MethodGet, "/api/v1/wallet-pnl?execution_account_id=main&interval=1w&fidelity=3h", "", "execution-secret"); response.Code != http.StatusForbidden {
		t.Fatalf("execution token status = %d", response.Code)
	}
	for _, accountID := range []string{"main", "wallet-1", "wallet-6", "wallet-7"} {
		response := performRequest(t, server, http.MethodGet, "/api/v1/wallet-pnl?execution_account_id="+accountID+"&interval=1w&fidelity=3h", "", "readonly-secret")
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"execution_account_id":"`+accountID+`"`) || !strings.Contains(response.Body.String(), `"metric":"trade_pnl"`) {
			t.Fatalf("account %s status=%d body=%s", accountID, response.Code, response.Body.String())
		}
	}
}

func TestWalletPnLEndpointRejectsUnknownDuplicateAndInvalidQueries(t *testing.T) {
	server := newWalletPnLHTTPTestServer(t, walletPnLTestSource{})
	tests := []struct {
		path string
		want int
	}{
		{"/api/v1/wallet-pnl?execution_account_id=unknown&interval=1w&fidelity=3h", http.StatusNotFound},
		{"/api/v1/wallet-pnl?execution_account_id=main&interval=1y&fidelity=3h", http.StatusBadRequest},
		{"/api/v1/wallet-pnl?execution_account_id=main&interval=1w&interval=1d&fidelity=3h", http.StatusBadRequest},
		{"/api/v1/wallet-pnl?execution_account_id=main&interval=1w&fidelity=3h&extra=x", http.StatusBadRequest},
	}
	for _, test := range tests {
		response := performRequest(t, server, http.MethodGet, test.path, "", "readonly-secret")
		if response.Code != test.want {
			t.Fatalf("path %s status=%d want=%d body=%s", test.path, response.Code, test.want, response.Body.String())
		}
	}
}

func TestWalletPnLEndpointReturns503WhenUnconfigured(t *testing.T) {
	server, err := New(Params{Service: baseExecutionService(t), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	response := performRequest(t, server, http.MethodGet, "/api/v1/wallet-pnl?execution_account_id=main&interval=1w&fidelity=3h", "", "")
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "WALLET_PNL_UNAVAILABLE") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestWalletPnLEndpointMapsTimeout(t *testing.T) {
	server := newWalletPnLHTTPTestServer(t, walletPnLTestSource{err: context.DeadlineExceeded})
	response := performRequest(t, server, http.MethodGet, "/api/v1/wallet-pnl?execution_account_id=main&interval=1w&fidelity=3h", "", "readonly-secret")
	if response.Code != http.StatusGatewayTimeout || !strings.Contains(response.Body.String(), "WALLET_PNL_TIMEOUT") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestWalletPnLAccountsEndpointIsReadOnlyAndDoesNotExposeAddresses(t *testing.T) {
	server := newWalletPnLHTTPTestServer(t, walletPnLTestSource{err: errors.New("must not read external PnL")})
	path := "/api/v1/wallet-pnl/accounts"
	for _, test := range []struct {
		method string
		token  string
		want   int
	}{
		{http.MethodGet, "", http.StatusUnauthorized},
		{http.MethodGet, "execution-secret", http.StatusForbidden},
		{http.MethodPost, "readonly-secret", http.StatusMethodNotAllowed},
		{http.MethodDelete, "readonly-secret", http.StatusMethodNotAllowed},
	} {
		response := performRequest(t, server, test.method, path, "", test.token)
		if response.Code != test.want {
			t.Fatalf("%s token=%q status=%d want=%d", test.method, test.token, response.Code, test.want)
		}
	}
	response := performRequest(t, server, http.MethodGet, path, "", "readonly-secret")
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	var envelope struct {
		Data walletpnl.AccountList `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Data.Accounts) != 4 || envelope.Data.ObservedAt.IsZero() {
		t.Fatalf("response = %#v", envelope)
	}
	for _, account := range envelope.Data.Accounts {
		if account.MarketSource != "POLYMARKET" || account.ExecutionAccountID == "" {
			t.Fatalf("account = %#v", account)
		}
	}
	if strings.Contains(response.Body.String(), "wallet_address") || strings.Contains(response.Body.String(), "0x") {
		t.Fatalf("discovery response contains a wallet identity: %s", response.Body.String())
	}
	for _, suffix := range []string{"?execution_account_id=main", "?user=0x1111", "?x=1&x=2", "?%zz=invalid"} {
		if result := performRequest(t, server, http.MethodGet, path+suffix, "", "readonly-secret"); result.Code != http.StatusBadRequest {
			t.Fatalf("path %s status=%d", path+suffix, result.Code)
		}
	}
}

func TestWalletPnLAccountsEndpointFailureIsNotEmptySuccess(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"registration", errors.New("private database detail"), http.StatusServiceUnavailable},
		{"timeout", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"canceled", context.Canceled, http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newWalletPnLHTTPTestServer(t, walletPnLTestSource{})
			service, err := walletpnl.New(walletpnl.Params{Addresses: walletPnLTestAddresses{err: test.err}, Source: walletPnLTestSource{}, Accounts: []string{"main"}})
			if err != nil {
				t.Fatal(err)
			}
			server.walletPnL = service
			response := performRequest(t, server, http.MethodGet, "/api/v1/wallet-pnl/accounts", "", "readonly-secret")
			if response.Code != test.want || strings.Contains(response.Body.String(), "private database detail") || strings.Contains(response.Body.String(), `"accounts":[]`) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	server := newWalletPnLHTTPTestServer(t, walletPnLTestSource{})
	server.walletPnL = nil
	if response := performRequest(t, server, http.MethodGet, "/api/v1/wallet-pnl/accounts", "", "readonly-secret"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured status=%d body=%s", response.Code, response.Body.String())
	}
}

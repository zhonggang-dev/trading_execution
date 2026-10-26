package httpapi

import (
	"context"
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

type walletPnLTestAddresses struct{ values map[string]string }

func (reader walletPnLTestAddresses) WalletAddress(_ context.Context, accountID string) (string, error) {
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

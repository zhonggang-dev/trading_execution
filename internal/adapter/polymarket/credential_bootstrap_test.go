package polymarket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestCredentialBootstrapperCreatesCredentials 验证 Credential Bootstrapper Creates Credentials 场景下的行为。
func TestCredentialBootstrapperCreatesCredentials(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		assertL1Headers(t, request, http.MethodPost, "7")
		_ = json.NewEncoder(writer).Encode(map[string]string{
			"apiKey": "created-key", "secret": "c2VjcmV0", "passphrase": "created-passphrase",
		})
	}))
	defer server.Close()

	bootstrapper := newTestCredentialBootstrapper(t, server.URL)
	signer, err := NewEOASigner(testPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := bootstrapper.CreateOrDerive(context.Background(), signer, 7)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || credentials.Key != "created-key" {
		t.Fatalf("requests/credentials = %d/%#v", requests.Load(), credentials)
	}
}

// TestCredentialBootstrapperDerivesAfterCreateFailure 验证 Credential Bootstrapper Derives After Create Failure 场景下的行为。
func TestCredentialBootstrapperDerivesAfterCreateFailure(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		switch request.URL.Path {
		case "/auth/api-key":
			assertL1Headers(t, request, http.MethodPost, "0")
			writer.WriteHeader(http.StatusConflict)
		case "/auth/derive-api-key":
			assertL1Headers(t, request, http.MethodGet, "0")
			_ = json.NewEncoder(writer).Encode(map[string]string{
				"apiKey": "derived-key", "secret": "c2VjcmV0", "passphrase": "derived-passphrase",
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	bootstrapper := newTestCredentialBootstrapper(t, server.URL)
	signer, err := NewEOASigner(testPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := bootstrapper.CreateOrDerive(context.Background(), signer, 0)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 || credentials.Key != "derived-key" {
		t.Fatalf("requests/credentials = %d/%#v", requests.Load(), credentials)
	}
}

// newTestCredentialBootstrapper 创建测试所需的模拟对象。
func newTestCredentialBootstrapper(t *testing.T, baseURL string) *CredentialBootstrapper {
	t.Helper()
	bootstrapper, err := NewCredentialBootstrapper(CredentialBootstrapParams{
		BaseURL: baseURL,
		Timeout: time.Second,
		Now:     func() time.Time { return time.Unix(1_700_000_000, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return bootstrapper
}

// assertL1Headers 执行对应的测试断言。
func assertL1Headers(t *testing.T, request *http.Request, method, nonce string) {
	t.Helper()
	if request.Method != method {
		t.Errorf("method = %s, want %s", request.Method, method)
	}
	checks := map[string]string{
		"POLY_ADDRESS":   testEOAAddress,
		"POLY_TIMESTAMP": strconv.FormatInt(1_700_000_000, 10),
		"POLY_NONCE":     nonce,
	}
	for header, want := range checks {
		if got := request.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if signature := request.Header.Get("POLY_SIGNATURE"); len(signature) != 132 || signature[:2] != "0x" {
		t.Errorf("POLY_SIGNATURE has invalid shape")
	}
}

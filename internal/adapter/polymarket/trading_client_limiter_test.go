package polymarket

import (
	"encoding/base64"
	"testing"
	"time"
)

// TestNewTradingClientRequestLimiter pins how TradingClientParams size the
// process-wide CLOB token bucket. Zero values must keep the long-standing
// 8 requests/s with burst 4: production only changes them through
// POLYMARKET_REQUESTS_PER_SECOND / POLYMARKET_BURST.
func TestNewTradingClientRequestLimiter(t *testing.T) {
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewStaticCredentialProvider([]TradingAccount{{
		ExecutionAccountID: "account-1",
		FunderAddress:      signer.Address(),
		SignatureType:      SignatureTypeEOA,
		API: APICredentials{
			Key:        "api-key",
			Secret:     base64.URLEncoding.EncodeToString([]byte("test-secret")),
			Passphrase: "passphrase",
		},
		Signer: signer,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		rate         float64
		burst        int
		wantRate     float64
		wantCapacity float64
	}{
		{"defaults when unset", 0, 0, 8, 4},
		{"configured production values", 20, 10, 20, 10},
		{"rate only keeps the default burst", 20, 0, 20, 4},
		{"burst only keeps the default rate", 0, 10, 8, 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewTradingClient(TradingClientParams{
				BaseURL:           "https://clob.example.invalid",
				Credentials:       provider,
				RequestTimeout:    2 * time.Second,
				RequestsPerSecond: test.rate,
				Burst:             test.burst,
			})
			if err != nil {
				t.Fatal(err)
			}
			if client.limiter.rate != test.wantRate || client.limiter.capacity != test.wantCapacity {
				t.Fatalf("limiter = %v/s burst %v, want %v/s burst %v",
					client.limiter.rate, client.limiter.capacity, test.wantRate, test.wantCapacity)
			}
		})
	}
}

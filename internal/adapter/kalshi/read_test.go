package kalshi

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const testBalanceBody = `{"balance":1234,"portfolio_value":0}`

func readTestClient(t *testing.T, writeURL string, readURLs []string, params ClientParams) *Client {
	t.Helper()
	params.BaseURL, params.ReadBaseURLs, params.APIKeyID = writeURL, readURLs, "key"
	if params.PrivateKey == nil {
		params.PrivateKey = testPrivateKey(t)
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: time.Second}
	}
	client, err := NewClient(params)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func countingServer(t *testing.T, count *atomic.Int32, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		count.Add(1)
		handler(writer, request)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestReadFailsOverToNextHostWithSignedRequest(t *testing.T) {
	privateKey := testPrivateKey(t)
	var primaryHits, fallbackHits atomic.Int32
	primary := countingServer(t, &primaryHits, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	})
	fallback := countingServer(t, &fallbackHits, func(writer http.ResponseWriter, request *http.Request) {
		verifySignature(t, request, "key", &privateKey.PublicKey)
		_, _ = writer.Write([]byte(testBalanceBody))
	})
	client := readTestClient(t, "https://write.invalid", []string{primary.URL, fallback.URL}, ClientParams{PrivateKey: privateKey})
	balance, err := client.GetBalance(context.Background())
	if err != nil || balance.Balance != 1234 {
		t.Fatalf("GetBalance() = %#v, %v", balance, err)
	}
	if primaryHits.Load() != 1 || fallbackHits.Load() != 1 {
		t.Fatalf("hits primary=%d fallback=%d, want 1/1", primaryHits.Load(), fallbackHits.Load())
	}
}

func TestReadWithoutReadHostsUsesBaseURL(t *testing.T) {
	var hits atomic.Int32
	server := countingServer(t, &hits, func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(testBalanceBody))
	})
	client := readTestClient(t, server.URL, nil, ClientParams{})
	if _, err := client.GetBalance(context.Background()); err != nil || hits.Load() != 1 {
		t.Fatalf("GetBalance() error = %v, hits = %d", err, hits.Load())
	}
}

func TestReadDoesNotRetryRejection(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := countingServer(t, &primaryHits, func(writer http.ResponseWriter, _ *http.Request) {
		http.NotFound(writer, nil)
	})
	fallback := countingServer(t, &fallbackHits, func(writer http.ResponseWriter, _ *http.Request) {})
	client := readTestClient(t, "https://write.invalid", []string{primary.URL, fallback.URL}, ClientParams{})
	_, err := client.GetOrder(context.Background(), "order")
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Code != "KALSHI_ORDER_VISIBILITY_PENDING" {
		t.Fatalf("GetOrder() error = %v", err)
	}
	if primaryHits.Load() != 1 || fallbackHits.Load() != 0 {
		t.Fatalf("a 404 was retried: primary=%d fallback=%d", primaryHits.Load(), fallbackHits.Load())
	}
}

func TestWriteUsesBaseURLOnlyAndIsNeverRetried(t *testing.T) {
	var writeHits, readHits atomic.Int32
	write := countingServer(t, &writeHits, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	})
	read := countingServer(t, &readHits, func(writer http.ResponseWriter, _ *http.Request) {})
	client := readTestClient(t, write.URL, []string{read.URL}, ClientParams{LiveTradingEnabled: true})
	prepared, err := client.PrepareOrder(validKalshiIntent(domain.SideBuy, "YES"))
	if err != nil {
		t.Fatalf("PrepareOrder() error = %v", err)
	}
	_, err = client.SubmitPrepared(context.Background(), prepared)
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Kind != port.VenueErrorAmbiguous {
		t.Fatalf("SubmitPrepared() error = %v, want ambiguous", err)
	}
	if writeHits.Load() != 1 || readHits.Load() != 0 {
		t.Fatalf("write hits=%d read hits=%d, want 1/0", writeHits.Load(), readHits.Load())
	}
}

func TestReadRejectsCachedCopyOfLiveResource(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := countingServer(t, &primaryHits, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Cache", "Hit from cloudfront")
		writer.Header().Set("Age", "3")
		_, _ = writer.Write([]byte(`{"orderbook_fp":{"yes_dollars":[["0.10","1.00"]],"no_dollars":[]}}`))
	})
	fallback := countingServer(t, &fallbackHits, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Cache", "Miss from cloudfront")
		_, _ = writer.Write([]byte(`{"orderbook_fp":{"yes_dollars":[["0.50","1.00"]],"no_dollars":[]}}`))
	})
	client := readTestClient(t, "https://write.invalid", []string{primary.URL, fallback.URL}, ClientParams{})
	var payload orderBookEnvelope
	if err := client.doAuthenticated(context.Background(), http.MethodGet,
		"/trade-api/v2/markets/TEST/orderbook?depth=15", nil, &payload); err != nil {
		t.Fatalf("orderbook read error = %v", err)
	}
	if payload.OrderBook.Yes[0][0] != "0.50" || primaryHits.Load() != 1 || fallbackHits.Load() != 1 {
		t.Fatalf("payload = %#v, hits = %d/%d; the cached book must not be used",
			payload, primaryHits.Load(), fallbackHits.Load())
	}
}

func TestReadAcceptsCachedCopyOfListEndpoint(t *testing.T) {
	var hits atomic.Int32
	server := countingServer(t, &hits, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Cache", "Hit from cloudfront")
		writer.Header().Set("Age", "2")
		_, _ = writer.Write([]byte(`{"markets":[],"cursor":""}`))
	})
	client := readTestClient(t, "https://write.invalid", []string{server.URL}, ClientParams{})
	if _, err := client.CoreMarkets(context.Background(), []string{"TEST"}); err != nil || hits.Load() != 1 {
		t.Fatalf("CoreMarkets() error = %v, hits = %d", err, hits.Load())
	}
}

func TestReadIdleTimeoutStopsStalledBodyAndFailsOver(t *testing.T) {
	var fallbackHits, primaryHits atomic.Int32
	primary := countingServer(t, &primaryHits, func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(`{"balance":`))
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
	})
	fallback := countingServer(t, &fallbackHits, func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(testBalanceBody))
	})
	client := readTestClient(t, "https://write.invalid", []string{primary.URL, fallback.URL},
		ClientParams{ReadIdleTimeout: 100 * time.Millisecond, ReadAttemptTimeout: 5 * time.Second})
	started := time.Now()
	if _, err := client.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second || fallbackHits.Load() != 1 {
		t.Fatalf("stalled body took %s, fallback hits %d", elapsed, fallbackHits.Load())
	}
}

func TestReadKeepsSlowBodyThatMakesProgress(t *testing.T) {
	var hits atomic.Int32
	server := countingServer(t, &hits, func(writer http.ResponseWriter, _ *http.Request) {
		for _, chunk := range []string{`{"balance":`, `1234,`, `"portfolio_value":`, `0}`} {
			_, _ = writer.Write([]byte(chunk))
			writer.(http.Flusher).Flush()
			time.Sleep(60 * time.Millisecond)
		}
	})
	// The body takes ~240ms in total, longer than the idle timeout, but never
	// pauses longer than it.
	client := readTestClient(t, "https://write.invalid", []string{server.URL},
		ClientParams{ReadIdleTimeout: 150 * time.Millisecond, ReadAttemptTimeout: 5 * time.Second})
	balance, err := client.GetBalance(context.Background())
	if err != nil || balance.Balance != 1234 || hits.Load() != 1 {
		t.Fatalf("GetBalance() = %#v, %v, hits = %d", balance, err, hits.Load())
	}
}

func TestReadSharesCallerDeadlineWithFallback(t *testing.T) {
	var fallbackHits, primaryHits atomic.Int32
	primary := countingServer(t, &primaryHits, func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	})
	fallback := countingServer(t, &fallbackHits, func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(testBalanceBody))
	})
	// The idle timeout alone would spend the whole deadline on the primary.
	client := readTestClient(t, "https://write.invalid", []string{primary.URL, fallback.URL},
		ClientParams{ReadIdleTimeout: 10 * time.Second, ReadAttemptTimeout: 10 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	if _, err := client.GetBalance(ctx); err != nil || fallbackHits.Load() != 1 {
		t.Fatalf("GetBalance() error = %v, fallback hits = %d", err, fallbackHits.Load())
	}
}

func TestReadRetriesRateLimitAfterDelay(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	primary := countingServer(t, &primaryHits, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
	})
	fallback := countingServer(t, &fallbackHits, func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(testBalanceBody))
	})
	client := readTestClient(t, "https://write.invalid", []string{primary.URL, fallback.URL}, ClientParams{})
	started := time.Now()
	if _, err := client.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < minRateLimitRetryDelay || fallbackHits.Load() != 1 {
		t.Fatalf("rate-limited retry after %s, fallback hits %d", elapsed, fallbackHits.Load())
	}
}

func TestNewClientRejectsInvalidReadBaseURLs(t *testing.T) {
	for _, urls := range [][]string{{"not a url"}, {"https://a.example?x=1"}, {"https://a.example", "https://a.example"}} {
		_, err := NewClient(ClientParams{BaseURL: "https://write.example", ReadBaseURLs: urls,
			APIKeyID: "key", PrivateKey: testPrivateKey(t)})
		if err == nil {
			t.Fatalf("NewClient() accepted read URLs %q", urls)
		}
	}
}

func TestOrderBookCaptureLogsUnderlyingFailureCodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	client := readTestClient(t, server.URL, nil, ClientParams{Logger: logger})
	source, err := NewOrderBookSource(OrderBookParams{Client: client, MarketBudget: 200 * time.Millisecond, Logger: logger})
	if err != nil {
		t.Fatalf("NewOrderBookSource() error = %v", err)
	}
	target := domain.BookTarget{MarketSource: domain.MarketSourceKalshi, MarketID: "SLOW", ConditionID: "kalshi:SLOW",
		OutcomeIndex: 0, OutcomeID: "YES", TokenID: "kalshi:SLOW:YES"}
	started := time.Now()
	books, err := source.Capture(context.Background(), time.Now(), []domain.BookTarget{target})
	if err != nil || len(books) != 1 || books[0].ErrorCode != "KALSHI_MARKET_REQUEST_FAILED" {
		t.Fatalf("Capture() = %#v, %v", books, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("capture ignored the market budget: %s", elapsed)
	}
	if text := logs.String(); !strings.Contains(text, "Kalshi orderbook reads failed") ||
		!strings.Contains(text, `"market: KALSHI_TRANSPORT_FAILED":1`) || !strings.Contains(text, `"SLOW"`) {
		t.Fatalf("failure summary log = %s", text)
	}
}

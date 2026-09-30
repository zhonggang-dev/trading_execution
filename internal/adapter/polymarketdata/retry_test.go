package polymarketdata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type scriptedDataAPIResponse struct {
	status     int
	retryAfter string
	body       string
	err        error
}

type scriptedDataAPITransport struct {
	mu        sync.Mutex
	responses []scriptedDataAPIResponse
	calls     int
}

func (transport *scriptedDataAPITransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.calls++
	if len(transport.responses) == 0 {
		return nil, fmt.Errorf("unexpected Data API call")
	}
	response := transport.responses[0]
	transport.responses = transport.responses[1:]
	if response.err != nil {
		return nil, response.err
	}
	header := make(http.Header)
	if response.retryAfter != "" {
		header.Set("Retry-After", response.retryAfter)
	}
	return &http.Response{
		StatusCode: response.status, Header: header,
		Body: io.NopCloser(strings.NewReader(response.body)), Request: request,
	}, nil
}

func retryTestClient(t *testing.T, now time.Time, responses ...scriptedDataAPIResponse) (
	*PositionClient, *scriptedDataAPITransport, *[]time.Duration,
) {
	t.Helper()
	transport := &scriptedDataAPITransport{responses: responses}
	client, err := NewPositionClient(PositionClientParams{
		BaseURL: "https://data-api.example", HTTPClient: &http.Client{Transport: transport},
		RequestsPerSecond: 15, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	waits := make([]time.Duration, 0)
	client.sleep = func(_ context.Context, wait time.Duration) error {
		waits = append(waits, wait)
		return nil
	}
	client.jitter = func(time.Duration) time.Duration { return 0 }
	return client, transport, &waits
}

const emptyPositionsBody = `[]`

func TestPositionClientRetriesRateLimitHonoringRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		retryAfter string
		wantWait   time.Duration
	}{
		{name: "delta seconds", retryAfter: "1", wantWait: time.Second},
		{name: "HTTP date", retryAfter: now.Add(2 * time.Second).Format(http.TimeFormat), wantWait: 2 * time.Second},
		{name: "capped", retryAfter: "120", wantWait: 10 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, transport, waits := retryTestClient(t, now,
				scriptedDataAPIResponse{status: http.StatusTooManyRequests, retryAfter: test.retryAfter, body: "rate limited"},
				scriptedDataAPIResponse{status: http.StatusOK, body: emptyPositionsBody},
			)
			positions, err := client.ListExternalPositions(context.Background(), "0xabc")
			if err != nil || positions == nil || len(positions) != 0 {
				t.Fatalf("ListExternalPositions() = %#v, %v", positions, err)
			}
			if transport.calls != 2 || len(*waits) != 1 || (*waits)[0] != test.wantWait {
				t.Fatalf("calls = %d, waits = %v; want 2 calls and one %s wait", transport.calls, *waits, test.wantWait)
			}
		})
	}
}

func TestPositionClientRetriesTransientFailuresWithExponentialBackoff(t *testing.T) {
	client, transport, waits := retryTestClient(t, time.Now(),
		scriptedDataAPIResponse{status: http.StatusTooManyRequests},
		scriptedDataAPIResponse{status: http.StatusBadGateway},
		scriptedDataAPIResponse{err: errors.New("connection reset by peer")},
		scriptedDataAPIResponse{status: http.StatusOK, body: emptyPositionsBody},
	)
	if _, err := client.ListExternalPositions(context.Background(), "0xabc"); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	if transport.calls != 4 || fmt.Sprint(*waits) != fmt.Sprint(want) {
		t.Fatalf("calls = %d, waits = %v; want 4 calls and waits %v", transport.calls, *waits, want)
	}
}

func TestPositionClientReturnsTypedErrorAfterRetryLimit(t *testing.T) {
	client, transport, waits := retryTestClient(t, time.Now(),
		scriptedDataAPIResponse{status: http.StatusTooManyRequests, body: "slow down"},
		scriptedDataAPIResponse{status: http.StatusServiceUnavailable, body: "slow down"},
		scriptedDataAPIResponse{status: http.StatusGatewayTimeout, body: "slow down"},
		scriptedDataAPIResponse{status: http.StatusTooManyRequests, retryAfter: "1", body: "slow down"},
	)
	positions, err := client.ListExternalPositions(context.Background(), "0xabc")
	var statusErr *HTTPStatusError
	if positions != nil || !errors.As(err, &statusErr) || statusErr.Status != http.StatusTooManyRequests ||
		statusErr.RetryAfter != time.Second {
		t.Fatalf("ListExternalPositions() = %#v, %v; want typed 429 error", positions, err)
	}
	if err.Error() != "Data API positions HTTP 429: slow down" {
		t.Fatalf("error text = %q", err.Error())
	}
	if transport.calls != 4 || len(*waits) != 3 {
		t.Fatalf("calls = %d, waits = %v; want 4 calls and 3 waits", transport.calls, *waits)
	}
}

func TestPositionClientDoesNotRetryNonTransientStatus(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound} {
		client, transport, waits := retryTestClient(t, time.Now(),
			scriptedDataAPIResponse{status: status, body: "bad"},
		)
		_, err := client.ListExternalPositions(context.Background(), "0xabc")
		var statusErr *HTTPStatusError
		if !errors.As(err, &statusErr) || statusErr.Status != status || transport.calls != 1 || len(*waits) != 0 {
			t.Fatalf("status %d: err = %v, calls = %d, waits = %v", status, err, transport.calls, *waits)
		}
	}
}

func TestPositionClientStopsWhenWaitExceedsCallerDeadline(t *testing.T) {
	client, transport, waits := retryTestClient(t, time.Now(),
		scriptedDataAPIResponse{status: http.StatusTooManyRequests, retryAfter: "5", body: "later"},
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	_, err := client.ListExternalPositions(ctx, "0xabc")
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.Status != http.StatusTooManyRequests {
		t.Fatalf("err = %v, want typed 429 error", err)
	}
	if transport.calls != 1 || len(*waits) != 0 || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("calls = %d, waits = %v, elapsed = %s; want immediate failure", transport.calls, *waits, time.Since(started))
	}
}

func TestPositionClientDoesNotRetryCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		cancel()
		return nil, context.Canceled
	})
	client, err := NewPositionClient(PositionClientParams{
		BaseURL: "https://data-api.example", HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.sleep = func(context.Context, time.Duration) error {
		t.Fatal("cancelled request must not be retried")
		return nil
	}
	if positions, err := client.ListExternalPositions(ctx, "0xabc"); err == nil || positions != nil {
		t.Fatalf("ListExternalPositions() = %#v, %v; want cancellation error", positions, err)
	}
}

func TestPositionClientCancellationDuringBackoffStopsRetrying(t *testing.T) {
	client, transport, _ := retryTestClient(t, time.Now(),
		scriptedDataAPIResponse{status: http.StatusTooManyRequests, body: "slow down"},
	)
	ctx, cancel := context.WithCancel(context.Background())
	client.sleep = func(ctx context.Context, wait time.Duration) error {
		cancel()
		return sleepContext(ctx, wait)
	}
	_, err := client.ListExternalPositions(ctx, "0xabc")
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.Status != http.StatusTooManyRequests || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want typed 429 wrapping context cancellation", err)
	}
	if !strings.HasPrefix(err.Error(), "Data API positions HTTP 429: slow down") {
		t.Fatalf("error text = %q", err.Error())
	}
	if transport.calls != 1 {
		t.Fatalf("calls = %d, want no retry after cancellation", transport.calls)
	}
}

func TestPositionClientFallsBackToBackoffForInvalidRetryAfter(t *testing.T) {
	client, transport, waits := retryTestClient(t, time.Now(),
		scriptedDataAPIResponse{status: http.StatusTooManyRequests, retryAfter: "-5"},
		scriptedDataAPIResponse{status: http.StatusTooManyRequests, retryAfter: "soon"},
		scriptedDataAPIResponse{status: http.StatusOK, body: emptyPositionsBody},
	)
	if _, err := client.ListExternalPositions(context.Background(), "0xabc"); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{500 * time.Millisecond, time.Second}
	if transport.calls != 3 || fmt.Sprint(*waits) != fmt.Sprint(want) {
		t.Fatalf("calls = %d, waits = %v; want 3 calls and waits %v", transport.calls, *waits, want)
	}
}

func TestRedeemActivitiesRetryRateLimit(t *testing.T) {
	wallet := "0x1111111111111111111111111111111111111111"
	condition := "0x" + fmt.Sprintf("%064x", 2)
	client, transport, waits := retryTestClient(t, time.Now(),
		scriptedDataAPIResponse{status: http.StatusTooManyRequests},
		scriptedDataAPIResponse{status: http.StatusOK, body: `[]`},
	)
	values, err := client.ListRedeemActivities(context.Background(), wallet, condition, time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))
	if err != nil || len(values) != 0 {
		t.Fatalf("ListRedeemActivities() = %#v, %v", values, err)
	}
	if transport.calls != 2 || len(*waits) != 1 || (*waits)[0] != 500*time.Millisecond {
		t.Fatalf("calls = %d, waits = %v", transport.calls, *waits)
	}

	failing, _, _ := retryTestClient(t, time.Now(),
		scriptedDataAPIResponse{status: http.StatusBadRequest, body: "bad market"},
	)
	if _, err := failing.ListRedeemActivities(context.Background(), wallet, condition, time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)); err == nil ||
		err.Error() != "Data API redemption activity HTTP 400: bad market" {
		t.Fatalf("error = %v", err)
	}
}

func TestParseRetryAfterRejectsInvalidValues(t *testing.T) {
	now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	for _, value := range []string{"", "-1", "soon"} {
		if _, ok := parseRetryAfter(value, now); ok {
			t.Fatalf("parseRetryAfter(%q) accepted", value)
		}
	}
	if wait, ok := parseRetryAfter(now.Add(-time.Minute).Format(http.TimeFormat), now); !ok || wait != 0 {
		t.Fatalf("past HTTP date = %s, %v; want zero wait", wait, ok)
	}
}

func TestPositionClientRetriesEveryServerError(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusNotImplemented, http.StatusBadGateway} {
		client, transport, _ := retryTestClient(t, time.Now(),
			scriptedDataAPIResponse{status: status, body: "down"},
			scriptedDataAPIResponse{status: http.StatusOK, body: "[]"},
		)
		if _, err := client.ListExternalPositions(context.Background(), "0xabc"); err != nil || transport.calls != 2 {
			t.Fatalf("status %d: err = %v, calls = %d; want success after one retry", status, err, transport.calls)
		}
	}
}

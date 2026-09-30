package polymarketdata

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	maxDataAPIRetries    = 3
	maxDataAPIRetryAfter = 10 * time.Second
	baseDataAPIBackoff   = 500 * time.Millisecond
)

// HTTPStatusError reports a non-200 Data API response after retries, if any,
// were exhausted. A failed read must never be interpreted as an empty result.
type HTTPStatusError struct {
	Resource   string
	Status     int
	Body       string
	RetryAfter time.Duration
}

// Error keeps the historical "Data API <resource> HTTP <status>: <body>" text.
func (err *HTTPStatusError) Error() string {
	return fmt.Sprintf("Data API %s HTTP %d: %s", err.Resource, err.Status, err.Body)
}

func retryableDataAPIStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

// parseRetryAfter accepts delta-seconds or an HTTP date and caps the wait.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	var wait time.Duration
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(maxDataAPIRetryAfter/time.Second) {
			return maxDataAPIRetryAfter, true
		}
		wait = time.Duration(seconds) * time.Second
	} else if at, err := http.ParseTime(value); err == nil {
		wait = at.Sub(now)
	} else {
		return 0, false
	}
	return min(max(wait, 0), maxDataAPIRetryAfter), true
}

// dataAPIBackoff returns 0.5s, 1s, 2s plus up to 25% jitter.
func (client *PositionClient) dataAPIBackoff(retry int) time.Duration {
	delay := baseDataAPIBackoff << (retry - 1)
	return delay + client.jitter(delay/4)
}

func defaultJitter(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	return rand.N(limit)
}

func sleepContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// dataAPIRetry describes whether a failed page read may be retried and, when
// the server sent a usable Retry-After, how long to wait first.
type dataAPIRetry struct {
	retryable     bool
	retryAfter    time.Duration
	hasRetryAfter bool
}

// doWithRetry performs one Data API page read. HTTP 429/502/503/504 and
// transport failures are retried at most maxDataAPIRetries times within the
// caller's deadline; every attempt still passes the shared rate limiter.
func (client *PositionClient) doWithRetry(ctx context.Context, request *http.Request, resource string) ([]byte, error) {
	for retry := 1; ; retry++ {
		if err := client.waitForRequest(ctx); err != nil {
			return nil, err
		}
		body, decision, err := client.doOnce(request.Clone(ctx), resource)
		if err == nil {
			return body, nil
		}
		if !decision.retryable || retry > maxDataAPIRetries || ctx.Err() != nil {
			return nil, err
		}
		wait := decision.retryAfter
		if !decision.hasRetryAfter {
			wait = client.dataAPIBackoff(retry)
		}
		// Do not burn the caller's remaining budget on a wait that cannot finish.
		if deadline, ok := ctx.Deadline(); ok && wait >= time.Until(deadline) {
			return nil, err
		}
		if sleepErr := client.sleep(ctx, wait); sleepErr != nil {
			// Keep the historical error prefix while exposing the cancellation cause.
			return nil, fmt.Errorf("%w; retry aborted: %w", err, sleepErr)
		}
	}
}

func (client *PositionClient) doOnce(request *http.Request, resource string) ([]byte, dataAPIRetry, error) {
	response, err := client.httpClient.Do(request)
	if err != nil {
		// Caller cancellation or deadline is final; other transport failures are transient.
		retryable := request.Context().Err() == nil
		return nil, dataAPIRetry{retryable: retryable}, fmt.Errorf("query Data API %s: %w", resource, err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxPositionsResponseBytes+1))
	response.Body.Close()
	if readErr != nil {
		return nil, dataAPIRetry{}, fmt.Errorf("read Data API %s: %w", resource, readErr)
	}
	if len(body) > maxPositionsResponseBytes {
		return nil, dataAPIRetry{}, fmt.Errorf("Data API %s response is too large", resource)
	}
	if response.StatusCode == http.StatusOK {
		return body, dataAPIRetry{}, nil
	}
	statusErr := &HTTPStatusError{Resource: resource, Status: response.StatusCode, Body: strings.TrimSpace(string(body))}
	if !retryableDataAPIStatus(response.StatusCode) {
		return nil, dataAPIRetry{}, statusErr
	}
	decision := dataAPIRetry{retryable: true}
	if retryAfter, ok := parseRetryAfter(response.Header.Get("Retry-After"), client.now()); ok {
		statusErr.RetryAfter = retryAfter
		decision.retryAfter, decision.hasRetryAfter = retryAfter, true
	}
	return nil, decision, statusErr
}

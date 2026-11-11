package kalshi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

// readAttempts is one attempt on the primary read host and one on the next.
// Trading reads are latency sensitive, so a GET is never retried further.
const readAttempts = 2

const (
	minRateLimitRetryDelay = 250 * time.Millisecond
	maxRateLimitRetryDelay = 2 * time.Second
)

var errReadStalled = errors.New("Kalshi response made no progress within the read idle timeout")

// doRead sends one signed GET. An attempt that fails transiently (transport,
// body read, 408/429/5xx, or a CDN copy of a live resource) is retried once on
// the next read host while the caller's context is alive. GETs never change
// state, so a retry cannot duplicate an order.
func (client *Client) doRead(ctx context.Context, requestPath string, target any) error {
	var lastErr error
	for attempt := 0; attempt < readAttempts; attempt++ {
		if attempt > 0 {
			if err := waitBeforeReadRetry(ctx, lastErr); err != nil {
				return lastErr
			}
		}
		base := client.readBaseURLs[attempt%len(client.readBaseURLs)]
		payload, err := client.readOnce(ctx, base, requestPath, readAttempts-attempt)
		if err == nil {
			if err := json.Unmarshal(payload, target); err != nil {
				return kalshiResponseFailure(http.MethodGet, "KALSHI_INVALID_RESPONSE", "decode Kalshi response", err)
			}
			return nil
		}
		lastErr = err
		if attempt+1 >= readAttempts || ctx.Err() != nil || !retryableRead(err) {
			return err
		}
		client.logger.Info("Kalshi read attempt failed; retrying on the next read host",
			"host", base.Host, "path", requestPathOnly(requestPath), "attempt", attempt+1, "error_code", errorCode(err))
	}
	return lastErr
}

func (client *Client) readOnce(ctx context.Context, base *url.URL, requestPath string, attemptsLeft int) ([]byte, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, client.readAttemptBudget(ctx, attemptsLeft))
	defer cancel()
	attemptCtx, stall := context.WithCancelCause(attemptCtx)
	defer stall(nil)
	watchdog := time.AfterFunc(client.readIdleTimeout, func() { stall(errReadStalled) })
	defer watchdog.Stop()

	request, err := client.signedRequest(attemptCtx, base, http.MethodGet, requestPath, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.readHTTPClient.Do(request)
	if err != nil {
		return nil, kalshiTransportFailure(http.MethodGet, attemptFailure(attemptCtx, err))
	}
	defer response.Body.Close()
	watchdog.Reset(client.readIdleTimeout)
	body := &progressReader{reader: response.Body, watchdog: watchdog, idle: client.readIdleTimeout}
	payload, err := io.ReadAll(io.LimitReader(body, maxAuthenticatedResponseBytes+1))
	if err != nil {
		return nil, kalshiResponseFailure(http.MethodGet, "KALSHI_RESPONSE_READ_FAILED", "read Kalshi response",
			attemptFailure(attemptCtx, err))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, mapKalshiHTTPError(http.MethodGet, response.StatusCode, response.Header, payload)
	}
	if len(payload) > maxAuthenticatedResponseBytes {
		return nil, kalshiResponseFailure(
			http.MethodGet, "KALSHI_RESPONSE_TOO_LARGE",
			fmt.Sprintf("Kalshi response exceeds %d bytes", maxAuthenticatedResponseBytes),
			fmt.Errorf("response body exceeded limit"),
		)
	}
	if readRequiresLiveCopy(requestPath) && servedFromCache(response.Header) {
		return nil, kalshiUnavailableError("KALSHI_CACHED_RESPONSE",
			"Kalshi served a cached copy of a live read", fmt.Errorf("host %s", base.Host))
	}
	return payload, nil
}

// readAttemptBudget leaves the remaining attempts a fair share of the caller's
// deadline, so a stalled primary host cannot spend the time the fallback needs.
func (client *Client) readAttemptBudget(ctx context.Context, attemptsLeft int) time.Duration {
	budget := client.readAttemptTimeout
	if deadline, ok := ctx.Deadline(); ok && attemptsLeft > 1 {
		if share := time.Until(deadline) / time.Duration(attemptsLeft); share < budget {
			budget = share
		}
	}
	return budget
}

// attemptFailure names why the attempt's own context ended (stalled or out of
// time) instead of the bare "context canceled" the transport reports.
func attemptFailure(attemptCtx context.Context, err error) error {
	if attemptCtx.Err() == nil {
		return err
	}
	if cause := context.Cause(attemptCtx); cause != nil && !errors.Is(err, cause) {
		return fmt.Errorf("%w: %w", err, cause)
	}
	return err
}

func retryableRead(err error) bool {
	var venueError *port.VenueError
	return errors.As(err, &venueError) && venueError.Kind == port.VenueErrorUnavailable &&
		venueError.Code != "KALSHI_RESPONSE_TOO_LARGE"
}

// waitBeforeReadRetry spaces a retry after a rate limit only: Kalshi's read
// budget is per account, so the next host shares it. It returns an error when
// the wait would not fit in the caller's deadline.
func waitBeforeReadRetry(ctx context.Context, previous error) error {
	var venueError *port.VenueError
	if !errors.As(previous, &venueError) || venueError.Code != "KALSHI_RATE_LIMITED" {
		return ctx.Err()
	}
	delay := min(max(venueError.RetryAfter, minRateLimitRetryDelay), maxRateLimitRetryDelay)
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// readRequiresLiveCopy lists the reads whose CDN copy would be stale state:
// account data and the per-market metadata/orderbook pair that gates orders.
// Kalshi marks only list endpoints (/markets, /series) cacheable today; this
// guard keeps a future cache policy change from serving an old book.
func readRequiresLiveCopy(requestPath string) bool {
	path := requestPathOnly(requestPath)
	switch {
	case strings.HasPrefix(path, "/trade-api/v2/portfolio/"), path == "/trade-api/v2/api_keys":
		return true
	case path == "/trade-api/v2/markets/candlesticks":
		return false
	default:
		return strings.HasPrefix(path, "/trade-api/v2/markets/")
	}
}

func servedFromCache(header http.Header) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(header.Get("X-Cache"))), "hit") {
		return true
	}
	age, err := strconv.Atoi(strings.TrimSpace(header.Get("Age")))
	return err == nil && age > 0
}

func requestPathOnly(requestPath string) string {
	path, _, _ := strings.Cut(requestPath, "?")
	return path
}

func errorCode(err error) string {
	var venueError *port.VenueError
	if errors.As(err, &venueError) && venueError.Code != "" {
		return venueError.Code
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "CONTEXT_DONE"
	}
	return "UNCLASSIFIED"
}

// progressReader pushes the stall deadline back whenever body bytes arrive.
type progressReader struct {
	reader   io.Reader
	watchdog *time.Timer
	idle     time.Duration
}

func (reader *progressReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if count > 0 {
		reader.watchdog.Reset(reader.idle)
	}
	return count, err
}

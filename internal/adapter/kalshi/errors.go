package kalshi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

func kalshiInvalidError(code, message string, cause error) error {
	return &port.VenueError{Kind: port.VenueErrorInvalid, Code: code, Message: message, Cause: cause}
}

func kalshiUnavailableError(code, message string, cause error) error {
	return &port.VenueError{Kind: port.VenueErrorUnavailable, Code: code, Message: message, Cause: cause}
}

func kalshiAmbiguousError(code string, cause error) error {
	return &port.VenueError{
		Kind:    port.VenueErrorAmbiguous,
		Code:    code,
		Message: "Kalshi request outcome is unknown and must be reconciled before another state-changing call",
		Cause:   cause,
	}
}

func kalshiAmbiguousOrderError(code, orderID string, cause error) error {
	return &port.VenueError{
		Kind:         port.VenueErrorAmbiguous,
		Code:         code,
		Message:      "Kalshi request outcome is unknown and must be reconciled before another state-changing call",
		VenueOrderID: strings.TrimSpace(orderID),
		Cause:        cause,
	}
}

// mapKalshiHTTPError distinguishes an explicit HTTP rejection from a write
// whose outcome may be unknown. A received 4xx proves ordinary order
// validation/FOK failures were not accepted. A conflict remains ambiguous for
// a state-changing call unless its body explicitly proves that a FOK order had
// insufficient resting volume; request timeouts and server failures are also
// ambiguous.
func mapKalshiHTTPError(method string, status int, headers http.Header, body []byte) error {
	apiCode, message := kalshiErrorResponse(body)
	code := classifyKalshiError(status, apiCode, message)
	kind := port.VenueErrorRejected
	if kalshiMethodMayMutate(method) && (status == http.StatusRequestTimeout ||
		(status == http.StatusConflict && code != "KALSHI_FOK_NOT_FILLED") || status >= 500) {
		kind = port.VenueErrorAmbiguous
	} else if !kalshiMethodMayMutate(method) &&
		(status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500) {
		kind = port.VenueErrorUnavailable
	}
	return &port.VenueError{
		Kind:       kind,
		Code:       code,
		Message:    message,
		HTTPStatus: status,
		RetryAfter: parseKalshiRetryAfter(headers.Get("Retry-After")),
	}
}

func kalshiErrorResponse(body []byte) (string, string) {
	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   struct {
			Code    string          `json:"code"`
			Message string          `json:"message"`
			Details json.RawMessage `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		code := kalshiFirstNonEmpty(payload.Error.Code, payload.Code)
		message := kalshiFirstNonEmpty(payload.Error.Message, payload.Message)
		if message == "" && len(payload.Error.Details) > 0 && string(payload.Error.Details) != "null" {
			var detail string
			if err := json.Unmarshal(payload.Error.Details, &detail); err == nil {
				message = detail
			}
		}
		if message != "" || code != "" {
			return code, kalshiFirstNonEmpty(message, code)
		}
	}
	message := strings.TrimSpace(string(body))
	if len(message) > 512 {
		message = message[:512]
	}
	if message == "" {
		message = "Kalshi request failed"
	}
	return "", message
}

func classifyKalshiError(status int, apiCode, message string) string {
	combined := strings.ToLower(strings.TrimSpace(apiCode + " " + message))
	switch {
	case status == http.StatusTooManyRequests:
		return "KALSHI_RATE_LIMITED"
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "KALSHI_AUTH_FAILED"
	case isKalshiFOKNotFilled(combined):
		return "KALSHI_FOK_NOT_FILLED"
	case status == http.StatusConflict:
		return "KALSHI_ORDER_CONFLICT"
	case strings.Contains(combined, "available_balance_too_low") || strings.Contains(combined, "insufficient balance"):
		return "KALSHI_INSUFFICIENT_BALANCE"
	case strings.Contains(combined, "invalid_order_size"):
		return "KALSHI_INVALID_ORDER_SIZE"
	case strings.Contains(combined, "closed") || strings.Contains(combined, "too late to enter"):
		return "KALSHI_MARKET_NOT_ACCEPTING"
	case status == http.StatusNotFound:
		return "KALSHI_ORDER_NOT_FOUND"
	case status >= 500:
		return "KALSHI_SERVER_ERROR"
	case strings.TrimSpace(apiCode) != "":
		return "KALSHI_" + normalizeKalshiErrorCode(apiCode)
	default:
		return "KALSHI_REJECTED"
	}
}

func isKalshiFOKNotFilled(combined string) bool {
	if strings.Contains(combined, "fill_or_kill_failed") || strings.Contains(combined, "fill-or-kill-failed") {
		return true
	}
	hasFOKIdentity := strings.Contains(combined, "fill_or_kill") || strings.Contains(combined, "fill-or-kill") ||
		strings.Contains(combined, "fill or kill") || strings.Contains(combined, "fok")
	hasInsufficientDepthEvidence := strings.Contains(combined, "could not be filled") ||
		strings.Contains(combined, "cannot be filled") || strings.Contains(combined, "would not be filled") ||
		strings.Contains(combined, "not enough liquidity") || strings.Contains(combined, "insufficient resting volume")
	return hasFOKIdentity && hasInsufficientDepthEvidence
}

func normalizeKalshiErrorCode(value string) string {
	var builder strings.Builder
	lastUnderscore := false
	for _, character := range strings.ToUpper(strings.TrimSpace(value)) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			builder.WriteRune(character)
			lastUnderscore = false
		} else if !lastUnderscore && builder.Len() > 0 {
			builder.WriteByte('_')
			lastUnderscore = true
		}
		if builder.Len() >= 96 {
			break
		}
	}
	return strings.Trim(builder.String(), "_")
}

func parseKalshiRetryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func kalshiMethodMayMutate(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func kalshiResponseFailure(method, code, message string, cause error) error {
	if kalshiMethodMayMutate(method) {
		return kalshiAmbiguousError(code, cause)
	}
	return kalshiUnavailableError(code, message, cause)
}

func kalshiTransportFailure(method string, cause error) error {
	if kalshiMethodMayMutate(method) {
		return kalshiAmbiguousError("KALSHI_TRANSPORT_OUTCOME_UNKNOWN", cause)
	}
	return kalshiUnavailableError("KALSHI_TRANSPORT_FAILED", "request Kalshi API", cause)
}

func kalshiLocalFailure(code, message string, cause error) error {
	return kalshiInvalidError(code, message, fmt.Errorf("Kalshi request was not sent: %w", cause))
}

func kalshiFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

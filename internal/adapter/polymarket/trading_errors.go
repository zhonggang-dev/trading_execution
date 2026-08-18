package polymarket

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

// newInvalidError 创建并初始化 Invalid Error。
func newInvalidError(code, message string) error {
	return &port.VenueError{Kind: port.VenueErrorInvalid, Code: code, Message: message}
}

// mapHTTPError 将外部值映射为 HTTP 数据 Error。
func mapHTTPError(method string, status int, headers http.Header, body []byte) error {
	message := responseMessage(body)
	kind := port.VenueErrorRejected
	code := classifyCLOBError(status, message)
	if status >= 500 && (method == http.MethodPost || method == http.MethodDelete) {
		kind = port.VenueErrorAmbiguous
	} else if status >= 500 {
		kind = port.VenueErrorUnavailable
	}
	return &port.VenueError{
		Kind:       kind,
		Code:       code,
		Message:    message,
		HTTPStatus: status,
		RetryAfter: parseRetryAfter(headers.Get("Retry-After")),
	}
}

// responseMessage 解析并规范化 Message。
func responseMessage(body []byte) string {
	var payload struct {
		Error    any    `json:"error"`
		ErrorMsg string `json:"errorMsg"`
		Message  string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		if payload.ErrorMsg != "" {
			return payload.ErrorMsg
		}
		if payload.Message != "" {
			return payload.Message
		}
		switch value := payload.Error.(type) {
		case string:
			return value
		case map[string]any:
			if message, ok := value["message"].(string); ok {
				return message
			}
		}
	}
	text := strings.TrimSpace(string(body))
	if len(text) > 512 {
		text = text[:512]
	}
	if text == "" {
		return "CLOB request failed"
	}
	return text
}

// classifyCLOBError 分类 CLOB 数据 Error。
func classifyCLOBError(status int, message string) string {
	lower := strings.ToLower(message)
	switch {
	case status == http.StatusTooManyRequests:
		return "CLOB_RATE_LIMITED"
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "CLOB_AUTH_FAILED"
	case status == http.StatusNotFound:
		return "CLOB_ORDER_NOT_FOUND"
	case strings.Contains(lower, "signature"):
		return "CLOB_INVALID_SIGNATURE"
	case strings.Contains(lower, "balance") || strings.Contains(lower, "allowance"):
		return "CLOB_INSUFFICIENT_BALANCE_ALLOWANCE"
	case strings.Contains(lower, "precision") || strings.Contains(lower, "decimal") || strings.Contains(lower, "invalid amount"):
		return "CLOB_INVALID_PRECISION"
	case strings.Contains(lower, "minimum") || strings.Contains(lower, "min order"):
		return "CLOB_MIN_ORDER_SIZE"
	case strings.Contains(lower, "fok"):
		return "CLOB_FOK_NOT_FILLED"
	case strings.Contains(lower, "fak") || strings.Contains(lower, "no orders found to match"):
		return "CLOB_FAK_NO_MATCH"
	case strings.Contains(lower, "closed") || strings.Contains(lower, "accepting orders"):
		return "CLOB_MARKET_NOT_ACCEPTING"
	case status >= 500:
		return "CLOB_SERVER_ERROR"
	default:
		return "CLOB_REJECTED"
	}
}

// parseRetryAfter 解析 Retry After。
func parseRetryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// ambiguousTransportError 将写请求传输失败包装为结果不确定的交易所错误。
func ambiguousTransportError(code string, err error) error {
	return ambiguousVenueError(code, "", err)
}

// ambiguousVenueError 将可能已受理的交易所失败包装为结果不确定错误。
func ambiguousVenueError(code, venueOrderID string, err error) error {
	return &port.VenueError{
		Kind:         port.VenueErrorAmbiguous,
		Code:         code,
		Message:      "request outcome is unknown and must be reconciled before another state-changing call",
		VenueOrderID: strings.TrimSpace(venueOrderID),
		Cause:        fmt.Errorf("venue response: %w", err),
	}
}

package strategyhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// MaxOrderReportsPerRequest is the strategy service's per-request batch limit.
const MaxOrderReportsPerRequest = 500

// ReportOrders posts cumulative order-state reports. Delivery is idempotent on
// the strategy side ((decision_id, report_seq)), so a whole-request failure may
// simply be resent. Per-report rejections come back in the results and are not
// an error of the call itself.
func (client *Client) ReportOrders(ctx context.Context, reports []domain.StrategyOrderReport) ([]domain.StrategyOrderReportResult, error) {
	if len(reports) == 0 || len(reports) > MaxOrderReportsPerRequest {
		return nil, fmt.Errorf("order report batch must contain 1..%d reports", MaxOrderReportsPerRequest)
	}
	payload, err := json.Marshal(struct {
		SchemaVersion string                       `json:"schema_version"`
		Reports       []domain.StrategyOrderReport `json:"reports"`
	}{domain.StrategyOrderReportSchemaVersion, reports})
	if err != nil {
		return nil, fmt.Errorf("encode strategy order reports: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.reportURL.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build strategy order report request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	if client.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+client.bearerToken)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("post strategy order reports: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read strategy order report response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return nil, fmt.Errorf("strategy order report response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("strategy order report HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		Data struct {
			Results []struct {
				Index      int    `json:"index"`
				ReportSeq  int64  `json:"report_seq"`
				DecisionID string `json:"decision_id"`
				Outcome    string `json:"outcome"`
				Error      *struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			} `json:"results"`
		} `json:"data"`
	}
	// The ack carries audit fields we do not consume, so unknown fields are
	// tolerated here (unlike the decision response, which is a trading input).
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode strategy order report response: %w", err)
	}
	if len(envelope.Data.Results) != len(reports) {
		return nil, fmt.Errorf("strategy order report response has %d results for %d reports", len(envelope.Data.Results), len(reports))
	}
	results := make([]domain.StrategyOrderReportResult, len(reports))
	for _, item := range envelope.Data.Results {
		if item.Index < 0 || item.Index >= len(reports) {
			return nil, fmt.Errorf("strategy order report response index %d is out of range", item.Index)
		}
		result := domain.StrategyOrderReportResult{
			DecisionID: reports[item.Index].DecisionID, ReportSeq: reports[item.Index].ReportSeq, Outcome: item.Outcome,
		}
		if item.Error != nil {
			result.ErrorCode, result.ErrorText = item.Error.Code, item.Error.Message
		}
		results[item.Index] = result
	}
	for index, result := range results {
		if result.DecisionID == "" {
			return nil, fmt.Errorf("strategy order report response is missing result %d", index)
		}
	}
	return results, nil
}

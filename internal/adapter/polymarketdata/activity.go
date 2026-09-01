package polymarketdata

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

var _ port.RedeemActivitySource = (*PositionClient)(nil)

type wireActivity struct {
	WalletAddress   string `json:"proxyWallet"`
	Timestamp       int64  `json:"timestamp"`
	ConditionID     string `json:"conditionId"`
	Type            string `json:"type"`
	TransactionHash string `json:"transactionHash"`
}

// ListRedeemActivities discovers candidate transaction hashes. The Data API is
// never treated as accounting evidence; the caller must verify the exact
// finalized exact-adapter PositionsRedeemed event through Polygon RPC.
func (client *PositionClient) ListRedeemActivities(
	ctx context.Context,
	walletAddress, conditionID string,
	start time.Time,
) ([]domain.RedeemActivity, error) {
	wallet, err := normalizedActivityAddress(walletAddress)
	if err != nil {
		return nil, fmt.Errorf("activity wallet: %w", err)
	}
	condition, err := normalizedActivityHash(conditionID, "condition id")
	if err != nil {
		return nil, err
	}
	if start.IsZero() {
		return nil, fmt.Errorf("activity start time is required")
	}

	const pageSize = 500
	activities := make([]domain.RedeemActivity, 0)
	seen := make(map[string]struct{})
	for page := 0; page < 20; page++ {
		endpoint := client.baseURL.ResolveReference(&url.URL{Path: "/activity"})
		query := endpoint.Query()
		query.Set("user", wallet)
		query.Set("market", condition)
		query.Set("type", "REDEEM")
		query.Set("start", strconv.FormatInt(start.UTC().Unix(), 10))
		query.Set("sortBy", "TIMESTAMP")
		query.Set("sortDirection", "ASC")
		query.Set("limit", strconv.Itoa(pageSize))
		query.Set("offset", strconv.Itoa(page*pageSize))
		endpoint.RawQuery = query.Encode()

		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if requestErr != nil {
			return nil, requestErr
		}
		if waitErr := client.waitForRequest(ctx); waitErr != nil {
			return nil, waitErr
		}
		response, requestErr := client.httpClient.Do(request)
		if requestErr != nil {
			return nil, fmt.Errorf("query Data API redemption activity: %w", requestErr)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxPositionsResponseBytes+1))
		response.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read Data API redemption activity: %w", readErr)
		}
		if len(body) > maxPositionsResponseBytes {
			return nil, fmt.Errorf("Data API redemption activity response is too large")
		}
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Data API redemption activity HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
		}
		var values []wireActivity
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if decodeErr := decoder.Decode(&values); decodeErr != nil {
			if decodeErr := json.Unmarshal(body, &values); decodeErr != nil {
				return nil, fmt.Errorf("decode Data API redemption activity: %w", decodeErr)
			}
		}
		for index, value := range values {
			activityWallet, normalizeErr := normalizedActivityAddress(value.WalletAddress)
			if normalizeErr != nil || activityWallet != wallet {
				return nil, fmt.Errorf("Data API redemption activity %d has a mismatched wallet", index+page*pageSize)
			}
			activityCondition, normalizeErr := normalizedActivityHash(value.ConditionID, "activity condition id")
			if normalizeErr != nil || activityCondition != condition {
				return nil, fmt.Errorf("Data API redemption activity %d has a mismatched condition", index+page*pageSize)
			}
			transactionHash, normalizeErr := normalizedActivityHash(value.TransactionHash, "activity transaction hash")
			if normalizeErr != nil || !strings.EqualFold(strings.TrimSpace(value.Type), "REDEEM") || value.Timestamp < 0 {
				return nil, fmt.Errorf("Data API redemption activity %d has invalid identity", index+page*pageSize)
			}
			occurredAt := time.Unix(value.Timestamp, 0).UTC()
			if occurredAt.Before(start.UTC()) {
				return nil, fmt.Errorf("Data API redemption activity predates requested start")
			}
			key := transactionHash + ":" + condition
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			activities = append(activities, domain.RedeemActivity{
				WalletAddress: wallet, ConditionID: condition,
				TransactionHash: transactionHash, OccurredAt: occurredAt,
			})
		}
		if len(values) < pageSize {
			sort.Slice(activities, func(i, j int) bool {
				if activities[i].OccurredAt.Equal(activities[j].OccurredAt) {
					return activities[i].TransactionHash < activities[j].TransactionHash
				}
				return activities[i].OccurredAt.Before(activities[j].OccurredAt)
			})
			return activities, nil
		}
	}
	return nil, fmt.Errorf("Data API redemption activity pagination exceeded 20 pages")
}

func normalizedActivityAddress(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	raw := strings.TrimPrefix(value, "0x")
	if len(raw) != 40 {
		return "", fmt.Errorf("address must contain 20 bytes")
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return "", fmt.Errorf("address is not hexadecimal")
	}
	return "0x" + raw, nil
}

func normalizedActivityHash(value, label string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	raw := strings.TrimPrefix(value, "0x")
	if len(raw) != 64 {
		return "", fmt.Errorf("%s must contain 32 bytes", label)
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return "", fmt.Errorf("%s is not hexadecimal", label)
	}
	return "0x" + raw, nil
}

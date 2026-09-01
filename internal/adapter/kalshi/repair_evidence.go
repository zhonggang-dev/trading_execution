package kalshi

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshirepair"
)

// RepairEvidenceSource is deliberately read-only. It resolves the venue order
// through the immutable client_order_id and then scopes the fills query to the
// returned authoritative Kalshi order_id.
type RepairEvidenceSource struct{ client *Client }

func NewRepairEvidenceSource(client *Client) (*RepairEvidenceSource, error) {
	if client == nil {
		return nil, fmt.Errorf("Kalshi repair client is required")
	}
	return &RepairEvidenceSource{client: client}, nil
}

func (source *RepairEvidenceSource) Inspect(ctx context.Context, order domain.Order) (kalshirepair.Evidence, error) {
	clientOrderID := strings.TrimSpace(order.Intent.ClientOrderID)
	if clientOrderID == "" {
		return kalshirepair.Evidence{}, fmt.Errorf("local client_order_id is required")
	}
	listed, err := source.client.FindOrderByClientOrderID(ctx, clientOrderID)
	if err != nil {
		return kalshirepair.Evidence{}, fmt.Errorf("find Kalshi order by client_order_id: %w", err)
	}
	authoritativeID := strings.TrimSpace(listed.OrderID)
	if authoritativeID == "" {
		return kalshirepair.Evidence{}, fmt.Errorf("Kalshi order has no authoritative order_id")
	}
	// The list endpoint is used only to recover the authoritative order ID from
	// the immutable client_order_id. Fetch the exact order next because list
	// responses may omit execution-policy fields needed for a safe repair.
	remote, err := source.client.GetOrder(ctx, authoritativeID)
	if err != nil {
		return kalshirepair.Evidence{}, fmt.Errorf("get authoritative Kalshi order by order_id: %w", err)
	}
	if strings.TrimSpace(remote.OrderID) != authoritativeID ||
		strings.TrimSpace(remote.ClientOrderID) != clientOrderID ||
		strings.TrimSpace(remote.Ticker) != strings.TrimSpace(listed.Ticker) {
		return kalshirepair.Evidence{}, fmt.Errorf("Kalshi list/detail order identity does not match")
	}
	fills, err := source.client.ListFills(ctx, authoritativeID)
	if err != nil {
		return kalshirepair.Evidence{}, fmt.Errorf("list Kalshi fills by authoritative order_id: %w", err)
	}
	fillIDs := make([]string, 0, len(fills))
	seen := make(map[string]struct{}, len(fills))
	for _, fill := range fills {
		fillID := strings.TrimSpace(fill.FillID)
		if fillID == "" || strings.TrimSpace(fill.OrderID) != authoritativeID {
			return kalshirepair.Evidence{}, fmt.Errorf("Kalshi fill identity does not match the authoritative order")
		}
		if _, duplicate := seen[fillID]; duplicate {
			continue
		}
		seen[fillID] = struct{}{}
		fillIDs = append(fillIDs, fillID)
	}
	sort.Strings(fillIDs)
	return kalshirepair.Evidence{
		OrderID: authoritativeID, ClientOrderID: strings.TrimSpace(remote.ClientOrderID),
		MarketID: strings.TrimSpace(remote.Ticker), OutcomeSide: strings.TrimSpace(remote.OutcomeSide),
		Action: strings.TrimSpace(remote.Action), BookSide: strings.TrimSpace(remote.BookSide),
		OrderType: strings.TrimSpace(remote.Type), TimeInForce: strings.TrimSpace(remote.TimeInForce),
		OrderPrice: remote.YesPrice, SelfTradePolicy: strings.TrimSpace(remote.SelfTradePreventionType),
		CancelOnPause: remote.CancelOrderOnPause, SubaccountNumber: remote.SubaccountNumber,
		Status:    strings.TrimSpace(remote.Status),
		FillCount: remote.FillCount, RemainingCount: remote.RemainingCount, InitialCount: remote.InitialCount,
		FillIDs: fillIDs, LastUpdatedAt: remote.LastUpdateTime.UTC(), ObservedAt: source.client.now().UTC(),
		OrderQuerySource: "KALSHI_ORDER_BY_CLIENT_THEN_ORDER_ID", FillQuerySource: "KALSHI_FILLS_BY_ORDER_ID",
	}, nil
}

var _ kalshirepair.EvidenceSource = (*RepairEvidenceSource)(nil)

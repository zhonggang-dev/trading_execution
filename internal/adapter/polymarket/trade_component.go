package polymarket

import (
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// validateOrderTradeComponent prevents another maker in a shared trade from
// being booked into this account. Ownership of part of a trade is not authority
// to consume every component. Identity comes from the exact order component,
// which can have a different token and side from the top-level taker fields.
func validateOrderTradeComponent(trade Trade, order domain.Order, funder string) error {
	components, err := accountTradeComponents(trade, funder)
	if err != nil {
		return err
	}
	owned := false
	for _, component := range components {
		if strings.EqualFold(component.OrderID, strings.TrimSpace(order.VenueOrderID)) {
			owned = component.TokenID == strings.TrimSpace(order.Intent.TokenID)
			break
		}
	}
	if !owned || !strings.EqualFold(strings.TrimSpace(trade.Market), strings.TrimSpace(order.Intent.ConditionID)) {
		return fmt.Errorf("trade %s exact order component does not match account, market and token", trade.ID)
	}
	side := trade.Side
	if !strings.EqualFold(strings.TrimSpace(trade.TakerOrderID), strings.TrimSpace(order.VenueOrderID)) {
		for _, maker := range trade.MakerOrders {
			if strings.EqualFold(strings.TrimSpace(maker.OrderID), strings.TrimSpace(order.VenueOrderID)) {
				side = maker.Side
				break
			}
		}
	}
	if !strings.EqualFold(strings.TrimSpace(side), string(order.Intent.Side)) {
		return fmt.Errorf("trade %s exact order component side does not match", trade.ID)
	}
	return nil
}

package polymarket

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// NoFillCLOBReader is the read-only CLOB surface the no-fill evidence needs.
// It deliberately exposes no state-changing method: the operator tool and the
// service hold the trading client only through this interface.
type NoFillCLOBReader interface {
	Get(ctx context.Context, order domain.Order) (port.VenueOrder, error)
	ListTrades(ctx context.Context, executionAccountID string, filter TradeFilter) ([]Trade, error)
	ListOpenOrders(ctx context.Context, executionAccountID string, filter OpenOrderFilter) ([]OpenOrder, error)
}

var _ NoFillCLOBReader = (*TradingClient)(nil)

// NoFillEvidenceReader collects the venue part of the no-fill evidence.
type NoFillEvidenceReader struct {
	clob NoFillCLOBReader
	now  func() time.Time
}

// NewNoFillEvidenceReader wraps a read-only CLOB reader.
func NewNoFillEvidenceReader(clob NoFillCLOBReader, now func() time.Time) (*NoFillEvidenceReader, error) {
	if clob == nil {
		return nil, fmt.Errorf("CLOB reader is required")
	}
	if now == nil {
		now = time.Now
	}
	return &NoFillEvidenceReader{clob: clob, now: now}, nil
}

// ReadOrder reads the single order. NotFound is true only for the CLOB's
// HTTP 200 + null answer; a visible order returns NotFound=false and any
// other failure is an error.
func (reader *NoFillEvidenceReader) ReadOrder(ctx context.Context, order domain.Order) (domain.NoFillOrderRead, error) {
	_, err := reader.clob.Get(ctx, order)
	readAt := reader.now().UTC()
	if err == nil {
		return domain.NoFillOrderRead{NotFound: false, Code: "VISIBLE", ReadAt: readAt}, nil
	}
	var venueError *port.VenueError
	if errors.As(err, &venueError) && venueError.Kind == port.VenueErrorUnavailable &&
		venueError.Code == "CLOB_ORDER_NOT_FOUND" {
		return domain.NoFillOrderRead{NotFound: true, Code: venueError.Code, ReadAt: readAt}, nil
	}
	return domain.NoFillOrderRead{}, fmt.Errorf("read CLOB order: %w", err)
}

// TradeReferences lists every trade of the order's market for the account's
// wallet since after and reports each one that names the order hash as taker
// or maker order, whatever its status (MATCHED, MINED, RETRYING, FAILED,
// CONFIRMED). A pagination or decoding failure is an error.
func (reader *NoFillEvidenceReader) TradeReferences(ctx context.Context, order domain.Order, after time.Time) (domain.NoFillTradeScan, error) {
	market := strings.TrimSpace(order.Intent.ConditionID)
	orderHash := strings.TrimSpace(order.VenueOrderID)
	if market == "" || orderHash == "" {
		return domain.NoFillTradeScan{}, fmt.Errorf("trade evidence requires the order condition id and venue order id")
	}
	if after.IsZero() {
		return domain.NoFillTradeScan{}, fmt.Errorf("trade evidence requires a lower time bound")
	}
	trades, err := reader.clob.ListTrades(ctx, order.Intent.ExecutionAccountID, TradeFilter{Market: market, After: after.UTC().Unix()})
	if err != nil {
		return domain.NoFillTradeScan{}, fmt.Errorf("list CLOB trades: %w", err)
	}
	scan := domain.NoFillTradeScan{
		Market: market, After: after.UTC(), Scanned: len(trades),
		References: []domain.NoFillTradeReference{}, ReadAt: reader.now().UTC(),
	}
	for _, trade := range trades {
		if strings.EqualFold(strings.TrimSpace(trade.TakerOrderID), orderHash) {
			scan.References = append(scan.References, domain.NoFillTradeReference{
				TradeID: strings.TrimSpace(trade.ID), Status: strings.TrimSpace(trade.Status), Role: "TAKER",
			})
		}
		for _, maker := range trade.MakerOrders {
			if strings.EqualFold(strings.TrimSpace(maker.OrderID), orderHash) {
				scan.References = append(scan.References, domain.NoFillTradeReference{
					TradeID: strings.TrimSpace(trade.ID), Status: strings.TrimSpace(trade.Status), Role: "MAKER",
				})
			}
		}
	}
	return scan, nil
}

// OpenOrders lists the account's open orders of the order's market and token.
func (reader *NoFillEvidenceReader) OpenOrders(ctx context.Context, order domain.Order) (domain.NoFillOpenOrderScan, error) {
	market := strings.TrimSpace(order.Intent.ConditionID)
	token := strings.TrimSpace(order.Intent.TokenID)
	orderHash := strings.TrimSpace(order.VenueOrderID)
	if market == "" || token == "" || orderHash == "" {
		return domain.NoFillOpenOrderScan{}, fmt.Errorf("open-order evidence requires the order condition, token and venue order id")
	}
	orders, err := reader.clob.ListOpenOrders(ctx, order.Intent.ExecutionAccountID, OpenOrderFilter{Market: market, TokenID: token})
	if err != nil {
		return domain.NoFillOpenOrderScan{}, fmt.Errorf("list CLOB open orders: %w", err)
	}
	scan := domain.NoFillOpenOrderScan{Market: market, TokenID: token, Returned: len(orders), ReadAt: reader.now().UTC()}
	for _, open := range orders {
		if strings.EqualFold(strings.TrimSpace(open.ID), orderHash) {
			scan.Contains = true
		}
	}
	return scan, nil
}

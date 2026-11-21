package polymarket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type recordedFillsFunc func(context.Context, string) ([]domain.Fill, error)

func (f recordedFillsFunc) ListOrderFills(ctx context.Context, orderID string) ([]domain.Fill, error) {
	return f(ctx, orderID)
}

// Reproduces 2026-11-21: Polymarket cut a market's V2 rate from 0.05 to 0.01
// after a 6.22 @ 0.54 taker BUY had been accounted with fee 0.07725 at 0.05.
// Every later re-read of that partially filled, cancelled order failed with
// "OrderFilled platform fee 0.07725 does not match V2 fee curve 0.01545".
func rateChangeFixture(t *testing.T) (*TradingClient, domain.Order, *int) {
	t.Helper()
	now := time.Date(2026, 11, 21, 1, 0, 0, 0, time.UTC)
	order := adapterOrder()
	order.VenueOrderID = "0xvenue"
	order.Intent.ConditionID = "condition-1"
	order.Intent.Side = domain.SideBuy
	tokenID := order.Intent.TokenID
	scheduleReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/data/trades":
			writeTestJSON(writer, map[string]any{"data": []map[string]any{{
				"id": "trade-1", "taker_order_id": "0xvenue", "market": "condition-1",
				"asset_id": tokenID, "side": "BUY", "size": "6.22", "price": "0.54",
				"status": "CONFIRMED", "fee_rate_bps": "0", "transaction_hash": "0x" + strings.Repeat("ab", 32),
				"match_time": "2026-11-19T16:50:52Z", "last_update": "2026-11-19T16:50:59Z",
				"trader_side": "TAKER", "maker_address": request.URL.Query().Get("maker_address"),
			}}, "next_cursor": "LTE="})
		case "/clob-markets/condition-1":
			scheduleReads++
			writeTestJSON(writer, map[string]any{
				"c": "condition-1", "t": []map[string]any{{"t": tokenID}},
				"fd": map[string]any{"r": 0.01, "e": 1, "to": true},
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	client := newTestTradingClient(t, server.URL, now)
	client.feeEvidence = feeEvidenceSourceFunc(func(_ context.Context, request FillFeeEvidenceRequest) (FillFeeEvidence, error) {
		return FillFeeEvidence{
			Source: v2OrderFilledFeeSource, ExchangeAddress: request.ExpectedExchangeAddress,
			TransactionHash: request.TransactionHash, OrderHash: request.VenueOrderID,
			MakerAddress: request.ExpectedMakerAddress, TokenID: request.TokenID,
			BuilderCode: request.ExpectedBuilderCode, Side: request.Side,
			MakerAmountBaseUnits: "3358800", TakerAmountBaseUnits: "6220000", TotalFeeBaseUnits: "77250",
			BuilderFeeBaseUnits: "0", BuilderFeeKnown: true, CollateralDecimals: 6, OutcomeTokenDecimals: 6,
			BlockNumber: 100, BlockHash: "0x" + strings.Repeat("ef", 32), LogIndex: 3,
			Confirmations: 64, Finalized: true,
		}, nil
	})
	return client, order, &scheduleReads
}

func recordedFill(rate string) domain.Fill {
	return domain.Fill{
		VenueFillID: "trade-1", FeeSource: domain.FeeSourcePolygonV2OrderFilled,
		PlatformFeeRate: domain.Decimal(rate), FeeExponent: "1",
	}
}

func TestListOrderFillsWithoutRecordedFillUsesCurrentSchedule(t *testing.T) {
	client, order, _ := rateChangeFixture(t)
	_, err := client.ListOrderFills(context.Background(), order)
	if err == nil || !strings.Contains(err.Error(), "does not match V2 fee curve 0.01545") {
		t.Fatalf("ListOrderFills() error = %v, want current-curve mismatch for an unrecorded fill", err)
	}
}

func TestListOrderFillsRevalidatesRecordedFillWithItsRecordedSchedule(t *testing.T) {
	client, order, scheduleReads := rateChangeFixture(t)
	client.recorded = recordedFillsFunc(func(_ context.Context, orderID string) ([]domain.Fill, error) {
		if orderID != order.ID {
			t.Fatalf("recorded fills read for %q, want %q", orderID, order.ID)
		}
		return []domain.Fill{recordedFill("0.05")}, nil
	})
	fills, err := client.ListOrderFills(context.Background(), order)
	if err != nil || len(fills) != 1 {
		t.Fatalf("ListOrderFills() fills/error = %#v/%v", fills, err)
	}
	fill := fills[0]
	if !fill.PlatformFeeRate.Equal("0.05") || !fill.FeeExponent.Equal("1") || !fill.PlatformFee.Equal("0.07725") {
		t.Fatalf("re-observed fee identity = rate %s exp %s fee %s, want the recorded 0.05/1/0.07725",
			fill.PlatformFeeRate, fill.FeeExponent, fill.PlatformFee)
	}
	if *scheduleReads != 0 {
		t.Fatalf("current fee schedule read %d times for a fully recorded order", *scheduleReads)
	}
}

func TestListOrderFillsStillRejectsFeeThatMismatchesRecordedSchedule(t *testing.T) {
	client, order, _ := rateChangeFixture(t)
	client.recorded = recordedFillsFunc(func(context.Context, string) ([]domain.Fill, error) {
		return []domain.Fill{recordedFill("0.04")}, nil
	})
	if _, err := client.ListOrderFills(context.Background(), order); err == nil ||
		!strings.Contains(err.Error(), "does not match V2 fee curve") {
		t.Fatalf("ListOrderFills() error = %v, want the on-chain fee checked against the recorded curve", err)
	}
}

func TestListOrderFillsIgnoresRecordedFillsWithoutV2Evidence(t *testing.T) {
	client, order, scheduleReads := rateChangeFixture(t)
	client.recorded = recordedFillsFunc(func(context.Context, string) ([]domain.Fill, error) {
		legacy := recordedFill("0.05")
		legacy.FeeSource = "CLOB_TRADE"
		return []domain.Fill{legacy}, nil
	})
	if _, err := client.ListOrderFills(context.Background(), order); err == nil || *scheduleReads != 1 {
		t.Fatalf("legacy recorded fill changed the schedule: err=%v reads=%d", err, *scheduleReads)
	}
}

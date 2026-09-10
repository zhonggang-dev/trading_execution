package polymarket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// This reproduces the 2026-09-09 incident without production identities: a
// 100-share YES taker trade contains two distinct 48-share NO maker SELLs.
func complementaryMakerFixture(funder string) Trade {
	return Trade{
		ID: "paired-trade", TakerOrderID: "foreign-taker", Market: "condition-paired",
		TokenID: "token-yes", Side: "SELL", Size: "100", Price: "0.958",
		Status: "CONFIRMED", FeeRateBPS: "0", TraderSide: "MAKER",
		MakerAddress: "0x2222222222222222222222222222222222222222",
		MatchTime:    "2026-09-09T10:02:10Z", LastUpdate: "2026-09-09T10:02:19Z",
		TransactionHash: "0x" + strings.Repeat("cd", 32),
		MakerOrders: []MakerOrder{
			{OrderID: "owned-maker", TokenID: "token-no", MakerAddress: funder, Side: "SELL", MatchedAmount: "48", Price: "0.042", FeeRateBPS: "0"},
			{OrderID: "foreign-maker", TokenID: "token-no", MakerAddress: "0x3333333333333333333333333333333333333333", Side: "SELL", MatchedAmount: "48", Price: "0.042", FeeRateBPS: "0"},
		},
	}
}

func pairedMakerOrder() domain.Order {
	order := adapterOrder()
	order.VenueOrderID, order.Status = "owned-maker", domain.OrderStatusUnknown
	order.Intent.ConditionID, order.Intent.TokenID = "condition-paired", "token-no"
	order.Intent.Side, order.Intent.Size, order.Intent.Price, order.Intent.WorstPrice = domain.SideSell, "48", "0.042", "0.042"
	order.MarketValidation.TickSize = "0.001"
	return order
}

func TestComplementaryMakerFillRecoveryUsesExactComponent(t *testing.T) {
	now := time.Date(2026, 9, 10, 4, 0, 0, 0, time.UTC)
	order := pairedMakerOrder()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/data/trades":
			if r.URL.Query().Get("asset_id") != "" {
				// The real API filters on the taker token, hiding the NO maker.
				writeTestJSON(w, map[string]any{"data": []any{}, "next_cursor": "LTE="})
				return
			}
			if r.URL.Query().Get("market") != order.Intent.ConditionID || r.URL.Query().Get("maker_address") == "" {
				t.Error("recovery lost the market or wallet boundary")
			}
			trade := complementaryMakerFixture(r.URL.Query().Get("maker_address"))
			// Duplicate observations must produce one fill, not twice the money.
			writeTestJSON(w, map[string]any{"data": []Trade{trade, trade}, "next_cursor": "LTE="})
		case "/clob-markets/condition-paired":
			writeTestJSON(w, map[string]any{"c": "condition-paired", "t": []map[string]any{{"t": "token-yes"}, {"t": "token-no"}}})
		case "/data/order/owned-maker":
			writeTestJSON(w, map[string]any{"id": "owned-maker", "market": "condition-paired", "asset_id": "token-no", "side": "SELL", "status": "MATCHED", "original_size": "48000000", "size_matched": "48000000", "price": "0.042"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, now)
	evidenceCalls := 0
	client.feeEvidence = feeEvidenceSourceFunc(func(_ context.Context, request FillFeeEvidenceRequest) (FillFeeEvidence, error) {
		evidenceCalls++
		if request.VenueOrderID != order.VenueOrderID || request.TokenID != "token-no" || request.Side != domain.SideSell || !request.Shares.Equal("48") || !request.Price.Equal("0.042") {
			t.Fatalf("wrong component reached settlement reader: %#v", request)
		}
		return FillFeeEvidence{
			Source: v2OrderFilledFeeSource, ExchangeAddress: request.ExpectedExchangeAddress,
			TransactionHash: request.TransactionHash, OrderHash: request.VenueOrderID,
			MakerAddress: request.ExpectedMakerAddress, TokenID: request.TokenID,
			BuilderCode: request.ExpectedBuilderCode, Side: request.Side,
			MakerAmountBaseUnits: "48000000", TakerAmountBaseUnits: "2016000", TotalFeeBaseUnits: "0",
			BuilderFeeBaseUnits: "0", BuilderFeeKnown: true, CollateralDecimals: 6, OutcomeTokenDecimals: 6,
			BlockNumber: 100, BlockHash: "0x" + strings.Repeat("ef", 32), LogIndex: 3,
			Confirmations: 64, Finalized: true,
		}, nil
	})
	fills, err := client.ListOrderFills(context.Background(), order)
	if err != nil || len(fills) != 1 || evidenceCalls != 1 {
		t.Fatalf("fills=%#v error=%v evidence calls=%d", fills, err, evidenceCalls)
	}
	fill := fills[0]
	if fill.LiquidityRole != domain.LiquidityRoleMaker || fill.TokenID != "token-no" || !fill.Shares.Equal("48") || !fill.Price.Equal("0.042") || !fill.GrossNotional.Equal("2.016") || !fill.TotalFee.Equal("0") || fill.SettlementEvidence == nil {
		t.Fatalf("incorrect recovered money or identity: %#v", fill)
	}
	observed, err := client.Get(context.Background(), order)
	if err != nil || observed.State != port.VenueOrderFilled || !observed.AverageFillPrice.Equal("0.042") || len(observed.TradeIDs) != 1 {
		t.Fatalf("fallback exact average=%#v err=%v", observed, err)
	}
	client.feeEvidence = nil
	if _, err := client.ListOrderFills(context.Background(), order); err == nil {
		t.Fatal("CLOB confirmation alone was accepted without receipt evidence")
	}
}

func TestExactTradeComponentRejectsForeignOrMismatchedIdentity(t *testing.T) {
	const funder = "0x1111111111111111111111111111111111111111"
	for _, test := range []struct {
		name   string
		mutate func(*Trade, *domain.Order)
	}{
		{"foreign maker in otherwise owned trade", func(_ *Trade, o *domain.Order) { o.VenueOrderID = "foreign-maker" }},
		{"wrong token", func(tr *Trade, _ *domain.Order) { tr.MakerOrders[0].TokenID = "token-yes" }},
		{"wrong condition", func(tr *Trade, _ *domain.Order) { tr.Market = "another-condition" }},
		{"wrong side", func(tr *Trade, _ *domain.Order) { tr.MakerOrders[0].Side = "BUY" }},
		{"missing component token", func(tr *Trade, _ *domain.Order) { tr.MakerOrders[0].TokenID = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			trade, order := complementaryMakerFixture(funder), pairedMakerOrder()
			test.mutate(&trade, &order)
			if err := validateOrderTradeComponent(trade, order, funder); err == nil {
				t.Fatal("mismatched component accepted")
			}
		})
	}
}

func TestGetNullOrderIsRetryableMissingEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { writeTestJSON(w, nil) }))
	defer server.Close()
	client := newTestTradingClient(t, server.URL, time.Now().UTC())
	observed, err := client.Get(context.Background(), pairedMakerOrder())
	var venueError *port.VenueError
	if !errors.As(err, &venueError) || venueError.Code != "CLOB_ORDER_NOT_FOUND" || venueError.Kind != port.VenueErrorUnavailable {
		t.Fatalf("null order=%#v, err=%v; want retryable missing evidence", observed, err)
	}
	if observed.State == port.VenueOrderCancelled || observed.State == port.VenueOrderFilled {
		t.Fatalf("missing order inferred a terminal outcome: %#v", observed)
	}
}

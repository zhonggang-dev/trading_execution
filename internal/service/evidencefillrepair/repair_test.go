package evidencefillrepair

import (
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	wallet    = "0xdd9275d3b1d2c423e19724fcd09c19abb20aa167"
	orderHash = "0x19dc0ebefe874d4054655858afe5037f5198f9bbb6875e8ed1660991319f9a3b"
	txHash    = "0xdb250c945dd1dfd4ff4e329642aa0ce6af51ea7ba4b669d19e97a78b41356ffe"
	tokenID   = "21826735481216108293779753600846510014290875703486929222291367558516215723615"
)

func fixtureTarget() Target {
	return Target{
		Account: "wallet-7", OrderID: "ord-a01018db", VenueOrderID: orderHash, VenueFillID: "ebb37fc1-9332-4e40-9f5b-f908237399a7",
		Wallet: wallet, TokenID: tokenID, TransactionHash: txHash, BlockNumber: 94890899, LogIndex: 769,
		MatchedAt: time.Unix(1791041489, 0).UTC(), Shares: "56", GrossNotional: "9.9315", TotalFee: "0.020425",
		DisplayPrice: "0.1773482143", EffectiveFeeRate: "0.0025", FeeExponent: "1", ExpectedReservedBalance: "12.704384",
	}
}

func fixtureOrder() domain.Order {
	return domain.Order{
		ID: "ord-a01018db", VenueOrderID: orderHash, Status: domain.OrderStatusUnknown, FilledSize: "0",
		Intent: domain.OrderIntent{
			ExecutionAccountID: "wallet-7", Venue: "polymarket", Side: domain.SideBuy, TokenID: tokenID,
			MarketID: "666608", ConditionID: "0xc57bc9d0", Size: "56",
		},
	}
}

func fixtureEvidence() evmrpc.OrderFilledEvidence {
	return evmrpc.OrderFilledEvidence{
		TransactionHash: txHash, LogIndex: 769, BlockNumber: 94890899, BlockHash: "0x6d6741749bb193f6243ee4d116153334634f906e6edfd5f18e49d7d07128dd33",
		ExchangeAddress: "0xe111180000d2663c0091e4f400237545b87b996b", OrderHash: orderHash, Maker: wallet,
		Taker: "0xe111180000d2663c0091e4f400237545b87b996b", Side: evmrpc.OrderSideBuy, TokenID: tokenID,
		MakerAmountBaseUnits: "9931500", TakerAmountBaseUnits: "56000000", FeeBaseUnits: "20425",
		Builder: zeroBuilderCode, Confirmations: 4000,
	}
}

func TestBuildFillAcceptsTheReviewedReceiptAndPassesLedgerAccounting(t *testing.T) {
	fill, err := BuildFill(fixtureTarget(), fixtureOrder(), fixtureEvidence(), "0.01", time.Unix(1791300000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !fill.Shares.Equal("56") || !fill.GrossNotional.Equal("9.9315") || !fill.TotalFee.Equal("0.020425") || fill.FeeSource != domain.FeeSourcePolygonV2OrderFilled {
		t.Fatalf("fill = %#v", fill)
	}
	if fill.Key == "" || !fill.NetCashDelta.Equal("-9.951925") {
		t.Fatalf("fill is not ledger-ready: key=%q net=%s", fill.Key, fill.NetCashDelta)
	}
}

func TestBuildFillRejectsEveryMismatch(t *testing.T) {
	cases := map[string]func(*Target, *evmrpc.OrderFilledEvidence, *domain.Order){
		"shallow": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.Confirmations = 10 },
		"tx": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) {
			e.TransactionHash = "0x" + strings.Repeat("1", 64)
		},
		"order hash": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) {
			e.OrderHash = "0x" + strings.Repeat("2", 64)
		},
		"maker": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) {
			e.Maker = "0x" + strings.Repeat("3", 40)
		},
		"token":     func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.TokenID = "1" },
		"sell side": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.Side = evmrpc.OrderSideSell },
		"block":     func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.BlockNumber++ },
		"log index": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.LogIndex++ },
		"builder": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) {
			e.Builder = "0x" + strings.Repeat("1", 64)
		},
		"gross":         func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.MakerAmountBaseUnits = "9931501" },
		"shares":        func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.TakerAmountBaseUnits = "55000000" },
		"fee":           func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.FeeBaseUnits = "20426" },
		"partial order": func(_ *Target, _ *evmrpc.OrderFilledEvidence, o *domain.Order) { o.Intent.Size = "60" },
		"display price": func(t *Target, _ *evmrpc.OrderFilledEvidence, _ *domain.Order) { t.DisplayPrice = "0.25" },
		"non canonical": func(_ *Target, e *evmrpc.OrderFilledEvidence, _ *domain.Order) { e.FeeBaseUnits = "020425" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			target, order, evidence := fixtureTarget(), fixtureOrder(), fixtureEvidence()
			mutate(&target, &evidence, &order)
			if _, err := BuildFill(target, order, evidence, "0.01", time.Now()); err == nil {
				t.Fatal("mismatching receipt was accepted")
			}
		})
	}
}

func TestValidateOrderOnlyAcceptsTheFrozenNeverFilledBuy(t *testing.T) {
	reservation := domain.AssetReservation{
		OrderID: "ord-a01018db", Status: domain.ReservationStatusReconciliationRequired,
		RemainingReservedBalance: "12.704384", SettledNotional: "0",
	}
	if err := ValidateOrder(fixtureTarget(), fixtureOrder(), reservation); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*domain.Order, *domain.AssetReservation){
		"filled status":   func(o *domain.Order, _ *domain.AssetReservation) { o.Status = domain.OrderStatusFilled },
		"already filled":  func(o *domain.Order, _ *domain.AssetReservation) { o.FilledSize = "1" },
		"sell":            func(o *domain.Order, _ *domain.AssetReservation) { o.Intent.Side = domain.SideSell },
		"other account":   func(o *domain.Order, _ *domain.AssetReservation) { o.Intent.ExecutionAccountID = "wallet-6" },
		"other venue id":  func(o *domain.Order, _ *domain.AssetReservation) { o.VenueOrderID = "0x1" },
		"released":        func(_ *domain.Order, r *domain.AssetReservation) { r.Status = domain.ReservationStatusReleased },
		"changed reserve": func(_ *domain.Order, r *domain.AssetReservation) { r.RemainingReservedBalance = "1" },
		"already settled": func(_ *domain.Order, r *domain.AssetReservation) { r.SettledNotional = "1" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			order, r := fixtureOrder(), reservation
			mutate(&order, &r)
			if err := ValidateOrder(fixtureTarget(), order, r); err == nil {
				t.Fatal("unexpected order accepted")
			}
		})
	}
}

package postgres

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func TestVerifyFillIdentityCoversEconomicsAndCanonicalEvidence(t *testing.T) {
	base := fillIdentityTestValue()
	if err := verifyFillIdentity(base, cloneFillEvidence(base)); err != nil {
		t.Fatalf("identical fill rejected: %v", err)
	}

	tests := map[string]func(*domain.Fill){
		"condition":         func(fill *domain.Fill) { fill.ConditionID = "other" },
		"shares":            func(fill *domain.Fill) { fill.Shares = "2" },
		"price":             func(fill *domain.Fill) { fill.Price = "0.6" },
		"gross":             func(fill *domain.Fill) { fill.GrossNotional = "0.6" },
		"clob fee metadata": func(fill *domain.Fill) { fill.FeeRateBPS = "1" },
		"platform rate":     func(fill *domain.Fill) { fill.PlatformFeeRate = "0.26" },
		"fee exponent":      func(fill *domain.Fill) { fill.FeeExponent = "3" },
		"platform fee":      func(fill *domain.Fill) { fill.PlatformFee = "0.01" },
		"builder rate":      func(fill *domain.Fill) { fill.BuilderFeeRateBPS = "1" },
		"builder fee":       func(fill *domain.Fill) { fill.BuilderFee = "0.01" },
		"total fee":         func(fill *domain.Fill) { fill.TotalFee = "0.01" },
		"net cash":          func(fill *domain.Fill) { fill.NetCashDelta = "-0.51" },
		"fee source":        func(fill *domain.Fill) { fill.FeeSource = "OTHER" },
		"transaction":       func(fill *domain.Fill) { fill.TransactionHash = "0x" + strings.Repeat("99", 32) },
		"raw digest":        func(fill *domain.Fill) { fill.RawPayloadSHA256 = strings.Repeat("f", 64) },
		"matched time":      func(fill *domain.Fill) { fill.MatchedAt = fill.MatchedAt.Add(time.Second) },
		"canonical evidence": func(fill *domain.Fill) {
			fill.SettlementEvidence.BlockHash = "0x" + strings.Repeat("88", 32)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := cloneFillEvidence(base)
			mutate(&changed)
			if err := verifyFillIdentity(base, changed); !errors.Is(err, port.ErrFillConflict) {
				t.Fatalf("verifyFillIdentity() error = %v, want fill conflict", err)
			}
		})
	}
}

func TestVerifyFillIdentityIgnoresGrowingConfirmations(t *testing.T) {
	stored := fillIdentityTestValue()
	incoming := cloneFillEvidence(stored)
	incoming.SettlementEvidence.Confirmations++
	if err := verifyFillIdentity(stored, incoming); err != nil {
		t.Fatalf("confirmation-only replay rejected: %v", err)
	}
}

func TestCanonicalSettlementEvidenceNormalizesCase(t *testing.T) {
	base := fillIdentityTestValue().SettlementEvidence
	changed := *base
	changed.Source = " polygon_v2_order_filled "
	changed.ExchangeAddress = strings.ToUpper(changed.ExchangeAddress)
	left, err := canonicalSettlementEvidence(base)
	if err != nil {
		t.Fatal(err)
	}
	right, err := canonicalSettlementEvidence(&changed)
	if err != nil {
		t.Fatal(err)
	}
	if string(left) != string(right) {
		t.Fatalf("normalized evidence differs:\n%s\n%s", left, right)
	}
}

func fillIdentityTestValue() domain.Fill {
	matched := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	txHash := "0x" + strings.Repeat("cd", 32)
	orderHash := "0x" + strings.Repeat("22", 32)
	return domain.Fill{
		Key: "fill-1", Venue: "polymarket", VenueFillID: "trade-1",
		OrderID: "order-1", VenueOrderID: orderHash,
		ExecutionAccountID: "account-1", MarketID: "market-1",
		ConditionID: "condition-1", TokenID: "42", Side: domain.SideBuy,
		LiquidityRole: domain.LiquidityRoleMaker, Status: domain.FillStatusConfirmed,
		Shares: "1", Price: "0.5", GrossNotional: "0.5", FeeRateBPS: "0",
		PlatformFeeRate: "0.25", FeeExponent: "2", PlatformFee: "0",
		BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0", NetCashDelta: "-0.5",
		TransactionHash: txHash, MatchedAt: matched, ObservedAt: matched,
		FeeSource:        domain.FeeSourcePolygonV2OrderFilled,
		RawPayloadSHA256: strings.Repeat("a", 64),
		SettlementEvidence: &domain.SettlementEvidence{
			SchemaVersion:   domain.SettlementEvidenceSchemaV1,
			Source:          domain.FeeSourcePolygonV2OrderFilled,
			ChainID:         domain.SettlementEvidencePolygonChainID,
			ExchangeAddress: "0x" + strings.Repeat("ab", 20),
			TransactionHash: txHash, BlockNumber: 100,
			BlockHash: "0x" + strings.Repeat("ef", 32), LogIndex: 7, Confirmations: 64,
			OrderHash: orderHash, MakerAddress: "0x" + strings.Repeat("11", 20),
			TokenID: "42", Side: domain.SideBuy,
			MakerAmountBaseUnits: "500000", TakerAmountBaseUnits: "1000000",
			TotalFeeBaseUnits: "0", BuilderCode: "0x" + strings.Repeat("00", 32),
			BuilderFeeKnown: true, BuilderFeeBaseUnits: "0",
			BuilderFeeSource:   domain.SettlementEvidenceZeroBuilder,
			CollateralDecimals: 6, OutcomeTokenDecimals: 6,
		},
	}
}

func cloneFillEvidence(fill domain.Fill) domain.Fill {
	if fill.SettlementEvidence != nil {
		copy := *fill.SettlementEvidence
		fill.SettlementEvidence = &copy
	}
	return fill
}

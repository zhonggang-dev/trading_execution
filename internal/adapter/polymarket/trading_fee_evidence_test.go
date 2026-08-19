package polymarket

import (
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestApplyFillFeeEvidenceUsesActualEventFeeAndExponent(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleTaker)
	evidence := feeEvidence()
	result, err := applyFillFeeEvidence(
		fill,
		marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true},
		evidence,
		evidence.ExchangeAddress,
		evidence.MakerAddress,
		zeroBytes32,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.GrossNotional.Equal("50") || !result.PlatformFee.Equal("1.5625") ||
		!result.TotalFee.Equal("1.5625") || !result.PlatformFeeRate.Equal("0.25") || !result.FeeExponent.Equal("2") ||
		result.FeeSource != domain.FeeSourcePolygonV2OrderFilled {
		t.Fatalf("event accounting = %#v", result)
	}
}

func TestApplyFillFeeEvidenceDigestIgnoresGrowingConfirmations(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleTaker)
	schedule := marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true}
	firstEvidence := feeEvidence()
	first, err := applyFillFeeEvidence(fill, schedule, firstEvidence, firstEvidence.ExchangeAddress, firstEvidence.MakerAddress, zeroBytes32)
	if err != nil {
		t.Fatal(err)
	}
	secondEvidence := firstEvidence
	secondEvidence.Confirmations++
	second, err := applyFillFeeEvidence(fill, schedule, secondEvidence, secondEvidence.ExchangeAddress, secondEvidence.MakerAddress, zeroBytes32)
	if err != nil {
		t.Fatal(err)
	}
	if first.RawPayloadSHA256 != second.RawPayloadSHA256 {
		t.Fatalf("confirmation growth changed raw digest: %s != %s", first.RawPayloadSHA256, second.RawPayloadSHA256)
	}
}

func TestApplyFillFeeEvidenceRequiresAllocationEvenWhenFeeIsZero(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleMaker)
	evidence := feeEvidence()
	evidence.TotalFeeBaseUnits = "0"
	evidence.BuilderFeeKnown = false
	_, err := applyFillFeeEvidence(
		fill,
		marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true},
		evidence,
		evidence.ExchangeAddress,
		evidence.MakerAddress,
		zeroBytes32,
	)
	if err == nil {
		t.Fatal("zero fee without authoritative builder allocation was accepted")
	}

	evidence.BuilderFeeKnown = true
	result, err := applyFillFeeEvidence(
		fill,
		marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true},
		evidence,
		evidence.ExchangeAddress,
		evidence.MakerAddress,
		zeroBytes32,
	)
	if err != nil || !result.TotalFee.Equal("0") || result.FeeSource == "" {
		t.Fatalf("verified zero-fee evidence = %#v, %v", result, err)
	}
}

func TestApplyFillFeeEvidenceFailsClosedOnAmountOrFeeMismatch(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleTaker)
	schedule := marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true}
	evidence := feeEvidence()
	evidence.TakerAmountBaseUnits = "99999999"
	if _, err := applyFillFeeEvidence(fill, schedule, evidence, evidence.ExchangeAddress, evidence.MakerAddress, zeroBytes32); err == nil {
		t.Fatal("event shares inconsistent with CLOB shares were accepted")
	}
	evidence = feeEvidence()
	evidence.TotalFeeBaseUnits = "1562499"
	if _, err := applyFillFeeEvidence(fill, schedule, evidence, evidence.ExchangeAddress, evidence.MakerAddress, zeroBytes32); err == nil {
		t.Fatal("event fee inconsistent with V2 schedule was accepted")
	}
}

func TestApplyFillFeeEvidenceDoesNotGuessPositiveBuilderFee(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleMaker)
	evidence := feeEvidence()
	evidence.BuilderCode = "0x1234"
	evidence.TotalFeeBaseUnits = "1000"
	evidence.BuilderFeeBaseUnits = "1000"
	// BuilderFeeRateBPS intentionally absent: OrderFilled proves only total fee.
	if _, err := applyFillFeeEvidence(
		fill,
		marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true},
		evidence,
		evidence.ExchangeAddress,
		evidence.MakerAddress,
		"0x1234",
	); err == nil {
		t.Fatal("positive builder fee without authoritative builder rate was accepted")
	}
}

func feeEvidenceFill(role domain.LiquidityRole) domain.Fill {
	return domain.Fill{
		Venue: "polymarket", VenueFillID: "trade-1", VenueOrderID: "0xorder",
		TokenID: "7", Side: domain.SideBuy, LiquidityRole: role,
		Shares: "100", Price: "0.5", FeeRateBPS: "750", TransactionHash: "0xtx",
		RawPayloadSHA256: "clob-digest",
	}
}

func feeEvidence() FillFeeEvidence {
	return FillFeeEvidence{
		Source:               domain.FeeSourcePolygonV2OrderFilled,
		ExchangeAddress:      polygonExchangeV2,
		TransactionHash:      "0xtx",
		OrderHash:            "0xorder",
		MakerAddress:         "0xmaker",
		TokenID:              "7",
		BuilderCode:          zeroBytes32,
		Side:                 domain.SideBuy,
		MakerAmountBaseUnits: "50000000",
		TakerAmountBaseUnits: "100000000",
		TotalFeeBaseUnits:    "1562500",
		BuilderFeeBaseUnits:  "0",
		BuilderFeeKnown:      true,
		CollateralDecimals:   6,
		OutcomeTokenDecimals: 6,
		BlockNumber:          1,
		BlockHash:            "0xblock",
		Confirmations:        2,
	}
}

package polymarket

import (
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestValidateEventGrossAcceptsWallet6PriceBucketDifference(t *testing.T) {
	err := validateEventGross("30.147057", "0.34", "10.199999", "0.01", 6)
	if err != nil {
		t.Fatalf("wallet-6 finalized fill was rejected: %v", err)
	}
}

func TestValidateEventGrossRejectsMaterialMismatchWithDiagnostics(t *testing.T) {
	err := validateEventGross("30.147057", "0.34", "10", "0.01", 6)
	if err == nil {
		t.Fatal("material OrderFilled gross mismatch was accepted")
	}
	for _, evidence := range []string{
		"shares=30.147057", "price=0.34", "gross=10", "computed_gross=10.24999938",
		"difference=0.24999938", "tolerance=0.150736285", "tick_size=0.01",
	} {
		if !strings.Contains(err.Error(), evidence) {
			t.Fatalf("error %q omitted %q", err, evidence)
		}
	}
}

func TestApplyFillFeeEvidenceUsesActualEventFeeAndExponent(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleTaker)
	evidence := feeEvidence()
	result, err := applyFillFeeEvidence(
		fill,
		marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true},
		evidence,
		"0.01",
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

func TestApplyFillFeeEvidenceAcceptsProductionV2SellFeeQuantum(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleTaker)
	fill.Side = domain.SideSell
	fill.Shares = "5"
	fill.Price = "0.216"
	evidence := feeEvidence()
	evidence.Side = domain.SideSell
	evidence.MakerAmountBaseUnits = "5000000"
	evidence.TakerAmountBaseUnits = "1080000"
	evidence.TotalFeeBaseUnits = "33860"
	result, err := applyFillFeeEvidence(
		fill,
		marketFeeSchedule{Rate: "0.04", Exponent: "1", TakerOnly: true},
		evidence,
		"0.001",
		evidence.ExchangeAddress,
		evidence.MakerAddress,
		zeroBytes32,
	)
	if err != nil {
		t.Fatalf("production V2 sell fee was rejected: %v", err)
	}
	if !result.GrossNotional.Equal("1.08") || !result.PlatformFee.Equal("0.03386") ||
		!result.TotalFee.Equal("0.03386") {
		t.Fatalf("event accounting = %#v", result)
	}

	evidence.TotalFeeBaseUnits = "33870"
	if _, err := applyFillFeeEvidence(
		fill,
		marketFeeSchedule{Rate: "0.04", Exponent: "1", TakerOnly: true},
		evidence,
		"0.001",
		evidence.ExchangeAddress,
		evidence.MakerAddress,
		zeroBytes32,
	); err == nil {
		t.Fatal("half-up fee quantum was accepted instead of the protocol-truncated fee")
	}
}

func TestApplyFillFeeEvidenceDigestIgnoresGrowingConfirmations(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleTaker)
	schedule := marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true}
	firstEvidence := feeEvidence()
	first, err := applyFillFeeEvidence(fill, schedule, firstEvidence, "0.01", firstEvidence.ExchangeAddress, firstEvidence.MakerAddress, zeroBytes32)
	if err != nil {
		t.Fatal(err)
	}
	secondEvidence := firstEvidence
	secondEvidence.Confirmations++
	second, err := applyFillFeeEvidence(fill, schedule, secondEvidence, "0.01", secondEvidence.ExchangeAddress, secondEvidence.MakerAddress, zeroBytes32)
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
		"0.01",
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
		"0.01",
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
	if _, err := applyFillFeeEvidence(fill, schedule, evidence, "0.01", evidence.ExchangeAddress, evidence.MakerAddress, zeroBytes32); err == nil {
		t.Fatal("event shares inconsistent with CLOB shares were accepted")
	}
	evidence = feeEvidence()
	evidence.TotalFeeBaseUnits = "1562499"
	if _, err := applyFillFeeEvidence(fill, schedule, evidence, "0.01", evidence.ExchangeAddress, evidence.MakerAddress, zeroBytes32); err == nil {
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
		"0.01",
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
		Finalized:            true,
	}
}

// TestSettlementEvidenceDigestShapeIsStable pins the digest of a fixed
// evidence payload. Every persisted execution_fills.raw_payload_sha256 was
// computed with this shape; a change here would make the ledger reject the
// re-observation of every already applied fill as an identity conflict.
func TestSettlementEvidenceDigestShapeIsStable(t *testing.T) {
	evidence := feeEvidence()
	evidence.Confirmations = 999
	evidence.Finalized = false
	if digest := settlementEvidenceDigest("clob-digest", evidence); digest != settlementEvidenceGoldenDigest {
		t.Fatalf("settlement evidence digest shape changed: %s", digest)
	}
}

const settlementEvidenceGoldenDigest = "62d0d4b8874ecd5231938d0be88792c28da2e8fb93e9b21d72be4b8f318d2b0d"

func TestApplyFillFeeEvidenceMarksShallowReceiptAsFinalityPendingMined(t *testing.T) {
	fill := feeEvidenceFill(domain.LiquidityRoleTaker)
	fill.Status = domain.FillStatusConfirmed
	confirmedAt := fill.MatchedAt
	fill.ConfirmedAt = &confirmedAt
	schedule := marketFeeSchedule{Rate: "0.25", Exponent: "2", TakerOnly: true}
	finalized := feeEvidence()
	final, err := applyFillFeeEvidence(fill, schedule, finalized, "0.01", finalized.ExchangeAddress, finalized.MakerAddress, zeroBytes32)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != domain.FillStatusConfirmed || final.ConfirmedAt == nil {
		t.Fatalf("finalized evidence status = %s, want CONFIRMED with confirmed_at", final.Status)
	}

	shallow := finalized
	shallow.Finalized = false
	shallow.Confirmations = 1
	pending, err := applyFillFeeEvidence(fill, schedule, shallow, "0.01", shallow.ExchangeAddress, shallow.MakerAddress, zeroBytes32)
	if err != nil {
		t.Fatalf("shallow receipt must produce a finality-pending fill, got %v", err)
	}
	if pending.Status != domain.FillStatusMined || pending.ConfirmedAt != nil {
		t.Fatalf("shallow receipt fill = status %s confirmed_at %v, want MINED without confirmed_at", pending.Status, pending.ConfirmedAt)
	}
	if pending.SettlementEvidence == nil || pending.SettlementEvidence.Confirmations != 1 ||
		!pending.TotalFee.Equal(final.TotalFee) || !pending.GrossNotional.Equal(final.GrossNotional) {
		t.Fatalf("finality-pending fill lost exact money or evidence: %#v", pending)
	}
	if pending.RawPayloadSHA256 != final.RawPayloadSHA256 {
		t.Fatalf("finality-pending and finalized observations must share one identity digest: %s != %s", pending.RawPayloadSHA256, final.RawPayloadSHA256)
	}

	shallow.Confirmations = 0
	if _, err := applyFillFeeEvidence(fill, schedule, shallow, "0.01", shallow.ExchangeAddress, shallow.MakerAddress, zeroBytes32); err == nil {
		t.Fatal("evidence without any confirmation must fail closed")
	}
}

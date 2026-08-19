package domain

import "testing"

func TestSettlementEvidenceValidatesExactPolygonFill(t *testing.T) {
	fill := settlementEvidenceTestFill()
	evidence := settlementEvidenceTestValue()
	if err := evidence.ValidateAgainst(fill); err != nil {
		t.Fatalf("ValidateAgainst() error = %v", err)
	}
	digest, err := evidence.SHA256()
	if err != nil {
		t.Fatalf("SHA256() error = %v", err)
	}
	if len(digest) != 64 {
		t.Fatalf("SHA256() length = %d, want 64", len(digest))
	}
}

func TestSettlementEvidenceRejectsChangedProofOrMoney(t *testing.T) {
	fill := settlementEvidenceTestFill()
	tests := map[string]func(*SettlementEvidence){
		"wrong chain":        func(value *SettlementEvidence) { value.ChainID = 1 },
		"wrong order":        func(value *SettlementEvidence) { value.OrderHash = "0x" + repeatHex("33", 32) },
		"wrong fee":          func(value *SettlementEvidence) { value.TotalFeeBaseUnits = "1" },
		"unconfirmed":        func(value *SettlementEvidence) { value.Confirmations = 0 },
		"unknown allocation": func(value *SettlementEvidence) { value.BuilderFeeKnown = false },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := settlementEvidenceTestValue()
			mutate(&value)
			if err := value.ValidateAgainst(fill); err == nil {
				t.Fatal("ValidateAgainst() error = nil, want rejection")
			}
		})
	}
}

func TestSettlementEvidenceCanonicalDigestNormalizesIdentityCase(t *testing.T) {
	first := settlementEvidenceTestValue()
	second := first
	second.Source = " polygon_v2_order_filled "
	second.ExchangeAddress = "0x" + repeatHex("AB", 20)
	second.TransactionHash = "0x" + repeatHex("CD", 32)
	firstDigest, err := first.SHA256()
	if err != nil {
		t.Fatalf("first SHA256() error = %v", err)
	}
	secondDigest, err := second.SHA256()
	if err != nil {
		t.Fatalf("second SHA256() error = %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("normalized evidence digests differ: %s != %s", firstDigest, secondDigest)
	}
}

func TestSettlementEvidenceCanonicalDigestIgnoresGrowingConfirmations(t *testing.T) {
	first := settlementEvidenceTestValue()
	second := first
	second.Confirmations++
	firstDigest, err := first.SHA256()
	if err != nil {
		t.Fatalf("first SHA256() error = %v", err)
	}
	secondDigest, err := second.SHA256()
	if err != nil {
		t.Fatalf("second SHA256() error = %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("confirmation growth changed immutable digest: %s != %s", firstDigest, secondDigest)
	}
}

func settlementEvidenceTestFill() Fill {
	return Fill{
		VenueOrderID:    "0x" + repeatHex("22", 32),
		TokenID:         "42",
		Side:            SideBuy,
		Shares:          "100",
		GrossNotional:   "50",
		PlatformFee:     "2.0625",
		BuilderFee:      "0",
		TotalFee:        "2.0625",
		TransactionHash: "0x" + repeatHex("cd", 32),
		FeeSource:       FeeSourcePolygonV2OrderFilled,
	}
}

func settlementEvidenceTestValue() SettlementEvidence {
	return SettlementEvidence{
		SchemaVersion:        SettlementEvidenceSchemaV1,
		Source:               FeeSourcePolygonV2OrderFilled,
		ChainID:              SettlementEvidencePolygonChainID,
		ExchangeAddress:      "0x" + repeatHex("ab", 20),
		TransactionHash:      "0x" + repeatHex("cd", 32),
		BlockNumber:          123,
		BlockHash:            "0x" + repeatHex("ef", 32),
		LogIndex:             7,
		Confirmations:        64,
		OrderHash:            "0x" + repeatHex("22", 32),
		MakerAddress:         "0x" + repeatHex("11", 20),
		TokenID:              "42",
		Side:                 SideBuy,
		MakerAmountBaseUnits: "50000000",
		TakerAmountBaseUnits: "100000000",
		TotalFeeBaseUnits:    "2062500",
		BuilderCode:          settlementZeroBytes32,
		BuilderFeeKnown:      true,
		BuilderFeeBaseUnits:  "0",
		BuilderFeeSource:     SettlementEvidenceZeroBuilder,
		CollateralDecimals:   6,
		OutcomeTokenDecimals: 6,
	}
}

func repeatHex(pair string, count int) string {
	result := ""
	for range count {
		result += pair
	}
	return result
}

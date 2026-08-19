package main

import (
	"context"
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type stubOrderFilledReader struct {
	evidence evmrpc.OrderFilledEvidence
	err      error
	request  evmrpc.OrderFilledEvidenceRequest
}

func (reader *stubOrderFilledReader) Read(
	_ context.Context,
	request evmrpc.OrderFilledEvidenceRequest,
) (evmrpc.OrderFilledEvidence, error) {
	reader.request = request
	return reader.evidence, reader.err
}

func TestPolygonFillFeeEvidenceMapsFinalizedReceipt(t *testing.T) {
	reader := &stubOrderFilledReader{evidence: validOrderFilledEvidence()}
	source, err := newPolygonFillFeeEvidence(reader)
	if err != nil {
		t.Fatalf("newPolygonFillFeeEvidence() error = %v", err)
	}
	request := validFillFeeEvidenceRequest()
	evidence, err := source.ResolveFillFeeEvidence(context.Background(), request)
	if err != nil {
		t.Fatalf("ResolveFillFeeEvidence() error = %v", err)
	}
	if reader.request.OrderHash != request.VenueOrderID || reader.request.Side != evmrpc.OrderSideBuy {
		t.Fatalf("reader request = %#v", reader.request)
	}
	if evidence.Source != polygonOrderFilledFeeSource || evidence.TotalFeeBaseUnits != "123" ||
		!evidence.BuilderFeeKnown || evidence.BuilderFeeBaseUnits != "0" || evidence.Confirmations != 64 {
		t.Fatalf("evidence = %#v", evidence)
	}
}

func TestPolygonFillFeeEvidenceFailsClosed(t *testing.T) {
	if _, err := newPolygonFillFeeEvidence(nil); err == nil {
		t.Fatal("nil reader must fail")
	}
	upstream := errors.New("receipt unavailable")
	source, _ := newPolygonFillFeeEvidence(&stubOrderFilledReader{err: upstream})
	if _, err := source.ResolveFillFeeEvidence(context.Background(), validFillFeeEvidenceRequest()); !errors.Is(err, upstream) {
		t.Fatalf("ResolveFillFeeEvidence() error = %v", err)
	}

	for name, mutate := range map[string]func(*evmrpc.OrderFilledEvidence){
		"exchange": func(value *evmrpc.OrderFilledEvidence) {
			value.ExchangeAddress = "0xe2222d279d744050d28e00520010520000310f59"
		},
		"builder": func(value *evmrpc.OrderFilledEvidence) {
			value.Builder = "0x1000000000000000000000000000000000000000000000000000000000000000"
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := validOrderFilledEvidence()
			mutate(&value)
			source, _ := newPolygonFillFeeEvidence(&stubOrderFilledReader{evidence: value})
			if _, err := source.ResolveFillFeeEvidence(context.Background(), validFillFeeEvidenceRequest()); err == nil {
				t.Fatal("mismatched receipt must fail")
			}
		})
	}
}

func TestPolygonFillFeeEvidenceDoesNotGuessNonZeroBuilderAllocation(t *testing.T) {
	request := validFillFeeEvidenceRequest()
	request.ExpectedBuilderCode = "0x1000000000000000000000000000000000000000000000000000000000000000"
	value := validOrderFilledEvidence()
	value.Builder = request.ExpectedBuilderCode
	source, _ := newPolygonFillFeeEvidence(&stubOrderFilledReader{evidence: value})
	evidence, err := source.ResolveFillFeeEvidence(context.Background(), request)
	if err != nil {
		t.Fatalf("ResolveFillFeeEvidence() error = %v", err)
	}
	if evidence.BuilderFeeKnown {
		t.Fatal("non-zero builder allocation must remain unknown without an authoritative split")
	}
}

func validFillFeeEvidenceRequest() polymarket.FillFeeEvidenceRequest {
	return polymarket.FillFeeEvidenceRequest{
		TransactionHash:         "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		VenueOrderID:            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ExecutionAccountID:      "account-a",
		ExpectedExchangeAddress: evmrpc.PolygonCTFExchangeV2Address,
		ExpectedMakerAddress:    "0x1111111111111111111111111111111111111111",
		ExpectedBuilderCode:     zeroBuilderCode,
		Side:                    domain.SideBuy,
		TokenID:                 "123456789",
		Shares:                  "2",
		Price:                   "0.5",
	}
}

func validOrderFilledEvidence() evmrpc.OrderFilledEvidence {
	request := validFillFeeEvidenceRequest()
	return evmrpc.OrderFilledEvidence{
		TransactionHash: request.TransactionHash,
		LogIndex:        7, BlockNumber: 100, BlockHash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ExchangeAddress: evmrpc.PolygonCTFExchangeV2Address,
		OrderHash:       request.VenueOrderID, Maker: request.ExpectedMakerAddress,
		Side: evmrpc.OrderSideBuy, TokenID: request.TokenID,
		MakerAmountBaseUnits: "1000000", TakerAmountBaseUnits: "2000000",
		FeeBaseUnits: "123", Builder: zeroBuilderCode, Metadata: zeroBuilderCode,
		Confirmations: 64,
	}
}

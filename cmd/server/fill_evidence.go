package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	polygonOrderFilledFeeSource = "POLYGON_V2_ORDER_FILLED"
	zeroBuilderCode             = "0x0000000000000000000000000000000000000000000000000000000000000000"
)

// polygonFillFeeEvidence adapts the strict, read-only Polygon receipt reader
// to the CLOB fill source. It never signs or sends an RPC transaction.
type polygonFillFeeEvidence struct {
	reader orderFilledEvidenceReader
}

type orderFilledEvidenceReader interface {
	Read(context.Context, evmrpc.OrderFilledEvidenceRequest) (evmrpc.OrderFilledEvidence, error)
}

func newPolygonFillFeeEvidence(reader orderFilledEvidenceReader) (*polygonFillFeeEvidence, error) {
	if reader == nil {
		return nil, fmt.Errorf("Polygon OrderFilled evidence reader is required")
	}
	return &polygonFillFeeEvidence{reader: reader}, nil
}

func (source *polygonFillFeeEvidence) ResolveFillFeeEvidence(
	ctx context.Context,
	request polymarket.FillFeeEvidenceRequest,
) (polymarket.FillFeeEvidence, error) {
	if strings.TrimSpace(request.ExecutionAccountID) == "" {
		return polymarket.FillFeeEvidence{}, fmt.Errorf("execution account id is required for fee evidence")
	}
	var side evmrpc.OrderSide
	switch request.Side {
	case domain.SideBuy:
		side = evmrpc.OrderSideBuy
	case domain.SideSell:
		side = evmrpc.OrderSideSell
	default:
		return polymarket.FillFeeEvidence{}, fmt.Errorf("unsupported fill side %q", request.Side)
	}
	evidence, err := source.reader.Read(ctx, evmrpc.OrderFilledEvidenceRequest{
		TransactionHash: request.TransactionHash,
		OrderHash:       request.VenueOrderID,
		Maker:           request.ExpectedMakerAddress,
		Side:            side,
		TokenID:         request.TokenID,
	})
	finalized := true
	var shallow *evmrpc.InsufficientConfirmationsError
	if errors.As(err, &shallow) {
		// The receipt is canonical and matches exactly; only the confirmation
		// depth is short. Hand the observed evidence back as finality-pending so
		// the fill ledger can hold the reservation without recording an outage.
		evidence, finalized = shallow.Evidence, false
	} else if err != nil {
		return polymarket.FillFeeEvidence{}, err
	}
	if evidence.Confirmations == 0 || evidence.BlockNumber == 0 {
		return polymarket.FillFeeEvidence{}, fmt.Errorf("OrderFilled evidence omitted receipt depth")
	}
	if !strings.EqualFold(evidence.ExchangeAddress, request.ExpectedExchangeAddress) {
		return polymarket.FillFeeEvidence{}, fmt.Errorf("OrderFilled exchange address does not match the validated market")
	}
	if !strings.EqualFold(evidence.Builder, request.ExpectedBuilderCode) {
		return polymarket.FillFeeEvidence{}, fmt.Errorf("OrderFilled builder code does not match the signed order")
	}

	// OrderFilled proves only the total fee. With the zero builder code the
	// builder allocation is authoritatively zero; non-zero builders remain
	// fail-closed until a separate, documented allocation source is installed.
	builderFeeKnown := strings.EqualFold(evidence.Builder, zeroBuilderCode)
	return polymarket.FillFeeEvidence{
		Source:               polygonOrderFilledFeeSource,
		ExchangeAddress:      evidence.ExchangeAddress,
		TransactionHash:      evidence.TransactionHash,
		OrderHash:            evidence.OrderHash,
		MakerAddress:         evidence.Maker,
		TokenID:              evidence.TokenID,
		BuilderCode:          evidence.Builder,
		Side:                 request.Side,
		MakerAmountBaseUnits: evidence.MakerAmountBaseUnits,
		TakerAmountBaseUnits: evidence.TakerAmountBaseUnits,
		TotalFeeBaseUnits:    evidence.FeeBaseUnits,
		BuilderFeeBaseUnits:  "0",
		BuilderFeeKnown:      builderFeeKnown,
		CollateralDecimals:   6,
		OutcomeTokenDecimals: 6,
		BlockNumber:          evidence.BlockNumber,
		BlockHash:            evidence.BlockHash,
		LogIndex:             evidence.LogIndex,
		Confirmations:        evidence.Confirmations,
		Finalized:            finalized,
	}, nil
}

var _ polymarket.FillFeeEvidenceSource = (*polygonFillFeeEvidence)(nil)

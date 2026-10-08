// Package evidencefillrepair is an operator-only, evidence-gated maintenance
// path. It is deliberately not wired into the server or an automatic scanner.
//
// It records ONE reviewed Polymarket fill whose exact amounts and total fee are
// proven by a finalized Polygon V2 OrderFilled receipt, but whose fee cannot be
// derived from the venue's current fee schedule (so the automatic fill
// synchronizer fails closed forever). Every identity and amount of the target
// is pinned in a manifest and compared with the receipt before anything is
// recorded; the fill then goes through the normal FillLedger.
package evidencefillrepair

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
)

// MinConfirmations is the receipt depth required before a repair may apply.
const MinConfirmations = 128

const zeroBuilderCode = "0x0000000000000000000000000000000000000000000000000000000000000000"

// Target pins every reviewed value of the one fill to record.
type Target struct {
	Account         string    `json:"account"`
	OrderID         string    `json:"order_id"`
	VenueOrderID    string    `json:"venue_order_id"`
	VenueFillID     string    `json:"venue_fill_id"`
	Wallet          string    `json:"wallet"`
	TokenID         string    `json:"token_id"`
	TransactionHash string    `json:"transaction_hash"`
	BlockNumber     uint64    `json:"block_number"`
	LogIndex        uint64    `json:"log_index"`
	MatchedAt       time.Time `json:"matched_at"`
	Shares          string    `json:"shares"`
	GrossNotional   string    `json:"gross_notional"`
	TotalFee        string    `json:"total_fee"`
	// DisplayPrice is the CLOB-reported average price. The exact price is
	// gross/shares, which may not be a finite decimal.
	DisplayPrice string `json:"display_price"`
	// EffectiveFeeRate and FeeExponent are DERIVED from the receipt fee, not
	// read from the venue schedule: the schedule cannot explain this fill.
	EffectiveFeeRate string `json:"effective_fee_rate"`
	FeeExponent      string `json:"fee_exponent"`
	// ExpectedReservedBalance pins the BUY reservation that is still frozen.
	ExpectedReservedBalance string `json:"expected_reserved_balance"`
}

// Fingerprint is the SHA-256 of the canonical target JSON.
func (t Target) Fingerprint() string {
	data, _ := json.Marshal(t)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// EvidenceRequest returns the receipt lookup that must prove the target.
func (t Target) EvidenceRequest() evmrpc.OrderFilledEvidenceRequest {
	return evmrpc.OrderFilledEvidenceRequest{
		TransactionHash: t.TransactionHash, OrderHash: t.VenueOrderID, Maker: t.Wallet,
		Side: evmrpc.OrderSideBuy, TokenID: t.TokenID,
	}
}

// ValidateOrder checks the local order and its frozen reservation against the
// pinned target. Only a never-filled BUY stuck in UNKNOWN is accepted.
func ValidateOrder(t Target, order domain.Order, reservation domain.AssetReservation) error {
	if order.ID != t.OrderID || order.VenueOrderID != t.VenueOrderID || order.Intent.ExecutionAccountID != t.Account ||
		order.Intent.TokenID != t.TokenID || order.Intent.Venue != "polymarket" || order.Intent.Side != domain.SideBuy {
		return fmt.Errorf("order identity does not match the reviewed target")
	}
	if order.Status != domain.OrderStatusUnknown || (!order.FilledSize.IsEmpty() && !order.FilledSize.Equal("0")) {
		return fmt.Errorf("order must be UNKNOWN with no filled size (status=%s filled=%s)", order.Status, order.FilledSize)
	}
	if reservation.OrderID != order.ID || reservation.Status != domain.ReservationStatusReconciliationRequired ||
		!reservation.RemainingReservedBalance.Equal(domain.Decimal(t.ExpectedReservedBalance)) ||
		!reservation.SettledNotional.Equal("0") {
		return fmt.Errorf("reservation is not the reviewed frozen, unsettled BUY reservation")
	}
	return nil
}

// BuildFill compares the receipt with every pinned value and returns a
// ledger-ready fill (key and exact net cash delta included). It is meant for
// FillLedger.Record directly: the fill processor would additionally force the
// fee onto the venue's five-decimal curve, which is exactly what this receipt
// cannot satisfy. The receipt itself is the authority for the fee.
// now is the observation/confirmation time.
func BuildFill(t Target, order domain.Order, evidence evmrpc.OrderFilledEvidence, tickSize domain.Decimal, now time.Time) (domain.Fill, error) {
	mismatch := func(what string) (domain.Fill, error) {
		return domain.Fill{}, fmt.Errorf("receipt does not match the reviewed target: %s", what)
	}
	if evidence.Confirmations < MinConfirmations {
		return domain.Fill{}, fmt.Errorf("receipt has %d confirmations, need %d", evidence.Confirmations, MinConfirmations)
	}
	if !strings.EqualFold(evidence.TransactionHash, t.TransactionHash) || !strings.EqualFold(evidence.OrderHash, t.VenueOrderID) ||
		!strings.EqualFold(evidence.Maker, t.Wallet) || evidence.TokenID != t.TokenID || evidence.Side != evmrpc.OrderSideBuy {
		return mismatch("identity")
	}
	if evidence.BlockNumber != t.BlockNumber || evidence.LogIndex != t.LogIndex {
		return mismatch("block/log position")
	}
	if !strings.EqualFold(evidence.Builder, zeroBuilderCode) {
		return mismatch("builder code is not zero")
	}
	gross, err := baseUnits(evidence.MakerAmountBaseUnits)
	if err != nil {
		return domain.Fill{}, err
	}
	shares, err := baseUnits(evidence.TakerAmountBaseUnits)
	if err != nil {
		return domain.Fill{}, err
	}
	fee, err := baseUnits(evidence.FeeBaseUnits)
	if err != nil {
		return domain.Fill{}, err
	}
	if !gross.Equal(domain.Decimal(t.GrossNotional)) || !shares.Equal(domain.Decimal(t.Shares)) || !fee.Equal(domain.Decimal(t.TotalFee)) {
		return mismatch("amounts or fee")
	}
	if !shares.Equal(order.Intent.Size) {
		return mismatch("shares differ from the order size; this repair only covers a complete fill")
	}
	sharesRat, _ := new(big.Rat).SetString(shares.String())
	grossRat, _ := new(big.Rat).SetString(gross.String())
	display, ok := new(big.Rat).SetString(t.DisplayPrice)
	if !ok || sharesRat.Sign() <= 0 {
		return mismatch("display price")
	}
	// The display price must reproduce the receipt gross within half a tick.
	if diff := new(big.Rat).Sub(new(big.Rat).Mul(sharesRat, display), grossRat); diff.Abs(diff).Cmp(
		new(big.Rat).Mul(sharesRat, new(big.Rat).Quo(ratOf(tickSize.String()), big.NewRat(2, 1)))) > 0 {
		return mismatch("display price is inconsistent with gross/shares")
	}
	confirmedAt := now.UTC()
	fill := domain.Fill{
		Venue: "polymarket", VenueFillID: t.VenueFillID, OrderID: order.ID, VenueOrderID: order.VenueOrderID,
		ExecutionAccountID: order.Intent.ExecutionAccountID, MarketID: order.Intent.MarketID,
		ConditionID: order.Intent.ConditionID, TokenID: order.Intent.TokenID, Side: domain.SideBuy,
		LiquidityRole: domain.LiquidityRoleTaker, Status: domain.FillStatusConfirmed,
		Shares: shares, Price: domain.Decimal(t.DisplayPrice), PriceTickSize: tickSize, GrossNotional: gross,
		FeeRateBPS: "0", PlatformFeeRate: domain.Decimal(t.EffectiveFeeRate), FeeExponent: domain.Decimal(t.FeeExponent),
		PlatformFee: fee, BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: fee,
		TransactionHash: t.TransactionHash, MatchedAt: t.MatchedAt.UTC(), VenueUpdatedAt: t.MatchedAt.UTC(),
		ObservedAt: confirmedAt, ConfirmedAt: &confirmedAt, FeeSource: domain.FeeSourcePolygonV2OrderFilled,
		SettlementEvidence: &domain.SettlementEvidence{
			SchemaVersion: domain.SettlementEvidenceSchemaV1, Source: domain.FeeSourcePolygonV2OrderFilled,
			ChainID: domain.SettlementEvidencePolygonChainID, ExchangeAddress: evidence.ExchangeAddress,
			TransactionHash: evidence.TransactionHash, BlockNumber: evidence.BlockNumber, BlockHash: evidence.BlockHash,
			LogIndex: evidence.LogIndex, Confirmations: evidence.Confirmations, OrderHash: evidence.OrderHash,
			MakerAddress: evidence.Maker, TokenID: evidence.TokenID, Side: domain.SideBuy,
			MakerAmountBaseUnits: evidence.MakerAmountBaseUnits, TakerAmountBaseUnits: evidence.TakerAmountBaseUnits,
			TotalFeeBaseUnits: evidence.FeeBaseUnits, BuilderCode: evidence.Builder, BuilderFeeKnown: true,
			BuilderFeeBaseUnits: "0", BuilderFeeSource: domain.SettlementEvidenceZeroBuilder,
			CollateralDecimals: 6, OutcomeTokenDecimals: 6,
		},
	}
	digest := sha256.Sum256([]byte(t.Fingerprint() + "|" + evidence.BlockHash))
	fill.RawPayloadSHA256 = hex.EncodeToString(digest[:])
	fill.Key = fillprocessor.FillKey(fill.Venue, fill.VenueFillID, fill.OrderID)
	grossRat, feeRat := ratOf(gross.String()), ratOf(fee.String())
	net := new(big.Rat).Neg(new(big.Rat).Add(grossRat, feeRat))
	fill.NetCashDelta = domain.Decimal(strings.TrimRight(strings.TrimRight(net.FloatString(6), "0"), "."))
	fill = fill.Normalize()
	if err := fill.ValidateAccounting(); err != nil {
		return domain.Fill{}, fmt.Errorf("fill failed ledger accounting: %w", err)
	}
	return fill, nil
}

func ratOf(value string) *big.Rat {
	rat, ok := new(big.Rat).SetString(value)
	if !ok {
		return new(big.Rat)
	}
	return rat
}

// baseUnits converts a canonical 6-decimal base-unit integer string to a Decimal.
func baseUnits(raw string) (domain.Decimal, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || (len(raw) > 1 && raw[0] == '0') {
		return "", fmt.Errorf("amount %q is not a canonical base-unit integer", raw)
	}
	value, ok := new(big.Int).SetString(raw, 10)
	if !ok || value.Sign() < 0 {
		return "", fmt.Errorf("amount %q is not a canonical base-unit integer", raw)
	}
	text := new(big.Rat).SetFrac(value, big.NewInt(1_000_000)).FloatString(6)
	text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	if text == "" {
		text = "0"
	}
	return domain.ParseDecimal(text)
}

package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

const (
	SettlementEvidenceSchemaV1       = "trading.settlement_evidence.v1"
	SettlementEvidencePolygonChainID = uint64(137)
	SettlementEvidenceZeroBuilder    = "ORDER_FILLED_ZERO_BUILDER"
	settlementZeroBytes32            = "0x0000000000000000000000000000000000000000000000000000000000000000"
)

// SettlementEvidence is the durable, non-secret proof behind one applied
// fill. It is intentionally excluded from public Fill JSON and is persisted in
// PostgreSQL for financial audit and deterministic re-verification.
type SettlementEvidence struct {
	SchemaVersion        string `json:"schema_version"`
	Source               string `json:"source"`
	ChainID              uint64 `json:"chain_id"`
	ExchangeAddress      string `json:"exchange_address"`
	TransactionHash      string `json:"transaction_hash"`
	BlockNumber          uint64 `json:"block_number"`
	BlockHash            string `json:"block_hash"`
	LogIndex             uint64 `json:"log_index"`
	Confirmations        uint64 `json:"confirmations"`
	OrderHash            string `json:"order_hash"`
	MakerAddress         string `json:"maker_address"`
	TokenID              string `json:"token_id"`
	Side                 Side   `json:"side"`
	MakerAmountBaseUnits string `json:"maker_amount_base_units"`
	TakerAmountBaseUnits string `json:"taker_amount_base_units"`
	TotalFeeBaseUnits    string `json:"total_fee_base_units"`
	BuilderCode          string `json:"builder_code"`
	BuilderFeeKnown      bool   `json:"builder_fee_known"`
	BuilderFeeBaseUnits  string `json:"builder_fee_base_units"`
	BuilderFeeSource     string `json:"builder_fee_source"`
	CollateralDecimals   uint8  `json:"collateral_decimals"`
	OutcomeTokenDecimals uint8  `json:"outcome_token_decimals"`
}

func (evidence SettlementEvidence) Normalize() SettlementEvidence {
	evidence.SchemaVersion = strings.TrimSpace(evidence.SchemaVersion)
	evidence.Source = strings.ToUpper(strings.TrimSpace(evidence.Source))
	evidence.ExchangeAddress = strings.ToLower(strings.TrimSpace(evidence.ExchangeAddress))
	evidence.TransactionHash = strings.ToLower(strings.TrimSpace(evidence.TransactionHash))
	evidence.BlockHash = strings.ToLower(strings.TrimSpace(evidence.BlockHash))
	evidence.OrderHash = strings.ToLower(strings.TrimSpace(evidence.OrderHash))
	evidence.MakerAddress = strings.ToLower(strings.TrimSpace(evidence.MakerAddress))
	evidence.TokenID = strings.TrimSpace(evidence.TokenID)
	evidence.Side = Side(strings.ToUpper(strings.TrimSpace(string(evidence.Side))))
	evidence.MakerAmountBaseUnits = strings.TrimSpace(evidence.MakerAmountBaseUnits)
	evidence.TakerAmountBaseUnits = strings.TrimSpace(evidence.TakerAmountBaseUnits)
	evidence.TotalFeeBaseUnits = strings.TrimSpace(evidence.TotalFeeBaseUnits)
	evidence.BuilderCode = strings.ToLower(strings.TrimSpace(evidence.BuilderCode))
	evidence.BuilderFeeBaseUnits = strings.TrimSpace(evidence.BuilderFeeBaseUnits)
	evidence.BuilderFeeSource = strings.ToUpper(strings.TrimSpace(evidence.BuilderFeeSource))
	return evidence
}

// ValidateAgainst proves that the durable event identity and integer money
// amounts are exactly the values the Fill ledger will apply.
func (evidence SettlementEvidence) ValidateAgainst(fill Fill) error {
	evidence = evidence.Normalize()
	fill = fill.Normalize()
	if evidence.SchemaVersion != SettlementEvidenceSchemaV1 || evidence.Source != FeeSourcePolygonV2OrderFilled {
		return fmt.Errorf("settlement evidence schema or source is unsupported")
	}
	if evidence.ChainID != SettlementEvidencePolygonChainID {
		return fmt.Errorf("settlement evidence chain_id must be 137")
	}
	if !canonicalFixedHex(evidence.ExchangeAddress, 20) ||
		!canonicalFixedHex(evidence.TransactionHash, 32) ||
		!canonicalFixedHex(evidence.BlockHash, 32) ||
		!canonicalFixedHex(evidence.OrderHash, 32) ||
		!canonicalFixedHex(evidence.MakerAddress, 20) ||
		!canonicalFixedHex(evidence.BuilderCode, 32) {
		return fmt.Errorf("settlement evidence contains a malformed address or bytes32 identity")
	}
	if evidence.BlockNumber == 0 || evidence.Confirmations == 0 {
		return fmt.Errorf("settlement evidence is not finalized")
	}
	if evidence.TransactionHash != strings.ToLower(fill.TransactionHash) ||
		evidence.OrderHash != strings.ToLower(fill.VenueOrderID) ||
		evidence.TokenID != fill.TokenID || evidence.Side != fill.Side ||
		evidence.Source != fill.FeeSource {
		return fmt.Errorf("settlement evidence does not match the fill identity")
	}
	if evidence.CollateralDecimals != 6 || evidence.OutcomeTokenDecimals != 6 {
		return fmt.Errorf("settlement evidence must use six-decimal pUSD and outcome-token units")
	}
	makerAmount, err := evidenceBaseUnitDecimal(evidence.MakerAmountBaseUnits, evidence.CollateralDecimals)
	if err != nil {
		return fmt.Errorf("settlement maker amount: %w", err)
	}
	takerAmount, err := evidenceBaseUnitDecimal(evidence.TakerAmountBaseUnits, evidence.OutcomeTokenDecimals)
	if err != nil {
		return fmt.Errorf("settlement taker amount: %w", err)
	}
	var shares, gross Decimal
	if fill.Side == SideBuy {
		gross, shares = makerAmount, takerAmount
	} else if fill.Side == SideSell {
		shares, gross = makerAmount, takerAmount
	} else {
		return fmt.Errorf("settlement evidence side is invalid")
	}
	if !shares.Equal(fill.Shares) || !gross.Equal(fill.GrossNotional) {
		return fmt.Errorf("settlement evidence amounts do not match the fill")
	}
	totalFee, err := evidenceBaseUnitDecimal(evidence.TotalFeeBaseUnits, evidence.CollateralDecimals)
	if err != nil {
		return fmt.Errorf("settlement total fee: %w", err)
	}
	if !evidence.BuilderFeeKnown {
		return fmt.Errorf("settlement evidence builder allocation is unknown")
	}
	builderFee, err := evidenceBaseUnitDecimal(evidence.BuilderFeeBaseUnits, evidence.CollateralDecimals)
	if err != nil {
		return fmt.Errorf("settlement builder fee: %w", err)
	}
	if !totalFee.Equal(fill.TotalFee) || !builderFee.Equal(fill.BuilderFee) {
		return fmt.Errorf("settlement evidence fees do not match the fill")
	}
	if evidence.BuilderCode == settlementZeroBytes32 {
		if evidence.BuilderFeeSource != SettlementEvidenceZeroBuilder || !builderFee.Equal("0") {
			return fmt.Errorf("zero builder evidence must prove a zero builder fee")
		}
	} else if evidence.BuilderFeeSource == "" {
		return fmt.Errorf("non-zero builder evidence requires an authoritative allocation source")
	}
	return nil
}

func (evidence SettlementEvidence) CanonicalJSON() ([]byte, error) {
	normalized := evidence.Normalize()
	return json.Marshal(normalized)
}

// CanonicalIdentityJSON serializes only immutable settlement identity. The
// confirmation count is an observation of the current chain head, so it grows
// over time even though the underlying OrderFilled event is unchanged. It must
// not participate in fill idempotency or evidence digests.
func (evidence SettlementEvidence) CanonicalIdentityJSON() ([]byte, error) {
	normalized := evidence.Normalize()
	normalized.Confirmations = 0
	return json.Marshal(normalized)
}

func (evidence SettlementEvidence) SHA256() (string, error) {
	payload, err := evidence.CanonicalIdentityJSON()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func evidenceBaseUnitDecimal(raw string, decimals uint8) (Decimal, error) {
	if !canonicalUint256(raw) {
		return "", fmt.Errorf("amount must be a canonical uint256 decimal string")
	}
	digits := raw
	for len(digits) <= int(decimals) {
		digits = "0" + digits
	}
	if decimals > 0 {
		cut := len(digits) - int(decimals)
		digits = digits[:cut] + "." + digits[cut:]
	}
	digits = strings.TrimRight(strings.TrimRight(digits, "0"), ".")
	if digits == "" {
		digits = "0"
	}
	return ParseDecimal(digits)
}

func canonicalUint256(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	integer, ok := new(big.Int).SetString(value, 10)
	return ok && integer.Sign() >= 0 && integer.BitLen() <= 256
}

func canonicalFixedHex(value string, bytes int) bool {
	if len(value) != 2+bytes*2 || !strings.HasPrefix(value, "0x") || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}

package evmrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"golang.org/x/crypto/sha3"
)

type ConditionPayout struct {
	Denominator *big.Int
	Numerators  [2]*big.Int
}

// BlockHeader returns the canonical hash and timestamp of an exact block.
// Callers use it to ensure a maintenance manifest still points at the same
// chain before reading balances.
func (reader *OrderFilledEvidenceReader) BlockHeader(ctx context.Context, block uint64) (string, time.Time, error) {
	raw, err := reader.rpcCall(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", block), false})
	if err != nil {
		return "", time.Time{}, err
	}
	var header struct {
		Hash string `json:"hash"`
		Time string `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &header); err != nil || len(header.Hash) != 66 || !strings.HasPrefix(header.Hash, "0x") {
		return "", time.Time{}, fmt.Errorf("invalid block header")
	}
	value, ok := new(big.Int).SetString(strings.TrimPrefix(header.Time, "0x"), 16)
	if !ok {
		return "", time.Time{}, fmt.Errorf("invalid block timestamp")
	}
	return strings.ToLower(header.Hash), time.Unix(value.Int64(), 0).UTC(), nil
}

// NativeBalanceAt reads the Polygon native balance at an explicit block. It
// is read-only and is used by operator maintenance paths to prove an EOA can
// pay the gas needed by an already-approved CLOB SELL or receipt lookup.
func (reader *OrderFilledEvidenceReader) NativeBalanceAt(ctx context.Context, wallet, block string) (domain.Decimal, error) {
	address, err := normalizedAddress(wallet)
	if err != nil {
		return "", err
	}
	raw, err := reader.rpcCall(ctx, "eth_getBalance", []any{address, strings.TrimSpace(block)})
	if err != nil {
		return "", err
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil || !strings.HasPrefix(encoded, "0x") {
		return "", fmt.Errorf("invalid native balance result")
	}
	units, ok := new(big.Int).SetString(encoded[2:], 16)
	if !ok || units.Sign() < 0 {
		return "", fmt.Errorf("invalid native balance quantity")
	}
	return domain.Decimal(formatUnits(units, 18)), nil
}

// ReadConditionPayout reads the Conditional Tokens payout denominator and
// both binary numerators at an exact block. A positive denominator is the
// chain-level settlement proof; it does not infer payout from an indexer.
func (reader *OrderFilledEvidenceReader) ReadConditionPayout(ctx context.Context, conditionID, block string) (ConditionPayout, error) {
	condition, err := normalizedHash(conditionID, "condition id")
	if err != nil {
		return ConditionPayout{}, err
	}
	denominator, err := reader.readUint256Call(ctx, selector("payoutDenominator(bytes32)"), condition[2:], block)
	if err != nil {
		return ConditionPayout{}, err
	}
	result := ConditionPayout{Denominator: denominator}
	for index := 0; index < 2; index++ {
		value, readErr := reader.readUint256Call(ctx, selector("payoutNumerators(bytes32,uint256)"), condition[2:]+fmt.Sprintf("%064x", index), block)
		if readErr != nil {
			return ConditionPayout{}, readErr
		}
		result.Numerators[index] = value
	}
	return result, nil
}

func (reader *OrderFilledEvidenceReader) readUint256Call(ctx context.Context, functionSelector, words, block string) (*big.Int, error) {
	raw, err := reader.rpcCall(ctx, "eth_call", []any{map[string]string{"to": PolymarketConditionalTokensAddress, "data": "0x" + functionSelector + words}, block})
	if err != nil {
		return nil, err
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil || !strings.HasPrefix(encoded, "0x") || len(encoded) != 66 {
		return nil, fmt.Errorf("invalid condition payout response")
	}
	value, ok := new(big.Int).SetString(encoded[2:], 16)
	if !ok {
		return nil, fmt.Errorf("invalid condition payout quantity")
	}
	return value, nil
}

func selector(signature string) string {
	hash := sha3.NewLegacyKeccak256()
	_, _ = hash.Write([]byte(signature))
	return fmt.Sprintf("%x", hash.Sum(nil)[:4])
}

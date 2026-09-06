package evmrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// ReadExternalSell verifies a confirmed maker SELL and its actual ERC20 cash
// movement. It never signs or submits a transaction. Ambiguous cash flows fail
// closed instead of being estimated from a displayed price.
func (reader *OrderFilledEvidenceReader) ReadExternalSell(ctx context.Context, request OrderFilledEvidenceRequest, collateral string) (OrderFilledEvidence, error) {
	request.Side = OrderSideSell
	evidence, err := reader.Read(ctx, request)
	if err != nil {
		return evidence, err
	}
	receipt, err := reader.getTransactionReceipt(ctx, evidence.TransactionHash)
	if err != nil {
		return evidence, err
	}
	expected, err := validateEvidenceRequest(request)
	if err != nil {
		return evidence, err
	}
	current, err := matchReceipt(receipt, expected)
	if err != nil {
		return evidence, err
	}
	if !sameEvidence(current, evidence) {
		return evidence, fmt.Errorf("sell receipt changed")
	}
	actual := new(big.Int)
	walletTopic := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(evidence.Maker, "0x")
	for _, log := range receipt.Logs {
		if !strings.EqualFold(log.Address, collateral) || len(log.Topics) != 3 || !strings.EqualFold(log.Topics[0], transferTopic) {
			continue
		}
		if log.Removed != nil && *log.Removed {
			return evidence, fmt.Errorf("removed transfer log")
		}
		amount, ok := new(big.Int).SetString(strings.TrimPrefix(log.Data, "0x"), 16)
		if !ok || amount.Sign() < 0 {
			return evidence, fmt.Errorf("invalid transfer amount")
		}
		if strings.EqualFold(log.Topics[2], walletTopic) {
			actual.Add(actual, amount)
		}
		if strings.EqualFold(log.Topics[1], walletTopic) {
			actual.Sub(actual, amount)
		}
	}
	gross, _ := new(big.Int).SetString(evidence.TakerAmountBaseUnits, 10)
	fee, _ := new(big.Int).SetString(evidence.FeeBaseUnits, 10)
	if gross == nil || fee == nil || actual.Sign() <= 0 || actual.Cmp(new(big.Int).Sub(gross, fee)) != 0 {
		return evidence, fmt.Errorf("actual collateral transfers do not equal SELL proceeds minus fee")
	}
	return evidence, nil
}

// ReadTokenBalance reads one known token at an explicit block. tokenID empty
// selects ERC20; otherwise ERC1155. All amounts retain exact six-decimal units.
func (reader *OrderFilledEvidenceReader) ReadTokenBalance(ctx context.Context, contract, wallet, tokenID, block string) (string, error) {
	address, err := normalizedAddress(wallet)
	if err != nil {
		return "", err
	}
	contract, err = normalizedAddress(contract)
	if err != nil {
		return "", err
	}
	data := "0x70a08231" + strings.Repeat("0", 24) + strings.TrimPrefix(address, "0x")
	if tokenID != "" {
		token, ok := new(big.Int).SetString(tokenID, 10)
		if !ok || token.Sign() < 0 || token.BitLen() > 256 {
			return "", fmt.Errorf("invalid token id")
		}
		data = "0x00fdd58e" + data[10:] + fmt.Sprintf("%064x", token)
	}
	raw, err := reader.rpcCall(ctx, "eth_call", []any{map[string]string{"to": contract, "data": data}, block})
	if err != nil {
		return "", err
	}
	var result string
	if err = json.Unmarshal(raw, &result); err != nil || len(result) != 66 || !strings.HasPrefix(result, "0x") {
		return "", fmt.Errorf("invalid balance result")
	}
	units, ok := new(big.Int).SetString(result[2:], 16)
	if !ok {
		return "", fmt.Errorf("invalid balance units")
	}
	return formatUnits(units, 6), nil
}

// ObservedBlock supplies a common block for a repair's asset snapshot.
func (reader *OrderFilledEvidenceReader) ObservedBlock(ctx context.Context) (uint64, error) {
	return reader.getLatestBlock(ctx)
}

// GetKnownPositionBalance verifies known dust omitted from the indexer's list.
func (reader *OrderFilledEvidenceReader) GetKnownPositionBalance(ctx context.Context, wallet, token string) (domain.Decimal, error) {
	chain, err := reader.getChainID(ctx)
	if err != nil {
		return "", err
	}
	if chain != 137 {
		return "", fmt.Errorf("expected Polygon")
	}
	value, err := reader.ReadTokenBalance(ctx, PolymarketConditionalTokensAddress, wallet, token, "latest")
	return domain.Decimal(value), err
}

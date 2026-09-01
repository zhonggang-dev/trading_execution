package evmrpc

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
)

func TestMatchRedemptionReceiptRequiresExactAdapterWalletConditionAndPayout(t *testing.T) {
	removed := false
	txHash := "0x" + strings.Repeat("11", 32)
	blockHash := "0x" + strings.Repeat("22", 32)
	wallet := "0x" + strings.Repeat("33", 20)
	condition := "0x" + strings.Repeat("44", 32)
	data := append(word(big.NewInt(64)), word(big.NewInt(48_000_000))...)
	data = append(data, word(big.NewInt(2))...)
	data = append(data, word(big.NewInt(0))...)
	data = append(data, word(big.NewInt(48_000_000))...)
	receipt := redemptionRPCReceipt{
		TransactionHash: txHash, BlockHash: blockHash, BlockNumber: "0x64", Status: "0x1",
		Logs: []redemptionRPCLog{{
			Address: PolymarketNegRiskAdapterAddress,
			Topics: []string{
				eventTopic("PositionsRedeemed(address,bytes32,uint256[],uint256)"),
				"0x" + strings.Repeat("0", 24) + wallet[2:], condition,
			},
			Data: "0x" + hex.EncodeToString(data), TransactionHash: txHash,
			BlockHash: blockHash, BlockNumber: "0x64", Removed: &removed,
		}},
	}
	evidence, err := matchRedemptionReceipt(receipt, txHash, wallet, condition, PolymarketNegRiskAdapterAddress)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.PayoutBaseUnits != "48000000" || evidence.EventType != "POSITIONS_REDEEMED" {
		t.Fatalf("redemption evidence = %#v", evidence)
	}
	if _, err := matchRedemptionReceipt(receipt, txHash, wallet, condition, PolymarketCollateralAdapterAddress); err == nil {
		t.Fatal("wrong adapter receipt was accepted")
	}
}

func word(value *big.Int) []byte {
	result := make([]byte, 32)
	copy(result[32-len(value.Bytes()):], value.Bytes())
	return result
}

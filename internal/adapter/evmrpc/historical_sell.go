package evmrpc

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// HistoricalSellEvidence keeps the maker address in process memory only.
// Callers persist a sanitized projection, never marshal Fill directly.
type HistoricalSellEvidence struct {
	Fill          OrderFilledEvidence
	SoldAt        time.Time
	ReceiptSHA256 string
}

// ReadHistoricalSell repeats the existing finality/cash checks, then verifies
// net CTF outflow and the canonical block's actual timestamp for a prior sale.
func (r *OrderFilledEvidenceReader) ReadHistoricalSell(ctx context.Context, request OrderFilledEvidenceRequest, collateral string) (HistoricalSellEvidence, error) {
	fill, err := r.ReadExternalSell(ctx, request, collateral)
	if err != nil {
		return HistoricalSellEvidence{}, err
	}
	receipt, err := r.getTransactionReceipt(ctx, fill.TransactionHash)
	if err != nil {
		return HistoricalSellEvidence{}, err
	}
	expected, err := validateEvidenceRequest(request)
	if err != nil {
		return HistoricalSellEvidence{}, err
	}
	current, err := matchReceipt(receipt, expected)
	if err != nil || !sameEvidence(current, fill) {
		return HistoricalSellEvidence{}, fmt.Errorf("historical receipt is not stable")
	}
	// The persisted receipt digest must describe the same payload on which
	// both cash and CTF checks passed, not merely an unchanged OrderFilled.
	if err = verifyExternalSellCash(receipt, fill, collateral); err != nil {
		return HistoricalSellEvidence{}, err
	}
	out := new(big.Int)
	walletTopic := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(fill.Maker, "0x")
	for _, log := range receipt.Logs {
		if !strings.EqualFold(log.Address, PolymarketConditionalTokensAddress) || len(log.Topics) != 4 {
			continue
		}
		direction := 0
		if strings.EqualFold(log.Topics[2], walletTopic) {
			direction++
		}
		if strings.EqualFold(log.Topics[3], walletTopic) {
			direction--
		}
		if direction == 0 {
			continue
		}
		data, err := decodeFixedHex(log.Data, (len(log.Data)-2)/2, "CTF transfer")
		if err != nil {
			return HistoricalSellEvidence{}, err
		}
		var ids, amounts []*big.Int
		switch strings.ToLower(log.Topics[0]) {
		case "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62":
			if len(data) != 64 {
				return HistoricalSellEvidence{}, fmt.Errorf("invalid CTF single transfer")
			}
			ids = []*big.Int{new(big.Int).SetBytes(data[:32])}
			amounts = []*big.Int{new(big.Int).SetBytes(data[32:])}
		case "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb":
			ids, amounts, err = historicalBatch(data)
			if err != nil {
				return HistoricalSellEvidence{}, err
			}
		default:
			continue
		}
		for i, id := range ids {
			if id.String() == fill.TokenID {
				if direction > 0 {
					out.Add(out, amounts[i])
				} else {
					out.Sub(out, amounts[i])
				}
			}
		}
	}
	maker, ok := new(big.Int).SetString(fill.MakerAmountBaseUnits, 10)
	if !ok || out.Cmp(maker) != 0 {
		return HistoricalSellEvidence{}, fmt.Errorf("CTF outflow does not equal the exact SELL amount")
	}
	raw, err := r.rpcCall(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", fill.BlockNumber), false})
	if err != nil {
		return HistoricalSellEvidence{}, err
	}
	var block struct {
		Hash      string `json:"hash"`
		Number    string `json:"number"`
		Timestamp string `json:"timestamp"`
	}
	if err = json.Unmarshal(raw, &block); err != nil {
		return HistoricalSellEvidence{}, fmt.Errorf("invalid historical block")
	}
	number, err := parseCanonicalQuantity(block.Number, "block number")
	if err != nil || number != fill.BlockNumber || !equalFixedHex(block.Hash, fill.BlockHash, 32) {
		return HistoricalSellEvidence{}, fmt.Errorf("receipt block is not canonical")
	}
	stamp, err := parseCanonicalQuantity(block.Timestamp, "block timestamp")
	if err != nil || stamp > 253402300799 {
		return HistoricalSellEvidence{}, fmt.Errorf("invalid historical block time")
	}
	encoded, _ := json.Marshal(receipt)
	digest := sha256.Sum256(encoded)
	return HistoricalSellEvidence{Fill: fill, SoldAt: time.Unix(int64(stamp), 0).UTC(), ReceiptSHA256: fmt.Sprintf("%x", digest)}, nil
}

func historicalBatch(data []byte) ([]*big.Int, []*big.Int, error) {
	if len(data) < 64 || len(data)%32 != 0 {
		return nil, nil, fmt.Errorf("invalid CTF batch")
	}
	arrays := make([][]*big.Int, 2)
	for i := range 2 {
		offset := new(big.Int).SetBytes(data[i*32 : (i+1)*32])
		if !offset.IsUint64() || offset.Uint64()%32 != 0 || offset.Uint64() < 64 || offset.Uint64() > uint64(len(data)-32) {
			return nil, nil, fmt.Errorf("invalid CTF batch offset")
		}
		at := int(offset.Uint64())
		size := new(big.Int).SetBytes(data[at : at+32])
		if !size.IsUint64() || size.Uint64() > uint64((len(data)-at-32)/32) {
			return nil, nil, fmt.Errorf("invalid CTF batch length")
		}
		for j := range int(size.Uint64()) {
			arrays[i] = append(arrays[i], new(big.Int).SetBytes(data[at+32+j*32:at+64+j*32]))
		}
	}
	if len(arrays[0]) != len(arrays[1]) {
		return nil, nil, fmt.Errorf("CTF batch arrays differ")
	}
	return arrays[0], arrays[1], nil
}

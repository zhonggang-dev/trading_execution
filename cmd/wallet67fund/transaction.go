package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	secp256k1ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"golang.org/x/crypto/sha3"
)

var secp256k1HalfOrder = mustBigIntHex("7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0")

type fundingTransaction struct {
	chainID              uint64
	nonce                uint64
	maxPriorityFeePerGas *big.Int
	maxFeePerGas         *big.Int
	gasLimit             uint64
	to                   string
	value                *big.Int
	data                 []byte
}

type signedFundingTransaction struct {
	digest  string
	raw     string
	hash    string
	yParity uint8
	r       *big.Int
	s       *big.Int
}

func newFundingTransaction(nonce, gasLimit uint64, priorityFee, maxFee *big.Int, recipient string) (fundingTransaction, error) {
	if gasLimit < minimumFundingGasEstimate || gasLimit > maximumFundingGasLimit {
		return fundingTransaction{}, fmt.Errorf("funding gas limit %d is outside [%d,%d]", gasLimit, minimumFundingGasEstimate, maximumFundingGasLimit)
	}
	if priorityFee == nil || priorityFee.Sign() <= 0 || maxFee == nil || maxFee.Sign() <= 0 || maxFee.Cmp(priorityFee) < 0 {
		return fundingTransaction{}, fmt.Errorf("funding EIP-1559 fees are invalid")
	}
	if priorityFee.Cmp(new(big.Int).SetUint64(maximumPriorityFeeWei)) > 0 {
		return fundingTransaction{}, fmt.Errorf("funding priority fee %s exceeds cap %d", priorityFee, maximumPriorityFeeWei)
	}
	if maxFee.Cmp(new(big.Int).SetUint64(maximumMaxFeePerGasWei)) > 0 {
		return fundingTransaction{}, fmt.Errorf("funding max fee %s exceeds cap %d", maxFee, maximumMaxFeePerGasWei)
	}
	recipient, err := normalizeAddress(recipient)
	if err != nil {
		return fundingTransaction{}, err
	}
	if recipient != wallet6ExpectedAddress && recipient != wallet7ExpectedAddress {
		return fundingTransaction{}, fmt.Errorf("funding transaction recipient is not wallet-6 or wallet-7")
	}
	return fundingTransaction{
		chainID: polygonChainID, nonce: nonce,
		maxPriorityFeePerGas: new(big.Int).Set(priorityFee),
		maxFeePerGas:         new(big.Int).Set(maxFee), gasLimit: gasLimit,
		to: recipient, value: new(big.Int).Set(fundingAmountWei), data: nil,
	}, nil
}

func signType2Funding(ctx context.Context, transaction fundingTransaction, account fundingAccount) (signedFundingTransaction, error) {
	if transaction.chainID != polygonChainID || transaction.value == nil || transaction.value.Cmp(fundingAmountWei) != 0 || len(transaction.data) != 0 {
		return signedFundingTransaction{}, fmt.Errorf("funding transaction fixed chain, value, or input mismatch")
	}
	if transaction.to != wallet6ExpectedAddress && transaction.to != wallet7ExpectedAddress {
		return signedFundingTransaction{}, fmt.Errorf("funding transaction recipient is not wallet-6 or wallet-7")
	}
	to, err := addressBytes(transaction.to)
	if err != nil {
		return signedFundingTransaction{}, err
	}
	unsigned := rlpList(
		rlpUint64(transaction.chainID),
		rlpUint64(transaction.nonce),
		rlpBigInt(transaction.maxPriorityFeePerGas),
		rlpBigInt(transaction.maxFeePerGas),
		rlpUint64(transaction.gasLimit),
		rlpBytes(to),
		rlpBigInt(transaction.value),
		rlpBytes(transaction.data),
		rlpList(),
	)
	digestBytes := keccak256([]byte{0x02}, unsigned)
	signature, err := account.signer.SignDigest(ctx, digestBytes)
	if err != nil {
		return signedFundingTransaction{}, fmt.Errorf("sign type-2 funding digest: %w", err)
	}
	if len(signature) != 65 || (signature[64] != 27 && signature[64] != 28) {
		return signedFundingTransaction{}, fmt.Errorf("sign type-2 funding digest: invalid Ethereum signature")
	}
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:64])
	if r.Sign() <= 0 || s.Sign() <= 0 || s.Cmp(secp256k1HalfOrder) > 0 {
		return signedFundingTransaction{}, fmt.Errorf("sign type-2 funding digest: signature is zero or not low-S")
	}
	if err := verifySignerAddress(digestBytes, signature, account.address); err != nil {
		return signedFundingTransaction{}, err
	}
	yParity := signature[64] - 27
	signed := rlpList(
		rlpUint64(transaction.chainID),
		rlpUint64(transaction.nonce),
		rlpBigInt(transaction.maxPriorityFeePerGas),
		rlpBigInt(transaction.maxFeePerGas),
		rlpUint64(transaction.gasLimit),
		rlpBytes(to),
		rlpBigInt(transaction.value),
		rlpBytes(transaction.data),
		rlpList(),
		rlpUint64(uint64(yParity)),
		rlpBigInt(r),
		rlpBigInt(s),
	)
	rawBytes := append([]byte{0x02}, signed...)
	return signedFundingTransaction{
		digest:  "0x" + hex.EncodeToString(digestBytes),
		raw:     "0x" + hex.EncodeToString(rawBytes),
		hash:    "0x" + hex.EncodeToString(keccak256(rawBytes)),
		yParity: yParity, r: r, s: s,
	}, nil
}

func verifySignerAddress(digest, signature []byte, expectedAddress string) error {
	compact := make([]byte, 65)
	compact[0] = signature[64]
	copy(compact[1:], signature[:64])
	publicKey, _, err := secp256k1ecdsa.RecoverCompact(compact, digest)
	if err != nil {
		return fmt.Errorf("recover type-2 funding signer: %w", err)
	}
	publicKeyBytes := publicKey.SerializeUncompressed()
	addressHash := keccak256(publicKeyBytes[1:])
	recovered := "0x" + hex.EncodeToString(addressHash[len(addressHash)-20:])
	if recovered != expectedAddress {
		return fmt.Errorf("type-2 funding signature recovered %s; require %s", recovered, expectedAddress)
	}
	return nil
}

func rlpUint64(value uint64) []byte {
	if value == 0 {
		return rlpBytes(nil)
	}
	return rlpBigInt(new(big.Int).SetUint64(value))
}

func rlpBigInt(value *big.Int) []byte {
	if value == nil || value.Sign() == 0 {
		return rlpBytes(nil)
	}
	if value.Sign() < 0 {
		panic("negative integer passed to RLP")
	}
	return rlpBytes(value.Bytes())
}

func rlpBytes(value []byte) []byte {
	if len(value) == 1 && value[0] < 0x80 {
		return append([]byte(nil), value...)
	}
	if len(value) <= 55 {
		return append([]byte{byte(0x80 + len(value))}, value...)
	}
	length := minimalBigEndian(uint64(len(value)))
	result := make([]byte, 1+len(length)+len(value))
	result[0] = byte(0xb7 + len(length))
	copy(result[1:], length)
	copy(result[1+len(length):], value)
	return result
}

func rlpList(encodedItems ...[]byte) []byte {
	payload := bytes.Join(encodedItems, nil)
	if len(payload) <= 55 {
		return append([]byte{byte(0xc0 + len(payload))}, payload...)
	}
	length := minimalBigEndian(uint64(len(payload)))
	result := make([]byte, 1+len(length)+len(payload))
	result[0] = byte(0xf7 + len(length))
	copy(result[1:], length)
	copy(result[1+len(length):], payload)
	return result
}

func minimalBigEndian(value uint64) []byte {
	encoded := new(big.Int).SetUint64(value).Bytes()
	return encoded
}

func normalizeAddress(value string) (string, error) {
	raw := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(value)), "0x")
	if len(raw) != 40 {
		return "", fmt.Errorf("address must contain exactly 20 bytes")
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return "", fmt.Errorf("address is not hexadecimal")
	}
	return "0x" + raw, nil
}

func addressBytes(value string) ([]byte, error) {
	normalized, err := normalizeAddress(value)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(normalized[2:])
}

func keccak256(parts ...[]byte) []byte {
	hash := sha3.NewLegacyKeccak256()
	for _, part := range parts {
		_, _ = hash.Write(part)
	}
	return hash.Sum(nil)
}

func mustBigIntHex(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 16)
	if !ok {
		panic("invalid big integer constant")
	}
	return parsed
}

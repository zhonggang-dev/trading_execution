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

type approvalTransaction struct {
	chainID              uint64
	nonce                uint64
	maxPriorityFeePerGas *big.Int
	maxFeePerGas         *big.Int
	gasLimit             uint64
	to                   string
	value                *big.Int
	data                 []byte
}

type signedApprovalTransaction struct {
	digest  string
	raw     string
	hash    string
	yParity uint8
	r       *big.Int
	s       *big.Int
}

func newApprovalTransaction(nonce, gasLimit uint64, priorityFee, maxFee *big.Int, spender string) (approvalTransaction, error) {
	if gasLimit < minimumApprovalGasEstimate || gasLimit > maximumApprovalGasLimit {
		return approvalTransaction{}, fmt.Errorf("approval gas limit %d is outside [%d,%d]", gasLimit, minimumApprovalGasEstimate, maximumApprovalGasLimit)
	}
	if priorityFee == nil || priorityFee.Sign() <= 0 || maxFee == nil || maxFee.Sign() <= 0 || maxFee.Cmp(priorityFee) < 0 {
		return approvalTransaction{}, fmt.Errorf("approval EIP-1559 fees are invalid")
	}
	if priorityFee.Cmp(new(big.Int).SetUint64(maximumPriorityFeeWei)) > 0 {
		return approvalTransaction{}, fmt.Errorf("approval priority fee %s exceeds cap %d", priorityFee, maximumPriorityFeeWei)
	}
	if maxFee.Cmp(new(big.Int).SetUint64(maximumMaxFeePerGasWei)) > 0 {
		return approvalTransaction{}, fmt.Errorf("approval max fee %s exceeds cap %d", maxFee, maximumMaxFeePerGasWei)
	}
	data, err := buildApproveCallData(spender, approvalAmount)
	if err != nil {
		return approvalTransaction{}, err
	}
	return approvalTransaction{
		chainID: polygonChainID, nonce: nonce,
		maxPriorityFeePerGas: new(big.Int).Set(priorityFee),
		maxFeePerGas:         new(big.Int).Set(maxFee), gasLimit: gasLimit,
		to: polygonPUSDAddress, value: new(big.Int), data: data,
	}, nil
}

func signType2Approval(ctx context.Context, transaction approvalTransaction, account approvalAccount) (signedApprovalTransaction, error) {
	if transaction.chainID != polygonChainID || transaction.to != polygonPUSDAddress || transaction.value == nil || transaction.value.Sign() != 0 {
		return signedApprovalTransaction{}, fmt.Errorf("approval transaction fixed chain, token, or value mismatch")
	}
	to, err := addressBytes(transaction.to)
	if err != nil {
		return signedApprovalTransaction{}, err
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
		return signedApprovalTransaction{}, fmt.Errorf("sign type-2 approval digest: %w", err)
	}
	if len(signature) != 65 || (signature[64] != 27 && signature[64] != 28) {
		return signedApprovalTransaction{}, fmt.Errorf("sign type-2 approval digest: invalid Ethereum signature")
	}
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:64])
	if r.Sign() <= 0 || s.Sign() <= 0 || s.Cmp(secp256k1HalfOrder) > 0 {
		return signedApprovalTransaction{}, fmt.Errorf("sign type-2 approval digest: signature is zero or not low-S")
	}
	if err := verifySignerAddress(digestBytes, signature, account.address); err != nil {
		return signedApprovalTransaction{}, err
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
	return signedApprovalTransaction{
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
		return fmt.Errorf("recover type-2 approval signer: %w", err)
	}
	publicKeyBytes := publicKey.SerializeUncompressed()
	addressHash := keccak256(publicKeyBytes[1:])
	recovered := "0x" + hex.EncodeToString(addressHash[len(addressHash)-20:])
	if recovered != expectedAddress {
		return fmt.Errorf("type-2 approval signature recovered %s; require %s", recovered, expectedAddress)
	}
	return nil
}

func buildApproveCallData(spender string, amount *big.Int) ([]byte, error) {
	spenderBytes, err := addressBytes(spender)
	if err != nil {
		return nil, fmt.Errorf("approval spender: %w", err)
	}
	if amount == nil || amount.Sign() < 0 || amount.BitLen() > 256 {
		return nil, fmt.Errorf("approval amount is not uint256")
	}
	data := make([]byte, 4+32+32)
	copy(data[:4], []byte{0x09, 0x5e, 0xa7, 0xb3})
	copy(data[4+12:4+32], spenderBytes)
	amountBytes := amount.Bytes()
	copy(data[len(data)-len(amountBytes):], amountBytes)
	return data, nil
}

func buildAllowanceCallData(owner, spender string) ([]byte, error) {
	ownerBytes, err := addressBytes(owner)
	if err != nil {
		return nil, fmt.Errorf("allowance owner: %w", err)
	}
	spenderBytes, err := addressBytes(spender)
	if err != nil {
		return nil, fmt.Errorf("allowance spender: %w", err)
	}
	data := make([]byte, 4+32+32)
	copy(data[:4], []byte{0xdd, 0x62, 0xed, 0x3e})
	copy(data[4+12:4+32], ownerBytes)
	copy(data[4+32+12:], spenderBytes)
	return data, nil
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

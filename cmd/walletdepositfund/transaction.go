package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	secp256k1ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"golang.org/x/crypto/sha3"
)

var secp256k1HalfOrder = mustBigIntHex("7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0")

type type2Transaction struct {
	chainID              uint64
	nonce                uint64
	maxPriorityFeePerGas *big.Int
	maxFeePerGas         *big.Int
	gasLimit             uint64
	to                   string
	value                *big.Int
	data                 []byte
}

type signedTransaction struct {
	digest string
	raw    string
	hash   string
}

func buildTransferCallData(recipient string, amount *big.Int) ([]byte, error) {
	recipientBytes, err := addressBytes(recipient)
	if err != nil {
		return nil, fmt.Errorf("transfer recipient: %w", err)
	}
	if amount == nil || amount.Sign() <= 0 || amount.BitLen() > 256 {
		return nil, fmt.Errorf("transfer amount is not a positive uint256")
	}
	data := make([]byte, 4+32+32)
	copy(data[:4], []byte{0xa9, 0x05, 0x9c, 0xbb})
	copy(data[4+12:4+32], recipientBytes)
	amountBytes := amount.Bytes()
	copy(data[len(data)-len(amountBytes):], amountBytes)
	return data, nil
}

func buildBalanceOfCallData(owner string) ([]byte, error) {
	ownerBytes, err := addressBytes(owner)
	if err != nil {
		return nil, fmt.Errorf("balance owner: %w", err)
	}
	data := make([]byte, 4+32)
	copy(data[:4], []byte{0x70, 0xa0, 0x82, 0x31})
	copy(data[4+12:], ownerBytes)
	return data, nil
}

func newTransferTransaction(nonce, gasLimit uint64, priorityFee, maxFee *big.Int, recipient string) (type2Transaction, error) {
	if gasLimit < minimumTransferGasEstimate || gasLimit > maximumTransferGasLimit {
		return type2Transaction{}, fmt.Errorf("transfer gas limit %d is outside [%d,%d]", gasLimit, minimumTransferGasEstimate, maximumTransferGasLimit)
	}
	if priorityFee == nil || priorityFee.Sign() <= 0 || maxFee == nil || maxFee.Sign() <= 0 || maxFee.Cmp(priorityFee) < 0 {
		return type2Transaction{}, fmt.Errorf("transfer EIP-1559 fees are invalid")
	}
	if priorityFee.Cmp(new(big.Int).SetUint64(maximumPriorityFeeWei)) > 0 || maxFee.Cmp(new(big.Int).SetUint64(maximumMaxFeePerGasWei)) > 0 {
		return type2Transaction{}, fmt.Errorf("transfer EIP-1559 fees exceed fixed caps")
	}
	data, err := buildTransferCallData(recipient, transferAmount)
	if err != nil {
		return type2Transaction{}, err
	}
	return type2Transaction{
		chainID: polygonChainID, nonce: nonce,
		maxPriorityFeePerGas: new(big.Int).Set(priorityFee), maxFeePerGas: new(big.Int).Set(maxFee),
		gasLimit: gasLimit, to: polygonPUSDAddress, value: new(big.Int), data: data,
	}, nil
}

func signType2Transaction(ctx context.Context, transaction type2Transaction, signer polymarket.DigestSigner, expectedSigner string) (signedTransaction, error) {
	if transaction.chainID != polygonChainID || transaction.to != polygonPUSDAddress || transaction.value == nil || transaction.value.Sign() != 0 {
		return signedTransaction{}, fmt.Errorf("transfer transaction fixed chain, token, or value mismatch")
	}
	to, err := addressBytes(transaction.to)
	if err != nil {
		return signedTransaction{}, err
	}
	unsigned := rlpList(
		rlpUint64(transaction.chainID), rlpUint64(transaction.nonce),
		rlpBigInt(transaction.maxPriorityFeePerGas), rlpBigInt(transaction.maxFeePerGas),
		rlpUint64(transaction.gasLimit), rlpBytes(to), rlpBigInt(transaction.value),
		rlpBytes(transaction.data), rlpList(),
	)
	digest := keccak256([]byte{0x02}, unsigned)
	signature, err := signer.SignDigest(ctx, digest)
	if err != nil {
		return signedTransaction{}, fmt.Errorf("sign type-2 transfer digest: %w", err)
	}
	if len(signature) != 65 || (signature[64] != 27 && signature[64] != 28) {
		return signedTransaction{}, fmt.Errorf("sign type-2 transfer digest: invalid Ethereum signature")
	}
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:64])
	if r.Sign() <= 0 || s.Sign() <= 0 || s.Cmp(secp256k1HalfOrder) > 0 {
		return signedTransaction{}, fmt.Errorf("sign type-2 transfer digest: signature is zero or not low-S")
	}
	if err := verifySignerAddress(digest, signature, expectedSigner); err != nil {
		return signedTransaction{}, err
	}
	signed := rlpList(
		rlpUint64(transaction.chainID), rlpUint64(transaction.nonce),
		rlpBigInt(transaction.maxPriorityFeePerGas), rlpBigInt(transaction.maxFeePerGas),
		rlpUint64(transaction.gasLimit), rlpBytes(to), rlpBigInt(transaction.value),
		rlpBytes(transaction.data), rlpList(), rlpUint64(uint64(signature[64]-27)),
		rlpBigInt(r), rlpBigInt(s),
	)
	raw := append([]byte{0x02}, signed...)
	return signedTransaction{
		digest: "0x" + hex.EncodeToString(digest),
		raw:    "0x" + hex.EncodeToString(raw),
		hash:   "0x" + hex.EncodeToString(keccak256(raw)),
	}, nil
}

func verifySignerAddress(digest, signature []byte, expected string) error {
	compact := make([]byte, 65)
	compact[0] = signature[64]
	copy(compact[1:], signature[:64])
	publicKey, _, err := secp256k1ecdsa.RecoverCompact(compact, digest)
	if err != nil {
		return fmt.Errorf("recover type-2 transfer signer: %w", err)
	}
	publicKeyBytes := publicKey.SerializeUncompressed()
	addressHash := keccak256(publicKeyBytes[1:])
	recovered := "0x" + hex.EncodeToString(addressHash[len(addressHash)-20:])
	if recovered != expected {
		return fmt.Errorf("type-2 transfer signature recovered %s; require %s", recovered, expected)
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
	length := new(big.Int).SetUint64(uint64(len(value))).Bytes()
	result := make([]byte, 1+len(length)+len(value))
	result[0] = byte(0xb7 + len(length))
	copy(result[1:], length)
	copy(result[1+len(length):], value)
	return result
}

func rlpList(items ...[]byte) []byte {
	payload := bytes.Join(items, nil)
	if len(payload) <= 55 {
		return append([]byte{byte(0xc0 + len(payload))}, payload...)
	}
	length := new(big.Int).SetUint64(uint64(len(payload))).Bytes()
	result := make([]byte, 1+len(length)+len(payload))
	result[0] = byte(0xf7 + len(length))
	copy(result[1:], length)
	copy(result[1+len(length):], payload)
	return result
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

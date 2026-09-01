package polymarket

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	secp256k1ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	redemptionChainID        = uint64(137)
	redemptionMinGas         = uint64(21_000)
	redemptionMaxGas         = uint64(1_000_000)
	redemptionMaxPriorityFee = uint64(100_000_000_000)
	redemptionMaxBaseFee     = uint64(500_000_000_000)
	redemptionMaxFee         = uint64(1_000_000_000_000)
)

var redemptionHalfOrder = mustRedemptionBigInt("7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0")

type redemptionType2Transaction struct {
	nonce       uint64
	priorityFee *big.Int
	maxFee      *big.Int
	gasLimit    uint64
	target      string
	data        []byte
}

func (client *RedemptionClient) submitEOACall(
	ctx context.Context, account TradingAccount, call redemptionCall,
) (domain.RedemptionSubmission, error) {
	if !strings.EqualFold(account.Signer.Address(), account.FunderAddress) {
		return domain.RedemptionSubmission{}, fmt.Errorf("EOA redemption signer and wallet differ")
	}
	chainResult, err := client.rpc(ctx, "eth_chainId", nil)
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	chainValue, err := jsonRPCString(chainResult, "chain id")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	chainID, err := parseRPCUint64(chainValue, "chain id")
	if err != nil || chainID != redemptionChainID {
		return domain.RedemptionSubmission{}, fmt.Errorf("RPC chain id must be Polygon 137")
	}
	latest, err := client.rpcUint64(ctx, "eth_getTransactionCount", []any{account.FunderAddress, "latest"}, "latest nonce")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	pending, err := client.rpcUint64(ctx, "eth_getTransactionCount", []any{account.FunderAddress, "pending"}, "pending nonce")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	if latest != pending {
		return domain.RedemptionSubmission{}, fmt.Errorf("EOA has an unrelated pending transaction; redemption nonce is ambiguous")
	}
	callObject := map[string]string{
		"from": account.FunderAddress, "to": call.Target, "value": "0x0", "data": call.Data,
	}
	if _, err := client.rpc(ctx, "eth_call", []any{callObject, "pending"}); err != nil {
		return domain.RedemptionSubmission{}, fmt.Errorf("simulate EOA redemption call: %w", err)
	}
	estimate, err := client.rpcUint64(ctx, "eth_estimateGas", []any{callObject, "pending"}, "redemption gas estimate")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	if estimate < redemptionMinGas || estimate > redemptionMaxGas {
		return domain.RedemptionSubmission{}, fmt.Errorf("redemption gas estimate %d is outside safe bounds", estimate)
	}
	gasLimit := (estimate*120 + 99) / 100
	if gasLimit > redemptionMaxGas {
		return domain.RedemptionSubmission{}, fmt.Errorf("redemption gas limit %d exceeds cap", gasLimit)
	}
	priorityFee, maxFee, err := client.redemptionFees(ctx)
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	requiredGas := new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), maxFee)
	balance, err := client.rpcBigInt(ctx, "eth_getBalance", []any{account.FunderAddress, "pending"}, "EOA native balance")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	if balance.Cmp(requiredGas) < 0 {
		return domain.RedemptionSubmission{}, fmt.Errorf("EOA native POL balance is below the maximum redemption gas budget")
	}
	data, err := decodeVariableHex(call.Data, "redemption call data")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	transaction := redemptionType2Transaction{
		nonce: latest, priorityFee: priorityFee, maxFee: maxFee,
		gasLimit: gasLimit, target: call.Target, data: data,
	}
	raw, transactionHash, err := signRedemptionType2(ctx, transaction, account)
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	sendResult, err := client.rpc(ctx, "eth_sendRawTransaction", []any{raw})
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	returnedHash, err := jsonRPCString(sendResult, "sent transaction hash")
	if err != nil || !strings.EqualFold(returnedHash, transactionHash) {
		return domain.RedemptionSubmission{}, fmt.Errorf("RPC returned a different transaction hash")
	}
	return domain.RedemptionSubmission{
		Provider: redemptionProviderPolygonRPC, Reference: strings.ToLower(transactionHash),
		TransactionHash: strings.ToLower(transactionHash), State: domain.RedemptionSubmissionPending,
	}, nil
}

func (client *RedemptionClient) redemptionFees(ctx context.Context) (*big.Int, *big.Int, error) {
	priority, err := client.rpcBigInt(ctx, "eth_maxPriorityFeePerGas", nil, "max priority fee")
	if err != nil {
		return nil, nil, err
	}
	blockResult, err := client.rpc(ctx, "eth_getBlockByNumber", []any{"pending", false})
	if err != nil {
		return nil, nil, err
	}
	var block struct {
		BaseFeePerGas string `json:"baseFeePerGas"`
	}
	if bytes.Equal(bytes.TrimSpace(blockResult), []byte("null")) || json.Unmarshal(blockResult, &block) != nil {
		return nil, nil, fmt.Errorf("pending block fee response is invalid")
	}
	baseFee, err := parseRPCQuantity(block.BaseFeePerGas, "pending base fee")
	if err != nil {
		return nil, nil, err
	}
	if priority.Sign() <= 0 || priority.Cmp(new(big.Int).SetUint64(redemptionMaxPriorityFee)) > 0 ||
		baseFee.Sign() <= 0 || baseFee.Cmp(new(big.Int).SetUint64(redemptionMaxBaseFee)) > 0 {
		return nil, nil, fmt.Errorf("Polygon gas fees exceed configured safety caps")
	}
	maxFee := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), priority)
	if maxFee.Cmp(new(big.Int).SetUint64(redemptionMaxFee)) > 0 {
		return nil, nil, fmt.Errorf("computed Polygon max fee exceeds configured safety cap")
	}
	return priority, maxFee, nil
}

func (client *RedemptionClient) resolveEOASubmission(
	ctx context.Context, submission domain.RedemptionSubmission,
) (domain.RedemptionSubmission, error) {
	hash := normalizeOptionalHash(submission.Reference)
	if hash == "" || (submission.TransactionHash != "" && !strings.EqualFold(hash, submission.TransactionHash)) {
		return domain.RedemptionSubmission{}, fmt.Errorf("EOA submission hash is invalid")
	}
	result, err := client.rpc(ctx, "eth_getTransactionReceipt", []any{hash})
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return domain.RedemptionSubmission{Provider: submission.Provider, Reference: hash, TransactionHash: hash, State: domain.RedemptionSubmissionPending}, nil
	}
	var receipt struct {
		TransactionHash string `json:"transactionHash"`
		BlockNumber     string `json:"blockNumber"`
		Status          string `json:"status"`
	}
	if json.Unmarshal(result, &receipt) != nil || !strings.EqualFold(receipt.TransactionHash, hash) {
		return domain.RedemptionSubmission{}, fmt.Errorf("EOA receipt identity is invalid")
	}
	status, err := parseRPCUint64(receipt.Status, "receipt status")
	if err != nil || status > 1 {
		return domain.RedemptionSubmission{}, fmt.Errorf("EOA receipt status is invalid")
	}
	if status == 0 {
		return domain.RedemptionSubmission{
			Provider: submission.Provider, Reference: hash, TransactionHash: hash,
			State: domain.RedemptionSubmissionFailed, FailureReason: "Polygon transaction reverted",
		}, nil
	}
	blockNumber, err := parseRPCUint64(receipt.BlockNumber, "receipt block number")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	latest, err := client.rpcUint64(ctx, "eth_blockNumber", nil, "latest block number")
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	state := domain.RedemptionSubmissionPending
	if latest >= blockNumber && latest-blockNumber+1 >= client.requiredConfirmations {
		state = domain.RedemptionSubmissionConfirmed
	}
	return domain.RedemptionSubmission{Provider: submission.Provider, Reference: hash, TransactionHash: hash, State: state}, nil
}

func (client *RedemptionClient) rpcBigInt(ctx context.Context, method string, params []any, label string) (*big.Int, error) {
	result, err := client.rpc(ctx, method, params)
	if err != nil {
		return nil, err
	}
	value, err := jsonRPCString(result, label)
	if err != nil {
		return nil, err
	}
	return parseRPCQuantity(value, label)
}

func (client *RedemptionClient) rpcUint64(ctx context.Context, method string, params []any, label string) (uint64, error) {
	value, err := client.rpcBigInt(ctx, method, params, label)
	if err != nil || !value.IsUint64() {
		return 0, fmt.Errorf("%s is not uint64", label)
	}
	return value.Uint64(), nil
}

func signRedemptionType2(
	ctx context.Context, transaction redemptionType2Transaction, account TradingAccount,
) (string, string, error) {
	to, ok := decodeAddress(transaction.target)
	if !ok || transaction.priorityFee == nil || transaction.maxFee == nil || transaction.gasLimit == 0 {
		return "", "", fmt.Errorf("redemption type-2 transaction is invalid")
	}
	unsigned := redemptionRLPList(
		redemptionRLPUint64(redemptionChainID), redemptionRLPUint64(transaction.nonce),
		redemptionRLPBigInt(transaction.priorityFee), redemptionRLPBigInt(transaction.maxFee),
		redemptionRLPUint64(transaction.gasLimit), redemptionRLPBytes(to), redemptionRLPBytes(nil),
		redemptionRLPBytes(transaction.data), redemptionRLPList(),
	)
	digest := keccak256([]byte{0x02}, unsigned)
	signature, err := account.Signer.SignDigest(ctx, digest)
	if err != nil {
		return "", "", err
	}
	if len(signature) != 65 || (signature[64] != 27 && signature[64] != 28) {
		return "", "", fmt.Errorf("redemption EOA signature is invalid")
	}
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:64])
	if r.Sign() <= 0 || s.Sign() <= 0 || s.Cmp(redemptionHalfOrder) > 0 {
		return "", "", fmt.Errorf("redemption EOA signature is not canonical")
	}
	if err := verifyRedemptionSigner(digest, signature, account.FunderAddress); err != nil {
		return "", "", err
	}
	signed := redemptionRLPList(
		redemptionRLPUint64(redemptionChainID), redemptionRLPUint64(transaction.nonce),
		redemptionRLPBigInt(transaction.priorityFee), redemptionRLPBigInt(transaction.maxFee),
		redemptionRLPUint64(transaction.gasLimit), redemptionRLPBytes(to), redemptionRLPBytes(nil),
		redemptionRLPBytes(transaction.data), redemptionRLPList(),
		redemptionRLPUint64(uint64(signature[64]-27)), redemptionRLPBigInt(r), redemptionRLPBigInt(s),
	)
	raw := append([]byte{0x02}, signed...)
	return "0x" + hex.EncodeToString(raw), "0x" + hex.EncodeToString(keccak256(raw)), nil
}

func verifyRedemptionSigner(digest, signature []byte, expected string) error {
	compact := make([]byte, 65)
	compact[0] = signature[64]
	copy(compact[1:], signature[:64])
	publicKey, _, err := secp256k1ecdsa.RecoverCompact(compact, digest)
	if err != nil {
		return err
	}
	serialized := publicKey.SerializeUncompressed()
	hash := keccak256(serialized[1:])
	recovered := "0x" + hex.EncodeToString(hash[len(hash)-20:])
	if !strings.EqualFold(recovered, expected) {
		return fmt.Errorf("redemption signature recovered a different EOA")
	}
	return nil
}

func redemptionRLPUint64(value uint64) []byte {
	if value == 0 {
		return redemptionRLPBytes(nil)
	}
	return redemptionRLPBigInt(new(big.Int).SetUint64(value))
}

func redemptionRLPBigInt(value *big.Int) []byte {
	if value == nil || value.Sign() == 0 {
		return redemptionRLPBytes(nil)
	}
	return redemptionRLPBytes(value.Bytes())
}

func redemptionRLPBytes(value []byte) []byte {
	if len(value) == 1 && value[0] < 0x80 {
		return append([]byte(nil), value...)
	}
	if len(value) <= 55 {
		return append([]byte{byte(0x80 + len(value))}, value...)
	}
	length := new(big.Int).SetUint64(uint64(len(value))).Bytes()
	result := append([]byte{byte(0xb7 + len(length))}, length...)
	return append(result, value...)
}

func redemptionRLPList(items ...[]byte) []byte {
	payload := bytes.Join(items, nil)
	if len(payload) <= 55 {
		return append([]byte{byte(0xc0 + len(payload))}, payload...)
	}
	length := new(big.Int).SetUint64(uint64(len(payload))).Bytes()
	result := append([]byte{byte(0xf7 + len(length))}, length...)
	return append(result, payload...)
}

func mustRedemptionBigInt(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 16)
	if !ok {
		panic("invalid redemption big integer")
	}
	return parsed
}

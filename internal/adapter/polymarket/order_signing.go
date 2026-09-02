package polymarket

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"golang.org/x/crypto/sha3"
)

const (
	ctfExchangeDomainName      = "Polymarket CTF Exchange"
	ctfExchangeDomainVersion   = "2"
	depositWalletDomainName    = "DepositWallet"
	depositWalletDomainVersion = "1"
	zeroBytes32                = "0x0000000000000000000000000000000000000000000000000000000000000000"
	polygonChainID             = int64(137)
	polygonExchangeV2          = "0xE111180000d2663C0091e4f400237545B87B996B"
	polygonNegRiskExchangeV2   = "0xe2222d279d744050d28e00520010520000310F59"
)

const (
	eip712DomainType  = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
	orderTypeString   = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)"
	typedDataSignType = "TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)" + orderTypeString
)

// signedOrderV2 表示后端使用的 signedOrderV2 类型。
type signedOrderV2 struct {
	Salt          json.Number `json:"salt"`
	Maker         string      `json:"maker"`
	Signer        string      `json:"signer"`
	TokenID       string      `json:"tokenId"`
	MakerAmount   string      `json:"makerAmount"`
	TakerAmount   string      `json:"takerAmount"`
	Side          string      `json:"side"`
	Expiration    string      `json:"expiration"`
	SignatureType uint8       `json:"signatureType"`
	Timestamp     string      `json:"timestamp"`
	Metadata      string      `json:"metadata"`
	Builder       string      `json:"builder"`
	Signature     string      `json:"signature"`
}

// postOrderPayload 表示后端使用的 postOrderPayload 类型。
type postOrderPayload struct {
	Order     signedOrderV2 `json:"order"`
	Owner     string        `json:"owner"`
	OrderType string        `json:"orderType"`
	DeferExec bool          `json:"deferExec"`
	PostOnly  bool          `json:"postOnly"`
}

// orderBuilder 表示后端使用的 orderBuilder 类型。
type orderBuilder struct {
	chainID        int64
	builderCode    string
	metadata       string
	minBuyNotional domain.Decimal
	now            func() time.Time
	random         io.Reader
}

// build 校验订单精度并构建签名后的 CLOB 提交载荷。
func (builder orderBuilder) build(ctx context.Context, order domain.Order, account TradingAccount) (postOrderPayload, string, error) {
	if order.MarketValidation == nil {
		return postOrderPayload{}, "", newInvalidError("MARKET_VALIDATION_REQUIRED", "persisted market validation is required before signing")
	}
	wireIntent, err := placementIntent(order)
	if err != nil {
		return postOrderPayload{}, "", err
	}
	amounts, err := buildRawAmounts(
		wireIntent,
		order.MarketValidation.TickSize,
		order.MarketValidation.MinOrderSize,
		builder.minBuyNotional,
	)
	if err != nil {
		return postOrderPayload{}, "", err
	}
	orderType, err := clobOrderType(order.Intent.TimeInForce)
	if err != nil {
		return postOrderPayload{}, "", err
	}
	salt, err := randomUint256(builder.random)
	if err != nil {
		return postOrderPayload{}, "", fmt.Errorf("generate order salt: %w", err)
	}
	timestamp := builder.now().UTC().UnixMilli()
	expiration := "0"
	if order.Intent.TimeInForce == domain.TimeInForceGTD {
		if order.Intent.ExpiresAt == nil || !order.Intent.ExpiresAt.After(builder.now().UTC()) {
			return postOrderPayload{}, "", newInvalidError("INVALID_EXPIRATION", "GTD expiration must be in the future")
		}
		expiration = fmt.Sprintf("%d", order.Intent.ExpiresAt.UTC().Unix())
	}
	metadata := normalizeBytes32(builder.metadata)
	builderCode := normalizeBytes32(builder.builderCode)
	if metadata == "" || builderCode == "" {
		return postOrderPayload{}, "", newInvalidError("INVALID_BYTES32", "metadata and builder code must be 32-byte hex values")
	}
	signerAddress := account.Signer.Address()
	if account.SignatureType == SignatureTypePolyEIP1271 {
		// Deposit Wallet orders are authored by the contract wallet. The EOA
		// controls it by signing the outer ERC-7739 TypedDataSign envelope.
		signerAddress = account.FunderAddress
	}
	exchange := polygonExchangeV2
	if order.MarketValidation.NegRisk {
		exchange = polygonNegRiskExchangeV2
	}
	digestInput := orderDigestInput{
		ChainID:       builder.chainID,
		Exchange:      exchange,
		Salt:          salt,
		Maker:         account.FunderAddress,
		Signer:        signerAddress,
		TokenID:       order.Intent.TokenID,
		MakerAmount:   amounts.MakerAmount,
		TakerAmount:   amounts.TakerAmount,
		Side:          amounts.Side,
		SignatureType: uint8(account.SignatureType),
		Timestamp:     timestamp,
		Metadata:      metadata,
		Builder:       builderCode,
	}
	orderIDDigest, err := orderDigest(digestInput)
	if err != nil {
		return postOrderPayload{}, "", fmt.Errorf("build order EIP-712 digest: %w", err)
	}
	signatureDigest := orderIDDigest
	var depositEnvelope *poly1271OrderEnvelope
	if account.SignatureType == SignatureTypePolyEIP1271 {
		depositEnvelope, err = buildPoly1271OrderEnvelope(digestInput)
		if err != nil {
			return postOrderPayload{}, "", fmt.Errorf("build deposit-wallet order envelope: %w", err)
		}
		signatureDigest = depositEnvelope.digest
	}
	signature, err := account.Signer.SignDigest(ctx, signatureDigest)
	if err != nil {
		return postOrderPayload{}, "", fmt.Errorf("sign order: %w", err)
	}
	if len(signature) != 65 {
		return postOrderPayload{}, "", fmt.Errorf("order signer returned %d bytes, want 65", len(signature))
	}
	if depositEnvelope != nil {
		signature = depositEnvelope.wrap(signature)
	}
	side := "BUY"
	if amounts.Side == 1 {
		side = "SELL"
	}
	return postOrderPayload{
		Order: signedOrderV2{
			Salt:          json.Number(salt.String()),
			Maker:         account.FunderAddress,
			Signer:        signerAddress,
			TokenID:       order.Intent.TokenID,
			MakerAmount:   amounts.MakerAmount,
			TakerAmount:   amounts.TakerAmount,
			Side:          side,
			Expiration:    expiration,
			SignatureType: uint8(account.SignatureType),
			Timestamp:     fmt.Sprintf("%d", timestamp),
			Metadata:      metadata,
			Builder:       builderCode,
			Signature:     "0x" + hex.EncodeToString(signature),
		},
		Owner:     account.API.Key,
		OrderType: orderType,
		DeferExec: false,
		PostOnly:  false,
	}, "0x" + hex.EncodeToString(orderIDDigest), nil
}

// orderDigestInput 表示后端使用的 orderDigestInput 类型。
type orderDigestInput struct {
	ChainID       int64
	Exchange      string
	Salt          *big.Int
	Maker         string
	Signer        string
	TokenID       string
	MakerAmount   string
	TakerAmount   string
	Side          uint8
	SignatureType uint8
	Timestamp     int64
	Metadata      string
	Builder       string
}

// orderDigest 构建订单签名或鉴权所需的规范化字节数据。
func orderDigest(input orderDigestInput) ([]byte, error) {
	hashes, err := buildOrderHashes(input)
	if err != nil {
		return nil, err
	}
	return hashes.digest, nil
}

type orderHashes struct {
	domainSeparator []byte
	contentsHash    []byte
	digest          []byte
}

func buildOrderHashes(input orderDigestInput) (orderHashes, error) {
	if input.ChainID <= 0 || input.Timestamp < 0 || input.Salt == nil || input.Salt.Sign() < 0 || input.Salt.BitLen() > 256 {
		return orderHashes{}, fmt.Errorf("chain id, timestamp, and salt must form valid unsigned values")
	}
	domainAddress, err := addressWord(input.Exchange)
	if err != nil {
		return orderHashes{}, err
	}
	domainSeparator := keccak256(concatWords(
		keccak256([]byte(eip712DomainType)),
		keccak256([]byte(ctfExchangeDomainName)),
		keccak256([]byte(ctfExchangeDomainVersion)),
		uint256Word(big.NewInt(input.ChainID)),
		domainAddress,
	))
	maker, err := addressWord(input.Maker)
	if err != nil {
		return orderHashes{}, err
	}
	signer, err := addressWord(input.Signer)
	if err != nil {
		return orderHashes{}, err
	}
	tokenID, err := parseUint256(input.TokenID)
	if err != nil {
		return orderHashes{}, fmt.Errorf("token id: %w", err)
	}
	makerAmount, err := parseUint256(input.MakerAmount)
	if err != nil {
		return orderHashes{}, fmt.Errorf("maker amount: %w", err)
	}
	takerAmount, err := parseUint256(input.TakerAmount)
	if err != nil {
		return orderHashes{}, fmt.Errorf("taker amount: %w", err)
	}
	metadata, err := bytes32Word(input.Metadata)
	if err != nil {
		return orderHashes{}, fmt.Errorf("metadata: %w", err)
	}
	builder, err := bytes32Word(input.Builder)
	if err != nil {
		return orderHashes{}, fmt.Errorf("builder: %w", err)
	}
	orderHash := keccak256(concatWords(
		keccak256([]byte(orderTypeString)),
		uint256Word(input.Salt), maker, signer,
		uint256Word(tokenID), uint256Word(makerAmount), uint256Word(takerAmount),
		uint256Word(new(big.Int).SetUint64(uint64(input.Side))),
		uint256Word(new(big.Int).SetUint64(uint64(input.SignatureType))),
		uint256Word(big.NewInt(input.Timestamp)), metadata, builder,
	))
	return orderHashes{
		domainSeparator: domainSeparator,
		contentsHash:    orderHash,
		digest:          keccak256([]byte{0x19, 0x01}, domainSeparator, orderHash),
	}, nil
}

type poly1271OrderEnvelope struct {
	digest          []byte
	domainSeparator []byte
	contentsHash    []byte
}

func buildPoly1271OrderEnvelope(input orderDigestInput) (*poly1271OrderEnvelope, error) {
	if input.SignatureType != uint8(SignatureTypePolyEIP1271) {
		return nil, fmt.Errorf("deposit-wallet envelope requires signature type %d", SignatureTypePolyEIP1271)
	}
	hashes, err := buildOrderHashes(input)
	if err != nil {
		return nil, err
	}
	depositWallet, err := addressWord(input.Signer)
	if err != nil {
		return nil, fmt.Errorf("deposit wallet signer: %w", err)
	}
	outerHash := keccak256(concatWords(
		keccak256([]byte(typedDataSignType)),
		hashes.contentsHash,
		keccak256([]byte(depositWalletDomainName)),
		keccak256([]byte(depositWalletDomainVersion)),
		uint256Word(big.NewInt(input.ChainID)),
		depositWallet,
		make([]byte, 32),
	))
	return &poly1271OrderEnvelope{
		digest:          keccak256([]byte{0x19, 0x01}, hashes.domainSeparator, outerHash),
		domainSeparator: hashes.domainSeparator,
		contentsHash:    hashes.contentsHash,
	}, nil
}

func (envelope *poly1271OrderEnvelope) wrap(signature []byte) []byte {
	length := make([]byte, 2)
	binary.BigEndian.PutUint16(length, uint16(len(orderTypeString)))
	wrapped := make([]byte, 0, len(signature)+64+len(orderTypeString)+2)
	wrapped = append(wrapped, signature...)
	wrapped = append(wrapped, envelope.domainSeparator...)
	wrapped = append(wrapped, envelope.contentsHash...)
	wrapped = append(wrapped, []byte(orderTypeString)...)
	wrapped = append(wrapped, length...)
	return wrapped
}

// concatWords 构建订单签名或鉴权所需的规范化字节数据。
func concatWords(words ...[]byte) []byte {
	result := make([]byte, 0, len(words)*32)
	for _, word := range words {
		result = append(result, word...)
	}
	return result
}

// uint256Word 构建订单签名或鉴权所需的规范化字节数据。
func uint256Word(value *big.Int) []byte {
	word := make([]byte, 32)
	valueBytes := value.Bytes()
	copy(word[len(word)-len(valueBytes):], valueBytes)
	return word
}

// parseUint256 解析 Uint 256。
func parseUint256(value string) (*big.Int, error) {
	parsed, ok := new(big.Int).SetString(strings.TrimSpace(value), 10)
	if !ok || parsed.Sign() < 0 || parsed.BitLen() > 256 {
		return nil, fmt.Errorf("invalid uint256 %q", value)
	}
	return parsed, nil
}

// addressWord 累加或记录 ress Word。
func addressWord(value string) ([]byte, error) {
	address, ok := decodeAddress(value)
	if !ok {
		return nil, fmt.Errorf("invalid address %q", value)
	}
	word := make([]byte, 32)
	copy(word[12:], address)
	return word, nil
}

// bytes32Word 构建订单签名或鉴权所需的规范化字节数据。
func bytes32Word(value string) ([]byte, error) {
	value = strings.TrimPrefix(strings.TrimSpace(value), "0x")
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return nil, fmt.Errorf("invalid bytes32 value")
	}
	return decoded, nil
}

// decodeAddress 解码并校验 Address。
func decodeAddress(value string) ([]byte, bool) {
	value = strings.TrimPrefix(strings.TrimSpace(value), "0x")
	if len(value) != 40 {
		return nil, false
	}
	decoded, err := hex.DecodeString(value)
	return decoded, err == nil
}

// keccak256 构建订单签名或鉴权所需的规范化字节数据。
func keccak256(parts ...[]byte) []byte {
	hash := sha3.NewLegacyKeccak256()
	for _, part := range parts {
		_, _ = hash.Write(part)
	}
	return hash.Sum(nil)
}

// randomUint256 returns a non-zero cryptographic salt that is also exactly
// representable by JSON/JavaScript number parsers. The official V2 clients
// serialize salt as a JSON number; using the full uint256 range can round the
// wire value and make CLOB hash a different order than the one we signed.
func randomUint256(reader io.Reader) (*big.Int, error) {
	if reader == nil {
		reader = rand.Reader
	}
	buffer := make([]byte, 7)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return nil, err
	}
	// IEEE-754 doubles exactly represent all integers through 2^53-1.
	buffer[0] &= 0x1f
	value := new(big.Int).SetBytes(buffer)
	if value.Sign() == 0 {
		value.SetInt64(1)
	}
	return value, nil
}

// normalizeBytes32 规范化 Bytes 32 的字段和表示。
func normalizeBytes32(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return zeroBytes32
	}
	value = strings.TrimPrefix(value, "0x")
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return ""
	}
	return "0x" + strings.ToLower(value)
}

// clobOrderType 将领域有效期类型映射为 CLOB 订单类型。
func clobOrderType(timeInForce domain.TimeInForce) (string, error) {
	switch timeInForce {
	case domain.TimeInForceGTC, domain.TimeInForceGTD, domain.TimeInForceFAK, domain.TimeInForceFOK:
		return string(timeInForce), nil
	case domain.TimeInForceIOC:
		return "", newInvalidError("ORDER_TYPE_UNSUPPORTED", "Polymarket CLOB has no IOC order type; use FAK explicitly")
	default:
		return "", newInvalidError("ORDER_TYPE_UNSUPPORTED", "unsupported Polymarket order type")
	}
}

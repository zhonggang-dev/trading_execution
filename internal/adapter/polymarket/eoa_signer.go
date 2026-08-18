package polymarket

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	secp256k1ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// EOASigner 表示后端使用的 EOASigner 类型。
type EOASigner struct {
	privateKey *secp256k1.PrivateKey
	address    string
}

// NewEOASigner 创建并初始化 EOA Signer。
func NewEOASigner(privateKeyHex string) (*EOASigner, error) {
	privateKeyHex = strings.TrimPrefix(strings.TrimSpace(privateKeyHex), "0x")
	privateKeyBytes, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("parse EOA private key: %w", err)
	}
	if len(privateKeyBytes) != 32 {
		return nil, fmt.Errorf("parse EOA private key: expected 32 bytes")
	}
	privateKey := secp256k1.PrivKeyFromBytes(privateKeyBytes)
	if privateKey.Key.IsZero() || !strings.EqualFold(hex.EncodeToString(privateKey.Serialize()), privateKeyHex) {
		return nil, fmt.Errorf("parse EOA private key: scalar is zero or outside secp256k1 range")
	}
	publicKey := privateKey.PubKey().SerializeUncompressed()
	addressHash := keccak256(publicKey[1:])
	return &EOASigner{
		privateKey: privateKey,
		address:    "0x" + hex.EncodeToString(addressHash[len(addressHash)-20:]),
	}, nil
}

// Address 返回当前签名器对应的钱包地址。
func (signer *EOASigner) Address() string { return signer.address }

// SignDigest 使用当前账户私钥签署指定摘要。
func (signer *EOASigner) SignDigest(ctx context.Context, digest []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(digest) != 32 {
		return nil, fmt.Errorf("EIP-712 digest must contain 32 bytes")
	}
	compact := secp256k1ecdsa.SignCompact(signer.privateKey, digest, false)
	if len(compact) != 65 || compact[0] < 27 || compact[0] > 28 {
		return nil, fmt.Errorf("sign EIP-712 digest: invalid compact recovery id")
	}
	// Decred emits header || R || S. Polymarket uses R || S || V with legacy
	// V=27/28, matching eth_account and the official TypeScript client.
	signature := make([]byte, 65)
	copy(signature[:64], compact[1:])
	signature[64] = compact[0]
	return signature, nil
}

package polymarket

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	secp256k1ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// TestEOASignatureRecoversConfiguredSigner 验证 EOA Signature Recovers Configured Signer 场景下的行为。
func TestEOASignatureRecoversConfiguredSigner(t *testing.T) {
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	digest, err := orderDigest(orderDigestInput{
		ChainID:       polygonChainID,
		Exchange:      polygonExchangeV2,
		Salt:          big.NewInt(7),
		Maker:         signer.Address(),
		Signer:        signer.Address(),
		TokenID:       "123456789012345678901234567890",
		MakerAmount:   "5000000",
		TakerAmount:   "10000000",
		Side:          0,
		SignatureType: 0,
		Timestamp:     1776672000000,
		Metadata:      zeroBytes32,
		Builder:       zeroBytes32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(digest), "19b66b8bc04ef181173c98ec88f1f838b949d50956580a8e91e8b8d1b6082cb0"; got != want {
		t.Fatalf("EIP-712 digest = %s, want frozen V2 vector %s", got, want)
	}
	signature, err := signer.SignDigest(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	if signature[64] != 27 && signature[64] != 28 {
		t.Fatalf("signature recovery id = %d", signature[64])
	}
	compact := make([]byte, 65)
	compact[0] = signature[64]
	copy(compact[1:], signature[:64])
	publicKey, _, err := secp256k1ecdsa.RecoverCompact(compact, digest)
	if err != nil {
		t.Fatal(err)
	}
	publicKeyBytes := publicKey.SerializeUncompressed()
	addressHash := keccak256(publicKeyBytes[1:])
	if recovered := "0x" + hex.EncodeToString(addressHash[len(addressHash)-20:]); recovered != signer.Address() {
		t.Fatalf("recovered %s, want %s (digest %s)", recovered, signer.Address(), hex.EncodeToString(digest))
	}
}

// TestHMACSignatureMatchesCanonicalVector 验证 HMAC Signature Matches Canonical Vector 场景下的行为。
func TestHMACSignatureMatchesCanonicalVector(t *testing.T) {
	got, err := hmacSignature("dGVzdC1zZWNyZXQ=", 1776672000, "POST", "/order", []byte(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	const want = "NrfG-yJbupN3jlkI_Dl8k2XL6Sl6ymMpln93E3svd6Y="
	if got != want {
		t.Fatalf("hmacSignature() = %s, want %s", got, want)
	}
}

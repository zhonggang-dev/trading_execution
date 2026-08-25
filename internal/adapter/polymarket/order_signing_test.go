package polymarket

import (
	"bytes"
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	secp256k1ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

func TestRandomOrderSaltFitsJSONSafeInteger(t *testing.T) {
	salt, err := randomUint256(bytes.NewReader(bytes.Repeat([]byte{0xff}, 7)))
	if err != nil {
		t.Fatal(err)
	}
	maxSafeInteger := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 53), big.NewInt(1))
	if salt.Sign() <= 0 || salt.Cmp(maxSafeInteger) > 0 {
		t.Fatalf("salt = %s, want 1..%s", salt, maxSafeInteger)
	}
}

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

func TestPoly1271SignatureMatchesOfficialSDKVector(t *testing.T) {
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	const depositWallet = "0x6F908B3B67b9F9C40413775fFf48b286aeF9a081"
	input := orderDigestInput{
		ChainID:       polygonChainID,
		Exchange:      polygonExchangeV2,
		Salt:          big.NewInt(7),
		Maker:         depositWallet,
		Signer:        depositWallet,
		TokenID:       "123456789012345678901234567890",
		MakerAmount:   "5000000",
		TakerAmount:   "10000000",
		Side:          0,
		SignatureType: uint8(SignatureTypePolyEIP1271),
		Timestamp:     1776672000000,
		Metadata:      zeroBytes32,
		Builder:       zeroBytes32,
	}
	orderID, err := orderDigest(input)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(orderID), "c322677dd9c7f400843bb535d0970202565e8d77ed51b98872e598b52008eaf9"; got != want {
		t.Fatalf("order ID digest = %s, want official SDK vector %s", got, want)
	}

	envelope, err := buildPoly1271OrderEnvelope(input)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(envelope.digest), "815fb93c9911c44ab966795387999c5bdf1359acb055d783adb1a342b82cbdb2"; got != want {
		t.Fatalf("outer digest = %s, want official SDK vector %s", got, want)
	}
	rawSignature, err := signer.SignDigest(context.Background(), envelope.digest)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := envelope.wrap(rawSignature)
	if got, want := len(wrapped), 317; got != want {
		t.Fatalf("wrapped signature length = %d, want %d", got, want)
	}
	if got, want := hex.EncodeToString(keccak256(wrapped)), "031e666981905b23bf7eaea5bb24f2c58016cd0fb048f0f3bbf83bd2ecc52719"; got != want {
		t.Fatalf("wrapped signature hash = %s, want official SDK vector %s", got, want)
	}
	if got, want := string(wrapped[129:len(wrapped)-2]), orderTypeString; got != want {
		t.Fatalf("wrapped contents type = %q, want %q", got, want)
	}
	if got, want := wrapped[len(wrapped)-2:], []byte{0x00, 0xba}; !bytes.Equal(got, want) {
		t.Fatalf("wrapped contents type length = %x, want %x", got, want)
	}
}

func TestPoly1271EnvelopeRejectsNonDepositSignatureType(t *testing.T) {
	_, err := buildPoly1271OrderEnvelope(orderDigestInput{SignatureType: uint8(SignatureTypeEOA)})
	if err == nil {
		t.Fatal("buildPoly1271OrderEnvelope() error = nil, want signature-type rejection")
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

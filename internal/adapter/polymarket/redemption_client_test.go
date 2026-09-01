package polymarket

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"
)

func TestDepositWalletBatchDigestMatchesEthAccountVector(t *testing.T) {
	call, err := setApprovalForAllCall(standardCollateralAdapter)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := depositWalletBatchDigest(
		"0x0000000000000000000000000000000000001234",
		big.NewInt(3), big.NewInt(1_000_000), []redemptionCall{call},
	)
	if err != nil {
		t.Fatal(err)
	}
	const expectedDigest = "32faa893843148413d267d1d3232ec96398e008cc377bad9fcf5d2c20eceecbb"
	if hex.EncodeToString(digest) != expectedDigest {
		t.Fatalf("Deposit Wallet digest = %x, want %s", digest, expectedDigest)
	}
	signer, err := NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signer.SignDigest(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	const expectedSignature = "83d1f5c583bc772fc81f27c988e3664f7ee5646f448185f3d05ff186f50b3ea95658ae88db5e43db48082687a99760b5f907ab9fe2ed3ef75448b6da405f2a731b"
	if hex.EncodeToString(signature) != expectedSignature {
		t.Fatalf("Deposit Wallet signature = %x, want %s", signature, expectedSignature)
	}
}

func TestRedeemPositionsCallTargetsExactAdapterAndBinaryPartition(t *testing.T) {
	condition := "0x9601280c3d5109783ba64644da25bdfdf120ce516a696f88a42f54ffd2ac761b"
	call, err := redeemPositionsCall(negRiskCollateralAdapter, condition)
	if err != nil {
		t.Fatal(err)
	}
	if call.Target != negRiskCollateralAdapter || call.Value != "0" {
		t.Fatalf("redeem target/value = %s/%s", call.Target, call.Value)
	}
	data, err := decodeVariableHex(call.Data, "redeem data")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 4+7*32 {
		t.Fatalf("redeem calldata length = %d", len(data))
	}
	wantSelector := keccak256([]byte("redeemPositions(address,bytes32,bytes32,uint256[])"))[:4]
	if hex.EncodeToString(data[:4]) != hex.EncodeToString(wantSelector) {
		t.Fatalf("redeem selector = %x", data[:4])
	}
	if new(big.Int).SetBytes(data[4+3*32:4+4*32]).Uint64() != 128 ||
		new(big.Int).SetBytes(data[4+4*32:4+5*32]).Uint64() != 2 ||
		new(big.Int).SetBytes(data[4+5*32:4+6*32]).Uint64() != 1 ||
		new(big.Int).SetBytes(data[4+6*32:]).Uint64() != 2 {
		t.Fatalf("redeem binary index set encoding is invalid")
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

func TestBuildApproveCallDataIsFixedExactAmount(t *testing.T) {
	data, err := buildApproveCallData(standardExchangeV2Address, approvalAmount)
	if err != nil {
		t.Fatal(err)
	}
	want := "095ea7b3" + strings.Repeat("0", 24) + strings.TrimPrefix(standardExchangeV2Address, "0x") +
		strings.Repeat("0", 56) + "0bebc200"
	if got := hex.EncodeToString(data); got != want {
		t.Fatalf("approve calldata = %s, want %s", got, want)
	}
}

func TestRLPMatchesCanonicalVectors(t *testing.T) {
	if got := hex.EncodeToString(rlpBytes(nil)); got != "80" {
		t.Fatalf("RLP empty string = %s", got)
	}
	if got := hex.EncodeToString(rlpBytes([]byte("dog"))); got != "83646f67" {
		t.Fatalf("RLP dog = %s", got)
	}
	list := rlpList(rlpBytes([]byte("cat")), rlpBytes([]byte("dog")))
	if got := hex.EncodeToString(list); got != "c88363617483646f67" {
		t.Fatalf("RLP [cat,dog] = %s", got)
	}
	long := bytes.Repeat([]byte{0x42}, 56)
	if got := hex.EncodeToString(rlpBytes(long)); !strings.HasPrefix(got, "b838") {
		t.Fatalf("RLP 56-byte prefix = %s", got[:4])
	}
}

func TestSignType2ApprovalIsDeterministicAndRecoversSigner(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	account := approvalAccount{executionAccountID: "test", address: signer.Address(), signer: signer}
	transaction, err := newApprovalTransaction(
		7, 60_000, big.NewInt(30_000_000_000), big.NewInt(100_000_000_000), standardExchangeV2Address,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := signType2Approval(context.Background(), transaction, account)
	if err != nil {
		t.Fatal(err)
	}
	second, err := signType2Approval(context.Background(), transaction, account)
	if err != nil {
		t.Fatal(err)
	}
	if first.raw != second.raw || first.hash != second.hash || first.digest != second.digest {
		t.Fatal("type-2 signing is not deterministic")
	}
	if !strings.HasPrefix(first.raw, "0x02") || len(first.hash) != 66 || len(first.digest) != 66 {
		t.Fatalf("signed transaction identity = %#v", first)
	}
	if first.yParity > 1 || first.r.Sign() <= 0 || first.s.Sign() <= 0 || first.s.Cmp(secp256k1HalfOrder) > 0 {
		t.Fatalf("signature components = y=%d r=%s s=%s", first.yParity, first.r, first.s)
	}
	const wantDigest = "0x4809d4bd25d36a97050c709952e3a98f181b741230abb7cf12bf8e0a2faab3e0"
	const wantRaw = "0x02f8b28189078506fc23ac0085174876e80082ea6094c011a7e12a19f7b1f670d46f03b03f3342e82dfb80b844095ea7b3000000000000000000000000e111180000d2663c0091e4f400237545b87b996b000000000000000000000000000000000000000000000000000000000bebc200c001a0075e043b618ef025a373478496702859344adff72fe510af94f1d8fd4a03b5f6a03c9587473282cd43194f0727b815a3df10273b5ff770404befc34e4bfb4f00b7"
	const wantHash = "0x3d13ee451fc561d80e17d44ceaabee384832e76a58b6943392c2e231dc16b1c5"
	if first.digest != wantDigest || first.raw != wantRaw || first.hash != wantHash {
		t.Fatalf("type-2 frozen vector = digest %s raw %s hash %s", first.digest, first.raw, first.hash)
	}
}

func TestSignType2ApprovalRejectsSignerAddressMismatch(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := newApprovalTransaction(0, 60_000, big.NewInt(1), big.NewInt(2), standardExchangeV2Address)
	if err != nil {
		t.Fatal(err)
	}
	account := approvalAccount{address: "0x1111111111111111111111111111111111111111", signer: signer}
	if _, err := signType2Approval(context.Background(), transaction, account); err == nil || !strings.Contains(err.Error(), "recovered") {
		t.Fatalf("signType2Approval() error = %v", err)
	}
}

func TestNewApprovalTransactionRejectsFeesAboveFixedCaps(t *testing.T) {
	tests := []struct {
		name     string
		priority *big.Int
		maxFee   *big.Int
		want     string
	}{
		{
			name:     "priority",
			priority: new(big.Int).Add(new(big.Int).SetUint64(maximumPriorityFeeWei), big.NewInt(1)),
			maxFee:   new(big.Int).Add(new(big.Int).SetUint64(maximumPriorityFeeWei), big.NewInt(1)),
			want:     "priority fee",
		},
		{
			name:     "max fee",
			priority: big.NewInt(1),
			maxFee:   new(big.Int).Add(new(big.Int).SetUint64(maximumMaxFeePerGasWei), big.NewInt(1)),
			want:     "max fee",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newApprovalTransaction(0, 60_000, test.priority, test.maxFee, standardExchangeV2Address)
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), "exceeds cap") {
				t.Fatalf("newApprovalTransaction() error = %v", err)
			}
		})
	}
}

package main

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

func TestBuildTransferCallData(t *testing.T) {
	data, err := buildTransferCallData(wallet6Deposit, big.NewInt(20_000_000))
	if err != nil {
		t.Fatal(err)
	}
	want := "a9059cbb000000000000000000000000635d25519789e40c3794d72a88cbb7f25ac443f80000000000000000000000000000000000000000000000000000000001312d00"
	if got := hex.EncodeToString(data); got != want {
		t.Fatalf("transfer calldata = %s, want %s", got, want)
	}
}

func TestSignFixedTransferIsDeterministic(t *testing.T) {
	signer, err := polymarket.NewEOASigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := newTransferTransaction(9, 70_000, big.NewInt(30_000_000_000), big.NewInt(70_000_000_000), wallet6Deposit, big.NewInt(20_000_000))
	if err != nil {
		t.Fatal(err)
	}
	first, err := signType2Transaction(context.Background(), transaction, signer, signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	second, err := signType2Transaction(context.Background(), transaction, signer, signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first.hash) != 66 || len(first.raw) <= 2 || len(first.digest) != 66 {
		t.Fatalf("signed transfer is not deterministic: %#v / %#v", first, second)
	}
	if got, want := first.hash, "0x5314c3be825acff10ed4944b1fa4c306699ba7ebef2a4cd9646a5eb6c4c6fbd0"; got != want {
		t.Fatalf("signed transfer hash = %s, want eth_account vector %s", got, want)
	}
	if got, want := first.raw, "0x02f8b38189098506fc23ac0085104c533c008301117094c011a7e12a19f7b1f670d46f03b03f3342e82dfb80b844a9059cbb000000000000000000000000635d25519789e40c3794d72a88cbb7f25ac443f80000000000000000000000000000000000000000000000000000000001312d00c001a09f0da644d7e15e0d6724c41b05cc766e52455519e5e93ba65dc81b87cdae71b5a035bcea5bc6acb8fa6b39d1434c127fec47adfc62f7513fda162fb35e131a54e7"; got != want {
		t.Fatalf("signed transfer raw transaction differs from eth_account vector")
	}
}

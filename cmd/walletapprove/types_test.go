package main

import (
	"context"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

type addressOnlySigner string

func (signer addressOnlySigner) Address() string { return string(signer) }
func (addressOnlySigner) SignDigest(context.Context, []byte) ([]byte, error) {
	return nil, nil
}

func fixedTradingAccounts() []polymarket.TradingAccount {
	return []polymarket.TradingAccount{
		{
			ExecutionAccountID: wallet6ExecutionAccountID,
			FunderAddress:      wallet6ExpectedAddress,
			SignatureType:      polymarket.SignatureTypeEOA,
			Signer:             addressOnlySigner(wallet6ExpectedAddress),
		},
		{
			ExecutionAccountID: wallet7ExecutionAccountID,
			FunderAddress:      wallet7ExpectedAddress,
			SignatureType:      polymarket.SignatureTypeEOA,
			Signer:             addressOnlySigner(wallet7ExpectedAddress),
		},
	}
}

func TestSelectApprovalAccountsRequiresOnlyExactWallet67EOAs(t *testing.T) {
	accounts := append(fixedTradingAccounts(), polymarket.TradingAccount{ExecutionAccountID: "main"})
	selected, err := selectApprovalAccounts(accounts)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[wallet6ExecutionAccountID].address != wallet6ExpectedAddress ||
		selected[wallet7ExecutionAccountID].address != wallet7ExpectedAddress {
		t.Fatalf("selected accounts = %#v", selected)
	}

	wrongAddress := fixedTradingAccounts()
	wrongAddress[0].FunderAddress = "0x1111111111111111111111111111111111111111"
	wrongAddress[0].Signer = addressOnlySigner(wrongAddress[0].FunderAddress)
	if _, err := selectApprovalAccounts(wrongAddress); err == nil || !strings.Contains(err.Error(), "fixed address") {
		t.Fatalf("wrong-address error = %v", err)
	}

	proxy := fixedTradingAccounts()
	proxy[1].SignatureType = polymarket.SignatureTypePolyProxy
	if _, err := selectApprovalAccounts(proxy); err == nil || !strings.Contains(err.Error(), "EOA") {
		t.Fatalf("proxy error = %v", err)
	}
}

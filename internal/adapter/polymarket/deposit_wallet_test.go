package polymarket

import (
	"strings"
	"testing"
)

func TestDeriveDepositWalletAddressesMatchesOfficialSDK(t *testing.T) {
	tests := []struct {
		name   string
		signer string
		uups   string
		beacon string
	}{
		{
			name:   "independent SDK vector",
			signer: "0xc8B82670FB1f9eA8F0F2875571aA008B140e57D9",
			uups:   "0x79F40C10F74F9ae61D37d89e3e7e66c01e8F12C5",
			beacon: "0x35dd7dC37AD358037cF50644Ae2042a6B4468cB6",
		},
		{
			name:   "wallet 6",
			signer: "0x0aeFD80dF02cC35E81AedE40B34e2e961BB4593F",
			uups:   "0x8247fE6e4e0b75a0F4ED4979c72285df8020c502",
			beacon: "0x635D25519789E40C3794D72a88CBb7F25AC443f8",
		},
		{
			name:   "wallet 7",
			signer: "0xc9bA353781F13ec9507BC0677156814d805FE6d9",
			uups:   "0xdCEA447ba0845799c90273fF810e05EFE823F057",
			beacon: "0xdD9275D3b1D2c423e19724FcD09C19ABb20aa167",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			uups, beacon, err := deriveDepositWalletAddresses(test.signer)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.EqualFold(uups, test.uups) {
				t.Fatalf("UUPS address = %s, want official SDK vector %s", uups, test.uups)
			}
			if !strings.EqualFold(beacon, test.beacon) {
				t.Fatalf("beacon address = %s, want official SDK vector %s", beacon, test.beacon)
			}
		})
	}
}

func TestValidateDepositWalletAddressAcceptsOnlyDeterministicWallet(t *testing.T) {
	const signer = "0x0aeFD80dF02cC35E81AedE40B34e2e961BB4593F"
	for _, wallet := range []string{
		"0x8247fE6e4e0b75a0F4ED4979c72285df8020c502",
		"0x635D25519789E40C3794D72a88CBb7F25AC443f8",
	} {
		if err := validateDepositWalletAddress(signer, wallet); err != nil {
			t.Fatalf("validateDepositWalletAddress(%s) error = %v", wallet, err)
		}
	}
	for _, wallet := range []string{
		signer,
		"0x1111111111111111111111111111111111111111",
	} {
		if err := validateDepositWalletAddress(signer, wallet); err == nil {
			t.Fatalf("validateDepositWalletAddress(%s) error = nil, want rejection", wallet)
		}
	}
}

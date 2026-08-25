package polymarket

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testPrivateKey = "0000000000000000000000000000000000000000000000000000000000000001"
	testEOAAddress = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
)

// TestLoadTradingAccountsAcceptsLegacyWalletMap 验证 Load Trading Accounts Accepts Legacy Wallet Map 场景下的行为。
func TestLoadTradingAccountsAcceptsLegacyWalletMap(t *testing.T) {
	path := writeWalletFile(t, `{
		"wallet-7": {
			"address": "0x7E5F4552091A69125D5DFCB7B8C2659029395BDF",
			"private_key": "`+testPrivateKey+`",
			"api_key": "key",
			"api_secret": "c2VjcmV0",
			"api_passphrase": "passphrase",
			"clob_host": "https://clob.polymarket.com",
			"chain_id": 137
		}
	}`)

	accounts, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 {
		t.Fatalf("accounts = %d, want 1", len(accounts))
	}
	account := accounts[0]
	if account.ExecutionAccountID != "wallet-7" || account.SignatureType != SignatureTypeEOA {
		t.Fatalf("account identity = %#v", account)
	}
	if account.Signer.Address() != testEOAAddress || account.FunderAddress != testEOAAddress {
		t.Fatalf("signer/funder = %s/%s", account.Signer.Address(), account.FunderAddress)
	}
}

// TestLoadTradingAccountsAcceptsAccountEnvelope 验证 Load Trading Accounts Accepts Account Envelope 场景下的行为。
func TestLoadTradingAccountsAcceptsAccountEnvelope(t *testing.T) {
	path := writeWalletFile(t, `{"accounts":[{
		"execution_account_id":"model-a/strategy-v2",
		"funder_address":"0x1111111111111111111111111111111111111111",
		"private_key":"`+testPrivateKey+`",
		"signature_type":2,
		"api_key":"key",
		"api_secret":"c2VjcmV0",
		"api_passphrase":"passphrase"
	}]}`)
	accounts, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if accounts[0].SignatureType != SignatureTypeGnosisSafe {
		t.Fatalf("signature type = %d", accounts[0].SignatureType)
	}
	if accounts[0].ExecutionAccountID != "model-a/strategy-v2" {
		t.Fatalf("execution account id = %q", accounts[0].ExecutionAccountID)
	}
}

func TestLoadTradingAccountsAcceptsDeterministicDepositWallet(t *testing.T) {
	path := writeWalletFile(t, `{"accounts":[{
		"execution_account_id":"wallet-6",
		"funder_address":"0x6F908B3B67b9F9C40413775fFf48b286aeF9a081",
		"private_key":"`+testPrivateKey+`",
		"signature_type":3,
		"api_key":"key",
		"api_secret":"c2VjcmV0",
		"api_passphrase":"passphrase"
	}]}`)
	accounts, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if accounts[0].SignatureType != SignatureTypePolyEIP1271 ||
		!strings.EqualFold(accounts[0].FunderAddress, "0x6F908B3B67b9F9C40413775fFf48b286aeF9a081") ||
		!strings.EqualFold(accounts[0].Signer.Address(), testEOAAddress) {
		t.Fatalf("deposit-wallet account = %#v", accounts[0])
	}
}

func TestLoadTradingAccountsRejectsForeignDepositWallet(t *testing.T) {
	path := writeWalletFile(t, `{"accounts":[{
		"execution_account_id":"wallet-6",
		"funder_address":"0x1111111111111111111111111111111111111111",
		"private_key":"`+testPrivateKey+`",
		"signature_type":3,
		"api_key":"key",
		"api_secret":"c2VjcmV0",
		"api_passphrase":"passphrase"
	}]}`)
	_, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err == nil || !strings.Contains(err.Error(), "deterministic deposit wallet") {
		t.Fatalf("error = %v", err)
	}
}

// TestLoadTradingAccountsRequiresExplicitProxySignatureType 验证 Load Trading Accounts Requires Explicit Proxy Signature Type 场景下的行为。
func TestLoadTradingAccountsRequiresExplicitProxySignatureType(t *testing.T) {
	path := writeWalletFile(t, `{"wallet":{
		"address":"0x1111111111111111111111111111111111111111",
		"private_key":"`+testPrivateKey+`",
		"api_key":"key",
		"api_secret":"c2VjcmV0",
		"api_passphrase":"passphrase"
	}}`)
	_, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err == nil || !strings.Contains(err.Error(), "signature_type is required") {
		t.Fatalf("error = %v", err)
	}
}

// TestLoadTradingAccountsRequiresCompleteAPICredentials 验证 Load Trading Accounts Requires Complete API Credentials 场景下的行为。
func TestLoadTradingAccountsRequiresCompleteAPICredentials(t *testing.T) {
	path := writeWalletFile(t, `{"wallet":{
		"private_key":"`+testPrivateKey+`",
		"api_key":"only-one-value"
	}}`)
	_, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err == nil || !strings.Contains(err.Error(), "must be provided together") {
		t.Fatalf("error = %v", err)
	}
}

// TestLoadTradingAccountsDoesNotBootstrapByDefault 验证 Load Trading Accounts Does Not Bootstrap By Default 场景下的行为。
func TestLoadTradingAccountsDoesNotBootstrapByDefault(t *testing.T) {
	path := writeWalletFile(t, `{"wallet":{"private_key":"`+testPrivateKey+`"}}`)
	_, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err == nil || !strings.Contains(err.Error(), "explicitly enable credential bootstrap") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadTradingAccountsRejectsWorldReadableSecret(t *testing.T) {
	path := writeWalletFile(t, `{"wallet":{"private_key":"`+testPrivateKey+`"}}`)
	if err := os.Chmod(path, 0o604); err != nil {
		t.Fatal(err)
	}
	_, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err == nil || !strings.Contains(err.Error(), "group or other users") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadTradingAccountsRejectsGroupReadableSecret(t *testing.T) {
	path := writeWalletFile(t, `{"wallet":{"private_key":"`+testPrivateKey+`"}}`)
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	_, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: path})
	if err == nil || !strings.Contains(err.Error(), "group or other users") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadTradingAccountsRejectsSymlink(t *testing.T) {
	target := writeWalletFile(t, `{"wallet":{"private_key":"`+testPrivateKey+`"}}`)
	link := filepath.Join(t.TempDir(), "wallets-link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, err := LoadTradingAccounts(context.Background(), WalletLoadParams{Path: link})
	if err == nil || !strings.Contains(err.Error(), "non-symlink") {
		t.Fatalf("error = %v", err)
	}
}

// writeWalletFile 实现当前测试场景所需的辅助行为。
func writeWalletFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wallets.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

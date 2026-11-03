package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/service/walletpnl"
)

func TestWalletAddressRepositoryIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	repository, err := NewWalletAddressRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	// Public funder identities are deliberately distinct from the associated
	// profile proxy and signing wallets. Read the selected account's funder.
	for _, account := range []struct{ id, funder string }{
		{"main", "0x6c07b5e271ffa2da038ce05e498cff856bf32357"},
		{"wallet-1", "0x7b14065318fc2f00e2c971bd4cb3770eda7f6d17"},
		{"wallet-6", "0x635d25519789e40c3794d72a88cbb7f25ac443f8"},
		{"wallet-7", "0xdd9275d3b1d2c423e19724fcd09c19abb20aa167"},
	} {
		insertAccount(t, db, account.id, account.funder, "100", "100", "0")
		address, err := repository.WalletAddress(context.Background(), account.id)
		if err != nil {
			t.Fatal(err)
		}
		if address != account.funder {
			t.Fatalf("account %s address = %q, want funder %q", account.id, address, account.funder)
		}
	}
	if _, err := repository.WalletAddress(context.Background(), "missing-wallet"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing account error = %v, want sql.ErrNoRows", err)
	}
}

func TestWalletPnLAccountDiscoveryRegistrationIntegration(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	repository, err := NewWalletAddressRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	insertAccount(t, db, "portfolio-a", "0x1111111111111111111111111111111111111111", "100", "100", "0")
	insertAccount(t, db, "kalshi:portfolio-b", "kalshi:portfolio-b", "100", "100", "0")
	service, err := walletpnl.New(walletpnl.Params{Addresses: repository, Source: noDiscoveryPnLSource{}, Accounts: []string{"portfolio-a"}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.ListAccounts(context.Background())
	if err != nil || len(result.Accounts) != 1 || result.Accounts[0].ExecutionAccountID != "portfolio-a" {
		t.Fatalf("result=%#v error=%v", result, err)
	}
	if _, err := db.Exec(`UPDATE execution_accounts SET wallet_address='invalid-funder' WHERE execution_account_id='portfolio-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListAccounts(context.Background()); !errors.Is(err, walletpnl.ErrUnavailable) {
		t.Fatalf("invalid address error=%v, want unavailable", err)
	}
	if _, err := db.Exec(`DELETE FROM execution_accounts WHERE execution_account_id='portfolio-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListAccounts(context.Background()); !errors.Is(err, walletpnl.ErrUnavailable) {
		t.Fatalf("missing registration error=%v, want unavailable", err)
	}
}

type noDiscoveryPnLSource struct{}

func (noDiscoveryPnLSource) Series(context.Context, string, string, string) (polymarketdata.PnLSeries, error) {
	panic("account discovery must not request external PnL data")
}

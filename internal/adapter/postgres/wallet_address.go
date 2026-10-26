package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

// WalletAddressRepository reads the authoritative execution_accounts identity.
type WalletAddressRepository struct{ db *sql.DB }

func NewWalletAddressRepository(db *sql.DB) (*WalletAddressRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &WalletAddressRepository{db: db}, nil
}

func (repository *WalletAddressRepository) WalletAddress(ctx context.Context, executionAccountID string) (string, error) {
	var address string
	err := repository.db.QueryRowContext(ctx, `
		SELECT LOWER(wallet_address)
		FROM execution_accounts
		WHERE execution_account_id=$1`, strings.TrimSpace(executionAccountID)).Scan(&address)
	if err != nil {
		return "", err
	}
	address = strings.TrimSpace(address)
	if len(address) != 42 || !strings.HasPrefix(address, "0x") {
		return "", fmt.Errorf("execution account wallet address is invalid")
	}
	if _, err := hex.DecodeString(address[2:]); err != nil {
		return "", fmt.Errorf("execution account wallet address is invalid")
	}
	return address, nil
}

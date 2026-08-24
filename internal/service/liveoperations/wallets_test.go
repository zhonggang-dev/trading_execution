package liveoperations

import (
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestBuildWalletsKeepsAccountsIsolatedAndExcludesUnmanagedPositions(t *testing.T) {
	accounts := []domain.LiveAccountState{
		{ExecutionAccountID: "wallet-b"},
		{ExecutionAccountID: "wallet-a"},
	}
	accounting := []domain.LiveWalletAccountingState{
		{ExecutionAccountID: "wallet-a", PeakCashUsed: "10", CumulativeInvestedCost: "14", RealizedPnL: "2"},
		{ExecutionAccountID: "wallet-b", PeakCashUsed: "0", CumulativeInvestedCost: "0", RealizedPnL: "0"},
	}
	positions := []domain.LivePosition{
		{ExecutionAccountID: "wallet-a", TokenID: "managed-a", UnrealizedPnL: "3"},
		{ExecutionAccountID: "wallet-a", TokenID: "external-only", UnrealizedPnL: "99"},
	}
	managed := []domain.LiveLedgerPosition{{Position: domain.Position{
		ExecutionAccountID: "wallet-a", TokenID: "managed-a",
	}}}

	wallets, err := buildWallets(accounts, accounting, positions, managed)
	if err != nil {
		t.Fatalf("buildWallets() error = %v", err)
	}
	if len(wallets) != 2 || wallets[0].ExecutionAccountID != "wallet-a" || wallets[1].ExecutionAccountID != "wallet-b" {
		t.Fatalf("wallet order = %#v", wallets)
	}
	first := wallets[0]
	if first.PositionCount != 1 || first.UnrealizedPnL != "3" || first.TotalPnL != "5" ||
		first.ReturnRate == nil || *first.ReturnRate != "0.5" {
		t.Fatalf("wallet-a = %#v", first)
	}
	if wallets[1].PositionCount != 0 || wallets[1].TotalPnL != "0" || wallets[1].ReturnRate != nil {
		t.Fatalf("wallet-b = %#v", wallets[1])
	}
}

func TestBuildWalletsFailsClosedWhenAccountingIsMissing(t *testing.T) {
	_, err := buildWallets(
		[]domain.LiveAccountState{{ExecutionAccountID: "wallet-a"}},
		nil,
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("buildWallets() error = nil, want missing accounting error")
	}
}

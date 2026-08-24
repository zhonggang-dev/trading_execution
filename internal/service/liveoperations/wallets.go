package liveoperations

import (
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// buildWallets 将事件账本的累计口径与当前已验真管理仓位合并为逐钱包绩效。
func buildWallets(
	accounts []domain.LiveAccountState,
	accounting []domain.LiveWalletAccountingState,
	positions []domain.LivePosition,
	managedPositions []domain.LiveLedgerPosition,
) ([]domain.LiveWallet, error) {
	accountingByID := make(map[string]domain.LiveWalletAccountingState, len(accounting))
	for _, item := range accounting {
		accountID := strings.TrimSpace(item.ExecutionAccountID)
		if accountID == "" {
			return nil, fmt.Errorf("wallet accounting execution account id is empty")
		}
		if _, duplicate := accountingByID[accountID]; duplicate {
			return nil, fmt.Errorf("wallet accounting is duplicated for execution account %q", accountID)
		}
		accountingByID[accountID] = item
	}

	positionCount := make(map[string]int, len(accounts))
	unrealizedByID := make(map[string]*big.Rat, len(accounts))
	knownAccounts := make(map[string]struct{}, len(accounts))
	managedPositionKeys := make(map[string]struct{}, len(managedPositions))
	for _, account := range accounts {
		accountID := strings.TrimSpace(account.ExecutionAccountID)
		if accountID == "" {
			return nil, fmt.Errorf("live wallet execution account id is empty")
		}
		if _, duplicate := knownAccounts[accountID]; duplicate {
			return nil, fmt.Errorf("live wallet execution account %q is duplicated", accountID)
		}
		knownAccounts[accountID] = struct{}{}
		unrealizedByID[accountID] = new(big.Rat)
	}
	for _, item := range managedPositions {
		managedPositionKeys[livePositionKey(item.Position.ExecutionAccountID, item.Position.TokenID)] = struct{}{}
	}
	for _, position := range positions {
		accountID := strings.TrimSpace(position.ExecutionAccountID)
		unrealized, found := unrealizedByID[accountID]
		if !found {
			return nil, fmt.Errorf("live position references unknown execution account %q", accountID)
		}
		if _, managed := managedPositionKeys[livePositionKey(accountID, position.TokenID)]; !managed {
			continue
		}
		value, err := decimalRat(domain.Decimal(position.UnrealizedPnL))
		if err != nil {
			return nil, fmt.Errorf("parse wallet unrealized pnl for account %s: %w", accountID, err)
		}
		unrealized.Add(unrealized, value)
		positionCount[accountID]++
	}

	result := make([]domain.LiveWallet, 0, len(accounts))
	for accountID := range knownAccounts {
		ledger, found := accountingByID[accountID]
		if !found {
			return nil, fmt.Errorf("wallet accounting is missing for execution account %q", accountID)
		}
		peak, err := decimalRat(defaultDecimal(ledger.PeakCashUsed))
		if err != nil || peak.Sign() < 0 {
			return nil, fmt.Errorf("peak cash used is invalid for execution account %q", accountID)
		}
		invested, err := decimalRat(defaultDecimal(ledger.CumulativeInvestedCost))
		if err != nil || invested.Sign() < 0 {
			return nil, fmt.Errorf("cumulative invested cost is invalid for execution account %q", accountID)
		}
		realized, err := decimalRat(defaultDecimal(ledger.RealizedPnL))
		if err != nil {
			return nil, fmt.Errorf("realized pnl is invalid for execution account %q: %w", accountID, err)
		}
		unrealized := unrealizedByID[accountID]
		total := new(big.Rat).Add(new(big.Rat).Set(realized), unrealized)
		wallet, err := makeLiveWallet(accountID, positionCount[accountID], peak, invested, realized, unrealized, total)
		if err != nil {
			return nil, err
		}
		result = append(result, wallet)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].ExecutionAccountID < result[right].ExecutionAccountID
	})
	return result, nil
}

// makeLiveWallet 将精确有理数转换成 JSON 数字，并显式处理收益率零分母。
func makeLiveWallet(
	accountID string,
	count int,
	peak *big.Rat,
	invested *big.Rat,
	realized *big.Rat,
	unrealized *big.Rat,
	total *big.Rat,
) (domain.LiveWallet, error) {
	peakNumber, err := numberFromRat(peak)
	if err != nil {
		return domain.LiveWallet{}, err
	}
	investedNumber, err := numberFromRat(invested)
	if err != nil {
		return domain.LiveWallet{}, err
	}
	realizedNumber, err := numberFromRat(realized)
	if err != nil {
		return domain.LiveWallet{}, err
	}
	unrealizedNumber, err := numberFromRat(unrealized)
	if err != nil {
		return domain.LiveWallet{}, err
	}
	totalNumber, err := numberFromRat(total)
	if err != nil {
		return domain.LiveWallet{}, err
	}
	wallet := domain.LiveWallet{
		ExecutionAccountID: accountID, PositionCount: count,
		PeakCashUsed: peakNumber, CumulativeInvestedCost: investedNumber,
		RealizedPnL: realizedNumber, UnrealizedPnL: unrealizedNumber, TotalPnL: totalNumber,
	}
	if peak.Sign() > 0 {
		returnNumber, err := numberFromRat(new(big.Rat).Quo(new(big.Rat).Set(total), peak))
		if err != nil {
			return domain.LiveWallet{}, err
		}
		wallet.ReturnRate = &returnNumber
	}
	return wallet, nil
}

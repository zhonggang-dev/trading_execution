package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// ValidateAccountScope keeps the aggregate scope distinct from a concrete
// account. Existing callers without a scope retain their all-account behavior.
func ValidateAccountScope(accountID, scope string) error {
	if (scope != "" && scope != "active" && scope != "all") || (accountID != "" && scope != "") {
		return ErrInvalidPerformanceAccount
	}
	return ValidatePerformanceAccountID(accountID)
}

func (reader *Reader) selectedManagedAccountsInTx(ctx context.Context, tx *sql.Tx, accountID, scope string) ([]ManagedAccount, error) {
	if err := ValidateAccountScope(accountID, scope); err != nil {
		return nil, err
	}
	accounts, err := reader.managedAccountsInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	selected := make([]ManagedAccount, 0, len(accounts))
	for _, account := range accounts {
		if accountID != "" && account.ExecutionAccountID != accountID {
			continue
		}
		if scope == "active" && account.Mode != "ACTIVE" && account.Mode != "SELL_ONLY" {
			continue
		}
		selected = append(selected, account)
	}
	if accountID != "" && len(selected) != 1 {
		return nil, ErrInvalidPerformanceAccount
	}
	return selected, nil
}

// Modes are immutable process configuration. Scope is applied in SQL before
// count, summaries and pagination, never to the returned page in the browser.
func (reader *Reader) normalizeScopedFilter(filter ListFilter) (ListFilter, error) {
	filter, err := normalizeFilter(filter)
	if err != nil || filter.AccountScope != "active" {
		return filter, err
	}
	filter.scopedAccountIDs = make([]string, 0)
	seen := map[string]string{}
	for _, account := range reader.config.ManagedAccounts {
		if account.ExecutionAccountID == "" || (account.MarketSource != "POLYMARKET" && account.MarketSource != "KALSHI") {
			return ListFilter{}, ErrUnavailable
		}
		if previous, ok := seen[account.ExecutionAccountID]; ok {
			if previous != account.MarketSource {
				return ListFilter{}, ErrUnavailable
			}
			continue
		}
		seen[account.ExecutionAccountID] = account.MarketSource
		mode, found := account.Mode, false
		for _, binding := range reader.config.Bindings {
			if binding.ExecutionAccountID != account.ExecutionAccountID {
				continue
			}
			if found || (binding.MarketSource != "" && binding.MarketSource != account.MarketSource) {
				return ListFilter{}, ErrUnavailable
			}
			found, mode = true, binding.Mode
		}
		if mode != "" && mode != "ACTIVE" && mode != "SELL_ONLY" && mode != "PAUSED" {
			return ListFilter{}, ErrUnavailable
		}
		if mode == "ACTIVE" || mode == "SELL_ONLY" {
			filter.scopedAccountIDs = append(filter.scopedAccountIDs, account.ExecutionAccountID)
		}
	}
	return filter, nil
}

func appendAccountScope(conditions []string, args *[]any, filter ListFilter, column string) []string {
	if filter.AccountScope != "active" {
		return conditions
	}
	encoded, _ := json.Marshal(filter.scopedAccountIDs)
	*args = append(*args, string(encoded))
	return append(conditions, fmt.Sprintf("%s IN (SELECT jsonb_array_elements_text($%d::jsonb))", column, len(*args)))
}

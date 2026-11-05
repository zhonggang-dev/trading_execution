package consolev2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type ManagedAccount struct {
	ExecutionAccountID string `json:"execution_account_id"`
	MarketSource       string `json:"market_source"`
	StrategyID         string `json:"strategy_id,omitempty"`
	Mode               string `json:"mode,omitempty"`
	PredictionModelID  string `json:"prediction_model_id,omitempty"`
	SourceMode         string `json:"source_mode,omitempty"`
}

type AccountDirectory struct {
	Accounts   []ManagedAccount `json:"accounts"`
	ObservedAt time.Time        `json:"observed_at"`
}

// Accounts intersects process-validated venue membership with the registered
// accounts. No credential-bearing execution_accounts column is selected.
func (reader *Reader) Accounts(ctx context.Context) (AccountDirectory, error) {
	if err := reader.requireDB(ctx); err != nil {
		return AccountDirectory{}, err
	}
	tx, err := reader.beginReadTx(ctx, "account directory")
	if err != nil {
		return AccountDirectory{}, err
	}
	defer tx.Rollback()
	accounts, err := reader.managedAccountsInTx(ctx, tx)
	if err != nil {
		return AccountDirectory{}, err
	}

	if err = tx.Commit(); err != nil {
		return AccountDirectory{}, dbUnavailable("commit account directory", err)
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].MarketSource != accounts[j].MarketSource {
			return accounts[i].MarketSource > accounts[j].MarketSource
		}
		return accounts[i].ExecutionAccountID < accounts[j].ExecutionAccountID
	})
	return AccountDirectory{Accounts: accounts, ObservedAt: reader.now().UTC()}, nil
}

// managedAccountsInTx is shared with account performance so registration,
// membership, and all financial facts are read in the same database snapshot.
func (reader *Reader) managedAccountsInTx(ctx context.Context, tx *sql.Tx) ([]ManagedAccount, error) {
	accounts := make([]ManagedAccount, 0, len(reader.config.ManagedAccounts))
	seen := map[string]string{}
	for _, account := range reader.config.ManagedAccounts {
		if account.ExecutionAccountID == "" || (account.MarketSource != "POLYMARKET" && account.MarketSource != "KALSHI") {
			return nil, ErrUnavailable
		}
		if previous, ok := seen[account.ExecutionAccountID]; ok {
			if previous != account.MarketSource {
				return nil, ErrUnavailable
			}
			continue
		}
		seen[account.ExecutionAccountID] = account.MarketSource
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM execution_accounts WHERE execution_account_id=$1)`, account.ExecutionAccountID).Scan(&exists); err != nil {
			return nil, dbUnavailable("registered account", err)
		}
		if exists {
			bindingFound := false
			for _, binding := range reader.config.Bindings {
				if binding.ExecutionAccountID != account.ExecutionAccountID {
					continue
				}
				if bindingFound || (binding.MarketSource != "" && binding.MarketSource != account.MarketSource) {
					return nil, ErrUnavailable
				}
				if binding.Mode != "" && binding.Mode != "ACTIVE" && binding.Mode != "SELL_ONLY" && binding.Mode != "PAUSED" {
					return nil, ErrUnavailable
				}
				if binding.SourceMode != "" && binding.SourceMode != "SANDBOX" && binding.SourceMode != "DIRECT" {
					return nil, ErrUnavailable
				}
				bindingFound = true
				account.StrategyID, account.Mode, account.PredictionModelID, account.SourceMode = binding.StrategyID, binding.Mode, binding.PredictionModelID, binding.SourceMode
			}
			accounts = append(accounts, account)
		}
	}
	return accounts, nil
}

type Settlement struct {
	SettlementID       string     `json:"settlement_id"`
	MarketSource       string     `json:"market_source"`
	ExecutionAccountID string     `json:"execution_account_id"`
	MarketID           string     `json:"market_id"`
	ConditionID        string     `json:"condition_id"`
	Status             string     `json:"status"`
	TransactionHash    string     `json:"transaction_hash,omitempty"`
	Payout             *string    `json:"payout"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	SettledAt          *time.Time `json:"settled_at"`
	ConfirmedAt        *time.Time `json:"confirmed_at"`
	AppliedAt          *time.Time `json:"applied_at"`
}

const settlementProjection = `
 SELECT 'polymarket-redemption:'||execution_account_id||':'||condition_id AS settlement_id,
 'POLYMARKET'::text AS market_source, execution_account_id,''::text AS market_id,condition_id,status,
 NULLIF(transaction_hash,'') AS transaction_hash,(payout_base_units/1000000::numeric)::text AS payout,
 created_at,updated_at,NULL::timestamptz AS settled_at,confirmed_at,applied_at
 FROM polymarket_redemptions
 UNION ALL
 SELECT 'kalshi-settlement:'||execution_account_id||':'||ticker,'KALSHI',execution_account_id,ticker,'',status,
 NULL::text,exchange_revenue::text,recorded_at,recorded_at,settled_at,NULL::timestamptz,applied_at
 FROM kalshi_settlements
`

func (reader *Reader) Settlements(ctx context.Context, filter ListFilter) (Page[Settlement], error) {
	filter, err := reader.normalizeScopedFilter(filter)
	if err != nil {
		return Page[Settlement]{}, err
	}
	if err = reader.requireDB(ctx); err != nil {
		return Page[Settlement]{}, err
	}
	now := reader.now().UTC()
	args := []any{}
	clauses := []string{"TRUE"}
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.ExecutionAccountID != "" {
		add("execution_account_id=$%d", filter.ExecutionAccountID)
	}
	clauses = appendAccountScope(clauses, &args, filter, "execution_account_id")
	if filter.MarketSource != "" {
		add("market_source=$%d", filter.MarketSource)
	}
	if filter.Status != "" {
		add("status=$%d", filter.Status)
	}
	if filter.Query != "" {
		add("concat_ws(' ',execution_account_id,market_id,condition_id,transaction_hash) ILIKE $%d", "%"+filter.Query+"%")
	}
	if filter.From != nil {
		add("created_at >= $%d", filter.From.UTC())
	} else {
		add("(status <> 'APPLIED' OR created_at >= $%d)", boundedSince(filter, now))
	}
	if filter.To != nil {
		add("created_at <= $%d", filter.To.UTC())
	}
	where := strings.Join(clauses, " AND ")
	projection := `SELECT * FROM (` + settlementProjection + `) settlements`
	tx, err := reader.beginReadTx(ctx, "settlements")
	if err != nil {
		return Page[Settlement]{}, err
	}
	defer tx.Rollback()
	total, err := countProjection(ctx, tx, projection, where, args, "settlements")
	if err != nil {
		return Page[Settlement]{}, err
	}
	listArgs := append([]any(nil), args...)
	limit, offset := pageLimitOffset(filter, &listArgs)
	rows, err := tx.QueryContext(ctx, `SELECT to_jsonb(settlements) FROM (`+settlementProjection+`) settlements WHERE `+where+` ORDER BY CASE WHEN status<>'APPLIED' THEN 0 ELSE 1 END,updated_at DESC,settlement_id DESC LIMIT `+limit+` OFFSET `+offset, listArgs...)
	if err != nil {
		return Page[Settlement]{}, dbUnavailable("list settlements", err)
	}
	defer rows.Close()
	items := make([]Settlement, 0, filter.Limit)
	for rows.Next() {
		var raw []byte
		var item Settlement
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &item)
		}
		if err != nil {
			return Page[Settlement]{}, dbUnavailable("scan settlement", err)
		}
		item.CreatedAt = item.CreatedAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
		if item.SettledAt != nil {
			v := item.SettledAt.UTC()
			item.SettledAt = &v
		}
		if item.ConfirmedAt != nil {
			v := item.ConfirmedAt.UTC()
			item.ConfirmedAt = &v
		}
		if item.AppliedAt != nil {
			v := item.AppliedAt.UTC()
			item.AppliedAt = &v
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return Page[Settlement]{}, dbUnavailable("iterate settlements", err)
	}
	if err = tx.Commit(); err != nil {
		return Page[Settlement]{}, dbUnavailable("commit settlements", err)
	}
	return finishPage(items, filter, total, now, map[string]any{"basis": "bounded_recent_or_pending_settlements"}), nil
}

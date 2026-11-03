package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

// The existing adoption integration fixture seals a real baseline, disposition,
// adoption, position event and lot using the production schema's deferred
// checks. It deliberately has no execution order/fill and no current route.
func assertRetiredAdoptionPortfolio(t *testing.T, db *sql.DB, account, lotID, token, condition string) {
	t.Helper()
	ctx := context.Background()
	reader := consolev2.New(db, consolev2.RuntimeConfig{})
	page, err := reader.PortfolioHoldings(ctx, consolev2.ListFilter{Limit: 1})
	if err != nil || page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("retired adopted account disappeared: %#v %v", page, err)
	}
	asset := page.Items[0]
	if asset.ExecutionAccountID != account || asset.MarketSource != "POLYMARKET" || asset.RemainingShares != "1.5" || asset.RemainingCost != "0.615" {
		t.Fatalf("adopted asset source/economics: %#v", asset)
	}
	directory, err := reader.Accounts(ctx)
	if err != nil || len(directory.Accounts) != 0 {
		t.Fatalf("historical asset fabricated current account configuration: %#v %v", directory, err)
	}
	identity := consolev2.PortfolioHoldingIdentity{ExecutionAccountID: account, MarketSource: "POLYMARKET", MarketID: asset.MarketID, ConditionID: condition, TokenID: token, OutcomeIndex: asset.OutcomeIndex, OutcomeName: asset.OutcomeName}
	members, err := reader.PortfolioHoldingLots(ctx, identity, consolev2.ListFilter{Limit: 1})
	if err != nil || members.Total != 1 || len(members.Items) != 1 || members.Items[0].LotID != lotID || members.Items[0].OpeningOrderID != "" || members.Items[0].OpeningFillKey != "" || members.Items[0].ModelID != "echo" {
		t.Fatalf("adopted lot null execution identity: %#v %v", members, err)
	}
	for _, wrong := range []consolev2.PortfolioHoldingIdentity{
		{ExecutionAccountID: account + "-other", MarketSource: "POLYMARKET", MarketID: asset.MarketID, ConditionID: condition, TokenID: token, OutcomeIndex: asset.OutcomeIndex, OutcomeName: asset.OutcomeName},
		{ExecutionAccountID: account, MarketSource: "POLYMARKET", MarketID: "other-market", ConditionID: condition, TokenID: token, OutcomeIndex: asset.OutcomeIndex, OutcomeName: asset.OutcomeName},
		{ExecutionAccountID: account, MarketSource: "POLYMARKET", MarketID: asset.MarketID, ConditionID: condition, TokenID: "other-token", OutcomeIndex: asset.OutcomeIndex, OutcomeName: asset.OutcomeName},
	} {
		if page, err := reader.PortfolioHoldingLots(ctx, wrong, consolev2.ListFilter{Limit: 1}); err != nil || page.Total != 0 {
			t.Fatalf("adoption borrowed by another identity: %#v %v", page, err)
		}
	}
	conflicting := consolev2.New(db, consolev2.RuntimeConfig{ManagedAccounts: []consolev2.ManagedAccount{{ExecutionAccountID: account, MarketSource: "KALSHI"}}})
	if _, err := conflicting.PortfolioHoldings(ctx, consolev2.ListFilter{Limit: 1}); !errors.Is(err, consolev2.ErrUnavailable) {
		t.Fatalf("adoption/config platform conflict accepted: %v", err)
	}

	// Corrupt only connection-local shadow copies, never the sealed fixture.
	// Each broken relation must fail closed instead of borrowing another lot's
	// valid baseline merely because the account, token or adoption ID exists.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	lotTable := pgx.Identifier{schema, "position_lots"}.Sanitize()
	baselineTable := pgx.Identifier{schema, "execution_external_position_baselines"}.Sanitize()
	for _, column := range []string{"lot_id", "external_adoption_id", "execution_account_id", "market_id", "condition_id", "token_id", "opening_order_id", "opening_fill_key"} {
		if _, err := db.Exec(`CREATE TEMP TABLE position_lots AS SELECT * FROM ` + lotTable); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE pg_temp.position_lots SET ` + pgx.Identifier{column}.Sanitize() + `='unrelated'`); err != nil {
			t.Fatal(err)
		}
		_, readErr := reader.PortfolioHoldings(ctx, consolev2.ListFilter{Limit: 1})
		if _, err := db.Exec(`DROP TABLE pg_temp.position_lots`); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(readErr, consolev2.ErrUnavailable) {
			t.Fatalf("mismatched adoption %s accepted: %v", column, readErr)
		}
	}
	if _, err := db.Exec(`CREATE TEMP TABLE execution_external_position_baselines AS SELECT * FROM ` + baselineTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE pg_temp.execution_external_position_baselines SET source='UNKNOWN'`); err != nil {
		t.Fatal(err)
	}
	_, readErr := reader.PortfolioHoldings(ctx, consolev2.ListFilter{Limit: 1})
	if _, err := db.Exec(`DROP TABLE pg_temp.execution_external_position_baselines`); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(readErr, consolev2.ErrUnavailable) {
		t.Fatalf("unknown baseline source guessed: %v", readErr)
	}
}

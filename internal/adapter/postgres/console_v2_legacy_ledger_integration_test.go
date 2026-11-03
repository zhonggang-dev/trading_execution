package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

// Run against facts produced by the existing fill, redemption, and external
// sell writers, rather than a second SQL fixture matching the new projection.
func assertConsoleLegacyLedgerParity(t *testing.T, db *sql.DB, account string, expectedTypes ...domain.LedgerActivityType) {
	t.Helper()
	repository, err := NewTradeHistoryRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := repository.ListLedgerActivities(context.Background(), domain.LedgerActivityFilter{ExecutionAccountID: account, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	page, err := consolev2.New(db, consolev2.RuntimeConfig{}).LedgerActivities(context.Background(), consolev2.ListFilter{ExecutionAccountID: account, Limit: 100})
	if err != nil || page.Total != int(legacy.Total) || len(page.Items) != len(legacy.Items) {
		t.Fatalf("legacy/V2 counts: legacy=%#v v2=%#v err=%v", legacy, page, err)
	}
	types := map[domain.LedgerActivityType]bool{}
	for index, item := range page.Items {
		if item.MarketSource != strings.ToUpper(item.Venue) || item.QuantityUnit != "shares" {
			t.Fatalf("incorrect legacy source or quantity: %#v", item)
		}
		var compatible domain.LedgerActivity
		raw, err := json.Marshal(item)
		if err == nil {
			err = json.Unmarshal(raw, &compatible)
		}
		if err != nil || !reflect.DeepEqual(compatible, legacy.Items[index]) {
			t.Fatalf("legacy row changed: old=%#v new=%#v err=%v", legacy.Items[index], compatible, err)
		}
		types[compatible.ActivityType] = true
	}
	for _, kind := range expectedTypes {
		if !types[kind] {
			t.Fatalf("missing nonempty %s legacy fixture", kind)
		}
	}
	var summary map[string]any
	raw, err := json.Marshal(legacy.Summary)
	if err == nil {
		err = json.Unmarshal(raw, &summary)
	}
	if err != nil {
		t.Fatal(err)
	}
	for key, old := range summary {
		if count, ok := old.(float64); ok {
			if page.Summary[key] != int(count) {
				t.Fatalf("legacy summary %s: old=%v new=%v", key, old, page.Summary[key])
			}
		} else if page.Summary[key] != old {
			t.Fatalf("legacy summary %s: old=%v new=%v", key, old, page.Summary[key])
		}
	}
}

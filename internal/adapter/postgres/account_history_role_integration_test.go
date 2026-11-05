package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresIntegrationHistoryNonSuperOwnerFKPermission(t *testing.T) {
	dsn := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL required")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse test database config")
	}
	admin := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = admin.Close() })
	random := make([]byte, 24)
	if _, err = rand.Read(random); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(random[:8])
	role := "history_owner_" + suffix
	schema := "history_acl_" + suffix
	password := hex.EncodeToString(random)
	if _, err = admin.Exec(`CREATE ROLE ` + role + ` LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD '` + password + `'`); err != nil {
		t.Fatal("create isolated non-super test owner", err)
	}
	var runtimeDB *sql.DB
	t.Cleanup(func() {
		if runtimeDB != nil {
			_ = runtimeDB.Close()
		}
		if _, e := admin.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`); e != nil {
			t.Errorf("clean role test schema: %v", e)
		}
		if _, e := admin.Exec(`DROP ROLE ` + role); e != nil {
			t.Errorf("clean isolated role: %v", e)
		}
	})
	if _, err = admin.Exec(`CREATE SCHEMA ` + schema + ` AUTHORIZATION ` + role); err != nil {
		t.Fatal(err)
	}
	runtimeCfg := cfg.Copy()
	runtimeCfg.User = role
	runtimeCfg.Password = password
	runtimeCfg.RuntimeParams["search_path"] = schema
	delete(runtimeCfg.RuntimeParams, "role")
	runtimeDB = stdlib.OpenDB(*runtimeCfg)
	runtimeDB.SetMaxOpenConns(4)
	var current, session string
	var super bool
	if err = runtimeDB.QueryRow(`SELECT current_user,session_user,rolsuper FROM pg_roles WHERE rolname=current_user`).Scan(&current, &session, &super); err != nil {
		t.Fatal("connect as isolated owner", err)
	}
	if current != role || session != role || super {
		t.Fatal("test must authenticate as a real non-super owner", current, session, super)
	}
	apply := func(name string) {
		t.Helper()
		raw, e := os.ReadFile(filepath.Join("..", "..", "..", "migrations", name))
		if e != nil {
			t.Fatal(e)
		}
		sqlText := strings.ReplaceAll(string(raw), "search_path=public,pg_temp", "search_path="+schema+",pg_temp")
		sqlText = strings.ReplaceAll(sqlText, "trading_execution_app", role)
		if _, e = runtimeDB.Exec(sqlText); e != nil {
			t.Fatal("apply isolated role migration", name, e)
		}
	}
	apply("0044_account_price_history.sql")
	var owns bool
	if err = runtimeDB.QueryRow(`SELECT tableowner=current_user FROM pg_tables WHERE schemaname=$1 AND tablename='account_price_history_windows'`, schema).Scan(&owns); err != nil || !owns {
		t.Fatal("non-super role must own raw relation", owns, err)
	}
	store := NewAccountPriceHistoryStore(runtimeDB)
	at := time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)
	a := historyTestAsset()
	window := h.Window{Asset: a, From: at, To: at.Add(time.Hour)}
	result := h.Result{Window: window, Series: h.Series{Points: []h.Point{{At: at, Price: "0.2", Source: "PM_HISTORY", FetchedAt: at, ResolutionSeconds: 3600}}, Coverage: []h.Coverage{{From: window.From, To: window.To, State: "complete", FetchedAt: at}}}}
	expectState := func(err error, want string) {
		t.Helper()
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != want {
			t.Fatalf("expected SQLSTATE %s, got %T %v", want, err, err)
		}
	}
	expectState(store.Save(context.Background(), result), "42501")
	if n := scalar(t, runtimeDB, `SELECT COUNT(*)::text FROM account_price_history_windows`); n != "0" {
		t.Fatal("failed FK check did not roll back raw row", n)
	}
	// Installations without the conventional application role must skip the
	// optional grant safely, without accidentally widening this owner's ACL.
	missingRoleMigration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0047_account_history_fk_lock_permission.sql"))
	if err != nil {
		t.Fatal(err)
	}
	missingRoleSQL := strings.ReplaceAll(string(missingRoleMigration), "search_path=public,pg_temp", "search_path="+schema+",pg_temp")
	missingRoleSQL = strings.ReplaceAll(missingRoleSQL, "trading_execution_app", role+"_absent")
	if _, err = runtimeDB.Exec(missingRoleSQL); err != nil {
		t.Fatal("missing application role must be a safe no-op", err)
	}
	expectState(store.Save(context.Background(), result), "42501")
	apply("0047_account_history_fk_lock_permission.sql")
	var tableUpdate, idUpdate, otherUpdate bool
	if err = runtimeDB.QueryRow(`SELECT has_table_privilege(current_user,'account_price_history_windows','UPDATE'),has_column_privilege(current_user,'account_price_history_windows','history_id','UPDATE'),has_column_privilege(current_user,'account_price_history_windows','recorded_at','UPDATE')`).Scan(&tableUpdate, &idUpdate, &otherUpdate); err != nil {
		t.Fatal(err)
	}
	if tableUpdate || !idUpdate || otherUpdate {
		t.Fatal("grant must remain limited to history_id", tableUpdate, idUpdate, otherUpdate)
	}
	for range 2 {
		if err = store.Save(context.Background(), result); err != nil {
			t.Fatal("minimum FK-lock grant must allow Save", err)
		}
	}
	apply("0047_account_history_fk_lock_permission.sql")
	if err = store.Save(context.Background(), result); err != nil {
		t.Fatal("rerun migration/Save must stay idempotent", err)
	}
	if n := scalar(t, runtimeDB, `SELECT COUNT(*)::text FROM account_price_history_windows`); n != "1" {
		t.Fatal("idempotent Save duplicated raw evidence", n)
	}
	data, err := store.Load(context.Background(), []h.Window{window})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := h.PriceAt(data.Series[a.Key()], at)
	if !ok || p.Price != "0.2" {
		t.Fatal("Load failed after grant", p, ok)
	}
	_, err = runtimeDB.Exec(`INSERT INTO account_price_history_points(asset_key,point_at,fetched_at,history_id,point) VALUES($1,$2,$2,9223372036854775806,'{}')`, a.Key(), at.Add(2*time.Hour))
	expectState(err, "23503")
	_, err = runtimeDB.Exec(`UPDATE account_price_history_windows SET history_id=history_id`)
	expectState(err, "P0001")
	_, err = runtimeDB.Exec(`UPDATE account_price_history_windows SET recorded_at=recorded_at`)
	expectState(err, "42501")
	_, err = runtimeDB.Exec(`DELETE FROM account_price_history_windows`)
	expectState(err, "42501")
	_, err = runtimeDB.Exec(`TRUNCATE account_price_history_windows`)
	expectState(err, "42501")
	if n := scalar(t, runtimeDB, `SELECT COUNT(*)::text FROM account_price_history_windows`); n != "1" {
		t.Fatal("raw evidence changed", n)
	}
	t.Log(fmt.Sprintf("non-super owner: 0044 rejected 42501; column grant enables FK Save/Load; orphan FK=23503; mutation=P0001/42501; idempotent"))
}

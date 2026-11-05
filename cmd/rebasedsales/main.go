// rebasedsales adds missing historical sale evidence without moving cash.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "rebasedsales: verification or audit transaction rejected")
		os.Exit(1)
	}
}
func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("rebasedsales", flag.ContinueOnError)
	manifest := flags.String("manifest", "", "reviewed two-sale manifest")
	apply := flags.Bool("apply", false, "commit append-only historical facts; default is full database dry-run then rollback")
	confirm := flags.String("confirm-sha256", "", "exact reviewed manifest SHA256, required for apply")
	dbEnv := flags.String("database-env", "REBASED_SALES_MAINTENANCE_DATABASE_URL", "environment variable with maintenance-owner connection; never pass a URL on the command line")
	actor := flags.String("actor", "", "accountable operator")
	reason := flags.String("reason", "", "reviewed repair reason")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *manifest == "" || *actor == "" || *reason == "" {
		return fmt.Errorf("manifest, actor, reason are required")
	}
	raw, err := os.ReadFile(*manifest)
	if err != nil {
		return fmt.Errorf("read manifest")
	}
	if _, err = postgres.DecodeRebasedSaleManifest(raw); err != nil {
		return err
	}
	if *apply && *confirm != fmt.Sprintf("%x", sha256.Sum256(raw)) {
		return fmt.Errorf("reviewed manifest confirmation required")
	}
	dsn := os.Getenv(*dbEnv)
	if dsn == "" {
		return fmt.Errorf("maintenance database environment required")
	}
	pgconfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("open maintenance connection")
	}
	pgconfig.RuntimeParams["search_path"] = "public,pg_temp"
	pgconfig.RuntimeParams["application_name"] = "rebased-sales-maintenance"
	db := stdlib.OpenDB(*pgconfig)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	rpc, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{RPCURL: os.Getenv("POLYGON_RPC_URL"), RequiredConfirmations: 128, HTTPClient: &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, RequestTimeout: 8 * time.Second, ReadAttempts: 2})
	if err != nil {
		return fmt.Errorf("read-only RPC configuration required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	report, err := postgres.RunRebasedHistoricalSales(ctx, db, rpc, raw, *apply, *confirm, *actor, *reason)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

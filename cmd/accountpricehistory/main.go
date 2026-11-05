// accountpricehistory maintains only durable market observations and coverage.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/accountprices"
	"github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	h "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "accountpricehistory: operation incomplete; rerun to resume")
		os.Exit(1)
	}
}
func run(parent context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("command required: plan, backfill, incremental, coverage")
	}
	command := args[0]
	if command != "plan" && command != "backfill" && command != "incremental" && command != "coverage" {
		return fmt.Errorf("unknown history command")
	}
	flags := flag.NewFlagSet("accountpricehistory "+command, flag.ContinueOnError)
	dbEnv := flags.String("database-env", "TRADING_EXECUTION_DATABASE_URL", "environment variable containing connection; never supply a URL on the command line")
	maxWindows := flags.Int("max-windows", 100, "maximum windows per run; rerun resumes remaining windows")
	timeout := flags.Duration("timeout", 45*time.Minute, "total bounded run duration")
	dryRun := flags.Bool("dry-run", false, "only report planned reads; do not collect or save market data")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 || *timeout > 6*time.Hour {
		return fmt.Errorf("invalid command budget")
	}
	dsn := os.Getenv(*dbEnv)
	if dsn == "" {
		return fmt.Errorf("history database environment required")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("invalid history database configuration")
	}
	config.RuntimeParams["application_name"] = "account-price-history"
	config.RuntimeParams["search_path"] = "public,pg_temp"
	db := stdlib.OpenDB(*config)
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	store := postgres.NewAccountPriceHistoryStore(db)
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	source, err := accountprices.New(accountprices.Params{PMBaseURL: os.Getenv("ACCOUNT_HISTORY_PM_BASE_URL"), GammaBaseURL: os.Getenv("ACCOUNT_HISTORY_GAMMA_BASE_URL"), KalshiBaseURL: os.Getenv("ACCOUNT_HISTORY_KALSHI_BASE_URL")})
	if err != nil {
		return err
	}
	if command == "coverage" {
		observedAt := time.Now().UTC()
		targets, e := store.Targets(ctx, observedAt)
		if e != nil {
			return e
		}
		data, e := store.Load(ctx, targets)
		if e != nil {
			return e
		}
		result := []assetCoverage{}
		for _, target := range targets {
			item, err := summarizeCoverage(ctx, target, data.Series[target.Asset.Key()], observedAt)
			if err != nil {
				return err
			}
			result = append(result, item)
		}
		return json.NewEncoder(out).Encode(struct {
			Version string          `json:"version"`
			Items   []assetCoverage `json:"items"`
		}{data.Version, result})
	}
	report, e := h.Run(ctx, store, source, h.SyncOptions{Now: time.Now().UTC(), MaxWindows: *maxWindows, Workers: 2, DryRun: *dryRun || command == "plan"})
	if err = json.NewEncoder(out).Encode(report); err != nil {
		return err
	}
	return e
}

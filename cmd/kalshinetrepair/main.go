package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type binding struct {
	ExecutionAccountID string `json:"execution_account_id"`
	APIKeyID           string `json:"api_key_id"`
	PrivateKeyPath     string `json:"private_key_path"`
}

type options struct {
	account          string
	ticker           string
	expectedCashDiff domain.Decimal
	cashTolerance    domain.Decimal
	apply            bool
	confirmation     string
	timeout          time.Duration
}

// maxCashTolerance is one cent, the rounding of a whole-cents exchange balance.
// Anything wider could hide a real cash difference behind the repair.
const maxCashTolerance = domain.Decimal("0.01")

// evidenceSource is the read-only part of the Kalshi client this tool uses.
type evidenceSource interface {
	GetMarketPosition(context.Context, string) (domain.Decimal, error)
	AvailableBalance(context.Context) (domain.Decimal, error)
}

// repairStore is the ledger side of the repair.
type repairStore interface {
	Run(context.Context, postgresadapter.KalshiNettingRepairRequest) (postgresadapter.KalshiNettingRepairReport, error)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "kalshinetrepair:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	opts, err := parseOptions(arguments)
	if err != nil {
		return err
	}
	if _, err := kalshiLogicalAccountID(opts.account); err != nil {
		return err
	}
	databaseURL := strings.TrimSpace(os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
	apiURL := strings.TrimSpace(os.Getenv("KALSHI_API_URL"))
	if databaseURL == "" || apiURL == "" {
		return fmt.Errorf("TRADING_EXECUTION_DATABASE_URL and KALSHI_API_URL are required")
	}
	if err := validateKalshiAPIURL(apiURL, opts.apply); err != nil {
		return err
	}
	selected, err := selectBinding(os.Getenv("KALSHI_LIVE_ACCOUNTS_JSON"), strings.TrimSpace(opts.account))
	if err != nil {
		return err
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	client, err := kalshi.NewClient(kalshi.ClientParams{
		BaseURL: apiURL, APIKeyID: selected.APIKeyID, PrivateKeyPath: selected.PrivateKeyPath,
		HTTPClient: repairHTTPClient(opts.timeout), LiveTradingEnabled: false,
	})
	if err != nil {
		return err
	}
	store, err := postgresadapter.NewKalshiNettingRepairStore(database, nil)
	if err != nil {
		return err
	}
	return repair(ctx, opts, client, store, os.Stdout)
}

// repair reads the exchange before the transaction starts, so the account row is
// never held locked across a network call. The ledger moving in between makes the
// comparison fail rather than pass.
func repair(ctx context.Context, opts options, source evidenceSource, store repairStore, output io.Writer) error {
	position, err := source.GetMarketPosition(ctx, opts.ticker)
	if err != nil {
		return fmt.Errorf("read Kalshi market position: %w", err)
	}
	cash, err := source.AvailableBalance(ctx)
	if err != nil {
		return fmt.Errorf("read Kalshi balance: %w", err)
	}
	report, runErr := store.Run(ctx, postgresadapter.KalshiNettingRepairRequest{
		ExecutionAccountID: opts.account, Ticker: opts.ticker,
		Evidence:         postgresadapter.KalshiNettingEvidence{PositionFP: position, Cash: cash},
		ExpectedCashDiff: opts.expectedCashDiff, CashTolerance: opts.cashTolerance,
		Apply: opts.apply, Confirmation: opts.confirmation,
	})
	if report.ExecutionAccountID != "" {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil && runErr == nil {
			return err
		}
	}
	return runErr
}

func repairHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			// Signed Kalshi headers are valid only for the configured origin/path.
			// Never forward them to a redirect target.
			return http.ErrUseLastResponse
		},
	}
}

func validateKalshiAPIURL(raw string, apply bool) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.User != nil || parsed.Host == "" ||
		parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("KALSHI_API_URL must be an authenticated HTTPS Kalshi endpoint")
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return fmt.Errorf("KALSHI_API_URL must use the standard HTTPS port")
	}
	host := strings.ToLower(parsed.Hostname())
	switch host {
	case "api.elections.kalshi.com", "external-api.kalshi.com", "demo-api.kalshi.co":
	default:
		return fmt.Errorf("KALSHI_API_URL host is not an approved official Kalshi host")
	}
	if apply && host != "external-api.kalshi.com" {
		return fmt.Errorf("--apply is restricted to the official Kalshi production endpoint")
	}
	return nil
}

func parseOptions(arguments []string) (options, error) {
	set := flag.NewFlagSet("kalshinetrepair", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var result options
	var expectedCashDiff, cashTolerance string
	set.StringVar(&result.account, "account", "", "exact internal execution account id (kalshi:<logical-id>)")
	set.StringVar(&result.ticker, "ticker", "", "exact Kalshi market ticker")
	set.StringVar(&expectedCashDiff, "expected-cash-diff", "0", "exchange cash minus local cash that other known causes explain")
	set.StringVar(&cashTolerance, "cash-tolerance", "0", "allowed cash difference around --expected-cash-diff, at most 0.01 (use 0.01 if the exchange reports whole cents)")
	set.BoolVar(&result.apply, "apply", false, "apply the repair; omitted means dry-run and rollback")
	set.StringVar(&result.confirmation, "confirmation", "", "required with --apply; the fingerprint printed by the dry run")
	set.DurationVar(&result.timeout, "timeout", 20*time.Second, "total database/API timeout")
	if err := set.Parse(arguments); err != nil {
		return options{}, err
	}
	result.account, result.ticker = strings.TrimSpace(result.account), strings.TrimSpace(result.ticker)
	result.confirmation = strings.TrimSpace(result.confirmation)
	if result.account == "" || result.ticker == "" {
		return options{}, fmt.Errorf("--account and --ticker are required; broad scans are forbidden")
	}
	if strings.Contains(result.ticker, ":") {
		return options{}, fmt.Errorf("--ticker must be a bare market ticker")
	}
	if _, err := kalshiLogicalAccountID(result.account); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	if result.timeout < time.Second {
		return options{}, fmt.Errorf("--timeout must be at least one second")
	}
	if result.apply && result.confirmation == "" {
		return options{}, fmt.Errorf("--apply requires --confirmation with the dry run's fingerprint")
	}
	var err error
	if result.expectedCashDiff, err = domain.ParseDecimal(strings.TrimSpace(expectedCashDiff)); err != nil {
		return options{}, fmt.Errorf("--expected-cash-diff: %w", err)
	}
	if result.cashTolerance, err = domain.ParseDecimal(strings.TrimSpace(cashTolerance)); err != nil {
		return options{}, fmt.Errorf("--cash-tolerance: %w", err)
	}
	if sign, err := result.cashTolerance.Sign(); err != nil || sign < 0 {
		return options{}, fmt.Errorf("--cash-tolerance must not be negative")
	}
	if comparison, err := result.cashTolerance.Compare(maxCashTolerance); err != nil || comparison > 0 {
		return options{}, fmt.Errorf("--cash-tolerance must not exceed %s", maxCashTolerance)
	}
	return result, nil
}

func kalshiLogicalAccountID(internalAccountID string) (string, error) {
	internalAccountID = strings.TrimSpace(internalAccountID)
	const prefix = "kalshi:"
	if !strings.HasPrefix(internalAccountID, prefix) {
		return "", fmt.Errorf("--account must be the internal Kalshi account id with kalshi: prefix")
	}
	logical := strings.TrimSpace(strings.TrimPrefix(internalAccountID, prefix))
	if logical == "" || strings.Contains(logical, ":") {
		return "", fmt.Errorf("--account must contain exactly one kalshi: prefix and a non-empty logical id")
	}
	return logical, nil
}

// selectBinding picks the credential of one Kalshi execution account from
// KALSHI_LIVE_ACCOUNTS_JSON. accountID is the full kalshi:<name> account.
func selectBinding(encoded, accountID string) (binding, error) {
	var bindings []binding
	if err := json.Unmarshal([]byte(strings.TrimSpace(encoded)), &bindings); err != nil {
		return binding{}, fmt.Errorf("decode KALSHI_LIVE_ACCOUNTS_JSON: %w", err)
	}
	var selected binding
	for _, candidate := range bindings {
		if strings.TrimSpace(candidate.ExecutionAccountID) != strings.TrimSpace(accountID) {
			continue
		}
		if selected.ExecutionAccountID != "" {
			return binding{}, fmt.Errorf("multiple Kalshi credentials are configured for execution account %q", accountID)
		}
		selected = candidate
	}
	selected.ExecutionAccountID = strings.TrimSpace(selected.ExecutionAccountID)
	selected.APIKeyID = strings.TrimSpace(selected.APIKeyID)
	selected.PrivateKeyPath = strings.TrimSpace(selected.PrivateKeyPath)
	if selected.ExecutionAccountID == "" || selected.APIKeyID == "" || selected.PrivateKeyPath == "" {
		return binding{}, fmt.Errorf("no complete Kalshi live credential exists for execution account %q", accountID)
	}
	return selected, nil
}

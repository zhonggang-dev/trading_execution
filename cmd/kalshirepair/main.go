package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshirepair"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type binding struct {
	ExecutionAccountID string `json:"execution_account_id"`
	APIKeyID           string `json:"api_key_id"`
	PrivateKeyPath     string `json:"private_key_path"`
}

type options struct {
	account       string
	order         string
	apply         bool
	confirmation  string
	finalityGrace time.Duration
	timeout       time.Duration
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "kalshirepair:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	opts, err := parseOptions(arguments)
	if err != nil {
		return err
	}
	logicalAccountID, err := kalshiLogicalAccountID(opts.account)
	if err != nil {
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
	selected, err := selectBinding(os.Getenv("KALSHI_LIVE_BINDINGS_JSON"), logicalAccountID)
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
	evidence, err := kalshi.NewRepairEvidenceSource(client)
	if err != nil {
		return err
	}
	store, err := postgresadapter.NewKalshiManualReviewRepairStore(database, nil)
	if err != nil {
		return err
	}
	service, err := kalshirepair.New(kalshirepair.Params{
		Store: store, Evidence: evidence, FinalityGrace: opts.finalityGrace,
	})
	if err != nil {
		return err
	}
	result, err := service.Run(ctx, kalshirepair.Request{
		ExecutionAccountID: opts.account, OrderID: opts.order,
		Apply: opts.apply, Confirmation: opts.confirmation,
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
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
	set := flag.NewFlagSet("kalshirepair", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var result options
	set.StringVar(&result.account, "account", "", "exact internal execution account id (kalshi:<logical-id>)")
	set.StringVar(&result.order, "order", "", "exact local order id")
	set.BoolVar(&result.apply, "apply", false, "apply the repair; omitted means read-only dry-run")
	set.StringVar(&result.confirmation, "confirm", "", "required with --apply; exact account/order")
	set.DurationVar(&result.finalityGrace, "finality-grace", 30*time.Second, "minimum age of confirmed Kalshi cancellation (cannot be below 30s)")
	set.DurationVar(&result.timeout, "timeout", 20*time.Second, "total database/API timeout")
	if err := set.Parse(arguments); err != nil {
		return options{}, err
	}
	result.account, result.order = strings.TrimSpace(result.account), strings.TrimSpace(result.order)
	if result.account == "" || result.order == "" {
		return options{}, fmt.Errorf("--account and --order are required; broad scans are forbidden")
	}
	if _, err := kalshiLogicalAccountID(result.account); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	if result.finalityGrace < 30*time.Second {
		return options{}, fmt.Errorf("--finality-grace must be at least 30 seconds")
	}
	if result.timeout < time.Second {
		return options{}, fmt.Errorf("--timeout must be at least one second")
	}
	if result.apply && strings.TrimSpace(result.confirmation) != result.account+"/"+result.order {
		return options{}, fmt.Errorf("--confirm must exactly equal account/order when --apply is used")
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

func selectBinding(encoded, accountID string) (binding, error) {
	var bindings []binding
	if err := json.Unmarshal([]byte(strings.TrimSpace(encoded)), &bindings); err != nil {
		return binding{}, fmt.Errorf("decode KALSHI_LIVE_BINDINGS_JSON: %w", err)
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
		return binding{}, fmt.Errorf("no complete Kalshi live binding exists for execution account %q", accountID)
	}
	return selected, nil
}

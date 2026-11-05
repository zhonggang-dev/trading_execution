// nofillrelease proves, and only with explicit confirmation applies, that an
// UNKNOWN Polymarket order was never accepted by the venue: it then cancels the
// order and releases its frozen reservation in one audited transaction. It
// replaces hand-written release SQL. It never signs, submits or cancels at the
// venue: the CLOB client is held through a read-only interface and the RPC is
// only queried with eth_chainId, eth_blockNumber, eth_getBlockByNumber and
// eth_getLogs.
//
//	nofillrelease --order ORD --actor OP --reason TEXT                     # dry run
//	nofillrelease --order ORD --actor OP --reason TEXT --apply --confirm-id ID
//	    [--legacy-no-expiration --min-age 1h]   # historical GTC orders without a venue expiry
//	    [--max-chunks 2000] [--timeout 15m]
//
// The confirmation id is printed by the dry run. --apply collects every piece
// of evidence again and refuses unless it is complete and the id still matches.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/nofillrelease"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, buildLive); err != nil {
		fmt.Fprintln(os.Stderr, "nofillrelease:", redact(err.Error()))
		os.Exit(1)
	}
}

// secrets collects every value that must never be printed.
var secrets struct {
	mu     sync.Mutex
	values []string
}

func addSecret(value string) {
	value = strings.TrimSpace(value)
	if len(value) < 4 {
		return
	}
	secrets.mu.Lock()
	defer secrets.mu.Unlock()
	secrets.values = append(secrets.values, value)
}

func redact(message string) string {
	for _, key := range []string{"TRADING_EXECUTION_DATABASE_URL", "POLYGON_RPC_URL"} {
		addSecret(os.Getenv(key))
	}
	secrets.mu.Lock()
	defer secrets.mu.Unlock()
	for _, secret := range secrets.values {
		message = strings.ReplaceAll(message, secret, "<redacted>")
	}
	return message
}

// settings are the validated command-line options.
type settings struct {
	orderID   string
	actor     string
	reason    string
	apply     bool
	confirmID string
	policy    nofillrelease.Policy
	timeout   time.Duration
}

// evaluator is the part of nofillrelease.Resolver the tool uses.
type evaluator interface {
	Evaluate(ctx context.Context, orderID string, options nofillrelease.EvaluateOptions) (nofillrelease.Evaluation, error)
	Release(ctx context.Context, evaluation nofillrelease.Evaluation) (domain.Order, error)
}

// buildFunc wires the evaluator; closing releases its resources.
type buildFunc func(ctx context.Context, options settings) (evaluator, func(), error)

type output struct {
	Mode           string          `json:"mode"`
	OrderID        string          `json:"order_id"`
	ConfirmID      string          `json:"confirm_id"`
	Decision       string          `json:"decision"`
	DecisionReason string          `json:"decision_reason"`
	EvidenceSHA256 string          `json:"evidence_sha256,omitempty"`
	Evidence       json.RawMessage `json:"evidence,omitempty"`
	ResultStatus   string          `json:"result_status,omitempty"`
	ResultRevision int64           `json:"result_revision,omitempty"`
}

func run(args []string, stdout io.Writer, build buildFunc) error {
	options, err := parse(args)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), options.timeout)
	defer cancel()
	resolver, closer, err := build(ctx, options)
	if err != nil {
		return err
	}
	if closer != nil {
		defer closer()
	}
	evaluation, err := resolver.Evaluate(ctx, options.orderID, nofillrelease.EvaluateOptions{
		Mode: domain.NoFillModeOperator, Actor: options.actor, Reason: options.reason,
	})
	if err != nil {
		return err
	}
	result := output{
		Mode: "dry-run", OrderID: options.orderID, ConfirmID: evaluation.ConfirmID,
		Decision: string(evaluation.Decision.Kind), DecisionReason: evaluation.Decision.Reason,
		EvidenceSHA256: evaluation.Digest,
	}
	if len(evaluation.EvidenceJSON) > 0 {
		result.Evidence = json.RawMessage(evaluation.EvidenceJSON)
	}
	refusal := error(nil)
	if evaluation.Decision.Kind != nofillrelease.DecisionResolve {
		refusal = fmt.Errorf("refused: evidence is not complete (%s: %s); the order stays UNKNOWN and frozen",
			evaluation.Decision.Kind, evaluation.Decision.Reason)
	}
	if options.apply && refusal == nil {
		if evaluation.ConfirmID != options.confirmID {
			refusal = fmt.Errorf("refused: --confirm-id does not match the current confirm id %s; the order or its reservation changed since the dry run", evaluation.ConfirmID)
		} else {
			released, err := resolver.Release(ctx, evaluation)
			if err != nil {
				return fmt.Errorf("release failed; nothing was changed unless the commit outcome is unknown: %w", err)
			}
			result.Mode = "apply"
			result.ResultStatus = string(released.Status)
			result.ResultRevision = released.Revision
		}
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	return refusal
}

func parse(args []string) (settings, error) {
	flags := flag.NewFlagSet("nofillrelease", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	order := flags.String("order", "", "execution order id (ord-...)")
	actor := flags.String("actor", "", "accountable operator identity")
	reason := flags.String("reason", "", "approved release reason")
	apply := flags.Bool("apply", false, "release instead of only printing the evidence")
	confirm := flags.String("confirm-id", "", "confirm id printed by the dry run (required with --apply)")
	legacy := flags.Bool("legacy-no-expiration", false, "admit a historical order without a venue expiry")
	minAge := flags.Duration("min-age", time.Hour, "minimum submit age of a --legacy-no-expiration order")
	maxChunks := flags.Int("max-chunks", 2000, "largest number of 100-block eth_getLogs ranges to scan")
	timeout := flags.Duration("timeout", 15*time.Minute, "overall deadline")
	if err := flags.Parse(args); err != nil {
		return settings{}, err
	}
	if flags.NArg() != 0 {
		return settings{}, fmt.Errorf("unexpected positional arguments")
	}
	options := settings{
		orderID: strings.TrimSpace(*order), actor: strings.TrimSpace(*actor), reason: strings.TrimSpace(*reason),
		apply: *apply, confirmID: strings.TrimSpace(*confirm), timeout: *timeout,
	}
	if options.orderID == "" || options.actor == "" || options.reason == "" {
		return settings{}, fmt.Errorf("--order, --actor and --reason are required")
	}
	if options.apply && options.confirmID == "" {
		return settings{}, fmt.Errorf("--apply requires --confirm-id from a dry run")
	}
	if !options.apply && options.confirmID != "" {
		return settings{}, fmt.Errorf("--confirm-id is only valid with --apply")
	}
	if options.timeout < time.Minute || options.timeout > 2*time.Hour {
		return settings{}, fmt.Errorf("--timeout must be between 1m and 2h")
	}
	grace := 30 * time.Minute
	if raw := strings.TrimSpace(os.Getenv("ORDER_UNACCEPTED_RESOLVE_GRACE")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return settings{}, fmt.Errorf("ORDER_UNACCEPTED_RESOLVE_GRACE: %w", err)
		}
		grace = parsed
	}
	options.policy = nofillrelease.Policy{
		Grace: grace, MaxChunks: *maxChunks, AllowLegacyNoExpiry: *legacy, LegacyMinAge: *minAge,
		RequireLeaseFree: true,
	}
	if _, err := options.policy.Normalize(); err != nil {
		return settings{}, err
	}
	return options, nil
}

// buildLive wires PostgreSQL, the read-only CLOB evidence reader and the
// OrderFilled scanner.
func buildLive(ctx context.Context, options settings) (evaluator, func(), error) {
	databaseURL := strings.TrimSpace(os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
	rpcURL := strings.TrimSpace(os.Getenv("POLYGON_RPC_URL"))
	accountsFile := strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE"))
	if databaseURL == "" || rpcURL == "" || accountsFile == "" {
		return nil, nil, fmt.Errorf("TRADING_EXECUTION_DATABASE_URL, POLYGON_RPC_URL and POLYMARKET_ACCOUNTS_FILE are required")
	}
	addSecret(databaseURL)
	addSecret(rpcURL)
	confirmations, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("POLYGON_ORDER_FILLED_CONFIRMATIONS")), 10, 64)
	if err != nil || confirmations == 0 {
		return nil, nil, fmt.Errorf("POLYGON_ORDER_FILLED_CONFIRMATIONS must be a positive integer")
	}
	requestTimeout := 10 * time.Second
	if raw := strings.TrimSpace(os.Getenv("POLYMARKET_REQUEST_TIMEOUT")); raw != "" {
		if requestTimeout, err = time.ParseDuration(raw); err != nil {
			return nil, nil, fmt.Errorf("POLYMARKET_REQUEST_TIMEOUT: %w", err)
		}
	}
	clobURL := strings.TrimSpace(os.Getenv("POLYMARKET_CLOB_URL"))
	if clobURL == "" {
		clobURL = "https://clob.polymarket.com"
	}

	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	closer := func() { _ = database.Close() }
	fail := func(err error) (evaluator, func(), error) {
		closer()
		return nil, nil, err
	}
	if err := database.PingContext(ctx); err != nil {
		return fail(fmt.Errorf("connect database: %w", err))
	}
	present, err := postgresadapter.NoFillReleaseTablesPresent(ctx, database)
	if err != nil {
		return fail(err)
	}
	if !present {
		return fail(errors.New("migration 0046 (no-fill release audit) is not applied; refusing to run"))
	}
	store, err := postgresadapter.NewNoFillReleaseStore(database, nil)
	if err != nil {
		return fail(err)
	}

	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{Path: accountsFile})
	if err != nil {
		return fail(fmt.Errorf("load Polymarket accounts: %w", err))
	}
	for _, account := range accounts {
		addSecret(account.API.Key)
		addSecret(account.API.Secret)
		addSecret(account.API.Passphrase)
		addSecret(account.Relayer.Key)
	}
	provider, err := polymarket.NewStaticCredentialProvider(accounts)
	if err != nil {
		return fail(err)
	}
	client, err := polymarket.NewTradingClient(polymarket.TradingClientParams{
		BaseURL: clobURL, Credentials: provider, RequestTimeout: requestTimeout,
	})
	if err != nil {
		return fail(err)
	}
	if _, err := client.ProbeProtocol(ctx, 3*time.Second); err != nil {
		return fail(fmt.Errorf("CLOB protocol preflight: %w", err))
	}
	// Only the read-only interface leaves this function: the tool cannot place
	// or cancel through it.
	var reads polymarket.NoFillCLOBReader = client
	venue, err := polymarket.NewNoFillEvidenceReader(reads, nil)
	if err != nil {
		return fail(err)
	}
	scanner, err := evmrpc.NewOrderFilledScanner(evmrpc.OrderFilledScanParams{
		RPCURL: rpcURL, RequiredConfirmations: confirmations, RequestTimeout: requestTimeout,
	})
	if err != nil {
		return fail(err)
	}
	resolver, err := nofillrelease.New(nofillrelease.Params{
		Store: store, Venue: venue, Chain: scanner, Policy: options.policy, Mode: nofillrelease.ModeOff,
		Actor: options.actor, Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		return fail(err)
	}
	return resolver, closer, nil
}

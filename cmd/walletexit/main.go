// walletexit is an operator-only cutover command. It is an audit/read-only
// command by default; --execute is the only path that can sign or POST a
// reviewed SELL manifest, and it requires an exact SHA256 confirmation.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/walletcutover"
	walletcutoverexit "github.com/UniPat-AI/trading_execution/internal/service/walletcutoverexit"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultTimeout = 2 * time.Minute
	zeroBytes32    = "0x0000000000000000000000000000000000000000000000000000000000000000"
)

type commandOptions struct {
	manifest     string
	journal      string
	accountsFile string
	clobURL      string
	rpcURL       string
	databaseURL  string
	confirm      string
	actor        string
	reason       string
	timeout      time.Duration
	execute      bool
}

type memoryJournal struct{ value walletcutoverexit.Journal }

type walletExitSnapshotStore interface {
	Snapshot(context.Context, []string) ([]walletcutover.AccountState, error)
}

// walletExitSnapshotStoreFactory keeps the production CLI wired to the real
// PostgreSQL guard while allowing command-level tests to inject a narrow,
// read-only guard double. It is deliberately limited to Snapshot; execution
// never receives a ledger mutation capability.
var walletExitSnapshotStoreFactory = func(db *sql.DB) (walletExitSnapshotStore, error) {
	return postgresadapter.NewWalletCutoverStore(db)
}

// walletExitProcessFreezeCheck is deliberately required only for --execute.
// Account pause/global kill switches do not stop independent auto-redeem and
// reconciliation loops, so production asset mutation requires the service
// unit itself to be inactive with MainPID=0. Tests may inject a narrow no-op
// check; the production default always reads systemd directly.
var walletExitProcessFreezeCheck = requireTradingServiceStopped

func (journal *memoryJournal) Load() (walletcutoverexit.Journal, error) {
	if journal.value.Version == 0 {
		journal.value.Version = 1
	}
	return journal.value, nil
}

func (journal *memoryJournal) Save(value walletcutoverexit.Journal) error {
	journal.value = value
	return nil
}

func main() {
	options, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(root, options.timeout)
	defer cancel()
	result, err := run(ctx, options)
	if err != nil {
		// Do not print a wrapped URL, request body, signature or credential. The
		// service truncates journal errors and adapter errors are intentionally
		// emitted as a generic operator failure here.
		fmt.Fprintln(os.Stderr, "wallet exit failed:", safeCommandError(err))
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "encode wallet exit result:", err)
		os.Exit(1)
	}
}

func parseOptions(arguments []string) (commandOptions, error) {
	options := commandOptions{
		accountsFile: strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")),
		clobURL:      env("POLYMARKET_CLOB_URL", "https://clob.polymarket.com"),
		rpcURL:       strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")),
		databaseURL:  strings.TrimSpace(os.Getenv("TRADING_EXECUTION_DATABASE_URL")),
		timeout:      defaultTimeout,
	}
	flags := flag.NewFlagSet("walletexit", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.manifest, "manifest", "", "exact reviewed wallet exit manifest")
	flags.StringVar(&options.journal, "journal-file", "", "absolute 0600 execution journal path")
	flags.StringVar(&options.accountsFile, "accounts-file", options.accountsFile, "private Polymarket accounts file")
	flags.StringVar(&options.clobURL, "clob-url", options.clobURL, "Polymarket CLOB URL")
	flags.StringVar(&options.rpcURL, "rpc-url", options.rpcURL, "Polygon JSON-RPC URL")
	flags.StringVar(&options.databaseURL, "database-url", options.databaseURL, "read-only Trading Execution PostgreSQL URL")
	flags.StringVar(&options.confirm, "confirm-sha256", "", "exact SHA256 of reviewed manifest bytes")
	flags.StringVar(&options.actor, "actor", "", "accountable operator identity")
	flags.StringVar(&options.reason, "reason", "", "approved maintenance reason")
	flags.DurationVar(&options.timeout, "timeout", options.timeout, "whole-command timeout")
	flags.BoolVar(&options.execute, "execute", false, "sign and submit the reviewed manifest")
	if err := flags.Parse(arguments); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected positional arguments")
	}
	options.manifest = strings.TrimSpace(options.manifest)
	if options.manifest == "" {
		return commandOptions{}, fmt.Errorf("--manifest is required")
	}
	if options.timeout <= 0 {
		return commandOptions{}, fmt.Errorf("--timeout must be positive")
	}
	if options.execute {
		if options.journal == "" || options.accountsFile == "" || options.rpcURL == "" || options.databaseURL == "" {
			return commandOptions{}, fmt.Errorf("--execute requires --journal-file, --accounts-file/POLYMARKET_ACCOUNTS_FILE, --rpc-url/POLYGON_RPC_URL, and --database-url/TRADING_EXECUTION_DATABASE_URL")
		}
		if options.confirm == "" || options.actor == "" || options.reason == "" {
			return commandOptions{}, fmt.Errorf("--execute requires --confirm-sha256, --actor, and --reason")
		}
	}
	return options, nil
}

func run(ctx context.Context, options commandOptions) (walletcutoverexit.RunResult, error) {
	if options.execute {
		info, err := os.Lstat(options.manifest)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
			return walletcutoverexit.RunResult{}, fmt.Errorf("execute manifest must be a private 0600 regular file")
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != os.Geteuid() {
			return walletcutoverexit.RunResult{}, fmt.Errorf("execute manifest must be owned by the current operator")
		}
	}
	data, err := os.ReadFile(options.manifest)
	if err != nil {
		return walletcutoverexit.RunResult{}, fmt.Errorf("read manifest: %w", err)
	}
	var manifest walletcutoverexit.Manifest
	var digest string
	if options.execute {
		manifest, digest, err = walletcutoverexit.ParseManifestForRecovery(data, time.Now().UTC())
	} else {
		manifest, digest, err = walletcutoverexit.ParseManifest(data, time.Now().UTC())
	}
	if err != nil {
		return walletcutoverexit.RunResult{}, err
	}
	if !options.execute {
		service, err := walletcutoverexit.New(walletcutoverexit.Params{Journal: &memoryJournal{}})
		if err != nil {
			return walletcutoverexit.RunResult{}, err
		}
		return service.Run(ctx, manifest, digest, walletcutoverexit.RunOptions{})
	}
	if !strings.EqualFold(strings.TrimSpace(options.confirm), digest) {
		return walletcutoverexit.RunResult{}, walletcutoverexit.ErrConfirmationRequired
	}
	if err := walletExitProcessFreezeCheck(ctx); err != nil {
		return walletcutoverexit.RunResult{}, err
	}
	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{Path: options.accountsFile, BootstrapMissingAPICredentials: false})
	if err != nil {
		return walletcutoverexit.RunResult{}, fmt.Errorf("load execution accounts: %w", err)
	}
	provider, err := polymarket.NewStaticCredentialProvider(accounts)
	if err != nil {
		return walletcutoverexit.RunResult{}, fmt.Errorf("build execution account provider: %w", err)
	}
	httpClient := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	client, err := polymarket.NewTradingClient(polymarket.TradingClientParams{
		BaseURL: options.clobURL, HTTPClient: httpClient, Credentials: provider,
		BuilderCode: env("POLYMARKET_BUILDER_CODE", zeroBytes32), Metadata: env("POLYMARKET_ORDER_METADATA", zeroBytes32),
		RequestTimeout: 15 * time.Second,
	})
	if err != nil {
		return walletcutoverexit.RunResult{}, fmt.Errorf("configure CLOB adapter: %w", err)
	}
	venue, err := polymarket.NewConditionalFundingVenue(client, client)
	if err != nil {
		return walletcutoverexit.RunResult{}, err
	}
	books, err := polymarket.NewOrderBookSource(polymarket.OrderBookParams{BaseURL: options.clobURL, HTTPClient: httpClient, Depth: 15, Workers: 1})
	if err != nil {
		return walletcutoverexit.RunResult{}, err
	}
	reader, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{RPCURL: options.rpcURL, RequiredConfirmations: 128, HTTPClient: httpClient, RequestTimeout: 15 * time.Second})
	if err != nil {
		return walletcutoverexit.RunResult{}, fmt.Errorf("configure Polygon read-only evidence: %w", err)
	}
	preflight := &clobPreflight{client: client, books: books, reader: reader, accounts: accountMap(accounts), block: manifest.Observed.Block, blockHash: strings.ToLower(manifest.Observed.BlockHash)}
	var redemptionVenue port.RedemptionVenue
	var redemptionCheck *redemptionPreflight
	if len(manifest.Redemptions) > 0 {
		redemptionReader, readerErr := evmrpc.NewRedemptionEvidenceReader(evmrpc.RedemptionEvidenceParams{RPCURL: options.rpcURL, RequiredConfirmations: 128, HTTPClient: httpClient})
		if readerErr != nil {
			return walletcutoverexit.RunResult{}, fmt.Errorf("configure redemption receipt evidence: %w", readerErr)
		}
		redemptionVenue, err = polymarket.NewRedemptionClient(polymarket.RedemptionClientParams{
			RelayerURL: env("POLYMARKET_RELAYER_URL", "https://relayer-v2.polymarket.com"), RPCURL: options.rpcURL,
			HTTPClient: httpClient, Credentials: provider, RequestTimeout: 15 * time.Second, RequiredConfirmations: 128,
		})
		if err != nil {
			return walletcutoverexit.RunResult{}, fmt.Errorf("configure redemption client: %w", err)
		}
		redemptionCheck = &redemptionPreflight{venue: redemptionVenue, reader: redemptionReader, chain: reader, accounts: accountMap(accounts), block: manifest.Observed.Block, blockHash: strings.ToLower(manifest.Observed.BlockHash)}
	}
	journal, err := walletcutoverexit.NewFileJournalStore(options.journal)
	if err != nil {
		return walletcutoverexit.RunResult{}, err
	}
	db, err := sql.Open("pgx", options.databaseURL)
	if err != nil {
		return walletcutoverexit.RunResult{}, fmt.Errorf("open read-only local database: %w", err)
	}
	defer db.Close()
	store, err := walletExitSnapshotStoreFactory(db)
	if err != nil {
		return walletcutoverexit.RunResult{}, err
	}
	guard := &databaseGuard{store: store}
	service, err := walletcutoverexit.New(walletcutoverexit.Params{Venue: venue, Preflight: preflight, Guard: guard, Redemption: redemptionVenue, RedemptionCheck: redemptionCheck, Journal: journal})
	if err != nil {
		return walletcutoverexit.RunResult{}, err
	}
	return service.Run(ctx, manifest, digest, walletcutoverexit.RunOptions{Execute: true, ConfirmSHA256: options.confirm, Actor: options.actor, Reason: options.reason})
}

func requireTradingServiceStopped(ctx context.Context) error {
	stateResult := exec.CommandContext(ctx, "systemctl", "is-active", "trading-execution.service")
	stateOutput, err := stateResult.Output()
	if err != nil && stateResult.ProcessState == nil {
		return fmt.Errorf("cannot inspect trading-execution.service state: %w", err)
	}
	state := strings.TrimSpace(string(stateOutput))
	if state != "inactive" && state != "failed" {
		return fmt.Errorf("trading-execution.service is not stopped: %s", firstNonEmptyText(state, "unknown"))
	}
	pidOutput, err := exec.CommandContext(ctx, "systemctl", "show", "trading-execution.service", "-p", "MainPID", "--value").Output()
	if err != nil {
		return fmt.Errorf("cannot inspect trading-execution.service MainPID: %w", err)
	}
	pid := strings.TrimSpace(string(pidOutput))
	if pid != "0" {
		return fmt.Errorf("stopped trading-execution.service still has MainPID %s", firstNonEmptyText(pid, "unknown"))
	}
	return nil
}

func firstNonEmptyText(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

type databaseGuard struct {
	store walletExitSnapshotStore
}

func (guard *databaseGuard) Check(ctx context.Context, _ string) error {
	states, err := guard.store.Snapshot(ctx, []string{"main", "wallet-1"})
	if err != nil {
		return fmt.Errorf("read local cutover guard: %w", err)
	}
	if len(states) != 2 {
		return fmt.Errorf("local cutover guard returned %d accounts, want 2", len(states))
	}
	for _, state := range states {
		if !state.Paused {
			return fmt.Errorf("execution account %s is not paused", state.ExecutionAccountID)
		}
		if state.RunningRecon || state.PendingOrder || state.ActiveReservation || state.PendingFill || state.PendingDelivery || state.PendingRedemption {
			return fmt.Errorf("execution account %s still has in-flight local work", state.ExecutionAccountID)
		}
	}
	return nil
}

type clobPreflight struct {
	client    *polymarket.TradingClient
	books     *polymarket.OrderBookSource
	reader    *evmrpc.OrderFilledEvidenceReader
	accounts  map[string]polymarket.TradingAccount
	block     uint64
	blockHash string
}

type redemptionPreflight struct {
	venue     port.RedemptionVenue
	reader    *evmrpc.RedemptionEvidenceReader
	chain     *evmrpc.OrderFilledEvidenceReader
	accounts  map[string]polymarket.TradingAccount
	block     uint64
	blockHash string
}

func (source *redemptionPreflight) Check(ctx context.Context, action walletcutoverexit.RedemptionAction) (walletcutoverexit.RedemptionPreflight, error) {
	account, exists := source.accounts[action.ExecutionAccountID]
	if !exists || !strings.EqualFold(account.FunderAddress, action.WalletAddress) {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption wallet identity does not match execution account")
	}
	latest, err := source.readerBlock(ctx)
	if err != nil {
		return walletcutoverexit.RedemptionPreflight{}, err
	}
	if latest < source.block+128 {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption evidence block is not finalized")
	}
	canonical, observedAt, err := source.chain.BlockHeader(ctx, source.block)
	if err != nil || !strings.EqualFold(canonical, source.blockHash) {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption evidence block hash is not canonical")
	}
	approved, err := source.venue.RedemptionApproved(ctx, action.WalletAddress, action.NegRisk)
	if err != nil {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption approval read failed")
	}
	if !approved {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption adapter approval is absent; walletexit never auto-approves")
	}
	payout, err := source.chain.ReadConditionPayout(ctx, action.ConditionID, fmt.Sprintf("0x%x", source.block))
	if err != nil || payout.Denominator.Sign() <= 0 {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("condition is not settled")
	}
	expectedPayout, payoutErr := conditionPayoutBaseUnits(action, payout)
	if payoutErr != nil {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("condition payout evidence is invalid: %w", payoutErr)
	}
	if !equalBaseUnits(expectedPayout, action.PayoutBaseUnits) {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("condition payout differs from manifest")
	}
	for index, tokenID := range action.TokenIDs {
		balance, balanceErr := source.chain.ReadTokenBalance(ctx, evmrpc.PolymarketConditionalTokensAddress, action.WalletAddress, tokenID, fmt.Sprintf("0x%x", source.block))
		if balanceErr != nil {
			return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("read redemption token balance failed")
		}
		actual, parseErr := domain.ParseDecimal(balance)
		if parseErr != nil {
			return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption token balance is invalid")
		}
		if comparison, compareErr := actual.Compare(action.Shares[index]); compareErr != nil || comparison < 0 {
			return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption token balance is below manifest shares")
		}
	}
	gas, err := source.readerNativeBalance(ctx, action.WalletAddress, fmt.Sprintf("0x%x", source.block))
	if err != nil {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("read redemption gas balance failed")
	}
	if sign, err := gas.Sign(); err != nil || sign <= 0 {
		return walletcutoverexit.RedemptionPreflight{}, fmt.Errorf("redemption gas balance is not positive")
	}
	return walletcutoverexit.RedemptionPreflight{WalletAddress: action.WalletAddress, Settled: true, PayoutBaseUnits: action.PayoutBaseUnits, Approved: true, BalanceVerified: true, ObservedBlock: source.block, ObservedBlockHash: canonical, ObservedAt: observedAt}, nil
}

func (source *redemptionPreflight) VerifyReceipt(ctx context.Context, action walletcutoverexit.RedemptionAction, submission domain.RedemptionSubmission) error {
	if strings.TrimSpace(submission.TransactionHash) == "" {
		return fmt.Errorf("redemption submission has no transaction hash")
	}
	receipt, err := source.reader.ResolveRedemptionReceipt(ctx, submission.TransactionHash, action.WalletAddress, action.ConditionID, action.NegRisk)
	if err != nil {
		return err
	}
	if !equalBaseUnits(receipt.PayoutBaseUnits, action.PayoutBaseUnits) {
		return fmt.Errorf("redemption receipt payout differs from manifest")
	}
	return nil
}

func equalBaseUnits(left, right string) bool {
	leftValue, leftOK := new(big.Int).SetString(strings.TrimSpace(left), 10)
	rightValue, rightOK := new(big.Int).SetString(strings.TrimSpace(right), 10)
	return leftOK && rightOK && leftValue.Sign() >= 0 && rightValue.Sign() >= 0 && leftValue.Cmp(rightValue) == 0
}

func conditionPayoutBaseUnits(action walletcutoverexit.RedemptionAction, payout evmrpc.ConditionPayout) (string, error) {
	if payout.Denominator == nil || payout.Denominator.Sign() <= 0 {
		return "", fmt.Errorf("payout denominator must be positive")
	}
	if len(action.Shares) != len(action.OutcomeIndexes) || len(action.Shares) == 0 {
		return "", fmt.Errorf("payout shares and outcome indexes must align")
	}
	total := new(big.Rat)
	for index, shares := range action.Shares {
		outcome := action.OutcomeIndexes[index]
		if outcome < 0 || outcome >= len(payout.Numerators) || payout.Numerators[outcome] == nil || payout.Numerators[outcome].Sign() < 0 {
			return "", fmt.Errorf("payout numerator for outcome %d is invalid", outcome)
		}
		term, err := shares.Multiply(domain.Decimal(payout.Numerators[outcome].String()))
		if err != nil {
			return "", fmt.Errorf("shares[%d] is invalid: %w", index, err)
		}
		term.Quo(term, new(big.Rat).SetInt(payout.Denominator))
		term.Mul(term, big.NewRat(1_000_000, 1))
		total.Add(total, term)
	}
	if total.Denom().Cmp(big.NewInt(1)) != 0 {
		return "", fmt.Errorf("payout is not an integer pUSD base-unit amount")
	}
	return total.Num().String(), nil
}

func (source *redemptionPreflight) readerBlock(ctx context.Context) (uint64, error) {
	// The receipt reader intentionally exposes the same finalized head used by
	// its canonical-block checks through the evidence call below.
	return source.chain.ObservedBlock(ctx)
}

func (source *redemptionPreflight) readerNativeBalance(ctx context.Context, wallet, block string) (domain.Decimal, error) {
	return source.chain.NativeBalanceAt(ctx, wallet, block)
}

func (source *clobPreflight) Check(ctx context.Context, action walletcutoverexit.AssetAction) (walletcutoverexit.Preflight, error) {
	account, exists := source.accounts[action.ExecutionAccountID]
	if !exists || !strings.EqualFold(account.FunderAddress, action.WalletAddress) {
		return walletcutoverexit.Preflight{}, fmt.Errorf("execution account %q wallet identity does not match manifest", action.ExecutionAccountID)
	}
	probe, err := source.client.ProbeAccount(ctx, action.ExecutionAccountID)
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("account read failed")
	}
	closedOnly, err := source.client.ClosedOnly(ctx, action.ExecutionAccountID)
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("account eligibility read failed")
	}
	allowance, err := source.client.GetBalanceAllowance(ctx, action.ExecutionAccountID, polymarket.BalanceAssetConditional, action.TokenID)
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("conditional funding read failed")
	}
	requiredExchange := polymarket.StandardExchangeV2Address
	if action.NegRisk {
		requiredExchange = polymarket.NegRiskExchangeV2Address
	}
	if !allowance.RequiredAllowancesPositive(requiredExchange) {
		return walletcutoverexit.Preflight{}, fmt.Errorf("conditional token approval is absent")
	}
	actual, err := decimalFromBaseUnits(allowance.Balance, 6)
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("conditional token balance is invalid")
	}
	if comparison, err := actual.Compare(action.Shares); err != nil || comparison < 0 {
		return walletcutoverexit.Preflight{}, fmt.Errorf("CLOB conditional balance is below requested shares")
	}
	tick, err := source.client.GetTickSize(ctx, action.TokenID)
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("tick size read failed")
	}
	negRisk, err := source.client.GetNegRisk(ctx, action.TokenID)
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("neg-risk read failed")
	}
	books, err := source.books.Capture(ctx, time.Now().UTC(), []domain.BookTarget{{MarketSource: domain.MarketSourcePolymarket, MarketID: action.MarketID, ConditionID: action.ConditionID, OutcomeIndex: action.OutcomeIndex, TokenID: action.TokenID}})
	if err != nil || len(books) != 1 || books[0].Status != domain.OrderBookStatusOK {
		return walletcutoverexit.Preflight{}, fmt.Errorf("recent best-bid snapshot unavailable")
	}
	latestBlock, err := source.reader.ObservedBlock(ctx)
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("chain block read failed")
	}
	if source.block == 0 || latestBlock < source.block+128 {
		return walletcutoverexit.Preflight{}, fmt.Errorf("manifest evidence block is not finalized")
	}
	canonicalHash, _, err := source.reader.BlockHeader(ctx, source.block)
	if err != nil || !strings.EqualFold(canonicalHash, source.blockHash) {
		return walletcutoverexit.Preflight{}, fmt.Errorf("manifest evidence block hash is not canonical")
	}
	observedBlock := source.block
	chainShares, err := source.reader.ReadTokenBalance(ctx, evmrpc.PolymarketConditionalTokensAddress, action.WalletAddress, action.TokenID, fmt.Sprintf("0x%x", observedBlock))
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("chain conditional balance read failed")
	}
	gas, err := source.reader.NativeBalanceAt(ctx, account.FunderAddress, fmt.Sprintf("0x%x", observedBlock))
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("native gas balance read failed")
	}
	if sign, err := gas.Sign(); err != nil || sign <= 0 {
		return walletcutoverexit.Preflight{}, fmt.Errorf("native gas balance is not positive")
	}
	orders, err := source.client.ListOpenOrders(ctx, action.ExecutionAccountID, polymarket.OpenOrderFilter{TokenID: action.TokenID})
	if err != nil {
		return walletcutoverexit.Preflight{}, fmt.Errorf("open-order read failed")
	}
	return walletcutoverexit.Preflight{WalletAddress: account.FunderAddress, ActualShares: domain.Decimal(chainShares), TickSize: tick,
		NegRisk: negRisk, Approved: true, Eligible: !closedOnly, OpenOrderCount: max(probe.OpenOrderCount, len(orders)), BestBid: books[0].BestBid,
		ObservedAt: time.Now().UTC(), ObservedBlock: observedBlock}, nil
}

func accountMap(accounts []polymarket.TradingAccount) map[string]polymarket.TradingAccount {
	result := make(map[string]polymarket.TradingAccount, len(accounts))
	for _, account := range accounts {
		result[account.ExecutionAccountID] = account
	}
	return result
}

func decimalFromBaseUnits(raw string, decimals int) (domain.Decimal, error) {
	units, ok := new(big.Int).SetString(strings.TrimSpace(raw), 10)
	if !ok || units.Sign() < 0 {
		return "", fmt.Errorf("invalid base-unit quantity")
	}
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	quotient := new(big.Rat).SetFrac(units, denominator)
	return domain.Decimal(quotient.FloatString(decimals)).Canonical(), nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func safeCommandError(err error) string {
	text := redactURLCredentials(strings.TrimSpace(err.Error()))
	for _, marker := range []string{"?key=", "?secret=", "?passphrase=", "api_key=", "api_secret=", "private_key="} {
		if index := strings.Index(strings.ToLower(text), marker); index >= 0 {
			text = text[:index] + "?redacted"
		}
	}
	if len(text) > 512 {
		text = text[:512]
	}
	return text
}

func redactURLCredentials(text string) string {
	for _, scheme := range []string{"postgres://", "postgresql://", "https://", "http://"} {
		start := strings.Index(strings.ToLower(text), scheme)
		if start < 0 {
			continue
		}
		relEnd := strings.IndexAny(text[start:], " \t\r\n,;)")
		end := len(text)
		if relEnd >= 0 {
			end = start + relEnd
		}
		parsed, parseErr := url.Parse(text[start:end])
		if parseErr != nil || parsed.User == nil {
			continue
		}
		username := parsed.User.Username()
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(username, "redacted")
		} else {
			parsed.User = url.User(username)
		}
		text = text[:start] + parsed.String() + text[end:]
	}
	return text
}

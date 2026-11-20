// eoaflatten is a one-off operator tool that clears the old Polymarket
// positions of an EOA wallet that is not managed by the trading service
// (signature_type 0, funder == EOA): unsettled positions are sold on the CLOB
// with FAK orders, settled ones are redeemed on-chain through the pUSD
// collateral adapters.
//
// It is a dry run unless --execute is given. The dry run reads positions,
// order books and chain state, simulates every redemption with eth_call and
// never places an order or broadcasts a transaction. Keys are read only from
// the accounts file named by POLYMARKET_ACCOUNTS_FILE; nothing secret is
// accepted on the command line or printed.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const userAgent = "trading-execution/eoaflatten"

type options struct {
	account           string
	execute           bool
	only              string
	sellSlippageTicks int
	confirmations     uint64
	txTimeout         time.Duration
	timeout           time.Duration
}

func main() {
	opts, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "参数错误：%v\n用法：eoaflatten --account wallet-2 [--execute] [--only sell|redeem] [--sell-slippage-ticks N]\n", err)
		os.Exit(2)
	}
	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootContext, opts.timeout)
	defer cancel()
	result, err := runCommand(ctx, opts, os.Stdout)
	if err != nil {
		slog.Error("eoaflatten failed", "error", err)
		os.Exit(1)
	}
	if len(result.Failures) > 0 {
		os.Exit(1)
	}
}

func parseOptions(arguments []string) (options, error) {
	opts := options{confirmations: 3, txTimeout: 5 * time.Minute, timeout: 30 * time.Minute}
	flags := flag.NewFlagSet("eoaflatten", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.account, "account", "", "execution_account_id in the accounts file, e.g. wallet-2")
	flags.BoolVar(&opts.execute, "execute", false, "really sell and redeem; default is a dry run")
	flags.StringVar(&opts.only, "only", onlyAll, "limit to sell or redeem")
	flags.IntVar(&opts.sellSlippageTicks, "sell-slippage-ticks", 0, "ticks below the best bid for the FAK limit")
	flags.Uint64Var(&opts.confirmations, "confirmations", opts.confirmations, "block confirmations to wait for each transaction")
	flags.DurationVar(&opts.txTimeout, "tx-timeout", opts.txTimeout, "maximum wait for one transaction to confirm")
	flags.DurationVar(&opts.timeout, "timeout", opts.timeout, "whole-command timeout")
	if err := flags.Parse(arguments); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	opts.account = strings.TrimSpace(opts.account)
	if opts.account == "" {
		return options{}, fmt.Errorf("--account is required")
	}
	opts.only = strings.ToLower(strings.TrimSpace(opts.only))
	if opts.only != onlyAll && opts.only != onlySell && opts.only != onlyRedeem {
		return options{}, fmt.Errorf("--only must be sell or redeem")
	}
	if opts.sellSlippageTicks < 0 || opts.sellSlippageTicks > 20 {
		return options{}, fmt.Errorf("--sell-slippage-ticks must be between 0 and 20")
	}
	if opts.confirmations == 0 || opts.confirmations > 256 {
		return options{}, fmt.Errorf("--confirmations must be between 1 and 256")
	}
	if opts.txTimeout < 10*time.Second || opts.timeout <= 0 {
		return options{}, fmt.Errorf("--tx-timeout must be at least 10s and --timeout positive")
	}
	return opts, nil
}

// environment holds the non-secret endpoints; the accounts file path is the
// only way keys reach the process.
type environment struct {
	accountsFile string
	rpcURL       string
	clobURL      string
	dataAPIURL   string
}

func readEnvironment() (environment, error) {
	env := environment{
		accountsFile: strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")),
		rpcURL:       strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")),
		clobURL:      envOr("POLYMARKET_CLOB_URL", "https://clob.polymarket.com"),
		dataAPIURL:   envOr("POLYMARKET_DATA_API_URL", "https://data-api.polymarket.com"),
	}
	if env.accountsFile == "" || env.rpcURL == "" {
		return environment{}, fmt.Errorf("POLYMARKET_ACCOUNTS_FILE and POLYGON_RPC_URL are required")
	}
	return env, nil
}

func runCommand(ctx context.Context, opts options, out io.Writer) (summary, error) {
	env, err := readEnvironment()
	if err != nil {
		return summary{}, err
	}
	const requestTimeout = 15 * time.Second
	// Without --execute the bootstrapper only derives (GET) an existing key.
	bootstrapper, err := polymarket.NewCredentialBootstrapper(polymarket.CredentialBootstrapParams{
		BaseURL: env.clobURL, Timeout: requestTimeout, DeriveOnly: !opts.execute,
	})
	if err != nil {
		return summary{}, err
	}
	account, err := polymarket.LoadTradingAccount(ctx, polymarket.WalletLoadParams{
		Path: env.accountsFile, CredentialBootstrapper: bootstrapper, BootstrapMissingAPICredentials: true,
	}, opts.account)
	if err != nil {
		return summary{}, err
	}
	if account.SignatureType != polymarket.SignatureTypeEOA || !strings.EqualFold(account.FunderAddress, account.Signer.Address()) {
		return summary{}, fmt.Errorf("account %s is not an EOA wallet (signature_type %d)", opts.account, account.SignatureType)
	}
	clients, err := newClients(env, account, opts, requestTimeout)
	if err != nil {
		return summary{}, err
	}
	probe, probeErr := clients.trading.ProbeAccount(ctx, account.ExecutionAccountID)
	if probeErr != nil {
		var venueError *port.VenueError
		if !errors.As(probeErr, &venueError) || venueError.Code != "CLOB_AUTH_FAILED" {
			return summary{}, fmt.Errorf("CLOB authenticated probe: %w", probeErr)
		}
		// Stored L2 credentials may have been revoked; derive (or, with
		// --execute, create) the deterministic key once and retry.
		fmt.Fprintf(out, "提示：CLOB 凭据被拒（%v），重新推导 L2 凭据\n", probeErr)
		credentials, deriveErr := bootstrapper.CreateOrDerive(ctx, account.Signer, 0)
		if deriveErr != nil {
			return summary{}, fmt.Errorf("CLOB credentials are invalid and re-derivation failed: %w", deriveErr)
		}
		account.API = credentials
		if clients, err = newClients(env, account, opts, requestTimeout); err != nil {
			return summary{}, err
		}
		if probe, probeErr = clients.trading.ProbeAccount(ctx, account.ExecutionAccountID); probeErr != nil {
			return summary{}, fmt.Errorf("CLOB credentials still rejected after re-derivation: %w", probeErr)
		}
	}
	work := &runner{
		out: out, opts: opts, accountID: account.ExecutionAccountID, wallet: strings.ToLower(account.FunderAddress),
		positions: clients.positions, books: clients.books, clob: clients.trading, chain: clients.chain,
		openOrders:   probe.OpenOrderCount,
		pollInterval: 3 * time.Second, sleep: sleepContext, now: time.Now,
	}
	return work.run(ctx)
}

type liveClients struct {
	trading   *polymarket.TradingClient
	chain     *polymarket.RedemptionClient
	positions *polymarketdata.PositionClient
	books     *polymarket.OrderBookSource
}

func newClients(env environment, account polymarket.TradingAccount, opts options, requestTimeout time.Duration) (liveClients, error) {
	provider, err := polymarket.NewStaticCredentialProvider([]polymarket.TradingAccount{account})
	if err != nil {
		return liveClients{}, err
	}
	trading, err := polymarket.NewTradingClient(polymarket.TradingClientParams{
		BaseURL: env.clobURL, HTTPClient: httpClient(requestTimeout), Credentials: provider, RequestTimeout: requestTimeout,
	})
	if err != nil {
		return liveClients{}, err
	}
	chain, err := polymarket.NewRedemptionClient(polymarket.RedemptionClientParams{
		RPCURL: env.rpcURL, HTTPClient: httpClient(requestTimeout), Credentials: provider,
		RequestTimeout: requestTimeout, RequiredConfirmations: opts.confirmations,
	})
	if err != nil {
		return liveClients{}, err
	}
	positions, err := polymarketdata.NewPositionClient(polymarketdata.PositionClientParams{
		BaseURL: env.dataAPIURL, HTTPClient: httpClient(requestTimeout),
	})
	if err != nil {
		return liveClients{}, err
	}
	books, err := polymarket.NewOrderBookSource(polymarket.OrderBookParams{
		BaseURL: env.clobURL, HTTPClient: httpClient(requestTimeout), Workers: 1,
	})
	if err != nil {
		return liveClients{}, err
	}
	return liveClients{trading: trading, chain: chain, positions: positions, books: books}, nil
}

// httpClient refuses redirects and adds a User-Agent: the production Polygon
// RPC rejects requests without one (HTTP 403), and RedemptionClient does not
// set it itself.
func httpClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: userAgentTransport{base: http.DefaultTransport, userAgent: userAgent},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type userAgentTransport struct {
	base      http.RoundTripper
	userAgent string
}

func (transport userAgentTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Header.Get("User-Agent") == "" {
		request = request.Clone(request.Context())
		request.Header.Set("User-Agent", transport.userAgent)
	}
	return transport.base.RoundTrip(request)
}

func sleepContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

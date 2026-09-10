// One-off operator maintenance for one explicitly authorized partial SELL.
// No HTTP mutations are permitted. Default mode is database read-only.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
	"github.com/UniPat-AI/trading_execution/internal/service/manualsellclosure"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var target = manualsellclosure.Target{
	Account: "wallet-7", Order: "ord-c66b95f95d4d66736c493be062ed0d13",
	VenueOrder: "0xe06c270d4768acfa76214fffcd218f779782048d759e0957c7070d9636ccbb20",
	Market:     "3701690", Condition: "0xcfbc2e5fbabb9e8a03bf234367244d4c97eaaf3ac33e2ba001541ee3e0e62416",
	Token:  "113115102429687063022899186163791494032794866907263639710901400810753815113311",
	Lot:    "lot:fill-70cb20a40d58f11a387b1615d4d2e9d7679f8c9525fa857a1624835da853853b",
	Wallet: "0xdd9275d3b1d2c423e19724fcd09c19abb20aa167",
	Trade:  "d00c4de3-c94e-4716-85b2-00512b4f5b5a", Transaction: "0x0620529cbf654e148db2162d44058d169f31303d77ccc81243a5af78283a9263",
	Block: 93541361, Log: 325, Requested: "48", Filled: "19.01", Price: "0.038", Gross: "0.72238", Remaining: "28.99",
}
var secrets []string

func emit(label string, value any) {
	b, _ := json.Marshal(value)
	s := string(b)
	for _, secret := range secrets {
		if len(secret) > 4 {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
	}
	fmt.Println(label, s)
}

type readOnlyCLOB struct{}

func (readOnlyCLOB) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != "GET" || r.URL.Scheme != "https" || r.URL.Host != "clob.polymarket.com" {
		return nil, fmt.Errorf("operator closure denies non-CLOB-GET")
	}
	return http.DefaultTransport.RoundTrip(r)
}

type exactFillSource struct{ client *polymarket.TradingClient }

func (s exactFillSource) ListOrderFills(ctx context.Context, o domain.Order) ([]domain.Fill, error) {
	fills, err := s.client.ListOrderFills(ctx, o)
	if err != nil {
		return nil, err
	}
	if len(fills) != 1 {
		return nil, fmt.Errorf("fill set changed")
	}
	if err = manualsellclosure.ValidateFill(target, fills[0]); err != nil {
		return nil, err
	}
	return fills, nil
}
func run() error {
	apply := len(os.Args) == 2 && os.Args[1] == "--apply-authorized-partial-sell-closure"
	if len(os.Args) > 1 && !apply {
		return fmt.Errorf("unsupported argument")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 170*time.Second)
	defer cancel()
	secrets = append(secrets, os.Getenv("TRADING_EXECUTION_DATABASE_URL"), os.Getenv("POLYGON_RPC_URL"))
	grace, err := time.ParseDuration(os.Getenv("CANCEL_FILL_FINALITY_GRACE"))
	if err != nil || grace != 30*time.Second {
		return fmt.Errorf("reviewed production finality grace must remain 30s")
	}
	config, err := pgx.ParseConfig(os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("invalid database config")
	}
	config.RuntimeParams["application_name"] = "authorized-partial-sell-closure-20260910"
	config.RuntimeParams["statement_timeout"] = "20000"
	config.RuntimeParams["lock_timeout"] = "5000"
	if !apply {
		config.RuntimeParams["default_transaction_read_only"] = "on"
	}
	db := stdlib.OpenDB(*config)
	defer db.Close()
	db.SetMaxOpenConns(3)
	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{Path: os.Getenv("POLYMARKET_ACCOUNTS_FILE")})
	if err != nil {
		return err
	}
	for _, a := range accounts {
		secrets = append(secrets, a.API.Key, a.API.Secret, a.API.Passphrase, a.Relayer.Key)
	}
	provider, err := polymarket.NewStaticCredentialProvider(accounts)
	if err != nil {
		return err
	}
	reader, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{RPCURL: os.Getenv("POLYGON_RPC_URL"), RequiredConfirmations: 64, RequestTimeout: 15 * time.Second})
	if err != nil {
		return err
	}
	evidence, err := newPolygonFillFeeEvidence(reader)
	if err != nil {
		return err
	}
	client, err := polymarket.NewTradingClient(polymarket.TradingClientParams{BaseURL: "https://clob.polymarket.com", Credentials: provider, FeeEvidence: evidence, HTTPClient: &http.Client{Transport: readOnlyCLOB{}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, RequestTimeout: 15 * time.Second, RequestsPerSecond: 2, Burst: 1})
	if err != nil {
		return err
	}
	repo, err := postgres.NewOrderRepository(db)
	if err != nil {
		return err
	}
	reservations, err := postgres.NewReservationManager(postgres.ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: domain.Decimal(os.Getenv("POLYMARKET_MAX_BUY_FEE_RATE_BPS"))})
	if err != nil {
		return err
	}
	ledger, err := postgres.NewFillLedger(postgres.FillLedgerParams{DB: db})
	if err != nil {
		return err
	}
	leases, err := postgres.NewOrderRecoveryLeaseStore(db)
	if err != nil {
		return err
	}
	processor, err := fillprocessor.New(fillprocessor.Params{Orders: repo, Source: exactFillSource{client}, Ledger: ledger})
	if err != nil {
		return err
	}
	emit("mode", map[string]any{"apply": apply, "target": target, "grace": grace.String(), "at": time.Now().UTC()})
	result, err := manualsellclosure.Run(ctx, manualsellclosure.Params{Target: target, Orders: repo, Reservations: reservations, Leases: leases, Source: client, Synchronizer: processor, CheckLocal: func(ctx context.Context, o domain.Order) error {
		return postgres.CheckManualSellClosureEvidence(ctx, db, target, o)
	}, Grace: grace, Audit: emit}, apply)
	if err != nil {
		return err
	}
	emit("result", result)
	return nil
}
func main() {
	if err := run(); err != nil {
		emit("error", err.Error())
		os.Exit(1)
	}
}

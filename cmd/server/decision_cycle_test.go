package main

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/accountmaintenance"
)

type decisionRunnerTestPositionSource struct{}

func (decisionRunnerTestPositionSource) ListOpenLots(context.Context, domain.StrategyExecutionContext) ([]domain.PositionLot, error) {
	return nil, nil
}

type decisionRunnerTestOrderBookSource struct{}

func (decisionRunnerTestOrderBookSource) Capture(context.Context, time.Time, []domain.BookTarget) ([]domain.OrderBookSnapshot, error) {
	return nil, nil
}

type decisionRunnerTestExecutor struct{}

func (decisionRunnerTestExecutor) Submit(context.Context, domain.OrderIntent) (port.OrderSubmitResult, error) {
	return port.OrderSubmitResult{}, nil
}

type decisionRunnerTestAccountGate struct{}

func (decisionRunnerTestAccountGate) AcquireExecutionAccount(context.Context, string) (func(), error) {
	return func() {}, nil
}

func TestBuildDecisionRunnerWiresSnapshotRecorder(t *testing.T) {
	database, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	params := buildDecisionRunnerParams{
		cfg: config.Config{
			Execution: config.Execution{Venue: "polymarket"},
			DecisionCycle: config.DecisionCycle{
				Enabled: true, PredictionInfraBaseURL: "https://prediction.example",
				PredictionInfraToken: "prediction-token", StrategyBaseURL: "https://strategy.example",
				StrategyToken: "strategy-token", Interval: 10 * time.Minute, Timeout: time.Minute,
				OrderSubmissionEnabled: true, RequireCompleteModelCoverage: true,
				PredictionLookback: 3 * time.Hour,
				PredictionModelID:  "producer", PredictionSourceMode: domain.PredictionSourceModeDirect,
				Accounts: []config.DecisionAccount{{
					ExecutionAccountID: "account", MarketSource: domain.MarketSourcePolymarket,
					StrategyID: domain.StrategyIDMultfactorV1, Mode: config.DecisionAccountActive,
				}},
			},
		},
		database: database, positionSource: decisionRunnerTestPositionSource{},
		orderBooks: decisionRunnerTestOrderBookSource{}, executor: decisionRunnerTestExecutor{},
		accountGate: decisionRunnerTestAccountGate{}, accountIDs: []string{"account"},
	}
	runner, err := buildDecisionRunner(params)
	if err != nil || runner == nil {
		t.Fatalf("buildDecisionRunner() = %#v, error %v", runner, err)
	}
	params.accountGate = nil
	if _, err := buildDecisionRunner(params); err == nil || !strings.Contains(err.Error(), "shared execution account gate") {
		t.Fatalf("buildDecisionRunner() without shared gate error = %v", err)
	}
}

func polymarketAccounts(ids ...string) []config.DecisionAccount {
	accounts := make([]config.DecisionAccount, 0, len(ids))
	for _, id := range ids {
		accounts = append(accounts, config.DecisionAccount{
			ExecutionAccountID: id, MarketSource: domain.MarketSourcePolymarket,
			StrategyID: domain.StrategyIDMultfactorV1, Mode: config.DecisionAccountActive,
		})
	}
	return accounts
}

func TestValidateDecisionAccountsRejectsAccountOutsideWalletFile(t *testing.T) {
	cycleConfig := config.DecisionCycle{Accounts: polymarketAccounts("account-missing")}
	if err := validateDecisionAccounts(cycleConfig, []string{"account-present"}); err == nil || !strings.Contains(err.Error(), "absent from the wallet file") {
		t.Fatalf("validateDecisionAccounts() error = %v", err)
	}
}

func TestValidateDecisionAccountsMatchesOnlyPolymarketAccountsToTheWalletFile(t *testing.T) {
	cycleConfig := config.DecisionCycle{Accounts: append(polymarketAccounts("main", "wallet-1", "wallet-6", "wallet-7"),
		config.DecisionAccount{ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi,
			StrategyID: "brier_mid_edge_once", Mode: config.DecisionAccountActive})}
	if err := validateDecisionAccounts(cycleConfig, []string{"main", "wallet-1", "wallet-6", "wallet-7"}); err != nil {
		t.Fatalf("validateDecisionAccounts() error = %v, a Kalshi account has no wallet", err)
	}
	if err := validateDecisionAccounts(cycleConfig, []string{"main", "wallet-1", "wallet-6", "wallet-7", "kalshi:main"}); err == nil {
		t.Fatal("a Kalshi account in the Polymarket wallet file must be rejected")
	}
}

func TestValidateDecisionAccountsRejectsExtraRetiredWallet(t *testing.T) {
	cycleConfig := config.DecisionCycle{Accounts: polymarketAccounts("main", "wallet-1", "wallet-6", "wallet-7")}
	err := validateDecisionAccounts(
		cycleConfig,
		[]string{"main", "wallet-1", "wallet-2", "wallet-6", "wallet-7"},
	)
	if err == nil || !strings.Contains(err.Error(), `wallet file execution account "wallet-2" is not a POLYMARKET account`) {
		t.Fatalf("validateDecisionAccounts() error = %v, want extra retired-wallet rejection", err)
	}
}

type recordingAccountGate struct{ acquired []string }

func (gate *recordingAccountGate) AcquireExecutionAccount(_ context.Context, accountID string) (func(), error) {
	gate.acquired = append(gate.acquired, accountID)
	return func() {}, nil
}

func TestVenueAccountGateRoutesKalshiAccountsToTheirOwnSlot(t *testing.T) {
	polymarket := &recordingAccountGate{}
	gate, err := newVenueAccountGate(polymarket, []string{"kalshi:main"})
	if err != nil {
		t.Fatal(err)
	}
	release, err := gate.AcquireExecutionAccount(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if strings.Join(polymarket.acquired, ",") != "main" {
		t.Fatalf("Polymarket gate acquired %v", polymarket.acquired)
	}
	releaseKalshi, err := gate.AcquireExecutionAccount(context.Background(), "kalshi:main")
	if err != nil {
		t.Fatal(err)
	}
	if len(polymarket.acquired) != 1 {
		t.Fatalf("Kalshi account reached the Polymarket gate: %v", polymarket.acquired)
	}
	// The slot is exclusive until released.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := gate.AcquireExecutionAccount(ctx, "kalshi:main"); err == nil {
		t.Fatal("a held Kalshi account slot was acquired twice")
	}
	releaseKalshi()
	releaseKalshi()
	again, err := gate.AcquireExecutionAccount(context.Background(), "kalshi:main")
	if err != nil {
		t.Fatalf("released Kalshi slot was not reusable: %v", err)
	}
	again()
	if _, err := gate.AcquireExecutionAccount(context.Background(), "kalshi:wallet-7"); err == nil {
		t.Fatal("an unconfigured Kalshi account was acquired")
	}
	for name, accounts := range map[string][]string{"no prefix": {"main"}, "duplicate": {"kalshi:a", "kalshi:a"}} {
		if _, err := newVenueAccountGate(polymarket, accounts); err == nil {
			t.Fatalf("%s: newVenueAccountGate accepted %v", name, accounts)
		}
	}
	if _, err := newVenueAccountGate(nil, nil); err == nil {
		t.Fatal("newVenueAccountGate accepted a nil Polymarket gate")
	}
}

// TestBuildDecisionRunnerHoldsKalshiAccountsWhenMarketDataKeyFails covers the
// decision-cycle half of acceptance 4: a broken Kalshi market-data key no
// longer stops the runner from being built; Kalshi accounts are held in
// maintenance and Polymarket accounts are untouched.
func TestBuildDecisionRunnerHoldsKalshiAccountsWhenMarketDataKeyFails(t *testing.T) {
	database, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registry := accountmaintenance.New()
	params := buildDecisionRunnerParams{
		cfg: config.Config{
			Execution: config.Execution{Venue: "polymarket"},
			Kalshi: config.Kalshi{MarketDataEnabled: true, APIURL: "https://kalshi.example",
				APIKeyID: "key", PrivateKeyPath: "/tmp/does-not-exist/kalshi.pem"},
			DecisionCycle: config.DecisionCycle{
				Enabled: true, PredictionInfraBaseURL: "https://prediction.example",
				PredictionInfraToken: "prediction-token", StrategyBaseURL: "https://strategy.example",
				StrategyToken: "strategy-token", Interval: 10 * time.Minute, Timeout: time.Minute,
				OrderSubmissionEnabled: true, RequireCompleteModelCoverage: true, PredictionLookback: 3 * time.Hour,
				PredictionModelID: "producer", PredictionSourceMode: domain.PredictionSourceModeDirect,
				Accounts: []config.DecisionAccount{
					{ExecutionAccountID: "account", MarketSource: domain.MarketSourcePolymarket,
						StrategyID: domain.StrategyIDMultfactorV1, Mode: config.DecisionAccountActive},
					{ExecutionAccountID: "kalshi:main", MarketSource: domain.MarketSourceKalshi,
						StrategyID: domain.StrategyIDMultfactorV1, Mode: config.DecisionAccountActive},
				},
			},
		},
		database: database, positionSource: decisionRunnerTestPositionSource{},
		orderBooks: decisionRunnerTestOrderBookSource{}, executor: decisionRunnerTestExecutor{},
		accountGate: decisionRunnerTestAccountGate{}, accountIDs: []string{"account"},
		maintenance: registry, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runner, err := buildDecisionRunner(params)
	if err != nil || runner == nil {
		t.Fatalf("buildDecisionRunner() = %#v, %v, want a runner without Kalshi books", runner, err)
	}
	if reason, ok := registry.MaintenanceReason("kalshi:main"); !ok || reason != accountmaintenance.ReasonKalshiMarketDataUnavailable {
		t.Fatalf("Kalshi maintenance = %q, %v", reason, ok)
	}
	if _, ok := registry.MaintenanceReason("account"); ok {
		t.Fatal("Polymarket account entered maintenance because of Kalshi market data")
	}
}

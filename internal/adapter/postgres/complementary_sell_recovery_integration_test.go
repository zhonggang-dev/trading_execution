package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
)

type complementaryFeeSource func(context.Context, polymarket.FillFeeEvidenceRequest) (polymarket.FillFeeEvidence, error)

func (f complementaryFeeSource) ResolveFillFeeEvidence(ctx context.Context, req polymarket.FillFeeEvidenceRequest) (polymarket.FillFeeEvidence, error) {
	return f(ctx, req)
}

// Exercise the complete adapter -> fill processor -> PostgreSQL ledger ->
// issue recorder path. Two accounts share a trade ID, but each receives only
// its own 48-share component, once. No manual state/balance repair is used.
func TestComplementarySellRecoveryPostgresIntegration(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, url)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db, Now: func() time.Time { return base.Add(20 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	accounts := []polymarket.TradingAccount{}
	orders := []domain.Order{}
	makers := []map[string]any{}
	for i := 0; i < 2; i++ {
		accountID := fmt.Sprintf("paired-account-%d", i)
		signer, err := polymarket.NewEOASigner(fmt.Sprintf("%064x", i+1))
		if err != nil {
			t.Fatal(err)
		}
		accounts = append(accounts, polymarket.TradingAccount{ExecutionAccountID: accountID, FunderAddress: signer.Address(), Signer: signer,
			API: polymarket.APICredentials{Key: fmt.Sprintf("key-%d", i), Secret: base64.URLEncoding.EncodeToString([]byte("synthetic-secret")), Passphrase: "synthetic-passphrase"}})
		insertAccount(t, db, accountID, signer.Address(), "10", "10", "0")
		if _, err := db.Exec(`INSERT INTO execution_positions (execution_account_id,market_id,condition_id,token_id,total_shares,available_shares,reserved_shares,cost_basis) VALUES ($1,'market-1','condition-1','42',48,48,0,24)`, accountID); err != nil {
			t.Fatal(err)
		}
		lotID := fmt.Sprintf("paired-lot-%d", i)
		insertOpenLotFixtureNamed(t, db, accountID, "42", lotID, "48")
		order := integrationOrder(fmt.Sprintf("paired-%d", i), accountID, "42", domain.SideSell, "48", "0.042")
		order.Intent.TargetLotID = lotID
		order.Intent.Price, order.Intent.TimeInForce = "0.042", domain.TimeInForceIOC
		order.MarketValidation = &domain.MarketValidation{TickSize: "0.001"}
		orderHash := "0x" + strings.Repeat(fmt.Sprintf("%02x", i+34), 32)
		order = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, order, orderHash, base)
		unknown, event := applyIntegrationTransition(t, order, domain.OrderStatusUnknown, domain.TransitionTriggerReconciliation, base.Add(5*time.Second), "")
		if err := repository.Transition(ctx, unknown, event); err != nil {
			t.Fatal(err)
		}
		if err := reservations.MarkUncertain(ctx, unknown, "CLOB_FILL_DETAILS_UNAVAILABLE"); err != nil {
			t.Fatal(err)
		}
		orders = append(orders, unknown)
		makers = append(makers, map[string]any{"order_id": orderHash, "asset_id": "42", "maker_address": signer.Address(), "side": "SELL", "matched_amount": "48", "price": "0.042", "fee_rate_bps": ""})
		old := startReconciliationFixtureRun(t, recorder, accountID, fmt.Sprintf("paired-old-%d", i), base.Add(10*time.Second))
		for _, issue := range []domain.ReconciliationIssue{
			{IssueID: fmt.Sprintf("paired-balance-%d", i), Fingerprint: "paired-balance", Type: domain.ReconciliationIssueBalanceDrift, LocalValue: "10", RemoteValue: "12.016", Source: "EVM_ERC20_ETH_CALL"},
			{IssueID: fmt.Sprintf("paired-position-%d", i), Fingerprint: "paired-position", Type: domain.ReconciliationIssuePositionDrift, MarketID: "market-1", ConditionID: "condition-1", TokenID: "42", LocalValue: "48", RemoteValue: "0", Source: "POLYMARKET_DATA_API"},
		} {
			issue.ExecutionAccountID, issue.RunID, issue.Status, issue.Resolution, issue.ObservedAt = accountID, old.RunID, domain.ReconciliationIssueOpen, domain.ReconciliationResolutionManual, base.Add(10*time.Second)
			if err := recorder.RecordIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
		}
		if i == 1 {
			// Matching the ending balance is insufficient: this older difference
			// includes an unexplained extra unit and must remain manual.
			issue := domain.ReconciliationIssue{IssueID: "paired-unexplained", Fingerprint: "paired-unexplained", ExecutionAccountID: accountID, RunID: old.RunID, Type: domain.ReconciliationIssueBalanceDrift, Status: domain.ReconciliationIssueOpen, Resolution: domain.ReconciliationResolutionManual, LocalValue: "9", RemoteValue: "12.016", Source: "EVM_ERC20_ETH_CALL", ObservedAt: base.Add(10 * time.Second)}
			if err := recorder.RecordIssue(ctx, issue); err != nil {
				t.Fatal(err)
			}
		}
		completeReconciliationFixtureRun(t, recorder, old, domain.ReconciliationRunAttentionRequired, base.Add(11*time.Second))
	}
	transactionHash := "0x" + strings.Repeat("cd", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/data/trades":
			rows := []any{}
			if r.URL.Query().Get("asset_id") == "" {
				rows = append(rows, map[string]any{"id": "shared-paired-trade", "taker_order_id": "foreign-taker", "market": "condition-1", "asset_id": "43", "side": "SELL", "size": "100", "price": "0.958", "status": "CONFIRMED", "fee_rate_bps": "0", "trader_side": "MAKER", "maker_address": "0x" + strings.Repeat("44", 20), "maker_orders": makers, "transaction_hash": transactionHash, "match_time": base.Add(6 * time.Second).Format(time.RFC3339), "last_update": base.Add(7 * time.Second).Format(time.RFC3339)})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": rows, "next_cursor": "LTE="})
		case "/clob-markets/condition-1":
			_ = json.NewEncoder(w).Encode(map[string]any{"c": "condition-1", "t": []map[string]string{{"t": "42"}, {"t": "43"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	provider, err := polymarket.NewStaticCredentialProvider(accounts)
	if err != nil {
		t.Fatal(err)
	}
	evidenceSource := complementaryFeeSource(func(_ context.Context, req polymarket.FillFeeEvidenceRequest) (polymarket.FillFeeEvidence, error) {
		index := 0
		if req.ExecutionAccountID == accounts[1].ExecutionAccountID {
			index = 1
		}
		if req.VenueOrderID != orders[index].VenueOrderID || !strings.EqualFold(req.ExpectedMakerAddress, accounts[index].FunderAddress) || req.TokenID != "42" || req.Side != domain.SideSell || !req.Shares.Equal("48") || !req.Price.Equal("0.042") {
			return polymarket.FillFeeEvidence{}, fmt.Errorf("incorrect exact component")
		}
		return polymarket.FillFeeEvidence{Source: domain.FeeSourcePolygonV2OrderFilled, ExchangeAddress: req.ExpectedExchangeAddress, TransactionHash: req.TransactionHash, OrderHash: req.VenueOrderID, MakerAddress: req.ExpectedMakerAddress, TokenID: req.TokenID, Side: req.Side, BuilderCode: req.ExpectedBuilderCode,
			MakerAmountBaseUnits: "48000000", TakerAmountBaseUnits: "2016000", TotalFeeBaseUnits: "0", BuilderFeeKnown: true, BuilderFeeBaseUnits: "0", CollateralDecimals: 6, OutcomeTokenDecimals: 6, BlockNumber: 100, BlockHash: "0x" + strings.Repeat("ef", 32), LogIndex: uint64(index + 3), Confirmations: 64, Finalized: true}, nil
	})
	client, err := polymarket.NewTradingClient(polymarket.TradingClientParams{BaseURL: server.URL, Credentials: provider, FeeEvidence: evidenceSource, RequestsPerSecond: 1000, Burst: 100, Now: func() time.Time { return base.Add(20 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := fillprocessor.New(fillprocessor.Params{Orders: repository, Source: client, Ledger: ledger, Now: func() time.Time { return base.Add(20 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	for i, order := range orders {
		result, err := processor.SyncOrder(ctx, order.ID)
		if err != nil || result.Applied != 1 {
			t.Fatalf("recover account %d: %#v %v", i, result, err)
		}
		stored, err := repository.Get(ctx, order.ID)
		if err != nil || stored.Status != domain.OrderStatusFilled || !stored.FilledSize.Equal("48") {
			t.Fatalf("recovered order=%#v err=%v", stored, err)
		}
		assertAccount(t, db, order.Intent.ExecutionAccountID, "12.016", "12.016", "0")
		assertPosition(t, db, order.Intent.ExecutionAccountID, "42", "0", "0", "0")
		duplicate, err := processor.SyncOrder(ctx, order.ID)
		if err != nil || duplicate.Applied != 0 || duplicate.Duplicates != 1 {
			t.Fatalf("duplicate=%#v err=%v", duplicate, err)
		}
		assertAccount(t, db, order.Intent.ExecutionAccountID, "12.016", "12.016", "0")
		var open int
		if err := db.QueryRow(`SELECT count(*) FROM reconciliation_issues WHERE execution_account_id=$1 AND status='OPEN'`, order.Intent.ExecutionAccountID).Scan(&open); err != nil || open != 2+i {
			t.Fatalf("premature issue closure=%d err=%v", open, err)
		}
		clean := startReconciliationFixtureRun(t, recorder, order.Intent.ExecutionAccountID, fmt.Sprintf("paired-clean-%d", i), base.Add(30*time.Second))
		completeReconciliationFixtureRun(t, recorder, clean, domain.ReconciliationRunCompleted, base.Add(31*time.Second))
		if err := db.QueryRow(`SELECT count(*) FROM reconciliation_issues WHERE execution_account_id=$1 AND status='OPEN'`, order.Intent.ExecutionAccountID).Scan(&open); err != nil || open != i {
			t.Fatalf("stale gates after clean sweep=%d err=%v", open, err)
		}
		var fills int
		if err := db.QueryRow(`SELECT count(*) FROM execution_fills WHERE order_id=$1`, order.ID).Scan(&fills); err != nil || fills != 1 {
			t.Fatalf("fill count=%d err=%v", fills, err)
		}
	}
}

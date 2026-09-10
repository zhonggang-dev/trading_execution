package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/fillprocessor"
	"github.com/UniPat-AI/trading_execution/internal/service/manualsellclosure"
)

type closureSource struct {
	now   *time.Time
	fill  domain.Fill
	state port.VenueOrderState
	extra bool
}

func (s *closureSource) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{ID: s.fill.VenueOrderID, State: s.state, RawStatus: "CANCELED", FilledSize: s.fill.Shares, AverageFillPrice: s.fill.Price, ObservedAt: *s.now, TradeIDs: []string{s.fill.VenueFillID}}, nil
}
func (s *closureSource) ListOrderFills(context.Context, domain.Order) ([]domain.Fill, error) {
	fills := []domain.Fill{s.fill}
	if s.extra {
		fills = append(fills, s.fill)
	}
	return fills, nil
}

type closureReservationFault struct {
	*ReservationManager
	fail bool
}

func (r *closureReservationFault) Reconcile(ctx context.Context, o domain.Order) (domain.AssetReservation, error) {
	if r.fail {
		return domain.AssetReservation{}, errors.New("injected release failure")
	}
	return r.ReservationManager.Reconcile(ctx, o)
}

type closureRepositoryRace struct {
	port.OrderRepository
	raced bool
}

func (r *closureRepositoryRace) Transition(ctx context.Context, o domain.Order, e domain.OrderEvent) error {
	if !r.raced {
		r.raced = true
		fresh, err := r.OrderRepository.Get(ctx, o.ID)
		if err != nil {
			return err
		}
		fresh.Revision++
		fresh.UpdatedAt = o.UpdatedAt
		e2 := e
		e2.ToStatus = fresh.Status
		e2.Trigger = domain.TransitionTriggerReconciliation
		if err = r.OrderRepository.Transition(ctx, fresh, e2); err != nil {
			return err
		}
	}
	return r.OrderRepository.Transition(ctx, o, e)
}

func TestAuditedPartialSellClosurePostgresIntegration(t *testing.T) {
	url := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("test database not configured")
	}
	for _, scenario := range []string{"success_and_replay", "dry_run", "extra_fill", "not_cancelled", "shallow_receipt", "wrong_wallet", "wrong_local_cash", "held_lease", "revision_race", "resume_after_release_failure"} {
		t.Run(scenario, func(t *testing.T) {
			db := newIntegrationDatabase(t, url)
			ctx := context.Background()
			now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			wallet := "0x" + strings.Repeat("11", 20)
			insertAccount(t, db, "closure-account", wallet, "10", "10", "0")
			if _, err := db.Exec(`INSERT INTO execution_positions (execution_account_id,market_id,condition_id,token_id,total_shares,available_shares,reserved_shares,cost_basis) VALUES ('closure-account','market-1','condition-1','42',48,48,0,24)`); err != nil {
				t.Fatal(err)
			}
			insertOpenLotFixtureNamed(t, db, "closure-account", "42", "closure-lot", "48")
			repo, _ := NewOrderRepository(db)
			reservations, _ := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
			ledger, _ := NewFillLedger(FillLedgerParams{DB: db})
			leases, _ := NewOrderRecoveryLeaseStore(db)
			o := integrationOrder("closure", "closure-account", "42", domain.SideSell, "48", "0.038")
			o.Intent.TargetLotID = "closure-lot"
			o.Intent.Price = "0.038"
			o.Intent.TimeInForce = domain.TimeInForceIOC
			o.MarketValidation = &domain.MarketValidation{TickSize: "0.001"}
			hash := "0x" + strings.Repeat("22", 32)
			txHash := "0x" + strings.Repeat("33", 32)
			o = createAcknowledgedIntegrationOrder(t, ctx, repo, reservations, o, hash, now)
			at := now.Add(10 * time.Second)
			proof := &domain.SettlementEvidence{SchemaVersion: domain.SettlementEvidenceSchemaV1, Source: domain.FeeSourcePolygonV2OrderFilled, ChainID: 137, ExchangeAddress: "0x" + strings.Repeat("44", 20), TransactionHash: txHash, BlockNumber: 100, BlockHash: "0x" + strings.Repeat("55", 32), LogIndex: 3, Confirmations: 100, OrderHash: hash, MakerAddress: wallet, TokenID: "42", Side: domain.SideSell, MakerAmountBaseUnits: "19010000", TakerAmountBaseUnits: "722380", TotalFeeBaseUnits: "0", BuilderCode: "0x" + strings.Repeat("00", 32), BuilderFeeKnown: true, BuilderFeeBaseUnits: "0", BuilderFeeSource: domain.SettlementEvidenceZeroBuilder, CollateralDecimals: 6, OutcomeTokenDecimals: 6}
			fill := domain.Fill{Venue: "polymarket", VenueFillID: "closure-trade", OrderID: o.ID, VenueOrderID: hash, ExecutionAccountID: o.Intent.ExecutionAccountID, MarketID: "market-1", ConditionID: "condition-1", TokenID: "42", Side: domain.SideSell, LiquidityRole: domain.LiquidityRoleTaker, Status: domain.FillStatusConfirmed, Shares: "19.01", Price: "0.038", PriceTickSize: "0.001", GrossNotional: "0.72238", FeeRateBPS: "0", PlatformFeeRate: "0", FeeExponent: "0", PlatformFee: "0", BuilderFeeRateBPS: "0", BuilderFee: "0", TotalFee: "0", TransactionHash: txHash, MatchedAt: at, ObservedAt: at, ConfirmedAt: &at, FeeSource: domain.FeeSourcePolygonV2OrderFilled, SettlementEvidence: proof}
			source := &closureSource{now: &now, fill: fill, state: port.VenueOrderCancelled}
			processor, _ := fillprocessor.New(fillprocessor.Params{Orders: repo, Source: source, Ledger: ledger})
			o, unknownEvent := applyIntegrationTransition(t, o, domain.OrderStatusUnknown, domain.TransitionTriggerReconciliation, at, "")
			if err := repo.Transition(ctx, o, unknownEvent); err != nil {
				t.Fatal(err)
			}
			o, reconcilingEvent := applyIntegrationTransition(t, o, domain.OrderStatusReconciling, domain.TransitionTriggerReconciliation, at, "")
			if err := repo.Transition(ctx, o, reconcilingEvent); err != nil {
				t.Fatal(err)
			}
			o, event := applyIntegrationTransition(t, o, domain.OrderStatusManualReview, domain.TransitionTriggerReconciliation, at.Add(time.Second), "")
			o.FailureCode = "RECONCILE_FAILED"
			o.FailureReason = "CLOB size_matched exceeds original_size"
			if err := repo.Transition(ctx, o, event); err != nil {
				t.Fatal(err)
			}
			if err := reservations.MarkUncertain(ctx, o, o.FailureReason); err != nil {
				t.Fatal(err)
			}
			source.fill.ObservedAt = at.Add(2 * time.Second)
			if _, err := processor.SyncOrder(ctx, o.ID); err != nil {
				t.Fatal(err)
			}
			o, _ = repo.Get(ctx, o.ID)
			now = at.Add(2 * time.Second)
			target := manualsellclosure.Target{Account: o.Intent.ExecutionAccountID, Order: o.ID, VenueOrder: hash, Market: "market-1", Condition: "condition-1", Token: "42", Lot: "closure-lot", Wallet: wallet, Trade: fill.VenueFillID, Transaction: txHash, Block: 100, Log: 3, Requested: "48", Filled: "19.01", Price: "0.038", Gross: "0.72238", Remaining: "28.99"}
			p := manualsellclosure.Params{Target: target, Orders: repo, Reservations: reservations, Leases: leases, Source: source, Synchronizer: processor, Grace: 30 * time.Second, Now: func() time.Time { return now }, Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, CheckLocal: func(ctx context.Context, o domain.Order) error {
				return CheckManualSellClosureEvidence(ctx, db, target, o)
			}}
			switch scenario {
			case "extra_fill":
				source.extra = true
			case "not_cancelled":
				source.state = port.VenueOrderPartiallyFilled
			case "shallow_receipt":
				source.fill.SettlementEvidence.Confirmations = 63
			case "wrong_wallet":
				source.fill.SettlementEvidence.MakerAddress = "0x" + strings.Repeat("99", 20)
			case "wrong_local_cash":
				if _, err := db.Exec(`UPDATE execution_account_events SET total_balance_delta=9 WHERE order_id=$1`, o.ID); err != nil {
					t.Fatal(err)
				}
			case "held_lease":
				if _, err := leases.AcquireOrderRecoveryLease(ctx, domain.OrderRecoveryLeaseRequest{OrderID: o.ID, ExecutionAccountID: o.Intent.ExecutionAccountID, OrderRevision: o.Revision, Holder: "another-worker", Now: now, TTL: 2 * time.Minute}); err != nil {
					t.Fatal(err)
				}
			case "revision_race":
				p.Orders = &closureRepositoryRace{OrderRepository: repo}
			case "resume_after_release_failure":
				p.Reservations = &closureReservationFault{ReservationManager: reservations, fail: true}
			}
			result, err := manualsellclosure.Run(ctx, p, scenario != "dry_run")
			success := scenario == "success_and_replay"
			if success || scenario == "dry_run" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("negative case unexpectedly released assets")
			}
			if scenario == "resume_after_release_failure" {
				current, _ := repo.Get(ctx, o.ID)
				r, _ := reservations.GetOrderReservation(ctx, o.ID)
				if current.Status != domain.OrderStatusCancelled || !r.RemainingReservedShares.Equal("28.99") {
					t.Fatal("failed release must preserve reservation with resumable audit state")
				}
				p.Reservations = reservations
				result, err = manualsellclosure.Run(ctx, p, true)
				if err != nil {
					t.Fatal(err)
				}
				success = true
			}
			if success {
				if !result.Applied || result.Order.Status != domain.OrderStatusCancelled || result.Reservation.Status != domain.ReservationStatusReleased || !result.Reservation.RemainingReservedShares.Equal("0") {
					t.Fatalf("incomplete result: %#v", result)
				}
				beforeRevision := result.Order.Revision
				reserveRevision := result.Reservation.Revision
				replay, err := manualsellclosure.Run(ctx, p, true)
				if err != nil || !replay.AlreadyClosed || replay.Order.Revision != beforeRevision || replay.Reservation.Revision != reserveRevision {
					t.Fatalf("non-idempotent replay: %#v %v", replay, err)
				}
				assertPosition(t, db, target.Account, target.Token, "28.99", "28.99", "0")
				var count int
				if err := db.QueryRow(`SELECT count(*) FROM execution_order_events WHERE order_id=$1 AND trigger='OPERATOR'`, o.ID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("operator audit count=%d err=%v", count, err)
				}
			} else {
				assertPosition(t, db, target.Account, target.Token, "28.99", "0", "28.99")
			}
			assertAccount(t, db, target.Account, "10.72238", "10.72238", "0")
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM execution_account_events WHERE order_id=$1`, o.ID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("duplicated cash event: %d %v", count, err)
			}
		})
	}
}

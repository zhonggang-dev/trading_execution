package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/evidencefillrepair"
)

// TestEvidenceFillRepairRecordsStuckBuyThroughTheLedger 验证补记命令构造的成交能走正常账本：订单变 FILLED、预占结清、现金与仓位入账，且重复执行是幂等的。
func TestEvidenceFillRepairRecordsStuckBuyThroughTheLedger(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	wallet := "0x" + strings.Repeat("dd", 20)
	insertAccount(t, db, "account-fee-repair", wallet, "110", "110", "0")
	repository, err := NewOrderRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := NewReservationManager(ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "1000"})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFillLedger(FillLedgerParams{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tokenID := "21826735481216108293779753600846510014290875703486929222291367558516215723615"
	orderHash := "0x" + strings.Repeat("19", 32)
	order := integrationOrder("fee-repair", "account-fee-repair", tokenID, domain.SideBuy, "56", "0.22")
	order.Intent.TimeInForce = domain.TimeInForceIOC
	order.MarketValidation = &domain.MarketValidation{TickSize: "0.01"}
	order = createAcknowledgedIntegrationOrder(t, ctx, repository, reservations, order, orderHash, now)
	unknown, event := applyIntegrationTransition(t, order, domain.OrderStatusUnknown, domain.TransitionTriggerReconciliation, now.Add(5*time.Second), "")
	if err := repository.Transition(ctx, unknown, event); err != nil {
		t.Fatal(err)
	}
	if err := reservations.MarkUncertain(ctx, unknown, "CLOB_ORDER_NOT_FOUND"); err != nil {
		t.Fatal(err)
	}
	reservation, err := reservations.GetOrderReservation(ctx, unknown.ID)
	if err != nil {
		t.Fatal(err)
	}

	target := evidencefillrepair.Target{
		Account: "account-fee-repair", OrderID: unknown.ID, VenueOrderID: orderHash, VenueFillID: "ebb37fc1-9332-4e40-9f5b-f908237399a7",
		Wallet: wallet, TokenID: tokenID, TransactionHash: "0x" + strings.Repeat("db", 32), BlockNumber: 94890899, LogIndex: 769,
		MatchedAt: now.Add(10 * time.Second), Shares: "56", GrossNotional: "9.9315", TotalFee: "0.020425",
		DisplayPrice: "0.1773482143", EffectiveFeeRate: "0.0025", FeeExponent: "1",
		ExpectedReservedBalance: reservation.RemainingReservedBalance.String(),
	}
	if err := evidencefillrepair.ValidateOrder(target, unknown, reservation); err != nil {
		t.Fatal(err)
	}
	evidence := evmrpc.OrderFilledEvidence{
		TransactionHash: target.TransactionHash, LogIndex: 769, BlockNumber: 94890899, BlockHash: "0x" + strings.Repeat("6d", 32),
		ExchangeAddress: "0xe111180000d2663c0091e4f400237545b87b996b", OrderHash: orderHash, Maker: wallet,
		Taker: "0xe111180000d2663c0091e4f400237545b87b996b", Side: evmrpc.OrderSideBuy, TokenID: tokenID,
		MakerAmountBaseUnits: "9931500", TakerAmountBaseUnits: "56000000", FeeBaseUnits: "20425",
		Builder: "0x" + strings.Repeat("00", 32), Confirmations: 4000,
	}
	fill, err := evidencefillrepair.BuildFill(target, unknown, evidence, "0.01", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	application, err := ledger.Record(ctx, unknown, fill)
	if err != nil {
		t.Fatalf("ledger rejected the repair fill: %v", err)
	}
	if !application.Applied || application.Order.Status != domain.OrderStatusFilled || !application.Order.FilledSize.Equal("56") ||
		!application.Order.FilledNotional.Equal("9.9315") || !application.Order.TotalFees.Equal("0.020425") {
		t.Fatalf("application = %#v", application)
	}
	assertAccount(t, db, "account-fee-repair", "100.048075", "100.048075", "0")
	assertPosition(t, db, "account-fee-repair", tokenID, "56", "56", "0")
	var status, remaining string
	if err := db.QueryRow(`SELECT status, remaining_reserved_balance::text FROM asset_reservations WHERE order_id=$1`, unknown.ID).Scan(&status, &remaining); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.ReservationStatusSettled) || !sameNumeric(remaining, "0") {
		t.Fatalf("reservation after repair = %s/%s", status, remaining)
	}
	duplicate, err := ledger.Record(ctx, application.Order, fill)
	if err != nil || !duplicate.Duplicate || duplicate.Applied {
		t.Fatalf("repeat repair = %#v err=%v", duplicate, err)
	}
	assertAccount(t, db, "account-fee-repair", "100.048075", "100.048075", "0")
}

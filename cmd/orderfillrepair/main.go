// orderfillrepair is an operator-only tool that records ONE reviewed fill from
// a finalized Polygon OrderFilled receipt when the automatic synchronizer cannot
// (see internal/service/evidencefillrepair). It is read-only unless --apply is
// given together with the exact reviewed manifest SHA-256.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/evidencefillrepair"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	path := flag.String("manifest", "", "reviewed target JSON")
	apply := flag.Bool("apply", false, "record the fill (requires actor, reason and the reviewed manifest SHA-256)")
	confirm := flag.String("confirm-sha256", "", "target fingerprint printed by the dry run")
	actor := flag.String("actor", "", "accountable operator identity")
	reason := flag.String("reason", "", "approved reason for the repair")
	flag.Parse()
	data, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var target evidencefillrepair.Target
	if err := json.Unmarshal(data, &target); err != nil {
		return err
	}
	fingerprint := target.Fingerprint()
	if *apply && (*confirm != fingerprint || *actor == "" || *reason == "") {
		return fmt.Errorf("--apply requires actor, reason and --confirm-sha256 equal to the dry-run fingerprint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	db, err := sql.Open("pgx", os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.Close()
	orders, err := postgresadapter.NewOrderRepository(db)
	if err != nil {
		return err
	}
	reservations, err := postgresadapter.NewReservationManager(postgresadapter.ReservationManagerParams{DB: db, MaxBuyFeeRateBPS: "0"})
	if err != nil {
		return err
	}
	var wallet string
	if err := db.QueryRowContext(ctx, `SELECT lower(wallet_address) FROM execution_accounts WHERE execution_account_id=$1`, target.Account).Scan(&wallet); err != nil {
		return fmt.Errorf("read account wallet: %w", err)
	}
	if wallet != target.Wallet {
		return fmt.Errorf("manifest wallet %s differs from the account wallet %s", target.Wallet, wallet)
	}
	order, err := orders.Get(ctx, target.OrderID)
	if err != nil {
		return err
	}
	reservation, err := reservations.GetOrderReservation(ctx, order.ID)
	if err != nil {
		return err
	}
	if err := evidencefillrepair.ValidateOrder(target, order, reservation); err != nil {
		return err
	}
	reader, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{
		RPCURL: os.Getenv("POLYGON_RPC_URL"), RequiredConfirmations: evidencefillrepair.MinConfirmations,
	})
	if err != nil {
		return err
	}
	evidence, err := reader.Read(ctx, target.EvidenceRequest())
	if err != nil {
		return fmt.Errorf("read receipt: %w", err)
	}
	if order.MarketValidation == nil || order.MarketValidation.TickSize.IsEmpty() {
		return fmt.Errorf("order has no persisted market tick size")
	}
	fill, err := evidencefillrepair.BuildFill(target, order, evidence, order.MarketValidation.TickSize, time.Now())
	if err != nil {
		return err
	}
	report := map[string]any{
		"target_sha256": fingerprint, "applied": false, "order_before": order.Status, "reservation_before": reservation.Status,
		"fill": fill, "receipt_confirmations": evidence.Confirmations,
	}
	if *apply {
		ledger, err := postgresadapter.NewFillLedger(postgresadapter.FillLedgerParams{DB: db})
		if err != nil {
			return err
		}
		var application domain.FillApplication
		for attempt := 0; ; attempt++ {
			application, err = ledger.Record(ctx, order, fill)
			if err == nil {
				break
			}
			if !errors.Is(err, port.ErrOrderRevisionConflict) || attempt >= 3 {
				return fmt.Errorf("record fill: %w", err)
			}
			// The service advanced the order concurrently; re-validate and retry.
			if order, err = orders.Get(ctx, order.ID); err != nil {
				return err
			}
			if reservation, err = reservations.GetOrderReservation(ctx, order.ID); err != nil {
				return err
			}
			if err = evidencefillrepair.ValidateOrder(target, order, reservation); err != nil {
				return err
			}
		}
		after, err := orders.Get(ctx, order.ID)
		if err != nil {
			return err
		}
		reservationAfter, err := reservations.GetOrderReservation(ctx, order.ID)
		if err != nil {
			return err
		}
		report["applied"], report["actor"], report["reason"] = application.Applied, *actor, *reason
		report["duplicate"], report["order_after"], report["reservation_after"] = application.Duplicate, after.Status, reservationAfter
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

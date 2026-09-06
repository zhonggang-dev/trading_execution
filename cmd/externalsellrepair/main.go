// externalsellrepair is an operator-only, read-only evidence collector by default.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type trade struct {
	VenueTradeID    string                            `json:"venue_trade_id"`
	ConditionID     string                            `json:"condition_id"`
	MatchedAt       time.Time                         `json:"matched_at"`
	Request         evmrpc.OrderFilledEvidenceRequest `json:"request"`
	Evidence        evmrpc.OrderFilledEvidence        `json:"evidence"`
	RemainingShares string                            `json:"remaining_shares"`
}
type manifest struct {
	Account       string    `json:"account"`
	Wallet        string    `json:"wallet"`
	Trades        []trade   `json:"trades"`
	ObservedAt    time.Time `json:"observed_at"`
	ObservedBlock uint64    `json:"observed_block"`
	Cash          string    `json:"cash"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("manifest", "", "reviewed exact external trade identities")
	apply := flag.Bool("apply", false, "commit the audited repair (requires paused account and approved manifest hash)")
	dryRun := flag.Bool("database-dry-run", false, "exercise all database changes and constraints, then ROLLBACK")
	confirm := flag.String("confirm-sha256", "", "exact SHA256 of the reviewed manifest bytes, required for --apply")
	actor := flag.String("actor", "", "accountable operator identity")
	reason := flag.String("reason", "", "approved reason for the repair")
	flag.Parse()
	data, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	batch := fmt.Sprintf("%x", sha256.Sum256(data))
	if *apply && (*dryRun || *confirm != batch || *actor == "" || *reason == "") {
		return fmt.Errorf("--apply requires actor, reason and the exact reviewed manifest SHA256; no --database-dry-run")
	}
	var m manifest
	if err = json.Unmarshal(data, &m); err != nil {
		return err
	}
	if m.Account == "" || m.Wallet == "" || len(m.Trades) == 0 || len(m.Trades) > 20 {
		return fmt.Errorf("invalid manifest")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	r, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{RPCURL: os.Getenv("POLYGON_RPC_URL"), RequiredConfirmations: 128})
	if err != nil {
		return err
	}
	for i := range m.Trades {
		t := &m.Trades[i]
		t.Request.Maker = m.Wallet
		t.Request.Side = evmrpc.OrderSideSell
		t.Evidence, err = r.ReadExternalSell(ctx, t.Request, polymarket.PUSDAddress)
		if err != nil {
			return fmt.Errorf("trade %s: %w", t.VenueTradeID, err)
		}
	}
	m.ObservedBlock, err = r.ObservedBlock(ctx)
	if err != nil {
		return err
	}
	block := fmt.Sprintf("0x%x", m.ObservedBlock)
	m.Cash, err = r.ReadTokenBalance(ctx, polymarket.PUSDAddress, m.Wallet, "", block)
	if err != nil {
		return err
	}
	for i := range m.Trades {
		m.Trades[i].RemainingShares, err = r.ReadTokenBalance(ctx, evmrpc.PolymarketConditionalTokensAddress, m.Wallet, m.Trades[i].Request.TokenID, block)
		if err != nil {
			return err
		}
	}
	m.ObservedAt = time.Now().UTC()
	if *apply || *dryRun {
		payload, err := json.Marshal(m)
		if err != nil {
			return err
		}
		db, err := sql.Open("pgx", os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
		if err != nil {
			return err
		}
		defer db.Close()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s'`); err != nil {
			return err
		}
		if *dryRun {
			// This pause is part of the transaction that is unconditionally rolled
			// back. A real apply never changes the account's risk control.
			if _, err = tx.ExecContext(ctx, `UPDATE execution_risk_controls SET paused=true,version=version+1,updated_at=clock_timestamp() WHERE execution_account_id=$1 AND control_scope='ACCOUNT' AND control_key=''`, m.Account); err != nil {
				return err
			}
			if *actor == "" {
				*actor = "dry-run"
			}
			if *reason == "" {
				*reason = "non-committing accounting validation"
			}
		}
		var result json.RawMessage
		err = tx.QueryRowContext(ctx, `SELECT apply_managed_external_sells($1::jsonb,$2,$3,$4)`, payload, batch, *actor, *reason).Scan(&result)
		if err != nil {
			return err
		}
		if *apply {
			if err = tx.Commit(); err != nil {
				return err
			}
		} else {
			if err = tx.Rollback(); err != nil {
				return err
			}
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"applied": *apply, "manifest_sha256": batch, "result": result})
	}
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	return e.Encode(m)
}

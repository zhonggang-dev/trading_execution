package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// Running Kalshi cash sync and read-only position check (migration 0045).
//
// Kalshi moves cash on its own (deposits, withdrawals), so the local ledger is
// brought back to the exchange balance while the process runs, not only at
// startup. Principle: rather sync late than book wrong. Cash is booked only in
// a quiet window that is proven inside the locked transaction (no reservation,
// no reserved cash, no non-terminal order, the account version unchanged since
// before the exchange balance was read, no settlement in progress, no deferred
// netting, positions verified equal to the exchange), and only when two
// consecutive sweeps observed exactly the same difference. Every other case is
// recorded with its reason in kalshi_ledger_checks and left for the next sweep.

// KalshiLedgerSyncTablesPresent reports whether migration 0045 has been
// applied. A binary that finds it missing keeps running with the drift-only
// behaviour instead of refusing to start.
func KalshiLedgerSyncTablesPresent(ctx context.Context, database *sql.DB) (bool, error) {
	if database == nil {
		return false, nil
	}
	var present bool
	if err := database.QueryRowContext(ctx, `
		SELECT to_regclass('kalshi_ledger_checks') IS NOT NULL
		   AND to_regclass('kalshi_balance_syncs') IS NOT NULL
		   AND to_regclass('kalshi_position_drift') IS NOT NULL
		   AND to_regclass('kalshi_settlement_reviews') IS NOT NULL`).Scan(&present); err != nil {
		return false, fmt.Errorf("inspect Kalshi ledger sync tables: %w", err)
	}
	return present, nil
}

// LedgerSnapshot reads the account version and whether it is quiet, without
// locking, before the exchange is asked anything.
func (store *KalshiSettlementStore) LedgerSnapshot(ctx context.Context, executionAccountID string) (domain.KalshiLedgerSnapshot, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if err := store.requireManaged(executionAccountID); err != nil {
		return domain.KalshiLedgerSnapshot{}, err
	}
	var snapshot domain.KalshiLedgerSnapshot
	var busy bool
	err := store.db.QueryRowContext(ctx, `
		SELECT account.version,
		       account.reserved_balance > 0
		       OR EXISTS (
				SELECT 1 FROM asset_reservations
				WHERE execution_account_id=account.execution_account_id
				  AND status IN ('ACTIVE','RECONCILIATION_REQUIRED'))
		       OR EXISTS (
				SELECT 1 FROM execution_orders
				WHERE execution_account_id=account.execution_account_id
				  AND status NOT IN ('CANCELLED','REJECTED','FILLED'))
		FROM execution_accounts account
		WHERE account.execution_account_id=$1`, executionAccountID).Scan(&snapshot.AccountVersion, &busy)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.KalshiLedgerSnapshot{}, fmt.Errorf("Kalshi execution account %s has not been initialized", executionAccountID)
	}
	if err != nil {
		return domain.KalshiLedgerSnapshot{}, fmt.Errorf("read Kalshi ledger snapshot: %w", err)
	}
	snapshot.Quiet = !busy
	return snapshot, nil
}

// kalshiLedgerCheckState is the account's kalshi_ledger_checks row.
type kalshiLedgerCheckState struct {
	balanceOutcome   string
	skipReason       string
	consecutiveSkips int64
	totalSkips       int64
	skippingSince    sql.NullTime
	pendingLocal     sql.NullString
	pendingExchange  sql.NullString
	pendingVersion   sql.NullInt64
	pendingFirstSeen sql.NullTime
	lastSyncedAt     sql.NullTime
	lastSyncEventID  string
	openDrifts       int
	// positionsOutcome empty leaves the position columns untouched.
	positionsOutcome string
	positionsReason  string
}

func (state *kalshiLedgerCheckState) clearPending() {
	state.pendingLocal, state.pendingExchange = sql.NullString{}, sql.NullString{}
	state.pendingVersion, state.pendingFirstSeen = sql.NullInt64{}, sql.NullTime{}
}

func (state *kalshiLedgerCheckState) skip(reason string, at time.Time) {
	if state.consecutiveSkips == 0 || !state.skippingSince.Valid {
		state.skippingSince = sql.NullTime{Time: at, Valid: true}
	}
	state.balanceOutcome, state.skipReason = domain.KalshiBalanceSkipped, reason
	state.consecutiveSkips++
	state.totalSkips++
	// Two observations count only when nothing came between them.
	state.clearPending()
}

func (state *kalshiLedgerCheckState) settle(outcome string) {
	state.balanceOutcome, state.skipReason = outcome, ""
	state.consecutiveSkips, state.skippingSince = 0, sql.NullTime{}
}

// SyncKalshiLedger runs one locked check of the account against what the sweep
// observed on the exchange. See the file comment for when cash is booked.
func (store *KalshiSettlementStore) SyncKalshiLedger(
	ctx context.Context, executionAccountID string, observation domain.KalshiLedgerObservation,
) (domain.KalshiLedgerSyncResult, error) {
	executionAccountID = strings.TrimSpace(executionAccountID)
	if err := store.requireManaged(executionAccountID); err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	if observation.ExchangeBalance.IsEmpty() || observation.ObservedAt.IsZero() {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("Kalshi ledger sync needs the exchange balance and a time")
	}
	if sign, err := observation.ExchangeBalance.Sign(); err != nil || sign < 0 {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("Kalshi cash balance is invalid")
	}
	if sign, err := observation.Threshold.Sign(); err != nil || sign <= 0 {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("Kalshi balance drift threshold must be positive")
	}
	at := observation.ObservedAt.UTC()
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("begin Kalshi ledger sync: %w", err)
	}
	defer tx.Rollback()
	// The startup sync's advisory lock, then the row lock Reserve, FillLedger
	// and settlement take: a reservation waits for this commit and then sees
	// the synced cash; one committed before it changed the version.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "kalshi-account-sync:"+executionAccountID); err != nil {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("lock Kalshi account sync: %w", err)
	}
	if err := lockExecutionAccount(ctx, tx, executionAccountID); err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	var local, reserved domain.Decimal
	var version int64
	if err := tx.QueryRowContext(ctx, `
		SELECT total_balance::text, reserved_balance::text, version
		FROM execution_accounts WHERE execution_account_id=$1`, executionAccountID).Scan(&local, &reserved, &version); err != nil {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("read Kalshi execution account: %w", err)
	}
	state, err := loadKalshiLedgerCheckState(ctx, tx, executionAccountID)
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	result := domain.KalshiLedgerSyncResult{
		LocalBalance: local, ExchangeBalance: observation.ExchangeBalance,
		PreviousSkipReason: state.skipReason, PreviousOpenPositionDrifts: state.openDrifts,
	}
	notCompared := func(reason string) {
		if !observation.PositionCheckEnabled {
			return
		}
		if observation.PositionsError != "" {
			state.positionsOutcome, state.positionsReason = domain.KalshiPositionsError, truncateKalshiReason("READ_FAILED: "+observation.PositionsError)
			return
		}
		state.positionsOutcome, state.positionsReason = domain.KalshiPositionsSkipped, reason
	}
	finish := func() (domain.KalshiLedgerSyncResult, error) {
		if state.openDrifts, err = countOpenKalshiPositionDrifts(ctx, tx, executionAccountID); err != nil {
			return domain.KalshiLedgerSyncResult{}, err
		}
		if err := writeKalshiLedgerCheckState(ctx, tx, executionAccountID, state, at); err != nil {
			return domain.KalshiLedgerSyncResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return domain.KalshiLedgerSyncResult{}, fmt.Errorf("commit Kalshi ledger sync: %w", err)
		}
		result.BalanceOutcome, result.SkipReason = state.balanceOutcome, state.skipReason
		result.ConsecutiveSkips, result.TotalSkips = state.consecutiveSkips, state.totalSkips
		result.PositionsOutcome, result.PositionsSkipReason = state.positionsOutcome, state.positionsReason
		result.OpenPositionDrifts = state.openDrifts
		return result, nil
	}

	// Conditions 1-4: the two sides may legitimately differ right now, so
	// neither the balance drift nor the positions are compared.
	reason, err := kalshiLedgerBusyReason(ctx, tx, executionAccountID, reserved, version, observation)
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	if reason != "" {
		notCompared(reason)
		state.skip(reason, at)
		return finish()
	}

	holdings, err := loadKalshiNetHoldings(ctx, tx, executionAccountID)
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	bothSides := false
	for _, holding := range holdings {
		if holding.yes.Sign() > 0 && holding.no.Sign() > 0 {
			bothSides = true
		}
	}
	mismatches := 0
	if observation.PositionCheckEnabled {
		if observation.Positions != nil {
			if mismatches, err = compareKalshiPositions(ctx, tx, executionAccountID, holdings, observation.Positions, at); err != nil {
				return domain.KalshiLedgerSyncResult{}, err
			}
			state.positionsOutcome, state.positionsReason = domain.KalshiPositionsMatched, ""
			if mismatches > 0 {
				state.positionsOutcome = domain.KalshiPositionsDrifted
			}
		} else {
			notCompared("NOT_READ")
		}
	}

	difference, err := numeric(ctx, tx, `SELECT ($1::numeric-$2::numeric)::text`, observation.ExchangeBalance.String(), local.String())
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	result.Difference = difference
	if err := tx.QueryRowContext(ctx, `SELECT ABS($1::numeric) > $2::numeric`, difference.String(), observation.Threshold.String()).Scan(&result.Drifted); err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	if result.Drifted {
		err = recordKalshiBalanceDrift(ctx, tx, executionAccountID, local, observation.ExchangeBalance, difference, at)
	} else {
		err = resolveKalshiBalanceDrift(ctx, tx, executionAccountID, local, observation.ExchangeBalance, difference, at)
	}
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("record Kalshi balance drift: %w", err)
	}

	// Conditions 5-6: the difference is real but cannot be told apart from an
	// unbooked netting payout or out-of-band trading, so it is not booked.
	switch {
	case bothSides:
		reason = domain.KalshiSyncSkipNettingPending
	case observation.PositionCheckEnabled && observation.Positions == nil:
		reason = domain.KalshiSyncSkipPositionsUnverified
	case observation.PositionCheckEnabled && mismatches > 0:
		reason = domain.KalshiSyncSkipPositionMismatch
	}
	if reason != "" {
		state.skip(reason, at)
		return finish()
	}
	if !result.Drifted {
		state.clearPending()
		state.settle(domain.KalshiBalanceInSync)
		return finish()
	}
	confirmed, err := state.confirms(local, observation.ExchangeBalance, version)
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	if !confirmed {
		state.pendingLocal = sql.NullString{String: local.String(), Valid: true}
		state.pendingExchange = sql.NullString{String: observation.ExchangeBalance.String(), Valid: true}
		state.pendingVersion = sql.NullInt64{Int64: version, Valid: true}
		state.pendingFirstSeen = sql.NullTime{Time: at, Valid: true}
		state.settle(domain.KalshiBalancePendingConfirmation)
		return finish()
	}
	firstSeen := state.pendingFirstSeen.Time.UTC()
	eventID, delta, err := bookKalshiBalanceSync(ctx, tx, executionAccountID, observation.ExchangeBalance)
	if err != nil {
		return domain.KalshiLedgerSyncResult{}, err
	}
	if eventID == "" {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("Kalshi balance sync recorded no event for a %s difference", difference)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO kalshi_balance_syncs (
			account_event_id, execution_account_id, local_balance_before, exchange_balance, difference,
			account_version_before, first_observed_at, confirmed_at, observations
		) VALUES ($1,$2,$3::numeric,$4::numeric,$5::numeric,$6,$7,$8,2)`,
		eventID, executionAccountID, local.String(), observation.ExchangeBalance.String(), delta.String(),
		observation.AccountVersionBefore, firstSeen, at); err != nil {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("record Kalshi balance sync audit: %w", err)
	}
	if err := resolveKalshiBalanceDrift(ctx, tx, executionAccountID, observation.ExchangeBalance, observation.ExchangeBalance, "0", at); err != nil {
		return domain.KalshiLedgerSyncResult{}, fmt.Errorf("resolve Kalshi balance drift: %w", err)
	}
	state.clearPending()
	state.settle(domain.KalshiBalanceSynced)
	state.lastSyncedAt, state.lastSyncEventID = sql.NullTime{Time: at, Valid: true}, eventID
	result.SyncEventID, result.FirstObservedAt = eventID, firstSeen
	return finish()
}

// confirms reports whether the waiting observation is exactly this one: same
// local cash, same exchange cash, same account version.
func (state *kalshiLedgerCheckState) confirms(local, exchange domain.Decimal, version int64) (bool, error) {
	if !state.pendingLocal.Valid || !state.pendingVersion.Valid || state.pendingVersion.Int64 != version {
		return false, nil
	}
	sameLocal, err := domain.Decimal(state.pendingLocal.String).Compare(local)
	if err != nil {
		return false, err
	}
	sameExchange, err := domain.Decimal(state.pendingExchange.String).Compare(exchange)
	if err != nil {
		return false, err
	}
	return sameLocal == 0 && sameExchange == 0, nil
}

// kalshiLedgerBusyReason checks conditions 1-4 under the account lock.
func kalshiLedgerBusyReason(
	ctx context.Context, tx *sql.Tx, accountID string, reserved domain.Decimal, version int64,
	observation domain.KalshiLedgerObservation,
) (string, error) {
	var reservations, orders, settlementPending bool
	if err := tx.QueryRowContext(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM asset_reservations
			        WHERE execution_account_id=$1 AND status IN ('ACTIVE','RECONCILIATION_REQUIRED')),
			EXISTS (SELECT 1 FROM execution_orders
			        WHERE execution_account_id=$1 AND status NOT IN ('CANCELLED','REJECTED','FILLED')),
			EXISTS (SELECT 1 FROM execution_positions position
			        JOIN kalshi_settlements settlement
			          ON settlement.execution_account_id=position.execution_account_id
			         AND position.token_id IN ('kalshi:'||settlement.ticker||':YES','kalshi:'||settlement.ticker||':NO')
			        WHERE position.execution_account_id=$1
			          AND position.lifecycle_status='OPEN' AND position.total_shares > 0
			          AND settlement.status <> 'APPLIED')`,
		accountID).Scan(&reservations, &orders, &settlementPending); err != nil {
		return "", fmt.Errorf("inspect Kalshi ledger activity: %w", err)
	}
	reservedSign, err := reserved.Sign()
	if err != nil {
		return "", fmt.Errorf("local Kalshi reserved balance is invalid")
	}
	switch {
	case reservations || reservedSign > 0:
		return domain.KalshiSyncSkipReservationActive, nil
	case orders:
		return domain.KalshiSyncSkipOrderInFlight, nil
	case version != observation.AccountVersionBefore:
		return domain.KalshiSyncSkipLedgerChanged, nil
	case settlementPending || observation.SettlementActivity || observation.SettlementReadFailed:
		return domain.KalshiSyncSkipSettlementPending, nil
	}
	return "", nil
}

// kalshiNetHolding is the account's open shares of one market.
type kalshiNetHolding struct {
	yes, no             *big.Rat
	yesShares, noShares string
}

// loadKalshiNetHoldings sums open shares per market, leaving out markets with
// any settlement row (a MANUAL_REVIEW market is tracked by that row).
func loadKalshiNetHoldings(ctx context.Context, tx *sql.Tx, accountID string) (map[string]*kalshiNetHolding, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT SUBSTRING(position.token_id FROM '^kalshi:(.+):(?:YES|NO)$'),
		       SUBSTRING(position.token_id FROM ':(YES|NO)$'), position.total_shares::text
		FROM execution_positions position
		WHERE position.execution_account_id=$1
		  AND position.lifecycle_status='OPEN' AND position.total_shares > 0
		  AND position.token_id ~ '^kalshi:.+:(YES|NO)$'
		  AND NOT EXISTS (
			SELECT 1 FROM kalshi_settlements settlement
			WHERE settlement.execution_account_id=position.execution_account_id
			  AND position.token_id IN ('kalshi:'||settlement.ticker||':YES','kalshi:'||settlement.ticker||':NO'))`,
		accountID)
	if err != nil {
		return nil, fmt.Errorf("list open Kalshi positions: %w", err)
	}
	defer rows.Close()
	holdings := make(map[string]*kalshiNetHolding)
	for rows.Next() {
		var ticker, side, shares string
		if err := rows.Scan(&ticker, &side, &shares); err != nil {
			return nil, err
		}
		value, ok := new(big.Rat).SetString(shares)
		if !ok {
			return nil, fmt.Errorf("Kalshi position %s shares %q are invalid", ticker, shares)
		}
		holding := holdings[ticker]
		if holding == nil {
			holding = &kalshiNetHolding{yes: new(big.Rat), no: new(big.Rat), yesShares: "0", noShares: "0"}
			holdings[ticker] = holding
		}
		if side == "YES" {
			holding.yes, holding.yesShares = value, shares
		} else {
			holding.no, holding.noShares = value, shares
		}
	}
	return holdings, rows.Err()
}

// compareKalshiPositions compares local net shares (YES minus NO) with the
// exchange per market, opens or extends a drift row for every difference and
// resolves rows that agree again. It never changes the ledger. It returns the
// number of markets that differ.
func compareKalshiPositions(
	ctx context.Context, tx *sql.Tx, accountID string, holdings map[string]*kalshiNetHolding,
	exchange []domain.KalshiMarketPosition, at time.Time,
) (int, error) {
	settled, err := kalshiSettlementTickers(ctx, tx, accountID)
	if err != nil {
		return 0, err
	}
	remote := make(map[string]domain.KalshiMarketPosition, len(exchange))
	for _, position := range exchange {
		ticker := strings.TrimSpace(position.Ticker)
		if _, ok := settled[ticker]; ok {
			continue
		}
		if _, duplicate := remote[ticker]; duplicate {
			return 0, fmt.Errorf("Kalshi position snapshot lists market %s twice", ticker)
		}
		remote[ticker] = position
	}
	openRows, err := openKalshiPositionDriftTickers(ctx, tx, accountID)
	if err != nil {
		return 0, err
	}
	tickers := make(map[string]struct{}, len(holdings)+len(remote)+len(openRows))
	for ticker := range holdings {
		tickers[ticker] = struct{}{}
	}
	for ticker := range remote {
		tickers[ticker] = struct{}{}
	}
	for _, ticker := range openRows {
		tickers[ticker] = struct{}{}
	}
	ordered := make([]string, 0, len(tickers))
	for ticker := range tickers {
		ordered = append(ordered, ticker)
	}
	sort.Strings(ordered)
	mismatches := 0
	for _, ticker := range ordered {
		holding := holdings[ticker]
		if holding == nil {
			holding = &kalshiNetHolding{yes: new(big.Rat), no: new(big.Rat), yesShares: "0", noShares: "0"}
		}
		exchangePosition, exchangeShares := new(big.Rat), domain.Decimal("0")
		var record any
		if position, ok := remote[ticker]; ok {
			value, parsed := new(big.Rat).SetString(position.PositionFP.String())
			if !parsed {
				return 0, fmt.Errorf("Kalshi position %s is invalid", ticker)
			}
			exchangePosition, exchangeShares = value, position.PositionFP
			if len(position.Raw) > 0 {
				record = string(position.Raw)
			}
		}
		net := new(big.Rat).Sub(holding.yes, holding.no)
		kind := ""
		switch {
		case net.Cmp(exchangePosition) != 0:
			kind = "NET_MISMATCH"
		case holding.yes.Sign() > 0 && holding.no.Sign() > 0:
			kind = "LOCAL_BOTH_SIDES"
		}
		if kind == "" {
			if _, err := tx.ExecContext(ctx, `
				UPDATE kalshi_position_drift
				SET status='RESOLVED', local_yes_shares=$3::numeric, local_no_shares=$4::numeric,
				    exchange_position=$5::numeric, last_seen_at=$6, resolved_at=$6
				WHERE execution_account_id=$1 AND ticker=$2 AND status='OPEN'`,
				accountID, ticker, holding.yesShares, holding.noShares, exchangeShares.String(), at); err != nil {
				return 0, fmt.Errorf("resolve Kalshi position drift: %w", err)
			}
			continue
		}
		mismatches++
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO kalshi_position_drift (
				execution_account_id, ticker, status, drift_kind, local_yes_shares, local_no_shares,
				exchange_position, exchange_record, observations, first_seen_at, last_seen_at
			) VALUES ($1,$2,'OPEN',$3,$4::numeric,$5::numeric,$6::numeric,$7::jsonb,1,$8,$8)
			ON CONFLICT (execution_account_id, ticker) DO UPDATE SET
				first_seen_at = CASE WHEN kalshi_position_drift.status='OPEN' THEN kalshi_position_drift.first_seen_at ELSE EXCLUDED.first_seen_at END,
				observations = CASE WHEN kalshi_position_drift.status='OPEN' THEN kalshi_position_drift.observations+1 ELSE 1 END,
				status='OPEN', drift_kind=EXCLUDED.drift_kind, local_yes_shares=EXCLUDED.local_yes_shares,
				local_no_shares=EXCLUDED.local_no_shares, exchange_position=EXCLUDED.exchange_position,
				exchange_record=EXCLUDED.exchange_record, last_seen_at=EXCLUDED.last_seen_at, resolved_at=NULL`,
			accountID, ticker, kind, holding.yesShares, holding.noShares, exchangeShares.String(), record, at); err != nil {
			return 0, fmt.Errorf("record Kalshi position drift: %w", err)
		}
	}
	return mismatches, nil
}

func kalshiSettlementTickers(ctx context.Context, tx *sql.Tx, accountID string) (map[string]struct{}, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ticker FROM kalshi_settlements WHERE execution_account_id=$1`, accountID)
	if err != nil {
		return nil, fmt.Errorf("list Kalshi settlement rows: %w", err)
	}
	defer rows.Close()
	tickers := make(map[string]struct{})
	for rows.Next() {
		var ticker string
		if err := rows.Scan(&ticker); err != nil {
			return nil, err
		}
		tickers[ticker] = struct{}{}
	}
	return tickers, rows.Err()
}

func openKalshiPositionDriftTickers(ctx context.Context, tx *sql.Tx, accountID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT ticker FROM kalshi_position_drift WHERE execution_account_id=$1 AND status='OPEN'`, accountID)
	if err != nil {
		return nil, fmt.Errorf("list open Kalshi position drifts: %w", err)
	}
	defer rows.Close()
	tickers := make([]string, 0)
	for rows.Next() {
		var ticker string
		if err := rows.Scan(&ticker); err != nil {
			return nil, err
		}
		tickers = append(tickers, ticker)
	}
	return tickers, rows.Err()
}

func countOpenKalshiPositionDrifts(ctx context.Context, tx *sql.Tx, accountID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM kalshi_position_drift WHERE execution_account_id=$1 AND status='OPEN'`,
		accountID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count open Kalshi position drifts: %w", err)
	}
	return count, nil
}

func loadKalshiLedgerCheckState(ctx context.Context, tx *sql.Tx, accountID string) (kalshiLedgerCheckState, error) {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO kalshi_ledger_checks (execution_account_id) VALUES ($1)
		ON CONFLICT (execution_account_id) DO NOTHING`, accountID); err != nil {
		return kalshiLedgerCheckState{}, fmt.Errorf("initialize Kalshi ledger check: %w", err)
	}
	var state kalshiLedgerCheckState
	if err := tx.QueryRowContext(ctx, `
		SELECT balance_outcome, balance_skip_reason, balance_consecutive_skips, balance_total_skips,
		       balance_skipping_since, pending_local_balance::text, pending_exchange_balance::text,
		       pending_account_version, pending_first_seen_at, last_synced_at, last_sync_event_id,
		       positions_open_drifts
		FROM kalshi_ledger_checks WHERE execution_account_id=$1 FOR UPDATE`, accountID).Scan(
		&state.balanceOutcome, &state.skipReason, &state.consecutiveSkips, &state.totalSkips,
		&state.skippingSince, &state.pendingLocal, &state.pendingExchange,
		&state.pendingVersion, &state.pendingFirstSeen, &state.lastSyncedAt, &state.lastSyncEventID,
		&state.openDrifts); err != nil {
		return kalshiLedgerCheckState{}, fmt.Errorf("read Kalshi ledger check: %w", err)
	}
	return state, nil
}

func writeKalshiLedgerCheckState(ctx context.Context, tx *sql.Tx, accountID string, state kalshiLedgerCheckState, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE kalshi_ledger_checks SET
			balance_checked_at=$2, balance_outcome=$3, balance_skip_reason=$4,
			balance_consecutive_skips=$5, balance_total_skips=$6, balance_skipping_since=$7,
			pending_local_balance=$8::numeric, pending_exchange_balance=$9::numeric,
			pending_account_version=$10, pending_first_seen_at=$11,
			last_synced_at=$12, last_sync_event_id=$13,
			positions_checked_at = CASE WHEN $14 = '' THEN positions_checked_at ELSE $2 END,
			positions_outcome = CASE WHEN $14 = '' THEN positions_outcome ELSE $14 END,
			positions_skip_reason = CASE WHEN $14 = '' THEN positions_skip_reason ELSE $15 END,
			positions_open_drifts=$16, updated_at=clock_timestamp()
		WHERE execution_account_id=$1`,
		accountID, at, state.balanceOutcome, state.skipReason, state.consecutiveSkips, state.totalSkips,
		state.skippingSince, state.pendingLocal, state.pendingExchange, state.pendingVersion,
		state.pendingFirstSeen, state.lastSyncedAt, state.lastSyncEventID,
		state.positionsOutcome, state.positionsReason, state.openDrifts); err != nil {
		return fmt.Errorf("record Kalshi ledger check: %w", err)
	}
	return nil
}

func truncateKalshiReason(reason string) string {
	const limit = 500
	runes := []rune(reason)
	if len(runes) <= limit {
		return reason
	}
	return string(runes[:limit])
}

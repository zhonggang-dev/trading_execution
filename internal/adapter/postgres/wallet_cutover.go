package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletcutover"
)

// WalletCutoverStore is the narrow database boundary used by the
// walletcutover CLI. It never exposes a generic balance update API.
type WalletCutoverStore struct {
	db *sql.DB
}

func NewWalletCutoverStore(db *sql.DB) (*WalletCutoverStore, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres database is required")
	}
	return &WalletCutoverStore{db: db}, nil
}

var _ walletcutover.Store = (*WalletCutoverStore)(nil)
var _ walletcutover.AppliedBatchSource = (*WalletCutoverStore)(nil)

func (store *WalletCutoverStore) LookupApplied(ctx context.Context, batchID, manifestSHA string) (walletcutover.ApplyResult, bool, error) {
	batchID = strings.TrimSpace(batchID)
	manifestSHA = strings.TrimSpace(manifestSHA)
	if batchID == "" || manifestSHA == "" {
		return walletcutover.ApplyResult{}, false, fmt.Errorf("batch id and manifest SHA are required")
	}
	var storedSHA, actor, reason string
	if err := store.db.QueryRowContext(ctx, `
		SELECT manifest_sha256, actor, reason
		  FROM execution_wallet_cutover_batches
		 WHERE batch_id=$1`, batchID).Scan(&storedSHA, &actor, &reason); err != nil {
		if err == sql.ErrNoRows {
			return walletcutover.ApplyResult{}, false, nil
		}
		return walletcutover.ApplyResult{}, false, fmt.Errorf("lookup wallet cutover batch: %w", err)
	}
	if storedSHA != manifestSHA {
		return walletcutover.ApplyResult{}, false, fmt.Errorf("wallet cutover batch exists with a different manifest SHA256")
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT execution_account_id, wallet_address
		  FROM execution_wallet_cutover_accounts
		 WHERE batch_id=$1 ORDER BY execution_account_id`, batchID)
	if err != nil {
		return walletcutover.ApplyResult{}, false, fmt.Errorf("lookup wallet cutover accounts: %w", err)
	}
	defer rows.Close()
	accounts := make([]walletcutover.AccountResult, 0, 2)
	for rows.Next() {
		var accountID, wallet string
		if err := rows.Scan(&accountID, &wallet); err != nil {
			return walletcutover.ApplyResult{}, false, fmt.Errorf("scan wallet cutover account: %w", err)
		}
		accounts = append(accounts, walletcutover.AccountResult{ExecutionAccountID: accountID, WalletAddress: wallet})
	}
	if err := rows.Err(); err != nil {
		return walletcutover.ApplyResult{}, false, fmt.Errorf("iterate wallet cutover accounts: %w", err)
	}
	if len(accounts) != 2 {
		return walletcutover.ApplyResult{}, false, fmt.Errorf("wallet cutover batch %q has incomplete account evidence", batchID)
	}
	seen := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		if account.ExecutionAccountID != "main" && account.ExecutionAccountID != "wallet-1" {
			return walletcutover.ApplyResult{}, false, fmt.Errorf("wallet cutover batch %q contains unauthorized account %q", batchID, account.ExecutionAccountID)
		}
		if _, exists := seen[account.ExecutionAccountID]; exists {
			return walletcutover.ApplyResult{}, false, fmt.Errorf("wallet cutover batch %q contains duplicate account %q", batchID, account.ExecutionAccountID)
		}
		seen[account.ExecutionAccountID] = struct{}{}
	}
	return walletcutover.ApplyResult{SchemaVersion: walletcutover.AppliedEvidenceSchemaVersion, AlreadyApplied: true, BatchID: batchID, ManifestSHA: manifestSHA, Accounts: accounts}, true, nil
}

func (store *WalletCutoverStore) Snapshot(ctx context.Context, accounts []string) ([]walletcutover.AccountState, error) {
	if len(accounts) != 2 {
		return nil, fmt.Errorf("wallet cutover snapshot requires exactly two accounts")
	}
	for _, account := range accounts {
		if account != "main" && account != "wallet-1" {
			return nil, fmt.Errorf("wallet cutover snapshot account %q is not allowed", account)
		}
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT a.execution_account_id, a.wallet_address, a.collateral_asset,
		       a.total_balance::text, a.available_balance::text, a.reserved_balance::text,
		       a.version,
		       COALESCE(c.paused, false),
		       EXISTS (SELECT 1 FROM reconciliation_runs r
		                WHERE r.execution_account_id=a.execution_account_id AND r.status='RUNNING'),
		       EXISTS (SELECT 1 FROM execution_orders o
		                WHERE o.execution_account_id=a.execution_account_id
		                  AND o.status NOT IN ('FILLED','REJECTED','CANCELLED','EXPIRED','MANUAL_REVIEW')),
		       EXISTS (SELECT 1 FROM asset_reservations r
		                WHERE r.execution_account_id=a.execution_account_id
		                  AND r.status IN ('ACTIVE','RECONCILIATION_REQUIRED')),
		       EXISTS (SELECT 1 FROM execution_fills f
		                WHERE f.execution_account_id=a.execution_account_id
		                  AND f.applied_at IS NULL AND f.status <> 'FAILED'),
		       EXISTS (SELECT 1 FROM strategy_order_intent_deliveries d
		                WHERE d.status IN ('PENDING','SUBMITTING')
		                  AND d.intent_payload->>'execution_account_id'=a.execution_account_id),
		       EXISTS (SELECT 1 FROM polymarket_redemptions r
		                WHERE r.execution_account_id=a.execution_account_id
		                  AND r.status NOT IN ('APPLIED','MANUAL_REVIEW'))
		  FROM execution_accounts a
		  LEFT JOIN execution_risk_controls c
		    ON c.execution_account_id=a.execution_account_id
		   AND c.control_scope='ACCOUNT' AND c.control_key=''
		 WHERE a.execution_account_id IN ($1,$2)
		 ORDER BY a.execution_account_id`, accounts[0], accounts[1])
	if err != nil {
		return nil, fmt.Errorf("query wallet cutover accounts: %w", err)
	}
	defer rows.Close()
	result := make([]walletcutover.AccountState, 0, 2)
	for rows.Next() {
		var state walletcutover.AccountState
		if err := rows.Scan(
			&state.ExecutionAccountID, &state.WalletAddress, &state.CollateralAsset,
			&state.TotalBalance, &state.AvailableBalance, &state.ReservedBalance,
			&state.Version, &state.Paused, &state.RunningRecon, &state.PendingOrder,
			&state.ActiveReservation, &state.PendingFill, &state.PendingDelivery,
			&state.PendingRedemption,
		); err != nil {
			return nil, fmt.Errorf("scan wallet cutover account: %w", err)
		}
		state.ExecutionAccountID = strings.TrimSpace(state.ExecutionAccountID)
		state.WalletAddress = strings.ToLower(strings.TrimSpace(state.WalletAddress))
		state.CollateralAsset = strings.TrimSpace(state.CollateralAsset)
		result = append(result, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet cutover accounts: %w", err)
	}
	return result, nil
}

// ValidateAssetCoverage prevents a cutover manifest from silently omitting a
// token that is still represented by the local managed ledger or by an
// effective immutable external-ownership baseline. Token id "0" is a valid
// conditional-token id and is intentionally not filtered out.
func (store *WalletCutoverStore) ValidateAssetCoverage(
	ctx context.Context,
	accounts []walletcutover.AccountManifest,
) error {
	if len(accounts) != 2 {
		return fmt.Errorf("wallet cutover asset coverage requires exactly two accounts")
	}
	manifestTokens := make(map[string]map[string]struct{}, len(accounts))
	ids := make([]string, 0, len(accounts))
	for _, account := range accounts {
		id := strings.TrimSpace(account.ExecutionAccountID)
		if id != "main" && id != "wallet-1" {
			return fmt.Errorf("wallet cutover asset coverage account %q is not allowed", id)
		}
		if _, exists := manifestTokens[id]; exists {
			return fmt.Errorf("wallet cutover asset coverage contains duplicate account %q", id)
		}
		manifestTokens[id] = make(map[string]struct{}, len(account.Assets))
		for _, asset := range account.Assets {
			manifestTokens[id][strings.TrimSpace(asset.TokenID)] = struct{}{}
		}
		ids = append(ids, id)
	}
	if ids[0] > ids[1] {
		ids[0], ids[1] = ids[1], ids[0]
	}
	rows, err := store.db.QueryContext(ctx, `
		WITH required AS (
			SELECT p.execution_account_id, p.token_id
			  FROM execution_positions p
			 WHERE p.execution_account_id IN ($1,$2)
			   AND p.total_shares > 0
			UNION
			SELECT l.execution_account_id, l.token_id
			  FROM position_lots l
			 WHERE l.execution_account_id IN ($1,$2)
			   AND l.status IN ('OPEN','SETTLED_PENDING_REDEEM')
			   AND l.remaining_shares > 0
			UNION
			SELECT item.execution_account_id, item.token_id
			  FROM execution_external_position_baseline_items item
			  LEFT JOIN execution_external_position_dispositions disposition
			    ON disposition.baseline_id=item.baseline_id
			   AND disposition.execution_account_id=item.execution_account_id
			   AND disposition.token_id=item.token_id
			   AND disposition.disposition_kind IN ('EXTERNAL_SELL','ADOPTION')
			 WHERE item.execution_account_id IN ($1,$2)
			 GROUP BY item.execution_account_id, item.token_id, item.shares
			HAVING item.shares - COALESCE(sum(disposition.shares_delta),0) > 0
			UNION
			SELECT latest.execution_account_id, latest.token_id
			  FROM (
				SELECT DISTINCT ON (a.execution_account_id, a.token_id)
				       a.execution_account_id, a.token_id,
				       a.chain_shares, a.managed_shares
				  FROM execution_wallet_cutover_assets a
				  JOIN execution_wallet_cutover_batches b USING (batch_id)
				 WHERE a.execution_account_id IN ($1,$2)
				 ORDER BY a.execution_account_id, a.token_id, b.created_at DESC, a.batch_id DESC
			  ) latest
			 WHERE latest.chain_shares > latest.managed_shares
		)
		SELECT execution_account_id, token_id
		  FROM required
		 ORDER BY execution_account_id, token_id`, ids[0], ids[1])
	if err != nil {
		return fmt.Errorf("query wallet cutover asset coverage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var accountID, tokenID string
		if err := rows.Scan(&accountID, &tokenID); err != nil {
			return fmt.Errorf("scan wallet cutover asset coverage: %w", err)
		}
		if _, ok := manifestTokens[accountID][tokenID]; !ok {
			return fmt.Errorf("wallet cutover manifest omits local asset %s/%s", accountID, tokenID)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate wallet cutover asset coverage: %w", err)
	}
	return nil
}

func (store *WalletCutoverStore) ApplyBaseline(ctx context.Context, params walletcutover.ApplyParams) (walletcutover.ApplyResult, error) {
	if params.Manifest.BatchID == "" || params.ManifestSHA == "" || params.Actor == "" || params.Reason == "" {
		return walletcutover.ApplyResult{}, fmt.Errorf("wallet cutover apply identity is incomplete")
	}
	manifestRaw := params.ManifestRaw
	if len(manifestRaw) == 0 {
		var err error
		manifestRaw, err = json.Marshal(params.Manifest)
		if err != nil {
			return walletcutover.ApplyResult{}, fmt.Errorf("encode wallet cutover manifest: %w", err)
		}
	}
	var manifestValue json.RawMessage
	if err := json.Unmarshal(manifestRaw, &manifestValue); err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("decode wallet cutover manifest: %w", err)
	}
	digest := sha256.Sum256(manifestRaw)
	if hex.EncodeToString(digest[:]) != params.ManifestSHA {
		return walletcutover.ApplyResult{}, fmt.Errorf("manifest SHA256 does not match manifest bytes")
	}
	chainRaw, err := json.Marshal(params.Chain)
	if err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("encode wallet cutover chain report: %w", err)
	}
	var chainValue json.RawMessage
	if err := json.Unmarshal(chainRaw, &chainValue); err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("decode wallet cutover chain report: %w", err)
	}
	payload, err := json.Marshal(struct {
		Manifest    json.RawMessage `json:"manifest"`
		ChainReport json.RawMessage `json:"chain_report"`
	}{Manifest: manifestValue, ChainReport: chainValue})
	if err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("encode wallet cutover payload: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("begin wallet cutover transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s'`); err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("configure wallet cutover transaction: %w", err)
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx,
		`SELECT execution_apply_wallet_reuse_cutover($1::jsonb,$2,$3,$4,$5)`,
		payload, params.Manifest.BatchID, params.ManifestSHA, params.Actor, params.Reason).Scan(&raw); err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("apply wallet cutover baseline: %w", err)
	}
	var result walletcutover.ApplyResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("decode wallet cutover result: %w", err)
	}
	result.ManifestSHA = params.ManifestSHA
	result.BatchID = params.Manifest.BatchID
	result.SchemaVersion = walletcutover.AppliedEvidenceSchemaVersion
	result.Applied = params.Commit && !result.AlreadyApplied
	result.Accounts = make([]walletcutover.AccountResult, 0, len(params.Manifest.Accounts))
	chainByID := make(map[string]walletcutover.ChainAccountReport, len(params.Chain.Accounts))
	for _, account := range params.Chain.Accounts {
		chainByID[account.ExecutionAccountID] = account
	}
	for _, account := range params.Manifest.CanonicalAccounts() {
		chain := chainByID[account.ExecutionAccountID]
		result.Accounts = append(result.Accounts, walletcutover.AccountResult{
			ExecutionAccountID: account.ExecutionAccountID,
			WalletAddress:      account.WalletAddress,
			BeforeBalance:      account.Ledger.TotalBalance,
			Adjustment:         decimalDifference(chain.CashBalance, account.Ledger.TotalBalance),
			AfterBalance:       chain.CashBalance,
			AssetCount:         len(account.Assets),
		})
	}
	if !params.Commit {
		if err := tx.Rollback(); err != nil {
			return walletcutover.ApplyResult{}, fmt.Errorf("rollback wallet cutover dry-run: %w", err)
		}
		return result, nil
	}
	if err := tx.Commit(); err != nil {
		return walletcutover.ApplyResult{}, fmt.Errorf("commit wallet cutover baseline: %w", err)
	}
	return result, nil
}

func decimalDifference(left, right domain.Decimal) domain.Decimal {
	l, err := left.Multiply("1")
	if err != nil {
		return ""
	}
	r, err := right.Multiply("1")
	if err != nil {
		return ""
	}
	return domain.Decimal(new(big.Rat).Sub(l, r).FloatString(30))
}

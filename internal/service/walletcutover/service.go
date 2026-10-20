package walletcutover

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// ChainSource is intentionally read-only. Implementations must query the
// exact finalized block supplied by the manifest; latest/unconfirmed values
// are never accepted as a cutover baseline.
type ChainSource interface {
	ChainID(context.Context) (uint64, error)
	LatestBlock(context.Context) (uint64, error)
	BlockHeader(context.Context, uint64) (string, time.Time, error)
	BalanceAt(context.Context, string, uint64) (domain.Decimal, error)
	TokenBalanceAt(context.Context, string, string, uint64) (domain.Decimal, error)
}

// Store owns the local database authority. ApplyBaseline must re-check all
// invariants in its own transaction; the service result is advisory input,
// never an authorization shortcut.
type Store interface {
	Snapshot(context.Context, []string) ([]AccountState, error)
	ApplyBaseline(context.Context, ApplyParams) (ApplyResult, error)
}

// AssetCoverageSource is implemented by the production store to make the
// manifest enumerate every currently-owned local token before the RPC read.
// It is optional so small unit-test stores can continue to exercise the
// service without rebuilding a database fixture.
type AssetCoverageSource interface {
	ValidateAssetCoverage(context.Context, []AccountManifest) error
}

// AppliedBatchSource is an optional replay read used before a fresh audit.
// It lets a caller recover a lost successful response without requiring the
// already-mutated account version to match the old manifest again.
type AppliedBatchSource interface {
	LookupApplied(context.Context, string, string) (ApplyResult, bool, error)
}

type AccountState struct {
	ExecutionAccountID string
	WalletAddress      string
	CollateralAsset    string
	TotalBalance       domain.Decimal
	AvailableBalance   domain.Decimal
	ReservedBalance    domain.Decimal
	Version            int64
	Paused             bool
	RunningRecon       bool
	PendingOrder       bool
	ActiveReservation  bool
	PendingFill        bool
	PendingDelivery    bool
	PendingRedemption  bool
}

type ApplyParams struct {
	Manifest    Manifest
	ManifestRaw []byte
	ManifestSHA string
	Actor       string
	Reason      string
	Chain       ChainReport
	Commit      bool
}

type ApplyResult struct {
	SchemaVersion  string          `json:"schema_version"`
	Applied        bool            `json:"applied"`
	AlreadyApplied bool            `json:"already_applied"`
	BatchID        string          `json:"batch_id"`
	ManifestSHA    string          `json:"manifest_sha256"`
	AccountCount   int             `json:"account_count,omitempty"`
	AssetCount     int             `json:"asset_count,omitempty"`
	Accounts       []AccountResult `json:"accounts,omitempty"`
}

type AccountResult struct {
	ExecutionAccountID string         `json:"execution_account_id"`
	WalletAddress      string         `json:"wallet_address,omitempty"`
	BeforeBalance      domain.Decimal `json:"before_balance"`
	Adjustment         domain.Decimal `json:"adjustment"`
	AfterBalance       domain.Decimal `json:"after_balance"`
	AssetCount         int            `json:"asset_count"`
}

type ChainReport struct {
	ChainID       uint64               `json:"chain_id"`
	BlockNumber   uint64               `json:"block_number"`
	BlockHash     string               `json:"block_hash"`
	BlockTime     time.Time            `json:"block_time"`
	LatestBlock   uint64               `json:"latest_block"`
	Confirmations uint64               `json:"confirmations"`
	Accounts      []ChainAccountReport `json:"accounts"`
}

type ChainAccountReport struct {
	ExecutionAccountID string             `json:"execution_account_id"`
	WalletAddress      string             `json:"wallet_address"`
	CashBalance        domain.Decimal     `json:"cash_balance"`
	Assets             []ChainAssetReport `json:"assets"`
}

type ChainAssetReport struct {
	TokenID string         `json:"token_id"`
	Shares  domain.Decimal `json:"shares"`
}

type Service struct {
	chain ChainSource
	store Store
	now   func() time.Time
}

type Params struct {
	Chain ChainSource
	Store Store
	Now   func() time.Time
}

func New(params Params) (*Service, error) {
	if params.Chain == nil || params.Store == nil {
		return nil, fmt.Errorf("wallet cutover chain source and store are required")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{chain: params.Chain, store: params.Store, now: params.Now}, nil
}

// Audit validates the supplied manifest against a fresh RPC snapshot and a
// repeatable local account snapshot. It never writes to either source.
func (s *Service) Audit(ctx context.Context, manifest Manifest) (ChainReport, []AccountState, error) {
	now := s.now().UTC()
	if err := manifest.Validate(now); err != nil {
		return ChainReport{}, nil, err
	}
	accounts := manifest.CanonicalAccounts()
	ids := make([]string, 0, len(accounts))
	for _, account := range accounts {
		ids = append(ids, strings.TrimSpace(account.ExecutionAccountID))
	}
	states, err := s.store.Snapshot(ctx, ids)
	if err != nil {
		return ChainReport{}, nil, fmt.Errorf("read local cutover snapshot: %w", err)
	}
	if err := validateLocalStates(accounts, states); err != nil {
		return ChainReport{}, nil, err
	}
	if coverage, ok := s.store.(AssetCoverageSource); ok {
		if err := coverage.ValidateAssetCoverage(ctx, accounts); err != nil {
			return ChainReport{}, nil, err
		}
	}
	report, err := s.readChain(ctx, manifest)
	if err != nil {
		return ChainReport{}, nil, err
	}
	if err := compareChain(manifest, report); err != nil {
		return ChainReport{}, nil, err
	}
	return report, states, nil
}

func (s *Service) readChain(ctx context.Context, manifest Manifest) (ChainReport, error) {
	block := manifest.EvidenceBlock
	chainID, err := s.chain.ChainID(ctx)
	if err != nil {
		return ChainReport{}, fmt.Errorf("read Polygon chain id: %w", err)
	}
	if chainID != block.ChainID {
		return ChainReport{}, fmt.Errorf("RPC chain id %d does not match evidence chain id %d", chainID, block.ChainID)
	}
	latest, err := s.chain.LatestBlock(ctx)
	if err != nil {
		return ChainReport{}, fmt.Errorf("read RPC latest block: %w", err)
	}
	if latest < block.Number || latest-block.Number < block.Confirmations {
		return ChainReport{}, fmt.Errorf("evidence block %d has only %d confirmations; require %d", block.Number, latest-block.Number, block.Confirmations)
	}
	hash, blockTime, err := s.chain.BlockHeader(ctx, block.Number)
	if err != nil {
		return ChainReport{}, fmt.Errorf("read evidence block header: %w", err)
	}
	hash = strings.ToLower(strings.TrimSpace(hash))
	if hash != strings.ToLower(strings.TrimSpace(block.Hash)) {
		return ChainReport{}, fmt.Errorf("evidence block hash changed: manifest=%s rpc=%s", block.Hash, hash)
	}
	report := ChainReport{ChainID: chainID, BlockNumber: block.Number, BlockHash: hash, BlockTime: blockTime.UTC(), LatestBlock: latest, Confirmations: latest - block.Number}
	for _, account := range manifest.CanonicalAccounts() {
		wallet := strings.ToLower(strings.TrimSpace(account.WalletAddress))
		cash, err := s.chain.BalanceAt(ctx, wallet, block.Number)
		if err != nil {
			return ChainReport{}, fmt.Errorf("read %s pUSD balance: %w", account.ExecutionAccountID, err)
		}
		entry := ChainAccountReport{ExecutionAccountID: account.ExecutionAccountID, WalletAddress: wallet, CashBalance: cash, Assets: make([]ChainAssetReport, 0, len(account.Assets))}
		for _, asset := range account.Assets {
			shares, err := s.chain.TokenBalanceAt(ctx, wallet, asset.TokenID, block.Number)
			if err != nil {
				return ChainReport{}, fmt.Errorf("read %s token %s balance: %w", account.ExecutionAccountID, asset.TokenID, err)
			}
			entry.Assets = append(entry.Assets, ChainAssetReport{TokenID: asset.TokenID, Shares: shares})
		}
		report.Accounts = append(report.Accounts, entry)
	}
	return report, nil
}

func validateLocalStates(accounts []AccountManifest, states []AccountState) error {
	if len(states) != len(accounts) {
		return fmt.Errorf("local snapshot returned %d accounts, require %d", len(states), len(accounts))
	}
	byID := make(map[string]AccountManifest, len(accounts))
	for _, account := range accounts {
		byID[account.ExecutionAccountID] = account
	}
	for _, state := range states {
		account, ok := byID[state.ExecutionAccountID]
		if !ok {
			return fmt.Errorf("local snapshot contains unauthorized account %q", state.ExecutionAccountID)
		}
		if strings.ToLower(strings.TrimSpace(state.WalletAddress)) != strings.ToLower(strings.TrimSpace(account.WalletAddress)) {
			return fmt.Errorf("%s wallet identity differs between manifest and database", state.ExecutionAccountID)
		}
		if state.CollateralAsset != PUSDAsset || state.Version != account.BeforeVersion {
			return fmt.Errorf("%s local asset/version does not match manifest CAS", state.ExecutionAccountID)
		}
		reservedSign, reservedErr := state.ReservedBalance.Sign()
		if reservedErr != nil || reservedSign != 0 || state.PendingOrder || state.ActiveReservation || state.PendingFill || state.PendingDelivery || state.PendingRedemption || state.RunningRecon {
			return fmt.Errorf("%s is not quiescent for cutover", state.ExecutionAccountID)
		}
		if !state.Paused {
			return fmt.Errorf("%s account control must be paused before baseline apply", state.ExecutionAccountID)
		}
		if !state.TotalBalance.Equal(account.Ledger.TotalBalance) || !state.AvailableBalance.Equal(account.Ledger.AvailableBalance) {
			return fmt.Errorf("%s local ledger changed since manifest", state.ExecutionAccountID)
		}
	}
	return nil
}

func compareChain(manifest Manifest, report ChainReport) error {
	byID := make(map[string]ChainAccountReport, len(report.Accounts))
	for _, value := range report.Accounts {
		byID[value.ExecutionAccountID] = value
	}
	for _, account := range manifest.Accounts {
		chain, ok := byID[account.ExecutionAccountID]
		if !ok {
			return fmt.Errorf("RPC snapshot omitted %s", account.ExecutionAccountID)
		}
		if !strings.EqualFold(chain.WalletAddress, account.WalletAddress) {
			return fmt.Errorf("%s RPC wallet identity differs from manifest", account.ExecutionAccountID)
		}
		if !chain.CashBalance.Equal(account.ChainBalance) {
			return fmt.Errorf("%s pUSD balance changed: manifest=%s rpc=%s", account.ExecutionAccountID, account.ChainBalance, chain.CashBalance)
		}
		assets := make(map[string]domain.Decimal, len(chain.Assets))
		for _, asset := range chain.Assets {
			assets[asset.TokenID] = asset.Shares
		}
		for _, asset := range account.Assets {
			shares, ok := assets[asset.TokenID]
			if !ok || !shares.Equal(asset.ChainShares) {
				return fmt.Errorf("%s token %s balance changed", account.ExecutionAccountID, asset.TokenID)
			}
		}
	}
	return nil
}

func (s *Service) Apply(ctx context.Context, params ApplyParams) (ApplyResult, error) {
	if err := params.Manifest.Validate(s.now().UTC()); err != nil {
		return ApplyResult{}, err
	}
	if strings.TrimSpace(params.ManifestSHA) == "" || len(params.ManifestSHA) != 64 {
		return ApplyResult{}, fmt.Errorf("manifest SHA256 is required")
	}
	if _, err := hex.DecodeString(params.ManifestSHA); err != nil || strings.ToLower(params.ManifestSHA) != params.ManifestSHA {
		return ApplyResult{}, fmt.Errorf("manifest SHA256 must be lowercase hexadecimal")
	}
	if strings.TrimSpace(params.Actor) == "" || strings.TrimSpace(params.Reason) == "" {
		return ApplyResult{}, fmt.Errorf("apply actor and reason are required")
	}
	if params.Actor != params.Manifest.Actor || params.Reason != params.Manifest.Reason {
		return ApplyResult{}, fmt.Errorf("apply actor/reason must exactly match the reviewed manifest")
	}
	if len(params.ManifestRaw) > 0 {
		if err := rejectDuplicateJSONKeys(params.ManifestRaw); err != nil {
			return ApplyResult{}, err
		}
		decoder := json.NewDecoder(bytes.NewReader(params.ManifestRaw))
		decoder.DisallowUnknownFields()
		var rawManifest Manifest
		if err := decoder.Decode(&rawManifest); err != nil {
			return ApplyResult{}, fmt.Errorf("decode reviewed manifest bytes: %w", err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				return ApplyResult{}, fmt.Errorf("reviewed manifest contains trailing JSON")
			}
			return ApplyResult{}, fmt.Errorf("reviewed manifest trailing data: %w", err)
		}
		if err := rawManifest.Validate(time.Time{}); err != nil {
			return ApplyResult{}, fmt.Errorf("reviewed manifest bytes are invalid: %w", err)
		}
		if !reflect.DeepEqual(rawManifest, params.Manifest) {
			return ApplyResult{}, fmt.Errorf("reviewed manifest bytes do not match typed manifest")
		}
	}
	if err := params.Chain.ValidateAgainst(params.Manifest); err != nil {
		return ApplyResult{}, err
	}
	return s.store.ApplyBaseline(ctx, params)
}

func (r ChainReport) ValidateAgainst(m Manifest) error {
	if r.ChainID != m.EvidenceBlock.ChainID || r.BlockNumber != m.EvidenceBlock.Number || !strings.EqualFold(r.BlockHash, m.EvidenceBlock.Hash) {
		return fmt.Errorf("chain report does not match manifest evidence block")
	}
	if r.Confirmations < m.EvidenceBlock.Confirmations {
		return fmt.Errorf("chain report confirmations %d are below manifest requirement %d", r.Confirmations, m.EvidenceBlock.Confirmations)
	}
	return compareChain(m, r)
}

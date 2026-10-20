// Package walletcutover contains the evidence and authorization boundary for
// reusing an existing execution wallet.  It deliberately has no signing or
// venue client: a cutover manifest can only describe and reconcile existing
// assets.
package walletcutover

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const (
	ManifestSchemaVersion        = "trading.wallet_reuse_cutover.v1"
	AppliedEvidenceSchemaVersion = "trading.wallet_reuse_cutover.applied.v1"
	PolygonChainID               = uint64(137)
	PUSDAsset                    = "pUSD"
)

var (
	hexAddressRE = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
	hexHashRE    = regexp.MustCompile(`^0x[0-9a-f]{64}$`)
	decimalIDRE  = regexp.MustCompile(`^[0-9]+$`)
	batchIDRE    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// Manifest is the reviewed, private, exact snapshot supplied by an operator.
// Chain values are always re-read by Service before they are accepted; the
// values in this document are comparison evidence rather than authority.
type Manifest struct {
	SchemaVersion string            `json:"schema_version"`
	BatchID       string            `json:"batch_id"`
	ObservedAt    time.Time         `json:"observed_at"`
	ExpiresAt     time.Time         `json:"expires_at"`
	EvidenceBlock EvidenceBlock     `json:"evidence_block"`
	Accounts      []AccountManifest `json:"accounts"`
	Actor         string            `json:"actor"`
	Reason        string            `json:"reason"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type EvidenceBlock struct {
	ChainID       uint64 `json:"chain_id"`
	Number        uint64 `json:"number"`
	Hash          string `json:"hash"`
	Confirmations uint64 `json:"confirmations"`
}

type AccountManifest struct {
	ExecutionAccountID string          `json:"execution_account_id"`
	WalletAddress      string          `json:"wallet_address"`
	BeforeVersion      int64           `json:"before_version"`
	Ledger             BalanceSnapshot `json:"ledger"`
	ChainBalance       domain.Decimal  `json:"chain_balance"`
	Assets             []AssetManifest `json:"assets"`
	OpenIssueIDs       []string        `json:"open_issue_ids,omitempty"`
	Evidence           map[string]any  `json:"evidence,omitempty"`
}

type BalanceSnapshot struct {
	Asset            string         `json:"asset"`
	TotalBalance     domain.Decimal `json:"total_balance"`
	AvailableBalance domain.Decimal `json:"available_balance"`
	ReservedBalance  domain.Decimal `json:"reserved_balance"`
}

// AssetManifest records every selected token at the evidence block. External
// residuals remain evidence-only; OLD_MANAGED may carry a reviewed reduction
// of an existing lot, preserving its opening identity and excluding PnL.
type AssetManifest struct {
	TokenID        string          `json:"token_id"`
	ConditionID    string          `json:"condition_id"`
	MarketID       string          `json:"market_id"`
	OutcomeIndex   *int            `json:"outcome_index,omitempty"`
	OutcomeName    string          `json:"outcome_name"`
	NegRisk        bool            `json:"neg_risk"`
	ChainShares    domain.Decimal  `json:"chain_shares"`
	ManagedShares  domain.Decimal  `json:"managed_shares"`
	ManagedCost    domain.Decimal  `json:"managed_cost"`
	AfterCost      domain.Decimal  `json:"after_cost"`
	LotAdjustments []LotAdjustment `json:"lot_adjustments,omitempty"`
	Ownership      string          `json:"ownership"`
	ResidualReason string          `json:"residual_reason,omitempty"`
	Evidence       map[string]any  `json:"evidence,omitempty"`
}

// LotAdjustment is a narrowly scoped, evidence-backed reduction of an old
// managed lot after an audited external exit. It preserves the lot's opening
// order/fill and owner; it never creates a SELL or realized PnL.
type LotAdjustment struct {
	LotID        string         `json:"lot_id"`
	BeforeShares domain.Decimal `json:"before_shares"`
	BeforeCost   domain.Decimal `json:"before_cost"`
	AfterShares  domain.Decimal `json:"after_shares"`
	AfterCost    domain.Decimal `json:"after_cost"`
	Reason       string         `json:"reason"`
}

func (m Manifest) Validate(now time.Time) error {
	if strings.TrimSpace(m.SchemaVersion) != ManifestSchemaVersion {
		return fmt.Errorf("schema_version must be %q", ManifestSchemaVersion)
	}
	if !batchIDRE.MatchString(strings.TrimSpace(m.BatchID)) {
		return fmt.Errorf("batch_id is invalid")
	}
	if m.ObservedAt.IsZero() || m.ExpiresAt.IsZero() || !m.ExpiresAt.After(m.ObservedAt) {
		return fmt.Errorf("observed_at and expires_at must be ordered")
	}
	if !now.IsZero() && !m.ExpiresAt.After(now.UTC()) {
		return fmt.Errorf("manifest expired at %s", m.ExpiresAt.UTC().Format(time.RFC3339))
	}
	if strings.TrimSpace(m.Actor) == "" || strings.TrimSpace(m.Reason) == "" {
		return fmt.Errorf("actor and reason are required")
	}
	if err := validateSafeEvidence(m.Metadata); err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	if m.EvidenceBlock.ChainID != PolygonChainID || m.EvidenceBlock.Number == 0 || m.EvidenceBlock.Confirmations == 0 {
		return fmt.Errorf("evidence_block must be a finalized Polygon block")
	}
	blockHash := strings.TrimSpace(m.EvidenceBlock.Hash)
	if blockHash != strings.ToLower(blockHash) || !hexHashRE.MatchString(blockHash) {
		return fmt.Errorf("evidence_block.hash must be a lowercase 32-byte hash")
	}
	if len(m.Accounts) != 2 {
		return fmt.Errorf("manifest must contain exactly main and wallet-1")
	}
	seenAccounts := map[string]struct{}{}
	for i := range m.Accounts {
		if err := m.Accounts[i].Validate(now, m.ObservedAt); err != nil {
			return fmt.Errorf("accounts[%d]: %w", i, err)
		}
		id := strings.TrimSpace(m.Accounts[i].ExecutionAccountID)
		if id != "main" && id != "wallet-1" {
			return fmt.Errorf("accounts[%d]: account %q is outside the cutover allowlist", i, id)
		}
		if _, ok := seenAccounts[id]; ok {
			return fmt.Errorf("duplicate execution account %q", id)
		}
		seenAccounts[id] = struct{}{}
	}
	if _, ok := seenAccounts["main"]; !ok {
		return fmt.Errorf("main is missing from manifest")
	}
	if _, ok := seenAccounts["wallet-1"]; !ok {
		return fmt.Errorf("wallet-1 is missing from manifest")
	}
	return nil
}

func (a AccountManifest) Validate(now time.Time, observedAt time.Time) error {
	id := strings.TrimSpace(a.ExecutionAccountID)
	if id == "" || strings.TrimSpace(a.WalletAddress) == "" || a.BeforeVersion < 1 {
		return fmt.Errorf("account identity and positive before_version are required")
	}
	wallet := strings.ToLower(strings.TrimSpace(a.WalletAddress))
	if wallet != strings.TrimSpace(a.WalletAddress) || !hexAddressRE.MatchString(wallet) {
		return fmt.Errorf("wallet_address must be a lowercase EVM address")
	}
	if strings.TrimSpace(a.Ledger.Asset) != PUSDAsset {
		return fmt.Errorf("ledger.asset must be %q", PUSDAsset)
	}
	if err := validateNonNegative("ledger.total_balance", a.Ledger.TotalBalance); err != nil {
		return err
	}
	if err := validateNonNegative("ledger.available_balance", a.Ledger.AvailableBalance); err != nil {
		return err
	}
	if err := validateNonNegative("ledger.reserved_balance", a.Ledger.ReservedBalance); err != nil {
		return err
	}
	if cmp, err := a.Ledger.TotalBalance.Compare(a.Ledger.AvailableBalance); err != nil {
		return fmt.Errorf("ledger balance: %w", err)
	} else if cmp != 0 {
		return fmt.Errorf("ledger balance must be idle with reserved_balance=0")
	}
	if sign, err := a.Ledger.ReservedBalance.Sign(); err != nil || sign != 0 {
		return fmt.Errorf("ledger.reserved_balance must be zero")
	}
	if err := validateNonNegative("chain_balance", a.ChainBalance); err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if observedAt.After(now.Add(5 * time.Minute)) {
		return fmt.Errorf("observed_at is too far in the future")
	}
	if err := validateSafeEvidence(a.Evidence); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	seenTokens := map[string]struct{}{}
	seenIssues := map[string]struct{}{}
	for _, rawIssueID := range a.OpenIssueIDs {
		issueID := strings.TrimSpace(rawIssueID)
		if issueID == "" || issueID != rawIssueID {
			return fmt.Errorf("open_issue_ids must contain canonical non-empty ids")
		}
		if _, exists := seenIssues[issueID]; exists {
			return fmt.Errorf("duplicate open issue id %q", issueID)
		}
		seenIssues[issueID] = struct{}{}
	}
	for i := range a.Assets {
		if err := a.Assets[i].Validate(); err != nil {
			return fmt.Errorf("assets[%d]: %w", i, err)
		}
		if _, ok := seenTokens[a.Assets[i].TokenID]; ok {
			return fmt.Errorf("duplicate token %q", a.Assets[i].TokenID)
		}
		seenTokens[a.Assets[i].TokenID] = struct{}{}
	}
	return nil
}

func (a AssetManifest) Validate() error {
	if !decimalIDRE.MatchString(strings.TrimSpace(a.TokenID)) {
		return fmt.Errorf("token_id must be a decimal token id")
	}
	if strings.TrimSpace(a.ConditionID) == "" || strings.TrimSpace(a.MarketID) == "" || strings.TrimSpace(a.OutcomeName) == "" {
		return fmt.Errorf("condition_id, market_id and outcome_name are required")
	}
	if err := validateSafeEvidence(a.Evidence); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	if err := validateNonNegative("chain_shares", a.ChainShares); err != nil {
		return err
	}
	if err := validateNonNegative("managed_shares", a.ManagedShares); err != nil {
		return err
	}
	if err := validateNonNegative("managed_cost", a.ManagedCost); err != nil {
		return err
	}
	if err := validateNonNegative("after_cost", a.AfterCost); err != nil {
		return err
	}
	if cmp, err := a.ManagedShares.Compare(a.ChainShares); err != nil {
		return fmt.Errorf("managed_shares: %w", err)
	} else if cmp > 0 {
		if a.Ownership != "OLD_MANAGED" {
			return fmt.Errorf("managed_shares cannot exceed chain_shares for external ownership")
		}
	}
	switch strings.TrimSpace(a.Ownership) {
	case "OLD_MANAGED", "RETAINED_OLD", "EXTERNAL":
	default:
		return fmt.Errorf("ownership must be OLD_MANAGED, RETAINED_OLD or EXTERNAL")
	}
	if (a.Ownership == "RETAINED_OLD" || a.Ownership == "EXTERNAL") && strings.TrimSpace(a.ResidualReason) == "" {
		return fmt.Errorf("residual_reason is required for retained external ownership")
	}
	if a.Ownership == "OLD_MANAGED" && !a.ManagedShares.Equal("0") && a.ManagedCost.Equal("0") {
		return fmt.Errorf("managed_cost is required for OLD_MANAGED shares")
	}
	if a.Ownership != "OLD_MANAGED" && (!a.ManagedCost.Equal("0") || !a.AfterCost.Equal("0")) {
		return fmt.Errorf("external residual assets cannot carry managed cost")
	}
	if a.OutcomeIndex != nil && (*a.OutcomeIndex < 0 || *a.OutcomeIndex > 1) {
		return fmt.Errorf("outcome_index must be 0 or 1")
	}
	seenLots := map[string]struct{}{}
	totalBefore, totalAfter := domain.Decimal("0"), domain.Decimal("0")
	for i := range a.LotAdjustments {
		adjustment := a.LotAdjustments[i]
		if strings.TrimSpace(adjustment.LotID) == "" || strings.TrimSpace(adjustment.Reason) == "" {
			return fmt.Errorf("lot_adjustments[%d] requires lot_id and reason", i)
		}
		if _, exists := seenLots[adjustment.LotID]; exists {
			return fmt.Errorf("duplicate lot adjustment %q", adjustment.LotID)
		}
		seenLots[adjustment.LotID] = struct{}{}
		for _, item := range []struct {
			name  string
			value domain.Decimal
		}{
			{"before_shares", adjustment.BeforeShares}, {"before_cost", adjustment.BeforeCost},
			{"after_shares", adjustment.AfterShares}, {"after_cost", adjustment.AfterCost},
		} {
			if err := validateNonNegative("lot_adjustments."+item.name, item.value); err != nil {
				return err
			}
		}
		if cmp, err := adjustment.AfterShares.Compare(adjustment.BeforeShares); err != nil || cmp > 0 {
			return fmt.Errorf("lot adjustment %q may only reduce shares", adjustment.LotID)
		}
		if cmp, err := adjustment.AfterCost.Compare(adjustment.BeforeCost); err != nil || cmp > 0 {
			return fmt.Errorf("lot adjustment %q may only reduce cost", adjustment.LotID)
		}
		totalBefore = addDecimal(totalBefore, adjustment.BeforeShares)
		totalAfter = addDecimal(totalAfter, adjustment.AfterShares)
	}
	if len(a.LotAdjustments) > 0 {
		if a.Ownership != "OLD_MANAGED" {
			return fmt.Errorf("lot adjustments require OLD_MANAGED ownership")
		}
		if !totalBefore.Equal(a.ManagedShares) || !totalAfter.Equal(a.ChainShares) {
			return fmt.Errorf("lot adjustments must exactly cover managed and chain shares")
		}
		var costBefore, costAfter domain.Decimal = "0", "0"
		for _, adjustment := range a.LotAdjustments {
			costBefore = addDecimal(costBefore, adjustment.BeforeCost)
			costAfter = addDecimal(costAfter, adjustment.AfterCost)
		}
		if !costBefore.Equal(a.ManagedCost) || !costAfter.Equal(a.AfterCost) {
			return fmt.Errorf("lot adjustments must exactly cover managed and after cost")
		}
	} else if a.Ownership == "OLD_MANAGED" {
		if !a.ManagedShares.Equal(a.ChainShares) || !a.ManagedCost.Equal(a.AfterCost) {
			return fmt.Errorf("OLD_MANAGED changes require explicit lot_adjustments")
		}
	}
	return nil
}

func addDecimal(left, right domain.Decimal) domain.Decimal {
	// Manifest quantities are validated before this helper is called. Using
	// rational arithmetic avoids float rounding in lot aggregate checks.
	l, _ := left.Multiply("1")
	r, _ := right.Multiply("1")
	return domain.Decimal(l.Add(l, r).FloatString(30))
}

func validateNonNegative(name string, value domain.Decimal) error {
	sign, err := value.Sign()
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if sign < 0 {
		return fmt.Errorf("%s must be non-negative", name)
	}
	return nil
}

func validateSafeEvidence(value any) error {
	return walkSafeEvidence(value, "")
}

func walkSafeEvidence(value any, path string) error {
	switch typed := value.(type) {
	case map[string]string:
		for key, item := range typed {
			if sensitiveEvidenceKey(key) {
				return fmt.Errorf("field %s is not allowed", key)
			}
			if err := walkSafeEvidence(item, path+"."+key); err != nil {
				return err
			}
		}
	case map[string]any:
		for key, item := range typed {
			if sensitiveEvidenceKey(key) {
				return fmt.Errorf("field %s is not allowed", key)
			}
			if err := walkSafeEvidence(item, path+"."+key); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range typed {
			if err := walkSafeEvidence(item, path); err != nil {
				return err
			}
		}
	}
	return nil
}

func sensitiveEvidenceKey(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, marker := range []string{"private", "secret", "passphrase", "api_key", "apikey", "password", "credential", "authorization", "url"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

// ReadManifest reads a private manifest, rejecting symlinks, group/other
// permissions, unknown fields, duplicate keys and files larger than 1 MiB.
func ReadManifest(path string, now time.Time) (Manifest, []byte, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Manifest{}, nil, "", fmt.Errorf("manifest path is required")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return Manifest{}, nil, "", fmt.Errorf("stat manifest: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return Manifest{}, nil, "", fmt.Errorf("manifest must be a regular non-symlink file")
	}
	if info.Mode().Perm() != 0o600 {
		return Manifest{}, nil, "", fmt.Errorf("manifest permissions must be exactly 0600")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != os.Geteuid() {
		return Manifest{}, nil, "", fmt.Errorf("manifest must be owned by the current operator")
	}
	if info.Size() <= 0 || info.Size() > 1<<20 {
		return Manifest{}, nil, "", fmt.Errorf("manifest size must be between 1 byte and 1 MiB")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, nil, "", err
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return Manifest{}, nil, "", err
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, nil, "", fmt.Errorf("decode manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Manifest{}, nil, "", fmt.Errorf("manifest contains trailing JSON")
		}
		return Manifest{}, nil, "", fmt.Errorf("manifest trailing data: %w", err)
	}
	// Expiry is enforced by the audit/apply service after the caller has a
	// chance to check an already-applied batch. This lets a lost apply response
	// be recovered idempotently without reusing an expired manifest for new
	// writes.
	if err := manifest.Validate(time.Time{}); err != nil {
		return Manifest{}, nil, "", err
	}
	digest := sha256.Sum256(data)
	return manifest, data, hex.EncodeToString(digest[:]), nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, isDelim := token.(json.Delim)
		if !isDelim {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("decode object key: %w", err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate JSON object key %q", key)
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delim)
		}
	}
	if err := walk(); err != nil {
		return fmt.Errorf("validate manifest JSON keys: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("manifest contains multiple JSON values")
		}
		return fmt.Errorf("manifest trailing data: %w", err)
	}
	return nil
}

// PrivateManifestMode reports whether path satisfies the same private-file
// check used by ReadManifest. It is useful to test the apply boundary without
// re-reading an actual manifest.
func PrivateManifestMode(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fs.ErrPermission
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != os.Geteuid() {
		return fs.ErrPermission
	}
	return nil
}

// CanonicalAccounts returns account manifests in deterministic account order.
func (m Manifest) CanonicalAccounts() []AccountManifest {
	result := append([]AccountManifest(nil), m.Accounts...)
	sort.Slice(result, func(i, j int) bool { return result[i].ExecutionAccountID < result[j].ExecutionAccountID })
	return result
}

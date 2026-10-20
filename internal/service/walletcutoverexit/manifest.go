package walletcutoverexit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// SchemaVersion is the private, operator-only manifest contract for the
// wallet reuse cutover. It is deliberately separate from strategy orders and
// can only describe risk-reducing exits.
const SchemaVersion = "trading.wallet_reuse_exit.v1"

const maxManifestBytes = 1 << 20

var ethereumAddress = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// Manifest is the exact, reviewed asset plan. The bytes of the file (rather
// than a re-marshalled value) are what the CLI hashes for the confirmation
// gate, so an operator cannot approve one file and execute another.
type Manifest struct {
	SchemaVersion string             `json:"schema_version"`
	ManifestID    string             `json:"manifest_id"`
	Observed      Evidence           `json:"observed"`
	ExpiresAt     time.Time          `json:"expires_at"`
	Actions       []AssetAction      `json:"asset_actions"`
	Redemptions   []RedemptionAction `json:"redemptions,omitempty"`
}

// Evidence is the finalized chain/read-model boundary used by every action.
// Hash and block are required even in dry-run manifests so a later operator
// cannot silently broaden the asset set.
type Evidence struct {
	Block     uint64    `json:"block"`
	BlockHash string    `json:"block_hash"`
	Observed  time.Time `json:"observed_at"`
	Nonce     string    `json:"nonce,omitempty"`
}

// AssetAction describes one exact token quantity. Ownership is informational
// and is intentionally allowed to be managed or external; the tool never
// converts external inventory into a normal strategy lot.
type AssetAction struct {
	ActionID           string             `json:"action_id"`
	ExecutionAccountID string             `json:"execution_account_id"`
	WalletAddress      string             `json:"wallet_address"`
	MarketID           string             `json:"market_id"`
	ConditionID        string             `json:"condition_id,omitempty"`
	OutcomeIndex       int                `json:"outcome_index"`
	OutcomeName        string             `json:"outcome_name,omitempty"`
	TokenID            string             `json:"token_id"`
	NegRisk            bool               `json:"neg_risk"`
	Shares             domain.Decimal     `json:"shares"`
	WorstPrice         domain.Decimal     `json:"worst_price"`
	TickSize           domain.Decimal     `json:"tick_size"`
	Side               domain.Side        `json:"side"`
	OrderType          domain.OrderType   `json:"order_type"`
	TimeInForce        domain.TimeInForce `json:"time_in_force"`
	ExpiresAt          time.Time          `json:"expires_at"`
	LotID              string             `json:"lot_id"`
	Ownership          string             `json:"ownership"`
	Nonce              string             `json:"nonce,omitempty"`
	EvidenceRef        string             `json:"evidence_ref,omitempty"`
}

// RedemptionAction is only executable when a separate read-only verifier has
// proved settlement, payout (including zero payout), wallet balance and the
// exact adapter approval. The manifest never authorizes an automatic approve.
type RedemptionAction struct {
	ActionID           string           `json:"action_id"`
	ExecutionAccountID string           `json:"execution_account_id"`
	WalletAddress      string           `json:"wallet_address"`
	ConditionID        string           `json:"condition_id"`
	NegRisk            bool             `json:"neg_risk"`
	TokenIDs           []string         `json:"token_ids"`
	Shares             []domain.Decimal `json:"shares"`
	OutcomeIndexes     []int            `json:"outcome_indexes"`
	PayoutBaseUnits    string           `json:"payout_base_units"`
	EvidenceRef        string           `json:"evidence_ref,omitempty"`
}

// ManifestHash returns the SHA-256 digest of the exact input bytes.
func ManifestHash(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// ParseManifest decodes and validates a manifest. The raw bytes are retained
// only by the caller for the exact hash confirmation; no secrets are logged.
func ParseManifest(data []byte, now time.Time) (Manifest, string, error) {
	return parseManifest(data, now, false)
}

// ParseManifestForRecovery permits an expired manifest to be loaded only so
// an existing journal entry can be reconciled by venue identity. The service
// still rejects any attempt to create a new signed submission from it.
func ParseManifestForRecovery(data []byte, now time.Time) (Manifest, string, error) {
	return parseManifest(data, now, true)
}

func parseManifest(data []byte, now time.Time, allowExpired bool) (Manifest, string, error) {
	var manifest Manifest
	if len(bytes.TrimSpace(data)) == 0 {
		return Manifest{}, "", fmt.Errorf("wallet exit manifest is empty")
	}
	if len(data) > maxManifestBytes {
		return Manifest{}, "", fmt.Errorf("wallet exit manifest exceeds %d bytes", maxManifestBytes)
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return Manifest{}, "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, "", fmt.Errorf("decode wallet exit manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Manifest{}, "", fmt.Errorf("wallet exit manifest contains trailing JSON")
		}
		return Manifest{}, "", fmt.Errorf("wallet exit manifest trailing data: %w", err)
	}
	hash := ManifestHash(data)
	if err := manifest.validate(now, allowExpired); err != nil {
		return Manifest{}, hash, err
	}
	return manifest, hash, nil
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
		return fmt.Errorf("validate wallet exit manifest JSON keys: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("wallet exit manifest contains multiple JSON values")
		}
		return fmt.Errorf("wallet exit manifest trailing data: %w", err)
	}
	return nil
}

// Validate performs all checks that are independent of live CLOB/RPC reads.
func (manifest Manifest) Validate(now time.Time) error {
	return manifest.validate(now, false)
}

// ValidateForRecovery validates identities and action shapes while allowing
// time bounds to be past. It is only safe when the caller proves every action
// already has a journal entry and therefore no new signature/POST is possible.
func (manifest Manifest) ValidateForRecovery(now time.Time) error {
	return manifest.validate(now, true)
}

func (manifest Manifest) validate(now time.Time, allowExpired bool) error {
	now = now.UTC()
	if manifest.SchemaVersion != SchemaVersion {
		return fmt.Errorf("wallet exit manifest schema_version must be %s", SchemaVersion)
	}
	if strings.TrimSpace(manifest.ManifestID) == "" || len(manifest.ManifestID) > 128 {
		return fmt.Errorf("wallet exit manifest_id is required and must be at most 128 characters")
	}
	if manifest.Observed.Block == 0 || !ethereumHash(manifest.Observed.BlockHash) || manifest.Observed.Observed.IsZero() {
		return fmt.Errorf("wallet exit manifest observed evidence requires a finalized block, block_hash, and observed_at")
	}
	if nonce := strings.TrimSpace(manifest.Observed.Nonce); nonce != "" {
		value, ok := new(big.Int).SetString(nonce, 10)
		if !ok || value.Sign() < 0 {
			return fmt.Errorf("observed nonce must be a non-negative integer")
		}
	}
	if manifest.ExpiresAt.IsZero() {
		return fmt.Errorf("wallet exit manifest is expired")
	}
	if !allowExpired && !manifest.ExpiresAt.After(now) {
		return fmt.Errorf("wallet exit manifest is expired")
	}
	if !allowExpired && manifest.ExpiresAt.Sub(now) > 24*time.Hour {
		return fmt.Errorf("wallet exit manifest expiry must be within 24 hours")
	}
	if len(manifest.Actions) == 0 && len(manifest.Redemptions) == 0 {
		return fmt.Errorf("wallet exit manifest contains no actions")
	}
	if len(manifest.Actions) > 100 || len(manifest.Redemptions) > 100 {
		return fmt.Errorf("wallet exit manifest contains too many actions")
	}
	seen := make(map[string]struct{}, len(manifest.Actions)+len(manifest.Redemptions))
	for i := range manifest.Actions {
		action := &manifest.Actions[i]
		if err := action.validate(manifest, now, allowExpired); err != nil {
			return fmt.Errorf("asset action %d: %w", i, err)
		}
		if _, exists := seen[action.ActionID]; exists {
			return fmt.Errorf("duplicate action_id %q", action.ActionID)
		}
		seen[action.ActionID] = struct{}{}
	}
	for i := range manifest.Redemptions {
		action := &manifest.Redemptions[i]
		if err := action.Validate(); err != nil {
			return fmt.Errorf("redemption action %d: %w", i, err)
		}
		if _, exists := seen[action.ActionID]; exists {
			return fmt.Errorf("duplicate action_id %q", action.ActionID)
		}
		seen[action.ActionID] = struct{}{}
	}
	return nil
}

func (action AssetAction) Validate(manifest Manifest, now time.Time) error {
	return action.validate(manifest, now, false)
}

func (action AssetAction) validate(manifest Manifest, now time.Time, allowExpired bool) error {
	if strings.TrimSpace(action.ActionID) == "" || len(action.ActionID) > 128 {
		return fmt.Errorf("action_id is required")
	}
	if action.ExecutionAccountID != "main" && action.ExecutionAccountID != "wallet-1" {
		return fmt.Errorf("execution_account_id %q is outside the cutover allowlist", action.ExecutionAccountID)
	}
	if !ethereumAddress.MatchString(strings.TrimSpace(action.WalletAddress)) {
		return fmt.Errorf("wallet_address is invalid")
	}
	if strings.TrimSpace(action.MarketID) == "" || !ethereumHash(strings.TrimSpace(action.ConditionID)) || strings.TrimSpace(action.TokenID) == "" || strings.TrimSpace(action.OutcomeName) == "" {
		return fmt.Errorf("market_id, condition_id, token_id, and outcome_name are required")
	}
	if action.OutcomeIndex != 0 && action.OutcomeIndex != 1 {
		return fmt.Errorf("outcome_index must be 0 or 1")
	}
	if action.Ownership == "managed" && strings.TrimSpace(action.LotID) == "" {
		return fmt.Errorf("lot_id is required to preserve old ownership")
	}
	if action.Ownership != "managed" && action.Ownership != "external" {
		return fmt.Errorf("ownership must be managed or external")
	}
	if nonce := strings.TrimSpace(action.Nonce); nonce != "" {
		value, ok := new(big.Int).SetString(nonce, 10)
		if !ok || value.Sign() < 0 {
			return fmt.Errorf("nonce must be a non-negative integer")
		}
	}
	if action.OrderType != domain.OrderTypeLimit {
		return fmt.Errorf("only LIMIT exit orders are allowed")
	}
	if action.Side != domain.SideSell {
		return fmt.Errorf("only SELL exit orders are allowed")
	}
	if action.TimeInForce != domain.TimeInForceFOK && action.TimeInForce != domain.TimeInForceIOC && action.TimeInForce != domain.TimeInForceFAK {
		return fmt.Errorf("exit order must use native FOK/IOC/FAK time_in_force")
	}
	if action.ExpiresAt.IsZero() || (!allowExpired && !action.ExpiresAt.After(now)) || action.ExpiresAt.After(manifest.ExpiresAt) {
		return fmt.Errorf("action expires_at must be in the future and no later than manifest expiry")
	}
	if sign, err := action.Shares.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("shares must be positive")
	}
	if action.Shares.Scale() > 2 {
		return fmt.Errorf("shares must use at most 2 decimal places")
	}
	if sign, err := action.WorstPrice.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("worst_price must be positive")
	}
	if sign, err := action.TickSize.Sign(); err != nil || sign <= 0 {
		return fmt.Errorf("tick_size must be positive")
	}
	if multiple, err := action.WorstPrice.IsMultipleOf(action.TickSize); err != nil || !multiple {
		return fmt.Errorf("worst_price must be an exact tick_size multiple")
	}
	if cmp, err := action.WorstPrice.Compare("1"); err != nil || cmp >= 0 {
		return fmt.Errorf("worst_price must be below 1")
	}
	return nil
}

func (action RedemptionAction) Validate() error {
	if strings.TrimSpace(action.ActionID) == "" || len(action.ActionID) > 128 {
		return fmt.Errorf("action_id is required")
	}
	if action.ExecutionAccountID != "main" && action.ExecutionAccountID != "wallet-1" {
		return fmt.Errorf("execution_account_id %q is outside the cutover allowlist", action.ExecutionAccountID)
	}
	if !ethereumAddress.MatchString(strings.TrimSpace(action.WalletAddress)) || !ethereumHash(strings.TrimSpace(action.ConditionID)) {
		return fmt.Errorf("wallet_address and condition_id are required")
	}
	if strings.TrimSpace(action.PayoutBaseUnits) == "" {
		return fmt.Errorf("payout_base_units is required, including explicit 0 for zero payout")
	}
	if value, ok := new(big.Int).SetString(strings.TrimSpace(action.PayoutBaseUnits), 10); !ok || value.Sign() < 0 {
		return fmt.Errorf("payout_base_units must be a non-negative integer")
	}
	if len(action.TokenIDs) == 0 || len(action.TokenIDs) != len(action.Shares) || len(action.TokenIDs) != len(action.OutcomeIndexes) {
		return fmt.Errorf("token_ids, shares, and outcome_indexes must contain matching non-empty lists")
	}
	seenTokens := make(map[string]struct{}, len(action.TokenIDs))
	for i, tokenID := range action.TokenIDs {
		tokenID = strings.TrimSpace(tokenID)
		if _, exists := seenTokens[tokenID]; exists {
			return fmt.Errorf("token_ids[%d] duplicates token %q", i, tokenID)
		}
		seenTokens[tokenID] = struct{}{}
		value, ok := new(big.Int).SetString(tokenID, 10)
		if !ok || value.Sign() < 0 || value.BitLen() > 256 {
			return fmt.Errorf("token_ids[%d] must be a decimal token id", i)
		}
		if sign, err := action.Shares[i].Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("shares[%d] must be positive", i)
		}
		if action.Shares[i].Scale() > 6 {
			return fmt.Errorf("shares[%d] must use at most 6 decimal places", i)
		}
		if action.OutcomeIndexes[i] != 0 && action.OutcomeIndexes[i] != 1 {
			return fmt.Errorf("outcome_indexes[%d] must be 0 or 1", i)
		}
	}
	return nil
}

func ethereumHash(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 66 || !strings.HasPrefix(value, "0x") {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}

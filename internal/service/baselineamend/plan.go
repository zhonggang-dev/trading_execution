// Package baselineamend derives an audited baseline amendment from observed
// chain and ledger state. It has no database, RPC, signing or venue access:
// the caller supplies exact facts and receives either one amendment or a
// refusal. Anything ambiguous is refused.
package baselineamend

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var hashRE = func(value string) bool {
	if len(value) != 66 || !strings.HasPrefix(value, "0x") {
		return false
	}
	for _, character := range value[2:] {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

// Identity is the token identity the reconciliation compares exactly.
type Identity struct {
	ConditionID  string
	OutcomeIndex *int
	OutcomeName  string
	NegRisk      bool
}

// Block is a finalized Polygon block the chain balance was read at.
type Block struct {
	Number        uint64
	Hash          string
	Confirmations uint64
	Time          time.Time
}

// Input is everything a plan is derived from.
type Input struct {
	Kind               domain.BaselineAmendmentKind
	ExecutionAccountID string
	WalletAddress      string
	TokenID            string
	Identity           Identity
	// ChainShares is the wallet's ERC-1155 balance of the token at Block.
	ChainShares domain.Decimal
	// ManagedShares is the ledger's execution_positions.total_shares.
	ManagedShares domain.Decimal
	// BaselineRemaining is the unmanaged baseline remaining before the amendment.
	BaselineRemaining domain.Decimal
	Block             Block
	// MaxAddShares bounds a residual so a large unexplained balance is never
	// registered as unmanaged by mistake.
	MaxAddShares domain.Decimal
	Actor        string
	Reason       string
	// Note is optional operator context, for example the burning transaction.
	Note string
}

// Build returns the amendment, or an error describing why it is refused.
func Build(in Input) (domain.BaselineAmendment, error) {
	account := strings.TrimSpace(in.ExecutionAccountID)
	token := strings.TrimSpace(in.TokenID)
	wallet := strings.TrimSpace(in.WalletAddress)
	if account == "" || token == "" || wallet == "" {
		return domain.BaselineAmendment{}, fmt.Errorf("execution account, wallet and token are required")
	}
	if wallet != strings.ToLower(wallet) {
		return domain.BaselineAmendment{}, fmt.Errorf("wallet address must be lowercase")
	}
	if strings.TrimSpace(in.Actor) == "" || strings.TrimSpace(in.Reason) == "" {
		return domain.BaselineAmendment{}, fmt.Errorf("actor and reason are required")
	}
	identity := in.Identity
	if strings.TrimSpace(identity.ConditionID) == "" || strings.TrimSpace(identity.OutcomeName) == "" ||
		identity.OutcomeIndex == nil || *identity.OutcomeIndex < 0 || *identity.OutcomeIndex > 1 {
		return domain.BaselineAmendment{}, fmt.Errorf("token identity (condition, outcome index and name) is incomplete")
	}
	if in.Block.Number == 0 || !hashRE(in.Block.Hash) || in.Block.Confirmations == 0 || in.Block.Time.IsZero() {
		return domain.BaselineAmendment{}, fmt.Errorf("a finalized evidence block is required")
	}
	chain, err := rat("chain shares", in.ChainShares)
	if err != nil {
		return domain.BaselineAmendment{}, err
	}
	managed, err := rat("managed shares", in.ManagedShares)
	if err != nil {
		return domain.BaselineAmendment{}, err
	}
	remaining, err := rat("baseline remaining", in.BaselineRemaining)
	if err != nil {
		return domain.BaselineAmendment{}, err
	}

	var shares *big.Rat
	switch in.Kind {
	case domain.BaselineAmendmentAddResidual:
		maxAdd, err := rat("max add shares", in.MaxAddShares)
		if err != nil {
			return domain.BaselineAmendment{}, err
		}
		shares = new(big.Rat).Sub(chain, new(big.Rat).Add(managed, remaining))
		if shares.Sign() <= 0 {
			return domain.BaselineAmendment{}, fmt.Errorf(
				"nothing to add: chain %s is not above managed %s plus baseline %s", in.ChainShares, in.ManagedShares, in.BaselineRemaining)
		}
		if shares.Cmp(maxAdd) > 0 {
			return domain.BaselineAmendment{}, fmt.Errorf(
				"residual %s exceeds the allowed maximum %s; larger balances need a reviewed explanation", format(shares), in.MaxAddShares)
		}
	case domain.BaselineAmendmentRetireConsumed:
		if chain.Sign() != 0 {
			return domain.BaselineAmendment{}, fmt.Errorf("cannot retire: the wallet still holds %s of the token", in.ChainShares)
		}
		if managed.Sign() != 0 {
			return domain.BaselineAmendment{}, fmt.Errorf("cannot retire: the ledger still holds %s managed shares", in.ManagedShares)
		}
		if remaining.Sign() <= 0 {
			return domain.BaselineAmendment{}, fmt.Errorf("cannot retire: there is no unmanaged baseline remaining")
		}
		shares = remaining
	default:
		return domain.BaselineAmendment{}, fmt.Errorf("kind must be %s or %s",
			domain.BaselineAmendmentAddResidual, domain.BaselineAmendmentRetireConsumed)
	}

	evidence := map[string]any{
		"schema_version":            domain.BaselineAmendmentEvidenceSchema,
		"wallet_address":            wallet,
		"block_number":              in.Block.Number,
		"block_hash":                in.Block.Hash,
		"block_time":                in.Block.Time.UTC().Format(time.RFC3339),
		"confirmations":             in.Block.Confirmations,
		"chain_shares":              in.ChainShares.String(),
		"managed_shares":            in.ManagedShares.String(),
		"baseline_remaining_before": in.BaselineRemaining.String(),
	}
	if note := strings.TrimSpace(in.Note); note != "" {
		evidence["note"] = note
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return domain.BaselineAmendment{}, fmt.Errorf("encode amendment evidence: %w", err)
	}
	amount := domain.Decimal(format(shares))
	digest := sha256.Sum256([]byte(strings.Join([]string{
		account, token, string(in.Kind), amount.String(), in.Block.Hash}, "|")))
	return domain.BaselineAmendment{
		AmendmentID:        fmt.Sprintf("%s:%s:%s", account, strings.ToLower(string(in.Kind)), hex.EncodeToString(digest[:])[:24]),
		ExecutionAccountID: account,
		TokenID:            token,
		ConditionID:        strings.TrimSpace(identity.ConditionID),
		OutcomeIndex:       identity.OutcomeIndex,
		OutcomeName:        strings.TrimSpace(identity.OutcomeName),
		NegRisk:            identity.NegRisk,
		Kind:               in.Kind,
		Shares:             amount,
		ObservedAt:         in.Block.Time.UTC(),
		Evidence:           raw,
		Actor:              strings.TrimSpace(in.Actor),
		Reason:             strings.TrimSpace(in.Reason),
	}, nil
}

func rat(label string, value domain.Decimal) (*big.Rat, error) {
	text := strings.TrimSpace(value.String())
	if text == "" || strings.ContainsAny(text, "/eE") {
		return nil, fmt.Errorf("%s %q is not a plain decimal", label, value)
	}
	result, ok := new(big.Rat).SetString(text)
	if !ok || result.Sign() < 0 {
		return nil, fmt.Errorf("%s %q must be a non-negative decimal", label, value)
	}
	return result, nil
}

// format renders an exact decimal; amounts here come from six-decimal chain
// units, so 30 digits are always sufficient.
func format(value *big.Rat) string {
	text := value.FloatString(30)
	text = strings.TrimRight(text, "0")
	return strings.TrimSuffix(text, ".")
}

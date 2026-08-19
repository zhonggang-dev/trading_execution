package polymarket

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// SignatureType 表示后端使用的 SignatureType 类型。
type SignatureType uint8

const (
	SignatureTypeEOA         SignatureType = 0
	SignatureTypePolyProxy   SignatureType = 1
	SignatureTypeGnosisSafe  SignatureType = 2
	SignatureTypePolyEIP1271 SignatureType = 3
)

// APICredentials 表示后端使用的 APICredentials 类型。
type APICredentials struct {
	Key        string
	Secret     string
	Passphrase string
}

// FillFeeEvidenceRequest identifies one CLOB V2 OrderFilled component. The
// evidence implementation must resolve exactly one matching log from the
// successful Polygon transaction and must not infer amounts from CLOB rates.
type FillFeeEvidenceRequest struct {
	TransactionHash         string
	VenueOrderID            string
	ExecutionAccountID      string
	ExpectedExchangeAddress string
	ExpectedMakerAddress    string
	ExpectedBuilderCode     string
	Side                    domain.Side
	TokenID                 string
	Shares                  domain.Decimal
	Price                   domain.Decimal
}

// FillFeeEvidence is the lossless accounting evidence emitted by the V2
// exchange. Amounts stay in integer base units until the trading adapter has
// verified identities, asset decimals, and the CLOB observation.
//
// OrderFilled exposes only the total fee. BuilderFeeKnown must therefore be
// true before the adapter can split that total. A zero builder code can be
// proven to have a zero builder fee; a non-zero builder requires an additional
// authoritative builder-fee source rather than a configured/rate-derived
// guess.
type FillFeeEvidence struct {
	Source               string
	ExchangeAddress      string
	TransactionHash      string
	OrderHash            string
	MakerAddress         string
	TokenID              string
	BuilderCode          string
	Side                 domain.Side
	MakerAmountBaseUnits string
	TakerAmountBaseUnits string
	TotalFeeBaseUnits    string
	BuilderFeeBaseUnits  string
	BuilderFeeRateBPS    domain.Decimal
	BuilderFeeKnown      bool
	CollateralDecimals   uint8
	OutcomeTokenDecimals uint8
	BlockNumber          uint64
	BlockHash            string
	LogIndex             uint64
	Confirmations        uint64
}

// FillFeeEvidenceSource resolves finalized on-chain settlement evidence for a
// CLOB trade. Implementations must fail when a receipt is missing/reverted,
// the matching OrderFilled log is absent/ambiguous, or finality is insufficient.
type FillFeeEvidenceSource interface {
	ResolveFillFeeEvidence(context.Context, FillFeeEvidenceRequest) (FillFeeEvidence, error)
}

// BalanceAssetType is the asset namespace accepted by the CLOB
// balance-allowance endpoint.
type BalanceAssetType string

const (
	BalanceAssetCollateral  BalanceAssetType = "COLLATERAL"
	BalanceAssetConditional BalanceAssetType = "CONDITIONAL"

	// Production CLOB V2 spends pUSD through both exchange contracts. A wallet
	// that has approved only one of them is not ready for arbitrary markets.
	StandardExchangeV2Address = "0xE111180000d2663C0091e4f400237545B87B996B"
	NegRiskExchangeV2Address  = "0xe2222d279d744050d28e00520010520000310F59"
	PUSDAddress               = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
)

// BalanceAllowance contains raw Polygon base-unit quantities. Callers must not
// format these integer values as decimal USDC without applying token decimals.
type BalanceAllowance struct {
	AssetType  BalanceAssetType
	TokenID    string
	Balance    string
	Allowances map[string]string
}

// Positive reports whether the base-unit quantity is a valid positive integer.
func (allowance BalanceAllowance) Positive() bool {
	value, ok := new(big.Int).SetString(strings.TrimSpace(allowance.Balance), 10)
	return ok && value.Sign() > 0
}

// AllAllowancesPositive requires at least one returned contract allowance and
// rejects zero, negative, or malformed values.
func (allowance BalanceAllowance) AllAllowancesPositive() bool {
	if len(allowance.Allowances) == 0 {
		return false
	}
	for _, raw := range allowance.Allowances {
		value, ok := new(big.Int).SetString(strings.TrimSpace(raw), 10)
		if !ok || value.Sign() <= 0 {
			return false
		}
	}
	return true
}

// RequiredAllowancesPositive verifies an explicit contract set. It does not
// treat an unrelated positive allowance as sufficient for CLOB trading.
func (allowance BalanceAllowance) RequiredAllowancesPositive(required ...string) bool {
	if len(required) == 0 {
		return false
	}
	for _, address := range required {
		raw, exists := allowance.Allowances[strings.ToLower(strings.TrimSpace(address))]
		if !exists {
			return false
		}
		value, ok := new(big.Int).SetString(strings.TrimSpace(raw), 10)
		if !ok || value.Sign() <= 0 {
			return false
		}
	}
	return true
}

// DigestSigner 定义精简的摘要签名接口，以支持 HSM/KMS 实现并避免执行适配器读取私钥。
type DigestSigner interface {
	Address() string
	SignDigest(ctx context.Context, digest []byte) ([]byte, error)
}

// TradingAccount 表示后端使用的 TradingAccount 类型。
type TradingAccount struct {
	ExecutionAccountID string
	FunderAddress      string
	SignatureType      SignatureType
	API                APICredentials
	Signer             DigestSigner
}

// validate 校验 对应数据 的字段和业务约束。
func (account TradingAccount) validate() error {
	if strings.TrimSpace(account.ExecutionAccountID) == "" {
		return fmt.Errorf("execution account id is required")
	}
	if account.Signer == nil {
		return fmt.Errorf("signer is required")
	}
	signerAddress := strings.TrimSpace(account.Signer.Address())
	if _, ok := decodeAddress(signerAddress); !ok {
		return fmt.Errorf("signer address is invalid")
	}
	if _, ok := decodeAddress(account.FunderAddress); !ok {
		return fmt.Errorf("funder address is invalid")
	}
	if account.SignatureType > SignatureTypePolyEIP1271 {
		return fmt.Errorf("signature type %d is unsupported", account.SignatureType)
	}
	if account.SignatureType == SignatureTypePolyEIP1271 {
		return fmt.Errorf("POLY_1271 requires the wrapped Solady signature flow and is deliberately disabled until contract-wallet conformance tests are installed")
	}
	if account.SignatureType == SignatureTypeEOA && !strings.EqualFold(signerAddress, account.FunderAddress) {
		return fmt.Errorf("EOA signer and funder must be the same address")
	}
	if strings.TrimSpace(account.API.Key) == "" || strings.TrimSpace(account.API.Secret) == "" || strings.TrimSpace(account.API.Passphrase) == "" {
		return fmt.Errorf("CLOB api key, secret, and passphrase are required")
	}
	return nil
}

// CredentialProvider 表示后端使用的 CredentialProvider 类型。
type CredentialProvider interface {
	Account(ctx context.Context, executionAccountID string) (TradingAccount, error)
}

// StaticCredentialProvider 适用于注入式密钥和测试，返回账户副本且绝不在不同钱包之间回退。
type StaticCredentialProvider struct {
	mu       sync.RWMutex
	accounts map[string]TradingAccount
}

// NewStaticCredentialProvider 创建并初始化 Static Credential Provider。
func NewStaticCredentialProvider(accounts []TradingAccount) (*StaticCredentialProvider, error) {
	provider := &StaticCredentialProvider{accounts: make(map[string]TradingAccount, len(accounts))}
	for _, account := range accounts {
		account.ExecutionAccountID = strings.TrimSpace(account.ExecutionAccountID)
		if _, ok := decodeAddress(strings.TrimSpace(account.FunderAddress)); !ok {
			return nil, fmt.Errorf("account %q: funder address is invalid", account.ExecutionAccountID)
		}
		account.FunderAddress = strings.ToLower(strings.TrimSpace(account.FunderAddress))
		account.API.Key = strings.TrimSpace(account.API.Key)
		account.API.Secret = strings.TrimSpace(account.API.Secret)
		account.API.Passphrase = strings.TrimSpace(account.API.Passphrase)
		if err := account.validate(); err != nil {
			return nil, fmt.Errorf("account %q: %w", account.ExecutionAccountID, err)
		}
		if _, exists := provider.accounts[account.ExecutionAccountID]; exists {
			return nil, fmt.Errorf("duplicate execution account %q", account.ExecutionAccountID)
		}
		provider.accounts[account.ExecutionAccountID] = account
	}
	if len(provider.accounts) == 0 {
		return nil, fmt.Errorf("at least one execution account is required")
	}
	return provider, nil
}

// Account 按执行账户标识返回隔离的交易凭证。
func (provider *StaticCredentialProvider) Account(_ context.Context, executionAccountID string) (TradingAccount, error) {
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	account, exists := provider.accounts[strings.TrimSpace(executionAccountID)]
	if !exists {
		return TradingAccount{}, fmt.Errorf("execution account %q is not configured", executionAccountID)
	}
	return account, nil
}

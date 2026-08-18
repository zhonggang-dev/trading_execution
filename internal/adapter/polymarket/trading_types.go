package polymarket

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// DigestSigner is intentionally small so production can use an HSM/KMS-backed
// implementation. The execution adapter never needs to read a private key.
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

// StaticCredentialProvider is suitable for injected secrets and tests. It
// copies account records and never falls back from one wallet to another.
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

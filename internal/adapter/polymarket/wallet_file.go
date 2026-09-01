package polymarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"
)

const maxWalletFileBytes = 4 << 20

// WalletAccountSpec 表示 Trading Execution 接受的磁盘密钥结构，也兼容旧实盘 wallets.json 映射。
// 映射键会转换为 execution_account_id，address 用作 funder_address；TradingAccount 创建后不再暴露敏感值，调用方不得记录该结构。
type WalletAccountSpec struct {
	ExecutionAccountID   string  `json:"execution_account_id"`
	Address              string  `json:"address"`
	FunderAddress        string  `json:"funder_address"`
	PrivateKey           string  `json:"private_key"`
	SignatureType        *uint8  `json:"signature_type"`
	APIKey               string  `json:"api_key"`
	APISecret            string  `json:"api_secret"`
	APIPassphrase        string  `json:"api_passphrase"`
	APINonce             *uint64 `json:"api_nonce"`
	RelayerAPIKey        string  `json:"relayer_api_key"`
	RelayerAPIKeyAddress string  `json:"relayer_api_key_address"`
}

// WalletLoadParams 表示后端使用的 WalletLoadParams 类型。
type WalletLoadParams struct {
	Path                           string
	CredentialBootstrapper         *CredentialBootstrapper
	BootstrapMissingAPICredentials bool
}

// LoadTradingAccounts 从钱包文件加载并构建隔离交易账户。
func LoadTradingAccounts(ctx context.Context, params WalletLoadParams) ([]TradingAccount, error) {
	path := strings.TrimSpace(params.Path)
	if path == "" {
		return nil, fmt.Errorf("Polymarket accounts file path is required")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("lstat Polymarket accounts file: %w", err)
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("Polymarket accounts file must be a non-symlink regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open Polymarket accounts file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat Polymarket accounts file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("Polymarket accounts file must be a regular file")
	}
	if !os.SameFile(pathInfo, info) {
		return nil, fmt.Errorf("Polymarket accounts file changed while opening")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("Polymarket accounts file must not be accessible to group or other users")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return nil, fmt.Errorf("Polymarket accounts file must be owned by the service user")
	}

	payload, err := io.ReadAll(io.LimitReader(file, maxWalletFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Polymarket accounts file: %w", err)
	}
	if len(payload) > maxWalletFileBytes {
		return nil, fmt.Errorf("Polymarket accounts file exceeds %d bytes", maxWalletFileBytes)
	}
	specs, err := decodeWalletAccountSpecs(payload)
	if err != nil {
		return nil, err
	}

	accounts := make([]TradingAccount, 0, len(specs))
	for _, spec := range specs {
		account, buildErr := buildTradingAccount(ctx, spec, params)
		if buildErr != nil {
			return nil, fmt.Errorf("execution account %q: %w", spec.ExecutionAccountID, buildErr)
		}
		accounts = append(accounts, account)
	}
	return accounts, nil
}

// decodeWalletAccountSpecs 解码并校验 Wallet Account Specs。
func decodeWalletAccountSpecs(payload []byte) ([]WalletAccountSpec, error) {
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil, fmt.Errorf("Polymarket accounts file is empty")
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, fmt.Errorf("decode Polymarket accounts file: %w", err)
	}
	if rawAccounts, exists := root["accounts"]; exists {
		var specs []WalletAccountSpec
		if err := json.Unmarshal(rawAccounts, &specs); err != nil {
			return nil, fmt.Errorf("decode Polymarket accounts: %w", err)
		}
		if len(specs) == 0 {
			return nil, fmt.Errorf("Polymarket accounts file contains no accounts")
		}
		return normalizeWalletAccountSpecs(specs)
	}

	// Legacy wallets.json is keyed by wallet id. Sorting makes startup and
	// readiness output deterministic without changing wallet selection.
	keys := make([]string, 0, len(root))
	for key := range root {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	specs := make([]WalletAccountSpec, 0, len(keys))
	for _, key := range keys {
		var spec WalletAccountSpec
		if err := json.Unmarshal(root[key], &spec); err != nil {
			return nil, fmt.Errorf("decode legacy wallet %q: %w", key, err)
		}
		if strings.TrimSpace(spec.ExecutionAccountID) == "" {
			spec.ExecutionAccountID = key
		}
		specs = append(specs, spec)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("Polymarket accounts file contains no accounts")
	}
	return normalizeWalletAccountSpecs(specs)
}

// normalizeWalletAccountSpecs 规范化 Wallet Account Specs 的字段和表示。
func normalizeWalletAccountSpecs(specs []WalletAccountSpec) ([]WalletAccountSpec, error) {
	seen := make(map[string]struct{}, len(specs))
	for index := range specs {
		spec := &specs[index]
		spec.ExecutionAccountID = strings.TrimSpace(spec.ExecutionAccountID)
		if spec.ExecutionAccountID == "" {
			return nil, fmt.Errorf("account at index %d has no execution_account_id", index)
		}
		if _, exists := seen[spec.ExecutionAccountID]; exists {
			return nil, fmt.Errorf("duplicate execution account %q", spec.ExecutionAccountID)
		}
		seen[spec.ExecutionAccountID] = struct{}{}
	}
	return specs, nil
}

// buildTradingAccount 根据钱包配置构建并校验隔离交易账户。
func buildTradingAccount(ctx context.Context, spec WalletAccountSpec, params WalletLoadParams) (TradingAccount, error) {
	signer, err := NewEOASigner(spec.PrivateKey)
	if err != nil {
		return TradingAccount{}, err
	}
	funder := strings.ToLower(firstNonEmpty(strings.TrimSpace(spec.FunderAddress), strings.TrimSpace(spec.Address), signer.Address()))
	signatureType := SignatureTypeEOA
	if spec.SignatureType != nil {
		signatureType = SignatureType(*spec.SignatureType)
	} else if !strings.EqualFold(funder, signer.Address()) {
		return TradingAccount{}, fmt.Errorf("signature_type is required when funder/address differs from the private-key signer")
	}

	credentials, credentialsPresent, err := credentialsFromSpec(spec)
	if err != nil {
		return TradingAccount{}, err
	}
	if !credentialsPresent {
		if !params.BootstrapMissingAPICredentials {
			return TradingAccount{}, fmt.Errorf("CLOB api credentials are missing; set all three values or explicitly enable credential bootstrap")
		}
		if params.CredentialBootstrapper == nil {
			return TradingAccount{}, fmt.Errorf("credential bootstrapper is required when CLOB api credentials are missing")
		}
		if signatureType == SignatureTypePolyEIP1271 {
			return TradingAccount{}, fmt.Errorf("POLY_1271 credential bootstrap is not supported by this adapter")
		}
		nonce := uint64(0)
		if spec.APINonce != nil {
			nonce = *spec.APINonce
		}
		credentials, err = params.CredentialBootstrapper.CreateOrDerive(ctx, signer, nonce)
		if err != nil {
			return TradingAccount{}, fmt.Errorf("create or derive CLOB api credentials: %w", err)
		}
	}

	account := TradingAccount{
		ExecutionAccountID: spec.ExecutionAccountID,
		FunderAddress:      funder,
		SignatureType:      signatureType,
		API:                credentials,
		Relayer: RelayerCredentials{
			Key:     strings.TrimSpace(spec.RelayerAPIKey),
			Address: strings.ToLower(strings.TrimSpace(spec.RelayerAPIKeyAddress)),
		},
		Signer: signer,
	}
	if err := account.validate(); err != nil {
		return TradingAccount{}, err
	}
	return account, nil
}

// credentialsFromSpec 从钱包配置提取并校验可选 API 凭证。
func credentialsFromSpec(spec WalletAccountSpec) (APICredentials, bool, error) {
	credentials := APICredentials{
		Key:        strings.TrimSpace(spec.APIKey),
		Secret:     strings.TrimSpace(spec.APISecret),
		Passphrase: strings.TrimSpace(spec.APIPassphrase),
	}
	present := 0
	for _, value := range []string{credentials.Key, credentials.Secret, credentials.Passphrase} {
		if value != "" {
			present++
		}
	}
	if present != 0 && present != 3 {
		return APICredentials{}, false, fmt.Errorf("api_key, api_secret, and api_passphrase must be provided together")
	}
	return credentials, present == 3, nil
}

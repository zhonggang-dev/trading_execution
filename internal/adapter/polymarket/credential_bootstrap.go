package polymarket

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	clobAuthDomainName    = "ClobAuthDomain"
	clobAuthDomainVersion = "1"
	clobAuthMessage       = "This message attests that I control the given wallet"
	maxAuthResponseBytes  = 1 << 20
)

// CredentialBootstrapParams 表示后端使用的 CredentialBootstrapParams 类型。
type CredentialBootstrapParams struct {
	BaseURL    string
	HTTPClient *http.Client
	Timeout    time.Duration
	Now        func() time.Time
}

// CredentialBootstrapper implements Polymarket L1 authentication. It is kept
// separate from TradingClient because creating an API key changes remote
// credential state and must be explicitly enabled by the operator.
type CredentialBootstrapper struct {
	baseURL    *url.URL
	httpClient *http.Client
	timeout    time.Duration
	now        func() time.Time
}

// NewCredentialBootstrapper 创建并初始化 Credential Bootstrapper。
func NewCredentialBootstrapper(params CredentialBootstrapParams) (*CredentialBootstrapper, error) {
	baseURL, err := url.Parse(strings.TrimSpace(params.BaseURL))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("Polymarket credential base URL is invalid")
	}
	if params.Timeout == 0 {
		params.Timeout = 5 * time.Second
	}
	if params.Timeout < 500*time.Millisecond || params.Timeout > 30*time.Second {
		return nil, fmt.Errorf("Polymarket credential timeout must be between 500ms and 30s")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: params.Timeout}
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &CredentialBootstrapper{
		baseURL:    baseURL,
		httpClient: params.HTTPClient,
		timeout:    params.Timeout,
		now:        params.Now,
	}, nil
}

// CreateOrDerive 为签名账户创建或派生 CLOB API 凭证。
func (bootstrapper *CredentialBootstrapper) CreateOrDerive(ctx context.Context, signer DigestSigner, nonce uint64) (APICredentials, error) {
	if signer == nil {
		return APICredentials{}, fmt.Errorf("signer is required")
	}
	credentials, createErr := bootstrapper.request(ctx, signer, nonce, http.MethodPost, "/auth/api-key")
	if createErr == nil {
		return credentials, nil
	}
	if err := ctx.Err(); err != nil {
		return APICredentials{}, err
	}
	credentials, deriveErr := bootstrapper.request(ctx, signer, nonce, http.MethodGet, "/auth/derive-api-key")
	if deriveErr != nil {
		return APICredentials{}, fmt.Errorf("create failed (%v); derive failed (%w)", createErr, deriveErr)
	}
	return credentials, nil
}

// request 签署 L1 鉴权请求并创建或派生 CLOB API 凭证。
func (bootstrapper *CredentialBootstrapper) request(ctx context.Context, signer DigestSigner, nonce uint64, method, path string) (APICredentials, error) {
	timestamp := bootstrapper.now().UTC().Unix()
	digest, err := clobAuthDigest(signer.Address(), timestamp, nonce)
	if err != nil {
		return APICredentials{}, err
	}
	signature, err := signer.SignDigest(ctx, digest)
	if err != nil {
		return APICredentials{}, fmt.Errorf("sign CLOB L1 authentication: %w", err)
	}
	if len(signature) != 65 {
		return APICredentials{}, fmt.Errorf("CLOB L1 signer returned %d bytes, want 65", len(signature))
	}

	requestContext, cancel := context.WithTimeout(ctx, bootstrapper.timeout)
	defer cancel()
	endpoint := bootstrapper.baseURL.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(requestContext, method, endpoint.String(), bytes.NewReader(nil))
	if err != nil {
		return APICredentials{}, fmt.Errorf("create CLOB L1 request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("POLY_ADDRESS", strings.ToLower(signer.Address()))
	request.Header.Set("POLY_SIGNATURE", "0x"+hex.EncodeToString(signature))
	request.Header.Set("POLY_TIMESTAMP", strconv.FormatInt(timestamp, 10))
	request.Header.Set("POLY_NONCE", strconv.FormatUint(nonce, 10))

	response, err := bootstrapper.httpClient.Do(request)
	if err != nil {
		return APICredentials{}, fmt.Errorf("CLOB L1 request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes+1))
	if err != nil {
		return APICredentials{}, fmt.Errorf("read CLOB L1 response: %w", err)
	}
	if len(body) > maxAuthResponseBytes {
		return APICredentials{}, fmt.Errorf("CLOB L1 response exceeds %d bytes", maxAuthResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return APICredentials{}, fmt.Errorf("CLOB L1 HTTP %d", response.StatusCode)
	}
	var payload struct {
		APIKey     string `json:"apiKey"`
		Secret     string `json:"secret"`
		Passphrase string `json:"passphrase"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return APICredentials{}, fmt.Errorf("decode CLOB L1 response: %w", err)
	}
	credentials := APICredentials{
		Key:        strings.TrimSpace(payload.APIKey),
		Secret:     strings.TrimSpace(payload.Secret),
		Passphrase: strings.TrimSpace(payload.Passphrase),
	}
	if credentials.Key == "" || credentials.Secret == "" || credentials.Passphrase == "" {
		return APICredentials{}, fmt.Errorf("CLOB L1 response omitted credentials")
	}
	return credentials, nil
}

// clobAuthDigest 构建订单签名或鉴权所需的规范化字节数据。
func clobAuthDigest(address string, timestamp int64, nonce uint64) ([]byte, error) {
	const domainType = "EIP712Domain(string name,string version,uint256 chainId)"
	const authType = "ClobAuth(address address,string timestamp,uint256 nonce,string message)"
	if timestamp < 0 {
		return nil, fmt.Errorf("CLOB L1 timestamp must be non-negative")
	}
	addressValue, err := addressWord(address)
	if err != nil {
		return nil, err
	}
	domainSeparator := keccak256(concatWords(
		keccak256([]byte(domainType)),
		keccak256([]byte(clobAuthDomainName)),
		keccak256([]byte(clobAuthDomainVersion)),
		uint256Word(big.NewInt(polygonChainID)),
	))
	authHash := keccak256(concatWords(
		keccak256([]byte(authType)),
		addressValue,
		keccak256([]byte(strconv.FormatInt(timestamp, 10))),
		uint256Word(new(big.Int).SetUint64(nonce)),
		keccak256([]byte(clobAuthMessage)),
	))
	return keccak256([]byte{0x19, 0x01}, domainSeparator, authHash), nil
}

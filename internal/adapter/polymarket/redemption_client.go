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
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	conditionalTokensAddress     = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
	standardCollateralAdapter    = "0xada100db00ca00073811820692005400218fce1f"
	negRiskCollateralAdapter     = "0xada2005600dec949baf300f4c6120000bdb6eaab"
	defaultRelayerURL            = "https://relayer-v2.polymarket.com"
	redemptionProviderRelayer    = "POLYMARKET_RELAYER"
	redemptionProviderPolygonRPC = "POLYGON_RPC"
	depositWalletBatchType       = "Batch(address wallet,uint256 nonce,uint256 deadline,Call[] calls)Call(address target,uint256 value,bytes data)"
	depositWalletCallType        = "Call(address target,uint256 value,bytes data)"
	redemptionResponseLimit      = int64(1 << 20)
)

type RedemptionClientParams struct {
	RelayerURL            string
	RPCURL                string
	HTTPClient            *http.Client
	Credentials           CredentialProvider
	RequestTimeout        time.Duration
	RequiredConfirmations uint64
	Now                   func() time.Time
}

type RedemptionClient struct {
	relayerURL            *url.URL
	rpcURL                *url.URL
	httpClient            *http.Client
	credentials           CredentialProvider
	timeout               time.Duration
	requiredConfirmations uint64
	now                   func() time.Time
	mu                    sync.Mutex
	nextID                uint64
}

var _ port.RedemptionVenue = (*RedemptionClient)(nil)

type redemptionCall struct {
	Target string `json:"target"`
	Value  string `json:"value"`
	Data   string `json:"data"`
}

func NewRedemptionClient(params RedemptionClientParams) (*RedemptionClient, error) {
	if params.Credentials == nil {
		return nil, fmt.Errorf("redemption credential provider is required")
	}
	relayerRaw := strings.TrimSpace(params.RelayerURL)
	if relayerRaw == "" {
		relayerRaw = defaultRelayerURL
	}
	relayerURL, err := secureRedemptionURL(relayerRaw, "Polymarket relayer")
	if err != nil {
		return nil, err
	}
	rpcURL, err := secureRedemptionURL(params.RPCURL, "Polygon RPC")
	if err != nil {
		return nil, err
	}
	if params.RequestTimeout <= 0 {
		params.RequestTimeout = 10 * time.Second
	}
	if params.RequiredConfirmations == 0 || params.RequiredConfirmations > 10_000 {
		return nil, fmt.Errorf("redemption confirmations must be between 1 and 10000")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: params.RequestTimeout}
	}
	if params.Now == nil {
		params.Now = func() time.Time { return time.Now().UTC() }
	}
	return &RedemptionClient{
		relayerURL: relayerURL, rpcURL: rpcURL, httpClient: params.HTTPClient,
		credentials: params.Credentials, timeout: params.RequestTimeout,
		requiredConfirmations: params.RequiredConfirmations, now: params.Now,
	}, nil
}

func secureRedemptionURL(raw, label string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("%s URL must be an absolute HTTPS URL without userinfo or fragment", label)
	}
	return parsed, nil
}

func redemptionAdapter(negRisk bool) string {
	if negRisk {
		return negRiskCollateralAdapter
	}
	return standardCollateralAdapter
}

func (client *RedemptionClient) RedemptionApproved(ctx context.Context, walletAddress string, negRisk bool) (bool, error) {
	wallet, ok := decodeAddress(walletAddress)
	if !ok {
		return false, fmt.Errorf("redemption wallet address is invalid")
	}
	operator, _ := decodeAddress(redemptionAdapter(negRisk))
	data := make([]byte, 4+64)
	copy(data[:4], []byte{0xe9, 0x85, 0xe9, 0xc5})
	copy(data[4+12:4+32], wallet)
	copy(data[4+32+12:], operator)
	result, err := client.rpc(ctx, "eth_call", []any{
		map[string]string{"to": conditionalTokensAddress, "data": "0x" + hex.EncodeToString(data)}, "latest",
	})
	if err != nil {
		return false, fmt.Errorf("read redemption adapter approval: %w", err)
	}
	var encoded string
	if json.Unmarshal(result, &encoded) != nil {
		return false, fmt.Errorf("redemption adapter approval response is invalid")
	}
	decoded, err := decodeFixedHex(encoded, 32, "redemption adapter approval")
	if err != nil {
		return false, err
	}
	for _, value := range decoded[:31] {
		if value != 0 {
			return false, fmt.Errorf("redemption adapter approval is not a canonical bool")
		}
	}
	if decoded[31] > 1 {
		return false, fmt.Errorf("redemption adapter approval is not a canonical bool")
	}
	return decoded[31] == 1, nil
}

func (client *RedemptionClient) SubmitRedemptionApproval(
	ctx context.Context, executionAccountID string, negRisk bool,
) (domain.RedemptionSubmission, error) {
	call, err := setApprovalForAllCall(redemptionAdapter(negRisk))
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	return client.dispatch(ctx, executionAccountID, []redemptionCall{call}, "Approve exact redemption adapter")
}

func (client *RedemptionClient) SubmitRedemption(
	ctx context.Context, executionAccountID, conditionID string, negRisk bool,
) (domain.RedemptionSubmission, error) {
	call, err := redeemPositionsCall(redemptionAdapter(negRisk), conditionID)
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	return client.dispatch(ctx, executionAccountID, []redemptionCall{call}, "Redeem condition "+strings.ToLower(conditionID))
}

func (client *RedemptionClient) dispatch(
	ctx context.Context, executionAccountID string, calls []redemptionCall, metadata string,
) (domain.RedemptionSubmission, error) {
	account, err := client.credentials.Account(ctx, strings.TrimSpace(executionAccountID))
	if err != nil {
		return domain.RedemptionSubmission{}, fmt.Errorf("select redemption account: %w", err)
	}
	switch account.SignatureType {
	case SignatureTypeEOA:
		if len(calls) != 1 {
			return domain.RedemptionSubmission{}, fmt.Errorf("EOA redemption requires exactly one call")
		}
		return client.submitEOACall(ctx, account, calls[0])
	case SignatureTypePolyEIP1271:
		return client.submitDepositWallet(ctx, account, calls, metadata)
	default:
		return domain.RedemptionSubmission{}, fmt.Errorf("redemption does not support signature type %d", account.SignatureType)
	}
}

func (client *RedemptionClient) submitDepositWallet(
	ctx context.Context, account TradingAccount, calls []redemptionCall, metadata string,
) (domain.RedemptionSubmission, error) {
	if strings.TrimSpace(account.Relayer.Key) == "" || strings.TrimSpace(account.Relayer.Address) == "" {
		return domain.RedemptionSubmission{}, fmt.Errorf("execution account %s requires relayer_api_key and relayer_api_key_address for Deposit Wallet redemption", account.ExecutionAccountID)
	}
	query := url.Values{"address": []string{account.Signer.Address()}, "type": []string{"WALLET"}}
	var params struct {
		Address string `json:"address"`
		Nonce   string `json:"nonce"`
	}
	if err := client.relayerJSON(ctx, account, http.MethodGet, "/v1/account/transactions/params", query, nil, &params); err != nil {
		return domain.RedemptionSubmission{}, fmt.Errorf("fetch Deposit Wallet nonce: %w", err)
	}
	nonce, err := parseUint256(params.Nonce)
	if err != nil {
		return domain.RedemptionSubmission{}, fmt.Errorf("relayer nonce: %w", err)
	}
	deadline := big.NewInt(client.now().UTC().Unix() + 600)
	digest, err := depositWalletBatchDigest(account.FunderAddress, nonce, deadline, calls)
	if err != nil {
		return domain.RedemptionSubmission{}, err
	}
	signature, err := account.Signer.SignDigest(ctx, digest)
	if err != nil {
		return domain.RedemptionSubmission{}, fmt.Errorf("sign Deposit Wallet redemption: %w", err)
	}
	if len(signature) != 65 || (signature[64] != 27 && signature[64] != 28) {
		return domain.RedemptionSubmission{}, fmt.Errorf("Deposit Wallet signer returned an invalid Ethereum signature")
	}
	payload := map[string]any{
		"type": "WALLET", "from": account.Signer.Address(), "to": depositWalletFactory,
		"nonce": nonce.String(), "signature": "0x" + hex.EncodeToString(signature), "metadata": metadata,
		"depositWalletParams": map[string]any{
			"depositWallet": account.FunderAddress, "deadline": deadline.String(), "calls": calls,
		},
	}
	var response struct {
		State           string `json:"state"`
		TransactionHash string `json:"transactionHash"`
		TransactionID   string `json:"transactionID"`
	}
	if err := client.relayerJSON(ctx, account, http.MethodPost, "/submit", nil, payload, &response); err != nil {
		return domain.RedemptionSubmission{}, fmt.Errorf("submit Deposit Wallet redemption: %w", err)
	}
	if strings.TrimSpace(response.TransactionID) == "" {
		return domain.RedemptionSubmission{}, fmt.Errorf("relayer response has no transaction id")
	}
	transactionHash := normalizeOptionalHash(response.TransactionHash)
	return domain.RedemptionSubmission{
		Provider: redemptionProviderRelayer, Reference: response.TransactionID,
		TransactionHash: transactionHash, State: relayerSubmissionState(response.State),
	}, nil
}

func (client *RedemptionClient) ResolveRedemptionSubmission(
	ctx context.Context, executionAccountID string, submission domain.RedemptionSubmission,
) (domain.RedemptionSubmission, error) {
	switch submission.Provider {
	case redemptionProviderRelayer:
		account, err := client.credentials.Account(ctx, strings.TrimSpace(executionAccountID))
		if err != nil {
			return domain.RedemptionSubmission{}, err
		}
		var response struct {
			State           string `json:"state"`
			TransactionHash string `json:"transaction_hash"`
			TransactionID   string `json:"transaction_id"`
			ErrorMessage    string `json:"error_msg"`
		}
		path := "/v1/account/transactions/" + url.PathEscape(submission.Reference)
		if err := client.relayerJSON(ctx, account, http.MethodGet, path, nil, nil, &response); err != nil {
			return domain.RedemptionSubmission{}, err
		}
		if response.TransactionID != submission.Reference {
			return domain.RedemptionSubmission{}, fmt.Errorf("relayer transaction identity changed")
		}
		return domain.RedemptionSubmission{
			Provider: submission.Provider, Reference: submission.Reference,
			TransactionHash: normalizeOptionalHash(response.TransactionHash),
			State:           relayerSubmissionState(response.State), FailureReason: strings.TrimSpace(response.ErrorMessage),
		}, nil
	case redemptionProviderPolygonRPC:
		return client.resolveEOASubmission(ctx, submission)
	default:
		return domain.RedemptionSubmission{}, fmt.Errorf("unknown redemption submission provider %q", submission.Provider)
	}
}

func relayerSubmissionState(state string) domain.RedemptionSubmissionState {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "STATE_CONFIRMED":
		return domain.RedemptionSubmissionConfirmed
	case "STATE_FAILED", "STATE_INVALID":
		return domain.RedemptionSubmissionFailed
	default:
		return domain.RedemptionSubmissionPending
	}
}

func setApprovalForAllCall(operator string) (redemptionCall, error) {
	operatorWord, err := addressWord(operator)
	if err != nil {
		return redemptionCall{}, err
	}
	data := append([]byte{0xa2, 0x2c, 0xb4, 0x65}, operatorWord...)
	data = append(data, uint256Word(big.NewInt(1))...)
	return redemptionCall{Target: conditionalTokensAddress, Value: "0", Data: "0x" + hex.EncodeToString(data)}, nil
}

func redeemPositionsCall(adapter, conditionID string) (redemptionCall, error) {
	condition, err := bytes32Word(strings.ToLower(strings.TrimSpace(conditionID)))
	if err != nil {
		return redemptionCall{}, fmt.Errorf("condition id: %w", err)
	}
	collateral, err := addressWord(PUSDAddress)
	if err != nil {
		return redemptionCall{}, err
	}
	selector := keccak256([]byte("redeemPositions(address,bytes32,bytes32,uint256[])"))[:4]
	data := concatWords(
		collateral, make([]byte, 32), condition, uint256Word(big.NewInt(128)),
		uint256Word(big.NewInt(2)), uint256Word(big.NewInt(1)), uint256Word(big.NewInt(2)),
	)
	return redemptionCall{Target: adapter, Value: "0", Data: "0x" + hex.EncodeToString(append(selector, data...))}, nil
}

func depositWalletBatchDigest(wallet string, nonce, deadline *big.Int, calls []redemptionCall) ([]byte, error) {
	if nonce == nil || deadline == nil || nonce.Sign() < 0 || deadline.Sign() <= 0 || len(calls) == 0 {
		return nil, fmt.Errorf("Deposit Wallet batch identity is invalid")
	}
	walletWord, err := addressWord(wallet)
	if err != nil {
		return nil, err
	}
	domain := keccak256(concatWords(
		keccak256([]byte(eip712DomainType)), keccak256([]byte(depositWalletDomainName)),
		keccak256([]byte(depositWalletDomainVersion)), uint256Word(big.NewInt(polygonChainID)), walletWord,
	))
	callHashes := make([]byte, 0, len(calls)*32)
	for _, call := range calls {
		target, targetErr := addressWord(call.Target)
		if targetErr != nil {
			return nil, targetErr
		}
		value, valueErr := parseUint256(call.Value)
		if valueErr != nil {
			return nil, valueErr
		}
		data, dataErr := decodeVariableHex(call.Data, "Deposit Wallet call data")
		if dataErr != nil {
			return nil, dataErr
		}
		callHashes = append(callHashes, keccak256(concatWords(
			keccak256([]byte(depositWalletCallType)), target, uint256Word(value), keccak256(data),
		))...)
	}
	batch := keccak256(concatWords(
		keccak256([]byte(depositWalletBatchType)), walletWord, uint256Word(nonce),
		uint256Word(deadline), keccak256(callHashes),
	))
	return keccak256([]byte{0x19, 0x01}, domain, batch), nil
}

func (client *RedemptionClient) relayerJSON(
	ctx context.Context, account TradingAccount, method, path string, query url.Values, payload any, output any,
) error {
	endpoint := client.relayerURL.ResolveReference(&url.URL{Path: path})
	endpoint.RawQuery = query.Encode()
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("RELAYER_API_KEY", account.Relayer.Key)
	request.Header.Set("RELAYER_API_KEY_ADDRESS", account.Relayer.Address)
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, redemptionResponseLimit+1))
	if err != nil {
		return err
	}
	if int64(len(responseBody)) > redemptionResponseLimit {
		return fmt.Errorf("relayer response is too large")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("relayer HTTP %d: %s", response.StatusCode, compactError(responseBody))
	}
	if output != nil && json.Unmarshal(responseBody, output) != nil {
		return fmt.Errorf("relayer response is invalid JSON")
	}
	return nil
}

func (client *RedemptionClient) rpc(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	client.mu.Lock()
	client.nextID++
	id := client.nextID
	client.mu.Unlock()
	payload, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, client.rpcURL.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, redemptionResponseLimit+1))
	if err != nil || int64(len(body)) > redemptionResponseLimit {
		return nil, fmt.Errorf("read %s response", method)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s HTTP %d", method, response.StatusCode)
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      uint64          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil || envelope.JSONRPC != "2.0" || envelope.ID != id {
		return nil, fmt.Errorf("%s response envelope is invalid", method)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("%s RPC error %d: %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 {
		return nil, fmt.Errorf("%s response has no result", method)
	}
	return envelope.Result, nil
}

func decodeFixedHex(value string, size int, label string) ([]byte, error) {
	raw := strings.TrimPrefix(strings.TrimSpace(value), "0x")
	if len(raw) != size*2 {
		return nil, fmt.Errorf("%s must contain %d bytes", label, size)
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%s is not hexadecimal", label)
	}
	return decoded, nil
}

func decodeVariableHex(value, label string) ([]byte, error) {
	raw := strings.TrimPrefix(strings.TrimSpace(value), "0x")
	if len(raw)%2 != 0 {
		return nil, fmt.Errorf("%s has odd length", label)
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%s is not hexadecimal", label)
	}
	return decoded, nil
}

func normalizeOptionalHash(value string) string {
	raw := strings.ToLower(strings.TrimSpace(value))
	if raw == "" || raw == "0x" {
		return ""
	}
	if _, err := decodeFixedHex(raw, 32, "transaction hash"); err != nil {
		return ""
	}
	return "0x" + strings.TrimPrefix(raw, "0x")
}

func compactError(body []byte) string {
	value := strings.Join(strings.Fields(string(body)), " ")
	if len(value) > 300 {
		value = value[:300]
	}
	return value
}

func parseRPCQuantity(value, label string) (*big.Int, error) {
	if !strings.HasPrefix(value, "0x") || value == "0x" || (len(value) > 3 && value[2] == '0') {
		return nil, fmt.Errorf("%s is not a canonical quantity", label)
	}
	parsed, ok := new(big.Int).SetString(value[2:], 16)
	if !ok || parsed.Sign() < 0 || parsed.BitLen() > 256 {
		return nil, fmt.Errorf("%s is invalid", label)
	}
	return parsed, nil
}

func parseRPCUint64(value, label string) (uint64, error) {
	parsed, err := parseRPCQuantity(value, label)
	if err != nil || !parsed.IsUint64() {
		return 0, fmt.Errorf("%s is not uint64", label)
	}
	return parsed.Uint64(), nil
}

func jsonRPCString(result json.RawMessage, label string) (string, error) {
	var value string
	if json.Unmarshal(result, &value) != nil || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s response is invalid", label)
	}
	return value, nil
}

func parseReferenceID(value string) error {
	if strings.TrimSpace(value) == "" || len(value) > 256 {
		return fmt.Errorf("submission reference is invalid")
	}
	if _, err := strconv.Unquote(strconv.Quote(value)); err != nil {
		return fmt.Errorf("submission reference is invalid")
	}
	return nil
}

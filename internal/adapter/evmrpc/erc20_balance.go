package evmrpc

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
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// ERC20BalanceParams 表示后端使用的 ERC20BalanceParams 类型。
type ERC20BalanceParams struct {
	RPCURL       string
	TokenAddress string
	Asset        string
	Decimals     int
	HTTPClient   *http.Client
	Now          func() time.Time
}

// ERC20BalanceClient 表示后端使用的 ERC20BalanceClient 类型。
type ERC20BalanceClient struct {
	rpcURL       *url.URL
	tokenAddress string
	asset        string
	decimals     int
	httpClient   *http.Client
	now          func() time.Time
}

var _ port.ExternalBalanceSource = (*ERC20BalanceClient)(nil)

// NewERC20BalanceClient 创建并初始化 ERC 20 Balance Client。
func NewERC20BalanceClient(params ERC20BalanceParams) (*ERC20BalanceClient, error) {
	rpcURL, err := url.Parse(strings.TrimSpace(params.RPCURL))
	if err != nil || rpcURL.Scheme == "" || rpcURL.Host == "" {
		return nil, fmt.Errorf("EVM RPC URL is invalid")
	}
	token, err := normalizedAddress(params.TokenAddress)
	if err != nil {
		return nil, fmt.Errorf("ERC20 token address: %w", err)
	}
	params.Asset = strings.TrimSpace(params.Asset)
	if params.Asset == "" || params.Decimals < 0 || params.Decimals > 36 {
		return nil, fmt.Errorf("ERC20 asset and decimals are invalid")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &ERC20BalanceClient{
		rpcURL: rpcURL, tokenAddress: token, asset: params.Asset, decimals: params.Decimals,
		httpClient: params.HTTPClient, now: params.Now,
	}, nil
}

// GetExternalBalance 查询指定钱包资产的外部链上余额。
func (client *ERC20BalanceClient) GetExternalBalance(
	ctx context.Context,
	walletAddress, asset string,
) (domain.ExternalBalance, error) {
	wallet, err := normalizedAddress(walletAddress)
	if err != nil {
		return domain.ExternalBalance{}, fmt.Errorf("wallet address: %w", err)
	}
	if strings.TrimSpace(asset) != client.asset {
		return domain.ExternalBalance{}, fmt.Errorf("balance source serves %s, not %s", client.asset, asset)
	}
	// balanceOf(address) selector + one left-padded address argument.
	callData := "0x70a08231" + strings.Repeat("0", 24) + strings.TrimPrefix(wallet, "0x")
	payload, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "method": "eth_call", "id": 1,
		"params": []any{map[string]string{"to": client.tokenAddress, "data": callData}, "latest"},
	})
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.rpcURL.String(), bytes.NewReader(payload))
		if err != nil {
			return domain.ExternalBalance{}, err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.httpClient.Do(request)
		if err != nil {
			lastErr = err
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("RPC HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
			continue
		}
		var result struct {
			Result string `json:"result"`
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &result); err != nil || result.Error != nil || !strings.HasPrefix(result.Result, "0x") {
			if result.Error != nil {
				lastErr = fmt.Errorf("RPC error %d: %s", result.Error.Code, result.Error.Message)
			} else {
				lastErr = fmt.Errorf("invalid RPC balance response")
			}
			continue
		}
		units := new(big.Int)
		if _, ok := units.SetString(strings.TrimPrefix(result.Result, "0x"), 16); !ok {
			lastErr = fmt.Errorf("RPC balance is not hexadecimal")
			continue
		}
		return domain.ExternalBalance{
			Asset: client.asset, Amount: domain.Decimal(formatUnits(units, client.decimals)),
			Source: "EVM_ERC20_ETH_CALL", ObservedAt: client.now().UTC(),
		}, nil
	}
	return domain.ExternalBalance{}, fmt.Errorf("read on-chain ERC20 balance after 3 attempts: %w", lastErr)
}

// normalizedAddress 规范化 d Address 的字段和表示。
func normalizedAddress(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	raw := strings.TrimPrefix(value, "0x")
	if len(raw) != 40 {
		return "", fmt.Errorf("address must contain 20 bytes")
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return "", fmt.Errorf("address is not hexadecimal")
	}
	return "0x" + raw, nil
}

// formatUnits 格式化 Units。
func formatUnits(value *big.Int, decimals int) string {
	if decimals == 0 {
		return value.String()
	}
	digits := value.String()
	for len(digits) <= decimals {
		digits = "0" + digits
	}
	whole := digits[:len(digits)-decimals]
	fraction := strings.TrimRight(digits[len(digits)-decimals:], "0")
	if fraction == "" {
		return whole
	}
	return whole + "." + fraction
}

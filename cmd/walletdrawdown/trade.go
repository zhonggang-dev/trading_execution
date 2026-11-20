package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketgamma"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/thinmarket"
	"github.com/UniPat-AI/trading_execution/internal/service/walletdrawdown"
)

func loadTrader(getenv func(string) string, publicClient *http.Client) (*walletdrawdown.Trader, error) {
	baseURL := strings.TrimSpace(getenv("WALLET_DRAWDOWN_EXECUTION_URL"))
	if baseURL == "" {
		return nil, nil
	}
	endpoint, err := url.Parse(baseURL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Port() == "" ||
		!net.ParseIP(endpoint.Hostname()).IsLoopback() || endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return nil, fmt.Errorf("WALLET_DRAWDOWN_EXECUTION_URL must be a loopback HTTP origin with a port")
	}
	token := strings.TrimSpace(getenv("EXECUTION_API_TOKEN"))
	if token == "" {
		return nil, fmt.Errorf("EXECUTION_API_TOKEN is required for drawdown trading")
	}
	endpoint.Path = "/api/v1/orders"

	gammaURL, clobURL := marketDataURLs(getenv)
	markets, err := polymarketgamma.New(polymarketgamma.Params{
		BaseURL: gammaURL, HTTPClient: publicClient, UserAgent: "trading-execution-walletdrawdown",
	})
	if err != nil {
		return nil, err
	}
	books, err := polymarket.NewOrderBookSource(polymarket.OrderBookParams{
		BaseURL: clobURL, HTTPClient: publicClient, Workers: 4, BatchSize: 200, Depth: 2,
	})
	if err != nil {
		return nil, err
	}
	finder, err := thinmarket.New(markets, books)
	if err != nil {
		return nil, err
	}
	// The execution server can spend up to 60s submitting and persisting a
	// venue attempt. The command's overall context still bounds the call.
	executionClient := *publicClient
	executionClient.Timeout = 65 * time.Second
	return &walletdrawdown.Trader{
		Finder: finder,
		Books:  books,
		Submit: func(ctx context.Context, intent domain.OrderIntent) (domain.Order, error) {
			return submitOrder(ctx, &executionClient, endpoint.String(), token, intent)
		},
	}, nil
}

func marketDataURLs(getenv func(string) string) (gammaURL, clobURL string) {
	gammaURL = strings.TrimSpace(getenv("POLYMARKET_GAMMA_URL"))
	if gammaURL == "" {
		gammaURL = "https://gamma-api.polymarket.com"
	}
	clobURL = strings.TrimSpace(getenv("POLYMARKET_CLOB_URL"))
	if clobURL == "" {
		clobURL = "https://clob.polymarket.com"
	}
	return gammaURL, clobURL
}

func submitOrder(ctx context.Context, client *http.Client, endpoint, token string, intent domain.OrderIntent) (domain.Order, error) {
	body, err := json.Marshal(intent)
	if err != nil {
		return domain.Order{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return domain.Order{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return domain.Order{}, err
	}
	defer resp.Body.Close()
	var response struct {
		Data  domain.Order `json:"data"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	const maxResponseSize = 64 << 10
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return domain.Order{}, err
	}
	if len(raw) > maxResponseSize || json.Unmarshal(raw, &response) != nil {
		return domain.Order{}, fmt.Errorf("invalid execution response (HTTP %d)", resp.StatusCode)
	}
	order := response.Data
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusUnprocessableEntity:
		if order.ID != "" && order.Intent.ClientOrderID == intent.ClientOrderID && order.Intent.ExecutionAccountID == intent.ExecutionAccountID {
			return order, nil
		}
	}
	return domain.Order{}, fmt.Errorf("execution response did not confirm this order (HTTP %d, code %q)", resp.StatusCode, response.Error.Code)
}

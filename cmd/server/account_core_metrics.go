package main

import (
	"net/http"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/accountquotes"
	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/service/accountcoremetrics"
	"github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

// This constructor creates only read dependencies: it never probes capabilities,
// synchronizes funds, starts a background loop, or builds an execution route.
func buildAccountCoreMetrics(reader *consolev2.Reader, cfg config.Config, history ...accountpricehistory.Reader) *accountcoremetrics.Service {
	client := &http.Client{Timeout: 2 * time.Second, Transport: accountquotes.NewLimitTransport(nil), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	source := &accountquotes.Source{CLOBURL: cfg.Polymarket.CLOBURL, GammaURL: cfg.Polymarket.GammaURL, HTTPClient: client}
	positions, err := polymarketdata.NewPositionClient(polymarketdata.PositionClientParams{BaseURL: cfg.Polymarket.DataAPIURL, HTTPClient: client})
	if err == nil {
		source.Positions = positions
	}
	key, path := cfg.Kalshi.APIKeyID, cfg.Kalshi.PrivateKeyPath
	if len(cfg.Kalshi.LiveAccounts) > 0 {
		key, path = cfg.Kalshi.LiveAccounts[0].APIKeyID, cfg.Kalshi.LiveAccounts[0].PrivateKeyPath
	}
	if key != "" && path != "" {
		k, e := kalshi.NewClient(kalshi.ClientParams{BaseURL: cfg.Kalshi.APIURL, APIKeyID: key, PrivateKeyPath: path, HTTPClient: client, LiveTradingEnabled: false})
		if e == nil {
			source.Kalshi = k
		}
	}
	return accountcoremetrics.New(reader, source, history...)
}

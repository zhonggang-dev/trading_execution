package main

import (
	"fmt"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/kalshi"
	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/service/kalshipnl"
)

// Official PnL is optional and has no startup venue calls, trading gates or
// database writes. The official host is fixed, independent of trading API URLs.
func buildKalshiPnLService(cfg config.Config, live *liveRuntime) (*kalshipnl.Service, error) {
	if live == nil {
		return nil, nil
	}
	managed := make(map[string]bool, len(live.kalshiAccounts))
	for _, id := range live.kalshiAccounts {
		managed[id] = true
	}
	sources := make(map[string]kalshipnl.Source)
	for _, account := range cfg.Kalshi.LiveAccounts {
		if !managed[account.ExecutionAccountID] || account.OfficialUserID == "" {
			continue
		}
		client, err := kalshi.NewClient(kalshi.ClientParams{BaseURL: "https://api.elections.kalshi.com", APIKeyID: account.APIKeyID, PrivateKeyPath: account.PrivateKeyPath, HTTPClient: noRedirectHTTPClient(5 * time.Second)})
		if err != nil {
			return nil, fmt.Errorf("configure optional Kalshi PnL client")
		}
		source, err := kalshi.NewPnLSource(client, account.OfficialUserID)
		if err != nil {
			return nil, fmt.Errorf("configure optional Kalshi PnL identity")
		}
		sources[account.ExecutionAccountID] = source
	}
	return kalshipnl.New(kalshipnl.Params{Sources: sources})
}

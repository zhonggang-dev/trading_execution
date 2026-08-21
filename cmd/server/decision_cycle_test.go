package main

import (
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestValidateDecisionAccountsRejectsBindingOutsideWalletFile(t *testing.T) {
	cycleConfig := config.DecisionCycle{Bindings: []domain.StrategyExecutionBinding{{
		ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-missing",
	}}}
	if err := validateDecisionAccounts(cycleConfig, []string{"account-present"}); err == nil || !strings.Contains(err.Error(), "absent from the wallet file") {
		t.Fatalf("validateDecisionAccounts() error = %v", err)
	}
}

func TestValidateDecisionAccountsAcceptsFourConfiguredWallets(t *testing.T) {
	cycleConfig := config.DecisionCycle{Bindings: []domain.StrategyExecutionBinding{
		{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "main"},
		{ModelID: "echo", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-1"},
		{ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "wallet-2"},
		{ModelID: "gemini_masked", StrategyID: domain.StrategyIDMultfactorV2, ExecutionAccountID: "wallet-3"},
	}}
	if err := validateDecisionAccounts(cycleConfig, []string{"main", "wallet-1", "wallet-2", "wallet-3"}); err != nil {
		t.Fatalf("validateDecisionAccounts() error = %v", err)
	}
}

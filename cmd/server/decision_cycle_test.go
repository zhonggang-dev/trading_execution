package main

import (
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/config"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestValidateDecisionAccountsRejectsBindingOutsideWalletFile(t *testing.T) {
	cycleConfig := config.DecisionCycle{Bindings: []domain.StrategyExecutionContext{{
		ModelID: "model-a", StrategyID: domain.StrategyIDMultfactorV1, ExecutionAccountID: "account-missing",
	}}}
	if err := validateDecisionAccounts(cycleConfig, []string{"account-present"}); err == nil || !strings.Contains(err.Error(), "absent from the wallet file") {
		t.Fatalf("validateDecisionAccounts() error = %v", err)
	}
}

func TestValidateDecisionAccountsAcceptsConfiguredWallet(t *testing.T) {
	cycleConfig := config.DecisionCycle{Bindings: []domain.StrategyExecutionContext{{
		ModelID: "model-a", StrategyID: "strategy-v1", ExecutionAccountID: "account-a",
	}}}
	if err := validateDecisionAccounts(cycleConfig, []string{"account-a"}); err != nil {
		t.Fatalf("validateDecisionAccounts() error = %v", err)
	}
}

package domain

import (
	"strings"
	"testing"
)

func TestUnattributedCashIssueImpact(t *testing.T) {
	in := ReconciliationIssue{Type: ReconciliationIssueUnattributedCashIn, Status: ReconciliationIssueOpen, Resolution: ReconciliationResolutionObserved}
	if scope := ClassifyReconciliationImpact(in); scope != ReconciliationImpactNone {
		t.Fatalf("unattributed incoming cash scope = %s", scope)
	}
	// Even if an operator escalated it, unknown incoming money never blocks.
	in.Resolution = ReconciliationResolutionManual
	if scope := ClassifyReconciliationImpact(in); scope != ReconciliationImpactNone {
		t.Fatalf("escalated unattributed incoming cash scope = %s", scope)
	}
	out := ReconciliationIssue{Type: ReconciliationIssueUnattributedCashOut, Status: ReconciliationIssueOpen, Resolution: ReconciliationResolutionManual}
	if scope := ClassifyReconciliationImpact(out); scope != ReconciliationImpactAccount {
		t.Fatalf("unattributed outgoing cash scope = %s", scope)
	}
	// A market identity never narrows an unexplained outflow.
	out.TokenID = "token-1"
	if scope := ClassifyReconciliationImpact(out); scope != ReconciliationImpactAccount {
		t.Fatalf("scoped unattributed outgoing cash scope = %s", scope)
	}
	surplus := ReconciliationIssue{Type: ReconciliationIssueBalanceDrift, Status: ReconciliationIssueOpen, Resolution: ReconciliationResolutionObserved}
	if scope := ClassifyReconciliationImpact(surplus); scope != ReconciliationImpactNone {
		t.Fatalf("observed balance surplus scope = %s", scope)
	}
}

func TestChainCashTransferIdentityAndSign(t *testing.T) {
	transfer := ChainCashTransfer{
		ExecutionAccountID: "wallet-6", TransactionHash: "0x" + strings.Repeat("ab", 32), LogIndex: 7,
		BlockNumber: 94681378, BlockHash: "0x" + strings.Repeat("cd", 32), Direction: ChainCashOut,
		Counterparty: "0x" + strings.Repeat("12", 20), Amount: "0.0015",
	}
	if err := transfer.Validate(); err != nil {
		t.Fatal(err)
	}
	if transfer.SignedAmount() != "-0.0015" || transfer.Reference() != transfer.TransactionHash+":7" {
		t.Fatalf("signed=%s reference=%s", transfer.SignedAmount(), transfer.Reference())
	}
	if ChainCashAccountEventID(transfer) != "account-chain-cash:wallet-6:"+transfer.TransactionHash+":7" {
		t.Fatalf("event id = %s", ChainCashAccountEventID(transfer))
	}
	transfer.Direction = ChainCashIn
	if transfer.SignedAmount() != "0.0015" {
		t.Fatalf("signed=%s", transfer.SignedAmount())
	}
	for name, mutate := range map[string]func(*ChainCashTransfer){
		"uppercase hash":  func(value *ChainCashTransfer) { value.TransactionHash = strings.ToUpper(value.TransactionHash) },
		"short address":   func(value *ChainCashTransfer) { value.Counterparty = "0x12" },
		"zero block":      func(value *ChainCashTransfer) { value.BlockNumber = 0 },
		"no direction":    func(value *ChainCashTransfer) { value.Direction = "" },
		"zero amount":     func(value *ChainCashTransfer) { value.Amount = "0" },
		"negative amount": func(value *ChainCashTransfer) { value.Amount = "-1" },
	} {
		invalid := transfer
		mutate(&invalid)
		if err := invalid.Validate(); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestChainCashFillValidate(t *testing.T) {
	fill := ChainCashFill{
		TransactionHash: "0x" + strings.Repeat("ab", 32), LogIndex: 3, BlockNumber: 94681378,
		BlockHash: "0x" + strings.Repeat("cd", 32), Exchange: "0x" + strings.Repeat("e1", 20),
		Maker: "0x" + strings.Repeat("12", 20), Taker: "0x" + strings.Repeat("34", 20),
	}
	if err := fill.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ChainCashFill){
		"uppercase hash": func(value *ChainCashFill) { value.TransactionHash = strings.ToUpper(value.TransactionHash) },
		"no block hash":  func(value *ChainCashFill) { value.BlockHash = "" },
		"short exchange": func(value *ChainCashFill) { value.Exchange = "0x12" },
		"short maker":    func(value *ChainCashFill) { value.Maker = "0x12" },
		"zero block":     func(value *ChainCashFill) { value.BlockNumber = 0 },
	} {
		invalid := fill
		mutate(&invalid)
		if err := invalid.Validate(); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

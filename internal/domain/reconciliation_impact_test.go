package domain

import "testing"

func TestClassifyReconciliationImpact(t *testing.T) {
	cases := []struct {
		name  string
		issue ReconciliationIssue
		want  ReconciliationImpactScope
	}{
		{"resolved blocks nothing", ReconciliationIssue{Type: ReconciliationIssueBalanceDrift, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueResolved}, ReconciliationImpactNone},
		{"observed external order", ReconciliationIssue{Type: ReconciliationIssueExternalOrder, Resolution: ReconciliationResolutionObserved, Status: ReconciliationIssueOpen, TokenID: "t"}, ReconciliationImpactNone},
		{"manual balance drift", ReconciliationIssue{Type: ReconciliationIssueBalanceDrift, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen}, ReconciliationImpactAccount},
		{"bounded balance drift", ReconciliationIssue{Type: ReconciliationIssueBalanceDrift, Resolution: ReconciliationResolutionRetry, Status: ReconciliationIssueOpen}, ReconciliationImpactNone},
		{"source conflict", ReconciliationIssue{Type: ReconciliationIssueSourceConflict, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen, TokenID: "t"}, ReconciliationImpactAccount},
		{"account-level source failure", ReconciliationIssue{Type: ReconciliationIssueSourceUnavailable, Resolution: ReconciliationResolutionRetry, Status: ReconciliationIssueOpen}, ReconciliationImpactAccount},
		{"order-level source failure", ReconciliationIssue{Type: ReconciliationIssueSourceUnavailable, Resolution: ReconciliationResolutionRetry, Status: ReconciliationIssueOpen, OrderID: "o", TokenID: "t"}, ReconciliationImpactOrder},
		{"unconfirmed submit", ReconciliationIssue{Type: ReconciliationIssueSubmitUnconfirmed, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen, OrderID: "o", MarketID: "m"}, ReconciliationImpactOrder},
		{"recovery pending", ReconciliationIssue{Type: ReconciliationIssueOrderRecoveryPending, Resolution: ReconciliationResolutionRetry, Status: ReconciliationIssueOpen, OrderID: "o", TokenID: "t"}, ReconciliationImpactOrder},
		{"recovery stalled without market identity", ReconciliationIssue{Type: ReconciliationIssueOrderRecoveryStalled, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen, OrderID: "o"}, ReconciliationImpactAccount},
		{"position drift", ReconciliationIssue{Type: ReconciliationIssuePositionDrift, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen, TokenID: "t"}, ReconciliationImpactToken},
		{"phantom without token", ReconciliationIssue{Type: ReconciliationIssuePhantomPosition, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen}, ReconciliationImpactAccount},
		{"external trade", ReconciliationIssue{Type: ReconciliationIssueExternalTrade, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen, TokenID: "t"}, ReconciliationImpactToken},
		{"unknown type fails closed", ReconciliationIssue{Type: "SOMETHING_NEW", Resolution: ReconciliationResolutionRetry, Status: ReconciliationIssueOpen, TokenID: "t"}, ReconciliationImpactAccount},
	}
	for _, testCase := range cases {
		if got := ClassifyReconciliationImpact(testCase.issue); got != testCase.want {
			t.Errorf("%s: scope = %s, want %s", testCase.name, got, testCase.want)
		}
	}
}

func TestBlocksIntentMatchesTokenConditionOrMarket(t *testing.T) {
	issue := ReconciliationIssue{
		Type: ReconciliationIssueOrderRecoveryPending, Resolution: ReconciliationResolutionRetry, Status: ReconciliationIssueOpen,
		OrderID: "o", MarketID: "market-1", ConditionID: "0xABC", TokenID: "token-yes",
	}
	if !issue.BlocksIntent(OrderIntent{TokenID: "token-yes"}) {
		t.Fatal("same token must be blocked")
	}
	if !issue.BlocksIntent(OrderIntent{TokenID: "token-no", ConditionID: "0xabc"}) {
		t.Fatal("the other outcome of the same condition must be blocked")
	}
	if !issue.BlocksIntent(OrderIntent{TokenID: "token-no", MarketID: "market-1"}) {
		t.Fatal("same market must be blocked")
	}
	if issue.BlocksIntent(OrderIntent{TokenID: "token-other", ConditionID: "0xdef", MarketID: "market-2"}) {
		t.Fatal("an unrelated market must not be blocked")
	}
	accountWide := ReconciliationIssue{Type: ReconciliationIssueSourceConflict, Resolution: ReconciliationResolutionManual, Status: ReconciliationIssueOpen}
	if !accountWide.BlocksIntent(OrderIntent{TokenID: "anything"}) {
		t.Fatal("account-wide issue must block every intent")
	}
	impact := SummarizeReconciliationImpact([]ReconciliationIssue{issue, {Type: ReconciliationIssueExternalOrder, Resolution: ReconciliationResolutionObserved, Status: ReconciliationIssueOpen, TokenID: "x"}})
	if impact.AccountWide || !impact.Partial() || len(impact.TokenIDs) != 1 || impact.TokenIDs[0] != "token-yes" || len(impact.Reasons) != 0 || len(impact.ScopedReasons) != 1 {
		t.Fatalf("impact = %#v", impact)
	}
}

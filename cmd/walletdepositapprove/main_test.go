package main

import (
	"strings"
	"testing"
)

func TestFixedPlanIsExactlyTheReviewedWallet7Set(t *testing.T) {
	seen := map[string]bool{}
	for _, approval := range fixedApprovals {
		key := string(approval.Kind) + ":" + approval.Spender
		if seen[key] {
			t.Fatalf("duplicate approval %s", key)
		}
		seen[key] = true
		if approval.Spender != strings.ToLower(approval.Spender) || len(approval.Spender) != 42 {
			t.Fatalf("spender %q is not a lower-case 20-byte address", approval.Spender)
		}
	}
	for _, key := range []string{
		"PUSD_ALLOWANCE:0xd91e80cf2e7be2e162c6513ced06f1dd0da35296",
		"PUSD_ALLOWANCE:0xada2005600dec949baf300f4c6120000bdb6eaab",
		"PUSD_ALLOWANCE:0xada100db00ca00073811820692005400218fce1f",
		"CTF_OPERATOR:0xd91e80cf2e7be2e162c6513ced06f1dd0da35296",
		"CTF_OPERATOR:0xada2005600dec949baf300f4c6120000bdb6eaab",
	} {
		if !seen[key] {
			t.Fatalf("fixed plan lacks %s", key)
		}
	}
	if len(fixedApprovals) != 5 {
		t.Fatalf("fixed plan has %d approvals, want 5", len(fixedApprovals))
	}
}

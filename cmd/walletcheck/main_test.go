package main

import "testing"

func TestParseOptionsLeavesBalanceAllowanceRefreshDisabledByDefault(t *testing.T) {
	options, err := parseOptions(nil)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if options.refreshBalanceAllowance {
		t.Fatal("refreshBalanceAllowance = true, want false")
	}
}

func TestParseOptionsEnablesExplicitBalanceAllowanceRefresh(t *testing.T) {
	options, err := parseOptions([]string{"--refresh-balance-allowance"})
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if !options.refreshBalanceAllowance {
		t.Fatal("refreshBalanceAllowance = false, want true")
	}
}

func TestParseOptionsRejectsPositionalArguments(t *testing.T) {
	if _, err := parseOptions([]string{"wallet-6"}); err == nil {
		t.Fatal("parseOptions() error = nil, want positional argument rejection")
	}
}

package main

import (
	"strings"
	"testing"
)

func TestParseOptionsDefaultsToDryRun(t *testing.T) {
	t.Setenv("POLYMARKET_ACCOUNTS_FILE", "/private/accounts.json")
	t.Setenv("POLYGON_RPC_URL", "https://polygon.example.invalid")
	t.Setenv("WALLET_APPROVE_JOURNAL_FILE", "")
	options, err := parseOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if options.execute() || options.journalFile != "" {
		t.Fatalf("options = %#v, want journal-free dry-run", options)
	}
}

func TestParseOptionsRequiresExactExecuteTokenAndJournal(t *testing.T) {
	t.Setenv("POLYMARKET_ACCOUNTS_FILE", "/private/accounts.json")
	t.Setenv("POLYGON_RPC_URL", "https://polygon.example.invalid")
	if _, err := parseOptions([]string{"--execute-token", "yes"}); err == nil || !strings.Contains(err.Error(), "exactly match") {
		t.Fatalf("wrong-token error = %v", err)
	}
	if _, err := parseOptions([]string{"--execute-token", exactExecuteToken}); err == nil || !strings.Contains(err.Error(), "journal") {
		t.Fatalf("missing-journal error = %v", err)
	}
	options, err := parseOptions([]string{
		"--execute-token", exactExecuteToken,
		"--journal-file", "/secure/walletapprove.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !options.execute() {
		t.Fatal("exact execute token did not enable execute mode")
	}
}

func TestParseOptionsRejectsPositionalArguments(t *testing.T) {
	t.Setenv("POLYMARKET_ACCOUNTS_FILE", "/private/accounts.json")
	t.Setenv("POLYGON_RPC_URL", "https://polygon.example.invalid")
	if _, err := parseOptions([]string{"wallet-6"}); err == nil {
		t.Fatal("parseOptions() accepted positional account scope")
	}
}

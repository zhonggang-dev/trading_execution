package main

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestParseOptionsDefaultsToDryRunAndRequiresExactScope(t *testing.T) {
	options, err := parseOptions([]string{"--account", "kalshi:wallet-7", "--order", "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	if options.apply || options.account != "kalshi:wallet-7" || options.order != "order-1" || options.finalityGrace != 30*time.Second {
		t.Fatalf("options=%#v", options)
	}
	for _, arguments := range [][]string{
		{"--account", "kalshi:wallet-7"},
		{"--order", "order-1"},
		{"--account", "wallet-7", "--order", "order-1"},
		{"--account", "kalshi:wallet-7", "--order", "order-1", "extra"},
		{"--account", "kalshi:wallet-7", "--order", "order-1", "--apply"},
		{"--account", "kalshi:wallet-7", "--order", "order-1", "--apply", "--confirm", "kalshi:wallet-7/other"},
		{"--account", "kalshi:wallet-7", "--order", "order-1", "--finality-grace", "29s"},
	} {
		if _, err := parseOptions(arguments); err == nil {
			t.Fatalf("parseOptions(%q) error=nil", arguments)
		}
	}
	options, err = parseOptions([]string{
		"--account", "kalshi:wallet-7", "--order", "order-1", "--apply", "--confirm", "kalshi:wallet-7/order-1",
	})
	if err != nil || !options.apply {
		t.Fatalf("apply options=%#v error=%v", options, err)
	}
}

func TestRepairHTTPClientNeverFollowsRedirects(t *testing.T) {
	client := repairHTTPClient(5 * time.Second)
	if client.Timeout != 5*time.Second || client.CheckRedirect == nil {
		t.Fatalf("client=%#v", client)
	}
	if err := client.CheckRedirect(&http.Request{}, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("redirect error=%v", err)
	}
}

func TestValidateKalshiAPIURLRequiresHTTPSOfficialHost(t *testing.T) {
	for _, value := range []string{
		"https://api.elections.kalshi.com/trade-api/v2",
		"https://external-api.kalshi.com/trade-api/v2",
		"https://demo-api.kalshi.co/trade-api/v2",
	} {
		if err := validateKalshiAPIURL(value, false); err != nil {
			t.Fatalf("validateKalshiAPIURL(%q) error=%v", value, err)
		}
	}
	if err := validateKalshiAPIURL("https://external-api.kalshi.com/trade-api/v2", true); err != nil {
		t.Fatalf("production apply URL rejected: %v", err)
	}
	for _, value := range []string{
		"https://api.elections.kalshi.com/trade-api/v2",
		"https://demo-api.kalshi.co/trade-api/v2",
	} {
		if err := validateKalshiAPIURL(value, true); err == nil {
			t.Fatalf("validateKalshiAPIURL(%q, apply=true) error=nil", value)
		}
	}
	for _, value := range []string{
		"http://api.elections.kalshi.com/trade-api/v2",
		"https://evil.example/trade-api/v2",
		"https://user@example.com/trade-api/v2",
		"https://external-api.kalshi.com:444/trade-api/v2",
		"https://external-api.kalshi.com/trade-api/v2?redirect=evil",
		"https://external-api.kalshi.com/trade-api/v2#fragment",
		"not-a-url",
	} {
		if err := validateKalshiAPIURL(value, false); err == nil {
			t.Fatalf("validateKalshiAPIURL(%q) error=nil", value)
		}
	}
}

func TestKalshiLogicalAccountIDIsExplicitAndUnambiguous(t *testing.T) {
	if got, err := kalshiLogicalAccountID("kalshi:wallet-7"); err != nil || got != "wallet-7" {
		t.Fatalf("logical account=%q error=%v", got, err)
	}
	for _, value := range []string{"wallet-7", "kalshi:", "kalshi:kalshi:wallet-7"} {
		if _, err := kalshiLogicalAccountID(value); err == nil {
			t.Fatalf("kalshiLogicalAccountID(%q) error=nil", value)
		}
	}
}

func TestSelectBindingRequiresOneCompleteExactAccount(t *testing.T) {
	encoded := `[
		{"execution_account_id":"wallet-6","api_key_id":"key-6","private_key_path":"/run/key-6.pem"},
		{"execution_account_id":"wallet-7","api_key_id":"key-7","private_key_path":"/run/key-7.pem"}
	]`
	selected, err := selectBinding(encoded, "wallet-7")
	if err != nil || selected.APIKeyID != "key-7" || selected.PrivateKeyPath != "/run/key-7.pem" {
		t.Fatalf("selected=%#v error=%v", selected, err)
	}
	if _, err := selectBinding(encoded, "wallet-8"); err == nil {
		t.Fatal("missing exact account binding must fail")
	}
	if _, err := selectBinding(`[
		{"execution_account_id":"wallet-7","api_key_id":"one","private_key_path":"/run/one"},
		{"execution_account_id":"wallet-7","api_key_id":"two","private_key_path":"/run/two"}
	]`, "wallet-7"); err == nil {
		t.Fatal("ambiguous account binding must fail")
	}
}

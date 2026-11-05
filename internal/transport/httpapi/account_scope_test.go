package httpapi

import (
	"net/http/httptest"
	"testing"
)

func TestConsoleV2AccountScopeValidation(t *testing.T) {
	options := consoleV2FilterOptions{Account: true}
	for _, raw := range []string{"account_scope=active", "account_scope=all", "execution_account_id=wallet-6"} {
		f, err := parseConsoleV2Filter(httptest.NewRequest("GET", "/?"+raw, nil), options)
		if err != nil {
			t.Fatal(raw, err)
		}
		if raw == "account_scope=active" && f.AccountScope != "active" {
			t.Fatal(f)
		}
	}
	for _, raw := range []string{"account_scope=", "account_scope=ACTIVE", "account_scope=unknown", "account_scope=active&account_scope=all", "execution_account_id=wallet-6&account_scope=active", "execution_account_id=main&account_scope=all"} {
		if _, err := parseConsoleV2Filter(httptest.NewRequest("GET", "/?"+raw, nil), options); err == nil {
			t.Fatal("accepted", raw)
		}
	}
	if _, err := parseConsoleV2Filter(httptest.NewRequest("GET", "/?account_scope=active", nil), consoleV2FilterOptions{}); err == nil {
		t.Fatal("non-account resource accepted scope")
	}
}

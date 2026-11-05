package config

import "testing"

func TestKalshiOfficialUserIDIsOptionalAndSeparate(t *testing.T) {
	const base = `{"execution_account_id":"kalshi:main","api_key_id":"key","private_key_path":"/key.pem"`
	a, e := decodeKalshiLiveAccounts("[" + base + "}]")
	if e != nil || a[0].OfficialUserID != "" {
		t.Fatalf("old config=%v %v", a, e)
	}
	a, e = decodeKalshiLiveAccounts("[" + base + `,"official_user_id":" 11111111-2222-3333-4444-555555555555 "}]`)
	if e != nil || a[0].OfficialUserID != "11111111-2222-3333-4444-555555555555" || a[0].APIKeyID != "key" {
		t.Fatalf("config=%v %v", a, e)
	}
}

package main

import (
	"reflect"
	"testing"
)

func TestInternalEntryDisabledAccountsPreservesLogicalGate(t *testing.T) {
	if got := internalEntryDisabledAccounts("main", "kalshi:main", []string{"main", "wallet-1"}); !reflect.DeepEqual(got, []string{"kalshi:main"}) {
		t.Fatalf("main Kalshi gate=%#v", got)
	}
	if got := internalEntryDisabledAccounts("wallet-7", "kalshi:wallet-7", []string{"main", "wallet-1"}); len(got) != 0 {
		t.Fatalf("wallet-7 Kalshi gate=%#v", got)
	}
}

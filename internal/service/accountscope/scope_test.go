package accountscope

import "testing"

func TestScopeSeparatesActiveAndManagedAccounts(t *testing.T) {
	scope, err := New(
		[]string{"main", "wallet-1"},
		[]string{"main", "wallet-1", "wallet-6", "wallet-7"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !scope.IsActive("main") || !scope.IsManaged("main") ||
		scope.IsActive("wallet-6") || !scope.IsManaged("wallet-6") ||
		scope.IsActive("wallet-2") || scope.IsManaged("wallet-2") {
		t.Fatal("scope did not preserve active/managed/retired boundaries")
	}
}

func TestScopeRejectsActiveAccountOutsideManagedSet(t *testing.T) {
	if _, err := New([]string{"wallet-2"}, []string{"main"}); err == nil {
		t.Fatal("New() error = nil, want active subset rejection")
	}
}

package accountscope

import (
	"fmt"
	"strings"
)

// Scope is an immutable live-process account allowlist.
type Scope struct {
	active  map[string]struct{}
	managed map[string]struct{}
}

// New requires at least one active account and proves it is a subset of all
// wallet-file accounts managed by this process.
func New(activeAccountIDs, managedAccountIDs []string) (*Scope, error) {
	managed, err := normalize("managed", managedAccountIDs)
	if err != nil {
		return nil, err
	}
	active, err := normalize("active", activeAccountIDs)
	if err != nil {
		return nil, err
	}
	for accountID := range active {
		if _, exists := managed[accountID]; !exists {
			return nil, fmt.Errorf("active execution account %q is not managed by this process", accountID)
		}
	}
	return &Scope{active: active, managed: managed}, nil
}

func normalize(label string, accountIDs []string) (map[string]struct{}, error) {
	if len(accountIDs) == 0 {
		return nil, fmt.Errorf("at least one %s execution account is required", label)
	}
	result := make(map[string]struct{}, len(accountIDs))
	for index, raw := range accountIDs {
		accountID := strings.TrimSpace(raw)
		if accountID == "" {
			return nil, fmt.Errorf("%s execution account %d is empty", label, index)
		}
		if _, duplicate := result[accountID]; duplicate {
			return nil, fmt.Errorf("%s execution account %q is duplicated", label, accountID)
		}
		result[accountID] = struct{}{}
	}
	return result, nil
}

func (scope *Scope) IsActive(executionAccountID string) bool {
	if scope == nil {
		return false
	}
	_, exists := scope.active[strings.TrimSpace(executionAccountID)]
	return exists
}

func (scope *Scope) IsManaged(executionAccountID string) bool {
	if scope == nil {
		return false
	}
	_, exists := scope.managed[strings.TrimSpace(executionAccountID)]
	return exists
}

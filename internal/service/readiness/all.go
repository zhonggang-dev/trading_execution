// Package readiness combines independent fail-closed production probes.
package readiness

import (
	"context"
	"fmt"
)

// Checker is the read-only readiness boundary used by the HTTP transport.
type Checker interface {
	Check(context.Context) error
}

// NamedChecker preserves which production dependency failed without exposing
// credentials or response payloads.
type NamedChecker struct {
	Name    string
	Checker Checker
}

// All requires every configured checker to pass in deterministic order.
type All struct {
	checks []NamedChecker
}

func NewAll(checks ...NamedChecker) (*All, error) {
	if len(checks) == 0 {
		return nil, fmt.Errorf("at least one readiness checker is required")
	}
	for index, check := range checks {
		if check.Name == "" || check.Checker == nil {
			return nil, fmt.Errorf("readiness checker %d requires a name and implementation", index)
		}
	}
	return &All{checks: append([]NamedChecker(nil), checks...)}, nil
}

func (all *All) Check(ctx context.Context) error {
	for _, check := range all.checks {
		if err := check.Checker.Check(ctx); err != nil {
			return fmt.Errorf("%s: %w", check.Name, err)
		}
	}
	return nil
}

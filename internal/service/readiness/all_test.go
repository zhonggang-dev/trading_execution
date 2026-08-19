package readiness

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type checkerFunc func(context.Context) error

func (check checkerFunc) Check(ctx context.Context) error { return check(ctx) }

func TestAllFailsClosedWithDependencyName(t *testing.T) {
	all, err := NewAll(
		NamedChecker{Name: "postgres", Checker: checkerFunc(func(context.Context) error { return nil })},
		NamedChecker{Name: "heartbeat", Checker: checkerFunc(func(context.Context) error { return errors.New("stale") })},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := all.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "heartbeat: stale") {
		t.Fatalf("Check() error = %v", err)
	}
}

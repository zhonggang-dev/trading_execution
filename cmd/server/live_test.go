package main

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/reconciliation"
)

func TestValidateStartupReconciliationRequiresEveryAccountCompleted(t *testing.T) {
	completed := reconciliation.SweepResult{Runs: []reconciliation.Result{{
		Run: domain.ReconciliationRun{
			ExecutionAccountID: "wallet-1",
			Status:             domain.ReconciliationRunCompleted,
		},
	}}}
	if err := validateStartupReconciliation(completed, 1); err != nil {
		t.Fatalf("validate completed startup reconciliation: %v", err)
	}

	tests := []struct {
		name   string
		result reconciliation.SweepResult
		want   string
	}{
		{
			name:   "missing account run",
			result: reconciliation.SweepResult{},
			want:   "want 1",
		},
		{
			name: "source error",
			result: reconciliation.SweepResult{
				Runs:   completed.Runs,
				Errors: []error{errors.New("position source unavailable")},
			},
			want: "position source unavailable",
		},
		{
			name: "attention required",
			result: reconciliation.SweepResult{Runs: []reconciliation.Result{{
				Run: domain.ReconciliationRun{
					ExecutionAccountID: "wallet-1",
					Status:             domain.ReconciliationRunAttentionRequired,
				},
				Issues: []domain.ReconciliationIssue{{}},
			}}},
			want: "requires attention",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStartupReconciliation(test.result, 1)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateStartupReconciliation() = %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestPartitionReconciliationAccountsQuarantinesOneOfFour(t *testing.T) {
	active, quarantined, err := partitionReconciliationAccounts(
		[]string{"main", "wallet-1", "wallet-2", "wallet-3"},
		[]string{"wallet-3"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"main", "wallet-1", "wallet-2"}; !reflect.DeepEqual(active, want) {
		t.Fatalf("active accounts = %#v, want %#v", active, want)
	}
	if want := []string{"wallet-3"}; !reflect.DeepEqual(quarantined, want) {
		t.Fatalf("quarantined accounts = %#v, want %#v", quarantined, want)
	}
}

func TestPartitionReconciliationAccountsFailsClosed(t *testing.T) {
	tests := []struct {
		name        string
		configured  []string
		quarantined []string
		want        string
	}{
		{name: "unknown", configured: []string{"wallet-1"}, quarantined: []string{"wallet-3"}, want: "not configured"},
		{name: "duplicate", configured: []string{"wallet-1", "wallet-2"}, quarantined: []string{"wallet-2", "wallet-2"}, want: "duplicated"},
		{name: "all", configured: []string{"wallet-3"}, quarantined: []string{"wallet-3"}, want: "cannot exclude every"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := partitionReconciliationAccounts(test.configured, test.quarantined)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("partitionReconciliationAccounts() error = %v, want %q", err, test.want)
			}
		})
	}
}

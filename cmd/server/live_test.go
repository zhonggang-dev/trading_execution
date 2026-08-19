package main

import (
	"errors"
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

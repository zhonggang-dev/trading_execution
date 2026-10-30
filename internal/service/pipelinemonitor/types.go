// Package pipelinemonitor observes business progress without changing business decisions.
package pipelinemonitor

import (
	"context"
	"time"
)

const (
	Healthy    = "healthy"
	Warning    = "warning"
	Critical   = "critical"
	Unknown    = "unknown"
	Suppressed = "suppressed"
)

// Check is one observation. ID and Scope must not contain changing counters or timestamps.
type Check struct {
	ID       string            `json:"id"`
	Scope    string            `json:"scope"`
	Status   string            `json:"status"`
	Summary  string            `json:"summary"`
	Details  string            `json:"details,omitempty"`
	Evidence map[string]string `json:"evidence,omitempty"`
}

// Snapshot is a bounded, immutable projection; sampled_at is collection time, not producer freshness.
type Snapshot struct {
	SchemaVersion string    `json:"schema_version"`
	Component     string    `json:"component"`
	SampledAt     time.Time `json:"sampled_at"`
	Stale         bool      `json:"stale"`
	Checks        []Check   `json:"checks"`
}

type Collector interface {
	Collect(context.Context, time.Time) ([]Check, error)
}
type CollectFunc func(context.Context, time.Time) ([]Check, error)

func (f CollectFunc) Collect(ctx context.Context, now time.Time) ([]Check, error) { return f(ctx, now) }

// ValidStatus rejects unrecognised observations rather than treating them as success.
func ValidStatus(s string) bool {
	switch s {
	case Healthy, Warning, Critical, Unknown, Suppressed:
		return true
	}
	return false
}

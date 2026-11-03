package main

import (
	"context"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/config"
)

func TestPipelineMonitorConfigurationFailureIsIsolated(t *testing.T) {
	s, err := buildPipelineMonitor(config.Config{PipelineMonitorError: "monitor-only malformed interval"}, nil, nil, time.Now(), nil)
	if err != nil || s == nil {
		t.Fatalf("monitor configuration broke runtime: %v", err)
	}
	snapshot, err := s.Snapshot(context.Background())
	if err != nil || len(snapshot.Checks) != 1 || snapshot.Checks[0].ID != "monitor.configuration" || snapshot.Checks[0].Status != "unknown" {
		t.Fatalf("unexpected diagnostic %+v %v", snapshot, err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCLIRejectsBeforeOpeningMaintenanceOrRPC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, []byte(`{"sales":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{nil, {"--manifest", path}, {"--manifest", path, "--actor", "test", "--reason", "fixture"}, {"--manifest", path, "--actor", "test", "--reason", "fixture", "--apply", "--confirm-sha256", "wrong"}} {
		var out bytes.Buffer
		if err := run(args, &out); err == nil || out.Len() != 0 {
			t.Fatal("invalid command reached a successful repair", args, out.String(), err)
		}
	}
}

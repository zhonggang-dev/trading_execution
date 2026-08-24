package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileJournalStoreCreatesPrivateDurablePlanAndRejectsMutation(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "walletapprove.json")
	store, err := newFileJournalStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	expected := initialApprovalJournal(time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC))
	loaded, err := store.LoadOrCreate(expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateApprovalJournal(loaded, expected); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %s", info.Mode())
	}
	loaded.PlanID = "tampered"
	if err := store.Save(loaded); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadOrCreate(expected); err == nil || !strings.Contains(err.Error(), "immutable plan mismatch") {
		t.Fatalf("tampered journal error = %v", err)
	}
}

func TestValidateApprovalJournalRejectsReorderedScopeAndIncompleteConfirmation(t *testing.T) {
	expected := initialApprovalJournal(time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC))
	reordered := expected
	reordered.Entries = append([]journalEntry(nil), expected.Entries...)
	reordered.Entries[0], reordered.Entries[1] = reordered.Entries[1], reordered.Entries[0]
	if err := validateApprovalJournal(reordered, expected); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("reordered journal error = %v", err)
	}
	incomplete := expected
	incomplete.Entries = append([]journalEntry(nil), expected.Entries...)
	incomplete.Entries[0].State = journalStateConfirmed
	if err := validateApprovalJournal(incomplete, expected); err == nil || !strings.Contains(err.Error(), "confirmed entry is incomplete") {
		t.Fatalf("incomplete journal error = %v", err)
	}
}

func TestValidateApprovalJournalRequiresProcessedPrefixAndPendingSuffix(t *testing.T) {
	expected := initialApprovalJournal(time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC))
	outOfOrder := expected
	outOfOrder.Entries = append([]journalEntry(nil), expected.Entries...)
	outOfOrder.Entries[1].State = journalStateAlreadyApproved
	outOfOrder.Entries[1].AllowanceBefore = approvalAmount.String()
	outOfOrder.Entries[1].AllowanceAfter = approvalAmount.String()
	if err := validateApprovalJournal(outOfOrder, expected); err == nil || !strings.Contains(err.Error(), "processed-prefix state order") {
		t.Fatalf("out-of-order journal error = %v", err)
	}
}

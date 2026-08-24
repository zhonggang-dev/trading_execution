package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	journalStatePending         = "PENDING"
	journalStateAlreadyApproved = "ALREADY_APPROVED"
	journalStateSigned          = "SIGNED"
	journalStateSignedApproved  = "SIGNED_NOT_BROADCAST_ALREADY_APPROVED"
	journalStateBroadcast       = "BROADCAST"
	journalStateConfirmed       = "CONFIRMED"
)

type approvalJournal struct {
	SchemaVersion         string         `json:"schema_version"`
	PlanID                string         `json:"plan_id"`
	ChainID               uint64         `json:"chain_id"`
	TokenAddress          string         `json:"token_address"`
	AmountBaseUnits       string         `json:"amount_base_units"`
	RequiredConfirmations uint64         `json:"required_confirmations"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
	Entries               []journalEntry `json:"entries"`
}

type journalEntry struct {
	ExecutionAccountID   string    `json:"execution_account_id"`
	Owner                string    `json:"owner"`
	Spender              string    `json:"spender"`
	State                string    `json:"state"`
	AllowanceBefore      string    `json:"allowance_before,omitempty"`
	Nonce                uint64    `json:"nonce,omitempty"`
	GasLimit             uint64    `json:"gas_limit,omitempty"`
	MaxPriorityFeePerGas string    `json:"max_priority_fee_per_gas,omitempty"`
	MaxFeePerGas         string    `json:"max_fee_per_gas,omitempty"`
	CallData             string    `json:"call_data,omitempty"`
	SigningDigest        string    `json:"signing_digest,omitempty"`
	RawTransaction       string    `json:"raw_transaction,omitempty"`
	TransactionHash      string    `json:"transaction_hash,omitempty"`
	SignedAt             time.Time `json:"signed_at,omitempty"`
	BroadcastAt          time.Time `json:"broadcast_at,omitempty"`
	ReceiptBlockNumber   uint64    `json:"receipt_block_number,omitempty"`
	ReceiptBlockHash     string    `json:"receipt_block_hash,omitempty"`
	Confirmations        uint64    `json:"confirmations,omitempty"`
	AllowanceAfter       string    `json:"allowance_after,omitempty"`
	ConfirmedAt          time.Time `json:"confirmed_at,omitempty"`
}

func initialApprovalJournal(now time.Time) approvalJournal {
	entries := make([]journalEntry, len(fixedApprovalTargets))
	for index, target := range fixedApprovalTargets {
		entries[index] = journalEntry{
			ExecutionAccountID: target.executionAccountID,
			Owner:              target.expectedAddress,
			Spender:            target.spender,
			State:              journalStatePending,
		}
	}
	now = now.UTC()
	return approvalJournal{
		SchemaVersion:         approvalJournalSchema,
		PlanID:                "wallet-6-wallet-7-pusd-v2-approval-200000000-polygon-137",
		ChainID:               polygonChainID,
		TokenAddress:          polygonPUSDAddress,
		AmountBaseUnits:       approvalAmount.String(),
		RequiredConfirmations: requiredConfirmations,
		CreatedAt:             now,
		UpdatedAt:             now,
		Entries:               entries,
	}
}

func validateApprovalJournal(actual, expected approvalJournal) error {
	if actual.SchemaVersion != expected.SchemaVersion || actual.PlanID != expected.PlanID ||
		actual.ChainID != expected.ChainID || actual.TokenAddress != expected.TokenAddress ||
		actual.AmountBaseUnits != expected.AmountBaseUnits || actual.RequiredConfirmations != expected.RequiredConfirmations {
		return fmt.Errorf("approval journal immutable plan mismatch")
	}
	if actual.CreatedAt.IsZero() || actual.UpdatedAt.IsZero() || actual.UpdatedAt.Before(actual.CreatedAt) {
		return fmt.Errorf("approval journal timestamps are invalid")
	}
	if len(actual.Entries) != len(expected.Entries) {
		return fmt.Errorf("approval journal entry count mismatch")
	}
	seenInFlight := false
	seenPending := false
	for index := range actual.Entries {
		entry, expectedEntry := actual.Entries[index], expected.Entries[index]
		if entry.ExecutionAccountID != expectedEntry.ExecutionAccountID || entry.Owner != expectedEntry.Owner || entry.Spender != expectedEntry.Spender {
			return fmt.Errorf("approval journal entry %d identity mismatch", index)
		}
		if err := validateJournalEntry(entry); err != nil {
			return fmt.Errorf("approval journal entry %d: %w", index, err)
		}
		switch entry.State {
		case journalStateAlreadyApproved, journalStateSignedApproved, journalStateConfirmed:
			if seenInFlight || seenPending {
				return fmt.Errorf("approval journal entry %d violates processed-prefix state order", index)
			}
		case journalStateSigned, journalStateBroadcast:
			if seenInFlight || seenPending {
				return fmt.Errorf("approval journal entry %d violates single-in-flight state order", index)
			}
			seenInFlight = true
		case journalStatePending:
			seenPending = true
		}
	}
	return nil
}

func validateJournalEntry(entry journalEntry) error {
	signedFieldsPresent := entry.GasLimit != 0 && entry.MaxPriorityFeePerGas != "" && entry.MaxFeePerGas != "" &&
		entry.CallData != "" && entry.SigningDigest != "" && entry.RawTransaction != "" && entry.TransactionHash != "" && !entry.SignedAt.IsZero()
	anyTransactionField := entry.Nonce != 0 || entry.GasLimit != 0 || entry.MaxPriorityFeePerGas != "" || entry.MaxFeePerGas != "" ||
		entry.CallData != "" || entry.SigningDigest != "" || entry.RawTransaction != "" || entry.TransactionHash != "" ||
		!entry.SignedAt.IsZero() || !entry.BroadcastAt.IsZero() || entry.ReceiptBlockNumber != 0 || entry.ReceiptBlockHash != "" ||
		entry.Confirmations != 0 || !entry.ConfirmedAt.IsZero()
	switch entry.State {
	case journalStatePending:
		if anyTransactionField {
			return fmt.Errorf("pending entry contains transaction state")
		}
	case journalStateAlreadyApproved:
		if entry.AllowanceBefore != approvalAmount.String() || anyTransactionField {
			return fmt.Errorf("already-approved entry is inconsistent")
		}
	case journalStateSigned:
		if !signedFieldsPresent || !entry.BroadcastAt.IsZero() || !entry.ConfirmedAt.IsZero() {
			return fmt.Errorf("signed entry is incomplete")
		}
	case journalStateSignedApproved:
		if !signedFieldsPresent || !entry.BroadcastAt.IsZero() || !entry.ConfirmedAt.IsZero() ||
			entry.ReceiptBlockNumber != 0 || entry.ReceiptBlockHash != "" || entry.Confirmations != 0 ||
			entry.AllowanceAfter != approvalAmount.String() {
			return fmt.Errorf("signed already-approved entry is inconsistent")
		}
	case journalStateBroadcast:
		if !signedFieldsPresent || entry.BroadcastAt.IsZero() || !entry.ConfirmedAt.IsZero() {
			return fmt.Errorf("broadcast entry is incomplete")
		}
	case journalStateConfirmed:
		if !signedFieldsPresent || entry.BroadcastAt.IsZero() || entry.ConfirmedAt.IsZero() ||
			entry.ReceiptBlockNumber == 0 || entry.ReceiptBlockHash == "" ||
			entry.Confirmations < requiredConfirmations || entry.AllowanceAfter != approvalAmount.String() {
			return fmt.Errorf("confirmed entry is incomplete")
		}
	default:
		return fmt.Errorf("unknown state %q", entry.State)
	}
	return nil
}

type fileJournalStore struct {
	path     string
	lockFile *os.File
}

func newFileJournalStore(path string) (*fileJournalStore, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "." || !filepath.IsAbs(path) {
		return nil, fmt.Errorf("approval journal path must be absolute")
	}
	directory := filepath.Dir(path)
	resolvedDirectory, err := filepath.EvalSymlinks(directory)
	if err != nil || resolvedDirectory != directory {
		return nil, fmt.Errorf("approval journal directory must exist and contain no symlink")
	}
	directoryInfo, err := os.Stat(directory)
	if err != nil || !directoryInfo.IsDir() {
		return nil, fmt.Errorf("approval journal directory is invalid")
	}
	if directoryInfo.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("approval journal directory must not be group/other writable")
	}
	if err := requireCurrentOwner(directoryInfo, "approval journal directory"); err != nil {
		return nil, err
	}
	lockPath := path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open approval journal lock: %w", err)
	}
	lockInfo, err := lockFile.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm()&0o077 != 0 {
		lockFile.Close()
		return nil, fmt.Errorf("approval journal lock must be a private regular file")
	}
	if err := requireCurrentOwner(lockInfo, "approval journal lock"); err != nil {
		lockFile.Close()
		return nil, err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("approval journal is locked by another process")
	}
	return &fileJournalStore{path: path, lockFile: lockFile}, nil
}

func (store *fileJournalStore) Close() error {
	if store == nil || store.lockFile == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(store.lockFile.Fd()), syscall.LOCK_UN)
	closeErr := store.lockFile.Close()
	store.lockFile = nil
	return errors.Join(unlockErr, closeErr)
}

func (store *fileJournalStore) LoadOrCreate(expected approvalJournal) (approvalJournal, error) {
	file, err := openPrivateRegularFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		if err := store.Save(expected); err != nil {
			return approvalJournal{}, err
		}
		return expected, nil
	}
	if err != nil {
		return approvalJournal{}, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maximumRPCResponseBytes+1))
	if err != nil {
		return approvalJournal{}, fmt.Errorf("read approval journal: %w", err)
	}
	if int64(len(payload)) > maximumRPCResponseBytes {
		return approvalJournal{}, fmt.Errorf("approval journal exceeds %d bytes", maximumRPCResponseBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var journal approvalJournal
	if err := decoder.Decode(&journal); err != nil {
		return approvalJournal{}, fmt.Errorf("decode approval journal: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return approvalJournal{}, err
	}
	if err := validateApprovalJournal(journal, expected); err != nil {
		return approvalJournal{}, err
	}
	return journal, nil
}

func (store *fileJournalStore) Save(journal approvalJournal) error {
	payload, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		return fmt.Errorf("encode approval journal: %w", err)
	}
	payload = append(payload, '\n')
	directory := filepath.Dir(store.path)
	if info, err := os.Lstat(store.path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("existing approval journal must be a private non-symlink regular file")
		}
		if err := requireCurrentOwner(info, "approval journal"); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lstat approval journal before save: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".walletapprove-journal-*")
	if err != nil {
		return fmt.Errorf("create approval journal temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		return fmt.Errorf("write approval journal: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync approval journal: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close approval journal: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("replace approval journal: %w", err)
	}
	removeTemporary = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open approval journal directory: %w", err)
	}
	defer directoryFile.Close()
	if err := directoryFile.Sync(); err != nil {
		return fmt.Errorf("sync approval journal directory: %w", err)
	}
	return nil
}

func openPrivateRegularFile(path string) (*os.File, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("approval journal must be a private non-symlink regular file")
	}
	if err := requireCurrentOwner(pathInfo, "approval journal"); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(pathInfo, openedInfo) {
		file.Close()
		return nil, fmt.Errorf("approval journal changed while opening")
	}
	return file, nil
}

func requireCurrentOwner(info os.FileInfo, field string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("%s must be owned by the current user", field)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("approval journal contains trailing JSON")
		}
		return fmt.Errorf("decode approval journal trailing data: %w", err)
	}
	return nil
}

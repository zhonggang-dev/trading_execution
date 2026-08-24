package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	journalStatePending   = "PENDING"
	journalStateSigned    = "SIGNED"
	journalStateBroadcast = "BROADCAST"
	journalStateConfirmed = "CONFIRMED"
)

type fundingJournal struct {
	SchemaVersion         string         `json:"schema_version"`
	PlanID                string         `json:"plan_id"`
	ChainID               uint64         `json:"chain_id"`
	FundingSource         string         `json:"funding_source"`
	AmountWei             string         `json:"amount_wei"`
	StartingNonce         string         `json:"starting_nonce,omitempty"`
	SourceBalanceBefore   string         `json:"source_balance_before,omitempty"`
	RequiredConfirmations uint64         `json:"required_confirmations"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
	Entries               []journalEntry `json:"entries"`
}

type journalEntry struct {
	ExecutionAccountID     string    `json:"execution_account_id"`
	Source                 string    `json:"source"`
	Recipient              string    `json:"recipient"`
	State                  string    `json:"state"`
	RecipientBalanceBefore string    `json:"recipient_balance_before,omitempty"`
	PlannedNonce           string    `json:"planned_nonce,omitempty"`
	Nonce                  uint64    `json:"nonce,omitempty"`
	GasLimit               uint64    `json:"gas_limit,omitempty"`
	MaxPriorityFeePerGas   string    `json:"max_priority_fee_per_gas,omitempty"`
	MaxFeePerGas           string    `json:"max_fee_per_gas,omitempty"`
	Input                  string    `json:"input,omitempty"`
	SigningDigest          string    `json:"signing_digest,omitempty"`
	RawTransaction         string    `json:"raw_transaction,omitempty"`
	TransactionHash        string    `json:"transaction_hash,omitempty"`
	SignedAt               time.Time `json:"signed_at,omitempty"`
	BroadcastAt            time.Time `json:"broadcast_at,omitempty"`
	ReceiptBlockNumber     uint64    `json:"receipt_block_number,omitempty"`
	ReceiptBlockHash       string    `json:"receipt_block_hash,omitempty"`
	Confirmations          uint64    `json:"confirmations,omitempty"`
	GasUsed                uint64    `json:"gas_used,omitempty"`
	EffectiveGasPrice      string    `json:"effective_gas_price,omitempty"`
	RecipientBalanceAfter  string    `json:"recipient_balance_after,omitempty"`
	ConfirmedAt            time.Time `json:"confirmed_at,omitempty"`
}

func initialFundingJournal(now time.Time) fundingJournal {
	entries := make([]journalEntry, len(fixedFundingTargets))
	for index, target := range fixedFundingTargets {
		entries[index] = journalEntry{
			ExecutionAccountID: target.executionAccountID,
			Source:             mainExpectedAddress,
			Recipient:          target.recipient,
			State:              journalStatePending,
		}
	}
	now = now.UTC()
	return fundingJournal{
		SchemaVersion:         fundingJournalSchema,
		PlanID:                "main-to-wallet-6-wallet-7-native-pol-60000000000000000-polygon-137",
		ChainID:               polygonChainID,
		FundingSource:         mainExpectedAddress,
		AmountWei:             fundingAmountWei.String(),
		RequiredConfirmations: requiredConfirmations,
		CreatedAt:             now,
		UpdatedAt:             now,
		Entries:               entries,
	}
}

func validateFundingJournal(actual, expected fundingJournal) error {
	if actual.SchemaVersion != expected.SchemaVersion || actual.PlanID != expected.PlanID ||
		actual.ChainID != expected.ChainID || actual.FundingSource != expected.FundingSource ||
		actual.AmountWei != expected.AmountWei || actual.RequiredConfirmations != expected.RequiredConfirmations {
		return fmt.Errorf("funding journal immutable plan mismatch")
	}
	if actual.CreatedAt.IsZero() || actual.UpdatedAt.IsZero() || actual.UpdatedAt.Before(actual.CreatedAt) {
		return fmt.Errorf("funding journal timestamps are invalid")
	}
	if len(actual.Entries) != len(expected.Entries) {
		return fmt.Errorf("funding journal entry count mismatch")
	}
	hasStartingNonce := actual.StartingNonce != ""
	hasSourceBalance := actual.SourceBalanceBefore != ""
	if hasStartingNonce != hasSourceBalance {
		return fmt.Errorf("funding journal prestate is incomplete")
	}
	prepared := hasStartingNonce && hasSourceBalance
	if prepared {
		startingNonce, ok := parseCanonicalNonNegativeDecimal(actual.StartingNonce)
		if !ok || !startingNonce.IsUint64() {
			return fmt.Errorf("funding journal starting nonce is invalid")
		}
		if balance, ok := parseCanonicalNonNegativeDecimal(actual.SourceBalanceBefore); !ok || balance.Sign() <= 0 {
			return fmt.Errorf("funding journal source balance is invalid")
		}
	}
	seenInFlight := false
	seenPending := false
	for index := range actual.Entries {
		entry, expectedEntry := actual.Entries[index], expected.Entries[index]
		if entry.ExecutionAccountID != expectedEntry.ExecutionAccountID || entry.Source != expectedEntry.Source || entry.Recipient != expectedEntry.Recipient {
			return fmt.Errorf("funding journal entry %d identity mismatch", index)
		}
		if err := validateJournalEntry(entry); err != nil {
			return fmt.Errorf("funding journal entry %d: %w", index, err)
		}
		if prepared {
			startingNonce, _ := parseCanonicalNonNegativeDecimal(actual.StartingNonce)
			plannedNonce, ok := parseCanonicalNonNegativeDecimal(entry.PlannedNonce)
			if !ok || !plannedNonce.IsUint64() || startingNonce.Uint64() > math.MaxUint64-uint64(index) || plannedNonce.Uint64() != startingNonce.Uint64()+uint64(index) {
				return fmt.Errorf("funding journal entry %d planned nonce mismatch", index)
			}
			if balance, ok := parseCanonicalNonNegativeDecimal(entry.RecipientBalanceBefore); !ok || balance.Sign() < 0 {
				return fmt.Errorf("funding journal entry %d recipient pre-balance is invalid", index)
			}
		} else if entry.PlannedNonce != "" || entry.RecipientBalanceBefore != "" {
			return fmt.Errorf("funding journal entry %d has partial prestate", index)
		}
		switch entry.State {
		case journalStateConfirmed:
			if seenInFlight || seenPending {
				return fmt.Errorf("funding journal entry %d violates processed-prefix state order", index)
			}
		case journalStateSigned, journalStateBroadcast:
			if seenInFlight || seenPending {
				return fmt.Errorf("funding journal entry %d violates single-in-flight state order", index)
			}
			seenInFlight = true
		case journalStatePending:
			seenPending = true
		}
	}
	return nil
}

func validateJournalEntry(entry journalEntry) error {
	signedFieldsPresent := entry.RecipientBalanceBefore != "" && entry.PlannedNonce != "" && entry.GasLimit != 0 && entry.MaxPriorityFeePerGas != "" && entry.MaxFeePerGas != "" &&
		entry.Input == "0x" && entry.SigningDigest != "" && entry.RawTransaction != "" && entry.TransactionHash != "" && !entry.SignedAt.IsZero()
	anyTransactionField := entry.Nonce != 0 || entry.GasLimit != 0 || entry.MaxPriorityFeePerGas != "" || entry.MaxFeePerGas != "" ||
		entry.Input != "" || entry.SigningDigest != "" || entry.RawTransaction != "" || entry.TransactionHash != "" ||
		!entry.SignedAt.IsZero() || !entry.BroadcastAt.IsZero() || entry.ReceiptBlockNumber != 0 || entry.ReceiptBlockHash != "" ||
		entry.Confirmations != 0 || entry.GasUsed != 0 || entry.EffectiveGasPrice != "" || entry.RecipientBalanceAfter != "" || !entry.ConfirmedAt.IsZero()
	switch entry.State {
	case journalStatePending:
		if anyTransactionField {
			return fmt.Errorf("pending entry contains transaction state")
		}
	case journalStateSigned:
		if !signedFieldsPresent || !entry.BroadcastAt.IsZero() || !entry.ConfirmedAt.IsZero() {
			return fmt.Errorf("signed entry is incomplete")
		}
	case journalStateBroadcast:
		if !signedFieldsPresent || entry.BroadcastAt.IsZero() || !entry.ConfirmedAt.IsZero() {
			return fmt.Errorf("broadcast entry is incomplete")
		}
	case journalStateConfirmed:
		if !signedFieldsPresent || entry.BroadcastAt.IsZero() || entry.ConfirmedAt.IsZero() ||
			entry.ReceiptBlockNumber == 0 || entry.ReceiptBlockHash == "" ||
			entry.Confirmations < requiredConfirmations || entry.GasUsed == 0 || entry.EffectiveGasPrice == "" || entry.RecipientBalanceAfter == "" {
			return fmt.Errorf("confirmed entry is incomplete")
		}
	default:
		return fmt.Errorf("unknown state %q", entry.State)
	}
	if signedFieldsPresent {
		plannedNonce, plannedOK := parseCanonicalNonNegativeDecimal(entry.PlannedNonce)
		if !plannedOK || !plannedNonce.IsUint64() || entry.Nonce != plannedNonce.Uint64() {
			return fmt.Errorf("signed nonce differs from planned nonce")
		}
	}
	if entry.EffectiveGasPrice != "" {
		if price, ok := parseCanonicalNonNegativeDecimal(entry.EffectiveGasPrice); !ok || price.Sign() <= 0 {
			return fmt.Errorf("effective gas price is invalid")
		}
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
		return nil, fmt.Errorf("funding journal path must be absolute")
	}
	directory := filepath.Dir(path)
	resolvedDirectory, err := filepath.EvalSymlinks(directory)
	if err != nil || resolvedDirectory != directory {
		return nil, fmt.Errorf("funding journal directory must exist and contain no symlink")
	}
	directoryInfo, err := os.Stat(directory)
	if err != nil || !directoryInfo.IsDir() {
		return nil, fmt.Errorf("funding journal directory is invalid")
	}
	if directoryInfo.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("funding journal directory must not be group/other writable")
	}
	if err := requireCurrentOwner(directoryInfo, "funding journal directory"); err != nil {
		return nil, err
	}
	lockPath := path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open funding journal lock: %w", err)
	}
	lockInfo, err := lockFile.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm()&0o077 != 0 {
		lockFile.Close()
		return nil, fmt.Errorf("funding journal lock must be a private regular file")
	}
	if err := requireCurrentOwner(lockInfo, "funding journal lock"); err != nil {
		lockFile.Close()
		return nil, err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("funding journal is locked by another process")
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

func (store *fileJournalStore) Load(expected fundingJournal) (fundingJournal, bool, error) {
	file, err := openPrivateRegularFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return fundingJournal{}, false, nil
	}
	if err != nil {
		return fundingJournal{}, false, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maximumRPCResponseBytes+1))
	if err != nil {
		return fundingJournal{}, false, fmt.Errorf("read funding journal: %w", err)
	}
	if int64(len(payload)) > maximumRPCResponseBytes {
		return fundingJournal{}, false, fmt.Errorf("funding journal exceeds %d bytes", maximumRPCResponseBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var journal fundingJournal
	if err := decoder.Decode(&journal); err != nil {
		return fundingJournal{}, false, fmt.Errorf("decode funding journal: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fundingJournal{}, false, err
	}
	if err := validateFundingJournal(journal, expected); err != nil {
		return fundingJournal{}, false, err
	}
	return journal, true, nil
}

func encodeFundingJournal(journal fundingJournal) ([]byte, error) {
	payload, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode funding journal: %w", err)
	}
	return append(payload, '\n'), nil
}

func (store *fileJournalStore) Create(journal fundingJournal) error {
	payload, err := encodeFundingJournal(journal)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(store.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("create initial funding journal exclusively: %w", err)
	}
	closeNeeded := true
	defer func() {
		if closeNeeded {
			_ = file.Close()
		}
	}()
	if info, err := file.Stat(); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("created funding journal is not an exact mode-0600 regular file")
	}
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("write initial funding journal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync initial funding journal: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close initial funding journal: %w", err)
	}
	closeNeeded = false
	return syncDirectory(filepath.Dir(store.path))
}

func (store *fileJournalStore) Save(journal fundingJournal) error {
	payload, err := encodeFundingJournal(journal)
	if err != nil {
		return err
	}
	directory := filepath.Dir(store.path)
	if info, err := os.Lstat(store.path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("existing funding journal must be a private non-symlink regular file")
		}
		if err := requireCurrentOwner(info, "funding journal"); err != nil {
			return err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("funding journal disappeared; refusing non-exclusive recreation")
	} else {
		return fmt.Errorf("lstat funding journal before save: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".wallet67fund-journal-*")
	if err != nil {
		return fmt.Errorf("create funding journal temporary file: %w", err)
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
		return fmt.Errorf("write funding journal: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync funding journal: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close funding journal: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("replace funding journal: %w", err)
	}
	removeTemporary = false
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open funding journal directory: %w", err)
	}
	defer directoryFile.Close()
	if err := directoryFile.Sync(); err != nil {
		return fmt.Errorf("sync funding journal directory: %w", err)
	}
	return nil
}

func openPrivateRegularFile(path string) (*os.File, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("funding journal must be a private non-symlink regular file")
	}
	if err := requireCurrentOwner(pathInfo, "funding journal"); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(pathInfo, openedInfo) {
		file.Close()
		return nil, fmt.Errorf("funding journal changed while opening")
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
			return fmt.Errorf("funding journal contains trailing JSON")
		}
		return fmt.Errorf("decode funding journal trailing data: %w", err)
	}
	return nil
}

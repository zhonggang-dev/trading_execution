package walletcutoverexit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const journalVersion = 1

const (
	JournalPrepared   = "PREPARED"
	JournalSubmitting = "SUBMITTING"
	JournalSubmitted  = "SUBMITTED"
	JournalPartial    = "PARTIALLY_FILLED"
	JournalFilled     = "FILLED"
	JournalRejected   = "REJECTED"
	JournalUnknown    = "UNKNOWN"
	JournalConfirmed  = "CONFIRMED"
)

// Journal is append-oriented in meaning: entries are only advanced to a
// later state. The JSON file is replaced atomically, but an existing action
// can never receive a second signed order for the same manifest hash.
type Journal struct {
	Version        int            `json:"version"`
	ManifestSHA256 string         `json:"manifest_sha256"`
	Entries        []JournalEntry `json:"entries"`
}

type JournalEntry struct {
	ActionID           string         `json:"action_id"`
	Kind               string         `json:"kind"`
	ManifestSHA256     string         `json:"manifest_sha256"`
	ActionHash         string         `json:"action_hash"`
	ExecutionAccountID string         `json:"execution_account_id"`
	TokenID            string         `json:"token_id,omitempty"`
	ConditionID        string         `json:"condition_id,omitempty"`
	Nonce              string         `json:"nonce,omitempty"`
	ClientOrderID      string         `json:"client_order_id,omitempty"`
	ExpectedVenueID    string         `json:"expected_venue_id,omitempty"`
	SubmissionProvider string         `json:"submission_provider,omitempty"`
	State              string         `json:"state"`
	FilledSize         domain.Decimal `json:"filled_size,omitempty"`
	RemainingSize      domain.Decimal `json:"remaining_size,omitempty"`
	VenueState         string         `json:"venue_state,omitempty"`
	TransactionHash    string         `json:"transaction_hash,omitempty"`
	Error              string         `json:"error,omitempty"`
	Actor              string         `json:"actor,omitempty"`
	Reason             string         `json:"reason,omitempty"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

// JournalStore is intentionally narrower than the application repositories;
// it is an operator journal and must not mutate positions or cash ledgers.
type JournalStore interface {
	Load() (Journal, error)
	Save(Journal) error
}

// JournalLocker serializes the complete operator run across processes. A
// per-method mutex is insufficient because two CLI processes could otherwise
// both Load an empty journal and sign different orders before either Save.
type JournalLocker interface {
	Acquire() (func(), error)
}

// FileJournalStore provides atomic 0600 persistence for the CLI.
type FileJournalStore struct {
	path string
	mu   sync.Mutex
}

var ErrJournalLocked = fmt.Errorf("wallet exit journal is already locked by another process")

func NewFileJournalStore(path string) (*FileJournalStore, error) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return nil, fmt.Errorf("journal path must be an absolute path")
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("journal path must be a regular non-symlink file")
		}
		if info.Mode().Perm() != 0o600 {
			return nil, fmt.Errorf("journal file must have mode 0600")
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect journal file: %w", err)
	}
	if parent := filepath.Dir(path); parent == "." {
		return nil, fmt.Errorf("journal path must include a directory")
	} else if info, err := os.Stat(parent); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("journal parent directory is not available")
	}
	return &FileJournalStore{path: path}, nil
}

func (store *FileJournalStore) Load() (Journal, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if info, err := os.Lstat(store.path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
			return Journal{}, fmt.Errorf("journal file must remain a regular 0600 file")
		}
	} else if !os.IsNotExist(err) {
		return Journal{}, fmt.Errorf("inspect wallet exit journal: %w", err)
	}
	data, err := os.ReadFile(store.path)
	if os.IsNotExist(err) {
		return Journal{Version: journalVersion, Entries: []JournalEntry{}}, nil
	}
	if err != nil {
		return Journal{}, fmt.Errorf("read wallet exit journal: %w", err)
	}
	var journal Journal
	if err := json.Unmarshal(data, &journal); err != nil {
		return Journal{}, fmt.Errorf("decode wallet exit journal: %w", err)
	}
	if err := validateJournal(journal); err != nil {
		return Journal{}, err
	}
	return journal, nil
}

func (store *FileJournalStore) Save(journal Journal) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := validateJournal(journal); err != nil {
		return err
	}
	data, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		return fmt.Errorf("encode wallet exit journal: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(store.path), ".wallet-exit-journal-*")
	if err != nil {
		return fmt.Errorf("create journal temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod journal temp file: %w", err)
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write wallet exit journal: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync wallet exit journal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close wallet exit journal: %w", err)
	}
	if err := os.Rename(tmpName, store.path); err != nil {
		return fmt.Errorf("replace wallet exit journal: %w", err)
	}
	directory, err := os.Open(filepath.Dir(store.path))
	if err != nil {
		return fmt.Errorf("open journal directory for sync: %w", err)
	}
	if err := directory.Sync(); err != nil {
		directory.Close()
		return fmt.Errorf("sync journal directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close journal directory: %w", err)
	}
	return nil
}

// Acquire obtains a non-blocking advisory lock that remains held from the
// journal Load through the final state Save. The lock file contains no secret
// or payload and is private to the operator account.
func (store *FileJournalStore) Acquire() (func(), error) {
	lockPath := store.path + ".lock"
	if info, statErr := os.Lstat(lockPath); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
			return nil, fmt.Errorf("wallet exit journal lock must be a regular 0600 file")
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != os.Geteuid() {
			return nil, fmt.Errorf("wallet exit journal lock must be owned by the current operator")
		}
	} else if !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("inspect wallet exit journal lock: %w", statErr)
	}
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wallet exit journal lock: %w", err)
	}
	if err := lockFile.Chmod(0o600); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("chmod wallet exit journal lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, ErrJournalLocked
		}
		return nil, fmt.Errorf("acquire wallet exit journal lock: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
	}, nil
}

func validateJournal(journal Journal) error {
	if journal.Version == 0 {
		journal.Version = journalVersion
	}
	if journal.Version != journalVersion {
		return fmt.Errorf("unsupported wallet exit journal version %d", journal.Version)
	}
	if strings.TrimSpace(journal.ManifestSHA256) == "" && len(journal.Entries) > 0 {
		return fmt.Errorf("wallet exit journal manifest_sha256 is required")
	}
	seen := make(map[string]struct{}, len(journal.Entries))
	for _, entry := range journal.Entries {
		if strings.TrimSpace(entry.ActionID) == "" || strings.TrimSpace(entry.ManifestSHA256) == "" || strings.TrimSpace(entry.State) == "" {
			return fmt.Errorf("wallet exit journal contains an incomplete entry")
		}
		if !validSHA(entry.ManifestSHA256) || entry.ManifestSHA256 != strings.ToLower(entry.ManifestSHA256) {
			return fmt.Errorf("wallet exit journal entry %q has an invalid manifest hash", entry.ActionID)
		}
		if entry.Kind != assetActionKind && entry.Kind != redemptionActionKind {
			return fmt.Errorf("wallet exit journal entry %q has an invalid action kind", entry.ActionID)
		}
		switch entry.State {
		case JournalPrepared, JournalSubmitting, JournalSubmitted, JournalPartial,
			JournalFilled, JournalRejected, JournalUnknown, JournalConfirmed:
		default:
			return fmt.Errorf("wallet exit journal entry %q has an invalid state", entry.ActionID)
		}
		if _, exists := seen[entry.ActionID]; exists {
			return fmt.Errorf("wallet exit journal contains duplicate action_id %q", entry.ActionID)
		}
		seen[entry.ActionID] = struct{}{}
		if journal.ManifestSHA256 != "" && entry.ManifestSHA256 != journal.ManifestSHA256 {
			return fmt.Errorf("wallet exit journal entry %q has a different manifest hash", entry.ActionID)
		}
	}
	return nil
}

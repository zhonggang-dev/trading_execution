package walletcutover

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

var testManifestNow = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

func validTestManifest() Manifest {
	return Manifest{
		SchemaVersion: ManifestSchemaVersion,
		BatchID:       "wallet-cutover-test",
		ObservedAt:    testManifestNow.Add(-time.Minute),
		ExpiresAt:     testManifestNow.Add(time.Hour),
		EvidenceBlock: EvidenceBlock{ChainID: PolygonChainID, Number: 100, Hash: "0x" + repeat("ab", 32), Confirmations: 128},
		Actor:         "operator",
		Reason:        "reviewed baseline",
		Accounts: []AccountManifest{
			testAccount("main", "0x"+repeat("11", 20), "10", "8"),
			testAccount("wallet-1", "0x"+repeat("22", 20), "20", "20"),
		},
	}
}

func testAccount(id, wallet, ledger, chain string) AccountManifest {
	return AccountManifest{
		ExecutionAccountID: id, WalletAddress: wallet, BeforeVersion: 1,
		Ledger:       BalanceSnapshot{Asset: PUSDAsset, TotalBalance: domain.Decimal(ledger), AvailableBalance: domain.Decimal(ledger), ReservedBalance: "0"},
		ChainBalance: domain.Decimal(chain), Assets: []AssetManifest{},
	}
}

func repeat(value string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += value
	}
	return result
}

func TestManifestRejectsDuplicateJSONKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	data := []byte(`{"schema_version":"trading.wallet_reuse_cutover.v1","schema_version":"trading.wallet_reuse_cutover.v1"}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := ReadManifest(path, testManifestNow); err == nil {
		t.Fatal("ReadManifest accepted duplicate JSON key")
	}
}

func TestManifestRequiresPrivateMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := ReadManifest(path, testManifestNow); err == nil {
		t.Fatal("ReadManifest accepted group-readable manifest")
	}
}

type fakeChain struct{ report ChainReport }

func (f fakeChain) ChainID(context.Context) (uint64, error)     { return f.report.ChainID, nil }
func (f fakeChain) LatestBlock(context.Context) (uint64, error) { return f.report.LatestBlock, nil }
func (f fakeChain) BlockHeader(context.Context, uint64) (string, time.Time, error) {
	return f.report.BlockHash, f.report.BlockTime, nil
}
func (f fakeChain) BalanceAt(_ context.Context, account string, _ uint64) (domain.Decimal, error) {
	for _, value := range f.report.Accounts {
		if value.WalletAddress == account {
			return value.CashBalance, nil
		}
	}
	return "", errors.New("wallet missing")
}
func (f fakeChain) TokenBalanceAt(context.Context, string, string, uint64) (domain.Decimal, error) {
	return "0", nil
}

type fakeStore struct {
	states []AccountState
	result ApplyResult
	params ApplyParams
}

func (f *fakeStore) Snapshot(context.Context, []string) ([]AccountState, error) {
	return append([]AccountState(nil), f.states...), nil
}
func (f *fakeStore) ApplyBaseline(_ context.Context, params ApplyParams) (ApplyResult, error) {
	f.params = params
	return f.result, nil
}

func TestAuditReadsFreshChainAndValidatesLocalCAS(t *testing.T) {
	manifest := validTestManifest()
	store := &fakeStore{states: []AccountState{
		{ExecutionAccountID: "main", WalletAddress: "0x" + repeat("11", 20), CollateralAsset: PUSDAsset, TotalBalance: "10", AvailableBalance: "10", ReservedBalance: "0", Version: 1, Paused: true},
		{ExecutionAccountID: "wallet-1", WalletAddress: "0x" + repeat("22", 20), CollateralAsset: PUSDAsset, TotalBalance: "20", AvailableBalance: "20", ReservedBalance: "0", Version: 1, Paused: true},
	}}
	chain := fakeChain{report: ChainReport{ChainID: PolygonChainID, BlockNumber: 100, BlockHash: "0x" + repeat("ab", 32), BlockTime: testManifestNow, LatestBlock: 300, Accounts: []ChainAccountReport{
		{ExecutionAccountID: "main", WalletAddress: "0x" + repeat("11", 20), CashBalance: "8"},
		{ExecutionAccountID: "wallet-1", WalletAddress: "0x" + repeat("22", 20), CashBalance: "20"},
	}}}
	service, err := New(Params{Chain: chain, Store: store, Now: func() time.Time { return testManifestNow }})
	if err != nil {
		t.Fatal(err)
	}
	report, _, err := service.Audit(context.Background(), manifest)
	if err != nil {
		t.Fatalf("Audit() error = %v", err)
	}
	if report.BlockNumber != manifest.EvidenceBlock.Number {
		t.Fatalf("block = %d", report.BlockNumber)
	}
}

func TestApplyRequiresActorReasonAndChainReport(t *testing.T) {
	manifest := validTestManifest()
	store := &fakeStore{}
	chain := fakeChain{}
	service, err := New(Params{Chain: chain, Store: store, Now: func() time.Time { return testManifestNow }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Apply(context.Background(), ApplyParams{Manifest: manifest, ManifestSHA: repeat("a", 64), Actor: "wrong", Reason: manifest.Reason}); err == nil {
		t.Fatal("Apply accepted actor mismatch")
	}
}

func TestApplyRejectsRawManifestThatDiffersFromTypedManifest(t *testing.T) {
	manifest := validTestManifest()
	rawManifest := manifest
	rawManifest.Reason = "tampered raw reason"
	raw, err := json.Marshal(rawManifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	store := &fakeStore{}
	chain := fakeChain{report: ChainReport{
		ChainID: PolygonChainID, BlockNumber: manifest.EvidenceBlock.Number,
		BlockHash: manifest.EvidenceBlock.Hash, Confirmations: manifest.EvidenceBlock.Confirmations,
		Accounts: []ChainAccountReport{
			{ExecutionAccountID: "main", WalletAddress: manifest.Accounts[0].WalletAddress, CashBalance: manifest.Accounts[0].ChainBalance},
			{ExecutionAccountID: "wallet-1", WalletAddress: manifest.Accounts[1].WalletAddress, CashBalance: manifest.Accounts[1].ChainBalance},
		},
	}}
	service, err := New(Params{Chain: chain, Store: store, Now: func() time.Time { return testManifestNow }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Apply(context.Background(), ApplyParams{
		Manifest: manifest, ManifestRaw: raw, ManifestSHA: hex.EncodeToString(digest[:]),
		Actor: manifest.Actor, Reason: manifest.Reason,
		Chain: ChainReport{ChainID: PolygonChainID, BlockNumber: manifest.EvidenceBlock.Number,
			BlockHash: manifest.EvidenceBlock.Hash, Confirmations: manifest.EvidenceBlock.Confirmations,
			Accounts: []ChainAccountReport{
				{ExecutionAccountID: "main", WalletAddress: manifest.Accounts[0].WalletAddress, CashBalance: manifest.Accounts[0].ChainBalance},
				{ExecutionAccountID: "wallet-1", WalletAddress: manifest.Accounts[1].WalletAddress, CashBalance: manifest.Accounts[1].ChainBalance},
			}},
	})
	if err == nil {
		t.Fatal("Apply accepted raw manifest with different typed values")
	}
}

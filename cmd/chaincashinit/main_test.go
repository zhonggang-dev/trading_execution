package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/chaincash"
)

const (
	testWallet6 = "0x6666666666666666666666666666666666666666"
	testReward  = "0x607c8c9866ef3b4665c5a384188706be738d8bf8"
)

type fakeChain struct {
	chainID  uint64
	latest   uint64
	balances map[uint64]domain.Decimal
}

func (chain *fakeChain) ChainID(context.Context) (uint64, error)     { return chain.chainID, nil }
func (chain *fakeChain) LatestBlock(context.Context) (uint64, error) { return chain.latest, nil }
func (chain *fakeChain) BlockHeader(_ context.Context, block uint64) (string, time.Time, error) {
	return "0x" + strings.Repeat("ab", 32), time.Unix(int64(block), 0).UTC(), nil
}
func (chain *fakeChain) BalanceAt(_ context.Context, _ string, block uint64) (domain.Decimal, error) {
	balance, found := chain.balances[block]
	if !found {
		return "", errors.New("unexpected balance block")
	}
	return balance, nil
}

type fakeTransfers struct {
	transfers []domain.ChainCashTransfer
	from, to  uint64
	calls     int
}

func (reader *fakeTransfers) ReadConfirmedTransfers(_ context.Context, accountID, _ string, from, to uint64) ([]domain.ChainCashTransfer, error) {
	reader.calls++
	reader.from, reader.to = from, to
	result := make([]domain.ChainCashTransfer, 0, len(reader.transfers))
	for _, transfer := range reader.transfers {
		transfer.ExecutionAccountID = accountID
		result = append(result, transfer)
	}
	return result, nil
}

type fakeInitializer struct {
	params []postgresadapter.ChainCashCursorInitialization
	err    error
}

func (store *fakeInitializer) InitializeChainCashCursor(
	_ context.Context, params postgresadapter.ChainCashCursorInitialization,
) (postgresadapter.ChainCashCursorInitializationReport, error) {
	store.params = append(store.params, params)
	return postgresadapter.ChainCashCursorInitializationReport{
		ExecutionAccountID: params.ExecutionAccountID, Executed: params.Execute && store.err == nil,
		CatchUpBlock: params.CatchUpBlock, TransfersPersisted: len(params.Transfers),
	}, store.err
}

func wallet6Reward() domain.ChainCashTransfer {
	return domain.ChainCashTransfer{
		TransactionHash: "0x6c728c7c" + strings.Repeat("0", 51) + "cca76", LogIndex: 12, BlockNumber: 94681378,
		BlockHash: "0x" + strings.Repeat("cd", 32), Direction: domain.ChainCashIn, Counterparty: testReward,
		Amount: "0.0015", Classification: domain.ChainCashReward,
	}
}

func wallet6Options(execute bool) options {
	return options{
		AccountID: "wallet-6", Wallet: strings.ToUpper(testWallet6[:2]) + testWallet6[2:], StartBlock: 94681377,
		Confirmations: 64, Actor: "operator", Reason: "enable chain cash ledger", Execute: execute,
		Now: func() time.Time { return time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC) },
	}
}

func TestRunCatchesUpWallet6FromBeforeTheReward(t *testing.T) {
	for _, execute := range []bool{false, true} {
		chain := &fakeChain{chainID: 137, latest: 94700064, balances: map[uint64]domain.Decimal{
			94681377: "1.700022", 94700000: "1.701522",
		}}
		transfers := &fakeTransfers{transfers: []domain.ChainCashTransfer{wallet6Reward()}}
		store := &fakeInitializer{}
		var stdout bytes.Buffer
		if err := run(context.Background(), wallet6Options(execute), chain, transfers, store, &stdout); err != nil {
			t.Fatalf("execute=%v: %v\n%s", execute, err, stdout.String())
		}
		if transfers.from != 94681378 || transfers.to != 94700000 {
			t.Fatalf("catch-up range = (%d, %d]", transfers.from-1, transfers.to)
		}
		if len(store.params) != 1 {
			t.Fatalf("initializer calls = %#v", store.params)
		}
		params := store.params[0]
		if params.Execute != execute || params.StartBlock != 94681377 || params.CatchUpBlock != 94700000 ||
			params.StartBalance != "1.700022" || params.WalletAddress != testWallet6 || len(params.Transfers) != 1 ||
			params.Transfers[0].Classification != domain.ChainCashReward || params.AppliedAt.IsZero() ||
			params.Evidence["catch_up_block"] != uint64(94700000) {
			t.Fatalf("params = %#v", params)
		}
		var printed output
		if err := json.Unmarshal(stdout.Bytes(), &printed); err != nil {
			t.Fatal(err)
		}
		wantMode := "dry-run"
		if execute {
			wantMode = "execute"
		}
		if printed.Mode != wantMode || printed.CatchUpNetTransfers != "0.0015" || printed.Classifications[domain.ChainCashReward] != 1 ||
			printed.Ledger == nil || printed.Ledger.Executed != execute || printed.Error != "" {
			t.Fatalf("printed = %#v", printed)
		}
	}
}

// fakeWalletLogs is the chain view behind the real classifier: pUSD transfers
// plus the venue OrderFilled logs naming the wallet.
type fakeWalletLogs struct{ logs domain.ChainCashLogs }

func (source fakeWalletLogs) ReadWalletLogs(context.Context, string, uint64, uint64) (domain.ChainCashLogs, error) {
	return source.logs, nil
}
func (fakeWalletLogs) RequireBlock(context.Context, uint64) error { return nil }

type fakeClassifierStore struct{}

func (fakeClassifierStore) GetChainCashCursor(context.Context, string) (domain.ChainCashCursor, bool, error) {
	return domain.ChainCashCursor{}, false, nil
}
func (fakeClassifierStore) ListRedemptionTransactionHashes(context.Context, string, []string) (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}
func (fakeClassifierStore) ApplyChainCashRange(context.Context, domain.ChainCashRange) (domain.ChainCashRangeResult, error) {
	return domain.ChainCashRangeResult{}, errors.New("the catch-up never applies a range through the service")
}

func TestRunCatchUpClassifiesPeerToPeerSettlementAsTrade(t *testing.T) {
	// The offline catch-up uses the same classifier as the service: a BUY
	// settled peer-to-peer (two makers and the fee collector) is TRADE, while
	// a payment in a transaction without a wallet OrderFilled stays unattributed.
	const (
		makerA       = "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
		makerB       = "0xb1ca909e00000000000000000000000000000001"
		feeCollector = "0x115f48dc2a731aa16251c6d6e1befc42f92accc9"
		buyTx        = "0xc939c707cd3fe9463e8fb8d5f68e721ca23201a1715372de96fe85828651dc9d"
	)
	blockHash := "0x" + strings.Repeat("cd", 32)
	payment := func(logIndex uint64, transactionHash, counterparty string, amount domain.Decimal) domain.ChainCashTransfer {
		return domain.ChainCashTransfer{
			TransactionHash: transactionHash, LogIndex: logIndex, BlockNumber: 94690000, BlockHash: blockHash,
			Direction: domain.ChainCashOut, Counterparty: counterparty, Amount: amount,
		}
	}
	logs := fakeWalletLogs{logs: domain.ChainCashLogs{
		Transfers: []domain.ChainCashTransfer{
			payment(11, buyTx, makerA, "4.80764"), payment(13, buyTx, makerB, "4.33236"),
			payment(15, buyTx, feeCollector, "0.02358"),
			payment(40, "0x"+strings.Repeat("ee", 32), makerA, "1"),
		},
		Fills: []domain.ChainCashFill{{
			TransactionHash: buyTx, LogIndex: 12, BlockNumber: 94690000, BlockHash: blockHash,
			Exchange: evmrpc.PolygonCTFExchangeV2Address, Maker: makerA, Taker: testWallet6,
		}},
	}}
	classifier, err := chaincash.New(chaincash.Params{
		Logs: logs, Store: fakeClassifierStore{},
		ExchangeAddresses: []string{evmrpc.PolygonCTFExchangeV2Address, evmrpc.PolygonNegRiskCTFExchangeV2Address},
		RewardSenders:     []string{testReward}, Confirmations: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	chain := &fakeChain{chainID: 137, latest: 94700064, balances: map[uint64]domain.Decimal{
		94681377: "20", 94700000: "9.83642",
	}}
	store := &fakeInitializer{}
	var stdout bytes.Buffer
	if err := run(context.Background(), wallet6Options(false), chain, classifier, store, &stdout); err != nil {
		t.Fatalf("%v\n%s", err, stdout.String())
	}
	var printed output
	if err := json.Unmarshal(stdout.Bytes(), &printed); err != nil {
		t.Fatal(err)
	}
	if printed.Classifications[domain.ChainCashTrade] != 3 || printed.Classifications[domain.ChainCashUnattributedOut] != 1 ||
		len(printed.Classifications) != 2 || printed.CatchUpNetTransfers != "-10.16358" {
		t.Fatalf("printed = %#v", printed)
	}
	if len(store.params) != 1 || len(store.params[0].Transfers) != 4 {
		t.Fatalf("initializer params = %#v", store.params)
	}
}

func TestRunRejectsIncompleteTransferLogs(t *testing.T) {
	// balanceOf moved by more than the transfers that were read: a lagging or
	// truncated log answer must never reach the database.
	chain := &fakeChain{chainID: 137, latest: 94700064, balances: map[uint64]domain.Decimal{
		94681377: "1.700022", 94700000: "1.751522",
	}}
	store := &fakeInitializer{}
	var stdout bytes.Buffer
	err := run(context.Background(), wallet6Options(true), chain, &fakeTransfers{transfers: []domain.ChainCashTransfer{wallet6Reward()}}, store, &stdout)
	if err == nil || !strings.Contains(err.Error(), "incomplete") || len(store.params) != 0 {
		t.Fatalf("err=%v calls=%d", err, len(store.params))
	}
	if !strings.Contains(stdout.String(), "incomplete") {
		t.Fatalf("evidence not printed: %s", stdout.String())
	}
}

func TestRunRejectsUnsafeInputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		chain  *fakeChain
		mutate func(*options)
		want   string
	}{
		{name: "wrong chain", chain: &fakeChain{chainID: 1, latest: 94700064}, want: "Polygon"},
		{name: "start block not final", chain: &fakeChain{chainID: 137, latest: 94681400}, want: "not final"},
		{name: "missing actor", chain: &fakeChain{chainID: 137, latest: 94700064}, mutate: func(value *options) { value.Actor = " " }, want: "required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := wallet6Options(true)
			if test.mutate != nil {
				test.mutate(&opts)
			}
			store := &fakeInitializer{}
			transfers := &fakeTransfers{}
			err := run(context.Background(), opts, test.chain, transfers, store, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), test.want) || len(store.params) != 0 || transfers.calls != 0 {
				t.Fatalf("err=%v store=%d transfers=%d", err, len(store.params), transfers.calls)
			}
		})
	}
}

func TestRunPropagatesLedgerRejection(t *testing.T) {
	chain := &fakeChain{chainID: 137, latest: 94700064, balances: map[uint64]domain.Decimal{94681377: "1.700022", 94700000: "1.700022"}}
	store := &fakeInitializer{err: errors.New("chain cash initialization rejected: the account already has a chain cash cursor")}
	var stdout bytes.Buffer
	err := run(context.Background(), wallet6Options(true), chain, &fakeTransfers{}, store, &stdout)
	if err == nil || !strings.Contains(stdout.String(), "already has a chain cash cursor") {
		t.Fatalf("err=%v out=%s", err, stdout.String())
	}
}

func TestMainRequiresRewardSenders(t *testing.T) {
	t.Setenv("POLYGON_ORDER_FILLED_CONFIRMATIONS", "64")
	t.Setenv("CHAIN_CASH_REWARD_SENDERS", "")
	err := mainWithArgs([]string{"--account", "wallet-6"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "CHAIN_CASH_REWARD_SENDERS") {
		t.Fatalf("err = %v", err)
	}
}

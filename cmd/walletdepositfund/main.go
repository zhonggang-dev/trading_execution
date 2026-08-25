package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
)

const (
	polygonChainID             = uint64(137)
	requiredConfirmations      = uint64(64)
	journalSchema              = "trading.wallet_deposit_funding.v2"
	exactExecuteToken          = "FUND_WALLET_6_REMAINING_181_AND_WALLET_7_REMAINING_201_PUSD_TO_DEPOSIT_WALLETS_POLYGON_137"
	defaultCommandTimeout      = 20 * time.Minute
	defaultRequestTimeout      = 10 * time.Second
	defaultReceiptPollInterval = 2 * time.Second
	maximumRPCResponseBytes    = int64(1 << 20)
	minimumTransferGasEstimate = uint64(21_000)
	maximumTransferGasLimit    = uint64(150_000)
	maximumPriorityFeeWei      = uint64(100_000_000_000)
	maximumBaseFeeWei          = uint64(500_000_000_000)
	maximumMaxFeePerGasWei     = uint64(1_000_000_000_000)
	polygonPUSDAddress         = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
	wallet6AccountID           = "wallet-6"
	wallet7AccountID           = "wallet-7"
	wallet6EOA                 = "0x0aefd80df02cc35e81aede40b34e2e961bb4593f"
	wallet7EOA                 = "0xc9ba353781f13ec9507bc0677156814d805fe6d9"
	wallet6Deposit             = "0x635d25519789e40c3794d72a88cbb7f25ac443f8"
	wallet7Deposit             = "0xdd9275d3b1d2c423e19724fcd09c19abb20aa167"
)

type fixedTarget struct {
	accountID, source, recipient string
	amountBaseUnits              uint64
}

var fixedTargets = []fixedTarget{
	{wallet6AccountID, wallet6EOA, wallet6Deposit, 181_000_000},
	{wallet7AccountID, wallet7EOA, wallet7Deposit, 201_000_000},
}

type options struct {
	accountsFile string
	rpcURL       string
	journalFile  string
	executeToken string
	timeout      time.Duration
	expected     map[string]expectedPrestate
}

type expectedPrestate struct {
	sourceBalance    string
	recipientBalance string
	nonce            string
}

type fundingAccount struct {
	address string
	signer  polymarket.DigestSigner
}

type fundingJournal struct {
	Schema                string         `json:"schema"`
	ChainID               uint64         `json:"chain_id"`
	Token                 string         `json:"token"`
	AmountBaseUnits       string         `json:"amount_base_units"`
	RequiredConfirmations uint64         `json:"required_confirmations"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
	Entries               []fundingEntry `json:"entries"`
}

type fundingEntry struct {
	ExecutionAccountID     string    `json:"execution_account_id"`
	Source                 string    `json:"source"`
	Recipient              string    `json:"recipient"`
	AmountBaseUnits        string    `json:"amount_base_units"`
	SourceBalanceBefore    string    `json:"source_balance_before"`
	RecipientBalanceBefore string    `json:"recipient_balance_before"`
	Nonce                  uint64    `json:"nonce"`
	GasLimit               uint64    `json:"gas_limit"`
	MaxPriorityFee         string    `json:"max_priority_fee_per_gas"`
	MaxFee                 string    `json:"max_fee_per_gas"`
	CallData               string    `json:"call_data"`
	SigningDigest          string    `json:"signing_digest"`
	RawTransaction         string    `json:"raw_transaction"`
	TransactionHash        string    `json:"transaction_hash"`
	State                  string    `json:"state"`
	ReceiptBlockNumber     uint64    `json:"receipt_block_number,omitempty"`
	ReceiptBlockHash       string    `json:"receipt_block_hash,omitempty"`
	Confirmations          uint64    `json:"confirmations,omitempty"`
	ConfirmedAt            time.Time `json:"confirmed_at,omitempty"`
}

type publicResult struct {
	DryRun          bool                `json:"dry_run"`
	ChainID         uint64              `json:"chain_id"`
	Token           string              `json:"token"`
	AmountBaseUnits string              `json:"amount_base_units"`
	Entries         []publicEntryResult `json:"entries"`
}

type publicEntryResult struct {
	ExecutionAccountID     string `json:"execution_account_id"`
	Source                 string `json:"source"`
	Recipient              string `json:"recipient"`
	AmountBaseUnits        string `json:"amount_base_units"`
	SourceBalanceBefore    string `json:"source_balance_before"`
	RecipientBalanceBefore string `json:"recipient_balance_before"`
	Nonce                  uint64 `json:"nonce"`
	GasLimit               uint64 `json:"gas_limit"`
	State                  string `json:"state"`
	TransactionHash        string `json:"transaction_hash,omitempty"`
	ReceiptBlockNumber     uint64 `json:"receipt_block_number,omitempty"`
	Confirmations          uint64 `json:"confirmations,omitempty"`
}

func main() {
	opts, err := parseOptions(os.Args[1:])
	if err != nil {
		slog.Error("deposit wallet funding arguments are invalid", "error", err)
		os.Exit(2)
	}
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(root, opts.timeout)
	defer cancel()
	result, err := run(ctx, opts)
	if err != nil {
		slog.Error("deposit wallet funding failed", "error", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		slog.Error("encode deposit wallet funding result", "error", err)
		os.Exit(1)
	}
}

func parseOptions(arguments []string) (options, error) {
	opts := options{
		accountsFile: strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")),
		rpcURL:       strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")),
		journalFile:  strings.TrimSpace(os.Getenv("WALLET_DEPOSIT_FUND_JOURNAL_FILE")),
		timeout:      defaultCommandTimeout,
		expected:     map[string]expectedPrestate{wallet6AccountID: {}, wallet7AccountID: {}},
	}
	w6, w7 := opts.expected[wallet6AccountID], opts.expected[wallet7AccountID]
	flags := flag.NewFlagSet("walletdepositfund", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.accountsFile, "accounts-file", opts.accountsFile, "private Polymarket accounts file")
	flags.StringVar(&opts.rpcURL, "rpc-url", opts.rpcURL, "Polygon JSON-RPC URL")
	flags.StringVar(&opts.journalFile, "journal-file", opts.journalFile, "private durable transfer journal")
	flags.StringVar(&opts.executeToken, "execute-token", "", "exact fixed-plan execution acknowledgement")
	flags.StringVar(&w6.sourceBalance, "expected-wallet6-source-balance", "", "wallet-6 pUSD source balance")
	flags.StringVar(&w6.recipientBalance, "expected-wallet6-recipient-balance", "", "wallet-6 Deposit Wallet pUSD balance")
	flags.StringVar(&w6.nonce, "expected-wallet6-nonce", "", "wallet-6 pending nonce")
	flags.StringVar(&w7.sourceBalance, "expected-wallet7-source-balance", "", "wallet-7 pUSD source balance")
	flags.StringVar(&w7.recipientBalance, "expected-wallet7-recipient-balance", "", "wallet-7 Deposit Wallet pUSD balance")
	flags.StringVar(&w7.nonce, "expected-wallet7-nonce", "", "wallet-7 pending nonce")
	flags.DurationVar(&opts.timeout, "timeout", opts.timeout, "whole-command timeout")
	if err := flags.Parse(arguments); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 || opts.timeout <= 0 || strings.TrimSpace(opts.accountsFile) == "" || strings.TrimSpace(opts.rpcURL) == "" {
		return options{}, fmt.Errorf("accounts file, RPC URL, positive timeout, and no positional arguments are required")
	}
	opts.expected[wallet6AccountID], opts.expected[wallet7AccountID] = w6, w7
	if opts.executeToken != "" && opts.executeToken != exactExecuteToken {
		return options{}, fmt.Errorf("execute token does not exactly match the fixed remaining-balance deposit-wallet plan")
	}
	if opts.executeToken == exactExecuteToken {
		if strings.TrimSpace(opts.journalFile) == "" {
			return options{}, fmt.Errorf("execute mode requires a journal file")
		}
		for id, expected := range opts.expected {
			if !canonicalUint(expected.sourceBalance) || !canonicalUint(expected.recipientBalance) || !canonicalUint64(expected.nonce) {
				return options{}, fmt.Errorf("execute mode requires canonical source balance, recipient balance, and nonce for %s", id)
			}
		}
	}
	return opts, nil
}

func run(ctx context.Context, opts options) (publicResult, error) {
	accounts, err := polymarket.LoadTradingAccounts(ctx, polymarket.WalletLoadParams{Path: opts.accountsFile})
	if err != nil {
		return publicResult{}, err
	}
	selected, err := selectAccounts(accounts)
	if err != nil {
		return publicResult{}, err
	}
	rpc, err := newRPCClient(opts.rpcURL)
	if err != nil {
		return publicResult{}, err
	}
	if chainID, err := rpcQuantityUint64(ctx, rpc, "eth_chainId", nil, "chain ID"); err != nil || chainID != polygonChainID {
		return publicResult{}, fmt.Errorf("RPC chain ID is not Polygon %d: %w", polygonChainID, err)
	}
	for _, address := range []string{polygonPUSDAddress, wallet6Deposit, wallet7Deposit} {
		if err := requireContract(ctx, rpc, address); err != nil {
			return publicResult{}, err
		}
	}
	execute := opts.executeToken == exactExecuteToken
	journal, exists, err := loadJournal(opts.journalFile)
	if err != nil {
		return publicResult{}, err
	}
	if !execute && exists {
		return publicResult{}, fmt.Errorf("dry-run refuses an existing execution journal")
	}
	if !exists {
		journal, err = prepareJournal(ctx, rpc, selected, opts, execute)
		if err != nil {
			return publicResult{}, err
		}
		if execute {
			if err := createJournal(opts.journalFile, journal); err != nil {
				return publicResult{}, err
			}
		}
	} else if err := validateJournal(journal, opts); err != nil {
		return publicResult{}, err
	}
	if execute {
		for index := range journal.Entries {
			if err := executeEntry(ctx, rpc, selected[journal.Entries[index].ExecutionAccountID], &journal, index, opts.journalFile); err != nil {
				return publicResult{}, err
			}
		}
	}
	return publicView(journal, !execute), nil
}

func selectAccounts(accounts []polymarket.TradingAccount) (map[string]fundingAccount, error) {
	required := map[string]string{wallet6AccountID: wallet6EOA, wallet7AccountID: wallet7EOA}
	result := make(map[string]fundingAccount, 2)
	for _, account := range accounts {
		expected, wanted := required[strings.TrimSpace(account.ExecutionAccountID)]
		if !wanted {
			continue
		}
		if _, duplicate := result[account.ExecutionAccountID]; duplicate {
			return nil, fmt.Errorf("duplicate source account %s", account.ExecutionAccountID)
		}
		if account.SignatureType != polymarket.SignatureTypeEOA || account.Signer == nil || !strings.EqualFold(account.FunderAddress, expected) || !strings.EqualFold(account.Signer.Address(), expected) {
			return nil, fmt.Errorf("%s must be the exact EOA source account before migration", account.ExecutionAccountID)
		}
		result[account.ExecutionAccountID] = fundingAccount{address: expected, signer: account.Signer}
	}
	for id := range required {
		if _, ok := result[id]; !ok {
			return nil, fmt.Errorf("required source account %s is absent", id)
		}
	}
	return result, nil
}

func prepareJournal(ctx context.Context, rpc *rpcClient, accounts map[string]fundingAccount, opts options, execute bool) (fundingJournal, error) {
	now := time.Now().UTC()
	journal := fundingJournal{Schema: journalSchema, ChainID: polygonChainID, Token: polygonPUSDAddress, AmountBaseUnits: "wallet-6=181000000,wallet-7=201000000", RequiredConfirmations: requiredConfirmations, CreatedAt: now, UpdatedAt: now}
	for _, target := range fixedTargets {
		amount := new(big.Int).SetUint64(target.amountBaseUnits)
		sourceBalance, err := readTokenBalance(ctx, rpc, target.source, "latest")
		if err != nil {
			return fundingJournal{}, err
		}
		recipientBalance, err := readTokenBalance(ctx, rpc, target.recipient, "latest")
		if err != nil {
			return fundingJournal{}, err
		}
		nonce, err := readNonce(ctx, rpc, target.source)
		if err != nil {
			return fundingJournal{}, err
		}
		if sourceBalance.Cmp(amount) < 0 {
			return fundingJournal{}, fmt.Errorf("%s pUSD balance is below the fixed remaining migration amount", target.accountID)
		}
		if execute {
			expected := opts.expected[target.accountID]
			if sourceBalance.String() != expected.sourceBalance || recipientBalance.String() != expected.recipientBalance || strconv.FormatUint(nonce, 10) != expected.nonce {
				return fundingJournal{}, fmt.Errorf("%s live prestate differs from approved exact assertions", target.accountID)
			}
		}
		data, err := buildTransferCallData(target.recipient, amount)
		if err != nil {
			return fundingJournal{}, err
		}
		if err := simulateTransfer(ctx, rpc, target.source, data); err != nil {
			return fundingJournal{}, err
		}
		gasLimit, err := estimateTransferGas(ctx, rpc, target.source, data)
		if err != nil {
			return fundingJournal{}, err
		}
		priority, maxFee, err := readFees(ctx, rpc)
		if err != nil {
			return fundingJournal{}, err
		}
		if err := requireGasBalance(ctx, rpc, target.source, gasLimit, maxFee); err != nil {
			return fundingJournal{}, err
		}
		transaction, err := newTransferTransaction(nonce, gasLimit, priority, maxFee, target.recipient, amount)
		if err != nil {
			return fundingJournal{}, err
		}
		signed, err := signType2Transaction(ctx, transaction, accounts[target.accountID].signer, target.source)
		if err != nil {
			return fundingJournal{}, err
		}
		journal.Entries = append(journal.Entries, fundingEntry{
			ExecutionAccountID: target.accountID, Source: target.source, Recipient: target.recipient, AmountBaseUnits: amount.String(),
			SourceBalanceBefore: sourceBalance.String(), RecipientBalanceBefore: recipientBalance.String(),
			Nonce: nonce, GasLimit: gasLimit, MaxPriorityFee: priority.String(), MaxFee: maxFee.String(),
			CallData: "0x" + hex.EncodeToString(transaction.data), SigningDigest: signed.digest,
			RawTransaction: signed.raw, TransactionHash: signed.hash, State: "SIGNED",
		})
	}
	return journal, nil
}

func executeEntry(ctx context.Context, rpc *rpcClient, account fundingAccount, journal *fundingJournal, index int, path string) error {
	entry := &journal.Entries[index]
	if err := reproduceEntry(ctx, account, *entry); err != nil {
		return err
	}
	if entry.State == "CONFIRMED" {
		return revalidateConfirmed(ctx, rpc, *entry)
	}
	if entry.State == "SIGNED" {
		transaction, err := readTransaction(ctx, rpc, entry.TransactionHash)
		if err != nil {
			return err
		}
		if transaction == nil {
			if err := requireUnchangedPrestate(ctx, rpc, *entry); err != nil {
				return err
			}
			result, err := rpc.Call(ctx, "eth_sendRawTransaction", []any{entry.RawTransaction})
			if err != nil {
				return err
			}
			var hash string
			if json.Unmarshal(result, &hash) != nil || strings.ToLower(hash) != entry.TransactionHash {
				return fmt.Errorf("RPC returned a different transaction hash")
			}
		} else if err := validateTransaction(*transaction, *entry); err != nil {
			return err
		}
		entry.State = "BROADCAST"
		journal.UpdatedAt = time.Now().UTC()
		if err := saveJournal(path, *journal); err != nil {
			return err
		}
	}
	if entry.State != "BROADCAST" {
		return fmt.Errorf("unsupported journal state %q", entry.State)
	}
	receipt, confirmations, err := waitForConfirmedReceipt(ctx, rpc, *entry)
	if err != nil {
		return err
	}
	entry.State = "CONFIRMED"
	entry.ReceiptBlockNumber = receipt.blockNumber
	entry.ReceiptBlockHash = receipt.blockHash
	entry.Confirmations = confirmations
	entry.ConfirmedAt = time.Now().UTC()
	journal.UpdatedAt = entry.ConfirmedAt
	return saveJournal(path, *journal)
}

func reproduceEntry(ctx context.Context, account fundingAccount, entry fundingEntry) error {
	if account.address != entry.Source || account.signer == nil {
		return fmt.Errorf("journal signer identity is unavailable")
	}
	priority, ok := new(big.Int).SetString(entry.MaxPriorityFee, 10)
	if !ok {
		return fmt.Errorf("journal priority fee is invalid")
	}
	maxFee, ok := new(big.Int).SetString(entry.MaxFee, 10)
	if !ok {
		return fmt.Errorf("journal max fee is invalid")
	}
	amount, err := entryAmount(entry)
	if err != nil {
		return err
	}
	transaction, err := newTransferTransaction(entry.Nonce, entry.GasLimit, priority, maxFee, entry.Recipient, amount)
	if err != nil {
		return err
	}
	signed, err := signType2Transaction(ctx, transaction, account.signer, entry.Source)
	if err != nil {
		return err
	}
	if "0x"+hex.EncodeToString(transaction.data) != entry.CallData || signed.digest != entry.SigningDigest || signed.raw != entry.RawTransaction || signed.hash != entry.TransactionHash {
		return fmt.Errorf("journaled signed transfer does not reproduce exactly")
	}
	return nil
}

func requireUnchangedPrestate(ctx context.Context, rpc *rpcClient, entry fundingEntry) error {
	source, err := readTokenBalance(ctx, rpc, entry.Source, "latest")
	if err != nil || source.String() != entry.SourceBalanceBefore {
		return fmt.Errorf("source balance changed before broadcast: %w", err)
	}
	recipient, err := readTokenBalance(ctx, rpc, entry.Recipient, "latest")
	if err != nil || recipient.String() != entry.RecipientBalanceBefore {
		return fmt.Errorf("recipient balance changed before broadcast: %w", err)
	}
	nonce, err := readNonce(ctx, rpc, entry.Source)
	if err != nil || nonce != entry.Nonce {
		return fmt.Errorf("source nonce changed before broadcast: %w", err)
	}
	return nil
}

type rpcClient struct {
	url        *url.URL
	httpClient *http.Client
	nextID     uint64
}

func newRPCClient(raw string) (*rpcClient, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) || parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("Polygon RPC URL is invalid")
	}
	client := &http.Client{Timeout: defaultRequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("RPC redirects are disabled") }}
	return &rpcClient{url: parsed, httpClient: client}, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (rpc *rpcClient) Call(ctx context.Context, method string, params []any) ([]byte, error) {
	rpc.nextID++
	payload, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpc.nextID, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, rpc.url.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := rpc.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%s transport: %w", method, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumRPCResponseBytes+1))
	if err != nil || int64(len(body)) > maximumRPCResponseBytes || response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s RPC response is invalid", method)
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      uint64          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil || envelope.JSONRPC != "2.0" || envelope.ID != rpc.nextID {
		return nil, fmt.Errorf("%s RPC envelope is invalid", method)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("%s RPC error %d: %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 {
		return nil, fmt.Errorf("%s RPC result is absent", method)
	}
	return envelope.Result, nil
}

func requireContract(ctx context.Context, rpc *rpcClient, address string) error {
	result, err := rpc.Call(ctx, "eth_getCode", []any{address, "latest"})
	if err != nil {
		return err
	}
	var code string
	if json.Unmarshal(result, &code) != nil || code == "0x" || !strings.HasPrefix(code, "0x") {
		return fmt.Errorf("required contract %s is not deployed", address)
	}
	return nil
}

func readNonce(ctx context.Context, rpc *rpcClient, address string) (uint64, error) {
	latest, err := rpcQuantityUint64(ctx, rpc, "eth_getTransactionCount", []any{address, "latest"}, "latest nonce")
	if err != nil {
		return 0, err
	}
	pending, err := rpcQuantityUint64(ctx, rpc, "eth_getTransactionCount", []any{address, "pending"}, "pending nonce")
	if err != nil || latest != pending {
		return 0, fmt.Errorf("account %s has a pending or unreadable nonce", address)
	}
	return latest, nil
}

func readTokenBalance(ctx context.Context, rpc *rpcClient, owner, block string) (*big.Int, error) {
	data, err := buildBalanceOfCallData(owner)
	if err != nil {
		return nil, err
	}
	result, err := rpc.Call(ctx, "eth_call", []any{map[string]string{"to": polygonPUSDAddress, "data": "0x" + hex.EncodeToString(data)}, block})
	if err != nil {
		return nil, err
	}
	var encoded string
	if json.Unmarshal(result, &encoded) != nil || !strings.HasPrefix(encoded, "0x") || len(encoded) != 66 {
		return nil, fmt.Errorf("balanceOf result is invalid")
	}
	value, ok := new(big.Int).SetString(encoded[2:], 16)
	if !ok {
		return nil, fmt.Errorf("balanceOf result is not hexadecimal")
	}
	return value, nil
}

func simulateTransfer(ctx context.Context, rpc *rpcClient, owner string, data []byte) error {
	result, err := rpc.Call(ctx, "eth_call", []any{map[string]string{"from": owner, "to": polygonPUSDAddress, "value": "0x0", "data": "0x" + hex.EncodeToString(data)}, "pending"})
	if err != nil {
		return err
	}
	var encoded string
	if json.Unmarshal(result, &encoded) != nil || len(encoded) != 66 || !strings.HasSuffix(encoded, "01") {
		return fmt.Errorf("pUSD transfer simulation did not return true")
	}
	return nil
}

func estimateTransferGas(ctx context.Context, rpc *rpcClient, owner string, data []byte) (uint64, error) {
	estimate, err := rpcQuantityUint64(ctx, rpc, "eth_estimateGas", []any{map[string]string{"from": owner, "to": polygonPUSDAddress, "value": "0x0", "data": "0x" + hex.EncodeToString(data)}, "pending"}, "transfer gas")
	if err != nil || estimate < minimumTransferGasEstimate || estimate > maximumTransferGasLimit {
		return 0, fmt.Errorf("transfer gas estimate is invalid: %w", err)
	}
	limit := (estimate*120 + 99) / 100
	if limit > maximumTransferGasLimit {
		return 0, fmt.Errorf("transfer gas limit exceeds cap")
	}
	return limit, nil
}

func readFees(ctx context.Context, rpc *rpcClient) (*big.Int, *big.Int, error) {
	priority, err := rpcQuantityBig(ctx, rpc, "eth_maxPriorityFeePerGas", nil, "priority fee")
	if err != nil {
		return nil, nil, err
	}
	result, err := rpc.Call(ctx, "eth_getBlockByNumber", []any{"pending", false})
	if err != nil {
		return nil, nil, err
	}
	var block struct {
		BaseFee string `json:"baseFeePerGas"`
	}
	if json.Unmarshal(result, &block) != nil {
		return nil, nil, fmt.Errorf("pending block is invalid")
	}
	base, err := parseQuantityBig(block.BaseFee, "base fee")
	if err != nil || priority.Sign() <= 0 || priority.Cmp(new(big.Int).SetUint64(maximumPriorityFeeWei)) > 0 || base.Sign() <= 0 || base.Cmp(new(big.Int).SetUint64(maximumBaseFeeWei)) > 0 {
		return nil, nil, fmt.Errorf("Polygon fee quote exceeds fixed caps")
	}
	maxFee := new(big.Int).Add(new(big.Int).Mul(base, big.NewInt(2)), priority)
	if maxFee.Cmp(new(big.Int).SetUint64(maximumMaxFeePerGasWei)) > 0 {
		return nil, nil, fmt.Errorf("computed max fee exceeds cap")
	}
	return priority, maxFee, nil
}

func requireGasBalance(ctx context.Context, rpc *rpcClient, owner string, gas uint64, maxFee *big.Int) error {
	balance, err := rpcQuantityBig(ctx, rpc, "eth_getBalance", []any{owner, "pending"}, "POL balance")
	required := new(big.Int).Mul(new(big.Int).SetUint64(gas), maxFee)
	if err != nil || balance.Cmp(required) < 0 {
		return fmt.Errorf("%s POL balance cannot cover capped gas: %w", owner, err)
	}
	return nil
}

type rpcTransaction struct {
	Hash, ChainID, Type, From, To, Nonce, Gas, MaxPriorityFeePerGas, MaxFeePerGas, Value, Input string
}

func readTransaction(ctx context.Context, rpc *rpcClient, hash string) (*rpcTransaction, error) {
	result, err := rpc.Call(ctx, "eth_getTransactionByHash", []any{hash})
	if err != nil || bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return nil, err
	}
	var transaction rpcTransaction
	if json.Unmarshal(result, &transaction) != nil {
		return nil, fmt.Errorf("transaction response is invalid")
	}
	return &transaction, nil
}

func validateTransaction(actual rpcTransaction, expected fundingEntry) error {
	checks := []bool{
		strings.ToLower(actual.Hash) == expected.TransactionHash,
		strings.ToLower(actual.From) == expected.Source,
		strings.ToLower(actual.To) == polygonPUSDAddress,
		strings.ToLower(actual.Input) == expected.CallData,
	}
	chain, e1 := parseQuantityUint64(actual.ChainID, "chain ID")
	typeID, e2 := parseQuantityUint64(actual.Type, "type")
	nonce, e3 := parseQuantityUint64(actual.Nonce, "nonce")
	gas, e4 := parseQuantityUint64(actual.Gas, "gas")
	value, e5 := parseQuantityBig(actual.Value, "value")
	priority, e6 := parseQuantityBig(actual.MaxPriorityFeePerGas, "priority fee")
	maxFee, e7 := parseQuantityBig(actual.MaxFeePerGas, "max fee")
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil ||
		chain != polygonChainID || typeID != 2 || nonce != expected.Nonce || gas != expected.GasLimit ||
		value.Sign() != 0 || priority.String() != expected.MaxPriorityFee || maxFee.String() != expected.MaxFee {
		return fmt.Errorf("RPC transaction numeric identity mismatch")
	}
	for _, ok := range checks {
		if !ok {
			return fmt.Errorf("RPC transaction identity mismatch")
		}
	}
	return nil
}

type rpcReceipt struct {
	TransactionHash, BlockHash, BlockNumber, From, To, Type, Status string
	Logs                                                            []rpcLog `json:"logs"`
}

type rpcLog struct {
	Address, Data, BlockHash, BlockNumber, TransactionHash string
	Topics                                                 []string `json:"topics"`
	Removed                                                *bool    `json:"removed"`
}

type validatedReceipt struct {
	blockNumber uint64
	blockHash   string
}

func waitForConfirmedReceipt(ctx context.Context, rpc *rpcClient, entry fundingEntry) (validatedReceipt, uint64, error) {
	for {
		result, err := rpc.Call(ctx, "eth_getTransactionReceipt", []any{entry.TransactionHash})
		if err != nil {
			return validatedReceipt{}, 0, err
		}
		if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
			if err := sleepContext(ctx, defaultReceiptPollInterval); err != nil {
				return validatedReceipt{}, 0, err
			}
			continue
		}
		var raw rpcReceipt
		if json.Unmarshal(result, &raw) != nil {
			return validatedReceipt{}, 0, fmt.Errorf("receipt response is invalid")
		}
		receipt, err := validateReceipt(raw, entry)
		if err != nil {
			return validatedReceipt{}, 0, err
		}
		latest, err := rpcQuantityUint64(ctx, rpc, "eth_blockNumber", nil, "latest block")
		if err != nil || latest < receipt.blockNumber {
			return validatedReceipt{}, 0, fmt.Errorf("latest block is behind receipt: %w", err)
		}
		confirmations := latest - receipt.blockNumber + 1
		if confirmations < requiredConfirmations {
			if err := sleepContext(ctx, defaultReceiptPollInterval); err != nil {
				return validatedReceipt{}, 0, err
			}
			continue
		}
		if err := verifyConfirmedBalances(ctx, rpc, entry, latest); err != nil {
			return validatedReceipt{}, 0, err
		}
		blockHash, err := readBlockHash(ctx, rpc, receipt.blockNumber)
		if err != nil || blockHash != receipt.blockHash {
			return validatedReceipt{}, 0, fmt.Errorf("receipt block is not canonical")
		}
		return receipt, confirmations, nil
	}
}

func validateReceipt(raw rpcReceipt, entry fundingEntry) (validatedReceipt, error) {
	expectedAmount, amountErr := entryAmount(entry)
	block, err := parseQuantityUint64(raw.BlockNumber, "receipt block")
	typeID, typeErr := parseQuantityUint64(raw.Type, "receipt type")
	status, statusErr := parseQuantityUint64(raw.Status, "receipt status")
	if err != nil || typeErr != nil || statusErr != nil || amountErr != nil || block == 0 || typeID != 2 || status != 1 || strings.ToLower(raw.TransactionHash) != entry.TransactionHash || strings.ToLower(raw.From) != entry.Source || strings.ToLower(raw.To) != polygonPUSDAddress {
		return validatedReceipt{}, fmt.Errorf("receipt identity or success status is invalid")
	}
	blockHash := strings.ToLower(raw.BlockHash)
	if len(blockHash) != 66 {
		return validatedReceipt{}, fmt.Errorf("receipt block hash is invalid")
	}
	transferTopic := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	fromTopic := addressTopic(entry.Source)
	toTopic := addressTopic(entry.Recipient)
	matches := 0
	for _, log := range raw.Logs {
		if strings.ToLower(log.Address) == polygonPUSDAddress && len(log.Topics) == 3 && strings.ToLower(log.Topics[0]) == transferTopic && strings.ToLower(log.Topics[1]) == fromTopic && strings.ToLower(log.Topics[2]) == toTopic {
			amount, ok := new(big.Int).SetString(strings.TrimPrefix(log.Data, "0x"), 16)
			if ok && amount.Cmp(expectedAmount) == 0 && strings.ToLower(log.TransactionHash) == entry.TransactionHash && strings.ToLower(log.BlockHash) == blockHash && log.Removed != nil && !*log.Removed {
				matches++
			}
		}
	}
	if matches != 1 {
		return validatedReceipt{}, fmt.Errorf("receipt contains %d exact pUSD Transfer logs; require 1", matches)
	}
	return validatedReceipt{blockNumber: block, blockHash: blockHash}, nil
}

func verifyConfirmedBalances(ctx context.Context, rpc *rpcClient, entry fundingEntry, block uint64) error {
	amount, err := entryAmount(entry)
	if err != nil {
		return err
	}
	sourceBefore, _ := new(big.Int).SetString(entry.SourceBalanceBefore, 10)
	recipientBefore, _ := new(big.Int).SetString(entry.RecipientBalanceBefore, 10)
	wantSource := new(big.Int).Sub(sourceBefore, amount)
	wantRecipient := new(big.Int).Add(recipientBefore, amount)
	tag := fmt.Sprintf("0x%x", block)
	source, err := readTokenBalance(ctx, rpc, entry.Source, tag)
	if err != nil || source.Cmp(wantSource) != 0 {
		return fmt.Errorf("confirmed source balance is not the exact pre-balance minus the fixed migration amount: %w", err)
	}
	recipient, err := readTokenBalance(ctx, rpc, entry.Recipient, tag)
	if err != nil || recipient.Cmp(wantRecipient) != 0 {
		return fmt.Errorf("confirmed recipient balance is not the exact pre-balance plus the fixed migration amount: %w", err)
	}
	return nil
}

func revalidateConfirmed(ctx context.Context, rpc *rpcClient, entry fundingEntry) error {
	result, err := rpc.Call(ctx, "eth_getTransactionReceipt", []any{entry.TransactionHash})
	if err != nil || bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return fmt.Errorf("confirmed receipt is absent: %w", err)
	}
	var raw rpcReceipt
	if json.Unmarshal(result, &raw) != nil {
		return fmt.Errorf("confirmed receipt is invalid")
	}
	receipt, err := validateReceipt(raw, entry)
	if err != nil || receipt.blockNumber != entry.ReceiptBlockNumber || receipt.blockHash != entry.ReceiptBlockHash {
		return fmt.Errorf("confirmed receipt changed: %w", err)
	}
	latest, err := rpcQuantityUint64(ctx, rpc, "eth_blockNumber", nil, "latest block")
	if err != nil || latest < receipt.blockNumber || latest-receipt.blockNumber+1 < requiredConfirmations {
		return fmt.Errorf("confirmed transfer has insufficient confirmations")
	}
	return verifyConfirmedBalances(ctx, rpc, entry, latest)
}

func readBlockHash(ctx context.Context, rpc *rpcClient, number uint64) (string, error) {
	result, err := rpc.Call(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", number), false})
	if err != nil {
		return "", err
	}
	var block struct {
		Hash   string `json:"hash"`
		Number string `json:"number"`
	}
	if json.Unmarshal(result, &block) != nil {
		return "", fmt.Errorf("block response is invalid")
	}
	actual, err := parseQuantityUint64(block.Number, "block number")
	if err != nil || actual != number || len(block.Hash) != 66 {
		return "", fmt.Errorf("block identity mismatch")
	}
	return strings.ToLower(block.Hash), nil
}

func rpcQuantityUint64(ctx context.Context, rpc *rpcClient, method string, params []any, field string) (uint64, error) {
	value, err := rpcQuantityBig(ctx, rpc, method, params, field)
	if err != nil || !value.IsUint64() {
		return 0, fmt.Errorf("%s is not uint64: %w", field, err)
	}
	return value.Uint64(), nil
}

func rpcQuantityBig(ctx context.Context, rpc *rpcClient, method string, params []any, field string) (*big.Int, error) {
	result, err := rpc.Call(ctx, method, params)
	if err != nil {
		return nil, err
	}
	var encoded string
	if json.Unmarshal(result, &encoded) != nil {
		return nil, fmt.Errorf("%s response is invalid", field)
	}
	return parseQuantityBig(encoded, field)
}

func parseQuantityUint64(value, field string) (uint64, error) {
	parsed, err := parseQuantityBig(value, field)
	if err != nil || !parsed.IsUint64() {
		return 0, fmt.Errorf("%s is not uint64: %w", field, err)
	}
	return parsed.Uint64(), nil
}

func parseQuantityBig(value, field string) (*big.Int, error) {
	if !strings.HasPrefix(value, "0x") || len(value) < 3 || (len(value) > 3 && value[2] == '0') {
		return nil, fmt.Errorf("%s is not a canonical hex quantity", field)
	}
	parsed, ok := new(big.Int).SetString(value[2:], 16)
	if !ok || parsed.Sign() < 0 || parsed.BitLen() > 256 {
		return nil, fmt.Errorf("%s is not uint256", field)
	}
	return parsed, nil
}

func addressTopic(address string) string {
	raw, _ := addressBytes(address)
	word := make([]byte, 32)
	copy(word[12:], raw)
	return "0x" + hex.EncodeToString(word)
}

func canonicalUint(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	return ok && parsed.Sign() >= 0 && parsed.BitLen() <= 256
}

func canonicalUint64(value string) bool {
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && strconv.FormatUint(parsed, 10) == value
}

func entryAmount(entry fundingEntry) (*big.Int, error) {
	if !canonicalUint(entry.AmountBaseUnits) || entry.AmountBaseUnits == "0" {
		return nil, fmt.Errorf("journal transfer amount is invalid")
	}
	amount, _ := new(big.Int).SetString(entry.AmountBaseUnits, 10)
	return amount, nil
}

func validateJournal(journal fundingJournal, opts options) error {
	if journal.Schema != journalSchema || journal.ChainID != polygonChainID || journal.Token != polygonPUSDAddress || journal.AmountBaseUnits != "wallet-6=181000000,wallet-7=201000000" || journal.RequiredConfirmations != requiredConfirmations || len(journal.Entries) != len(fixedTargets) {
		return fmt.Errorf("existing journal does not match the fixed transfer plan")
	}
	for index, entry := range journal.Entries {
		target := fixedTargets[index]
		expected := opts.expected[target.accountID]
		if entry.ExecutionAccountID != target.accountID || entry.Source != target.source || entry.Recipient != target.recipient || entry.AmountBaseUnits != strconv.FormatUint(target.amountBaseUnits, 10) || entry.SourceBalanceBefore != expected.sourceBalance || entry.RecipientBalanceBefore != expected.recipientBalance || strconv.FormatUint(entry.Nonce, 10) != expected.nonce || (entry.State != "SIGNED" && entry.State != "BROADCAST" && entry.State != "CONFIRMED") {
			return fmt.Errorf("journal entry %d differs from the approved exact plan", index)
		}
	}
	return nil
}

func loadJournal(path string) (fundingJournal, bool, error) {
	if strings.TrimSpace(path) == "" {
		return fundingJournal{}, false, nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return fundingJournal{}, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return fundingJournal{}, false, fmt.Errorf("journal must be a private regular file")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return fundingJournal{}, false, err
	}
	var journal fundingJournal
	if json.Unmarshal(payload, &journal) != nil {
		return fundingJournal{}, false, fmt.Errorf("journal JSON is invalid")
	}
	return journal, true, nil
}

func createJournal(path string, journal fundingJournal) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	writeErr := encoder.Encode(journal)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	return errors.Join(writeErr, file.Close())
}

func saveJournal(path string, journal fundingJournal) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".wallet-deposit-fund.*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(journal); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return err
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(directoryHandle.Sync(), directoryHandle.Close())
}

func publicView(journal fundingJournal, dryRun bool) publicResult {
	result := publicResult{DryRun: dryRun, ChainID: polygonChainID, Token: polygonPUSDAddress, AmountBaseUnits: journal.AmountBaseUnits}
	for _, entry := range journal.Entries {
		result.Entries = append(result.Entries, publicEntryResult{
			ExecutionAccountID: entry.ExecutionAccountID, Source: entry.Source, Recipient: entry.Recipient,
			AmountBaseUnits:     entry.AmountBaseUnits,
			SourceBalanceBefore: entry.SourceBalanceBefore, RecipientBalanceBefore: entry.RecipientBalanceBefore,
			Nonce: entry.Nonce, GasLimit: entry.GasLimit, State: entry.State,
			TransactionHash: entry.TransactionHash, ReceiptBlockNumber: entry.ReceiptBlockNumber, Confirmations: entry.Confirmations,
		})
	}
	return result
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

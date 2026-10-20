// walletcutover audits and, only with an explicitly reviewed private
// manifest, records the main/wallet-1 reuse baseline. It never signs,
// submits, cancels, redeems, or changes strategy bindings.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	postgresadapter "github.com/UniPat-AI/trading_execution/internal/adapter/postgres"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/walletcutover"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type chainSource struct {
	cash  *evmrpc.ERC20TransferLogReader
	token *evmrpc.OrderFilledEvidenceReader
}

func (source chainSource) ChainID(ctx context.Context) (uint64, error) {
	return source.cash.ChainID(ctx)
}
func (source chainSource) LatestBlock(ctx context.Context) (uint64, error) {
	return source.cash.LatestBlock(ctx)
}
func (source chainSource) BlockHeader(ctx context.Context, block uint64) (string, time.Time, error) {
	return source.cash.BlockHeader(ctx, block)
}
func (source chainSource) BalanceAt(ctx context.Context, wallet string, block uint64) (domain.Decimal, error) {
	return source.cash.BalanceAt(ctx, wallet, block)
}
func (source chainSource) TokenBalanceAt(ctx context.Context, wallet, token string, block uint64) (domain.Decimal, error) {
	value, err := source.token.ReadTokenBalance(ctx, evmrpc.PolymarketConditionalTokensAddress, wallet, token, fmt.Sprintf("0x%x", block))
	if err != nil {
		return "", err
	}
	return domain.ParseDecimal(value)
}

type output struct {
	SchemaVersion string                       `json:"schema_version,omitempty"`
	Mode          string                       `json:"mode"`
	ManifestSHA   string                       `json:"manifest_sha256"`
	BatchID       string                       `json:"batch_id"`
	Chain         walletcutover.ChainReport    `json:"chain"`
	Local         []walletcutover.AccountState `json:"local"`
	Result        *walletcutover.ApplyResult   `json:"result,omitempty"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "walletcutover:", safeCommandError(err))
		os.Exit(1)
	}
}

func safeCommandError(err error) string {
	message := err.Error()
	for _, key := range []string{"TRADING_EXECUTION_DATABASE_URL", "POLYGON_RPC_URL"} {
		if secret := strings.TrimSpace(os.Getenv(key)); secret != "" {
			message = strings.ReplaceAll(message, secret, "<redacted>")
		}
	}
	return message
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("walletcutover", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	manifestPath := flags.String("manifest", "", "private reviewed wallet_reuse_cutover.v1 manifest")
	apply := flags.Bool("apply-baseline", false, "commit the audited accounting baseline")
	dryRun := flags.Bool("database-dry-run", false, "exercise the complete database apply transaction and roll it back")
	confirm := flags.String("confirm-sha256", "", "exact SHA-256 of the reviewed manifest bytes (required for --apply-baseline)")
	actor := flags.String("actor", "", "accountable operator identity")
	reason := flags.String("reason", "", "approved cutover reason")
	timeout := flags.Duration("timeout", 10*time.Minute, "overall RPC/database deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if strings.TrimSpace(*manifestPath) == "" {
		return fmt.Errorf("--manifest is required")
	}
	if *apply && *dryRun {
		return fmt.Errorf("--apply-baseline cannot be combined with --database-dry-run")
	}
	if !*apply && *confirm != "" {
		return fmt.Errorf("--confirm-sha256 is only valid with --apply-baseline")
	}
	if *apply && (strings.TrimSpace(*confirm) == "" || strings.TrimSpace(*actor) == "" || strings.TrimSpace(*reason) == "") {
		return fmt.Errorf("--apply-baseline requires --confirm-sha256, --actor and --reason")
	}
	if *timeout < time.Second {
		return fmt.Errorf("--timeout must be at least one second")
	}

	now := time.Now().UTC()
	manifest, raw, digest, err := walletcutover.ReadManifest(*manifestPath, now)
	if err != nil {
		return err
	}
	if *apply && !strings.EqualFold(strings.TrimSpace(*confirm), digest) {
		return fmt.Errorf("--confirm-sha256 does not match the manifest bytes")
	}
	if *apply && (*actor != manifest.Actor || *reason != manifest.Reason) {
		return fmt.Errorf("--actor and --reason must exactly match the reviewed manifest")
	}

	databaseURL := strings.TrimSpace(os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
	if databaseURL == "" {
		return fmt.Errorf("TRADING_EXECUTION_DATABASE_URL is required")
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	store, err := postgresadapter.NewWalletCutoverStore(database)
	if err != nil {
		return err
	}
	// Replay is deliberately checked before expiry, RPC configuration, and the
	// fresh chain audit. A lost response for an already committed exact batch
	// must remain recoverable even after the reviewed file has expired.
	if replaySource, ok := any(store).(walletcutover.AppliedBatchSource); ok {
		replay, found, err := replaySource.LookupApplied(ctx, manifest.BatchID, digest)
		if err != nil {
			return err
		}
		if found {
			mode := "applied-replay"
			if *apply || *dryRun {
				mode = "apply-baseline-replay"
			}
			result := output{SchemaVersion: walletcutover.AppliedEvidenceSchemaVersion, Mode: mode, ManifestSHA: digest, BatchID: manifest.BatchID, Result: &replay}
			return json.NewEncoder(stdout).Encode(result)
		}
	}

	rpcURL := strings.TrimSpace(os.Getenv("POLYGON_RPC_URL"))
	if rpcURL == "" {
		return fmt.Errorf("POLYGON_RPC_URL is required for a new audit/apply")
	}
	confirmations, err := configuredConfirmations()
	if err != nil {
		return err
	}
	if manifest.EvidenceBlock.Confirmations < confirmations {
		return fmt.Errorf("manifest evidence confirmations %d are below configured %d", manifest.EvidenceBlock.Confirmations, confirmations)
	}
	cash, err := evmrpc.NewERC20TransferLogReader(evmrpc.ERC20TransferLogParams{
		RPCURL: rpcURL, TokenAddress: polymarket.PUSDAddress,
		ExchangeAddresses: []string{evmrpc.PolygonCTFExchangeV2Address, evmrpc.PolygonNegRiskCTFExchangeV2Address},
		Decimals:          6, RequestTimeout: *timeout,
	})
	if err != nil {
		return err
	}
	token, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{
		RPCURL: rpcURL, RequiredConfirmations: 1, RequestTimeout: *timeout,
	})
	if err != nil {
		return err
	}
	service, err := walletcutover.New(walletcutover.Params{Chain: chainSource{cash: cash, token: token}, Store: store, Now: func() time.Time { return now }})
	if err != nil {
		return err
	}
	chain, local, err := service.Audit(ctx, manifest)
	if err != nil {
		return err
	}
	result := output{Mode: "audit", ManifestSHA: digest, BatchID: manifest.BatchID, Chain: chain, Local: local}
	if *dryRun || *apply {
		operator := manifest.Actor
		reasonText := manifest.Reason
		if *apply {
			operator, reasonText = *actor, *reason
		}
		applied, err := service.Apply(ctx, walletcutover.ApplyParams{
			Manifest: manifest, ManifestRaw: raw, ManifestSHA: digest, Actor: operator,
			Reason: reasonText, Chain: chain, Commit: *apply,
		})
		if err != nil {
			return err
		}
		if *dryRun {
			result.Mode = "database-dry-run"
		}
		if *apply {
			result.Mode = "apply-baseline"
			result.SchemaVersion = walletcutover.AppliedEvidenceSchemaVersion
		}
		result.Result = &applied
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func configuredConfirmations() (uint64, error) {
	raw := strings.TrimSpace(os.Getenv("POLYGON_ORDER_FILLED_CONFIRMATIONS"))
	if raw == "" {
		return 0, fmt.Errorf("POLYGON_ORDER_FILLED_CONFIRMATIONS is required")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("POLYGON_ORDER_FILLED_CONFIRMATIONS must be a positive integer")
	}
	return value, nil
}

// baselineamend derives, and only with explicit confirmation records, one
// audited external-position baseline amendment. It never signs, submits,
// cancels, redeems, or changes the position ledger, lots, fills or cash.
//
//	baselineamend --account main --token <id> --kind add|retire \
//	    --actor OP --reason TEXT [--block N] [--note TEXT]            # dry run
//	baselineamend ... --block N --apply --confirm-id <amendment id>   # record
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
	"github.com/UniPat-AI/trading_execution/internal/service/baselineamend"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type output struct {
	Mode      string                   `json:"mode"`
	Inserted  *bool                    `json:"inserted,omitempty"`
	Amendment domain.BaselineAmendment `json:"amendment"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "baselineamend:", redact(err.Error()))
		os.Exit(1)
	}
}

func redact(message string) string {
	for _, key := range []string{"TRADING_EXECUTION_DATABASE_URL", "POLYGON_RPC_URL"} {
		if secret := strings.TrimSpace(os.Getenv(key)); secret != "" {
			message = strings.ReplaceAll(message, secret, "<redacted>")
		}
	}
	return message
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("baselineamend", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	account := flags.String("account", "", "execution account id")
	token := flags.String("token", "", "decimal ERC-1155 token id")
	kind := flags.String("kind", "", "add (register an uncaptured residual) or retire (baseline burned on-chain)")
	actor := flags.String("actor", "", "accountable operator identity")
	reason := flags.String("reason", "", "approved amendment reason")
	note := flags.String("note", "", "optional evidence note, e.g. the burning transaction hash")
	block := flags.Uint64("block", 0, "evidence block number (default: latest minus configured confirmations); required with --apply")
	maxAdd := flags.String("max-add-shares", "0.1", "refuse a residual larger than this many shares")
	apply := flags.Bool("apply", false, "append the amendment instead of only printing it")
	confirm := flags.String("confirm-id", "", "exact amendment id printed by the dry run (required with --apply)")
	timeout := flags.Duration("timeout", 2*time.Minute, "overall RPC/database deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	var amendmentKind domain.BaselineAmendmentKind
	switch strings.ToLower(strings.TrimSpace(*kind)) {
	case "add":
		amendmentKind = domain.BaselineAmendmentAddResidual
	case "retire":
		amendmentKind = domain.BaselineAmendmentRetireConsumed
	default:
		return fmt.Errorf("--kind must be add or retire")
	}
	if strings.TrimSpace(*account) == "" || strings.TrimSpace(*token) == "" ||
		strings.TrimSpace(*actor) == "" || strings.TrimSpace(*reason) == "" {
		return fmt.Errorf("--account, --token, --actor and --reason are required")
	}
	if *apply && (*block == 0 || strings.TrimSpace(*confirm) == "") {
		return fmt.Errorf("--apply requires --block and --confirm-id from a dry run")
	}
	if !*apply && *confirm != "" {
		return fmt.Errorf("--confirm-id is only valid with --apply")
	}
	maxShares, err := domain.ParseDecimal(*maxAdd)
	if err != nil {
		return fmt.Errorf("--max-add-shares: %w", err)
	}

	databaseURL := strings.TrimSpace(os.Getenv("TRADING_EXECUTION_DATABASE_URL"))
	rpcURL := strings.TrimSpace(os.Getenv("POLYGON_RPC_URL"))
	if databaseURL == "" || rpcURL == "" {
		return fmt.Errorf("TRADING_EXECUTION_DATABASE_URL and POLYGON_RPC_URL are required")
	}
	confirmations, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("POLYGON_ORDER_FILLED_CONFIRMATIONS")), 10, 64)
	if err != nil || confirmations == 0 {
		return fmt.Errorf("POLYGON_ORDER_FILLED_CONFIRMATIONS must be a positive integer")
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
	store, err := postgresadapter.NewBaselineAmendmentStore(database)
	if err != nil {
		return err
	}
	facts, err := store.Facts(ctx, *account, *token)
	if err != nil {
		return err
	}
	identity, err := resolveIdentity(amendmentKind, facts)
	if err != nil {
		return err
	}

	cash, err := evmrpc.NewERC20TransferLogReader(evmrpc.ERC20TransferLogParams{
		RPCURL: rpcURL, TokenAddress: polymarket.PUSDAddress,
		ExchangeAddresses: []string{evmrpc.PolygonCTFExchangeV2Address, evmrpc.PolygonNegRiskCTFExchangeV2Address},
		Decimals:          6, RequestTimeout: *timeout,
	})
	if err != nil {
		return err
	}
	tokens, err := evmrpc.NewOrderFilledEvidenceReader(evmrpc.OrderFilledEvidenceParams{
		RPCURL: rpcURL, RequiredConfirmations: 1, RequestTimeout: *timeout,
	})
	if err != nil {
		return err
	}
	if cid, err := cash.ChainID(ctx); err != nil || cid != 137 {
		return fmt.Errorf("RPC is not Polygon mainnet (chain id %d, err %v)", cid, err)
	}
	latest, err := cash.LatestBlock(ctx)
	if err != nil {
		return err
	}
	evidenceBlock := *block
	if evidenceBlock == 0 {
		if latest <= confirmations {
			return fmt.Errorf("chain head %d is not past %d confirmations", latest, confirmations)
		}
		evidenceBlock = latest - confirmations
	}
	if latest < evidenceBlock || latest-evidenceBlock < confirmations {
		return fmt.Errorf("block %d has fewer than %d confirmations", evidenceBlock, confirmations)
	}
	hash, blockTime, err := cash.BlockHeader(ctx, evidenceBlock)
	if err != nil {
		return err
	}
	balance, err := tokens.ReadTokenBalance(ctx, evmrpc.PolymarketConditionalTokensAddress,
		facts.WalletAddress, strings.TrimSpace(*token), fmt.Sprintf("0x%x", evidenceBlock))
	if err != nil {
		return err
	}
	chainShares, err := domain.ParseDecimal(balance)
	if err != nil {
		return err
	}

	amendment, err := baselineamend.Build(baselineamend.Input{
		Kind: amendmentKind, ExecutionAccountID: *account, WalletAddress: facts.WalletAddress,
		TokenID: *token, Identity: identity, ChainShares: chainShares,
		ManagedShares: facts.ManagedShares, BaselineRemaining: facts.BaselineRemaining,
		Block:        baselineamend.Block{Number: evidenceBlock, Hash: hash, Confirmations: latest - evidenceBlock, Time: blockTime},
		MaxAddShares: maxShares, Actor: *actor, Reason: *reason, Note: *note,
	})
	if err != nil {
		return err
	}
	result := output{Mode: "dry-run", Amendment: amendment}
	if *apply {
		if amendment.AmendmentID != strings.TrimSpace(*confirm) {
			return fmt.Errorf("--confirm-id does not match the derived amendment id %s", amendment.AmendmentID)
		}
		inserted, err := store.Append(ctx, amendment)
		if err != nil {
			return err
		}
		result.Mode = "apply"
		result.Inserted = &inserted
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// resolveIdentity takes the exact identity reconciliation will compare from
// the ledger: a residual follows the managed position, a retirement the
// sealed baseline item. Sources that disagree, or an unknown neg_risk, refuse.
func resolveIdentity(kind domain.BaselineAmendmentKind, facts postgresadapter.BaselineAmendmentFacts) (baselineamend.Identity, error) {
	source := facts.Baseline
	label := "baseline item"
	if kind == domain.BaselineAmendmentAddResidual {
		source, label = facts.Position, "managed position"
	}
	if source == nil {
		return baselineamend.Identity{}, fmt.Errorf("token has no %s to take its identity from", label)
	}
	if source.NegRisk == nil {
		return baselineamend.Identity{}, fmt.Errorf("neg_risk of the %s is unknown; refusing to guess", label)
	}
	identity := baselineamend.Identity{
		ConditionID: source.ConditionID, OutcomeIndex: source.OutcomeIndex,
		OutcomeName: source.OutcomeName, NegRisk: *source.NegRisk,
	}
	if other := facts.Baseline; kind == domain.BaselineAmendmentAddResidual && other != nil {
		if other.ConditionID != identity.ConditionID || other.OutcomeName != identity.OutcomeName ||
			(other.NegRisk != nil && *other.NegRisk != identity.NegRisk) {
			return baselineamend.Identity{}, fmt.Errorf("position and baseline identities of the token disagree")
		}
	}
	return identity, nil
}

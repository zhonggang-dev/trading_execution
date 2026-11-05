package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/evmrpc"
	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarket"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// RebasedSaleManifest contains exact public event identities and reviewed lot
// allocations. Funding addresses and RPC credentials never enter this payload.
type RebasedSaleManifest struct {
	Sales []RebasedSale `json:"sales"`
}
type RebasedSale struct {
	Account                   string    `json:"execution_account_id"`
	LotID                     string    `json:"lot_id"`
	MarketID                  string    `json:"market_id"`
	ConditionID               string    `json:"condition_id"`
	TokenID                   string    `json:"token_id"`
	TransactionHash           string    `json:"transaction_hash"`
	OrderHash                 string    `json:"order_hash"`
	BlockNumber               uint64    `json:"block_number"`
	BlockHash                 string    `json:"block_hash"`
	FillLogIndex              uint64    `json:"fill_log_index"`
	TradeShares               string    `json:"trade_shares"`
	TradeGross                string    `json:"trade_gross_proceeds"`
	TradeFee                  string    `json:"trade_fee"`
	Shares                    string    `json:"allocated_shares"`
	Gross                     string    `json:"allocated_gross_proceeds"`
	Fee                       string    `json:"allocated_fee"`
	Principal                 string    `json:"allocated_principal"`
	SoldAt                    time.Time `json:"sold_at"`
	RebaseEventID             string    `json:"rebase_account_event_id"`
	OfficialActivitySHA256    string    `json:"official_activity_sha256"`
	OfficialActivityTimestamp int64     `json:"official_activity_timestamp"`
}
type HistoricalSaleVerifier interface {
	ReadHistoricalSell(context.Context, evmrpc.OrderFilledEvidenceRequest, string) (evmrpc.HistoricalSellEvidence, error)
}
type RebasedSaleReport struct {
	ManifestSHA256      string        `json:"manifest_sha256"`
	Applied             bool          `json:"applied"`
	AlreadyApplied      bool          `json:"already_applied"`
	Rows                int           `json:"rows"`
	CashEffect          string        `json:"cash_effect"`
	OriginalFactsBefore string        `json:"original_facts_before"`
	OriginalFactsAfter  string        `json:"original_facts_after"`
	Sales               []RebasedSale `json:"sales"`
}

var rebasedHash = regexp.MustCompile(`^0x[0-9a-f]{64}$`)

func DecodeRebasedSaleManifest(raw []byte) (RebasedSaleManifest, error) {
	var m RebasedSaleManifest
	if len(raw) > 64*1024 {
		return m, fmt.Errorf("manifest too large")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, fmt.Errorf("invalid manifest")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return m, fmt.Errorf("trailing manifest data")
	}
	if len(m.Sales) != 2 {
		return m, fmt.Errorf("this repair requires exactly two reviewed sales")
	}
	seenAccount, seenLot := map[string]bool{}, map[string]bool{}
	for _, s := range m.Sales {
		if s.Account == "" || s.LotID == "" || len(s.Account) > 128 || len(s.LotID) > 256 || strings.ContainsAny(s.Account+s.LotID, "\x00\r\n") || seenAccount[s.Account] || seenLot[s.LotID] {
			return m, fmt.Errorf("invalid or repeated sale identity")
		}
		seenAccount[s.Account], seenLot[s.LotID] = true, true
		if !rebasedHash.MatchString(s.TransactionHash) || !rebasedHash.MatchString(s.OrderHash) || !rebasedHash.MatchString(s.BlockHash) || !rebasedHash.MatchString(s.ConditionID) || s.TokenID == "" || s.MarketID == "" || s.RebaseEventID == "" || s.BlockNumber == 0 || s.BlockNumber > uint64(^uint64(0)>>1) || s.FillLogIndex > uint64(^uint64(0)>>1) || s.SoldAt.IsZero() || s.SoldAt.Nanosecond() != 0 || s.OfficialActivityTimestamp != s.SoldAt.Unix() || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(s.OfficialActivitySHA256) {
			return m, fmt.Errorf("invalid sale evidence identity")
		}
		token, ok := new(big.Int).SetString(s.TokenID, 10)
		if !ok || token.Sign() < 0 || token.BitLen() > 256 || token.String() != s.TokenID {
			return m, fmt.Errorf("invalid token identity")
		}
		amounts := []string{s.TradeShares, s.TradeGross, s.TradeFee, s.Shares, s.Gross, s.Fee, s.Principal}
		ns := make([]*big.Rat, len(amounts))
		for i, a := range amounts {
			v, e := domain.ParseDecimal(a)
			if e != nil || v.Canonical().String() != a {
				return m, fmt.Errorf("amounts must be canonical decimals")
			}
			ns[i], _ = v.Multiply("1")
			if ns[i].Sign() < 0 {
				return m, fmt.Errorf("negative sale amount")
			}
		}
		if ns[0].Sign() <= 0 || ns[3].Sign() <= 0 || ns[6].Sign() <= 0 || ns[3].Cmp(ns[0]) > 0 || ns[2].Cmp(ns[1]) > 0 || ns[5].Cmp(ns[4]) > 0 || new(big.Rat).Mul(ns[4], ns[0]).Cmp(new(big.Rat).Mul(ns[1], ns[3])) != 0 || new(big.Rat).Mul(ns[5], ns[0]).Cmp(new(big.Rat).Mul(ns[2], ns[3])) != 0 {
			return m, fmt.Errorf("invalid exact proportional sale allocation")
		}
	}
	sort.Slice(m.Sales, func(i, j int) bool { return m.Sales[i].Account < m.Sales[j].Account })
	return m, nil
}

// RunRebasedHistoricalSales never writes existing financial relations. It
// verifies external receipts before locking rows, inserts only this audit
// relation, and rolls back by default. The database role must be maintenance.
func RunRebasedHistoricalSales(ctx context.Context, db *sql.DB, verifier HistoricalSaleVerifier, raw []byte, apply bool, confirmation, actor, reason string) (RebasedSaleReport, error) {
	m, err := DecodeRebasedSaleManifest(raw)
	if err != nil {
		return RebasedSaleReport{}, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if db == nil || verifier == nil {
		return RebasedSaleReport{}, fmt.Errorf("database and evidence verifier required")
	}
	if actor == "" || reason == "" || len(actor) > 256 || len(reason) > 1024 || strings.ContainsAny(actor+reason, "\x00\r\n") {
		return RebasedSaleReport{}, fmt.Errorf("operator and reason required")
	}
	if apply && confirmation != hash {
		return RebasedSaleReport{}, fmt.Errorf("apply requires exact reviewed manifest SHA256")
	}
	evidence := make([]evmrpc.HistoricalSellEvidence, len(m.Sales))
	wallets := make([]string, len(m.Sales))
	for i, s := range m.Sales {
		if err = db.QueryRowContext(ctx, `SELECT LOWER(wallet_address) FROM execution_accounts WHERE execution_account_id=$1`, s.Account).Scan(&wallets[i]); err != nil {
			return RebasedSaleReport{}, fmt.Errorf("sale %d account identity unavailable", i+1)
		}
		e, verifyErr := verifier.ReadHistoricalSell(ctx, evmrpc.OrderFilledEvidenceRequest{TransactionHash: s.TransactionHash, OrderHash: s.OrderHash, Maker: wallets[i], Side: evmrpc.OrderSideSell, TokenID: s.TokenID}, polymarket.PUSDAddress)
		if verifyErr != nil {
			return RebasedSaleReport{}, fmt.Errorf("sale %d canonical receipt verification failed", i+1)
		}
		f := e.Fill
		if f.TransactionHash != s.TransactionHash || f.OrderHash != s.OrderHash || f.Maker != wallets[i] || f.Side != evmrpc.OrderSideSell || f.TokenID != s.TokenID || f.LogIndex != s.FillLogIndex || f.BlockNumber != s.BlockNumber || f.BlockHash != s.BlockHash || f.Confirmations < 128 || !e.SoldAt.Equal(s.SoldAt) || !rebasedUnitsEqual(f.MakerAmountBaseUnits, s.TradeShares) || !rebasedUnitsEqual(f.TakerAmountBaseUnits, s.TradeGross) || !rebasedUnitsEqual(f.FeeBaseUnits, s.TradeFee) || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(e.ReceiptSHA256) {
			return RebasedSaleReport{}, fmt.Errorf("sale %d differs from reviewed chain evidence", i+1)
		}
		evidence[i] = e
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return RebasedSaleReport{}, fmt.Errorf("begin historical repair")
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='10s'`); err != nil {
		return RebasedSaleReport{}, fmt.Errorf("set bounded repair transaction")
	}
	for i, s := range m.Sales {
		var wallet string
		if err = tx.QueryRowContext(ctx, `SELECT LOWER(wallet_address) FROM execution_accounts WHERE execution_account_id=$1 FOR UPDATE`, s.Account).Scan(&wallet); err != nil || wallet != wallets[i] {
			return RebasedSaleReport{}, fmt.Errorf("account identity changed")
		}
		var id string
		if err = tx.QueryRowContext(ctx, `SELECT lot_id FROM position_lots WHERE lot_id=$1 AND execution_account_id=$2 FOR UPDATE`, s.LotID, s.Account).Scan(&id); err != nil {
			return RebasedSaleReport{}, fmt.Errorf("target lot unavailable")
		}
		var open int
		if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM execution_orders WHERE execution_account_id=$1 AND token_id=$2 AND status NOT IN('FILLED','REJECTED','CANCELLED','EXPIRED')`, s.Account, s.TokenID).Scan(&open); err != nil || open != 0 {
			return RebasedSaleReport{}, fmt.Errorf("target token has nonterminal orders")
		}
	}
	before, err := rebasedOriginalFacts(ctx, tx, m)
	if err != nil {
		return RebasedSaleReport{}, err
	}
	report := RebasedSaleReport{ManifestSHA256: hash, Rows: len(m.Sales), CashEffect: "0", OriginalFactsBefore: before, Sales: m.Sales}
	existing := 0
	for _, s := range m.Sales {
		var batch string
		e := tx.QueryRowContext(ctx, `SELECT manifest_sha256 FROM historical_rebased_lot_sales WHERE lot_id=$1`, s.LotID).Scan(&batch)
		if e == nil {
			if batch != hash {
				return report, fmt.Errorf("conflicting historical sale already recorded")
			}
			existing++
		} else if e != sql.ErrNoRows {
			return report, fmt.Errorf("read historical sale audit")
		}
	}
	if existing != 0 && existing != len(m.Sales) {
		return report, fmt.Errorf("partial historical batch exists")
	}
	if existing == len(m.Sales) {
		report.AlreadyApplied = true
		report.Applied = apply
		report.Rows = 0
		report.OriginalFactsAfter = before
		return report, nil
	}
	for i, s := range m.Sales {
		saleID := fmt.Sprintf("%x", sha256.Sum256([]byte(s.Account+"\x00"+s.LotID+"\x00"+s.TransactionHash+"\x00"+fmt.Sprint(s.FillLogIndex))))
		proof, _ := json.Marshal(map[string]any{"source": "POLYGON_V2_ORDER_FILLED", "receipt_sha256": evidence[i].ReceiptSHA256, "confirmations": evidence[i].Fill.Confirmations, "cash_verified": true, "ctf_verified": true, "canonical_block_verified": true, "official_activity_sha256": s.OfficialActivitySHA256, "official_activity_timestamp": s.OfficialActivityTimestamp, "activity_verification": "reviewed_manifest"})
		_, err = tx.ExecContext(ctx, `INSERT INTO historical_rebased_lot_sales(sale_id,manifest_sha256,execution_account_id,lot_id,market_id,condition_id,token_id,transaction_hash,order_hash,chain_id,block_number,block_hash,fill_log_index,trade_shares,trade_gross_proceeds,trade_fee,allocated_shares,allocated_gross_proceeds,allocated_fee,allocated_principal,sold_at,rebase_account_event_id,cash_effect,evidence,actor,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,137,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,0,$22,$23,$24)`, saleID, hash, s.Account, s.LotID, s.MarketID, s.ConditionID, s.TokenID, s.TransactionHash, s.OrderHash, s.BlockNumber, s.BlockHash, s.FillLogIndex, s.TradeShares, s.TradeGross, s.TradeFee, s.Shares, s.Gross, s.Fee, s.Principal, s.SoldAt, s.RebaseEventID, string(proof), actor, reason)
		if err != nil {
			return report, fmt.Errorf("sale %d audited insert rejected", i+1)
		}
	}
	after, err := rebasedOriginalFacts(ctx, tx, m)
	if err != nil {
		return report, err
	}
	report.OriginalFactsAfter = after
	if after != before {
		return report, fmt.Errorf("existing financial facts changed")
	}
	if apply {
		if err = tx.Commit(); err != nil {
			return report, fmt.Errorf("historical audit commit failed")
		}
		report.Applied = true
	} else if err = tx.Rollback(); err != nil {
		return report, fmt.Errorf("historical audit rollback failed")
	}
	return report, nil
}
func rebasedUnitsEqual(units, amount string) bool {
	n, ok := new(big.Int).SetString(units, 10)
	if !ok || n.Sign() < 0 || n.String() != units {
		return false
	}
	d, e := domain.ParseDecimal(amount)
	if e != nil {
		return false
	}
	v, _ := d.Multiply("1000000")
	return v.IsInt() && v.Num().Cmp(n) == 0
}
func rebasedOriginalFacts(ctx context.Context, tx *sql.Tx, m RebasedSaleManifest) (string, error) {
	ids := []string{}
	lots := []string{}
	for _, s := range m.Sales {
		ids = append(ids, s.Account)
		lots = append(lots, s.LotID)
	}
	accounts, _ := json.Marshal(ids)
	lotIDs, _ := json.Marshal(lots)
	var hash string
	err := tx.QueryRowContext(ctx, `SELECT jsonb_build_object(
 'accounts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY a.execution_account_id) FROM execution_accounts a WHERE a.execution_account_id IN(SELECT jsonb_array_elements_text($1::jsonb))),
 'lots',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.lot_id) FROM position_lots l WHERE l.lot_id IN(SELECT jsonb_array_elements_text($2::jsonb))),
 'positions',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.execution_account_id,p.token_id) FROM execution_positions p WHERE EXISTS(SELECT 1 FROM position_lots l WHERE l.lot_id IN(SELECT jsonb_array_elements_text($2::jsonb)) AND l.execution_account_id=p.execution_account_id AND l.token_id=p.token_id)),
 'account_events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.account_event_id) FROM execution_account_events e WHERE e.execution_account_id IN(SELECT jsonb_array_elements_text($1::jsonb))),
 'position_events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.position_event_id) FROM position_events e WHERE e.execution_account_id IN(SELECT jsonb_array_elements_text($1::jsonb)))
 )::text`, string(accounts), string(lotIDs)).Scan(&hash)
	if err != nil {
		return "", fmt.Errorf("fingerprint original financial facts")
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(hash))), nil
}

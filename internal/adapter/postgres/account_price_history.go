package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	history "github.com/UniPat-AI/trading_execution/internal/service/accountpricehistory"
)

type AccountPriceHistoryStore struct{ db *sql.DB }

func NewAccountPriceHistoryStore(db *sql.DB) *AccountPriceHistoryStore {
	return &AccountPriceHistoryStore{db: db}
}
func (s *AccountPriceHistoryStore) Lock(ctx context.Context) (func(), bool, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(7146248100123)`).Scan(&ok); err != nil {
		conn.Close()
		return nil, false, err
	}
	if !ok {
		conn.Close()
		return func() {}, false, nil
	}
	return func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(c, `SELECT pg_advisory_unlock(7146248100123)`); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}, true, nil
}
func (s *AccountPriceHistoryStore) Save(ctx context.Context, r history.Result) error {
	if err := r.Window.Asset.Validate(); err != nil {
		return err
	}
	if r.Window.To.Before(r.Window.From) {
		return fmt.Errorf("invalid history window")
	}
	for _, p := range r.Series.Points {
		if !history.ValidPrice(p.Price) || p.At.IsZero() || p.At.After(r.Window.To) || p.ResolutionSeconds < 0 || (p.ValidUntil != nil && p.ValidUntil.Before(p.At)) || (p.SourceAt != nil && p.SourceAt.After(p.At)) {
			return fmt.Errorf("invalid history point")
		}
	}
	for _, p := range r.Series.Settlements {
		if !history.ValidPrice(p.Price) || p.SourceReference == "" || p.EffectiveAt.IsZero() {
			return fmt.Errorf("invalid settlement evidence")
		}
	}
	r.Series.Points = latestHistoryPoints(r.Series.Points)
	history.SortSeries(&r.Series)
	// Observation timestamps remain in the payload; equality ignores fetch time.
	canonical := r.Series
	canonical.Points = append([]history.Point(nil), canonical.Points...)
	canonical.Settlements = append([]history.Settlement(nil), canonical.Settlements...)
	canonical.Coverage = append([]history.Coverage(nil), canonical.Coverage...)
	for i := range canonical.Points {
		canonical.Points[i].FetchedAt = time.Time{}
	}
	for i := range canonical.Settlements {
		canonical.Settlements[i].FetchedAt = time.Time{}
	}
	for i := range canonical.Coverage {
		canonical.Coverage[i].FetchedAt = time.Time{}
	}
	evidence, _ := json.Marshal(canonical)
	sum := fmt.Sprintf("%x", sha256.Sum256(evidence))
	asset, _ := json.Marshal(r.Window.Asset)
	series, err := json.Marshal(r.Series)
	if err != nil {
		return err
	}
	if len(series) > 8<<20 {
		return fmt.Errorf("history window exceeds budget")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,71462481))`, r.Window.Asset.Key()); err != nil {
		return err
	}
	var id int64
	err = tx.QueryRowContext(ctx, `INSERT INTO account_price_history_windows(asset_key,asset,window_from,window_to,evidence_sha256,series) VALUES($1,$2::jsonb,$3,$4,$5,$6::jsonb) ON CONFLICT(asset_key,window_from,window_to,evidence_sha256) DO NOTHING RETURNING history_id`, r.Window.Asset.Key(), string(asset), r.Window.From, r.Window.To, sum, string(series)).Scan(&id)
	if err == sql.ErrNoRows {
		err = tx.QueryRowContext(ctx, `SELECT history_id FROM account_price_history_windows WHERE asset_key=$1 AND window_from=$2 AND window_to=$3 AND evidence_sha256=$4`, r.Window.Asset.Key(), r.Window.From, r.Window.To, sum).Scan(&id)
	}
	if err != nil {
		return err
	}
	if len(r.Series.Points) > 0 {
		raw, _ := json.Marshal(r.Series.Points)
		_, err = tx.ExecContext(ctx, `INSERT INTO account_price_history_points(asset_key,point_at,valid_until,fetched_at,history_id,point)
 SELECT $1,(p->>'at')::timestamptz,(p->>'valid_until')::timestamptz,(p->>'fetched_at')::timestamptz,$2,p FROM jsonb_array_elements($3::jsonb)p
 ON CONFLICT(asset_key,point_at) DO UPDATE SET valid_until=EXCLUDED.valid_until,fetched_at=EXCLUDED.fetched_at,history_id=EXCLUDED.history_id,point=EXCLUDED.point WHERE EXCLUDED.fetched_at>=account_price_history_points.fetched_at`, r.Window.Asset.Key(), id, string(raw))
		if err != nil {
			return err
		}
	}
	if len(r.Series.Settlements) > 0 {
		raw, _ := json.Marshal(r.Series.Settlements)
		_, err = tx.ExecContext(ctx, `INSERT INTO account_price_history_settlements(asset_key,effective_at,fetched_at,history_id,settlement)
 SELECT $1,(p->>'effective_at')::timestamptz,(p->>'fetched_at')::timestamptz,$2,p FROM jsonb_array_elements($3::jsonb)p
 ON CONFLICT(asset_key,effective_at) DO UPDATE SET fetched_at=EXCLUDED.fetched_at,history_id=EXCLUDED.history_id,settlement=EXCLUDED.settlement WHERE EXCLUDED.fetched_at>=account_price_history_settlements.fetched_at`, r.Window.Asset.Key(), id, string(raw))
		if err != nil {
			return err
		}
	}
	for _, c := range r.Series.Coverage {
		if err = saveHistoryCoverage(ctx, tx, r.Window.Asset.Key(), id, c); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func saveHistoryCoverage(ctx context.Context, tx *sql.Tx, key string, id int64, c history.Coverage) error {
	if c.State != "complete" && c.State != "missing" && c.State != "error" || c.To.Before(c.From) {
		return fmt.Errorf("invalid coverage state")
	}
	coverageKey := c.State
	if c.State == "complete" {
		var earliest, latest sql.NullTime
		if err := tx.QueryRowContext(ctx, `SELECT MIN(window_from),MAX(window_to) FROM account_price_history_coverage WHERE asset_key=$1 AND state='complete' AND window_to>=$2 AND window_from<=$3`, key, c.From, c.To).Scan(&earliest, &latest); err != nil {
			return err
		}
		if earliest.Valid && earliest.Time.Before(c.From) {
			c.From = earliest.Time
		}
		if latest.Valid && latest.Time.After(c.To) {
			c.To = latest.Time
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM account_price_history_coverage WHERE asset_key=$1 AND state='complete' AND window_to>=$2 AND window_from<=$3`, key, c.From, c.To); err != nil {
			return err
		}
		coverageKey = "complete:" + strconv.FormatInt(c.From.UnixNano(), 10)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO account_price_history_coverage(asset_key,coverage_key,window_from,window_to,state,reason,fetched_at,history_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
 ON CONFLICT(asset_key,coverage_key) DO UPDATE SET window_from=EXCLUDED.window_from,window_to=EXCLUDED.window_to,state=EXCLUDED.state,reason=EXCLUDED.reason,fetched_at=EXCLUDED.fetched_at,history_id=EXCLUDED.history_id WHERE EXCLUDED.fetched_at>=account_price_history_coverage.fetched_at`, key, coverageKey, c.From, c.To, c.State, c.Reason, c.FetchedAt, id)
	return err
}
func (s *AccountPriceHistoryStore) Load(ctx context.Context, windows []history.Window) (history.Dataset, error) {
	out := history.Dataset{Series: map[string]history.Series{}, Version: "0", ObservedAt: time.Now().UTC()}
	if len(windows) == 0 {
		return out, nil
	}
	if len(windows) > history.MaxWindows {
		return out, fmt.Errorf("history window budget exceeded")
	}
	type request struct {
		Key  string    `json:"key"`
		From time.Time `json:"from"`
		To   time.Time `json:"to"`
	}
	rs := make([]request, 0, len(windows))
	for _, w := range windows {
		if e := w.Asset.Validate(); e != nil {
			return out, e
		}
		rs = append(rs, request{w.Asset.Key(), w.From.Add(-time.Hour), w.To})
	}
	raw, _ := json.Marshal(rs)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	// Read projections, never the growing raw audit window stream. The point
	// budget applies to unique effective inputs, not number of hourly runs.
	query := `WITH requested AS(SELECT * FROM jsonb_to_recordset($1::jsonb)AS r(key text,"from" timestamptz,"to" timestamptz)), input AS(
 SELECT DISTINCT p.asset_key,p.history_id,'point'::text AS kind,p.point::text AS payload FROM requested r JOIN account_price_history_points p ON p.asset_key=r.key AND p.point_at<=r."to" AND (p.point_at>=r."from" OR p.valid_until>=r."from")
 UNION ALL SELECT DISTINCT p.asset_key,p.history_id,'settlement',p.settlement::text FROM requested r JOIN account_price_history_settlements p ON p.asset_key=r.key AND p.effective_at<=r."to"
 UNION ALL SELECT DISTINCT c.asset_key,c.history_id,'coverage',jsonb_build_object('from',c.window_from,'to',c.window_to,'state',c.state,'reason',c.reason,'fetched_at',c.fetched_at)::text FROM requested r JOIN account_price_history_coverage c ON c.asset_key=r.key AND c.window_to>=r."from" AND c.window_from<=r."to")
 SELECT asset_key,history_id,kind,payload FROM input LIMIT 2020001`
	rows, err := tx.QueryContext(ctx, query, string(raw))
	if err != nil {
		return out, err
	}
	defer rows.Close()
	points, other, payloadBytes := 0, 0, 0
	digests := [][32]byte{}
	for rows.Next() {
		var key, kind, payload string
		var id int64
		if err = rows.Scan(&key, &id, &kind, &payload); err != nil {
			return out, err
		}
		payloadBytes += len(payload) + len(key) + len(kind)
		if payloadBytes > 64<<20 {
			return out, fmt.Errorf("history effective input byte budget exceeded")
		}
		series := out.Series[key]
		switch kind {
		case "point":
			var p history.Point
			if err = json.Unmarshal([]byte(payload), &p); err != nil {
				return out, fmt.Errorf("invalid history point projection")
			}
			series.Points = append(series.Points, p)
			points++
		case "settlement":
			var p history.Settlement
			if err = json.Unmarshal([]byte(payload), &p); err != nil {
				return out, fmt.Errorf("invalid history settlement projection")
			}
			series.Settlements = append(series.Settlements, p)
			other++
		case "coverage":
			var p history.Coverage
			if err = json.Unmarshal([]byte(payload), &p); err != nil {
				return out, fmt.Errorf("invalid history coverage projection")
			}
			series.Coverage = append(series.Coverage, p)
			other++
		}
		if points > history.MaxPoints || other > 20000 {
			return out, fmt.Errorf("history effective input budget exceeded")
		}
		out.Series[key] = series
		digests = append(digests, sha256.Sum256([]byte(key+"\x00"+kind+"\x00"+payload)))
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if err = rows.Close(); err != nil {
		return out, err
	}
	for key, series := range out.Series {
		history.SortSeries(&series)
		out.Series[key] = series
	}
	sort.Slice(digests, func(i, j int) bool { return bytes.Compare(digests[i][:], digests[j][:]) < 0 })
	fingerprint := sha256.New()
	for _, digest := range digests {
		_, _ = fingerprint.Write(digest[:])
	}
	out.Version = fmt.Sprintf("%x", fingerprint.Sum(nil))
	if err = tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}

func latestHistoryPoints(points []history.Point) []history.Point {
	byAt := map[int64]history.Point{}
	for _, p := range points {
		k := p.At.UnixNano()
		old, ok := byAt[k]
		if !ok || p.FetchedAt.After(old.FetchedAt) {
			byAt[k] = p
		}
	}
	result := make([]history.Point, 0, len(byAt))
	for _, p := range byAt {
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].At.Before(result[j].At) })
	return result
}
func (s *AccountPriceHistoryStore) Targets(ctx context.Context, now time.Time) ([]history.Window, error) {
	// This reads authoritative lot identities and their actual final disposition.
	// Source is explicit order venue/intent or a verified PM adoption baseline.
	rows, err := s.db.QueryContext(ctx, `SELECT CASE WHEN l.external_adoption_id IS NOT NULL AND b.source='POLYMARKET_DATA_API' THEN 'POLYMARKET' ELSE COALESCE(NULLIF(UPPER(o.intent->>'market_source'),''),NULLIF(UPPER(o.venue),'')) END,l.market_id,l.condition_id,l.token_id,l.outcome_index,NULLIF(l.outcome_name,''),l.opened_at,CASE WHEN l.remaining_shares>0 THEN $1 ELSE COALESCE(h.sold_at,l.closed_at) END
 FROM position_lots l LEFT JOIN execution_orders o ON o.order_id=l.opening_order_id AND o.execution_account_id=l.execution_account_id
 LEFT JOIN execution_external_position_adoptions a ON a.external_adoption_id=l.external_adoption_id AND a.lot_id=l.lot_id
 LEFT JOIN execution_external_position_baselines b ON b.baseline_id=a.baseline_id
 LEFT JOIN historical_rebased_lot_sales h ON h.lot_id=l.lot_id WHERE l.opened_at<=$1 ORDER BY l.opened_at,l.lot_id LIMIT 100001`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grouped := map[string][]history.Window{}
	count := 0
	for rows.Next() {
		var a history.Asset
		var source sql.NullString
		var end sql.NullTime
		var from time.Time
		if err = rows.Scan(&source, &a.MarketID, &a.ConditionID, &a.TokenID, &a.OutcomeIndex, &a.OutcomeName, &from, &end); err != nil {
			return nil, err
		}
		count++
		if count > 100000 {
			return nil, fmt.Errorf("history target budget exceeded")
		}
		if !source.Valid || !end.Valid {
			return nil, fmt.Errorf("history target lacks source or disposition")
		}
		a.MarketSource = source.String
		if err = a.Validate(); err != nil {
			return nil, err
		}
		if end.Time.Before(from) {
			return nil, fmt.Errorf("history target has reversed lifecycle")
		}
		w := history.Window{Asset: a, From: from.UTC(), To: end.Time.UTC()}
		grouped[a.Key()] = append(grouped[a.Key()], w)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	result := []history.Window{}
	for _, ws := range grouped {
		sort.Slice(ws, func(i, j int) bool { return ws[i].From.Before(ws[j].From) })
		merged := []history.Window{}
		for _, w := range ws {
			if len(merged) > 0 && !w.From.After(merged[len(merged)-1].To) {
				if w.To.After(merged[len(merged)-1].To) {
					merged[len(merged)-1].To = w.To
				}
			} else {
				merged = append(merged, w)
			}
		}
		result = append(result, merged...)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Asset.Key() == result[j].Asset.Key() {
			return result[i].From.Before(result[j].From)
		}
		return result[i].Asset.Key() < result[j].Asset.Key()
	})
	return result, nil
}

var _ history.Store = (*AccountPriceHistoryStore)(nil)

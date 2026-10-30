package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	pm "github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
	"github.com/UniPat-AI/trading_execution/internal/service/tradingmonitor"
)

// PipelineMonitorSource makes bounded, read-only observations using existing
// business identities. A transaction deadline also bounds the rows examined by
// PostgreSQL; LIMIT alone does not bound server-side work.
type PipelineMonitorSource struct {
	db         *sql.DB
	stallAfter time.Duration
}

func NewPipelineMonitorSource(db *sql.DB, stallAfter time.Duration) (*PipelineMonitorSource, error) {
	if db == nil || stallAfter <= 0 {
		return nil, fmt.Errorf("pipeline evidence requires a database and positive stall limit")
	}
	return &PipelineMonitorSource{db: db, stallAfter: stallAfter}, nil
}

func (s *PipelineMonitorSource) Observe(ctx context.Context, now time.Time, bindings []tradingmonitor.Binding) ([]pm.Check, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin read-only trading monitor transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout='2000ms'"); err != nil {
		return nil, fmt.Errorf("bound trading monitor statement: %w", err)
	}
	checks := []pm.Check{}
	for _, b := range bindings {
		ids := []string{b.ExecutionAccountID}
		if b.KalshiEnabled {
			ids = append(ids, "kalshi:"+b.ExecutionAccountID)
		}
		for _, accountID := range ids {
			accountChecks, err := s.account(ctx, tx, now, b, accountID)
			if err != nil {
				return checks, err
			}
			checks = append(checks, accountChecks...)
			issues, err := s.issues(ctx, tx, accountID)
			if err != nil {
				return checks, err
			}
			checks = append(checks, issues...)
			progress, err := s.progress(ctx, tx, now, accountID)
			if err != nil {
				return checks, err
			}
			checks = append(checks, progress...)
			venueFailure, err := s.venueFailures(ctx, tx, now, accountID)
			if err != nil {
				return checks, err
			}
			checks = append(checks, venueFailure)
		}
		cycle, err := s.cycle(ctx, tx, now, b)
		if err != nil {
			return checks, err
		}
		checks = append(checks, cycle)
	}
	if err := tx.Commit(); err != nil {
		return checks, fmt.Errorf("finish read-only trading monitor transaction: %w", err)
	}
	return checks, nil
}

func (s *PipelineMonitorSource) venueFailures(ctx context.Context, tx *sql.Tx, now time.Time, accountID string) (pm.Check, error) {
	check := pm.Check{ID: "venue.execution_errors", Scope: accountID, Status: pm.Healthy, Summary: "近期执行未发现具名交易所基础故障", Details: "只统计真实执行证据；不是额外认证/allowance健康探测，正常未成交和市场关闭不计作系统故障"}
	rows, err := tx.QueryContext(ctx, `SELECT order_id,failure_code,updated_at FROM execution_orders
 WHERE execution_account_id=$1 AND updated_at >= $2 AND failure_code IN
 ('CLOB_TRADING_PAUSED','CLOB_AUTH_FAILED','CLOB_INVALID_SIGNATURE','CLOB_INSUFFICIENT_BALANCE_ALLOWANCE','CLOB_RATE_LIMITED','CLOB_SERVER_ERROR',
  'KALSHI_AUTH_FAILED','KALSHI_RATE_LIMITED','KALSHI_SERVER_ERROR','KALSHI_TRANSPORT_FAILED')
 ORDER BY updated_at DESC LIMIT 101`, accountID, now.Add(-15*time.Minute))
	if err != nil {
		return check, fmt.Errorf("read bounded venue failure evidence: %w", err)
	}
	defer rows.Close()
	count := 0
	sample := ""
	code := ""
	last := time.Time{}
	for rows.Next() {
		var id, c string
		var at time.Time
		if err := rows.Scan(&id, &c, &at); err != nil {
			return check, err
		}
		count++
		if sample == "" {
			sample = id
			code = c
			last = at
		}
		switch c {
		case "CLOB_TRADING_PAUSED", "CLOB_AUTH_FAILED", "CLOB_INVALID_SIGNATURE", "CLOB_INSUFFICIENT_BALANCE_ALLOWANCE", "KALSHI_AUTH_FAILED":
			check.Status = pm.Critical
		}
	}
	if err := rows.Err(); err != nil {
		return check, err
	}
	if count >= 3 && check.Status == pm.Healthy {
		check.Status = pm.Warning
	}
	if check.Status != pm.Healthy {
		check.Summary = "近期执行观测到交易所基础异常"
	}
	lastFailure := "none_observed"
	if !last.IsZero() {
		lastFailure = last.UTC().Format(time.RFC3339)
	}
	check.Evidence = map[string]string{"sampled_orders": strconv.Itoa(count), "sample_order_id": sample, "sample_failure_code": code, "last_failure_at": lastFailure, "window": "15m"}
	if count > 100 {
		check.Evidence["sampling_complete"] = "false"
	} else {
		check.Evidence["sampling_complete"] = "true"
	}
	return check, nil
}

func (s *PipelineMonitorSource) issues(ctx context.Context, tx *sql.Tx, accountID string) ([]pm.Check, error) {
	rows, err := tx.QueryContext(ctx, `SELECT issue_id,issue_type,impact_scope,condition_id,token_id
 FROM reconciliation_issues WHERE execution_account_id=$1 AND status='OPEN' AND impact_scope<>'NONE'
 ORDER BY observed_at LIMIT 101`, accountID)
	if err != nil {
		return nil, fmt.Errorf("read bounded reconciliation issue evidence: %w", err)
	}
	defer rows.Close()
	accountCount, scopedCount, total := 0, 0, 0
	accountSample, scopedSample := "", ""
	sampleType, condition, token := "", "", ""
	for rows.Next() {
		var id, kind, scope, c, t string
		if err := rows.Scan(&id, &kind, &scope, &c, &t); err != nil {
			return nil, err
		}
		total++
		if scope == "ACCOUNT" {
			accountCount++
			if accountSample == "" {
				accountSample = id
			}
		} else {
			scopedCount++
			if scopedSample == "" {
				scopedSample = id
				sampleType = kind
				condition = c
				token = t
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	a := pm.Check{ID: "account.reconciliation_issues", Scope: accountID, Status: pm.Healthy, Summary: "未发现账户级未解决对账门禁", Evidence: map[string]string{"sampled_account_issues": strconv.Itoa(accountCount), "sample_issue_id": accountSample}}
	p := pm.Check{ID: "market.reconciliation_issues", Scope: accountID, Status: pm.Healthy, Summary: "未发现市场级未解决对账门禁", Evidence: map[string]string{"sampled_scoped_issues": strconv.Itoa(scopedCount), "sample_issue_id": scopedSample, "sample_issue_type": sampleType, "sample_condition_id": condition, "sample_token_id": token}}
	if accountCount > 0 {
		a.Status = pm.Critical
		a.Summary = "账户存在未解决的全账户对账门禁"
	}
	if scopedCount > 0 {
		p.Status = pm.Warning
		p.Summary = "部分市场存在未解决对账门禁"
		p.Details = "只阻断相同 token/condition/market，不表示该钱包全部市场或其他钱包被阻断"
	}
	if total > 100 {
		for _, c := range []*pm.Check{&a, &p} {
			c.Evidence["sampling_complete"] = "false"
			if c.Status == pm.Healthy {
				c.Status = pm.Unknown
			}
			c.Details += "；本次最多采样101条，计数不是全量"
		}
	}
	return []pm.Check{a, p}, nil
}

func (s *PipelineMonitorSource) account(ctx context.Context, tx *sql.Tx, now time.Time, b tradingmonitor.Binding, id string) ([]pm.Check, error) {
	var enabled, policy, paused, strategyPaused, kill sql.NullBool
	var available, reserved string
	var maxAge sql.NullInt64
	var reconciled sql.NullTime
	err := tx.QueryRowContext(ctx, `
SELECT binding.enabled,policy.enabled,account_control.paused,strategy_control.paused,global_control.kill_switch,
       account.available_balance::text,account.reserved_balance::text,policy.max_state_age_ms,
       (SELECT r.completed_at FROM reconciliation_runs r
        WHERE r.execution_account_id=account.execution_account_id
          AND r.status IN ('COMPLETED','ATTENTION_REQUIRED') AND r.completed_at IS NOT NULL
        ORDER BY r.completed_at DESC,r.run_id DESC LIMIT 1)
FROM execution_accounts account
LEFT JOIN execution_strategy_bindings binding ON binding.execution_account_id=account.execution_account_id
 AND binding.model_id=$2 AND binding.strategy_id=$3
LEFT JOIN execution_risk_policies policy ON policy.execution_account_id=account.execution_account_id
LEFT JOIN execution_risk_controls account_control ON account_control.execution_account_id=account.execution_account_id
 AND account_control.control_scope='ACCOUNT' AND account_control.control_key=''
LEFT JOIN execution_risk_controls strategy_control ON strategy_control.execution_account_id=account.execution_account_id
 AND strategy_control.control_scope='STRATEGY' AND strategy_control.control_key=$3
LEFT JOIN execution_risk_global_control global_control ON global_control.singleton=TRUE
WHERE account.execution_account_id=$1`, id, b.ModelID, b.StrategyID).Scan(&enabled, &policy, &paused, &strategyPaused, &kill, &available, &reserved, &maxAge, &reconciled)
	if errors.Is(err, sql.ErrNoRows) {
		return []pm.Check{{ID: "account.authorization", Scope: id, Status: pm.Critical, Summary: "活动执行账户没有账本配置"}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read trading monitor account evidence: %w", err)
	}
	auth := pm.Check{ID: "account.authorization", Scope: id, Status: pm.Healthy, Summary: "账户数据库授权与控制正常", Evidence: map[string]string{"model_id": b.ModelID, "strategy_id": b.StrategyID}}
	switch {
	case !enabled.Valid || !policy.Valid || !paused.Valid || !kill.Valid:
		auth.Status = pm.Critical
		auth.Summary = "账户缺少必需授权或控制记录"
	case !enabled.Bool || !policy.Bool || paused.Bool || kill.Bool || (strategyPaused.Valid && strategyPaused.Bool):
		auth.Status = pm.Critical
		auth.Summary = "账户数据库授权或风控开关阻断 placement"
	}
	auth.Details = "按账户及策略检查；不改变开关，不限制 Cancel/Get。计划停用由运行配置另行标注"
	cash := pm.Check{ID: "account.cash", Scope: id, Status: pm.Healthy, Summary: "账户可用资金观测", Evidence: map[string]string{"available_cash": available, "reserved_cash": reserved}}
	if b.EntryDisabled {
		cash.Status = pm.Suppressed
		cash.Summary = "账户按配置不要求新开仓资金"
	} else {
		balance, ok := new(big.Rat).SetString(available)
		if !ok {
			cash.Status = pm.Unknown
			cash.Summary = "账户现金观测格式异常"
		} else if balance.Sign() <= 0 {
			cash.Status = pm.Warning
			cash.Summary = "活动账户没有可用新开仓资金"
			cash.Details = "仅是开仓资金不足；不宣称 SELL/Cancel 被阻断，不自动释放预占"
		}
	}
	rec := pm.Check{ID: "account.risk_state", Scope: id, Status: pm.Healthy, Summary: "账户已完成对账仍在政策有效期内"}
	if !maxAge.Valid || maxAge.Int64 <= 0 {
		rec.Status = pm.Unknown
		rec.Summary = "账户对账有效期政策不可读"
	} else if !reconciled.Valid {
		rec.Status = pm.Critical
		rec.Summary = "账户从未完成可用对账"
	} else {
		age := now.Sub(reconciled.Time)
		limit := time.Duration(maxAge.Int64) * time.Millisecond
		rec.Evidence = map[string]string{"last_completed_at": reconciled.Time.UTC().Format(time.RFC3339), "max_state_age": limit.String(), "age": age.Round(time.Second).String()}
		if age < -maxRiskFutureSkew || age > limit {
			rec.Status = pm.Critical
			rec.Summary = "账户对账结果超出风控有效期"
		} else if age >= limit*4/5 {
			rec.Status = pm.Warning
			rec.Summary = "账户对账结果即将过期"
		}
	}
	return []pm.Check{auth, cash, rec}, nil
}

func (s *PipelineMonitorSource) cycle(ctx context.Context, tx *sql.Tx, now time.Time, b tradingmonitor.Binding) (pm.Check, error) {
	check := pm.Check{ID: "strategy.progress", Scope: b.Scope(), Status: pm.Healthy, Summary: "策略周期已持久化输出"}
	var cycleID string
	var created, decision time.Time
	var output bool
	var badBooks int
	var block string
	err := tx.QueryRowContext(ctx, `
SELECT r.cycle_id,r.created_at,r.decision_at,r.output_payload IS NOT NULL,
       COALESCE((SELECT count(*)::int FROM jsonb_array_elements(COALESCE(r.input_payload->'orderbooks','[]'::jsonb)) book
                 WHERE book->>'status' IN ('ERROR','MISSING')),0),
       COALESCE(r.output_payload->'entry_policy'->>'block_reason','')
FROM strategy_decision_runs r
WHERE r.execution_account_id=$1 AND r.model_id=$2 AND r.strategy_id=$3
ORDER BY r.decision_at DESC,r.cycle_id DESC LIMIT 1`, b.ExecutionAccountID, b.ModelID, b.StrategyID).Scan(&cycleID, &created, &decision, &output, &badBooks, &block)
	if errors.Is(err, sql.ErrNoRows) {
		check.Status = pm.Unknown
		check.Summary = "尚无该路由的持久化策略周期"
		return check, nil
	}
	if err != nil {
		return check, fmt.Errorf("read trading monitor strategy evidence: %w", err)
	}
	check.Evidence = map[string]string{"cycle_id": cycleID, "decision_at": decision.UTC().Format(time.RFC3339), "entry_block_reason": block, "missing_or_error_books": strconv.Itoa(badBooks)}
	if !output && now.Sub(created) > s.stallAfter {
		check.Status = pm.Critical
		check.Summary = "策略输入已保存但输出超时"
	} else if !output {
		check.Summary = "策略周期正在处理中"
	} else if badBooks > 0 {
		check.Status = pm.Warning
		check.Summary = "策略输入含无法读取的盘口"
		check.Details = "只影响对应市场；EMPTY 与正常无流动性不计作读取错误"
	}
	if now.Sub(decision) > 24*time.Hour {
		check.Status = pm.Unknown
		check.Summary = "持久化策略证据已过时"
		check.Details = "旧周期不能证明当前策略状态；当前调度状态由真实 Runner 单独报告"
	}
	return check, nil
}

func (s *PipelineMonitorSource) progress(ctx context.Context, tx *sql.Tx, now time.Time, accountID string) ([]pm.Check, error) {
	types := []struct{ id, summary, query string }{
		{"intent.delivery", "订单意图投递", `WITH recent_cycles AS MATERIALIZED (
 SELECT cycle_id FROM strategy_decision_runs WHERE execution_account_id=$1 ORDER BY decision_at DESC LIMIT 100
)
SELECT client_order_id,status,since FROM (
 (SELECT client_order_id,status,created_at AS since FROM strategy_order_intent_deliveries
  WHERE status='PENDING' AND intent_payload->>'execution_account_id'=$1 ORDER BY created_at LIMIT 101)
 UNION ALL
 (SELECT client_order_id,status,claimed_at AS since FROM strategy_order_intent_deliveries
  WHERE status='SUBMITTING' AND intent_payload->>'execution_account_id'=$1 ORDER BY claimed_at LIMIT 101)
 UNION ALL
 (SELECT d.client_order_id,d.status,d.created_at AS since FROM recent_cycles r
  JOIN strategy_order_intent_deliveries d ON d.cycle_id=r.cycle_id
  WHERE d.status='UNKNOWN' AND NOT EXISTS (
    SELECT 1 FROM execution_orders resolved
    WHERE resolved.client_order_id=d.client_order_id
      AND d.intent_payload->>'execution_account_id'=$1
      AND resolved.execution_account_id=CASE
        WHEN upper(btrim(COALESCE(d.intent_payload->>'market_source','')))='KALSHI' THEN 'kalshi:'||$1
        ELSE $1
      END
      AND resolved.status IN ('FILLED','CANCELLED','REJECTED')
  ) ORDER BY d.created_at LIMIT 101)
) observed ORDER BY since LIMIT 101`},
		{"order.recovery", "未知订单与撤单恢复", `SELECT order_id,status,updated_at FROM (
 (SELECT order_id,status,updated_at FROM execution_orders
  WHERE status IN ('UNKNOWN','CANCEL_PENDING') AND execution_account_id=$1 ORDER BY updated_at LIMIT 101)
 UNION ALL
 (SELECT order_id,status,updated_at FROM
  (SELECT order_id,status,updated_at FROM execution_orders WHERE execution_account_id=$1 ORDER BY updated_at DESC,order_id DESC LIMIT 1000) recent
  WHERE status='MANUAL_REVIEW')
) observed ORDER BY updated_at LIMIT 101`},
		{"fill.application", "已确认成交入账", `SELECT fill_key,status,confirmed_at FROM execution_fills
 WHERE status='CONFIRMED' AND applied_at IS NULL AND confirmed_at IS NOT NULL AND execution_account_id=$1 ORDER BY confirmed_at LIMIT 101`},
	}
	checks := []pm.Check{}
	for _, kind := range types {
		// Durable strategy intents retain the logical account; executionrouter
		// projects Kalshi placements to kalshi:<account> only at execution time.
		// The logical account was already observed above, so do not invent a
		// healthy empty Kalshi-specific intent queue.
		if kind.id == "intent.delivery" && strings.HasPrefix(accountID, "kalshi:") {
			continue
		}
		rows, err := tx.QueryContext(ctx, kind.query, accountID)
		if err != nil {
			return checks, fmt.Errorf("read bounded trading progress: %w", err)
		}
		count, stalled := 0, 0
		oldest := time.Time{}
		sample := ""
		sampleState := ""
		for rows.Next() {
			var id, state string
			var since time.Time
			if err := rows.Scan(&id, &state, &since); err != nil {
				rows.Close()
				return checks, err
			}
			count++
			if now.Sub(since) > s.stallAfter {
				stalled++
				if oldest.IsZero() {
					oldest = since
					sample = id
					sampleState = state
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return checks, err
		}
		check := pm.Check{ID: kind.id, Scope: accountID, Status: pm.Healthy, Summary: kind.summary + "观测正常", Evidence: map[string]string{"sampled_rows": strconv.Itoa(count), "stalled_sample_rows": strconv.Itoa(stalled), "sample_id": sample, "sample_state": sampleState, "stall_limit": s.stallAfter.String()}}
		if kind.id == "intent.delivery" {
			check.Evidence["unknown_cycle_window"] = "latest_100_account_cycles"
			check.Evidence["resolved_unknown_policy"] = "same_client_id_expected_venue_account_terminal_order"
		}
		if kind.id == "order.recovery" {
			check.Evidence["manual_review_window"] = "latest_1000_updated_account_orders"
		}
		if stalled > 0 {
			check.Status = pm.Warning
			check.Summary = kind.summary + "存在超时未解决项"
			check.Evidence["oldest_observed_at"] = oldest.UTC().Format(time.RFC3339)
			check.Details = "只读提醒；UNKNOWN 不自动重投，预占不自动释放，成交必须依权威证据入账"
		}
		if !oldest.IsZero() && now.Sub(oldest) > 2*s.stallAfter {
			check.Status = pm.Critical
		}
		if count > 100 {
			check.Evidence["sampling_complete"] = "false"
			if check.Status == pm.Healthy {
				check.Status = pm.Unknown
			}
			check.Details += "；本次最多读取101条，计数不是全量"
		} else {
			check.Evidence["sampling_complete"] = "true"
		}
		checks = append(checks, check)
	}
	return checks, nil
}

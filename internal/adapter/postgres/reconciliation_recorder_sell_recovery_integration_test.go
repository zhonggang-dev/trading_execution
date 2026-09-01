package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestReconciliationRecorderResolvesOnlyEvidenceBackedRecoveredSellDrift proves
// that a late, already-applied SELL fill can release a stale position gate on a
// later healthy sweep without manufacturing another fill. Incomplete identity,
// incomplete evidence, a still-mismatched ledger, a reproduced issue, or an
// unhealthy source sweep all remain fail-closed.
func TestReconciliationRecorderResolvesOnlyEvidenceBackedRecoveredSellDrift(t *testing.T) {
	databaseURL := os.Getenv("TRADING_EXECUTION_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TRADING_EXECUTION_TEST_DATABASE_URL is not set")
	}
	db := newIntegrationDatabase(t, databaseURL)
	recorder, err := NewReconciliationRecorder(db)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)

	tests := []struct {
		name               string
		positionShares     string
		issueConditionID   string
		fillShares         string
		issueSource        string
		fillVenue          string
		secondRunStatus    domain.ReconciliationRunStatus
		reproduceCurrent   bool
		wantStatus         domain.ReconciliationIssueStatus
		wantAutomaticCount int
	}{
		{
			name: "clean exact zero recovery", positionShares: "0", issueConditionID: "condition",
			fillShares: "59", secondRunStatus: domain.ReconciliationRunCompleted,
			wantStatus: domain.ReconciliationIssueResolved, wantAutomaticCount: 1,
		},
		{
			name: "source failure", positionShares: "0", issueConditionID: "condition",
			fillShares: "59", secondRunStatus: domain.ReconciliationRunAttentionRequired,
			wantStatus: domain.ReconciliationIssueOpen,
		},
		{
			name: "identity incomplete", positionShares: "0", issueConditionID: "",
			fillShares: "59", secondRunStatus: domain.ReconciliationRunCompleted,
			wantStatus: domain.ReconciliationIssueOpen,
		},
		{
			name: "still mismatched", positionShares: "1", issueConditionID: "condition",
			fillShares: "59", secondRunStatus: domain.ReconciliationRunCompleted,
			wantStatus: domain.ReconciliationIssueOpen,
		},
		{
			name: "fill delta incomplete", positionShares: "0", issueConditionID: "condition",
			fillShares: "58", secondRunStatus: domain.ReconciliationRunCompleted,
			wantStatus: domain.ReconciliationIssueOpen,
		},
		{
			name: "no official fill", positionShares: "0", issueConditionID: "condition",
			secondRunStatus: domain.ReconciliationRunCompleted,
			wantStatus:      domain.ReconciliationIssueOpen,
		},
		{
			name: "untrusted issue source", positionShares: "0", issueConditionID: "condition",
			fillShares: "59", issueSource: "UNTRUSTED_POSITION_SOURCE",
			secondRunStatus: domain.ReconciliationRunCompleted, wantStatus: domain.ReconciliationIssueOpen,
		},
		{
			name: "non polymarket fill", positionShares: "0", issueConditionID: "condition",
			fillShares: "59", fillVenue: "kalshi",
			secondRunStatus: domain.ReconciliationRunCompleted, wantStatus: domain.ReconciliationIssueOpen,
		},
		{
			name: "issue reproduced", positionShares: "0", issueConditionID: "condition",
			fillShares: "59", secondRunStatus: domain.ReconciliationRunCompleted, reproduceCurrent: true,
			wantStatus: domain.ReconciliationIssueOpen,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accountID := fmt.Sprintf("account-sell-recovery-%d", index)
			marketID := fmt.Sprintf("market-sell-recovery-%d", index)
			conditionID := fmt.Sprintf("condition-sell-recovery-%d", index)
			tokenID := fmt.Sprintf("token-sell-recovery-%d", index)
			issueConditionID := conditionID
			if test.issueConditionID == "" {
				issueConditionID = ""
			}
			issueSource := test.issueSource
			if issueSource == "" {
				issueSource = "POLYMARKET_DATA_API"
			}
			insertAccount(t, db, accountID, fmt.Sprintf("0xsellrecovery%d", index), "10", "10", "0")
			insertRecoveredSellPosition(t, db, accountID, marketID, conditionID, tokenID, test.positionShares)

			oldRun := startReconciliationFixtureRun(t, recorder, accountID, fmt.Sprintf("run-sell-recovery-old-%d", index), base)
			oldIssue := domain.ReconciliationIssue{
				IssueID: fmt.Sprintf("issue-sell-recovery-%d", index), RunID: oldRun.RunID,
				Fingerprint:        fmt.Sprintf("fingerprint-sell-recovery-%d", index),
				ExecutionAccountID: accountID, Type: domain.ReconciliationIssuePositionDrift,
				Resolution: domain.ReconciliationResolutionManual, Status: domain.ReconciliationIssueOpen,
				MarketID: marketID, ConditionID: issueConditionID, TokenID: tokenID,
				LocalValue: "59", RemoteValue: "0", Source: issueSource,
				Details: "local shares are absent from the external position snapshot", ObservedAt: base,
			}
			if err := recorder.RecordIssue(context.Background(), oldIssue); err != nil {
				t.Fatal(err)
			}
			completeReconciliationFixtureRun(t, recorder, oldRun, domain.ReconciliationRunAttentionRequired, base.Add(time.Second))

			if test.fillShares != "" {
				fillVenue := test.fillVenue
				if fillVenue == "" {
					fillVenue = "polymarket"
				}
				insertRecoveredSellFill(t, db, accountID, marketID, conditionID, tokenID, test.fillShares, fillVenue, base.Add(2*time.Second), index)
			}
			newRun := startReconciliationFixtureRun(t, recorder, accountID, fmt.Sprintf("run-sell-recovery-new-%d", index), base.Add(3*time.Second))
			if test.reproduceCurrent {
				reproduced := oldIssue
				reproduced.IssueID = fmt.Sprintf("issue-sell-recovery-reproduced-%d", index)
				reproduced.RunID = newRun.RunID
				reproduced.ObservedAt = base.Add(3 * time.Second)
				if err := recorder.RecordIssue(context.Background(), reproduced); err != nil {
					t.Fatal(err)
				}
			}
			completeReconciliationFixtureRun(t, recorder, newRun, test.secondRunStatus, base.Add(4*time.Second))

			var status domain.ReconciliationIssueStatus
			var resolution domain.ReconciliationResolution
			if err := db.QueryRow(`
				SELECT status,resolution FROM reconciliation_issues
				WHERE execution_account_id=$1 AND fingerprint=$2`, accountID, oldIssue.Fingerprint,
			).Scan(&status, &resolution); err != nil {
				t.Fatal(err)
			}
			if status != test.wantStatus {
				t.Fatalf("drift status = %s, want %s", status, test.wantStatus)
			}
			if status == domain.ReconciliationIssueResolved && resolution != domain.ReconciliationResolutionAutomatic {
				t.Fatalf("resolved drift resolution = %s, want AUTOMATIC", resolution)
			}
			var summaryJSON []byte
			if err := db.QueryRow(`SELECT summary FROM reconciliation_runs WHERE run_id=$1`, newRun.RunID).Scan(&summaryJSON); err != nil {
				t.Fatal(err)
			}
			var summary map[string]int
			if err := json.Unmarshal(summaryJSON, &summary); err != nil {
				t.Fatal(err)
			}
			if summary["issues_automatic"] != test.wantAutomaticCount {
				t.Fatalf("issues_automatic = %d, want %d", summary["issues_automatic"], test.wantAutomaticCount)
			}

			if status == domain.ReconciliationIssueResolved {
				thirdRun := startReconciliationFixtureRun(t, recorder, accountID, fmt.Sprintf("run-sell-recovery-idempotent-%d", index), base.Add(5*time.Second))
				completeReconciliationFixtureRun(t, recorder, thirdRun, domain.ReconciliationRunCompleted, base.Add(6*time.Second))
				if err := db.QueryRow(`SELECT summary FROM reconciliation_runs WHERE run_id=$1`, thirdRun.RunID).Scan(&summaryJSON); err != nil {
					t.Fatal(err)
				}
				summary = nil
				if err := json.Unmarshal(summaryJSON, &summary); err != nil {
					t.Fatal(err)
				}
				if summary["issues_automatic"] != 0 {
					t.Fatalf("idempotent sweep resolved %d additional issues", summary["issues_automatic"])
				}
			}
		})
	}
}

func startReconciliationFixtureRun(
	t *testing.T,
	recorder *ReconciliationRecorder,
	accountID, runID string,
	startedAt time.Time,
) domain.ReconciliationRun {
	t.Helper()
	run, err := (domain.ReconciliationRunParams{
		RunID: runID, ExecutionAccountID: accountID,
		Trigger: domain.ReconciliationTriggerScheduled, StartedAt: startedAt,
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	return run
}

func completeReconciliationFixtureRun(
	t *testing.T,
	recorder *ReconciliationRecorder,
	run domain.ReconciliationRun,
	status domain.ReconciliationRunStatus,
	completedAt time.Time,
) {
	t.Helper()
	run.Status = status
	run.CompletedAt = &completedAt
	if err := recorder.Complete(context.Background(), run); err != nil {
		t.Fatal(err)
	}
}

func insertRecoveredSellPosition(
	t *testing.T,
	db *sql.DB,
	accountID, marketID, conditionID, tokenID, shares string,
) {
	t.Helper()
	lifecycle := "OPEN"
	if shares == "0" {
		lifecycle = "CLOSED"
	}
	if _, err := db.Exec(`
		INSERT INTO execution_positions (
			execution_account_id,market_id,condition_id,outcome_index,outcome_name,token_id,
			total_shares,available_shares,reserved_shares,cost_basis,lifecycle_status
		) VALUES ($1,$2,$3,0,'YES',$4,$5::numeric,$5::numeric,0,
		          CASE WHEN $5::numeric=0 THEN 0 ELSE $5::numeric*0.1 END,$6)`,
		accountID, marketID, conditionID, tokenID, shares, lifecycle); err != nil {
		t.Fatal(err)
	}
}

func insertRecoveredSellFill(
	t *testing.T,
	db *sql.DB,
	accountID, marketID, conditionID, tokenID, shares, venue string,
	appliedAt time.Time,
	index int,
) {
	t.Helper()
	orderID := fmt.Sprintf("order-sell-recovery-%d", index)
	fillKey := fmt.Sprintf("fill-sell-recovery-%d", index)
	venueOrderID := fmt.Sprintf("0x%064x", index+1)
	transactionHash := fmt.Sprintf("0x%064x", index+1001)
	intentJSON, err := json.Marshal(map[string]string{"condition_id": conditionID, "side": "SELL"})
	if err != nil {
		t.Fatal(err)
	}
	settlementEvidence, err := json.Marshal(map[string]any{
		"schema_version":          "trading.settlement_evidence.v1",
		"source":                  "POLYGON_V2_ORDER_FILLED",
		"chain_id":                "137",
		"exchange_address":        "0x1111111111111111111111111111111111111111",
		"transaction_hash":        transactionHash,
		"block_number":            123,
		"block_hash":              "0x2222222222222222222222222222222222222222222222222222222222222222",
		"log_index":               index,
		"confirmations":           64,
		"order_hash":              venueOrderID,
		"maker_address":           "0x3333333333333333333333333333333333333333",
		"token_id":                tokenID,
		"side":                    "SELL",
		"maker_amount_base_units": "1",
		"taker_amount_base_units": "1",
		"total_fee_base_units":    "0",
		"builder_code":            "0x0000000000000000000000000000000000000000000000000000000000000000",
		"builder_fee_known":       true,
		"builder_fee_base_units":  "0",
		"builder_fee_source":      "ZERO_BUILDER_FEE",
		"collateral_decimals":     "6",
		"outcome_token_decimals":  "6",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_orders (
			order_id,client_order_id,execution_account_id,venue,market_id,token_id,
			intent,venue_order_id,status,filled_size,filled_notional,total_fees,
			average_fill_price,revision,created_at,updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,'FILLED',$9::numeric,
		          $9::numeric*0.1,0,0.1,1,$10,$10)`,
		orderID, "client-"+orderID, accountID, venue, marketID, tokenID,
		intentJSON, venueOrderID, shares, appliedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO execution_fills (
			fill_key,venue,venue_fill_id,order_id,venue_order_id,execution_account_id,
			market_id,condition_id,token_id,side,liquidity_role,status,shares,price,gross_notional,
			fee_rate_bps,platform_fee,builder_fee_rate_bps,builder_fee,total_fee,net_cash_delta,
			fee_source,transaction_hash,settlement_evidence,
			matched_at,first_observed_at,last_observed_at,confirmed_at,applied_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'SELL','TAKER','CONFIRMED',
		          $10::numeric,0.1,$10::numeric*0.1,0,0,0,0,0,$10::numeric*0.1,
		          'POLYGON_V2_ORDER_FILLED',$11,$12::jsonb,$13,$13,$13,$13,$13)`,
		fillKey, venue, "venue-"+fillKey, orderID, venueOrderID, accountID,
		marketID, conditionID, tokenID, shares, transactionHash, settlementEvidence, appliedAt); err != nil {
		t.Fatal(err)
	}
}

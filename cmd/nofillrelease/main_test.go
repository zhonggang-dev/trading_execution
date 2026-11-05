package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/nofillrelease"
)

var (
	toolSubmitAt = time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)
	toolHash     = "0x" + strings.Repeat("89", 32)
)

// legacyFacts is the shape of the 10-29 SELL left UNKNOWN by a 5xx: a
// historical GTC order without a venue expiry, only failed reads since.
func legacyFacts() domain.NoFillLocalFacts {
	completed := toolSubmitAt.Add(900 * time.Millisecond)
	return domain.NoFillLocalFacts{
		Order: domain.Order{
			ID: "ord-898503ce", VenueOrderID: toolHash, Status: domain.OrderStatusUnknown,
			FilledSize: "0", FilledNotional: "0", TotalFees: "0", Revision: 9,
			CreatedAt: toolSubmitAt.Add(-time.Second), UpdatedAt: toolSubmitAt.Add(time.Hour),
			Intent: domain.OrderIntent{Venue: "polymarket", ExecutionAccountID: "wallet-6", ClientOrderID: "c",
				ConditionID: "0xcondition", TokenID: "77", Side: domain.SideSell, Size: "12"},
		},
		Attempts: []domain.OrderAttempt{
			{ID: "attempt:ord-898503ce:1", Kind: domain.OrderAttemptSubmit, Outcome: domain.AttemptOutcomeUnknown,
				VenueOrderID: toolHash, HTTPStatus: 500, ErrorCode: "CLOB_SERVER_ERROR", StartedAt: toolSubmitAt, CompletedAt: &completed},
			{ID: "attempt:ord-898503ce:2", Kind: domain.OrderAttemptReconcile, Outcome: domain.AttemptOutcomeUnknown,
				ErrorCode: "CLOB_ORDER_NOT_FOUND", StartedAt: toolSubmitAt.Add(time.Minute), CompletedAt: &completed},
		},
		EventStatuses: []domain.OrderStatus{domain.OrderStatusReceived, domain.OrderStatusValidating, domain.OrderStatusReserved,
			domain.OrderStatusSubmitting, domain.OrderStatusUnknown, domain.OrderStatusReconciling, domain.OrderStatusUnknown},
		Reservation: &domain.AssetReservation{OrderID: "ord-898503ce", Side: domain.SideSell, Status: domain.ReservationStatusReconciliationRequired,
			RemainingReservedBalance: "0", RemainingReservedShares: "12", SettledShares: "0", SettledNotional: "0", SettledFees: "0", Revision: 4},
	}
}

type toolStore struct {
	facts   domain.NoFillLocalFacts
	applied []domain.NoFillResolution
}

func (store *toolStore) Facts(context.Context, string) (domain.NoFillLocalFacts, error) {
	return store.facts, nil
}

func (store *toolStore) Apply(_ context.Context, resolution domain.NoFillResolution) (domain.Order, error) {
	store.applied = append(store.applied, resolution)
	order := store.facts.Order
	order.Status, order.Revision = domain.OrderStatusCancelled, order.Revision+2
	return order, nil
}

func (store *toolStore) RecordShadow(context.Context, domain.NoFillResolution) (bool, error) {
	return false, errors.New("the tool never records shadows")
}

func (store *toolStore) HasShadow(context.Context, string) (bool, error) { return false, nil }

type toolVenue struct{ tradesErr error }

func (toolVenue) ReadOrder(context.Context, domain.Order) (domain.NoFillOrderRead, error) {
	return domain.NoFillOrderRead{NotFound: true, Code: "CLOB_ORDER_NOT_FOUND"}, nil
}

func (venue toolVenue) TradeReferences(context.Context, domain.Order, time.Time) (domain.NoFillTradeScan, error) {
	return domain.NoFillTradeScan{References: []domain.NoFillTradeReference{}}, venue.tradesErr
}

func (toolVenue) OpenOrders(context.Context, domain.Order) (domain.NoFillOpenOrderScan, error) {
	return domain.NoFillOpenOrderScan{}, nil
}

type toolChain struct{}

func (toolChain) ScanOrderFilled(context.Context, domain.NoFillChainScanRequest) (domain.NoFillChainEvidence, error) {
	return domain.NoFillChainEvidence{ChainID: 137, FromBlock: 1, ToBlock: 90000, ToBlockHashBefore: "0xaa", ToBlockHashAfter: "0xaa",
		Matches: []domain.NoFillChainMatch{}, Controls: []domain.NoFillChainControl{{BlockNumber: 1, Logs: 3}, {BlockNumber: 2, Logs: 3}, {BlockNumber: 3, Logs: 3}}}, nil
}

func fakeBuild(store *toolStore, venue toolVenue, now time.Time) buildFunc {
	return func(_ context.Context, options settings) (evaluator, func(), error) {
		resolver, err := nofillrelease.New(nofillrelease.Params{
			Store: store, Venue: venue, Chain: toolChain{}, Policy: options.policy, Mode: nofillrelease.ModeOff,
			Actor: options.actor, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now },
		})
		return resolver, nil, err
	}
}

func runTool(t *testing.T, store *toolStore, venue toolVenue, args ...string) (output, error) {
	t.Helper()
	var stdout bytes.Buffer
	err := run(args, &stdout, fakeBuild(store, venue, toolSubmitAt.Add(9*time.Hour)))
	var result output
	if stdout.Len() > 0 {
		if decodeErr := json.Unmarshal(stdout.Bytes(), &result); decodeErr != nil {
			t.Fatalf("decode output %q: %v", stdout.String(), decodeErr)
		}
	}
	return result, err
}

func TestToolArgumentValidation(t *testing.T) {
	for _, args := range [][]string{
		{"--order", "ord-1", "--reason", "r"},
		{"--order", "ord-1", "--actor", "op"},
		{"--order", "ord-1", "--actor", "op", "--reason", "r", "--apply"},
		{"--order", "ord-1", "--actor", "op", "--reason", "r", "--confirm-id", "x"},
		{"--order", "ord-1", "--actor", "op", "--reason", "r", "--min-age", "1m", "--legacy-no-expiration"},
	} {
		if _, err := parse(args); err == nil {
			t.Fatalf("parse(%v) accepted invalid arguments", args)
		}
	}
}

func TestToolDryRunThenApplyLegacyOrder(t *testing.T) {
	store := &toolStore{facts: legacyFacts()}
	base := []string{"--order", "ord-898503ce", "--actor", "zhangyan", "--reason", "10-29 5xx, never accepted"}

	refused, err := runTool(t, store, toolVenue{}, base...)
	if err == nil || refused.Decision != string(nofillrelease.DecisionNotEligible) || len(store.applied) != 0 {
		t.Fatalf("dry run without --legacy-no-expiration = %#v, %v; want refusal", refused, err)
	}

	legacy := append(append([]string{}, base...), "--legacy-no-expiration")
	dryRun, err := runTool(t, store, toolVenue{}, legacy...)
	if err != nil || dryRun.Mode != "dry-run" || dryRun.Decision != string(nofillrelease.DecisionResolve) ||
		len(dryRun.ConfirmID) != 64 || len(dryRun.EvidenceSHA256) != 64 || len(store.applied) != 0 {
		t.Fatalf("legacy dry run = %#v, %v", dryRun, err)
	}
	var evidence nofillrelease.Evidence
	if err := json.Unmarshal(dryRun.Evidence, &evidence); err != nil || evidence.Mode != "OPERATOR" ||
		evidence.Actor != "zhangyan" || evidence.Release.Shares != "12" {
		t.Fatalf("evidence = %#v, %v", evidence, err)
	}

	if _, err := runTool(t, store, toolVenue{}, append(append([]string{}, legacy...), "--apply", "--confirm-id", strings.Repeat("0", 64))...); err == nil || len(store.applied) != 0 {
		t.Fatalf("apply with a wrong confirm id = %v; want refusal without writes", err)
	}

	// Recovery keeps polling between the dry run and the apply.
	store.facts.Order.Revision += 2
	store.facts.Reservation.Revision++
	applied, err := runTool(t, store, toolVenue{}, append(append([]string{}, legacy...), "--apply", "--confirm-id", dryRun.ConfirmID)...)
	if err != nil || applied.Mode != "apply" || applied.ResultStatus != string(domain.OrderStatusCancelled) || len(store.applied) != 1 {
		t.Fatalf("apply = %#v, %v", applied, err)
	}
	resolution := store.applied[0]
	if resolution.Mode != domain.NoFillModeOperator || resolution.Actor != "zhangyan" || resolution.Reason != "10-29 5xx, never accepted" ||
		resolution.ReasonCode != domain.NoFillReleaseOperatorReasonCode || resolution.ExpectedReleaseShares != "12" {
		t.Fatalf("resolution = %#v", resolution)
	}
}

func TestToolRefusesIncompleteEvidence(t *testing.T) {
	store := &toolStore{facts: legacyFacts()}
	args := []string{"--order", "ord-898503ce", "--actor", "op", "--reason", "r", "--legacy-no-expiration"}
	dryRun, err := runTool(t, store, toolVenue{}, args...)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runTool(t, store, toolVenue{tradesErr: errors.New("CLOB 503")}, append(args, "--apply", "--confirm-id", dryRun.ConfirmID)...)
	if err == nil || result.Decision != string(nofillrelease.DecisionInconclusive) || len(store.applied) != 0 {
		t.Fatalf("apply with an unreadable source = %#v, %v; want refusal", result, err)
	}
	store.facts.FillCount = 1
	if _, err := runTool(t, store, toolVenue{}, args...); err == nil {
		t.Fatal("an order with a fill row must be refused")
	}
}

func TestRedactRemovesRegisteredSecrets(t *testing.T) {
	addSecret("postgres://user:pa55word@db/x")
	addSecret("api-secret-value")
	message := redact("connect postgres://user:pa55word@db/x failed with api-secret-value")
	if strings.Contains(message, "pa55word") || strings.Contains(message, "api-secret-value") {
		t.Fatalf("redact() = %q", message)
	}
}

package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/memory"
	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
)

// recoveryTestParams builds a service with one UNKNOWN BUY on token-1 (size 5 at
// worst price 0.4, so at most 2 collateral could have left the wallet), a
// healthy position on token-2, and configurable external snapshots.
type recoveryTestFixture struct {
	orders    *fakeOrders
	fills     *fakeFills
	refresher *fakeRefresher
	ledger    *fakeLedger
	leases    *memory.OrderRecoveryLeaseStore
	external  []domain.ExternalPosition
	balance   domain.Decimal
}

func unknownBuyOrder() domain.Order {
	order := testOrder("order-unknown", "0xvenue-unknown", domain.OrderStatusUnknown)
	order.Intent.WorstPrice = "0.4"
	order.Intent.Price = "0.38"
	order.Revision = 4
	return order
}

func newRecoveryFixture() *recoveryTestFixture {
	unknown := unknownBuyOrder()
	return &recoveryTestFixture{
		orders:    &fakeOrders{orders: []domain.Order{unknown}},
		fills:     &fakeFills{},
		refresher: &fakeRefresher{orders: map[string]domain.Order{unknown.ID: unknown}},
		ledger: &fakeLedger{
			balance:   testBalance("100"),
			positions: []domain.Position{openPosition("token-1", "10"), otherTokenPosition("token-2", "3")},
		},
		leases: memory.NewOrderRecoveryLeaseStore(),
		external: []domain.ExternalPosition{
			{ConditionID: "condition-1", TokenID: "token-1", OutcomeName: "YES", Shares: "15", Source: "POLYMARKET_DATA_API", ObservedAt: testNow},
			{ConditionID: "condition-2", TokenID: "token-2", OutcomeName: "YES", Shares: "3", Source: "POLYMARKET_DATA_API", ObservedAt: testNow},
		},
		balance: "100",
	}
}

func otherTokenPosition(tokenID string, shares domain.Decimal) domain.Position {
	position := openPosition(tokenID, shares)
	position.MarketID = "market-2"
	position.ConditionID = "condition-2"
	return position
}

func (fixture *recoveryTestFixture) service(t *testing.T, policy orderrecovery.Policy) *Service {
	t.Helper()
	return newTestService(t, Params{
		Orders: fixture.orders, Venue: &fakeVenue{}, Ledger: fixture.ledger,
		Fills: fixture.fills, OrderRefresher: fixture.refresher,
		Recovery: newTestRecoveryGuard(t, fixture.leases, policy),
		PositionSources: []port.ExternalPositionSource{positionSourceFunc(func(context.Context, string) ([]domain.ExternalPosition, error) {
			return fixture.external, nil
		})},
		BalanceSources: []port.ExternalBalanceSource{balanceSourceFunc(func(context.Context, string, string) (domain.ExternalBalance, error) {
			return domain.ExternalBalance{Asset: "USDC", Amount: fixture.balance, Source: "CHAIN", ObservedAt: testNow}, nil
		})},
	})
}

func runScheduled(t *testing.T, service *Service) Result {
	t.Helper()
	result, err := service.RunAccount(context.Background(), RunAccountParams{ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerScheduled})
	if err != nil && !errors.Is(err, errVenueDown) {
		t.Fatalf("RunAccount() error = %v", err)
	}
	return result
}

var errVenueDown = errors.New("clob get order: 503")

func findIssue(issues []domain.ReconciliationIssue, issueType domain.ReconciliationIssueType) (domain.ReconciliationIssue, bool) {
	for _, issue := range issues {
		if issue.Type == issueType {
			return issue, true
		}
	}
	return domain.ReconciliationIssue{}, false
}

// TestUnknownOrderWithVenueFailureGatesOnlyItsMarket 验证一张 UNKNOWN 订单的交易所
// 读取失败只产生订单级问题：同 token 的持仓比较延后，余额差额在冻结预占范围内记为
// RETRY_LATER，其它 token 照常比较，影响范围不是账户级。
func TestUnknownOrderWithVenueFailureGatesOnlyItsMarket(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.refresher.errors = map[string]error{"order-unknown": errVenueDown}
	fixture.balance = "98.5" // 1.5 left the wallet: inside the 2.0 the BUY reserved
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	if result.Run.Status != domain.ReconciliationRunAttentionRequired {
		t.Fatalf("run status = %s, want ATTENTION_REQUIRED", result.Run.Status)
	}
	source, ok := findIssue(result.Issues, domain.ReconciliationIssueSourceUnavailable)
	if !ok || source.OrderID != "order-unknown" || source.ImpactScope != domain.ReconciliationImpactOrder {
		t.Fatalf("source issue = %#v ok=%v, want order-scoped SOURCE_UNAVAILABLE", source, ok)
	}
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
	balance, ok := findIssue(result.Issues, domain.ReconciliationIssueBalanceDrift)
	if !ok || balance.Resolution != domain.ReconciliationResolutionRetry || balance.ImpactScope != domain.ReconciliationImpactNone {
		t.Fatalf("balance issue = %#v ok=%v, want RETRY_LATER drift attributed to the unresolved order", balance, ok)
	}
	// The order-scoped SOURCE_UNAVAILABLE already gates token-1; no duplicate
	// ORDER_RECOVERY_PENDING is needed in the same run.
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueOrderRecoveryPending)
	gated := false
	for _, issue := range result.Issues {
		if issue.Status == domain.ReconciliationIssueOpen && issue.BlocksIntent(domain.OrderIntent{TokenID: "token-1"}) {
			gated = true
		}
	}
	if !gated {
		t.Fatalf("no open issue gates token-1: %#v", result.Issues)
	}
	if result.Impact.AccountWide || !result.Impact.Partial() || len(result.Impact.TokenIDs) != 1 || result.Impact.TokenIDs[0] != "token-1" {
		t.Fatalf("impact = %#v, want token-1 only", result.Impact)
	}
	if result.Run.Summary["positions_deferred_for_unresolved_orders"] != 1 || result.Run.Summary["orders_unresolved"] != 1 ||
		result.Run.Summary["balance_explained_by_unresolved_orders"] != 1 || result.Run.Summary["impact_account_wide"] != 0 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
	lease, exists := fixture.leases.Lease("order-unknown")
	if !exists || lease.Attempts != 1 || lease.Holder != "" || lease.NextRetryAt == nil {
		t.Fatalf("lease = %#v exists=%v, want one failed attempt with backoff", lease, exists)
	}
	unrelated := domain.Order{Intent: domain.OrderIntent{ExecutionAccountID: "account-1", MarketID: "market-2", ConditionID: "condition-2", TokenID: "token-2"}}
	for _, issue := range result.Issues {
		if issue.Status == domain.ReconciliationIssueOpen && issue.BlocksIntent(unrelated.Intent) {
			t.Fatalf("issue %#v blocks an unrelated market", issue)
		}
	}
}

// TestUnknownOrderCannotExplainDriftBeyondItsReservation 验证差额超过未解决订单冻结
// 预占时，仍按账户级 MANUAL_REVIEW 余额漂移处理。
func TestUnknownOrderCannotExplainDriftBeyondItsReservation(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.refresher.errors = map[string]error{"order-unknown": errVenueDown}
	fixture.balance = "97" // 3.0 missing: more than the 2.0 the BUY could have spent
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	balance, ok := findIssue(result.Issues, domain.ReconciliationIssueBalanceDrift)
	if !ok || balance.Resolution != domain.ReconciliationResolutionManual || balance.ImpactScope != domain.ReconciliationImpactAccount {
		t.Fatalf("balance issue = %#v ok=%v, want MANUAL account-wide drift", balance, ok)
	}
	if !result.Impact.AccountWide {
		t.Fatalf("impact = %#v, want account-wide", result.Impact)
	}
}

// TestUnknownOrderHeldByAnotherWorkerIsDeferred 验证同一订单只允许一个恢复者：
// 被 coordinator 持有租约的订单本轮不调用交易所，只记录 ORDER_RECOVERY_PENDING。
func TestUnknownOrderHeldByAnotherWorkerIsDeferred(t *testing.T) {
	fixture := newRecoveryFixture()
	if _, err := fixture.leases.AcquireOrderRecoveryLease(context.Background(), domain.OrderRecoveryLeaseRequest{
		OrderID: "order-unknown", ExecutionAccountID: "account-1", Holder: "ordercoordinator", OrderRevision: 4, Now: testNow, TTL: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	if fixture.refresher.calls != 0 || len(fixture.fills.calls) != 0 {
		t.Fatalf("held order was worked: refresh=%d fills=%v", fixture.refresher.calls, fixture.fills.calls)
	}
	pending, ok := findIssue(result.Issues, domain.ReconciliationIssueOrderRecoveryPending)
	if !ok || pending.Resolution != domain.ReconciliationResolutionRetry || pending.ImpactScope != domain.ReconciliationImpactOrder {
		t.Fatalf("pending issue = %#v ok=%v", pending, ok)
	}
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
	if result.Run.Summary["orders_recovery_deferred"] != 1 || result.Impact.AccountWide {
		t.Fatalf("summary = %#v impact = %#v", result.Run.Summary, result.Impact)
	}
}

// TestFocusOrderBypassesBackoffButScheduledRunHonorsIt 验证退避期内定时对账推迟该
// 订单，而 ORDER_UNKNOWN 即时触发的关注订单可以立即重试。
func TestFocusOrderBypassesBackoffButScheduledRunHonorsIt(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.refresher.errors = map[string]error{"order-unknown": errVenueDown}
	fixture.balance = "100"
	service := fixture.service(t, orderrecovery.Policy{BaseBackoff: 30 * time.Second})

	first := runScheduled(t, service)
	if fixture.refresher.calls != 1 || first.Run.Summary["orders_recovery_failed"] != 1 {
		t.Fatalf("first run refresh calls = %d summary = %#v", fixture.refresher.calls, first.Run.Summary)
	}
	second := runScheduled(t, service)
	if fixture.refresher.calls != 1 || second.Run.Summary["orders_recovery_deferred"] != 1 {
		t.Fatalf("second run refresh calls = %d summary = %#v, want deferral inside backoff", fixture.refresher.calls, second.Run.Summary)
	}
	assertIssue(t, second.Issues, domain.ReconciliationIssueOrderRecoveryPending, domain.ReconciliationIssueOpen)
	focused, err := service.RunAccount(context.Background(), RunAccountParams{
		ExecutionAccountID: "account-1", Trigger: domain.ReconciliationTriggerOrderUnknown, FocusOrderID: "order-unknown",
	})
	if err != nil && !errors.Is(err, errVenueDown) {
		t.Fatalf("focused RunAccount() error = %v", err)
	}
	if fixture.refresher.calls != 2 || focused.Run.Summary["orders_recovery_failed"] != 1 {
		t.Fatalf("focused run refresh calls = %d summary = %#v, want backoff bypass", fixture.refresher.calls, focused.Run.Summary)
	}
	lease, _ := fixture.leases.Lease("order-unknown")
	if lease.Attempts != 2 {
		t.Fatalf("lease attempts = %d, want 2", lease.Attempts)
	}
}

// TestEscalatedRecoveryOrderEntersManualQueueWithoutReleasingReservation 验证超过
// 升级窗口的订单记 ORDER_RECOVERY_STALLED（MANUAL_REVIEW），影响范围仍限于该订单市场，
// 且不触发任何预占释放。
func TestEscalatedRecoveryOrderEntersManualQueueWithoutReleasingReservation(t *testing.T) {
	fixture := newRecoveryFixture()
	long := testNow.Add(-45 * time.Minute)
	if _, err := fixture.leases.AcquireOrderRecoveryLease(context.Background(), domain.OrderRecoveryLeaseRequest{
		OrderID: "order-unknown", ExecutionAccountID: "account-1", Holder: "ordercoordinator", OrderRevision: 4, Now: long, TTL: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.leases.ReleaseOrderRecoveryLease(context.Background(), domain.OrderRecoveryRelease{
		OrderID: "order-unknown", Holder: "ordercoordinator", Now: long, Kind: domain.OrderRecoveryOutcomeFailed,
		Error: "clob 503", NextRetryAt: long.Add(30 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	service := fixture.service(t, orderrecovery.Policy{EscalateAfter: 30 * time.Minute})

	result := runScheduled(t, service)
	stalled, ok := findIssue(result.Issues, domain.ReconciliationIssueOrderRecoveryStalled)
	if !ok || stalled.Resolution != domain.ReconciliationResolutionManual || stalled.OrderID != "order-unknown" ||
		stalled.ImpactScope != domain.ReconciliationImpactOrder {
		t.Fatalf("stalled issue = %#v ok=%v", stalled, ok)
	}
	if result.Impact.AccountWide || result.Run.Summary["orders_recovery_escalated"] != 1 {
		t.Fatalf("impact = %#v summary = %#v", result.Impact, result.Run.Summary)
	}
	if fixture.refresher.finalizations != 0 {
		t.Fatalf("escalation released a reservation: finalizations = %d", fixture.refresher.finalizations)
	}
	lease, _ := fixture.leases.Lease("order-unknown")
	if !lease.Escalated() {
		t.Fatalf("lease = %#v, want escalated", lease)
	}
}

// TestWaitingOrderExplainedByFinalityPendingFillIsHealthy 验证等待终局的正常成交路径
// 不会被当成未解决订单：账本视图已经吸收了 MINED 成交，本轮 COMPLETED 且无订单级问题。
func TestWaitingOrderExplainedByFinalityPendingFillIsHealthy(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.fills.errors = map[string]error{"order-unknown": &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE", Message: "trade details pending"}}
	fill := finalityPendingFill("unknown", "token-1", domain.SideBuy, "5", "-2", testNow.Add(-time.Minute))
	fixture.ledger.finalityPending = []domain.Fill{fill}
	fixture.balance = "98"
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	if result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("run status = %s issues = %#v, want COMPLETED", result.Run.Status, result.Issues)
	}
	assertNoIssue(t, result.Issues, domain.ReconciliationIssueOrderRecoveryPending)
	if result.Run.Summary["orders_recovery_explained_by_finality_pending_fills"] != 1 || result.Run.Summary["orders_unresolved"] != 0 ||
		result.Run.Summary["positions_explained_by_finality_pending_fills"] != 1 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
}

// TestPropagatingOrderInsideGraceIsSilentButExcluded 验证成交明细尚未传播、且没有
// 任何链上证据的短暂窗口内：不产生问题，但该 token 本轮不参与持仓比较。
func TestPropagatingOrderInsideGraceIsSilentButExcluded(t *testing.T) {
	fixture := newRecoveryFixture()
	fixture.fills.errors = map[string]error{"order-unknown": &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_FILL_DETAILS_UNAVAILABLE", Message: "trade details pending"}}
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	if result.Run.Status != domain.ReconciliationRunCompleted {
		t.Fatalf("run status = %s issues = %#v, want COMPLETED inside the grace window", result.Run.Status, result.Issues)
	}
	assertNoIssue(t, result.Issues, domain.ReconciliationIssuePositionDrift)
	if result.Run.Summary["orders_recovery_propagating"] != 1 || result.Run.Summary["positions_deferred_for_unresolved_orders"] != 1 {
		t.Fatalf("summary = %#v", result.Run.Summary)
	}
}

// TestResolvedRecoveryOrderReleasesLease 验证订单脱离恢复状态后租约被删除，且本轮 COMPLETED。
func TestResolvedRecoveryOrderReleasesLease(t *testing.T) {
	fixture := newRecoveryFixture()
	filled := unknownBuyOrder()
	filled.Status = domain.OrderStatusFilled
	fixture.refresher.orders["order-unknown"] = filled
	fixture.external[0].Shares = "10"
	service := fixture.service(t, orderrecovery.Policy{})

	result := runScheduled(t, service)
	if result.Run.Status != domain.ReconciliationRunCompleted || result.Run.Summary["orders_recovery_resolved"] != 1 {
		t.Fatalf("run = %#v issues = %#v", result.Run, result.Issues)
	}
	if _, exists := fixture.leases.Lease("order-unknown"); exists {
		t.Fatal("resolved order still has a lease row")
	}
}

package reconciliation

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const defaultLookback = 48 * time.Hour

// OrderRefresher 将所有生命周期变更交给已有的可审计状态机，对账模块不会直接写 execution_orders。
type OrderRefresher interface {
	Refresh(context.Context, string) (domain.Order, error)
	FinalizeCancellation(context.Context, string) (domain.Order, error)
}

// Params 表示后端使用的 Params 类型。
type Params struct {
	Orders          port.ReconciliationOrderRepository
	Venue           port.VenueReconciliationSource
	PositionSources []port.ExternalPositionSource
	BalanceSources  []port.ExternalBalanceSource
	Ledger          port.ReconciliationLedger
	Fills           port.FillSynchronizer
	OrderRefresher  OrderRefresher
	Recorder        port.ReconciliationRecorder
	TradeLookback   time.Duration
	PositionEpsilon domain.Decimal
	BalanceEpsilon  domain.Decimal
	Now             func() time.Time
	NewID           func() string
}

// Service 表示后端使用的 Service 类型。
type Service struct {
	orders          port.ReconciliationOrderRepository
	venue           port.VenueReconciliationSource
	positionSources []port.ExternalPositionSource
	balanceSources  []port.ExternalBalanceSource
	ledger          port.ReconciliationLedger
	fills           port.FillSynchronizer
	orderRefresher  OrderRefresher
	recorder        port.ReconciliationRecorder
	tradeLookback   time.Duration
	positionEpsilon domain.Decimal
	balanceEpsilon  domain.Decimal
	now             func() time.Time
	newID           func() string
}

// Result 表示后端使用的 Result 类型。
type Result struct {
	Run    domain.ReconciliationRun     `json:"run"`
	Issues []domain.ReconciliationIssue `json:"issues"`
}

// RunAccountParams 收拢单账户对账所需的业务参数，避免公共函数参数持续膨胀。
type RunAccountParams struct {
	ExecutionAccountID string
	Trigger            domain.ReconciliationTrigger
	FocusOrderID       string
}

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.Orders == nil || params.Venue == nil || params.Ledger == nil || params.Fills == nil ||
		params.OrderRefresher == nil || params.Recorder == nil {
		return nil, fmt.Errorf("orders, venue, ledger, fills, order refresher, and recorder are required")
	}
	if len(params.PositionSources) == 0 || len(params.BalanceSources) == 0 {
		return nil, fmt.Errorf("at least one external position source and balance source are required")
	}
	for index, source := range params.PositionSources {
		if source == nil {
			return nil, fmt.Errorf("position source %d is nil", index)
		}
	}
	for index, source := range params.BalanceSources {
		if source == nil {
			return nil, fmt.Errorf("balance source %d is nil", index)
		}
	}
	if params.TradeLookback == 0 {
		params.TradeLookback = defaultLookback
	}
	if params.TradeLookback < time.Minute {
		return nil, fmt.Errorf("trade lookback must be at least one minute")
	}
	if params.PositionEpsilon.IsEmpty() {
		params.PositionEpsilon = "0.000001"
	}
	if params.BalanceEpsilon.IsEmpty() {
		params.BalanceEpsilon = "0.000001"
	}
	for name, epsilon := range map[string]domain.Decimal{
		"position epsilon": params.PositionEpsilon,
		"balance epsilon":  params.BalanceEpsilon,
	} {
		if sign, err := epsilon.Sign(); err != nil || sign < 0 {
			return nil, fmt.Errorf("%s must be a non-negative decimal", name)
		}
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	if params.NewID == nil {
		params.NewID = newID
	}
	return &Service{
		orders: params.Orders, venue: params.Venue,
		positionSources: append([]port.ExternalPositionSource(nil), params.PositionSources...),
		balanceSources:  append([]port.ExternalBalanceSource(nil), params.BalanceSources...),
		ledger:          params.Ledger, fills: params.Fills, orderRefresher: params.OrderRefresher,
		recorder: params.Recorder, tradeLookback: params.TradeLookback,
		positionEpsilon: params.PositionEpsilon, balanceEpsilon: params.BalanceEpsilon,
		now: params.Now, newID: params.NewID,
	}, nil
}

// RunAccount 按证据优先顺序恢复成交并核对指定账户的订单、仓位和余额。
func (service *Service) RunAccount(ctx context.Context, params RunAccountParams) (Result, error) {
	params = params.normalize()
	if params.ExecutionAccountID == "" {
		return Result{}, fmt.Errorf("execution account id is required")
	}
	if !validTrigger(params.Trigger) {
		return Result{}, fmt.Errorf("unsupported reconciliation trigger %q", params.Trigger)
	}
	now := service.now().UTC()
	run, err := (domain.ReconciliationRunParams{
		RunID: service.newID(), ExecutionAccountID: params.ExecutionAccountID,
		Trigger: params.Trigger, FocusOrderID: params.FocusOrderID, StartedAt: now,
	}).Build()
	if err != nil {
		return Result{}, err
	}
	if err := service.recorder.Start(ctx, run); err != nil {
		return Result{Run: run}, err
	}
	state := runState{service: service, run: &run, now: now}
	scope := accountRunScope{
		executionAccountID: params.ExecutionAccountID,
		focusOrderID:       params.FocusOrderID,
		scanAfter:          reconciliationScanStart(now, service.tradeLookback, params.Trigger),
	}
	balance, orders, err := state.loadLocalAuthority(ctx, scope)
	if err != nil {
		return service.finish(ctx, state, errors.Join(err, errors.Join(state.errors...)))
	}
	evidence, venueErr := state.collectVenueEvidence(ctx, scope, orders)
	state.reconcileOrders(ctx, reconcileOrdersParams{orders: orders, focusOrderID: scope.focusOrderID, evidence: evidence})
	positionErr := state.reconcilePositions(ctx, scope.executionAccountID, balance.WalletAddress)
	balanceErr := state.reconcileBalance(ctx, scope.executionAccountID)

	return service.finish(ctx, state, errors.Join(venueErr, positionErr, balanceErr, errors.Join(state.errors...)))
}

// normalize 去除单账户对账参数中允许出现的首尾空白。
func (params RunAccountParams) normalize() RunAccountParams {
	params.ExecutionAccountID = strings.TrimSpace(params.ExecutionAccountID)
	params.FocusOrderID = strings.TrimSpace(params.FocusOrderID)
	return params
}

// runState 表示后端使用的 runState 类型。
type runState struct {
	service *Service
	run     *domain.ReconciliationRun
	now     time.Time
	issues  []domain.ReconciliationIssue
	errors  []error
}

// issue 补全对账问题身份并通过参数构建器持久化到运行结果。
func (state *runState) issue(ctx context.Context, params domain.ReconciliationIssueParams) {
	issue := domain.ReconciliationIssue(params)
	issue.IssueID = state.service.newID()
	issue.RunID = state.run.RunID
	issue.ExecutionAccountID = state.run.ExecutionAccountID
	issue.ObservedAt = state.now
	if issue.Status == domain.ReconciliationIssueResolved {
		resolvedAt := state.now
		issue.ResolvedAt = &resolvedAt
	}
	issue.Fingerprint = issueFingerprint(issue)
	issue, err := domain.ReconciliationIssueParams(issue).Build()
	if err != nil {
		state.errors = append(state.errors, err)
		return
	}
	if err := state.service.recorder.RecordIssue(ctx, issue); err != nil {
		state.errors = append(state.errors, err)
	}
	state.issues = append(state.issues, issue)
	state.run.Summary["issues_total"]++
	state.run.Summary["issues_"+strings.ToLower(string(issue.Status))]++
	state.run.Summary["issues_"+strings.ToLower(string(issue.Resolution))]++
}

// addInfrastructureIssue 累加或记录 Infrastructure Issue。
func (state *runState) addInfrastructureIssue(ctx context.Context, source, operation string, err error) {
	state.issue(ctx, domain.ReconciliationIssueParams{
		Type: domain.ReconciliationIssueSourceUnavailable, Resolution: domain.ReconciliationResolutionRetry,
		Status: domain.ReconciliationIssueOpen, Source: source,
		Details: operation + " failed; the result is unknown, not empty: " + err.Error(),
	})
	state.errors = append(state.errors, err)
}

// finish 完成并持久化 对应数据。
func (service *Service) finish(ctx context.Context, state runState, cause error) (Result, error) {
	completedAt := service.now().UTC()
	state.run.CompletedAt = &completedAt
	state.run.Status = domain.ReconciliationRunCompleted
	for _, issue := range state.issues {
		if issue.Status == domain.ReconciliationIssueOpen {
			state.run.Status = domain.ReconciliationRunAttentionRequired
			break
		}
	}
	if cause != nil {
		state.run.Error = cause.Error()
		// Infrastructure uncertainty is not a successful reconciliation. It is
		// still ATTENTION_REQUIRED when useful comparisons completed; FAILED is
		// reserved for a run that could not even establish local authority.
		if state.run.Summary["local_orders"] == 0 && state.run.Summary["local_positions"] == 0 {
			state.run.Status = domain.ReconciliationRunFailed
		}
	}
	completeErr := service.recorder.Complete(ctx, *state.run)
	return Result{Run: *state.run, Issues: state.issues}, errors.Join(cause, completeErr)
}

// validTrigger 判断当前业务条件是否成立。
func validTrigger(trigger domain.ReconciliationTrigger) bool {
	switch trigger {
	case domain.ReconciliationTriggerStartup, domain.ReconciliationTriggerScheduled,
		domain.ReconciliationTriggerOrderUnknown, domain.ReconciliationTriggerCancelUnknown,
		domain.ReconciliationTriggerAssetDrift:
		return true
	default:
		return false
	}
}

// issueFingerprint 根据稳定业务身份生成幂等标识。
func issueFingerprint(issue domain.ReconciliationIssue) string {
	parts := []string{
		string(issue.Type), issue.ExecutionAccountID, issue.OrderID, issue.VenueOrderID,
		issue.VenueTradeID, issue.MarketID, issue.ConditionID, issue.TokenID, issue.Source,
		issue.LocalValue.String(), issue.RemoteValue.String(),
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

// newID 生成随机且稳定格式的对账运行标识。
func newID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err == nil {
		return "recon:" + hex.EncodeToString(value)
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return "recon:" + hex.EncodeToString(digest[:16])
}

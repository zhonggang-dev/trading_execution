package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

// AccountReconciler 表示后端使用的 AccountReconciler 类型。
type AccountReconciler interface {
	RunAccount(context.Context, string, domain.ReconciliationTrigger, string) (Result, error)
}

// RunnerParams 表示后端使用的 RunnerParams 类型。
type RunnerParams struct {
	Service  AccountReconciler
	Accounts []string
	Interval time.Duration
}

// Runner 表示后端使用的 Runner 类型。
type Runner struct {
	service  AccountReconciler
	accounts []string
	interval time.Duration
	requests chan request

	mu          sync.Mutex
	lastResults map[string]Result
}

// request 表示后端使用的 request 类型。
type request struct {
	accountID string
	trigger   domain.ReconciliationTrigger
	orderID   string
}

// SweepResult 表示后端使用的 SweepResult 类型。
type SweepResult struct {
	Trigger domain.ReconciliationTrigger `json:"trigger"`
	Runs    []Result                     `json:"runs"`
	Errors  []error                      `json:"-"`
}

var _ port.ReconciliationTriggerer = (*Runner)(nil)

// NewRunner 校验账户和周期配置后创建对账运行器。
func NewRunner(params RunnerParams) (*Runner, error) {
	if params.Service == nil {
		return nil, fmt.Errorf("reconciliation service is required")
	}
	if params.Interval == 0 {
		params.Interval = 5 * time.Minute
	}
	if params.Interval < time.Second {
		return nil, fmt.Errorf("reconciliation interval must be at least one second")
	}
	seen := make(map[string]struct{})
	accounts := make([]string, 0, len(params.Accounts))
	for _, raw := range params.Accounts {
		account := strings.TrimSpace(raw)
		if account == "" {
			return nil, fmt.Errorf("reconciliation account id is empty")
		}
		if _, duplicate := seen[account]; duplicate {
			continue
		}
		seen[account] = struct{}{}
		accounts = append(accounts, account)
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("at least one reconciliation account is required")
	}
	return &Runner{
		service: params.Service, accounts: accounts, interval: params.Interval,
		requests: make(chan request, 1024), lastResults: make(map[string]Result),
	}, nil
}

// Trigger 以非阻塞方式提交一次账户对账请求。
func (runner *Runner) Trigger(accountID string, trigger domain.ReconciliationTrigger, orderID string) {
	request := request{accountID: strings.TrimSpace(accountID), trigger: trigger, orderID: strings.TrimSpace(orderID)}
	if request.accountID == "" {
		return
	}
	select {
	case runner.requests <- request:
	default:
	}
}

// Run 先执行启动对账再持续处理周期和即时对账请求。
func (runner *Runner) Run(ctx context.Context) error {
	startup := runner.Sweep(ctx, domain.ReconciliationTriggerStartup)
	if err := ctx.Err(); err != nil {
		return err
	}
	ticker := time.NewTicker(runner.interval)
	defer ticker.Stop()
	var accumulated []error
	accumulated = append(accumulated, startup.Errors...)
	for {
		select {
		case <-ctx.Done():
			return errors.Join(append(accumulated, ctx.Err())...)
		case <-ticker.C:
			result := runner.Sweep(ctx, domain.ReconciliationTriggerScheduled)
			accumulated = appendBounded(accumulated, result.Errors...)
		case requested := <-runner.requests:
			result, err := runner.service.RunAccount(ctx, requested.accountID, requested.trigger, requested.orderID)
			runner.remember(requested.accountID, result)
			if err != nil {
				accumulated = appendBounded(accumulated, err)
			}
		}
	}
}

// Sweep 执行一次有界扫描并处理选中的记录。
func (runner *Runner) Sweep(ctx context.Context, trigger domain.ReconciliationTrigger) SweepResult {
	result := SweepResult{Trigger: trigger}
	for _, accountID := range runner.accounts {
		if err := ctx.Err(); err != nil {
			result.Errors = append(result.Errors, err)
			break
		}
		run, err := runner.service.RunAccount(ctx, accountID, trigger, "")
		result.Runs = append(result.Runs, run)
		runner.remember(accountID, run)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("reconcile %s: %w", accountID, err))
		}
	}
	return result
}

// LastResult 返回指定账户最近一次对账结果。
func (runner *Runner) LastResult(accountID string) (Result, bool) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	result, ok := runner.lastResults[strings.TrimSpace(accountID)]
	return result, ok
}

// remember 并发安全地保存账户最近一次对账结果。
func (runner *Runner) remember(accountID string, result Result) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.lastResults[accountID] = result
}

// appendBounded 追加并限制 Bounded。
func appendBounded(existing []error, values ...error) []error {
	for _, value := range values {
		if value == nil {
			continue
		}
		if len(existing) == 100 {
			copy(existing, existing[1:])
			existing = existing[:99]
		}
		existing = append(existing, value)
	}
	return existing
}

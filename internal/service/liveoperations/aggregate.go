package liveoperations

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// accountObservation 保存一个账户本轮读取到的外部资金、持仓、订单和成交事实。
type accountObservation struct {
	account       domain.LiveAccountState
	balance       domain.ExternalBalance
	positions     []domain.ExternalPosition
	openOrders    []domain.VenueOrderSnapshot
	trades        []domain.VenueTradeSnapshot
	positionAt    time.Time
	openOrdersAt  time.Time
	tradesAt      time.Time
	openOrdersErr error
	tradesErr     error
	coreErr       error
}

// build 采集全部事实来源并生成一份新的不可变快照。
func (service *Service) build(ctx context.Context) (domain.LiveOperationsSnapshot, error) {
	observedAt := service.now().UTC()
	query := domain.LiveOperationsQuery{
		ExecutionAccountIDs: service.accounts,
		RunID:               service.runID,
		DayStart:            time.Date(observedAt.Year(), observedAt.Month(), observedAt.Day(), 0, 0, 0, 0, time.UTC),
		ObservedAt:          observedAt,
		RecentOrderSince:    observedAt.Add(-24 * time.Hour),
		EventLimit:          service.eventLimit,
	}
	local, err := service.repository.LoadLiveOperations(ctx, query)
	if err != nil {
		return domain.LiveOperationsSnapshot{}, fmt.Errorf("load local live operations state: %w", err)
	}
	observations, err := service.collectExternalAccounts(ctx, local.Accounts, query.RecentOrderSince)
	if err != nil {
		return domain.LiveOperationsSnapshot{}, err
	}
	books, bookErr := service.capturePositionBooks(ctx, observedAt, local.Positions)
	return service.composeSnapshot(composeParams{
		observedAt: observedAt, local: local, observations: observations,
		books: books, bookErr: bookErr,
	})
}

// collectExternalAccounts 并行采集各执行账户，任何核心余额或持仓失败都使本轮刷新失败。
func (service *Service) collectExternalAccounts(ctx context.Context, accounts []domain.LiveAccountState, tradesAfter time.Time) ([]accountObservation, error) {
	results := make(chan accountObservation, len(accounts))
	for _, account := range accounts {
		account := account
		go func() {
			results <- service.collectExternalAccount(ctx, account, tradesAfter)
		}()
	}
	observations := make([]accountObservation, 0, len(accounts))
	var coreErrors []error
	for range accounts {
		observation := <-results
		observations = append(observations, observation)
		if observation.coreErr != nil {
			coreErrors = append(coreErrors, fmt.Errorf("execution account %s: %w", observation.account.ExecutionAccountID, observation.coreErr))
		}
	}
	if err := errors.Join(coreErrors...); err != nil {
		return nil, fmt.Errorf("core live operations data unavailable: %w", err)
	}
	sort.Slice(observations, func(left, right int) bool {
		return observations[left].account.ExecutionAccountID < observations[right].account.ExecutionAccountID
	})
	return observations, nil
}

// collectExternalAccount 读取一个钱包的真实余额、Data API 持仓以及 CLOB 订单和成交。
func (service *Service) collectExternalAccount(ctx context.Context, account domain.LiveAccountState, tradesAfter time.Time) accountObservation {
	result := accountObservation{account: account}
	balance, balanceErr := service.balanceSource.GetExternalBalance(ctx, account.WalletAddress, account.CollateralAsset)
	if balanceErr != nil {
		result.coreErr = errors.Join(result.coreErr, fmt.Errorf("read wallet collateral balance: %w", balanceErr))
	} else {
		if balance.ObservedAt.IsZero() || !strings.EqualFold(strings.TrimSpace(balance.Asset), strings.TrimSpace(account.CollateralAsset)) {
			result.coreErr = errors.Join(result.coreErr, fmt.Errorf("wallet balance identity or observation time is invalid"))
		} else if amount, parseErr := decimalRat(balance.Amount); parseErr != nil || amount.Sign() < 0 {
			result.coreErr = errors.Join(result.coreErr, fmt.Errorf("wallet balance amount is invalid"))
		} else {
			result.balance = balance
		}
	}
	positions, positionErr := service.positionSource.ListExternalPositions(ctx, account.WalletAddress)
	result.positionAt = service.now().UTC()
	if positionErr != nil {
		result.coreErr = errors.Join(result.coreErr, fmt.Errorf("read external positions: %w", positionErr))
	} else {
		result.positions = positions
		for _, position := range positions {
			if position.ObservedAt.IsZero() {
				result.coreErr = errors.Join(result.coreErr, fmt.Errorf("external position %s has no observation time", position.TokenID))
				break
			}
		}
		if len(positions) > 0 {
			result.positionAt = oldestExternalPositionTime(positions, result.positionAt)
		}
	}
	result.openOrders, result.openOrdersErr = service.venue.ListReconciliationOpenOrders(ctx, account.ExecutionAccountID)
	result.openOrdersAt = service.now().UTC()
	if result.openOrdersErr == nil && len(result.openOrders) > 0 {
		result.openOrdersAt = oldestVenueOrderTime(result.openOrders, result.openOrdersAt)
	}
	result.trades, result.tradesErr = service.venue.ListReconciliationTrades(ctx, account.ExecutionAccountID, tradesAfter)
	result.tradesAt = service.now().UTC()
	if result.tradesErr == nil && len(result.trades) > 0 {
		result.tradesAt = oldestVenueTradeTime(result.trades, result.tradesAt)
	}
	return result
}

// capturePositionBooks 批量获取当前本地非零仓位对应的 CLOB 盘口。
func (service *Service) capturePositionBooks(ctx context.Context, observedAt time.Time, positions []domain.LiveLedgerPosition) (map[string]domain.OrderBookSnapshot, error) {
	targets := make([]domain.BookTarget, 0, len(positions))
	seen := make(map[string]struct{}, len(positions))
	for _, item := range positions {
		position := item.Position
		if position.OutcomeIndex == nil || (*position.OutcomeIndex != 0 && *position.OutcomeIndex != 1) {
			continue
		}
		if _, duplicate := seen[position.TokenID]; duplicate {
			continue
		}
		seen[position.TokenID] = struct{}{}
		targets = append(targets, domain.BookTarget{
			MarketID: position.MarketID, ConditionID: position.ConditionID,
			OutcomeIndex: *position.OutcomeIndex, TokenID: position.TokenID,
		})
	}
	if len(targets) == 0 {
		return map[string]domain.OrderBookSnapshot{}, nil
	}
	books, err := service.orderBooks.Capture(ctx, observedAt, targets)
	if err != nil {
		return map[string]domain.OrderBookSnapshot{}, err
	}
	result := make(map[string]domain.OrderBookSnapshot, len(books))
	for _, book := range books {
		result[book.TokenID] = book
	}
	return result, nil
}

// composeParams 收拢生成 HTTP 快照所需的本地和外部状态。
type composeParams struct {
	observedAt   time.Time
	local        domain.LiveOperationsLocalState
	observations []accountObservation
	books        map[string]domain.OrderBookSnapshot
	bookErr      error
}

// composeSnapshot 计算资金、持仓、订单、风险和一致性状态。
func (service *Service) composeSnapshot(params composeParams) (domain.LiveOperationsSnapshot, error) {
	quality := newQualityCollector()
	quality.add("ledger", "事件账本", domain.LiveHealthHealthy, "PostgreSQL 可重复读快照已完成")
	quality.add("clob", "CLOB 订单与成交", domain.LiveHealthHealthy, "CLOB 订单和成交读取成功")
	quality.add("positions", "链上持仓", domain.LiveHealthHealthy, "Data API 持仓已读取")
	quality.add("reconcile", "链上对账", domain.LiveHealthHealthy, "最近对账未发现未关闭问题")

	exposureLimit, presetName, riskState := summarizeRiskPolicies(params.local.RiskPolicies, quality)
	positions, positionTotals, err := buildPositions(buildPositionsParams{
		observedAt: params.observedAt, local: params.local.Positions,
		observations: params.observations, books: params.books,
		exposureLimit: exposureLimit, quality: quality, bookErr: params.bookErr,
	})
	if err != nil {
		return domain.LiveOperationsSnapshot{}, fmt.Errorf("build live positions: %w", err)
	}
	orders, err := buildOrders(params.observedAt, params.local.Orders, params.observations, quality)
	if err != nil {
		return domain.LiveOperationsSnapshot{}, fmt.Errorf("build live orders: %w", err)
	}
	checkVenueTrades(params.local.ConfirmedTradeIDs, params.observations, quality)
	reconciliationHealth := reconciliationStatus(params.observedAt, params.local.Reconciliations, params.local.RiskPolicies, quality)
	workers, workerHealth := buildWorkers(params.observedAt, params.local.Workers)
	funnel := normalizedFunnel(params.local.Funnel)

	availableCash := new(big.Rat)
	oldestSource := params.local.DatabaseObservedAt
	for _, observation := range params.observations {
		if err := addDecimal(availableCash, observation.balance.Amount); err != nil {
			return domain.LiveOperationsSnapshot{}, fmt.Errorf("sum wallet balance: %w", err)
		}
		oldestSource = earlierTime(oldestSource, observation.balance.ObservedAt)
		oldestSource = earlierTime(oldestSource, observation.positionAt)
		if observation.openOrdersErr != nil {
			quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, "至少一个账户无法读取 CLOB Open Orders")
		} else {
			oldestSource = earlierTime(oldestSource, observation.openOrdersAt)
		}
		if observation.tradesErr != nil {
			quality.add("clob", "CLOB 订单与成交", domain.LiveHealthDegraded, "至少一个账户无法读取 CLOB /trades")
		} else {
			oldestSource = earlierTime(oldestSource, observation.tradesAt)
		}
	}
	oldestSource = earlierTime(oldestSource, positionTotals.oldestBookAt)
	equity := new(big.Rat).Add(new(big.Rat).Set(availableCash), positionTotals.marketValue)
	capital, err := buildCapital(buildCapitalParams{
		equity: equity, availableCash: availableCash, grossExposure: positionTotals.cost,
		exposureLimit: exposureLimit, unrealizedPnL: positionTotals.unrealizedPnL,
		realizedPnLToday: params.local.RealizedPnLToday, feeToday: params.local.FeeToday,
	})
	if err != nil {
		return domain.LiveOperationsSnapshot{}, err
	}
	risks, err := buildRisks(buildRisksParams{
		positionTotals: positionTotals, exposureLimit: exposureLimit,
		policies: params.local.RiskPolicies, dailyTraded: params.local.DailyTradedNotional,
		positions: params.local.Positions, observedAt: params.observedAt,
	})
	if err != nil {
		return domain.LiveOperationsSnapshot{}, err
	}
	venueHealth := quality.status("clob")
	positionHealth := quality.status("positions")
	engineHealth := worstHealth(worstHealth(venueHealth, positionHealth), reconciliationHealth)
	engineHealth = worstHealth(engineHealth, workerHealth)
	engineHealth = worstHealth(engineHealth, riskState)
	engineHealth = worstHealth(engineHealth, liveRiskHealth(risks))
	snapshot := domain.LiveOperationsSnapshot{
		ObservedAt: params.observedAt, SourceObservedAt: oldestSource,
		Engine: domain.LiveEngine{
			Health: engineHealth, RunID: service.runID, PresetName: presetName,
			StartedAt: service.startedAt, VenueName: service.venueName,
			VenueStatus: venueHealth, LedgerStatus: domain.LiveHealthHealthy,
			ReconciliationStatus: reconciliationHealth,
		},
		Capital: capital, Workers: workers, Funnel: funnel, Risks: risks,
		Orders: orders, Positions: positions, Events: nonNilEvents(params.local.Events),
		DataQuality: quality.list(),
	}
	if snapshot.SourceObservedAt.IsZero() {
		snapshot.SourceObservedAt = params.observedAt
	}
	return snapshot, nil
}

// oldestExternalPositionTime 返回外部持仓中最早的观察时间。
func oldestExternalPositionTime(values []domain.ExternalPosition, fallback time.Time) time.Time {
	result := fallback
	for _, value := range values {
		result = earlierTime(result, value.ObservedAt)
	}
	return result
}

// oldestVenueOrderTime 返回外部订单中最早的观察时间。
func oldestVenueOrderTime(values []domain.VenueOrderSnapshot, fallback time.Time) time.Time {
	result := fallback
	for _, value := range values {
		result = earlierTime(result, value.ObservedAt)
	}
	return result
}

// oldestVenueTradeTime 返回外部成交中最早的观察时间。
func oldestVenueTradeTime(values []domain.VenueTradeSnapshot, fallback time.Time) time.Time {
	result := fallback
	for _, value := range values {
		result = earlierTime(result, value.ObservedAt)
	}
	return result
}

// earlierTime 返回两个非零时间中较早的一个。
func earlierTime(left time.Time, right time.Time) time.Time {
	if right.IsZero() {
		return left
	}
	if left.IsZero() || right.Before(left) {
		return right.UTC()
	}
	return left.UTC()
}

// qualityCollector 合并同一事实来源的多条检查，并保留最严重状态。
type qualityCollector struct {
	items map[string]domain.LiveDataQuality
	order []string
}

// newQualityCollector 创建数据质量收集器。
func newQualityCollector() *qualityCollector {
	return &qualityCollector{items: make(map[string]domain.LiveDataQuality)}
}

// add 写入或升级一个数据质量结果。
func (collector *qualityCollector) add(id string, name string, status domain.LiveHealth, detail string) {
	current, exists := collector.items[id]
	if !exists {
		collector.order = append(collector.order, id)
		collector.items[id] = domain.LiveDataQuality{ID: id, Name: name, Status: status, Detail: detail}
		return
	}
	if worstHealth(current.Status, status) == status && status != current.Status {
		current.Status, current.Detail = status, detail
	} else if status == current.Status && detail != "" && !strings.Contains(current.Detail, detail) {
		current.Detail += "；" + detail
	}
	collector.items[id] = current
}

// status 返回指定事实来源的当前健康状态。
func (collector *qualityCollector) status(id string) domain.LiveHealth {
	if item, found := collector.items[id]; found {
		return item.Status
	}
	return domain.LiveHealthStopped
}

// list 按稳定顺序返回数据质量数组。
func (collector *qualityCollector) list() []domain.LiveDataQuality {
	result := make([]domain.LiveDataQuality, 0, len(collector.order))
	for _, id := range collector.order {
		result = append(result, collector.items[id])
	}
	return result
}

// nonNilEvents 保证空事件编码为 [] 而不是 null。
func nonNilEvents(values []domain.LiveEvent) []domain.LiveEvent {
	if values == nil {
		return []domain.LiveEvent{}
	}
	return values
}

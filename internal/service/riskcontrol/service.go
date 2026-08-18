package riskcontrol

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const defaultMaxFutureSkew = 2 * time.Second

// Params 表示后端使用的 Params 类型。
type Params struct {
	Source        port.HardRiskSource
	MaxFutureSkew time.Duration
	Now           func() time.Time
}

// Service is the Go-owned hard-risk gate. It only accepts or rejects the
// original intent; it never changes side, price, size, market, or account.
type Service struct {
	source        port.HardRiskSource
	maxFutureSkew time.Duration
	now           func() time.Time
}

var _ port.Guard = (*Service)(nil)

// New 校验依赖和配置后创建当前服务实例。
func New(params Params) (*Service, error) {
	if params.Source == nil {
		return nil, fmt.Errorf("hard risk source is required")
	}
	if params.MaxFutureSkew == 0 {
		params.MaxFutureSkew = defaultMaxFutureSkew
	}
	if params.MaxFutureSkew < 0 {
		return nil, fmt.Errorf("hard risk maximum future skew must not be negative")
	}
	if params.Now == nil {
		params.Now = time.Now
	}
	return &Service{source: params.Source, maxFutureSkew: params.MaxFutureSkew, now: params.Now}, nil
}

// Check 根据当前硬风控策略检查订单意图。
func (service *Service) Check(ctx context.Context, intent domain.OrderIntent) error {
	intent = intent.Normalize()
	now := service.now().UTC()
	snapshot, err := service.source.Snapshot(ctx, intent, now)
	if err != nil {
		return fmt.Errorf("load hard risk snapshot: %w", err)
	}
	if err := validateSnapshot(snapshot, intent); err != nil {
		return fmt.Errorf("validate hard risk snapshot: %w", err)
	}
	if err := checkControls(snapshot.Controls); err != nil {
		return err
	}
	if err := checkTimestamp("PRICE", intent.MarketSnapshotAt, now, snapshot.Limits.MaxPriceAge, service.maxFutureSkew); err != nil {
		return err
	}
	if err := checkTimestamp("SIGNAL", intent.SignalAt, now, snapshot.Limits.MaxSignalAge, service.maxFutureSkew); err != nil {
		return err
	}
	if err := checkTimestamp("RISK_STATE", &snapshot.ObservedAt, now, snapshot.Limits.MaxStateAge, service.maxFutureSkew); err != nil {
		return err
	}

	candidateNotional, err := orderNotional(intent)
	if err != nil {
		return reject("INVALID_ORDER_NOTIONAL", err.Error())
	}
	if exceeds(candidateNotional, snapshot.Limits.MaxOrderNotional) {
		return reject("MAX_ORDER_NOTIONAL_EXCEEDED", "order notional exceeds the single-order limit")
	}
	dailyTraded, _ := decimalRat(snapshot.DailyTradedNotional)
	if exceeds(new(big.Rat).Add(dailyTraded, candidateNotional), snapshot.Limits.MaxDailyTradedNotional) {
		return reject("DAILY_TRADED_NOTIONAL_EXCEEDED", "order would exceed the wallet daily traded-notional limit")
	}

	state, err := calculateState(snapshot, intent)
	if err != nil {
		return fmt.Errorf("calculate hard risk state: %w", err)
	}
	if state.sameDirectionOrder {
		if intent.Side == domain.SideSell {
			return reject("DUPLICATE_SELL_ORDER", "an active sell order already reserves this token")
		}
		return reject("SAME_DIRECTION_ORDER_EXISTS", "an active order already exists for this token and direction")
	}

	if intent.Side == domain.SideSell {
		candidateSize, _ := decimalRat(intent.Size)
		if candidateSize.Cmp(state.availablePositionSize) > 0 {
			return reject("INSUFFICIENT_SELL_POSITION", "sell size exceeds wallet position after active sell reservations")
		}
		return nil
	}

	availableBalance, _ := decimalRat(snapshot.AvailableBalance)
	if candidateNotional.Cmp(availableBalance) > 0 {
		return reject("INSUFFICIENT_WALLET_BALANCE", "available wallet balance is below protected order notional")
	}
	if exceeds(new(big.Rat).Add(state.marketExposure, candidateNotional), snapshot.Limits.MaxMarketExposure) {
		return reject("MAX_MARKET_EXPOSURE_EXCEEDED", "order would exceed the wallet exposure limit for this market")
	}
	if exceeds(new(big.Rat).Add(state.strategyExposure, candidateNotional), snapshot.Limits.MaxStrategyExposure) {
		return reject("MAX_STRATEGY_EXPOSURE_EXCEEDED", "order would exceed the strategy capital-usage limit")
	}
	if exceeds(new(big.Rat).Add(state.walletExposure, candidateNotional), snapshot.Limits.MaxWalletExposure) {
		return reject("MAX_WALLET_EXPOSURE_EXCEEDED", "order would exceed the wallet total exposure limit")
	}
	return nil
}

// calculatedState 表示后端使用的 calculatedState 类型。
type calculatedState struct {
	marketExposure        *big.Rat
	strategyExposure      *big.Rat
	walletExposure        *big.Rat
	availablePositionSize *big.Rat
	sameDirectionOrder    bool
}

// calculateState 精确计算 State。
func calculateState(snapshot domain.HardRiskSnapshot, intent domain.OrderIntent) (calculatedState, error) {
	state := calculatedState{
		marketExposure:        new(big.Rat),
		strategyExposure:      new(big.Rat),
		walletExposure:        new(big.Rat),
		availablePositionSize: new(big.Rat),
	}
	for index, position := range snapshot.Positions {
		riskValue, err := decimalRat(position.RiskValue)
		if err != nil {
			return calculatedState{}, fmt.Errorf("position %d risk value: %w", index, err)
		}
		state.walletExposure.Add(state.walletExposure, riskValue)
		if strings.TrimSpace(position.MarketID) == intent.MarketID {
			state.marketExposure.Add(state.marketExposure, riskValue)
		}
		if strings.TrimSpace(position.StrategyID) == intent.StrategyID {
			state.strategyExposure.Add(state.strategyExposure, riskValue)
		}
		if strings.TrimSpace(position.TokenID) == intent.TokenID {
			size, err := decimalRat(position.AvailableShares)
			if err != nil {
				return calculatedState{}, fmt.Errorf("position %d available shares: %w", index, err)
			}
			state.availablePositionSize.Add(state.availablePositionSize, size)
		}
	}
	for index, order := range snapshot.OpenOrders {
		remainingSize, err := decimalRat(order.RemainingSize)
		if err != nil {
			return calculatedState{}, fmt.Errorf("open order %d remaining size: %w", index, err)
		}
		if strings.TrimSpace(order.TokenID) == intent.TokenID && order.Side == intent.Side {
			state.sameDirectionOrder = true
		}
		if order.Side != domain.SideBuy {
			continue
		}
		price, err := decimalRat(order.WorstPrice)
		if err != nil {
			return calculatedState{}, fmt.Errorf("open order %d worst price: %w", index, err)
		}
		exposure := new(big.Rat).Mul(price, remainingSize)
		state.walletExposure.Add(state.walletExposure, exposure)
		if strings.TrimSpace(order.MarketID) == intent.MarketID {
			state.marketExposure.Add(state.marketExposure, exposure)
		}
		if strings.TrimSpace(order.StrategyID) == intent.StrategyID {
			state.strategyExposure.Add(state.strategyExposure, exposure)
		}
	}
	return state, nil
}

// validateSnapshot 校验 Snapshot 的字段和业务约束。
func validateSnapshot(snapshot domain.HardRiskSnapshot, intent domain.OrderIntent) error {
	if strings.TrimSpace(snapshot.SnapshotID) == "" || strings.TrimSpace(snapshot.ExecutionAccountID) != intent.ExecutionAccountID || snapshot.ObservedAt.IsZero() {
		return fmt.Errorf("snapshot id, account identity, and observed_at are required")
	}
	if strings.TrimSpace(snapshot.Limits.PolicyID) == "" {
		return fmt.Errorf("risk policy id is required")
	}
	for name, value := range map[string]domain.Decimal{
		"max_order_notional":        snapshot.Limits.MaxOrderNotional,
		"max_market_exposure":       snapshot.Limits.MaxMarketExposure,
		"max_strategy_exposure":     snapshot.Limits.MaxStrategyExposure,
		"max_wallet_exposure":       snapshot.Limits.MaxWalletExposure,
		"max_daily_traded_notional": snapshot.Limits.MaxDailyTradedNotional,
	} {
		if sign, err := value.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("%s must be a positive decimal", name)
		}
	}
	if snapshot.Limits.MaxPriceAge <= 0 || snapshot.Limits.MaxSignalAge <= 0 || snapshot.Limits.MaxStateAge <= 0 {
		return fmt.Errorf("risk freshness limits must be positive")
	}
	for name, value := range map[string]domain.Decimal{
		"total_balance":     snapshot.TotalBalance,
		"available_balance": snapshot.AvailableBalance,
		"reserved_balance":  snapshot.ReservedBalance,
	} {
		if sign, err := value.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("%s must be a non-negative decimal", name)
		}
	}
	totalBalance, _ := decimalRat(snapshot.TotalBalance)
	availableBalance, _ := decimalRat(snapshot.AvailableBalance)
	reservedBalance, _ := decimalRat(snapshot.ReservedBalance)
	if totalBalance.Cmp(new(big.Rat).Add(availableBalance, reservedBalance)) != 0 {
		return fmt.Errorf("total_balance must equal available_balance + reserved_balance")
	}
	if sign, err := snapshot.DailyTradedNotional.Sign(); err != nil || sign < 0 {
		return fmt.Errorf("daily traded notional must be a non-negative decimal")
	}
	for index, position := range snapshot.Positions {
		if strings.TrimSpace(position.StrategyID) == "" || strings.TrimSpace(position.MarketID) == "" || strings.TrimSpace(position.TokenID) == "" {
			return fmt.Errorf("position %d identity is incomplete", index)
		}
		for name, value := range map[string]domain.Decimal{
			"total_shares":     position.TotalShares,
			"available_shares": position.AvailableShares,
			"reserved_shares":  position.ReservedShares,
		} {
			if sign, err := value.Sign(); err != nil || sign < 0 {
				return fmt.Errorf("position %d %s must be non-negative", index, name)
			}
		}
		totalShares, _ := decimalRat(position.TotalShares)
		availableShares, _ := decimalRat(position.AvailableShares)
		reservedShares, _ := decimalRat(position.ReservedShares)
		if totalShares.Cmp(new(big.Rat).Add(availableShares, reservedShares)) != 0 {
			return fmt.Errorf("position %d total_shares must equal available_shares + reserved_shares", index)
		}
		if sign, err := position.RiskValue.Sign(); err != nil || sign < 0 {
			return fmt.Errorf("position %d risk value must be non-negative", index)
		}
	}
	for index, order := range snapshot.OpenOrders {
		if strings.TrimSpace(order.OrderID) == "" || strings.TrimSpace(order.StrategyID) == "" ||
			strings.TrimSpace(order.MarketID) == "" || strings.TrimSpace(order.TokenID) == "" {
			return fmt.Errorf("open order %d identity is incomplete", index)
		}
		if order.Side != domain.SideBuy && order.Side != domain.SideSell {
			return fmt.Errorf("open order %d side is invalid", index)
		}
		if sign, err := order.RemainingSize.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("open order %d remaining size must be positive", index)
		}
		if sign, err := order.WorstPrice.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("open order %d worst price must be positive", index)
		}
	}
	return nil
}

// checkControls 检查 Controls。
func checkControls(controls domain.TradingControls) error {
	switch {
	case controls.GlobalKillSwitch:
		return reject("GLOBAL_KILL_SWITCH", "global trading kill switch is enabled")
	case controls.ExecutionAccountPaused:
		return reject("EXECUTION_ACCOUNT_PAUSED", "execution account is paused")
	case controls.StrategyPaused:
		return reject("STRATEGY_PAUSED", "strategy is paused")
	case controls.MarketPaused:
		return reject("MARKET_RISK_PAUSED", "market is paused by the risk control plane")
	default:
		return nil
	}
}

// checkTimestamp 检查 Timestamp。
func checkTimestamp(prefix string, value *time.Time, now time.Time, maxAge, maxFutureSkew time.Duration) error {
	if value == nil || value.IsZero() {
		return reject(prefix+"_TIMESTAMP_REQUIRED", strings.ToLower(prefix)+" timestamp is required")
	}
	timestamp := value.UTC()
	if timestamp.After(now.Add(maxFutureSkew)) {
		return reject(prefix+"_TIMESTAMP_FUTURE", strings.ToLower(prefix)+" timestamp exceeds the allowed clock skew")
	}
	if now.Sub(timestamp) > maxAge {
		return reject(prefix+"_STALE", strings.ToLower(prefix)+" is older than the hard-risk freshness limit")
	}
	return nil
}

// orderNotional 按最坏成交价格计算订单名义金额。
func orderNotional(intent domain.OrderIntent) (*big.Rat, error) {
	price := intent.WorstPrice
	if price.IsEmpty() && intent.Type == domain.OrderTypeLimit {
		price = intent.Price
	}
	if price.IsEmpty() {
		return nil, fmt.Errorf("protected order price is required")
	}
	return price.Multiply(intent.Size)
}

// decimalRat 将十进制值转换为精确有理数。
func decimalRat(value domain.Decimal) (*big.Rat, error) {
	decimal, err := domain.ParseDecimal(value.String())
	if err != nil {
		return nil, err
	}
	parsed, ok := new(big.Rat).SetString(decimal.String())
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", value)
	}
	return parsed, nil
}

// exceeds 判断计算值是否超过 对应数据。
func exceeds(value *big.Rat, limit domain.Decimal) bool {
	maximum, err := decimalRat(limit)
	return err != nil || value.Cmp(maximum) > 0
}

// reject 构建并返回 对应数据 的拒绝结果。
func reject(code, reason string) error {
	return &port.Rejection{Code: code, Reason: reason}
}

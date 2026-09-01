// Package timeexit implements the deterministic, sell-only holding-period
// exit. It deliberately lives below the prediction service so an unavailable
// forecast or strategy endpoint cannot prevent an already-due position from
// being offered back to Polymarket.
package timeexit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const HoldDuration = 48 * time.Hour

const (
	StatusSubmitted             = "SUBMITTED"
	StatusIdempotent            = "IDEMPOTENT"
	StatusFailed                = "FAILED"
	StatusFullyReserved         = "FULLY_RESERVED"
	StatusMarketNotTradable     = "MARKET_NOT_TRADABLE"
	StatusBookUnavailable       = "BOOK_UNAVAILABLE"
	StatusInsufficientLiquidity = "INSUFFICIENT_LIQUIDITY"
	StatusBelowMinimumSize      = "BELOW_MINIMUM_SIZE"
)

// Params contains only execution-owned dependencies. No prediction or alpha
// strategy dependency is permitted on the mandatory holding-period path.
type Params struct {
	Trades     port.PositionExitTradeSource
	Markets    port.MarketUniverse
	OrderBooks port.OrderBookSource
	Executor   port.OrderExecutor
	Accounts   []string
	Venue      string
}

type Service struct {
	trades     port.PositionExitTradeSource
	markets    port.MarketUniverse
	orderBooks port.OrderBookSource
	executor   port.OrderExecutor
	accounts   []string
	venue      string
}

type LotResult struct {
	ExecutionAccountID string             `json:"execution_account_id"`
	LotID              string             `json:"lot_id"`
	TokenID            string             `json:"token_id"`
	Status             string             `json:"status"`
	HeldSeconds        int64              `json:"held_seconds"`
	SellSize           domain.Decimal     `json:"sell_size,omitempty"`
	WorstPrice         domain.Decimal     `json:"worst_price,omitempty"`
	OrderID            string             `json:"order_id,omitempty"`
	OrderStatus        domain.OrderStatus `json:"order_status,omitempty"`
	Error              string             `json:"error,omitempty"`
}

type RunResult struct {
	ScheduledAt time.Time   `json:"scheduled_at"`
	Scanned     int         `json:"scanned"`
	Due         int         `json:"due"`
	Submitted   int         `json:"submitted"`
	Skipped     int         `json:"skipped"`
	Failed      int         `json:"failed"`
	Lots        []LotResult `json:"lots"`
}

type candidate struct {
	trade domain.PositionExitTrade
}

func New(params Params) (*Service, error) {
	if params.Trades == nil || params.Markets == nil || params.OrderBooks == nil || params.Executor == nil {
		return nil, fmt.Errorf("time exit requires trade, market, orderbook, and execution dependencies")
	}
	accounts, err := normalizeAccounts(params.Accounts)
	if err != nil {
		return nil, err
	}
	venue := strings.ToLower(strings.TrimSpace(params.Venue))
	if venue == "" {
		return nil, fmt.Errorf("time exit venue is required")
	}
	return &Service{
		trades: params.Trades, markets: params.Markets, orderBooks: params.OrderBooks,
		executor: params.Executor, accounts: accounts, venue: venue,
	}, nil
}

func normalizeAccounts(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for index, value := range values {
		account := strings.TrimSpace(value)
		if account == "" {
			return nil, fmt.Errorf("time exit account %d is empty", index)
		}
		if _, duplicate := seen[account]; duplicate {
			return nil, fmt.Errorf("time exit account %q is duplicated", account)
		}
		seen[account] = struct{}{}
		result = append(result, account)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("time exit requires at least one execution account")
	}
	sort.Strings(result)
	return result, nil
}

// Run scans every managed open lot. Only lots whose exact opening fill time is
// at least 48 hours old can reach OrderExecutor.Submit.
func (service *Service) Run(ctx context.Context, scheduledAt time.Time) (RunResult, error) {
	scheduledAt = scheduledAt.UTC()
	result := RunResult{ScheduledAt: scheduledAt, Lots: []LotResult{}}
	if scheduledAt.IsZero() {
		return result, fmt.Errorf("time exit scheduled_at is required")
	}

	candidates := make([]candidate, 0)
	targets := make(map[string]domain.BookTarget)
	var runErrors []error
	for _, accountID := range service.accounts {
		trades, err := service.trades.ListOpenPositionExitTrades(ctx, accountID)
		if err != nil {
			runErrors = append(runErrors, fmt.Errorf("load open position lots for %s: %w", accountID, err))
			continue
		}
		for _, trade := range trades {
			result.Scanned++
			if err := trade.Validate(scheduledAt); err != nil {
				runErrors = append(runErrors, fmt.Errorf("validate open position lot %s: %w", trade.LotID, err))
				continue
			}
			if trade.ExecutionAccountID != accountID {
				runErrors = append(runErrors, fmt.Errorf("position lot %s belongs to unexpected account %s", trade.LotID, trade.ExecutionAccountID))
				continue
			}
			if scheduledAt.Sub(trade.EnteredAt.UTC()) < HoldDuration {
				continue
			}
			result.Due++
			lotResult := baseLotResult(trade, scheduledAt)
			if sign, _ := trade.AvailableShares.Sign(); sign == 0 {
				lotResult.Status = StatusFullyReserved
				result.Skipped++
				result.Lots = append(result.Lots, lotResult)
				continue
			}

			market, found, err := service.markets.FindByCondition(ctx, trade.ConditionID)
			if err != nil {
				runErrors = append(runErrors, fmt.Errorf("load market for lot %s: %w", trade.LotID, err))
				continue
			}
			if !found || !tradableMarketMatches(market, trade) {
				lotResult.Status = StatusMarketNotTradable
				result.Skipped++
				result.Lots = append(result.Lots, lotResult)
				continue
			}
			target := domain.BookTarget{
				MarketID: trade.MarketID, ConditionID: trade.ConditionID,
				OutcomeIndex: trade.OutcomeIndex, TokenID: trade.TokenID,
			}
			if existing, ok := targets[trade.TokenID]; ok && existing != target {
				runErrors = append(runErrors, fmt.Errorf("token %s has conflicting time-exit identities", trade.TokenID))
				continue
			}
			targets[trade.TokenID] = target
			candidates = append(candidates, candidate{trade: trade})
		}
	}

	booksByToken := make(map[string]domain.OrderBookSnapshot, len(targets))
	if len(targets) != 0 {
		books, err := service.orderBooks.Capture(ctx, scheduledAt, sortedTargets(targets))
		if err != nil {
			runErrors = append(runErrors, fmt.Errorf("capture time-exit orderbooks: %w", err))
		} else {
			for _, book := range books {
				if _, duplicate := booksByToken[book.TokenID]; duplicate {
					runErrors = append(runErrors, fmt.Errorf("orderbook source returned duplicate token %s", book.TokenID))
					continue
				}
				booksByToken[book.TokenID] = book
			}
		}
	}

	for _, item := range candidates {
		if err := ctx.Err(); err != nil {
			runErrors = append(runErrors, err)
			break
		}
		lotResult := baseLotResult(item.trade, scheduledAt)
		book, found := booksByToken[item.trade.TokenID]
		if !found || !usableBookMatches(book, item.trade) {
			lotResult.Status = StatusBookUnavailable
			result.Skipped++
			result.Lots = append(result.Lots, lotResult)
			continue
		}
		size, err := service.sellSize(item.trade.AvailableShares, book)
		if err != nil {
			runErrors = append(runErrors, fmt.Errorf("calculate sell size for lot %s: %w", item.trade.LotID, err))
			continue
		}
		if sign, _ := size.Sign(); sign == 0 {
			lotResult.Status = StatusInsufficientLiquidity
			result.Skipped++
			result.Lots = append(result.Lots, lotResult)
			continue
		}
		if !book.MinOrderSize.IsEmpty() {
			if comparison, compareErr := size.Compare(book.MinOrderSize); compareErr != nil || comparison < 0 {
				lotResult.Status = StatusBelowMinimumSize
				result.Skipped++
				result.Lots = append(result.Lots, lotResult)
				continue
			}
		}

		intent, err := service.buildIntent(item.trade, book, scheduledAt, size)
		if err != nil {
			runErrors = append(runErrors, fmt.Errorf("build time-exit order for lot %s: %w", item.trade.LotID, err))
			continue
		}
		lotResult.SellSize = size
		lotResult.WorstPrice = book.BestBid
		submitted, submitErr := service.executor.Submit(ctx, intent)
		lotResult.OrderID = submitted.Order.ID
		lotResult.OrderStatus = submitted.Order.Status
		if submitErr != nil {
			lotResult.Status = StatusFailed
			lotResult.Error = submitErr.Error()
			result.Failed++
			if strings.TrimSpace(submitted.Order.ID) == "" {
				runErrors = append(runErrors, fmt.Errorf("submit time-exit order for lot %s: %w", item.trade.LotID, submitErr))
			}
		} else if submitted.Created {
			lotResult.Status = StatusSubmitted
			result.Submitted++
		} else if submitted.Order.Status == domain.OrderStatusRejected || submitted.Order.Status == domain.OrderStatusManualReview {
			lotResult.Status = StatusFailed
			result.Failed++
		} else {
			lotResult.Status = StatusIdempotent
			result.Submitted++
		}
		result.Lots = append(result.Lots, lotResult)
	}
	return result, errors.Join(runErrors...)
}

func baseLotResult(trade domain.PositionExitTrade, scheduledAt time.Time) LotResult {
	return LotResult{
		ExecutionAccountID: trade.ExecutionAccountID, LotID: trade.LotID, TokenID: trade.TokenID,
		HeldSeconds: int64(scheduledAt.Sub(trade.EnteredAt.UTC()) / time.Second),
	}
}

func tradableMarketMatches(market domain.MarketSnapshot, trade domain.PositionExitTrade) bool {
	if market.MarketID != trade.MarketID || market.ConditionID != trade.ConditionID || market.NegRisk != trade.NegRisk ||
		market.Resolved || market.Closed || !market.Active || market.Paused || !market.AcceptingOrders {
		return false
	}
	for _, outcome := range market.Outcomes {
		if outcome.Index == trade.OutcomeIndex && outcome.TokenID == trade.TokenID && strings.EqualFold(outcome.Name, trade.OutcomeName) {
			return true
		}
	}
	return false
}

func usableBookMatches(book domain.OrderBookSnapshot, trade domain.PositionExitTrade) bool {
	if book.MarketID != trade.MarketID || book.ConditionID != trade.ConditionID || book.OutcomeIndex != trade.OutcomeIndex ||
		book.TokenID != trade.TokenID || book.Status != domain.OrderBookStatusOK || len(book.Bids) == 0 || book.BestBid.IsEmpty() {
		return false
	}
	return book.Validate() == nil
}

// sellSize takes only liquidity at the exact best bid. A FOK order therefore
// either exits the selected chunk immediately or changes nothing; later polls
// can continue with the remainder at a fresh price.
func (service *Service) sellSize(available domain.Decimal, book domain.OrderBookSnapshot) (domain.Decimal, error) {
	liquidity := new(big.Rat)
	for _, level := range book.Bids {
		if !level.Price.Equal(book.BestBid) {
			break
		}
		value, err := level.Size.Multiply("1")
		if err != nil {
			return "", err
		}
		liquidity.Add(liquidity, value)
	}
	if liquidity.Sign() <= 0 {
		return "0", nil
	}
	limit, err := minimumDecimal(available, domain.Decimal(liquidity.FloatString(12)))
	if err != nil {
		return "", err
	}
	places := 4 - decimalPlaces(book.BestBid)
	if places > 2 {
		places = 2
	}
	if places < 0 {
		return "", fmt.Errorf("best bid exceeds supported FOK precision")
	}
	return floorDecimal(limit, places)
}

func (service *Service) buildIntent(
	trade domain.PositionExitTrade,
	book domain.OrderBookSnapshot,
	scheduledAt time.Time,
	size domain.Decimal,
) (domain.OrderIntent, error) {
	cycle := "time-exit:" + trade.ExecutionAccountID + ":" + scheduledAt.Format("20060102T150405Z")
	digest := sha256.Sum256([]byte(cycle + "\x00" + trade.LotID))
	signalID := cycle + ":" + trade.LotID
	outcomeIndex, negRisk := trade.OutcomeIndex, trade.NegRisk
	marketSnapshotAt, signalAt := book.SourceAt.UTC(), scheduledAt.UTC()
	return (domain.OrderIntentParams{
		ModelID: trade.ModelID, StrategyID: domain.CanonicalStrategyID(trade.StrategyID),
		ExecutionAccountID: trade.ExecutionAccountID,
		SignalID:           signalID, ClientOrderID: "time-exit-order-" + hex.EncodeToString(digest[:16]),
		Venue: service.venue, MarketID: trade.MarketID, ConditionID: trade.ConditionID,
		OutcomeIndex: &outcomeIndex, OutcomeName: trade.OutcomeName, TokenID: trade.TokenID,
		TargetLotID: trade.LotID, ExpectedNegRisk: &negRisk,
		MarketSnapshotAt: &marketSnapshotAt, SignalAt: &signalAt,
		Side: domain.SideSell, Type: domain.OrderTypeLimit,
		Price: book.BestBid, WorstPrice: book.BestBid, Size: size,
		TimeInForce: domain.TimeInForceFOK,
		Metadata: map[string]string{
			"time_exit_reason":         "HOLD_DURATION_48H",
			"time_exit_scheduled_at":   scheduledAt.Format(time.RFC3339Nano),
			"time_exit_entered_at":     trade.EnteredAt.UTC().Format(time.RFC3339Nano),
			"time_exit_eligible_at":    trade.EnteredAt.UTC().Add(HoldDuration).Format(time.RFC3339Nano),
			"time_exit_held_seconds":   strconv.FormatInt(int64(scheduledAt.Sub(trade.EnteredAt.UTC())/time.Second), 10),
			"strategy_reference_price": book.BestBid.String(),
			"target_lot_id":            trade.LotID,
		},
	}).Build()
}

func sortedTargets(values map[string]domain.BookTarget) []domain.BookTarget {
	result := make([]domain.BookTarget, 0, len(values))
	for _, target := range values {
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TokenID < result[j].TokenID })
	return result
}

func minimumDecimal(values ...domain.Decimal) (domain.Decimal, error) {
	if len(values) == 0 {
		return "", fmt.Errorf("at least one decimal is required")
	}
	minimum := values[0]
	for _, value := range values[1:] {
		comparison, err := value.Compare(minimum)
		if err != nil {
			return "", err
		}
		if comparison < 0 {
			minimum = value
		}
	}
	return minimum, nil
}

func floorDecimal(value domain.Decimal, places int) (domain.Decimal, error) {
	if places < 0 {
		return "", fmt.Errorf("decimal places must be non-negative")
	}
	rat, err := value.Multiply("1")
	if err != nil {
		return "", err
	}
	if rat.Sign() < 0 {
		return "", fmt.Errorf("decimal value must be non-negative")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(places)), nil)
	scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(scale))
	integer := new(big.Int).Quo(scaled.Num(), scaled.Denom())
	return domain.Decimal(new(big.Rat).SetFrac(integer, scale).FloatString(places)), nil
}

func decimalPlaces(value domain.Decimal) int {
	text := strings.TrimSpace(value.String())
	if point := strings.IndexByte(text, '.'); point >= 0 {
		return len(strings.TrimRight(text[point+1:], "0"))
	}
	return 0
}

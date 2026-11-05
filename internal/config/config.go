package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/service/chaincash"
	"github.com/UniPat-AI/trading_execution/internal/service/orderrecovery"
	"github.com/UniPat-AI/trading_execution/internal/service/pipelinemonitor"
)

// Live ceilings for the local CLOB request token bucket. They follow Polymarket's
// per-signer order limit for the Standard tier (40 tokens/s, burst 60): the
// bucket is shared by every request kind, so a typo above these values would
// silently stop the local limiter from protecting the venue limit. The venue
// may change tiers; adjust here together with docs/wallet-connection.md.
const (
	maxLivePolymarketRequestsPerSecond = 40
	maxLivePolymarketBurst             = 60
)

// Config 表示后端使用的 Config 类型。
type Config struct {
	App                  App
	HTTP                 HTTP
	Database             Database
	Execution            Execution
	Polymarket           Polymarket
	Kalshi               Kalshi
	LiveOperations       LiveOperations
	DecisionCycle        DecisionCycle
	PipelineMonitor      pipelinemonitor.Config
	PipelineMonitorError string
}

// App 表示后端使用的 App 类型。
type App struct {
	Name     string
	Env      string
	LogLevel slog.Level
}

// HTTP 表示后端使用的 HTTP 类型。
type HTTP struct {
	Address           string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	APIToken          string
	JobToken          string
	ReadOnlyToken     string
}

// LiveOperations 表示实盘只读快照后台刷新与过期策略。
type LiveOperations struct {
	Interval       time.Duration
	RefreshTimeout time.Duration
	MaxSnapshotAge time.Duration
	EventLimit     int
}

// DecisionCycle configures the pull-based prediction -> algorithm -> execution
// workflow. OrderSubmissionEnabled is deliberately independent from Enabled so
// production can run the complete decision path without creating orders.
type DecisionCycle struct {
	Enabled                bool
	OrderSubmissionEnabled bool
	// EntrySubmissionDisabled is an operator-owned sell-only gate for every
	// account. It blocks BUY intents without blocking validated exits from
	// managed position lots. Per-account sell-only is DecisionAccount.Mode.
	EntrySubmissionDisabled bool
	// PredictionModelID is the one prediction model every account trades on.
	// It selects the PIT snapshot rows and is also the model_id sent to the
	// strategy and written on orders and lots. There is no separate internal
	// model name: lots belong to their account, so changing the model never
	// strands an open position.
	PredictionModelID string
	// PredictionSourceMode fixes the trusted result channel of that model.
	PredictionSourceMode domain.PredictionSourceMode
	// Accounts lists every execution account the decision cycle owns, one row
	// per account. An account trades exactly one venue.
	Accounts                     []DecisionAccount
	RequireCompleteModelCoverage bool
	PredictionInfraBaseURL       string
	PredictionInfraToken         string
	StrategyBaseURL              string
	StrategyToken                string
	Interval                     time.Duration
	StartupDelay                 time.Duration
	MaxStartLateness             time.Duration
	Timeout                      time.Duration
	// StrategyTimeout bounds one account's strategy call
	// (DECISION_CYCLE_STRATEGY_TIMEOUT, default 90s); it must be shorter
	// than Timeout. A timeout fails only that account.
	StrategyTimeout time.Duration
	// PredictionTimeout bounds the prediction snapshot request
	// (DECISION_CYCLE_PREDICTION_TIMEOUT, default 30s); it must be shorter
	// than Timeout. A timeout makes the cycle exit-only.
	PredictionTimeout  time.Duration
	PredictionLookback time.Duration
	// DeliveryConcurrency is how many intents of one wallet's batch are submitted
	// at once. 1 (the default) is strictly sequential. Asset reservation stays
	// in strategy order at every value.
	DeliveryConcurrency int
	// OrderReportStrategyIDs lists the strategies whose order outcomes are
	// reported back to the strategy service (POST /api/v1/strategy-order-reports).
	// Empty (the default) disables reporting entirely.
	OrderReportStrategyIDs []string
	// OrderReportLookback bounds how far back each report pass re-reads orders.
	OrderReportLookback time.Duration
}

// Database 表示后端使用的 Database 类型。
type Database struct {
	URL            string
	ConnectTimeout time.Duration
}

// Execution 表示后端使用的 Execution 类型。
type Execution struct {
	Mode                 string
	Venue                string
	AllowMarketOrders    bool
	CoordinatorInterval  time.Duration
	CoordinatorBatchSize int
	// Order recovery isolation: one order whose venue outcome is uncertain is
	// serialized through a durable lease, bounded by a per-order timeout,
	// retried with backoff, and escalated to the manual queue after the
	// escalation window. Its reservation is never released automatically.
	OrderRecoveryTimeout       time.Duration
	OrderRecoveryLeaseTTL      time.Duration
	OrderRecoveryRetryBackoff  time.Duration
	OrderRecoveryMaxBackoff    time.Duration
	OrderRecoveryEscalateAfter time.Duration
	OrderRecoveryPendingGrace  time.Duration
}

// OrderRecoveryPolicy 返回订单恢复守卫使用的租约、超时、退避和升级策略。
func (execution Execution) OrderRecoveryPolicy() orderrecovery.Policy {
	return orderrecovery.Policy{
		LeaseTTL:      execution.OrderRecoveryLeaseTTL,
		Timeout:       execution.OrderRecoveryTimeout,
		BaseBackoff:   execution.OrderRecoveryRetryBackoff,
		MaxBackoff:    execution.OrderRecoveryMaxBackoff,
		EscalateAfter: execution.OrderRecoveryEscalateAfter,
	}
}

// Polymarket 保存 fail-closed 实盘装配配置，钱包秘密只留在受限文件中。
type Polymarket struct {
	LiveTradingEnabled       bool
	AutoRedeemEnabled        bool
	AccountsFile             string
	CLOBURL                  string
	RelayerURL               string
	GeoblockURL              string
	FrontendOnlyAPICountries []string
	GammaURL                 string
	DataAPIURL               string
	PolygonRPCURL            string
	RequestTimeout           time.Duration
	StartupTimeout           time.Duration
	MaxClockSkew             time.Duration
	HeartbeatInterval        time.Duration
	HeartbeatCallTimeout     time.Duration
	HeartbeatStaleAfter      time.Duration
	ReconciliationInterval   time.Duration
	// ReconciliationScheduleOffset pins scheduled reconciliation to wall-clock
	// instants congruent to the offset modulo ReconciliationInterval. Nil keeps
	// the legacy start-relative ticker.
	ReconciliationScheduleOffset *time.Duration
	ReconciliationLookback       time.Duration
	PositionEpsilon              domain.Decimal
	BalanceEpsilon               domain.Decimal
	CancelFillFinalityGrace      time.Duration
	MaxReconcileAttempts         int
	MaxBuyFeeRateBPS             domain.Decimal
	LatestBookMaxAge             time.Duration
	FeeScheduleTTL               time.Duration
	OrderFilledConfirmations     int
	FillFinalityMaxAge           time.Duration
	AutoRedeemInterval           time.Duration
	AutoRedeemRetryInterval      time.Duration
	AutoRedeemAmbiguityTimeout   time.Duration
	AutoRedeemBatchSize          int
	AutoRedeemMaxSubmitAttempts  int
	// RequestsPerSecond and Burst size the process-wide CLOB request token
	// bucket shared by orders, cancels, fill reads, heartbeats and
	// reconciliation. Zero means unset: the adapter default (8/s, burst 4)
	// applies, so the default lives in exactly one place.
	RequestsPerSecond int
	Burst             int
	// ChainCash* configure the on-chain pUSD cash ledger. It is active only for
	// accounts initialized with cmd/chaincashinit; address lists are lowercase.
	ChainCashRewardSenders   []string
	ChainCashDepositSources  map[string][]string
	ChainCashMaxBlocksPerRun int
	ChainCashLogChunkBlocks  int
}

// Kalshi keeps authenticated market-data credentials outside the repository.
// Order submission is intentionally not controlled by these read-path fields.
type Kalshi struct {
	MarketDataEnabled bool
	APIURL            string
	APIKeyID          string
	PrivateKeyPath    string
	RequestTimeout    time.Duration
	// LiveAccounts gives each Kalshi execution account its own credential.
	LiveAccounts []KalshiLiveAccount
	// SettlementEnabled applies Kalshi's own settlement records to the ledger
	// of each tradable Kalshi route.
	SettlementEnabled  bool
	SettlementInterval time.Duration
	// BalanceDriftThreshold is the largest tolerated gap, in dollars, between
	// the local cash ledger and the exchange balance.
	BalanceDriftThreshold domain.Decimal
}

// KalshiLiveAccount gives one Kalshi execution account (a DECISION_CYCLE
// account row with market_source KALSHI) its isolated credential.
// PrivateKeyPath points at a root-provisioned 0600 secret file; private key
// material is never accepted inline.
type KalshiLiveAccount struct {
	ExecutionAccountID string `json:"execution_account_id"`
	APIKeyID           string `json:"api_key_id"`
	PrivateKeyPath     string `json:"private_key_path"`
	// OfficialUserID enables the optional official account PnL read path.
	// It is not an API key id and never changes the execution identity.
	OfficialUserID string `json:"official_user_id,omitempty"`
}

// DecisionAccountMode is the operator state of one execution account.
type DecisionAccountMode string

const (
	// DecisionAccountActive sends the account to the strategy and lets it buy
	// and sell.
	DecisionAccountActive DecisionAccountMode = "ACTIVE"
	// DecisionAccountSellOnly still sends the account to the strategy, but only
	// its exits are executed; every BUY is blocked.
	DecisionAccountSellOnly DecisionAccountMode = "SELL_ONLY"
	// DecisionAccountPaused stops new strategy requests and every BUY. The
	// account stays reconciled and its already persisted intents still deliver.
	DecisionAccountPaused DecisionAccountMode = "PAUSED"
)

// DecisionAccount is one row of DECISION_CYCLE_ACCOUNTS_JSON: which venue the
// account trades, which strategy runs it, and its operator mode.
type DecisionAccount struct {
	ExecutionAccountID string              `json:"execution_account_id"`
	MarketSource       domain.MarketSource `json:"market_source"`
	StrategyID         string              `json:"strategy_id"`
	Mode               DecisionAccountMode `json:"mode"`
}

// legacyDecisionVariables were replaced by DECISION_CYCLE_PREDICTION_MODEL_ID,
// DECISION_CYCLE_ACCOUNTS_JSON and KALSHI_LIVE_ACCOUNTS_JSON. Any of them in
// the environment, even empty, stops startup so a half-converted environment
// can never run with a silently different account topology.
var legacyDecisionVariables = []string{
	"DECISION_CYCLE_BINDINGS_JSON",
	"DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON",
	"DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON",
	"DECISION_CYCLE_ENTRY_DISABLED_ACCOUNTS_JSON",
	"DECISION_CYCLE_STRATEGY_DISABLED_ACCOUNTS_JSON",
	"DECISION_CYCLE_KALSHI_STRATEGY_INPUT_ENABLED",
	"KALSHI_LIVE_BINDINGS_JSON",
}

// Bindings returns one strategy binding per configured account. Prediction
// and strategy model are the same configured model.
func (cycle DecisionCycle) Bindings() []domain.StrategyExecutionBinding {
	bindings := make([]domain.StrategyExecutionBinding, 0, len(cycle.Accounts))
	modelID := strings.TrimSpace(cycle.PredictionModelID)
	for _, account := range cycle.Accounts {
		bindings = append(bindings, domain.StrategyExecutionBinding{
			PredictionModelID: modelID, ModelID: modelID,
			StrategyID: account.StrategyID, ExecutionAccountID: account.ExecutionAccountID,
			MarketSource: account.MarketSource,
		}.Normalize())
	}
	return bindings
}

// PredictionSourceModes returns the source-mode map of the configured model.
func (cycle DecisionCycle) PredictionSourceModes() map[string]domain.PredictionSourceMode {
	return map[string]domain.PredictionSourceMode{
		strings.TrimSpace(cycle.PredictionModelID): cycle.PredictionSourceMode,
	}
}

// EntryDisabledAccounts returns the accounts that may not buy: SELL_ONLY and
// PAUSED.
func (cycle DecisionCycle) EntryDisabledAccounts() []string {
	return cycle.accountsInModes(DecisionAccountSellOnly, DecisionAccountPaused)
}

// StrategyDisabledAccounts returns the accounts that are not sent to the
// strategy: PAUSED.
func (cycle DecisionCycle) StrategyDisabledAccounts() []string {
	return cycle.accountsInModes(DecisionAccountPaused)
}

// AccountsForSource returns the configured accounts of one venue, in
// configuration order.
func (cycle DecisionCycle) AccountsForSource(source domain.MarketSource) []DecisionAccount {
	result := make([]DecisionAccount, 0, len(cycle.Accounts))
	for _, account := range cycle.Accounts {
		if account.MarketSource.Normalize() == source.Normalize() {
			result = append(result, account)
		}
	}
	return result
}

func (cycle DecisionCycle) accountsInModes(modes ...DecisionAccountMode) []string {
	result := make([]string, 0)
	for _, account := range cycle.Accounts {
		for _, mode := range modes {
			if account.Mode == mode {
				result = append(result, account.ExecutionAccountID)
				break
			}
		}
	}
	return result
}

// Load 从环境变量加载并校验服务配置。
func Load() (Config, error) {
	if err := rejectLegacyDecisionVariables(); err != nil {
		return Config{}, err
	}
	executionMode := strings.ToLower(env("EXECUTION_MODE", "paper"))
	logLevel, err := parseLogLevel(env("LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}
	readHeaderTimeout, err := duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	readTimeout, err := duration("HTTP_READ_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	// Live placement can perform several bounded upstream checks before CLOB
	// acknowledgement. A short write deadline makes the client observe a
	// timeout even though the idempotent order state machine continues safely.
	writeTimeout, err := duration("HTTP_WRITE_TIMEOUT", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	idleTimeout, err := duration("HTTP_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	shutdownTimeout, err := duration("HTTP_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	databaseConnectTimeout, err := duration("DATABASE_CONNECT_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	allowMarketOrders, err := boolean("EXECUTION_ALLOW_MARKET_ORDERS", false)
	if err != nil {
		return Config{}, err
	}
	coordinatorInterval, err := duration("ORDER_COORDINATOR_INTERVAL", 2*time.Second)
	if err != nil {
		return Config{}, err
	}
	coordinatorBatchSize, err := integer("ORDER_COORDINATOR_BATCH_SIZE", 100, 1, 1000)
	if err != nil {
		return Config{}, err
	}
	orderRecoveryTimeout, err := duration("ORDER_RECOVERY_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	orderRecoveryLeaseTTL, err := duration("ORDER_RECOVERY_LEASE_TTL", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	orderRecoveryRetryBackoff, err := duration("ORDER_RECOVERY_RETRY_BACKOFF", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	orderRecoveryMaxBackoff, err := duration("ORDER_RECOVERY_MAX_BACKOFF", 10*time.Minute)
	if err != nil {
		return Config{}, err
	}
	orderRecoveryEscalateAfter, err := duration("ORDER_RECOVERY_ESCALATE_AFTER", 30*time.Minute)
	if err != nil {
		return Config{}, err
	}
	orderRecoveryPendingGrace, err := duration("ORDER_RECOVERY_PENDING_GRACE", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	liveTradingEnabled, err := boolean("POLYMARKET_LIVE_TRADING_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	if executionMode == "live" && liveTradingEnabled {
		for _, key := range []string{
			"POLYMARKET_MAX_BUY_FEE_RATE_BPS",
			"POLYGON_ORDER_FILLED_CONFIRMATIONS",
		} {
			if strings.TrimSpace(os.Getenv(key)) == "" {
				return Config{}, fmt.Errorf("%s must be explicitly configured in live mode", key)
			}
		}
	}
	autoRedeemEnabled, err := boolean("POLYMARKET_AUTO_REDEEM_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	polymarketRequestTimeout, err := duration("POLYMARKET_REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	kalshiMarketDataEnabled, err := boolean("KALSHI_MARKET_DATA_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	kalshiRequestTimeout, err := duration("KALSHI_REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	kalshiLiveAccounts, err := decodeKalshiLiveAccounts(os.Getenv("KALSHI_LIVE_ACCOUNTS_JSON"))
	if err != nil {
		return Config{}, err
	}
	kalshiSettlementEnabled, err := boolean("KALSHI_SETTLEMENT_ENABLED", true)
	if err != nil {
		return Config{}, err
	}
	kalshiSettlementInterval, err := duration("KALSHI_SETTLEMENT_INTERVAL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	kalshiBalanceDriftThreshold, err := decimal("KALSHI_BALANCE_DRIFT_THRESHOLD", "0.01")
	if err != nil {
		return Config{}, err
	}
	startupTimeout, err := duration("POLYMARKET_STARTUP_TIMEOUT", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	maxClockSkew, err := duration("POLYMARKET_MAX_CLOCK_SKEW", 3*time.Second)
	if err != nil {
		return Config{}, err
	}
	heartbeatInterval, err := duration("POLYMARKET_HEARTBEAT_INTERVAL", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	heartbeatCallTimeout, err := duration("POLYMARKET_HEARTBEAT_CALL_TIMEOUT", 3*time.Second)
	if err != nil {
		return Config{}, err
	}
	heartbeatStaleAfter, err := duration("POLYMARKET_HEARTBEAT_STALE_AFTER", 9*time.Second)
	if err != nil {
		return Config{}, err
	}
	reconciliationInterval, err := duration("RECONCILIATION_INTERVAL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	reconciliationScheduleOffset, err := reconciliationScheduleOffset(reconciliationInterval)
	if err != nil {
		return Config{}, err
	}
	reconciliationLookback, err := duration("RECONCILIATION_TRADE_LOOKBACK", 48*time.Hour)
	if err != nil {
		return Config{}, err
	}
	positionEpsilon, err := nonNegativeDecimal("RECONCILIATION_POSITION_EPSILON", "0.000001")
	if err != nil {
		return Config{}, err
	}
	balanceEpsilon, err := nonNegativeDecimal("RECONCILIATION_BALANCE_EPSILON", "0.000001")
	if err != nil {
		return Config{}, err
	}
	cancelFillFinalityGrace, err := duration("CANCEL_FILL_FINALITY_GRACE", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	maxReconcileAttempts, err := integer("MAX_ORDER_RECONCILE_ATTEMPTS", 5, 1, 100)
	if err != nil {
		return Config{}, err
	}
	maxBuyFeeRateBPS, err := nonNegativeDecimal("POLYMARKET_MAX_BUY_FEE_RATE_BPS", "10000")
	if err != nil {
		return Config{}, err
	}
	latestBookMaxAge, err := duration("POLYMARKET_LATEST_BOOK_MAX_AGE", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	requestsPerSecond, err := integer("POLYMARKET_REQUESTS_PER_SECOND", 0, 1, 1000)
	if err != nil {
		return Config{}, err
	}
	polymarketBurst, err := integer("POLYMARKET_BURST", 0, 1, 1000)
	if err != nil {
		return Config{}, err
	}
	feeScheduleTTL, err := duration("POLYMARKET_FEE_SCHEDULE_TTL", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}
	orderFilledConfirmations, err := integer("POLYGON_ORDER_FILLED_CONFIRMATIONS", 64, 1, 10_000)
	if err != nil {
		return Config{}, err
	}
	fillFinalityMaxAge, err := duration("POLYGON_FILL_FINALITY_MAX_AGE", 30*time.Minute)
	if err != nil {
		return Config{}, err
	}
	autoRedeemInterval, err := duration("POLYMARKET_AUTO_REDEEM_INTERVAL", time.Minute)
	if err != nil {
		return Config{}, err
	}
	autoRedeemRetryInterval, err := duration("POLYMARKET_AUTO_REDEEM_RETRY_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	autoRedeemAmbiguityTimeout, err := duration("POLYMARKET_AUTO_REDEEM_AMBIGUITY_TIMEOUT", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}
	autoRedeemBatchSize, err := integer("POLYMARKET_AUTO_REDEEM_BATCH_SIZE", 20, 1, 100)
	if err != nil {
		return Config{}, err
	}
	autoRedeemMaxSubmitAttempts, err := integer("POLYMARKET_AUTO_REDEEM_MAX_SUBMIT_ATTEMPTS", 3, 1, 10)
	if err != nil {
		return Config{}, err
	}
	chainCashRewardSenders, err := chaincash.ParseAddressList(os.Getenv("CHAIN_CASH_REWARD_SENDERS"))
	if err != nil {
		return Config{}, fmt.Errorf("CHAIN_CASH_REWARD_SENDERS: %w", err)
	}
	chainCashDepositSources, err := chaincash.ParseDepositSources(os.Getenv("CHAIN_CASH_DEPOSIT_SOURCES"))
	if err != nil {
		return Config{}, fmt.Errorf("CHAIN_CASH_DEPOSIT_SOURCES: %w", err)
	}
	// 1500 blocks = 15 chunks x 4 eth_getLogs calls (Transfer IN/OUT, OrderFilled
	// maker/taker) = 60 calls per catch-up run, about 12 seconds on the free
	// dRPC plan, while still catching up ten times faster than Polygon grows
	// (about 150 blocks per 5-minute reconciliation interval).
	chainCashMaxBlocksPerRun, err := integer("CHAIN_CASH_MAX_BLOCKS_PER_RUN", 1500, 1, 3000)
	if err != nil {
		return Config{}, err
	}
	// The production dRPC plan rejects eth_getLogs spans above 101 blocks.
	chainCashLogChunkBlocks, err := integer("CHAIN_CASH_LOG_CHUNK_BLOCKS", 100, 1, 100)
	if err != nil {
		return Config{}, err
	}
	// A catch-up run must stay well inside the per-account reconciliation
	// timeout: at most 30 chunks (120 eth_getLogs calls, about 24 seconds).
	// A run that cannot finish would time out before persisting anything and
	// keep the account blocked on every later run.
	if chunks := (chainCashMaxBlocksPerRun + chainCashLogChunkBlocks - 1) / chainCashLogChunkBlocks; chunks > 30 {
		return Config{}, fmt.Errorf(
			"CHAIN_CASH_MAX_BLOCKS_PER_RUN=%d with CHAIN_CASH_LOG_CHUNK_BLOCKS=%d needs %d log chunks per run; at most 30 are allowed",
			chainCashMaxBlocksPerRun, chainCashLogChunkBlocks, chunks)
	}
	liveOperationsInterval, err := duration("LIVE_OPERATIONS_INTERVAL", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	liveOperationsRefreshTimeout, err := duration("LIVE_OPERATIONS_REFRESH_TIMEOUT", 8*time.Second)
	if err != nil {
		return Config{}, err
	}
	liveOperationsMaxAge, err := duration("LIVE_OPERATIONS_MAX_SNAPSHOT_AGE", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	liveOperationsEventLimit, err := integer("LIVE_OPERATIONS_EVENT_LIMIT", 50, 1, 200)
	if err != nil {
		return Config{}, err
	}
	decisionCycle, err := loadDecisionCycle()
	if err != nil {
		return Config{}, err
	}
	pipelineMonitor, err := pipelinemonitor.LoadConfig(os.Getenv)
	pipelineMonitorError := ""
	if err != nil {
		pipelineMonitorError = err.Error()
		pipelineMonitor, _ = pipelinemonitor.LoadConfig(func(string) string { return "" })
	}
	config := Config{
		App: App{
			Name:     env("APP_NAME", "trading_execution"),
			Env:      strings.ToLower(env("APP_ENV", "local")),
			LogLevel: logLevel,
		},
		HTTP: HTTP{
			Address:           env("HTTP_ADDRESS", ":8090"),
			ReadHeaderTimeout: readHeaderTimeout,
			ReadTimeout:       readTimeout,
			WriteTimeout:      writeTimeout,
			IdleTimeout:       idleTimeout,
			ShutdownTimeout:   shutdownTimeout,
			APIToken:          strings.TrimSpace(os.Getenv("EXECUTION_API_TOKEN")),
			JobToken:          strings.TrimSpace(os.Getenv("POSITION_EXIT_JOB_TOKEN")),
			ReadOnlyToken:     strings.TrimSpace(os.Getenv("LIVE_OPERATIONS_READ_ONLY_TOKEN")),
		},
		Database: Database{
			URL:            strings.TrimSpace(os.Getenv("TRADING_EXECUTION_DATABASE_URL")),
			ConnectTimeout: databaseConnectTimeout,
		},
		Execution: Execution{
			Mode:                 executionMode,
			Venue:                strings.ToLower(env("EXECUTION_VENUE", "polymarket-paper")),
			AllowMarketOrders:    allowMarketOrders,
			CoordinatorInterval:  coordinatorInterval,
			CoordinatorBatchSize: coordinatorBatchSize,

			OrderRecoveryTimeout:       orderRecoveryTimeout,
			OrderRecoveryLeaseTTL:      orderRecoveryLeaseTTL,
			OrderRecoveryRetryBackoff:  orderRecoveryRetryBackoff,
			OrderRecoveryMaxBackoff:    orderRecoveryMaxBackoff,
			OrderRecoveryEscalateAfter: orderRecoveryEscalateAfter,
			OrderRecoveryPendingGrace:  orderRecoveryPendingGrace,
		},
		Polymarket: Polymarket{
			LiveTradingEnabled: liveTradingEnabled,
			AutoRedeemEnabled:  autoRedeemEnabled,
			AccountsFile:       strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")),
			CLOBURL:            env("POLYMARKET_CLOB_URL", "https://clob.polymarket.com"),
			RelayerURL:         env("POLYMARKET_RELAYER_URL", "https://relayer-v2.polymarket.com"),
			GeoblockURL:        env("POLYMARKET_GEOBLOCK_URL", "https://polymarket.com/api/geoblock"),
			// Empty by default so a deployment cannot silently keep an old policy
			// exception. Production must opt in after checking the current official
			// geographic-restrictions page.
			FrontendOnlyAPICountries:     commaSeparated(os.Getenv("POLYMARKET_FRONTEND_ONLY_API_COUNTRIES")),
			GammaURL:                     env("POLYMARKET_GAMMA_URL", "https://gamma-api.polymarket.com"),
			DataAPIURL:                   env("POLYMARKET_DATA_API_URL", "https://data-api.polymarket.com"),
			PolygonRPCURL:                strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")),
			RequestTimeout:               polymarketRequestTimeout,
			StartupTimeout:               startupTimeout,
			MaxClockSkew:                 maxClockSkew,
			HeartbeatInterval:            heartbeatInterval,
			HeartbeatCallTimeout:         heartbeatCallTimeout,
			HeartbeatStaleAfter:          heartbeatStaleAfter,
			ReconciliationInterval:       reconciliationInterval,
			ReconciliationScheduleOffset: reconciliationScheduleOffset,
			ReconciliationLookback:       reconciliationLookback,
			PositionEpsilon:              positionEpsilon,
			BalanceEpsilon:               balanceEpsilon,
			CancelFillFinalityGrace:      cancelFillFinalityGrace,
			MaxReconcileAttempts:         maxReconcileAttempts,
			MaxBuyFeeRateBPS:             maxBuyFeeRateBPS,
			LatestBookMaxAge:             latestBookMaxAge,
			FeeScheduleTTL:               feeScheduleTTL,
			RequestsPerSecond:            requestsPerSecond,
			Burst:                        polymarketBurst,
			OrderFilledConfirmations:     orderFilledConfirmations,
			FillFinalityMaxAge:           fillFinalityMaxAge,
			AutoRedeemInterval:           autoRedeemInterval,
			AutoRedeemRetryInterval:      autoRedeemRetryInterval,
			AutoRedeemAmbiguityTimeout:   autoRedeemAmbiguityTimeout,
			AutoRedeemBatchSize:          autoRedeemBatchSize,
			AutoRedeemMaxSubmitAttempts:  autoRedeemMaxSubmitAttempts,
			ChainCashRewardSenders:       chainCashRewardSenders,
			ChainCashDepositSources:      chainCashDepositSources,
			ChainCashMaxBlocksPerRun:     chainCashMaxBlocksPerRun,
			ChainCashLogChunkBlocks:      chainCashLogChunkBlocks,
		},
		Kalshi: Kalshi{
			MarketDataEnabled: kalshiMarketDataEnabled,
			APIURL:            env("KALSHI_API_URL", "https://external-api.kalshi.com"),
			APIKeyID:          strings.TrimSpace(os.Getenv("KALSHI_API_KEY_ID")),
			PrivateKeyPath:    strings.TrimSpace(os.Getenv("KALSHI_PRIVATE_KEY_PATH")),
			RequestTimeout:    kalshiRequestTimeout,
			LiveAccounts:      kalshiLiveAccounts,

			SettlementEnabled:     kalshiSettlementEnabled,
			SettlementInterval:    kalshiSettlementInterval,
			BalanceDriftThreshold: kalshiBalanceDriftThreshold,
		},
		LiveOperations: LiveOperations{
			Interval: liveOperationsInterval, RefreshTimeout: liveOperationsRefreshTimeout,
			MaxSnapshotAge: liveOperationsMaxAge, EventLimit: liveOperationsEventLimit,
		},
		DecisionCycle:        decisionCycle,
		PipelineMonitor:      pipelineMonitor,
		PipelineMonitorError: pipelineMonitorError,
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Validate 校验当前模型的字段完整性和业务约束。
func (config Config) Validate() error {
	if strings.TrimSpace(config.App.Name) == "" || strings.TrimSpace(config.HTTP.Address) == "" {
		return fmt.Errorf("app name and HTTP address are required")
	}
	if config.Execution.Mode != "paper" && config.Execution.Mode != "live" {
		return fmt.Errorf("execution mode must be paper or live")
	}
	if strings.TrimSpace(config.Execution.Venue) == "" {
		return fmt.Errorf("execution venue is required")
	}
	if config.App.Env != "local" && len(config.HTTP.APIToken) < 32 {
		return fmt.Errorf("EXECUTION_API_TOKEN must contain at least 32 bytes outside local environment")
	}
	if config.App.Env != "local" && strings.TrimSpace(config.Database.URL) == "" {
		return fmt.Errorf("TRADING_EXECUTION_DATABASE_URL is required outside local environment")
	}
	if config.Execution.CoordinatorInterval < 100*time.Millisecond || config.Execution.CoordinatorInterval > time.Minute {
		return fmt.Errorf("ORDER_COORDINATOR_INTERVAL must be between 100ms and 1m")
	}
	if _, err := config.Execution.OrderRecoveryPolicy().Normalize(); err != nil {
		return fmt.Errorf("ORDER_RECOVERY_* settings are invalid: %w", err)
	}
	if config.Execution.OrderRecoveryPendingGrace < 0 {
		return fmt.Errorf("ORDER_RECOVERY_PENDING_GRACE must not be negative")
	}
	if config.Execution.Mode == "live" {
		if !config.Polymarket.LiveTradingEnabled {
			return fmt.Errorf("POLYMARKET_LIVE_TRADING_ENABLED must be explicitly true for live mode")
		}
		if config.App.Env != "production" {
			return fmt.Errorf("live mode requires APP_ENV=production")
		}
		if config.Execution.Venue != "polymarket" {
			return fmt.Errorf("live mode requires EXECUTION_VENUE=polymarket")
		}
		if strings.TrimSpace(config.Polymarket.AccountsFile) == "" {
			return fmt.Errorf("POLYMARKET_ACCOUNTS_FILE is required in live mode")
		}
		if strings.TrimSpace(config.Polymarket.PolygonRPCURL) == "" {
			return fmt.Errorf("POLYGON_RPC_URL is required in live mode")
		}
		if len(config.HTTP.ReadOnlyToken) < 32 {
			return fmt.Errorf("LIVE_OPERATIONS_READ_ONLY_TOKEN must contain at least 32 bytes in live mode")
		}
		if len(config.HTTP.JobToken) < 32 {
			return fmt.Errorf("POSITION_EXIT_JOB_TOKEN must contain at least 32 bytes in live mode")
		}
		if config.HTTP.JobToken == config.HTTP.APIToken ||
			config.HTTP.JobToken == config.HTTP.ReadOnlyToken ||
			(config.DecisionCycle.PredictionInfraToken != "" && config.HTTP.JobToken == config.DecisionCycle.PredictionInfraToken) ||
			(config.DecisionCycle.StrategyToken != "" && config.HTTP.JobToken == config.DecisionCycle.StrategyToken) {
			return fmt.Errorf("POSITION_EXIT_JOB_TOKEN must be different from execution, read-only, and decision-cycle tokens")
		}
		if config.HTTP.ReadOnlyToken == config.HTTP.APIToken || (config.HTTP.JobToken != "" && config.HTTP.ReadOnlyToken == config.HTTP.JobToken) {
			return fmt.Errorf("LIVE_OPERATIONS_READ_ONLY_TOKEN must be different from execution and job tokens")
		}
		for name, value := range map[string]string{
			"POLYMARKET_CLOB_URL":     config.Polymarket.CLOBURL,
			"POLYMARKET_GEOBLOCK_URL": config.Polymarket.GeoblockURL,
			"POLYMARKET_GAMMA_URL":    config.Polymarket.GammaURL,
			"POLYMARKET_DATA_API_URL": config.Polymarket.DataAPIURL,
			"POLYGON_RPC_URL":         config.Polymarket.PolygonRPCURL,
			"POLYMARKET_RELAYER_URL":  config.Polymarket.RelayerURL,
		} {
			if !secureExternalURL(value) {
				return fmt.Errorf("%s must be an absolute HTTPS URL in live mode", name)
			}
		}
		if !canonicalPolymarketCLOBURL(config.Polymarket.CLOBURL) {
			return fmt.Errorf("POLYMARKET_CLOB_URL must be the canonical https://clob.polymarket.com origin in live mode")
		}
		if !loopbackAddress(config.HTTP.Address) {
			return fmt.Errorf("live HTTP_ADDRESS must bind to a loopback address")
		}
		if config.HTTP.WriteTimeout < 4*config.Polymarket.RequestTimeout {
			return fmt.Errorf("live HTTP_WRITE_TIMEOUT must be at least four times POLYMARKET_REQUEST_TIMEOUT")
		}
		if comparison, err := config.Polymarket.MaxBuyFeeRateBPS.Compare("10000"); err != nil || comparison > 0 {
			return fmt.Errorf("POLYMARKET_MAX_BUY_FEE_RATE_BPS must not exceed 10000")
		}
		if config.Polymarket.RequestsPerSecond > maxLivePolymarketRequestsPerSecond {
			return fmt.Errorf("POLYMARKET_REQUESTS_PER_SECOND must not exceed %d in live mode", maxLivePolymarketRequestsPerSecond)
		}
		if config.Polymarket.Burst > maxLivePolymarketBurst {
			return fmt.Errorf("POLYMARKET_BURST must not exceed %d in live mode", maxLivePolymarketBurst)
		}
		if config.LiveOperations.Interval < 5*time.Second || config.LiveOperations.Interval > 60*time.Second {
			return fmt.Errorf("LIVE_OPERATIONS_INTERVAL must be between 5s and 60s in live mode")
		}
		if config.LiveOperations.RefreshTimeout >= config.LiveOperations.Interval {
			return fmt.Errorf("LIVE_OPERATIONS_REFRESH_TIMEOUT must be less than LIVE_OPERATIONS_INTERVAL")
		}
		if config.LiveOperations.MaxSnapshotAge <= config.LiveOperations.Interval {
			return fmt.Errorf("LIVE_OPERATIONS_MAX_SNAPSHOT_AGE must be greater than LIVE_OPERATIONS_INTERVAL")
		}
	}
	if config.Polymarket.AutoRedeemEnabled && config.Execution.Mode != "live" {
		return fmt.Errorf("POLYMARKET_AUTO_REDEEM_ENABLED requires live execution mode")
	}
	if config.Polymarket.AutoRedeemEnabled {
		if config.Polymarket.AutoRedeemInterval < 10*time.Second || config.Polymarket.AutoRedeemInterval > 10*time.Minute {
			return fmt.Errorf("POLYMARKET_AUTO_REDEEM_INTERVAL must be between 10s and 10m")
		}
		if config.Polymarket.AutoRedeemRetryInterval < 5*time.Second || config.Polymarket.AutoRedeemRetryInterval > time.Hour {
			return fmt.Errorf("POLYMARKET_AUTO_REDEEM_RETRY_INTERVAL must be between 5s and 1h")
		}
		if config.Polymarket.AutoRedeemAmbiguityTimeout < time.Minute || config.Polymarket.AutoRedeemAmbiguityTimeout > 24*time.Hour {
			return fmt.Errorf("POLYMARKET_AUTO_REDEEM_AMBIGUITY_TIMEOUT must be between 1m and 24h")
		}
		if config.Polymarket.AutoRedeemBatchSize < 1 || config.Polymarket.AutoRedeemBatchSize > 100 {
			return fmt.Errorf("POLYMARKET_AUTO_REDEEM_BATCH_SIZE must be between 1 and 100")
		}
		if config.Polymarket.AutoRedeemMaxSubmitAttempts < 1 || config.Polymarket.AutoRedeemMaxSubmitAttempts > 10 {
			return fmt.Errorf("POLYMARKET_AUTO_REDEEM_MAX_SUBMIT_ATTEMPTS must be between 1 and 10")
		}
	}
	if config.DecisionCycle.OrderSubmissionEnabled && !config.DecisionCycle.Enabled {
		return fmt.Errorf("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED requires DECISION_CYCLE_ENABLED=true")
	}
	if config.Kalshi.MarketDataEnabled {
		if !secureExternalURL(config.Kalshi.APIURL) {
			return fmt.Errorf("KALSHI_API_URL must be an absolute HTTPS URL")
		}
		if strings.TrimSpace(config.Kalshi.APIKeyID) == "" || strings.TrimSpace(config.Kalshi.PrivateKeyPath) == "" {
			return fmt.Errorf("KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH are required when Kalshi market data is enabled")
		}
		if !filepath.IsAbs(config.Kalshi.PrivateKeyPath) {
			return fmt.Errorf("KALSHI_PRIVATE_KEY_PATH must be absolute")
		}
		if config.Kalshi.RequestTimeout <= 0 || config.Kalshi.RequestTimeout > time.Minute {
			return fmt.Errorf("KALSHI_REQUEST_TIMEOUT must be positive and at most 1m")
		}
	}
	if config.Kalshi.SettlementEnabled {
		if config.Kalshi.SettlementInterval < 30*time.Second || config.Kalshi.SettlementInterval > time.Hour {
			return fmt.Errorf("KALSHI_SETTLEMENT_INTERVAL must be between 30s and 1h")
		}
		if sign, err := config.Kalshi.BalanceDriftThreshold.Sign(); err != nil || sign <= 0 {
			return fmt.Errorf("KALSHI_BALANCE_DRIFT_THRESHOLD must be a positive dollar amount")
		}
	}
	if config.DecisionCycle.OrderSubmissionEnabled && !config.DecisionCycle.RequireCompleteModelCoverage {
		return fmt.Errorf("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED requires DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE=true")
	}
	if config.DecisionCycle.Enabled {
		if config.Execution.Mode != "live" {
			return fmt.Errorf("DECISION_CYCLE_ENABLED requires EXECUTION_MODE=live")
		}
		if config.DecisionCycle.Interval != 10*time.Minute {
			return fmt.Errorf("DECISION_CYCLE_INTERVAL must be exactly 10m")
		}
		if config.DecisionCycle.StartupDelay < 0 || config.DecisionCycle.StartupDelay >= config.DecisionCycle.Interval {
			return fmt.Errorf("DECISION_CYCLE_STARTUP_DELAY must be non-negative and less than the interval")
		}
		if config.DecisionCycle.MaxStartLateness <= 0 || config.DecisionCycle.MaxStartLateness >= config.DecisionCycle.Interval {
			return fmt.Errorf("DECISION_CYCLE_MAX_START_LATENESS must be positive and less than the interval")
		}
		if config.DecisionCycle.Timeout <= 0 || config.DecisionCycle.Timeout >= config.DecisionCycle.Interval {
			return fmt.Errorf("DECISION_CYCLE_TIMEOUT must be positive and less than DECISION_CYCLE_INTERVAL")
		}
		if config.DecisionCycle.StrategyTimeout <= 0 || config.DecisionCycle.StrategyTimeout >= config.DecisionCycle.Timeout {
			return fmt.Errorf("DECISION_CYCLE_STRATEGY_TIMEOUT must be positive and less than DECISION_CYCLE_TIMEOUT")
		}
		if config.DecisionCycle.PredictionTimeout <= 0 || config.DecisionCycle.PredictionTimeout >= config.DecisionCycle.Timeout {
			return fmt.Errorf("DECISION_CYCLE_PREDICTION_TIMEOUT must be positive and less than DECISION_CYCLE_TIMEOUT")
		}
		if config.DecisionCycle.PredictionLookback < 10*time.Minute || config.DecisionCycle.PredictionLookback > 24*time.Hour {
			return fmt.Errorf("DECISION_CYCLE_PREDICTION_LOOKBACK must be between 10m and 24h")
		}
		for name, value := range map[string]string{
			"DECISION_CYCLE_PREDICTION_INFRA_URL": config.DecisionCycle.PredictionInfraBaseURL,
			"DECISION_CYCLE_STRATEGY_URL":         config.DecisionCycle.StrategyBaseURL,
		} {
			if !secureServiceURL(value) {
				return fmt.Errorf("%s must be HTTPS or loopback HTTP without credentials", name)
			}
		}
		if len(config.DecisionCycle.PredictionInfraToken) < 32 || len(config.DecisionCycle.StrategyToken) < 32 {
			return fmt.Errorf("decision-cycle prediction and strategy tokens must each contain at least 32 bytes")
		}
		if config.DecisionCycle.PredictionInfraToken == config.DecisionCycle.StrategyToken ||
			config.DecisionCycle.PredictionInfraToken == config.HTTP.APIToken ||
			config.DecisionCycle.StrategyToken == config.HTTP.APIToken ||
			config.DecisionCycle.PredictionInfraToken == config.HTTP.ReadOnlyToken ||
			config.DecisionCycle.StrategyToken == config.HTTP.ReadOnlyToken ||
			(config.HTTP.JobToken != "" && (config.DecisionCycle.PredictionInfraToken == config.HTTP.JobToken ||
				config.DecisionCycle.StrategyToken == config.HTTP.JobToken)) {
			return fmt.Errorf("decision-cycle tokens must be distinct from each other and existing service tokens")
		}
	}
	// Live startup derives account authorization, the wallet-file account set
	// and the Kalshi routes from the account rows, so live mode needs them even
	// while the decision cycle itself is disabled.
	if config.Execution.Mode == "live" || config.DecisionCycle.Enabled ||
		len(config.DecisionCycle.Accounts) > 0 || len(config.Kalshi.LiveAccounts) > 0 {
		if err := validateDecisionAccounts(config.DecisionCycle, config.Kalshi.LiveAccounts); err != nil {
			return err
		}
	}
	return nil
}

func loadDecisionCycle() (DecisionCycle, error) {
	enabled, err := boolean("DECISION_CYCLE_ENABLED", false)
	if err != nil {
		return DecisionCycle{}, err
	}
	submitEnabled, err := boolean("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED", false)
	if err != nil {
		return DecisionCycle{}, err
	}
	entrySubmissionDisabled, err := boolean("DECISION_CYCLE_ENTRY_SUBMISSION_DISABLED", false)
	if err != nil {
		return DecisionCycle{}, err
	}
	requireCompleteModelCoverage, err := boolean("DECISION_CYCLE_REQUIRE_COMPLETE_MODEL_COVERAGE", false)
	if err != nil {
		return DecisionCycle{}, err
	}
	interval, err := duration("DECISION_CYCLE_INTERVAL", 10*time.Minute)
	if err != nil {
		return DecisionCycle{}, err
	}
	startupDelay, err := nonNegativeDuration("DECISION_CYCLE_STARTUP_DELAY", 15*time.Second)
	if err != nil {
		return DecisionCycle{}, err
	}
	maxStartLateness, err := duration("DECISION_CYCLE_MAX_START_LATENESS", 30*time.Second)
	if err != nil {
		return DecisionCycle{}, err
	}
	timeout, err := duration("DECISION_CYCLE_TIMEOUT", 8*time.Minute)
	if err != nil {
		return DecisionCycle{}, err
	}
	strategyTimeout, err := duration("DECISION_CYCLE_STRATEGY_TIMEOUT", 90*time.Second)
	if err != nil {
		return DecisionCycle{}, err
	}
	predictionTimeout, err := duration("DECISION_CYCLE_PREDICTION_TIMEOUT", 30*time.Second)
	if err != nil {
		return DecisionCycle{}, err
	}
	predictionLookback, err := duration("DECISION_CYCLE_PREDICTION_LOOKBACK", 3*time.Hour)
	if err != nil {
		return DecisionCycle{}, err
	}
	accounts, err := decodeDecisionAccounts(os.Getenv("DECISION_CYCLE_ACCOUNTS_JSON"))
	if err != nil {
		return DecisionCycle{}, err
	}
	predictionSourceMode := domain.PredictionSourceMode(
		strings.ToUpper(env("DECISION_CYCLE_PREDICTION_SOURCE_MODE", string(domain.PredictionSourceModeSandbox))),
	)
	deliveryConcurrency, err := integer("DECISION_CYCLE_DELIVERY_CONCURRENCY", 1, 1, 8)
	if err != nil {
		return DecisionCycle{}, err
	}
	orderReportStrategyIDs, err := decodeStringList(
		"DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON",
		os.Getenv("DECISION_CYCLE_ORDER_REPORT_STRATEGY_IDS_JSON"),
	)
	if err != nil {
		return DecisionCycle{}, err
	}
	orderReportLookback, err := duration("DECISION_CYCLE_ORDER_REPORT_LOOKBACK", 24*time.Hour)
	if err != nil {
		return DecisionCycle{}, err
	}
	return DecisionCycle{
		Enabled: enabled, OrderSubmissionEnabled: submitEnabled,
		EntrySubmissionDisabled:      entrySubmissionDisabled,
		PredictionModelID:            strings.TrimSpace(os.Getenv("DECISION_CYCLE_PREDICTION_MODEL_ID")),
		PredictionSourceMode:         predictionSourceMode,
		Accounts:                     accounts,
		RequireCompleteModelCoverage: requireCompleteModelCoverage,
		PredictionInfraBaseURL:       strings.TrimSpace(os.Getenv("DECISION_CYCLE_PREDICTION_INFRA_URL")),
		PredictionInfraToken:         strings.TrimSpace(os.Getenv("DECISION_CYCLE_PREDICTION_INFRA_TOKEN")),
		StrategyBaseURL:              strings.TrimSpace(os.Getenv("DECISION_CYCLE_STRATEGY_URL")),
		StrategyToken:                strings.TrimSpace(os.Getenv("DECISION_CYCLE_STRATEGY_TOKEN")),
		Interval:                     interval, StartupDelay: startupDelay,
		MaxStartLateness: maxStartLateness, Timeout: timeout,
		StrategyTimeout: strategyTimeout, PredictionTimeout: predictionTimeout,
		PredictionLookback:     predictionLookback,
		DeliveryConcurrency:    deliveryConcurrency,
		OrderReportStrategyIDs: orderReportStrategyIDs,
		OrderReportLookback:    orderReportLookback,
	}, nil
}

// rejectLegacyDecisionVariables stops startup while any replaced variable is
// still present in the environment.
func rejectLegacyDecisionVariables() error {
	present := make([]string, 0)
	for _, name := range legacyDecisionVariables {
		if _, exists := os.LookupEnv(name); exists {
			present = append(present, name)
		}
	}
	if len(present) > 0 {
		return fmt.Errorf(
			"replaced decision-cycle variables are still set: %s; configure DECISION_CYCLE_PREDICTION_MODEL_ID, "+
				"DECISION_CYCLE_ACCOUNTS_JSON and KALSHI_LIVE_ACCOUNTS_JSON instead and remove these lines",
			strings.Join(present, ", "),
		)
	}
	return nil
}

// decodeStringList parses one JSON string-array variable: an empty value
// means an empty list, otherwise exactly one JSON array of trimmed strings.
func decodeStringList(name, value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var items []string
	if err := decoder.Decode(&items); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%s must contain exactly one JSON array", name)
	}
	for index := range items {
		items[index] = strings.TrimSpace(items[index])
	}
	return items, nil
}

// decodeDecisionAccounts parses DECISION_CYCLE_ACCOUNTS_JSON. Empty means no
// accounts; otherwise it must be exactly one JSON array of account rows.
func decodeDecisionAccounts(value string) ([]DecisionAccount, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var accounts []DecisionAccount
	if err := decoder.Decode(&accounts); err != nil {
		return nil, fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON must contain exactly one JSON array")
	}
	for index := range accounts {
		account := &accounts[index]
		account.ExecutionAccountID = strings.TrimSpace(account.ExecutionAccountID)
		account.MarketSource = domain.MarketSource(strings.ToUpper(strings.TrimSpace(string(account.MarketSource))))
		account.StrategyID = domain.CanonicalStrategyID(account.StrategyID)
		account.Mode = DecisionAccountMode(strings.ToUpper(strings.TrimSpace(string(account.Mode))))
	}
	return accounts, nil
}

// decodeKalshiLiveAccounts parses KALSHI_LIVE_ACCOUNTS_JSON.
func decodeKalshiLiveAccounts(value string) ([]KalshiLiveAccount, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var accounts []KalshiLiveAccount
	if err := decoder.Decode(&accounts); err != nil {
		return nil, fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON must be a JSON array: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON must contain exactly one JSON array")
	}
	seenAccounts := make(map[string]struct{}, len(accounts))
	seenKeys := make(map[string]struct{}, len(accounts))
	for index := range accounts {
		account := &accounts[index]
		account.ExecutionAccountID = strings.TrimSpace(account.ExecutionAccountID)
		account.APIKeyID = strings.TrimSpace(account.APIKeyID)
		account.PrivateKeyPath = strings.TrimSpace(account.PrivateKeyPath)
		account.OfficialUserID = strings.TrimSpace(account.OfficialUserID)
		if account.ExecutionAccountID == "" || account.APIKeyID == "" || account.PrivateKeyPath == "" {
			return nil, fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON account %d is incomplete", index)
		}
		if !domain.IsKalshiExecutionAccount(account.ExecutionAccountID) {
			return nil, fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON account %q must start with %q",
				account.ExecutionAccountID, domain.KalshiExecutionAccountPrefix)
		}
		if !filepath.IsAbs(account.PrivateKeyPath) {
			return nil, fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON account %q private_key_path must be absolute", account.ExecutionAccountID)
		}
		if _, duplicate := seenAccounts[account.ExecutionAccountID]; duplicate {
			return nil, fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON repeats account %q", account.ExecutionAccountID)
		}
		seenAccounts[account.ExecutionAccountID] = struct{}{}
		if _, duplicate := seenKeys[account.APIKeyID]; duplicate {
			return nil, fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON reuses one API key across accounts")
		}
		seenKeys[account.APIKeyID] = struct{}{}
	}
	return accounts, nil
}

// validateDecisionAccounts checks the model and account rows. Every account is
// independent: there is no fixed account set, model whitelist or per-account
// strategy pin. A Kalshi account needs exactly one live credential.
func validateDecisionAccounts(cycle DecisionCycle, kalshiAccounts []KalshiLiveAccount) error {
	modelID := strings.TrimSpace(cycle.PredictionModelID)
	if modelID == "" || len(modelID) > 200 {
		return fmt.Errorf("DECISION_CYCLE_PREDICTION_MODEL_ID must contain 1-200 bytes")
	}
	if !cycle.PredictionSourceMode.Valid() {
		return fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODE must be exactly DIRECT or SANDBOX")
	}
	if len(cycle.Accounts) == 0 {
		return fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON must contain at least one account")
	}
	seen := make(map[string]struct{}, len(cycle.Accounts))
	kalshi := make(map[string]struct{})
	notPaused := 0
	for index, account := range cycle.Accounts {
		if account.ExecutionAccountID == "" {
			return fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON account %d execution_account_id is required", index)
		}
		if _, duplicate := seen[account.ExecutionAccountID]; duplicate {
			return fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON repeats account %q", account.ExecutionAccountID)
		}
		seen[account.ExecutionAccountID] = struct{}{}
		switch account.MarketSource {
		case domain.MarketSourcePolymarket, domain.MarketSourceKalshi:
		default:
			return fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON account %q market_source must be POLYMARKET or KALSHI", account.ExecutionAccountID)
		}
		if account.MarketSource == domain.MarketSourceKalshi {
			kalshi[account.ExecutionAccountID] = struct{}{}
		}
		switch account.Mode {
		case DecisionAccountActive, DecisionAccountSellOnly:
			notPaused++
		case DecisionAccountPaused:
		default:
			return fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON account %q mode must be ACTIVE, SELL_ONLY or PAUSED", account.ExecutionAccountID)
		}
		binding := domain.StrategyExecutionBinding{
			PredictionModelID: modelID, ModelID: modelID, StrategyID: account.StrategyID,
			ExecutionAccountID: account.ExecutionAccountID, MarketSource: account.MarketSource,
		}
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON account %q: %w", account.ExecutionAccountID, err)
		}
	}
	if notPaused == 0 {
		return fmt.Errorf("DECISION_CYCLE_ACCOUNTS_JSON cannot pause every account")
	}
	credentialed := make(map[string]struct{}, len(kalshiAccounts))
	for _, account := range kalshiAccounts {
		if _, exists := kalshi[account.ExecutionAccountID]; !exists {
			return fmt.Errorf("KALSHI_LIVE_ACCOUNTS_JSON account %q is not a KALSHI account in DECISION_CYCLE_ACCOUNTS_JSON", account.ExecutionAccountID)
		}
		credentialed[account.ExecutionAccountID] = struct{}{}
	}
	for accountID := range kalshi {
		if _, exists := credentialed[accountID]; !exists {
			return fmt.Errorf("KALSHI account %q has no credential in KALSHI_LIVE_ACCOUNTS_JSON", accountID)
		}
	}
	return nil
}

func secureServiceURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return false
	}
	if parsed.Scheme == "https" {
		return true
	}
	if parsed.Scheme != "http" {
		return false
	}
	if strings.EqualFold(parsed.Hostname(), "localhost") {
		return true
	}
	ip := net.ParseIP(parsed.Hostname())
	return ip != nil && ip.IsLoopback()
}

func secureExternalURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}

func canonicalPolymarketCLOBURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "clob.polymarket.com") {
		return false
	}
	return (parsed.Port() == "" || parsed.Port() == "443") && parsed.User == nil &&
		(parsed.Path == "" || parsed.Path == "/") && parsed.RawQuery == "" && parsed.Fragment == ""
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// env 读取环境变量并在为空时返回默认值。
func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// duration 读取并解析持续时间配置。
func duration(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return parsed, nil
}

func nonNegativeDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative duration", key)
	}
	return parsed, nil
}

// reconciliationScheduleOffset reads the optional wall-clock phase of
// scheduled reconciliation. Unset returns nil so existing deployments keep the
// start-relative ticker.
func reconciliationScheduleOffset(interval time.Duration) (*time.Duration, error) {
	const key = "RECONCILIATION_SCHEDULE_OFFSET"
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 || parsed >= interval {
		return nil, fmt.Errorf("%s must be a duration in [0, RECONCILIATION_INTERVAL=%s)", key, interval)
	}
	return &parsed, nil
}

// boolean 读取并解析布尔配置。
func boolean(key string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return parsed, nil
}

// integer 读取并校验有上下界的整数配置。
func integer(key string, fallback, minimum, maximum int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, minimum, maximum)
	}
	return parsed, nil
}

// decimal 将十进制值转换为精确有理数。
func decimal(key, fallback string) (domain.Decimal, error) {
	value, err := domain.ParseDecimal(env(key, fallback))
	if err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	if sign, _ := value.Sign(); sign <= 0 {
		return "", fmt.Errorf("%s must be positive", key)
	}
	return value, nil
}

func nonNegativeDecimal(key, fallback string) (domain.Decimal, error) {
	value, err := domain.ParseDecimal(env(key, fallback))
	if err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	if sign, _ := value.Sign(); sign < 0 {
		return "", fmt.Errorf("%s must be non-negative", key)
	}
	return value, nil
}

func commaSeparated(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.ToUpper(strings.TrimSpace(part)); part != "" {
			result = append(result, part)
		}
	}
	return result
}

// parseLogLevel 解析 Log Level。
func parseLogLevel(value string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(value))); err != nil {
		return 0, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return level, nil
}

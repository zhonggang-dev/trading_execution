package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// Config 表示后端使用的 Config 类型。
type Config struct {
	App            App
	HTTP           HTTP
	Database       Database
	Execution      Execution
	Polymarket     Polymarket
	LiveOperations LiveOperations
	DecisionCycle  DecisionCycle
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
	// SubmissionDisabledAccounts keeps a configured binding in the four-wallet
	// topology while excluding that account from automatic decision processing,
	// reconciliation, and durable intent delivery.
	SubmissionDisabledAccounts []string
	// EntrySubmissionDisabled is an operator-owned sell-only gate. It blocks
	// BUY intents without blocking validated exits from managed position lots.
	EntrySubmissionDisabled      bool
	RequireCompleteModelCoverage bool
	PredictionInfraBaseURL       string
	PredictionInfraToken         string
	StrategyBaseURL              string
	StrategyToken                string
	Interval                     time.Duration
	StartupDelay                 time.Duration
	MaxStartLateness             time.Duration
	Timeout                      time.Duration
	PredictionLookback           time.Duration
	MidPriceLookback             time.Duration
	Bindings                     []domain.StrategyExecutionBinding
	PredictionSourceModes        map[string]domain.PredictionSourceMode
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
	MaxOrderSize         domain.Decimal
	MaxOrderNotional     domain.Decimal
	CoordinatorInterval  time.Duration
	CoordinatorBatchSize int
}

// Polymarket 保存 fail-closed 实盘装配配置，钱包秘密只留在受限文件中。
type Polymarket struct {
	LiveTradingEnabled       bool
	AccountsFile             string
	CLOBURL                  string
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
	ReconciliationLookback   time.Duration
	PositionEpsilon          domain.Decimal
	BalanceEpsilon           domain.Decimal
	CancelFillFinalityGrace  time.Duration
	MaxReconcileAttempts     int
	MaxBuyFeeRateBPS         domain.Decimal
	OrderFilledConfirmations int
}

// Load 从环境变量加载并校验服务配置。
func Load() (Config, error) {
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
	maxOrderSize, err := decimal("EXECUTION_MAX_ORDER_SIZE", "1000")
	if err != nil {
		return Config{}, err
	}
	maxOrderNotional, err := decimal("EXECUTION_MAX_ORDER_NOTIONAL", "500")
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
	liveTradingEnabled, err := boolean("POLYMARKET_LIVE_TRADING_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	if executionMode == "live" && liveTradingEnabled {
		for _, key := range []string{
			"EXECUTION_MAX_ORDER_SIZE",
			"EXECUTION_MAX_ORDER_NOTIONAL",
			"POLYMARKET_MAX_BUY_FEE_RATE_BPS",
			"POLYGON_ORDER_FILLED_CONFIRMATIONS",
		} {
			if strings.TrimSpace(os.Getenv(key)) == "" {
				return Config{}, fmt.Errorf("%s must be explicitly configured in live mode", key)
			}
		}
	}
	polymarketRequestTimeout, err := duration("POLYMARKET_REQUEST_TIMEOUT", 5*time.Second)
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
	orderFilledConfirmations, err := integer("POLYGON_ORDER_FILLED_CONFIRMATIONS", 64, 1, 10_000)
	if err != nil {
		return Config{}, err
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
			MaxOrderSize:         maxOrderSize,
			MaxOrderNotional:     maxOrderNotional,
			CoordinatorInterval:  coordinatorInterval,
			CoordinatorBatchSize: coordinatorBatchSize,
		},
		Polymarket: Polymarket{
			LiveTradingEnabled: liveTradingEnabled,
			AccountsFile:       strings.TrimSpace(os.Getenv("POLYMARKET_ACCOUNTS_FILE")),
			CLOBURL:            env("POLYMARKET_CLOB_URL", "https://clob.polymarket.com"),
			GeoblockURL:        env("POLYMARKET_GEOBLOCK_URL", "https://polymarket.com/api/geoblock"),
			// Empty by default so a deployment cannot silently keep an old policy
			// exception. Production must opt in after checking the current official
			// geographic-restrictions page.
			FrontendOnlyAPICountries: commaSeparated(os.Getenv("POLYMARKET_FRONTEND_ONLY_API_COUNTRIES")),
			GammaURL:                 env("POLYMARKET_GAMMA_URL", "https://gamma-api.polymarket.com"),
			DataAPIURL:               env("POLYMARKET_DATA_API_URL", "https://data-api.polymarket.com"),
			PolygonRPCURL:            strings.TrimSpace(os.Getenv("POLYGON_RPC_URL")),
			RequestTimeout:           polymarketRequestTimeout,
			StartupTimeout:           startupTimeout,
			MaxClockSkew:             maxClockSkew,
			HeartbeatInterval:        heartbeatInterval,
			HeartbeatCallTimeout:     heartbeatCallTimeout,
			HeartbeatStaleAfter:      heartbeatStaleAfter,
			ReconciliationInterval:   reconciliationInterval,
			ReconciliationLookback:   reconciliationLookback,
			PositionEpsilon:          positionEpsilon,
			BalanceEpsilon:           balanceEpsilon,
			CancelFillFinalityGrace:  cancelFillFinalityGrace,
			MaxReconcileAttempts:     maxReconcileAttempts,
			MaxBuyFeeRateBPS:         maxBuyFeeRateBPS,
			OrderFilledConfirmations: orderFilledConfirmations,
		},
		LiveOperations: LiveOperations{
			Interval: liveOperationsInterval, RefreshTimeout: liveOperationsRefreshTimeout,
			MaxSnapshotAge: liveOperationsMaxAge, EventLimit: liveOperationsEventLimit,
		},
		DecisionCycle: decisionCycle,
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
		if config.LiveOperations.Interval < 5*time.Second || config.LiveOperations.Interval > 15*time.Second {
			return fmt.Errorf("LIVE_OPERATIONS_INTERVAL must be between 5s and 15s in live mode")
		}
		if config.LiveOperations.RefreshTimeout >= config.LiveOperations.Interval {
			return fmt.Errorf("LIVE_OPERATIONS_REFRESH_TIMEOUT must be less than LIVE_OPERATIONS_INTERVAL")
		}
		if config.LiveOperations.MaxSnapshotAge <= config.LiveOperations.Interval {
			return fmt.Errorf("LIVE_OPERATIONS_MAX_SNAPSHOT_AGE must be greater than LIVE_OPERATIONS_INTERVAL")
		}
	}
	if config.DecisionCycle.OrderSubmissionEnabled && !config.DecisionCycle.Enabled {
		return fmt.Errorf("DECISION_CYCLE_ORDER_SUBMISSION_ENABLED requires DECISION_CYCLE_ENABLED=true")
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
		if config.DecisionCycle.PredictionLookback < 10*time.Minute || config.DecisionCycle.PredictionLookback > 24*time.Hour {
			return fmt.Errorf("DECISION_CYCLE_PREDICTION_LOOKBACK must be between 10m and 24h")
		}
		if config.DecisionCycle.MidPriceLookback < 2*time.Hour || config.DecisionCycle.MidPriceLookback > 7*24*time.Hour {
			return fmt.Errorf("DECISION_CYCLE_MID_PRICE_LOOKBACK must be between 2h and 168h")
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
		if err := validateDecisionBindings(config.DecisionCycle.Bindings); err != nil {
			return err
		}
		if err := validateFourWalletSubmissionTopology(config.DecisionCycle.Bindings); err != nil {
			return err
		}
		if err := validateDecisionSubmissionDisabledAccounts(
			config.DecisionCycle.Bindings,
			config.DecisionCycle.SubmissionDisabledAccounts,
		); err != nil {
			return err
		}
		if err := validateCurrentWallet67Quarantine(config.DecisionCycle.SubmissionDisabledAccounts); err != nil {
			return err
		}
		if err := validateDecisionPredictionSourceModes(
			config.DecisionCycle.Bindings,
			config.DecisionCycle.PredictionSourceModes,
		); err != nil {
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
	predictionLookback, err := duration("DECISION_CYCLE_PREDICTION_LOOKBACK", 3*time.Hour)
	if err != nil {
		return DecisionCycle{}, err
	}
	midPriceLookback, err := duration("DECISION_CYCLE_MID_PRICE_LOOKBACK", 48*time.Hour)
	if err != nil {
		return DecisionCycle{}, err
	}
	bindings, err := decodeDecisionBindings(os.Getenv("DECISION_CYCLE_BINDINGS_JSON"))
	if err != nil {
		return DecisionCycle{}, err
	}
	predictionSourceModes, err := decodeDecisionPredictionSourceModes(
		os.Getenv("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON"),
	)
	if err != nil {
		return DecisionCycle{}, err
	}
	submissionDisabledAccounts, err := decodeDecisionSubmissionDisabledAccounts(
		os.Getenv("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON"),
	)
	if err != nil {
		return DecisionCycle{}, err
	}
	return DecisionCycle{
		Enabled: enabled, OrderSubmissionEnabled: submitEnabled,
		SubmissionDisabledAccounts:   submissionDisabledAccounts,
		EntrySubmissionDisabled:      entrySubmissionDisabled,
		RequireCompleteModelCoverage: requireCompleteModelCoverage,
		PredictionInfraBaseURL:       strings.TrimSpace(os.Getenv("DECISION_CYCLE_PREDICTION_INFRA_URL")),
		PredictionInfraToken:         strings.TrimSpace(os.Getenv("DECISION_CYCLE_PREDICTION_INFRA_TOKEN")),
		StrategyBaseURL:              strings.TrimSpace(os.Getenv("DECISION_CYCLE_STRATEGY_URL")),
		StrategyToken:                strings.TrimSpace(os.Getenv("DECISION_CYCLE_STRATEGY_TOKEN")),
		Interval:                     interval, StartupDelay: startupDelay,
		MaxStartLateness: maxStartLateness, Timeout: timeout,
		PredictionLookback: predictionLookback, MidPriceLookback: midPriceLookback,
		Bindings: bindings, PredictionSourceModes: predictionSourceModes,
	}, nil
}

func decodeDecisionPredictionSourceModes(value string) (map[string]domain.PredictionSourceMode, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	opening, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON: %w", err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON must be a JSON object")
	}
	modes := make(map[string]domain.PredictionSourceMode)
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return nil, fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON: %w", tokenErr)
		}
		predictionModelID, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON keys must be strings")
		}
		if _, duplicate := modes[predictionModelID]; duplicate {
			return nil, fmt.Errorf(
				"DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON contains duplicate prediction model %q",
				predictionModelID,
			)
		}
		var mode domain.PredictionSourceMode
		if decodeErr := decoder.Decode(&mode); decodeErr != nil {
			return nil, fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON: %w", decodeErr)
		}
		modes[predictionModelID] = mode
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON must contain exactly one JSON object")
	}
	return modes, nil
}

func decodeDecisionSubmissionDisabledAccounts(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var accounts []string
	if err := decoder.Decode(&accounts); err != nil {
		return nil, fmt.Errorf("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON must contain exactly one JSON array")
	}
	for index := range accounts {
		accounts[index] = strings.TrimSpace(accounts[index])
	}
	return accounts, nil
}

func decodeDecisionBindings(value string) ([]domain.StrategyExecutionBinding, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var bindings []domain.StrategyExecutionBinding
	if err := decoder.Decode(&bindings); err != nil {
		return nil, fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON must contain exactly one JSON array")
	}
	return bindings, nil
}

func validateDecisionBindings(bindings []domain.StrategyExecutionBinding) error {
	if len(bindings) == 0 {
		return fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON must contain at least one binding")
	}
	modelStrategies := make(map[string]struct{}, len(bindings))
	accounts := make(map[string]struct{}, len(bindings))
	logicalModelSources := make(map[string]string, len(bindings))
	sourceModelTargets := make(map[string]string, len(bindings))
	for index, binding := range bindings {
		binding = binding.Normalize()
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON binding %d: %w", index, err)
		}
		pair := binding.ModelID + "\x00" + binding.StrategyID
		if _, exists := modelStrategies[pair]; exists {
			return fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON contains duplicate model/strategy binding")
		}
		if _, exists := accounts[binding.ExecutionAccountID]; exists {
			return fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON contains duplicate execution account")
		}
		if sourceModelID, exists := logicalModelSources[binding.ModelID]; exists && sourceModelID != binding.PredictionModelID {
			return fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON maps logical model %q to multiple prediction models", binding.ModelID)
		}
		if logicalModelID, exists := sourceModelTargets[binding.PredictionModelID]; exists && logicalModelID != binding.ModelID {
			return fmt.Errorf("DECISION_CYCLE_BINDINGS_JSON maps prediction model %q to multiple logical models", binding.PredictionModelID)
		}
		modelStrategies[pair] = struct{}{}
		accounts[binding.ExecutionAccountID] = struct{}{}
		logicalModelSources[binding.ModelID] = binding.PredictionModelID
		sourceModelTargets[binding.PredictionModelID] = binding.ModelID
		bindings[index] = binding
	}
	return nil
}

func validateDecisionPredictionSourceModes(
	bindings []domain.StrategyExecutionBinding,
	modes map[string]domain.PredictionSourceMode,
) error {
	expected := make(map[string]string, len(bindings))
	for _, rawBinding := range bindings {
		binding := rawBinding.Normalize()
		expected[binding.PredictionModelID] = binding.ModelID
	}
	for predictionModelID, logicalModelID := range expected {
		mode, exists := modes[predictionModelID]
		if !exists {
			return fmt.Errorf(
				"DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON is missing configured prediction model %q",
				predictionModelID,
			)
		}
		if !mode.Valid() {
			return fmt.Errorf(
				"DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON model %q must be exactly DIRECT or SANDBOX",
				predictionModelID,
			)
		}
		switch logicalModelID {
		case "echo":
			if mode != domain.PredictionSourceModeDirect {
				return fmt.Errorf("this release requires logical model echo to use DIRECT prediction source mode")
			}
		case "gemini_masked":
			if mode != domain.PredictionSourceModeSandbox {
				return fmt.Errorf("this release requires logical model gemini_masked to use SANDBOX prediction source mode")
			}
		}
	}
	for predictionModelID := range modes {
		if _, exists := expected[predictionModelID]; !exists {
			return fmt.Errorf(
				"DECISION_CYCLE_PREDICTION_SOURCE_MODES_JSON contains unconfigured prediction model %q",
				predictionModelID,
			)
		}
	}
	return nil
}

func validateDecisionSubmissionDisabledAccounts(
	bindings []domain.StrategyExecutionBinding,
	accounts []string,
) error {
	bound := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		bound[binding.Normalize().ExecutionAccountID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(accounts))
	for index, accountID := range accounts {
		accountID = strings.TrimSpace(accountID)
		if accountID == "" {
			return fmt.Errorf("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON account %d is empty", index)
		}
		if _, duplicate := seen[accountID]; duplicate {
			return fmt.Errorf("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON contains duplicate account %q", accountID)
		}
		if _, exists := bound[accountID]; !exists {
			return fmt.Errorf("DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON account %q is not a configured binding", accountID)
		}
		seen[accountID] = struct{}{}
	}
	return nil
}

// validateCurrentWallet67Quarantine keeps the release quarantine boundary
// limited to the exact four configured wallets. This permits a reviewed
// wallet-6/wallet-7-only cohort by quarantining main and wallet-1; the database
// authorization gate still requires the enabled binding/policy/control set to
// equal the non-quarantined accounts exactly.
func validateCurrentWallet67Quarantine(accounts []string) error {
	allowed := map[string]struct{}{
		"main": {}, "wallet-1": {}, "wallet-6": {}, "wallet-7": {},
	}
	configured := make(map[string]struct{}, len(accounts))
	for _, accountID := range accounts {
		accountID = strings.TrimSpace(accountID)
		if _, exists := allowed[accountID]; !exists {
			return fmt.Errorf(
				"this release permits only main, wallet-1, wallet-6, and wallet-7 in DECISION_CYCLE_SUBMISSION_DISABLED_ACCOUNTS_JSON",
			)
		}
		configured[accountID] = struct{}{}
	}
	_, hasMain := configured["main"]
	_, hasWallet1 := configured["wallet-1"]
	_, hasWallet6 := configured["wallet-6"]
	_, hasWallet7 := configured["wallet-7"]
	supported := len(configured) == 0 ||
		(len(configured) == 1 && (hasWallet6 || hasWallet7)) ||
		(len(configured) == 2 && ((hasWallet6 && hasWallet7) || (hasMain && hasWallet1)))
	if !supported {
		return fmt.Errorf("submission-disabled accounts do not match a reviewed release cohort")
	}
	return nil
}

// validateFourWalletSubmissionTopology prevents a legacy one-binding canary
// from becoming production merely by enabling the two generic cycle gates.
// Changing the live topology is a deliberate code/config contract change.
func validateFourWalletSubmissionTopology(bindings []domain.StrategyExecutionBinding) error {
	if len(bindings) != 4 {
		return fmt.Errorf("live decision submission requires the exact 2 models x 2 strategies x 4 accounts topology")
	}
	expectedAccounts := map[string]struct{}{
		"main": {}, "wallet-1": {}, "wallet-6": {}, "wallet-7": {},
	}
	expectedRoutes := map[string]string{
		"echo\x00" + domain.StrategyIDMultfactorV2:          "main",
		"echo\x00" + domain.StrategyIDMultfactorV1:          "wallet-1",
		"gemini_masked\x00" + domain.StrategyIDMultfactorV1: "wallet-6",
		"gemini_masked\x00" + domain.StrategyIDMultfactorV2: "wallet-7",
	}
	accounts := make(map[string]struct{}, len(bindings))
	logicalModels := make(map[string]struct{}, 2)
	predictionModels := make(map[string]struct{}, 2)
	pairs := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		binding = binding.Normalize()
		accounts[binding.ExecutionAccountID] = struct{}{}
		logicalModels[binding.ModelID] = struct{}{}
		predictionModels[binding.PredictionModelID] = struct{}{}
		pair := binding.ModelID + "\x00" + binding.StrategyID
		expectedAccountID, expected := expectedRoutes[pair]
		if !expected {
			return fmt.Errorf("live decision submission contains unsupported model/strategy route %q/%q", binding.ModelID, binding.StrategyID)
		}
		if binding.ExecutionAccountID != expectedAccountID {
			return fmt.Errorf(
				"live decision submission route %q/%q must use execution account %q",
				binding.ModelID, binding.StrategyID, expectedAccountID,
			)
		}
		pairs[pair] = struct{}{}
	}
	if len(logicalModels) != 2 || len(predictionModels) != 2 || len(pairs) != 4 {
		return fmt.Errorf("live decision submission requires the exact 2 models x 2 strategies x 4 accounts topology")
	}
	for accountID := range expectedAccounts {
		if _, exists := accounts[accountID]; !exists {
			return fmt.Errorf("live decision submission requires execution accounts main, wallet-1, wallet-6, and wallet-7")
		}
	}
	for pair := range expectedRoutes {
		if _, exists := pairs[pair]; !exists {
			return fmt.Errorf("live decision submission requires the exact echo/gemini_masked route matrix")
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

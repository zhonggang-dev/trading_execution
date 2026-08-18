package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// Config 表示后端使用的 Config 类型。
type Config struct {
	App       App
	HTTP      HTTP
	Database  Database
	Execution Execution
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
}

// Database 表示后端使用的 Database 类型。
type Database struct {
	URL            string
	ConnectTimeout time.Duration
}

// Execution 表示后端使用的 Execution 类型。
type Execution struct {
	Mode              string
	Venue             string
	AllowMarketOrders bool
	MaxOrderSize      domain.Decimal
	MaxOrderNotional  domain.Decimal
}

// Load 从环境变量加载并校验服务配置。
func Load() (Config, error) {
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
	writeTimeout, err := duration("HTTP_WRITE_TIMEOUT", 10*time.Second)
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
		},
		Database: Database{
			URL:            strings.TrimSpace(os.Getenv("TRADING_EXECUTION_DATABASE_URL")),
			ConnectTimeout: databaseConnectTimeout,
		},
		Execution: Execution{
			Mode:              strings.ToLower(env("EXECUTION_MODE", "paper")),
			Venue:             strings.ToLower(env("EXECUTION_VENUE", "polymarket-paper")),
			AllowMarketOrders: allowMarketOrders,
			MaxOrderSize:      maxOrderSize,
			MaxOrderNotional:  maxOrderNotional,
		},
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
	if config.Execution.Mode != "paper" {
		return fmt.Errorf("execution mode %q is unavailable: install and wire a real venue adapter before enabling live mode", config.Execution.Mode)
	}
	if strings.TrimSpace(config.Execution.Venue) == "" {
		return fmt.Errorf("execution venue is required")
	}
	if config.App.Env != "local" && len(config.HTTP.APIToken) < 32 {
		return fmt.Errorf("EXECUTION_API_TOKEN must contain at least 32 bytes outside local environment")
	}
	return nil
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

// parseLogLevel 解析 Log Level。
func parseLogLevel(value string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(value))); err != nil {
		return 0, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return level, nil
}

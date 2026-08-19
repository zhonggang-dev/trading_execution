package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// LiveNumber 表示运维接口中的精确 JSON 数字，避免资金数据经过 float64 后产生二进制舍入。
type LiveNumber string

// liveNumberPattern 限制为 RFC 8259 可直接编码的普通十进制 JSON number。
var liveNumberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// NewLiveNumber 将领域十进制数转换为运维接口使用的 JSON 数字。
func NewLiveNumber(value Decimal) (LiveNumber, error) {
	parsed, err := ParseDecimal(value.String())
	if err != nil {
		return "", err
	}
	if !liveNumberPattern.MatchString(parsed.String()) {
		return "", fmt.Errorf("live number must use canonical JSON decimal notation")
	}
	return LiveNumber(parsed), nil
}

// MarshalJSON 将精确十进制文本直接编码成 JSON number。
func (number LiveNumber) MarshalJSON() ([]byte, error) {
	parsed, err := ParseDecimal(string(number))
	if err != nil {
		return nil, err
	}
	if !liveNumberPattern.MatchString(parsed.String()) {
		return nil, fmt.Errorf("live number must use canonical JSON decimal notation")
	}
	return []byte(parsed.String()), nil
}

// UnmarshalJSON 从 JSON number 解码运维数值。
func (number *LiveNumber) UnmarshalJSON(data []byte) error {
	if number == nil {
		return fmt.Errorf("live number target is nil")
	}
	if len(data) == 0 || data[0] == '"' || string(data) == "null" {
		return fmt.Errorf("live number must be a JSON number")
	}
	parsed, err := ParseDecimal(string(data))
	if err != nil {
		return err
	}
	if !liveNumberPattern.MatchString(parsed.String()) {
		return fmt.Errorf("live number must use canonical JSON decimal notation")
	}
	*number = LiveNumber(parsed)
	return nil
}

// LiveHealth 表示实盘运维组件的健康等级。
type LiveHealth string

const (
	LiveHealthHealthy  LiveHealth = "healthy"
	LiveHealthDegraded LiveHealth = "degraded"
	LiveHealthStopped  LiveHealth = "stopped"
)

// LiveFlowState 表示漏斗、生命周期和风险条目的展示状态。
type LiveFlowState string

const (
	LiveFlowDone    LiveFlowState = "done"
	LiveFlowActive  LiveFlowState = "active"
	LiveFlowWarning LiveFlowState = "warning"
	LiveFlowIdle    LiveFlowState = "idle"
	LiveFlowSafe    LiveFlowState = "safe"
)

// LiveOperationsSnapshot 表示前端每次读取的完整实盘运维快照。
type LiveOperationsSnapshot struct {
	ObservedAt           time.Time         `json:"observedAt"`
	DataFreshnessSeconds int64             `json:"dataFreshnessSeconds"`
	Engine               LiveEngine        `json:"engine"`
	Capital              LiveCapital       `json:"capital"`
	Workers              []LiveWorker      `json:"workers"`
	Funnel               []LiveFunnelStage `json:"funnel"`
	Risks                []LiveRisk        `json:"risks"`
	Orders               []LiveOrder       `json:"orders"`
	Positions            []LivePosition    `json:"positions"`
	Events               []LiveEvent       `json:"events"`
	DataQuality          []LiveDataQuality `json:"dataQuality"`
	SourceObservedAt     time.Time         `json:"-"`
}

// LiveEngine 表示实盘进程、交易所、账本和对账的总体状态。
type LiveEngine struct {
	Health               LiveHealth `json:"health"`
	RunID                string     `json:"runId"`
	PresetName           string     `json:"presetName"`
	StartedAt            time.Time  `json:"startedAt"`
	VenueName            string     `json:"venueName"`
	VenueStatus          LiveHealth `json:"venueStatus"`
	LedgerStatus         LiveHealth `json:"ledgerStatus"`
	ReconciliationStatus LiveHealth `json:"reconciliationStatus"`
}

// LiveCapital 表示所有配置执行账户合并后的资金与盈亏。
type LiveCapital struct {
	Equity           LiveNumber `json:"equity"`
	AvailableCash    LiveNumber `json:"availableCash"`
	GrossExposure    LiveNumber `json:"grossExposure"`
	ExposureLimit    LiveNumber `json:"exposureLimit"`
	RealizedPnLToday LiveNumber `json:"realizedPnlToday"`
	UnrealizedPnL    LiveNumber `json:"unrealizedPnl"`
	FeeToday         LiveNumber `json:"feeToday"`
}

// LiveWorker 表示一个调度线程最近一次持久化的 heartbeat。
type LiveWorker struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	Purpose         string     `json:"purpose"`
	Cadence         string     `json:"cadence"`
	Status          LiveHealth `json:"status"`
	LastHeartbeatAt *time.Time `json:"lastHeartbeatAt"`
	CurrentTask     string     `json:"currentTask"`
	CurrentCycleID  string     `json:"currentCycleId,omitempty"`
	LastError       string     `json:"lastError,omitempty"`
	MetricLabel     string     `json:"metricLabel"`
	MetricValue     string     `json:"metricValue"`
}

// LiveFunnelStage 表示同一个 run_id 和 cycle_id 下的一步执行漏斗。
type LiveFunnelStage struct {
	ID              string        `json:"id"`
	Index           int           `json:"index"`
	Name            string        `json:"name"`
	Description     string        `json:"description"`
	Count           int64         `json:"count"`
	ThroughputLabel string        `json:"throughputLabel"`
	State           LiveFlowState `json:"state"`
}

// LiveRisk 表示一个当前生效的硬风控指标及其限额。
type LiveRisk struct {
	ID      string        `json:"id"`
	Name    string        `json:"name"`
	Current LiveNumber    `json:"current"`
	Limit   LiveNumber    `json:"limit"`
	Unit    string        `json:"unit"`
	Hint    string        `json:"hint"`
	State   LiveFlowState `json:"state"`
}

// LiveLifecycleStep 表示订单不可变事件映射出的一个生命周期节点。
type LiveLifecycleStep struct {
	Name      string        `json:"name"`
	Status    LiveFlowState `json:"status"`
	Timestamp time.Time     `json:"timestamp"`
	Detail    string        `json:"detail"`
}

// LiveOrder 表示本系统订单与 CLOB、真实 Fill 验真后的聚合视图。
type LiveOrder struct {
	OrderID              string              `json:"orderId"`
	ExecutionAccountID   string              `json:"executionAccountId"`
	VenueOrderID         string              `json:"venueOrderId,omitempty"`
	MarketID             string              `json:"marketId"`
	ConditionID          string              `json:"conditionId"`
	TokenID              string              `json:"tokenId"`
	MarketLabel          string              `json:"marketLabel"`
	OutcomeName          string              `json:"outcomeName"`
	Side                 Side                `json:"side"`
	Status               string              `json:"status"`
	Price                LiveNumber          `json:"price"`
	Shares               LiveNumber          `json:"shares"`
	FilledShares         LiveNumber          `json:"filledShares"`
	AgeSeconds           int64               `json:"ageSeconds"`
	ModelID              string              `json:"modelId"`
	StrategyID           string              `json:"strategyId"`
	TriggeredBy          string              `json:"triggeredBy"`
	PredictedProbability *float64            `json:"predictedProbability,omitempty"`
	Edge                 *LiveNumber         `json:"edge,omitempty"`
	Lifecycle            []LiveLifecycleStep `json:"lifecycle"`
}

// LivePosition 表示链上/Data API 持仓与本地成本账本合并后的盯市视图。
type LivePosition struct {
	PositionID           string     `json:"positionId"`
	ExecutionAccountID   string     `json:"executionAccountId"`
	MarketID             string     `json:"marketId"`
	ConditionID          string     `json:"conditionId"`
	TokenID              string     `json:"tokenId"`
	MarketLabel          string     `json:"marketLabel"`
	OutcomeName          string     `json:"outcomeName"`
	Shares               LiveNumber `json:"shares"`
	AveragePrice         LiveNumber `json:"averagePrice"`
	MarkPrice            LiveNumber `json:"markPrice"`
	Cost                 LiveNumber `json:"cost"`
	MarketValue          LiveNumber `json:"marketValue"`
	UnrealizedPnL        LiveNumber `json:"unrealizedPnl"`
	ExposurePct          LiveNumber `json:"exposurePct"`
	StrategyID           string     `json:"strategyId"`
	PredictionAgeMinutes *int64     `json:"predictionAgeMinutes,omitempty"`
}

// LiveEvent 表示最近发生的成交、订单、风险、预测或系统事件。
type LiveEvent struct {
	ID          string    `json:"id"`
	Timestamp   time.Time `json:"timestamp"`
	Severity    string    `json:"severity"`
	Thread      string    `json:"thread"`
	Section     string    `json:"section"`
	Title       string    `json:"title"`
	Detail      string    `json:"detail"`
	MarketLabel string    `json:"marketLabel,omitempty"`
	OrderID     *string   `json:"orderId"`
}

// LiveDataQuality 表示一个事实来源当前的新鲜度或一致性结果。
type LiveDataQuality struct {
	ID     string     `json:"id"`
	Name   string     `json:"name"`
	Status LiveHealth `json:"status"`
	Detail string     `json:"detail"`
}

// LiveOperationsQuery 表示 PostgreSQL 运维只读模型的查询范围。
type LiveOperationsQuery struct {
	ExecutionAccountIDs []string
	RunID               string
	DayStart            time.Time
	ObservedAt          time.Time
	RecentOrderSince    time.Time
	EventLimit          int
}

// LiveOperationsLocalState 表示一个 PostgreSQL 可重复读事务内得到的本地权威状态。
type LiveOperationsLocalState struct {
	Accounts            []LiveAccountState
	Positions           []LiveLedgerPosition
	Orders              []LiveLedgerOrder
	RiskPolicies        []LiveRiskPolicyState
	Reconciliations     []LiveReconciliationState
	Workers             []LiveWorkerState
	Funnel              []LiveFunnelStage
	Events              []LiveEvent
	ConfirmedTradeIDs   map[string]struct{}
	RealizedPnLToday    Decimal
	FeeToday            Decimal
	DailyTradedNotional Decimal
	DatabaseObservedAt  time.Time
}

// LiveAccountState 表示内部聚合使用的账户账本身份，钱包地址不会进入 HTTP 响应。
type LiveAccountState struct {
	ExecutionAccountID string
	WalletAddress      string
	CollateralAsset    string
	TotalBalance       Decimal
	AvailableBalance   Decimal
	ReservedBalance    Decimal
	UpdatedAt          time.Time
}

// LiveLedgerPosition 表示本地仓位快照及其来源策略。
type LiveLedgerPosition struct {
	Position       Position
	ModelID        string
	StrategyID     string
	MarketLabel    string
	LatestSignalAt *time.Time
}

// LiveLedgerOrder 表示本地订单及其不可变生命周期事件。
type LiveLedgerOrder struct {
	Order       Order
	Events      []OrderEvent
	MarketLabel string
}

// LiveRiskPolicyState 表示一个账户当前生效的实盘硬风控策略。
type LiveRiskPolicyState struct {
	ExecutionAccountID     string
	PolicyID               string
	Enabled                bool
	MaxMarketExposure      Decimal
	MaxWalletExposure      Decimal
	MaxDailyTradedNotional Decimal
	MaxSignalAge           time.Duration
	MaxStateAge            time.Duration
	KillSwitch             bool
	AccountPaused          bool
}

// LiveReconciliationState 表示账户最近一次对账及尚未关闭的问题数量。
type LiveReconciliationState struct {
	ExecutionAccountID string
	Run                ReconciliationRun
	OpenIssues         int64
}

// LiveWorkerState 表示 runtime_status 表中的原始 heartbeat。
type LiveWorkerState struct {
	ThreadID        string
	RunID           string
	Name            string
	Purpose         string
	Cadence         string
	MaxHeartbeatAge time.Duration
	LastHeartbeatAt time.Time
	CurrentTask     string
	CurrentCycleID  string
	LastError       string
	MetricLabel     string
	MetricValue     string
	Stopped         bool
}

// LiveFunnelReport 表示一个业务线程原子上报的完整单轮漏斗。
type LiveFunnelReport struct {
	RunID      string
	CycleID    string
	ObservedAt time.Time
	Stages     []LiveFunnelStage
}

// NormalizeLiveWorkerID 规范化允许运维页面展示的线程标识。
func NormalizeLiveWorkerID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// CloneLiveOperationsSnapshot 通过 JSON 往返复制只读快照，防止调用方修改缓存中的切片。
func CloneLiveOperationsSnapshot(snapshot LiveOperationsSnapshot) (LiveOperationsSnapshot, error) {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return LiveOperationsSnapshot{}, err
	}
	var cloned LiveOperationsSnapshot
	if err := json.Unmarshal(payload, &cloned); err != nil {
		return LiveOperationsSnapshot{}, err
	}
	cloned.SourceObservedAt = snapshot.SourceObservedAt
	return cloned, nil
}

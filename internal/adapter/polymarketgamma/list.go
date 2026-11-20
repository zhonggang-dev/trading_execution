package polymarketgamma

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"strconv"
	"strings"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const (
	// maxListPageSize 是 Gamma /markets 单页上限，超出时服务端静默截断为 100。
	maxListPageSize = 100
	// maxListOffset 是 Gamma /markets 可用的最大 offset，更深分页服务端返回 422。
	maxListOffset = 2000
)

var _ port.MarketLister = (*Client)(nil)

// gammaListedMarket 在执行字段之外声明列表筛选用的成交量与流动性。
type gammaListedMarket struct {
	gammaMarket
	Question     string         `json:"question"`
	Slug         string         `json:"slug"`
	EndDate      *string        `json:"endDate"`
	VolumeNum    flexibleNumber `json:"volumeNum"`
	LiquidityNum flexibleNumber `json:"liquidityNum"`
}

// ListMarkets 返回一页正在接受下单的二元市场，页间按流动性升序（同流动性的成交量次序由调用方排序）。
// 非二元、缺少关键状态字段或状态自相矛盾的市场被跳过而不是让整页失败，
// 因为这是只读筛选，不能被个别脏数据阻断。
func (client *Client) ListMarkets(ctx context.Context, filter domain.MarketListFilter) (domain.MarketListingPage, error) {
	if filter.Limit < 1 || filter.Limit > maxListPageSize {
		return domain.MarketListingPage{}, fmt.Errorf("Gamma market list limit must be between 1 and %d", maxListPageSize)
	}
	if filter.Offset < 0 || filter.Offset > maxListOffset {
		return domain.MarketListingPage{}, fmt.Errorf("Gamma market list offset must be between 0 and %d", maxListOffset)
	}
	endpoint := *client.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/markets"
	query := url.Values{}
	query.Set("closed", "false")
	query.Set("active", "true")
	query.Set("enable_order_book", "true")
	query.Set("order", "liquidityNum")
	query.Set("ascending", "true")
	query.Set("limit", strconv.Itoa(filter.Limit))
	query.Set("offset", strconv.Itoa(filter.Offset))
	if !filter.MaxVolume.IsEmpty() {
		query.Set("volume_num_max", filter.MaxVolume.String())
	}
	if !filter.MinLiquidity.IsEmpty() {
		query.Set("liquidity_num_min", filter.MinLiquidity.String())
	}
	if !filter.MaxLiquidity.IsEmpty() {
		query.Set("liquidity_num_max", filter.MaxLiquidity.String())
	}
	endpoint.RawQuery = query.Encode()

	body, err := client.get(ctx, endpoint.String())
	if err != nil {
		return domain.MarketListingPage{}, fmt.Errorf("list Gamma markets: %w", err)
	}
	if bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		return domain.MarketListingPage{}, fmt.Errorf("decode Gamma market list: response must be a JSON array")
	}
	var raw []gammaListedMarket
	if err := json.Unmarshal(body, &raw); err != nil {
		return domain.MarketListingPage{}, fmt.Errorf("decode Gamma market list: %w", err)
	}

	observedAt := client.now().UTC()
	listings := make([]domain.MarketListing, 0, len(raw))
	for _, item := range raw {
		if item.AcceptingOrders == nil || !*item.AcceptingOrders {
			continue
		}
		snapshot, err := mapMarket(item.gammaMarket, strings.TrimSpace(item.ConditionID), observedAt)
		if err != nil || snapshot.Closed || snapshot.Paused || !snapshot.AcceptingOrders {
			continue
		}
		endDate, _ := mapClosedTime(item.EndDate)
		listings = append(listings, domain.MarketListing{
			MarketID:    snapshot.MarketID,
			ConditionID: snapshot.ConditionID,
			Question:    strings.TrimSpace(item.Question),
			Slug:        strings.TrimSpace(item.Slug),
			EndDate:     endDate,
			Volume:      item.VolumeNum.decimal(),
			Liquidity:   item.LiquidityNum.decimal(),
			Outcomes:    snapshot.Outcomes,
		})
	}
	page := domain.MarketListingPage{Markets: listings, Fetched: len(raw)}
	if len(raw) > 0 {
		page.LastLiquidity = raw[len(raw)-1].LiquidityNum.decimal()
	}
	return page, nil
}

// flexibleNumber 接受 JSON number、numeric string 与 null；null 视为 0，
// 并把可能带指数的浮点文本转换为 Decimal 能接受的纯十进制表示。
type flexibleNumber struct {
	value *big.Rat
}

// UnmarshalJSON 解析 Gamma 的数值字段。
func (number *flexibleNumber) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		number.value = nil
		return nil
	}
	text := string(trimmed)
	if trimmed[0] == '"' {
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return fmt.Errorf("decode numeric string: %w", err)
		}
	}
	text = strings.TrimSpace(text)
	if text == "" {
		number.value = nil
		return nil
	}
	parsed, ok := new(big.Rat).SetString(text)
	if !ok {
		return fmt.Errorf("invalid number %q", text)
	}
	number.value = parsed
	return nil
}

// decimal 返回八位小数的十进制表示，缺失时为 0。
func (number flexibleNumber) decimal() domain.Decimal {
	if number.value == nil {
		return "0"
	}
	return domain.Decimal(number.value.FloatString(8)).Canonical()
}

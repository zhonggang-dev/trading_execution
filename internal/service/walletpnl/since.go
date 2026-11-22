package walletpnl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// maxAccountSince 限制 WALLET_PNL_ACCOUNT_SINCE_JSON 的条目数。
const maxAccountSince = 64

// ParseAccountSince 解析 WALLET_PNL_ACCOUNT_SINCE_JSON：JSON 对象，
// 键为执行账户 ID，值为 RFC3339 整秒时间，表示该账户官方盈亏曲线的起点
// （复用旧地址的钱包用账本重置时刻，排除复用前的历史）。空串表示不启用。
// 账户必须属于官方盈亏受管集合 managed；重复键、空账户、非法时间一律拒绝。
func ParseAccountSince(raw string, managed []string) (map[string]time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	allowed := make(map[string]struct{}, len(managed))
	for _, account := range managed {
		if account = strings.TrimSpace(account); account != "" {
			allowed[account] = struct{}{}
		}
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("decode wallet PnL account since: %w", err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("decode wallet PnL account since: must be a JSON object")
	}
	result := make(map[string]time.Time)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("decode wallet PnL account since: %w", err)
		}
		account, _ := keyToken.(string)
		var value string
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("decode wallet PnL account since %q: value must be an RFC3339 string", account)
		}
		if account != strings.TrimSpace(account) || account == "" {
			return nil, fmt.Errorf("wallet PnL account since: account id is invalid")
		}
		if _, duplicate := result[account]; duplicate {
			return nil, fmt.Errorf("wallet PnL account since: duplicate account %q", account)
		}
		if _, ok := allowed[account]; !ok {
			return nil, fmt.Errorf("wallet PnL account since: account %q is not a managed Polymarket account", account)
		}
		since, err := time.Parse(time.RFC3339, strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("wallet PnL account since %q: time must be RFC3339", account)
		}
		// 官方序列按秒取点；带小数秒的起点无法精确比较，直接拒绝。
		if since.Nanosecond() != 0 {
			return nil, fmt.Errorf("wallet PnL account since %q: time must be whole seconds", account)
		}
		result[account] = since.UTC()
		if len(result) > maxAccountSince {
			return nil, fmt.Errorf("too many wallet PnL account since entries")
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("decode wallet PnL account since: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("decode wallet PnL account since: trailing data")
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// rebaseSince 让曲线从 since 起以 0 计：
//   - 基数 = 时间 ≤ since 的最后一个点的值（没有则为 0）；
//   - 去掉时间 < since 的点，其余点减去基数；
//   - 如果去掉了点，且 since 处没有点，在 since 处补一个值为 0 的点
//     （整条序列都早于 since 时，结果就只有这一个 0 点）；
//   - 序列全部晚于 since 时原样返回（短周期前端按首点算区间变化，不受影响）。
//
// 调用方传入的是缓存结果的副本，这里返回新切片，不改动入参。
func rebaseSince(points []Point, since time.Time) ([]Point, error) {
	at := since.Unix()
	sorted := sort.SliceIsSorted(points, func(left, right int) bool { return points[left].Timestamp < points[right].Timestamp })
	if !sorted {
		return nil, fmt.Errorf("%w: wallet PnL points are not ordered", ErrUnavailable)
	}
	baseIndex := -1
	dropped := 0
	for index, point := range points {
		if point.Timestamp <= at {
			baseIndex = index
		}
		if point.Timestamp < at {
			dropped++
		}
	}
	if baseIndex < 0 {
		return points, nil
	}
	base, err := pointRat(points[baseIndex].TradePnL)
	if err != nil {
		return nil, err
	}
	kept := points[dropped:]
	result := make([]Point, 0, len(kept)+1)
	if len(kept) == 0 || kept[0].Timestamp != at {
		result = append(result, Point{Timestamp: at, TradePnL: "0"})
	}
	for _, point := range kept {
		value, err := pointRat(point.TradePnL)
		if err != nil {
			return nil, err
		}
		scale := max(point.TradePnL.Scale(), points[baseIndex].TradePnL.Scale())
		point.TradePnL = formatExact(value.Sub(value, base), scale)
		result = append(result, point)
	}
	return result, nil
}

func pointRat(value domain.Decimal) (*big.Rat, error) {
	rat, ok := new(big.Rat).SetString(strings.TrimSpace(value.String()))
	if !ok {
		return nil, fmt.Errorf("%w: wallet PnL point value is invalid", ErrUnavailable)
	}
	return rat, nil
}

// formatExact 按两个操作数的最大小数位输出差值；十进制减法在该位数下是精确的。
func formatExact(value *big.Rat, scale int) domain.Decimal {
	text := value.FloatString(scale)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	if text == "-0" || text == "" {
		text = "0"
	}
	return domain.Decimal(text)
}

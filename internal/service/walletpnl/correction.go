package walletpnl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// Correction 描述官方盈亏序列里一个已知的「幽灵持仓」：上游漏记了某些成交，
// 于是在 EffectiveAt 之后一直以为钱包还持有 PhantomShares 份该 token，
// 并且少记了 MissingCash 的净现金。对 EffectiveAt 之后的每个点，
// 真实值 = 官方值 + MissingCash − PhantomShares × 该点取值时刻的价格。
type Correction struct {
	WalletAddress string
	TokenID       string
	EffectiveAt   time.Time
	PhantomShares domain.Decimal
	MissingCash   domain.Decimal
}

var walletAddressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// ParseCorrections 解析 WALLET_PNL_CORRECTIONS_JSON。空串表示不修正。
func ParseCorrections(raw string) ([]Correction, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var entries []struct {
		WalletAddress string `json:"wallet_address"`
		TokenID       string `json:"token_id"`
		EffectiveAt   string `json:"effective_at"`
		PhantomShares string `json:"phantom_shares"`
		MissingCash   string `json:"missing_cash"`
		Note          string `json:"note"`
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entries); err != nil {
		return nil, fmt.Errorf("decode wallet PnL corrections: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("decode wallet PnL corrections: trailing data")
	}
	if len(entries) > 64 {
		return nil, fmt.Errorf("too many wallet PnL corrections")
	}
	seen := make(map[string]struct{}, len(entries))
	corrections := make([]Correction, 0, len(entries))
	for index, entry := range entries {
		address := strings.ToLower(strings.TrimSpace(entry.WalletAddress))
		if !walletAddressPattern.MatchString(address) {
			return nil, fmt.Errorf("wallet PnL correction %d: wallet_address is invalid", index)
		}
		token := strings.TrimSpace(entry.TokenID)
		if token == "" || len(token) > 100 || strings.Trim(token, "0123456789") != "" {
			return nil, fmt.Errorf("wallet PnL correction %d: token_id is invalid", index)
		}
		effectiveAt, err := time.Parse(time.RFC3339, strings.TrimSpace(entry.EffectiveAt))
		if err != nil {
			return nil, fmt.Errorf("wallet PnL correction %d: effective_at must be RFC3339", index)
		}
		shares, err := domain.ParseDecimal(entry.PhantomShares)
		if err != nil {
			return nil, fmt.Errorf("wallet PnL correction %d: phantom_shares is invalid", index)
		}
		cash, err := domain.ParseDecimal(entry.MissingCash)
		if err != nil {
			return nil, fmt.Errorf("wallet PnL correction %d: missing_cash is invalid", index)
		}
		// 同一 token 可以有多条事件（如先卖出、后买回），按生效时间各自叠加。
		key := address + "|" + token + "|" + effectiveAt.UTC().Format(time.RFC3339)
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("wallet PnL correction %d: duplicate wallet, token and effective_at", index)
		}
		seen[key] = struct{}{}
		corrections = append(corrections, Correction{WalletAddress: address, TokenID: token, EffectiveAt: effectiveAt.UTC(), PhantomShares: shares, MissingCash: cash})
	}
	return corrections, nil
}

// PriceReader 读取 outcome token 的历史价格桶。
type PriceReader interface {
	PriceHistory(ctx context.Context, tokenID string, start, end int64, bucketSeconds int) ([]polymarketdata.PricePoint, error)
}

// BlockClock 读取 Polygon 区块的出块时间和当前最新区块。
type BlockClock interface {
	BlockTimestamp(ctx context.Context, blockNumber uint64) (time.Time, error)
	ObservedBlock(ctx context.Context) (uint64, error)
}

// Corrector 把已知幽灵持仓从官方 trade_pnl 序列中扣掉。
type Corrector struct {
	byWallet   map[string][]Correction
	prices     PriceReader
	blocks     BlockClock
	now        func() time.Time
	mu         sync.Mutex
	blockTimes map[uint64]int64
	chunks     map[string]priceChunk
	// anchorSpacing 是固定锚点的区块间隔；latest 缓存最新区块，避免每次请求都查。
	anchorSpacing uint64
	latest        uint64
	latestAt      time.Time
}

type priceChunk struct {
	points    []polymarketdata.PricePoint
	expiresAt time.Time // 零值表示历史区间，永久有效
}

const (
	fineBucketSeconds   = 300
	hourlyBucketSeconds = 3600
	dailyBucketSeconds  = 86400
	// 上游 5 分钟桶约保留 60 天、1 小时桶约保留 90 天，留出余量。
	fineBucketMaxAge   = 55 * 24 * time.Hour
	hourlyBucketMaxAge = 85 * 24 * time.Hour
	maxPriceChunks     = 512
	maxBlockTimes      = 4096
	// 约 17 天一段（1.5 秒出块）。段内出块速度稳定，用两端锚点插值误差只有几秒。
	defaultAnchorSpacing = 1_000_000
	// 锚点需要已被 RPC 后端稳定提供，留出余量后才把一段视为已完成。
	anchorFinalityBlocks  = 1000
	blockFetchParallelism = 8
	priceFetchParallelism = 4
)

func NewCorrector(corrections []Correction, prices PriceReader, blocks BlockClock, now func() time.Time) (*Corrector, error) {
	if len(corrections) == 0 {
		return nil, nil
	}
	if prices == nil || blocks == nil {
		return nil, fmt.Errorf("wallet PnL corrections require price and block readers")
	}
	if now == nil {
		now = time.Now
	}
	byWallet := make(map[string][]Correction)
	for _, correction := range corrections {
		byWallet[correction.WalletAddress] = append(byWallet[correction.WalletAddress], correction)
	}
	return &Corrector{byWallet: byWallet, prices: prices, blocks: blocks, now: now, blockTimes: make(map[uint64]int64), chunks: make(map[string]priceChunk), anchorSpacing: defaultAnchorSpacing}, nil
}

// Apply 原地修正属于该钱包的序列点。任何价格或区块时间缺失都返回错误，
// 调用方据此报「暂不可用」，不会回退成未修正的官方值。
func (corrector *Corrector) Apply(ctx context.Context, walletAddress string, points []Point) error {
	if corrector == nil {
		return nil
	}
	corrections := corrector.byWallet[strings.ToLower(strings.TrimSpace(walletAddress))]
	if len(corrections) == 0 || len(points) == 0 {
		return nil
	}
	earliest := corrections[0].EffectiveAt.Unix()
	for _, correction := range corrections[1:] {
		earliest = min(earliest, correction.EffectiveAt.Unix())
	}
	valuation, err := corrector.valuationTimes(ctx, points, earliest)
	if err != nil {
		return err
	}
	adjust := make([]*big.Rat, len(points))
	for _, correction := range corrections {
		indexes := make([]int, 0, len(points))
		for index := range points {
			if valuation[index] >= correction.EffectiveAt.Unix() {
				indexes = append(indexes, index)
			}
		}
		if len(indexes) == 0 {
			continue
		}
		prices, err := corrector.pricesAt(ctx, correction.TokenID, indexes, valuation)
		if err != nil {
			return err
		}
		cash, _ := new(big.Rat).SetString(correction.MissingCash.String())
		shares, _ := new(big.Rat).SetString(correction.PhantomShares.String())
		for _, index := range indexes {
			delta := new(big.Rat).Sub(cash, new(big.Rat).Mul(shares, prices[index]))
			if adjust[index] == nil {
				adjust[index] = delta
			} else {
				adjust[index].Add(adjust[index], delta)
			}
		}
	}
	for index, delta := range adjust {
		if delta == nil {
			continue
		}
		base, ok := new(big.Rat).SetString(points[index].TradePnL.String())
		if !ok {
			return fmt.Errorf("wallet PnL point value is invalid")
		}
		points[index].TradePnL = formatRat(base.Add(base, delta))
	}
	return nil
}

// valuationTimes 返回每个点上游实际取值的时刻：有 source_block 时用出块时间，
// 否则用点的时间戳。出块时间按固定锚点计算，与本次查询包含哪些点无关，
// 所以同一个区块在 1 周、1 个月、全部等任何周期里都得到同一个时刻：
//   - 区块所在的锚点段（每 anchorSpacing 个区块一段）已完成：用段两端锚点按区块号插值；
//   - 仍在出块的当前段：直接读该区块的真实出块时间。
//
// 早于 earliest（最早生效时间）的点不需要修正，直接用时间戳；结果不晚于点时间戳
// （上游先取值、后发布）。所有出块时间都永久缓存。
func (corrector *Corrector) valuationTimes(ctx context.Context, points []Point, earliest int64) ([]int64, error) {
	times := make([]int64, len(points))
	blocks := make([]uint64, len(points))
	hasBlocks := false
	for index, point := range points {
		times[index] = point.Timestamp
		if point.SourceBlock == nil || point.Timestamp < earliest {
			continue
		}
		block, err := strconv.ParseUint(*point.SourceBlock, 10, 64)
		if err != nil || block == 0 {
			return nil, fmt.Errorf("wallet PnL source block is invalid")
		}
		blocks[index] = block
		hasBlocks = true
	}
	if !hasBlocks {
		return times, nil
	}
	latest, err := corrector.latestBlock(ctx)
	if err != nil {
		return nil, err
	}
	spacing := corrector.anchorSpacing
	anchored := func(block uint64) (low, high uint64, ok bool) {
		low = block / spacing * spacing
		high = low + spacing
		return low, high, high+anchorFinalityBlocks <= latest
	}
	need := make(map[uint64]struct{})
	for _, block := range blocks {
		if block == 0 {
			continue
		}
		if low, high, ok := anchored(block); ok {
			need[low], need[high] = struct{}{}, struct{}{}
		} else {
			need[block] = struct{}{}
		}
	}
	known, err := corrector.blockTimesFor(ctx, need)
	if err != nil {
		return nil, err
	}
	for index, block := range blocks {
		if block == 0 {
			continue
		}
		low, high, ok := anchored(block)
		if !ok {
			times[index] = known[block]
		} else {
			lowTime, highTime := known[low], known[high]
			if highTime <= lowTime {
				return nil, fmt.Errorf("wallet PnL anchor block times are not increasing")
			}
			offset := new(big.Int).Mul(big.NewInt(highTime-lowTime), new(big.Int).SetUint64(block-low))
			offset.Quo(offset, new(big.Int).SetUint64(spacing))
			times[index] = lowTime + offset.Int64()
		}
		times[index] = min(times[index], points[index].Timestamp)
	}
	return times, nil
}

func (corrector *Corrector) latestBlock(ctx context.Context) (uint64, error) {
	now := corrector.now()
	corrector.mu.Lock()
	if corrector.latest != 0 && now.Sub(corrector.latestAt) < time.Minute {
		latest := corrector.latest
		corrector.mu.Unlock()
		return latest, nil
	}
	corrector.mu.Unlock()
	latest, err := corrector.blocks.ObservedBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("read latest block for wallet PnL corrections: %w", err)
	}
	corrector.mu.Lock()
	corrector.latest, corrector.latestAt = latest, now
	corrector.mu.Unlock()
	return latest, nil
}

// blockTimesFor 并发读取尚未缓存的出块时间（最多 blockFetchParallelism 路）。
func (corrector *Corrector) blockTimesFor(ctx context.Context, need map[uint64]struct{}) (map[uint64]int64, error) {
	result := make(map[uint64]int64, len(need))
	var missing []uint64
	corrector.mu.Lock()
	for block := range need {
		if value, ok := corrector.blockTimes[block]; ok {
			result[block] = value
		} else {
			missing = append(missing, block)
		}
	}
	corrector.mu.Unlock()
	if len(missing) == 0 {
		return result, nil
	}
	type fetched struct {
		block uint64
		time  int64
		err   error
	}
	jobs := make(chan uint64)
	out := make(chan fetched, len(missing))
	var wg sync.WaitGroup
	for range min(blockFetchParallelism, len(missing)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for block := range jobs {
				timestamp, err := corrector.blocks.BlockTimestamp(ctx, block)
				out <- fetched{block: block, time: timestamp.Unix(), err: err}
			}
		}()
	}
	for _, block := range missing {
		jobs <- block
	}
	close(jobs)
	wg.Wait()
	close(out)
	var firstErr error
	corrector.mu.Lock()
	defer corrector.mu.Unlock()
	for item := range out {
		if item.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("read wallet PnL source block time: %w", item.err)
			}
			continue
		}
		if len(corrector.blockTimes) >= maxBlockTimes {
			corrector.blockTimes = make(map[uint64]int64)
		}
		corrector.blockTimes[item.block] = item.time
		result[item.block] = item.time
	}
	return result, firstErr
}

func bucketFor(now, at int64) (bucket int, maxStale int64) {
	age := time.Duration(now-at) * time.Second
	switch {
	case age <= fineBucketMaxAge:
		return fineBucketSeconds, 6 * 3600
	case age <= hourlyBucketMaxAge:
		return hourlyBucketSeconds, 12 * 3600
	default:
		return dailyBucketSeconds, 3 * 86400
	}
}

// pricesAt 取 token 在每个指定点取值时刻的价格（取不晚于该时刻的最近一个桶）。
func (corrector *Corrector) pricesAt(ctx context.Context, token string, indexes []int, valuation []int64) (map[int]*big.Rat, error) {
	now := corrector.now().UTC().Unix()
	type window struct{ low, high int64 }
	windows := make(map[int]*window)
	for _, index := range indexes {
		bucket, _ := bucketFor(now, valuation[index])
		w := windows[bucket]
		if w == nil {
			windows[bucket] = &window{low: valuation[index], high: valuation[index]}
			continue
		}
		w.low = min(w.low, valuation[index])
		w.high = max(w.high, valuation[index])
	}
	series := make(map[int][]polymarketdata.PricePoint, len(windows))
	for bucket, w := range windows {
		points, err := corrector.priceSeries(ctx, token, bucket, w.low-int64(bucket), w.high, now)
		if err != nil {
			return nil, err
		}
		series[bucket] = points
	}
	result := make(map[int]*big.Rat, len(indexes))
	for _, index := range indexes {
		at := valuation[index]
		bucket, maxStale := bucketFor(now, at)
		points := series[bucket]
		position := sort.Search(len(points), func(i int) bool { return points[i].Timestamp > at }) - 1
		if position < 0 || at-points[position].Timestamp > maxStale {
			return nil, fmt.Errorf("wallet PnL correction price is unavailable for token %s at %d", token, at)
		}
		price, ok := new(big.Rat).SetString(points[position].Price.String())
		if !ok {
			return nil, fmt.Errorf("wallet PnL correction price is invalid")
		}
		result[index] = price
	}
	return result, nil
}

// priceSeries 按 15 天对齐分段拉取价格并缓存；完全落在过去的分段永久缓存。
// 未缓存的分段并发拉取（最多 priceFetchParallelism 路），控制首次加载长周期的耗时。
func (corrector *Corrector) priceSeries(ctx context.Context, token string, bucket int, low, high, now int64) ([]polymarketdata.PricePoint, error) {
	const span = int64(polymarketdata.MaxPriceHistorySpanSeconds)
	var starts []int64
	for start := (low / span) * span; start <= high; start += span {
		if min(start+span, now) <= start {
			break
		}
		starts = append(starts, start)
	}
	chunks := make([][]polymarketdata.PricePoint, len(starts))
	errs := make([]error, len(starts))
	limit := make(chan struct{}, priceFetchParallelism)
	var wg sync.WaitGroup
	for index, start := range starts {
		key := fmt.Sprintf("%s|%d|%d", token, bucket, start)
		corrector.mu.Lock()
		cached, ok := corrector.chunks[key]
		corrector.mu.Unlock()
		if ok && (cached.expiresAt.IsZero() || !time.Unix(now, 0).After(cached.expiresAt)) {
			chunks[index] = cached.points
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			limit <- struct{}{}
			defer func() { <-limit }()
			points, err := corrector.prices.PriceHistory(ctx, token, start, min(start+span, now), bucket)
			if err != nil {
				errs[index] = fmt.Errorf("read wallet PnL correction prices: %w", err)
				return
			}
			entry := priceChunk{points: points}
			if start+span > now-2*3600 {
				entry.expiresAt = time.Unix(now, 0).Add(5 * time.Minute)
			}
			corrector.mu.Lock()
			if len(corrector.chunks) >= maxPriceChunks {
				corrector.chunks = make(map[string]priceChunk)
			}
			corrector.chunks[key] = entry
			corrector.mu.Unlock()
			chunks[index] = points
		}()
	}
	wg.Wait()
	var merged []polymarketdata.PricePoint
	for index := range starts {
		if errs[index] != nil {
			return nil, errs[index]
		}
		merged = append(merged, chunks[index]...)
	}
	sort.Slice(merged, func(left, right int) bool { return merged[left].Timestamp < merged[right].Timestamp })
	return merged, nil
}

func formatRat(value *big.Rat) domain.Decimal {
	text := value.FloatString(6)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	}
	if text == "-0" || text == "" {
		text = "0"
	}
	return domain.Decimal(text)
}

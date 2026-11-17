package walletpnl

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

const testWallet = "0x7b14065318fc2f00e2c971bd4cb3770eda7f6d17"

func TestParseCorrections(t *testing.T) {
	corrections, err := ParseCorrections(`[{"wallet_address":"0x7B14065318fc2f00e2c971bd4cb3770eda7f6d17","token_id":"123","effective_at":"2026-08-07T09:51:10Z","phantom_shares":"51.13","missing_cash":"1.73842","note":"伊朗封锁"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(corrections) != 1 || corrections[0].WalletAddress != testWallet || corrections[0].PhantomShares != "51.13" || !corrections[0].EffectiveAt.Equal(time.Date(2026, 8, 7, 9, 51, 10, 0, time.UTC)) {
		t.Fatalf("corrections = %#v", corrections)
	}
	if empty, err := ParseCorrections("  "); err != nil || empty != nil {
		t.Fatalf("empty = %#v, %v", empty, err)
	}
	for name, raw := range map[string]string{
		"bad address":   `[{"wallet_address":"0x12","token_id":"1","effective_at":"2026-08-07T00:00:00Z","phantom_shares":"1","missing_cash":"0"}]`,
		"bad token":     `[{"wallet_address":"` + testWallet + `","token_id":"0xab","effective_at":"2026-08-07T00:00:00Z","phantom_shares":"1","missing_cash":"0"}]`,
		"bad time":      `[{"wallet_address":"` + testWallet + `","token_id":"1","effective_at":"2026-08-07","phantom_shares":"1","missing_cash":"0"}]`,
		"bad shares":    `[{"wallet_address":"` + testWallet + `","token_id":"1","effective_at":"2026-08-07T00:00:00Z","phantom_shares":"x","missing_cash":"0"}]`,
		"unknown field": `[{"wallet_address":"` + testWallet + `","token_id":"1","effective_at":"2026-08-07T00:00:00Z","phantom_shares":"1","missing_cash":"0","extra":1}]`,
		"trailing":      `[] []`,
	} {
		if _, err := ParseCorrections(raw); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

type fakePrices struct {
	mu     sync.Mutex
	calls  int
	points map[string][]polymarketdata.PricePoint
	err    error
}

func (fake *fakePrices) PriceHistory(_ context.Context, token string, start, end int64, bucket int) ([]polymarketdata.PricePoint, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	if fake.err != nil {
		return nil, fake.err
	}
	var out []polymarketdata.PricePoint
	for _, point := range fake.points[token] {
		if point.Timestamp >= start && point.Timestamp <= end {
			out = append(out, point)
		}
	}
	return out, nil
}

type fakeBlocks map[uint64]int64

// latestKey 存放 fakeBlocks 的最新区块号（区块号 0 不会作为真实区块被查询）。
const latestKey = 0

func (fake fakeBlocks) BlockTimestamp(_ context.Context, block uint64) (time.Time, error) {
	value, ok := fake[block]
	if !ok || block == latestKey {
		return time.Time{}, errors.New("unknown block")
	}
	return time.Unix(value, 0), nil
}

func (fake fakeBlocks) ObservedBlock(context.Context) (uint64, error) {
	if latest, ok := fake[latestKey]; ok {
		return uint64(latest), nil
	}
	return 1 << 40, nil
}

func block(value string) *string { return &value }

func TestCorrectorAppliesPhantomPositionsAtBlockTime(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	base := now.Unix() - 3*86400
	prices := &fakePrices{points: map[string][]polymarketdata.PricePoint{
		"11": {{Timestamp: base - 600, Price: "0.5"}, {Timestamp: base + 1200, Price: "0.6"}, {Timestamp: base + 7200, Price: "0.8"}},
		"22": {{Timestamp: base - 600, Price: "0.9"}},
	}}
	// 锚点间隔 100：区块 100 在 base、200 在 base+10000、300 在 base+20000。
	// 区块 112 落在已完成的 [100,200] 段，插值到 base+1200；区块 200 落在 [200,300] 段，取 base+10000。
	blocks := fakeBlocks{latestKey: 5000, 100: base, 200: base + 10000, 300: base + 20000}
	corrector, err := NewCorrector([]Correction{
		{WalletAddress: testWallet, TokenID: "11", EffectiveAt: time.Unix(base-60, 0), PhantomShares: "10", MissingCash: "1"},
		{WalletAddress: testWallet, TokenID: "22", EffectiveAt: time.Unix(base+5000, 0), PhantomShares: "2", MissingCash: "0.5"},
	}, prices, blocks, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	corrector.anchorSpacing = 100
	points := []Point{
		{Timestamp: base - 3600, TradePnL: "-1", SourceBlock: block("10")},  // 早于生效时间：不变，也不参与插值（区块 10 无出块时间）
		{Timestamp: base + 60, TradePnL: "5", SourceBlock: block("100")},    // 价 0.5 → 5+1-5 = 1
		{Timestamp: base + 1300, TradePnL: "5", SourceBlock: block("112")},  // 插值 base+1200，价 0.6 → 0
		{Timestamp: base + 10100, TradePnL: "5", SourceBlock: block("200")}, // 价 0.8 → 5+1-8；第二笔 0.5-1.8 → -3.3
	}
	if err := corrector.Apply(context.Background(), strings.ToUpper(testWallet[:2])+testWallet[2:], points); err != nil {
		t.Fatal(err)
	}
	want := []domain.Decimal{"-1", "1", "0", "-3.3"}
	for index, point := range points {
		if point.TradePnL != want[index] {
			t.Fatalf("point %d = %s, want %s", index, point.TradePnL, want[index])
		}
	}
	calls := prices.calls
	again := []Point{{Timestamp: base + 60, TradePnL: "5", SourceBlock: block("100")}}
	if err := corrector.Apply(context.Background(), testWallet, again); err != nil || again[0].TradePnL != "1" {
		t.Fatalf("again = %#v, %v", again, err)
	}
	if prices.calls != calls {
		t.Fatalf("expected cached prices, calls %d -> %d", calls, prices.calls)
	}
}

func TestCorrectorLeavesOtherWalletsAndFailsClosed(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	prices := &fakePrices{points: map[string][]polymarketdata.PricePoint{}}
	corrector, err := NewCorrector([]Correction{{WalletAddress: testWallet, TokenID: "11", EffectiveAt: time.Unix(1, 0), PhantomShares: "1", MissingCash: "0"}}, prices, fakeBlocks{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	other := []Point{{Timestamp: now.Unix() - 60, TradePnL: "3"}}
	if err := corrector.Apply(context.Background(), "0x0000000000000000000000000000000000000001", other); err != nil || other[0].TradePnL != "3" {
		t.Fatalf("other wallet changed: %#v, %v", other, err)
	}
	missing := []Point{{Timestamp: now.Unix() - 60, TradePnL: "3"}}
	if err := corrector.Apply(context.Background(), testWallet, missing); err == nil {
		t.Fatal("expected missing price error")
	}
	unknownBlock := []Point{{Timestamp: now.Unix() - 60, TradePnL: "3", SourceBlock: block("5")}}
	if err := corrector.Apply(context.Background(), testWallet, unknownBlock); err == nil {
		t.Fatal("expected block time error")
	}
	var none *Corrector
	if err := none.Apply(context.Background(), testWallet, other); err != nil {
		t.Fatal(err)
	}
	if empty, err := NewCorrector(nil, nil, nil, nil); err != nil || empty != nil {
		t.Fatalf("empty corrector = %#v, %v", empty, err)
	}
}

func TestBucketForUsesCoarserBucketsForOlderPoints(t *testing.T) {
	now := int64(1_800_000_000)
	if bucket, _ := bucketFor(now, now-10*86400); bucket != fineBucketSeconds {
		t.Fatalf("recent bucket = %d", bucket)
	}
	if bucket, _ := bucketFor(now, now-70*86400); bucket != hourlyBucketSeconds {
		t.Fatalf("middle bucket = %d", bucket)
	}
	if bucket, _ := bucketFor(now, now-100*86400); bucket != dailyBucketSeconds {
		t.Fatalf("old bucket = %d", bucket)
	}
}

func TestServiceAppliesCorrectionsAndFailsClosed(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0) }
	corrections := []Correction{{WalletAddress: "0x1111111111111111111111111111111111111111", TokenID: "7", EffectiveAt: time.Unix(0, 0), PhantomShares: "2", MissingCash: "0"}}
	prices := &fakePrices{points: map[string][]polymarketdata.PricePoint{"7": {{Timestamp: 1, Price: "0.5"}}}}
	corrector, err := NewCorrector(corrections, prices, fakeBlocks{}, now)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Params{Addresses: fakeAddresses{address: "0x1111111111111111111111111111111111111111"}, Source: &fakeSource{}, Accounts: []string{"wallet-1"}, Corrector: corrector})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Query(context.Background(), Query{ExecutionAccountID: "wallet-1", Interval: "1w", Fidelity: "3h"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 1 || result.Points[0].TradePnL != "-1" || result.Metric != "trade_pnl" {
		t.Fatalf("result = %#v", result)
	}

	failing, err := NewCorrector(corrections, &fakePrices{err: errors.New("down")}, fakeBlocks{}, now)
	if err != nil {
		t.Fatal(err)
	}
	service, err = New(Params{Addresses: fakeAddresses{address: "0x1111111111111111111111111111111111111111"}, Source: &fakeSource{}, Accounts: []string{"wallet-1"}, Corrector: failing})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Query(context.Background(), Query{ExecutionAccountID: "wallet-1", Interval: "1w", Fidelity: "3h"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
}

func TestCorrectorValuationIsIndependentOfQueryRange(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	base := now.Unix() - 5*86400
	prices := &fakePrices{points: map[string][]polymarketdata.PricePoint{"11": {
		{Timestamp: base, Price: "0.5"}, {Timestamp: base + 1500, Price: "0.6"}, {Timestamp: base + 2400, Price: "0.7"}, {Timestamp: base + 9000, Price: "0.9"},
	}}}
	// 最新区块 1300：段 [100,200] 已完成（200+1000 ≤ 1300）；区块 400 所在段 [400,500] 未完成，读真实出块时间。
	blocks := fakeBlocks{latestKey: 1300, 100: base, 200: base + 10000, 400: base + 25000}
	corrector, err := NewCorrector([]Correction{{WalletAddress: testWallet, TokenID: "11", EffectiveAt: time.Unix(base-60, 0), PhantomShares: "10", MissingCash: "0"}}, prices, blocks, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	corrector.anchorSpacing = 100
	week := []Point{{Timestamp: base + 1600, TradePnL: "0", SourceBlock: block("115")}}
	all := []Point{
		{Timestamp: base + 200, TradePnL: "0", SourceBlock: block("101")},
		{Timestamp: base + 1600, TradePnL: "0", SourceBlock: block("115")},
		{Timestamp: base + 26000, TradePnL: "0", SourceBlock: block("400")},
	}
	if err := corrector.Apply(context.Background(), testWallet, week); err != nil {
		t.Fatal(err)
	}
	if err := corrector.Apply(context.Background(), testWallet, all); err != nil {
		t.Fatal(err)
	}
	// 区块 115 → base+1500，价 0.6 → -6；两种查询范围必须一致。
	if week[0].TradePnL != "-6" || all[1].TradePnL != week[0].TradePnL {
		t.Fatalf("week=%s all=%s", week[0].TradePnL, all[1].TradePnL)
	}
	if all[2].TradePnL != "-9" {
		t.Fatalf("current-segment block = %s, want -9", all[2].TradePnL)
	}
}

func TestCorrectorStacksEventsForSameToken(t *testing.T) {
	corrections, err := ParseCorrections(`[
		{"wallet_address":"` + testWallet + `","token_id":"7","effective_at":"2026-08-07T09:51:12Z","phantom_shares":"51.11","missing_cash":"25.0439"},
		{"wallet_address":"` + testWallet + `","token_id":"7","effective_at":"2026-08-07T13:41:37Z","phantom_shares":"-4.99","missing_cash":"-2.5449"}]`)
	if err != nil {
		t.Fatal(err)
	}
	sell := time.Date(2026, 8, 7, 9, 51, 12, 0, time.UTC).Unix()
	buy := time.Date(2026, 8, 7, 13, 41, 37, 0, time.UTC).Unix()
	now := time.Unix(buy+86400, 0)
	prices := &fakePrices{points: map[string][]polymarketdata.PricePoint{"7": {{Timestamp: sell - 60, Price: "0.5"}}}}
	corrector, err := NewCorrector(corrections, prices, fakeBlocks{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	points := []Point{
		{Timestamp: sell - 10, TradePnL: "0"},
		{Timestamp: sell + 600, TradePnL: "0"}, // 卖出后、买回前：25.0439 − 51.11×0.5 = −0.5111
		{Timestamp: buy + 600, TradePnL: "0"},  // 买回后净额：22.499 − 46.12×0.5 = −0.561
	}
	if err := corrector.Apply(context.Background(), testWallet, points); err != nil {
		t.Fatal(err)
	}
	want := []string{"0", "-0.5111", "-0.561"}
	for index, point := range points {
		if point.TradePnL.String() != want[index] {
			t.Fatalf("point %d = %s, want %s", index, point.TradePnL, want[index])
		}
	}
	if _, err := ParseCorrections(`[
		{"wallet_address":"` + testWallet + `","token_id":"7","effective_at":"2026-08-07T09:51:12Z","phantom_shares":"1","missing_cash":"0"},
		{"wallet_address":"` + testWallet + `","token_id":"7","effective_at":"2026-08-07T09:51:12Z","phantom_shares":"2","missing_cash":"0"}]`); err == nil {
		t.Fatal("expected duplicate effective_at error")
	}
}

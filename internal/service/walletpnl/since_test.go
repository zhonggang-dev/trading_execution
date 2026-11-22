package walletpnl

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func TestParseAccountSince(t *testing.T) {
	managed := []string{"wallet-2", "wallet-3", "main"}
	for _, raw := range []string{"", "  ", "{}"} {
		got, err := ParseAccountSince(raw, managed)
		if err != nil || got != nil {
			t.Fatalf("raw %q = %v, %v; want disabled", raw, got, err)
		}
	}
	got, err := ParseAccountSince(`{"wallet-2":"2026-11-20T12:34:14Z","wallet-3":"2026-11-20T20:34:14+08:00"}`, managed)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 11, 20, 12, 34, 14, 0, time.UTC)
	if len(got) != 2 || !got["wallet-2"].Equal(want) || !got["wallet-3"].Equal(want) || got["wallet-3"].Location() != time.UTC {
		t.Fatalf("parsed = %v", got)
	}
	for _, raw := range []string{
		`[]`,
		`"wallet-2"`,
		`{"wallet-2":"2026-11-20"}`,
		`{"wallet-2":"2026-11-20T12:34:14.5Z"}`,
		`{"wallet-2":1763642054}`,
		`{"wallet-9":"2026-11-20T12:34:14Z"}`,
		`{"kalshi:main":"2026-11-20T12:34:14Z"}`,
		`{"":"2026-11-20T12:34:14Z"}`,
		`{" wallet-2":"2026-11-20T12:34:14Z"}`,
		`{"wallet-2":"2026-11-20T12:34:14Z","wallet-2":"2026-11-21T00:00:00Z"}`,
		`{"wallet-2":"2026-11-20T12:34:14Z"} {}`,
		`{"wallet-2":"2026-11-20T12:34:14Z"`,
	} {
		if _, err := ParseAccountSince(raw, managed); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func sincePoints(values ...any) []Point {
	points := make([]Point, 0, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		points = append(points, Point{Timestamp: int64(values[index].(int)), TradePnL: domain.Decimal(values[index+1].(string))})
	}
	return points
}

func pointsText(points []Point) string {
	parts := make([]string, 0, len(points))
	for _, point := range points {
		parts = append(parts, time.Unix(point.Timestamp, 0).UTC().Format("15:04")+"="+point.TradePnL.String())
	}
	return strings.Join(parts, ",")
}

func TestRebaseSince(t *testing.T) {
	since := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	at := int(since.Unix())
	hour := 3600
	cases := []struct {
		name   string
		points []Point
		want   string
	}{
		{"跨越起点：以起点前最后一点为基数并补 0 点", sincePoints(at-2*hour, "-100.5", at-hour, "-88.25", at+hour, "-80", at+2*hour, "-90.125"),
			"12:00=0,13:00=8.25,14:00=-1.875"},
		{"起点处正好有点：该点变为 0，不重复补点", sincePoints(at-hour, "5", at, "7.1", at+hour, "9"),
			"12:00=0,13:00=1.9"},
		{"全部在起点之后：原样返回", sincePoints(at+hour, "-88.21", at+2*hour, "-88.27"),
			"13:00=-88.21,14:00=-88.27"},
		{"起点处是首点：以它为基数，不补点", sincePoints(at, "3", at+hour, "4"),
			"12:00=0,13:00=1"},
		{"全部在起点之前：只剩起点处的 0 点", sincePoints(at-2*hour, "1", at-hour, "2"),
			"12:00=0"},
		{"空序列：原样返回", nil, ""},
	}
	for _, tc := range cases {
		got, err := rebaseSince(tc.points, since)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if text := pointsText(got); text != tc.want {
			t.Fatalf("%s: got %s, want %s", tc.name, text, tc.want)
		}
	}
	// 补出来的 0 点没有 source_block。
	got, _ := rebaseSince(sincePoints(at-hour, "1", at+hour, "2"), since)
	if got[0].SourceBlock != nil || got[0].Timestamp != int64(at) {
		t.Fatalf("zero point = %#v", got[0])
	}
	if _, err := rebaseSince(sincePoints(at+hour, "1", at-hour, "2"), since); err == nil {
		t.Fatal("accepted unordered points")
	}
}

type sinceSource struct {
	mu     sync.Mutex
	calls  int
	points []polymarketdata.PnLPoint
}

func (source *sinceSource) Series(context.Context, string, string, string) (polymarketdata.PnLSeries, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.calls++
	return polymarketdata.PnLSeries{Points: append([]polymarketdata.PnLPoint(nil), source.points...)}, nil
}

func TestServiceAppliesAccountSinceWithoutMutatingSharedCache(t *testing.T) {
	since := time.Date(2026, 11, 20, 12, 34, 14, 0, time.UTC)
	at := since.Unix()
	source := &sinceSource{points: []polymarketdata.PnLPoint{
		{Timestamp: at - 3600, TradePnL: "-12.5"},
		{Timestamp: at + 3600, TradePnL: "-10"},
	}}
	// main 与 wallet-2 共用同一地址，缓存按地址共享；只有 wallet-2 配置了起点。
	address := "0x1111111111111111111111111111111111111111"
	service, err := New(Params{
		Addresses: fakeAddresses{byID: map[string]string{"main": address, "wallet-2": address}},
		Source:    source, Accounts: []string{"main", "wallet-2"}, TTL: time.Minute,
		AccountSince: map[string]time.Time{"wallet-2": since},
	})
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		rebased, err := service.Query(context.Background(), Query{ExecutionAccountID: "wallet-2", Interval: "all", Fidelity: "1d"})
		if err != nil {
			t.Fatal(err)
		}
		if len(rebased.Points) != 2 || rebased.Points[0].Timestamp != at || rebased.Points[0].TradePnL != "0" ||
			rebased.Points[1].TradePnL != "2.5" || rebased.ExecutionAccountID != "wallet-2" {
			t.Fatalf("round %d rebased = %#v", round, rebased.Points)
		}
		original, err := service.Query(context.Background(), Query{ExecutionAccountID: "main", Interval: "all", Fidelity: "1d"})
		if err != nil {
			t.Fatal(err)
		}
		if len(original.Points) != 2 || original.Points[0].TradePnL != "-12.5" || original.Points[1].TradePnL != "-10" {
			t.Fatalf("round %d cache mutated: %#v", round, original.Points)
		}
	}
	source.mu.Lock()
	calls := source.calls
	source.mu.Unlock()
	if calls != 1 {
		t.Fatalf("source calls = %d, want 1 (cached by address)", calls)
	}
}

func TestServiceRejectsAccountSinceOutsideManagedSet(t *testing.T) {
	_, err := New(Params{
		Addresses: fakeAddresses{address: "0x1111111111111111111111111111111111111111"},
		Source:    &sinceSource{}, Accounts: []string{"main"},
		AccountSince: map[string]time.Time{"wallet-9": time.Now()},
	})
	if err == nil {
		t.Fatal("accepted unmanaged account since")
	}
}

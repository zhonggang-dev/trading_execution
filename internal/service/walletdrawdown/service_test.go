package walletdrawdown

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/adapter/polymarketdata"
	"github.com/UniPat-AI/trading_execution/internal/domain"
)

type memStore struct {
	samples []Sample
}

func (m *memStore) WalletAddress(context.Context, string) (string, error) { return "0xwallet", nil }
func (m *memStore) LoadSample(_ context.Context, account string, slot time.Time) (Sample, bool, error) {
	for _, s := range m.samples {
		if s.AccountID == account && s.SlotAt.Equal(slot) {
			return s, true, nil
		}
	}
	return Sample{}, false, nil
}
func (m *memStore) WindowSamples(_ context.Context, account string, from, before time.Time) ([]Sample, error) {
	out := []Sample{}
	for _, s := range m.samples {
		if s.AccountID == account && !s.SampledAt.Before(from) && s.SlotAt.Before(before) {
			out = append(out, s)
		}
	}
	return out, nil
}
func (m *memStore) LastNotifiedDrawdown(_ context.Context, account string, before time.Time) (*big.Rat, error) {
	var clear time.Time
	for _, s := range m.samples {
		if s.AccountID == account && s.SlotAt.Before(before) && !s.Exceeded && s.SlotAt.After(clear) {
			clear = s.SlotAt
		}
	}
	var last *Sample
	for i := range m.samples {
		s := &m.samples[i]
		if s.AccountID == account && s.SlotAt.Before(before) && s.SlotAt.After(clear) && s.Notified && (last == nil || s.SlotAt.After(last.SlotAt)) {
			last = s
		}
	}
	if last == nil {
		return nil, nil
	}
	return ratOf(*last.Drawdown)
}
func (m *memStore) InsertSample(_ context.Context, s Sample) (bool, error) {
	if _, found, _ := m.LoadSample(context.Background(), s.AccountID, s.SlotAt); found {
		return false, nil
	}
	m.samples = append(m.samples, s)
	return true, nil
}
func (m *memStore) MarkNotified(_ context.Context, account string, slot time.Time, notified bool, _ string) error {
	for i := range m.samples {
		if m.samples[i].AccountID == account && m.samples[i].SlotAt.Equal(slot) {
			m.samples[i].Notified = notified
		}
	}
	return nil
}

type fakeSources struct {
	cash      string
	marks     []polymarketdata.PositionMark
	chain     map[string]domain.Decimal
	chainRead []string
}

func (f *fakeSources) Cash(context.Context, string) (domain.ExternalBalance, error) {
	return domain.ExternalBalance{Asset: "pUSD", Amount: domain.Decimal(f.cash), BlockNumber: 7}, nil
}
func (f *fakeSources) Positions(context.Context, string) ([]polymarketdata.PositionMark, error) {
	return f.marks, nil
}
func (f *fakeSources) ChainShares(_ context.Context, _ string, token string) (domain.Decimal, error) {
	f.chainRead = append(f.chainRead, token)
	return f.chain[token], nil
}

type fakeNotifier struct {
	cards []map[string]any
	err   error
}

func (f *fakeNotifier) SendCard(_ context.Context, card map[string]any, _ time.Time) error {
	if f.err != nil {
		return f.err
	}
	f.cards = append(f.cards, card)
	return nil
}

func testConfig() Config {
	return Config{AccountID: "main", Threshold: big.NewRat(50, 1), RenotifyStep: big.NewRat(20, 1), Window: time.Hour, Slot: 10 * time.Minute}
}

func mark(token, title, shares, price string) polymarketdata.PositionMark {
	return polymarketdata.PositionMark{TokenID: token, ConditionID: "c-" + token, Title: title, Outcome: "No", Shares: domain.Decimal(shares), CurrentPrice: domain.Decimal(price)}
}

var base = time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)

func runAt(t *testing.T, store *memStore, src *fakeSources, n *fakeNotifier, at time.Time) Report {
	t.Helper()
	report, err := Run(context.Background(), testConfig(), store, src, n, at, false)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func TestFirstSampleOnlyRecords(t *testing.T) {
	store, n := &memStore{}, &fakeNotifier{}
	src := &fakeSources{cash: "100", marks: []polymarketdata.PositionMark{mark("t1", "A", "100", "0.5")}}
	report := runAt(t, store, src, n, base.Add(30*time.Second))
	s := report.Sample
	if s.Equity != "150.000000" || s.Drawdown != nil || s.Exceeded || len(n.cards) != 0 || !s.SlotAt.Equal(base) {
		t.Fatalf("sample = %#v", s)
	}
}

func TestDrawdownUsesWindowPeakAndNotifiesOnceThenOnStep(t *testing.T) {
	store, n := &memStore{}, &fakeNotifier{}
	src := &fakeSources{cash: "100", marks: []polymarketdata.PositionMark{mark("t1", "APEC Ukraine", "100", "0.90")}}
	runAt(t, store, src, n, base) // equity 190
	src.marks[0].CurrentPrice = "1.10"
	runAt(t, store, src, n, base.Add(10*time.Minute)) // 210 peak
	src.marks[0].CurrentPrice = "0.50"
	r := runAt(t, store, src, n, base.Add(20*time.Minute)) // 150: drawdown 60
	if *r.Sample.Drawdown != "60.000000" || !r.Sample.Exceeded || r.Notify != "sent" || len(n.cards) != 1 {
		t.Fatalf("report = %#v", r)
	}
	if !r.Sample.PeakAt.Equal(base.Add(10 * time.Minute)) {
		t.Fatalf("peak at = %v", r.Sample.PeakAt)
	}
	card := n.cards[0]
	text := flatten(card)
	if !strings.Contains(text, "回撤 60.00 美元") || !strings.Contains(text, "APEC Ukraine") || !strings.Contains(text, "-60.00") {
		t.Fatalf("card = %s", text)
	}
	src.marks[0].CurrentPrice = "0.40"
	r = runAt(t, store, src, n, base.Add(30*time.Minute)) // 140: drawdown 70 < 60+20
	if r.Notify != "suppressed_same_episode" || len(n.cards) != 1 {
		t.Fatalf("second = %#v", r)
	}
	src.marks[0].CurrentPrice = "0.29"
	r = runAt(t, store, src, n, base.Add(40*time.Minute)) // 129: drawdown 81 >= 80
	if r.Notify != "sent" || len(n.cards) != 2 || !strings.Contains(flatten(n.cards[1]), "继续扩大") {
		t.Fatalf("third = %#v", r)
	}
}

func TestEpisodeEndsWhenBackUnderThreshold(t *testing.T) {
	store, n := &memStore{}, &fakeNotifier{}
	src := &fakeSources{cash: "0", marks: []polymarketdata.PositionMark{mark("t1", "A", "100", "1")}}
	runAt(t, store, src, n, base)
	src.marks[0].CurrentPrice = "0.4"
	runAt(t, store, src, n, base.Add(10*time.Minute)) // drawdown 60, sent
	src.marks[0].CurrentPrice = "0.9"
	runAt(t, store, src, n, base.Add(20*time.Minute)) // drawdown 10, episode ends
	src.marks[0].CurrentPrice = "0.35"
	r := runAt(t, store, src, n, base.Add(30*time.Minute)) // drawdown 65 again: new episode
	if r.Notify != "sent" || len(n.cards) != 2 {
		t.Fatalf("report = %#v cards=%d", r, len(n.cards))
	}
}

func TestPeakOutsideWindowIsIgnored(t *testing.T) {
	store, n := &memStore{}, &fakeNotifier{}
	src := &fakeSources{cash: "0", marks: []polymarketdata.PositionMark{mark("t1", "A", "100", "1")}}
	runAt(t, store, src, n, base) // 100
	src.marks[0].CurrentPrice = "0.6"
	runAt(t, store, src, n, base.Add(50*time.Minute)) // 60: drawdown 40
	src.marks[0].CurrentPrice = "0.55"
	r := runAt(t, store, src, n, base.Add(70*time.Minute)) // window starts 10:10, peak is 60 -> drawdown 5
	if *r.Sample.Drawdown != "5.000000" || r.Sample.Exceeded {
		t.Fatalf("sample = %#v", r.Sample)
	}
}

func TestRedeemableWinnerUsesChainShares(t *testing.T) {
	store, n := &memStore{}, &fakeNotifier{}
	winner := mark("w", "Settled", "10", "1")
	winner.Redeemable = true
	loser := mark("l", "Lost", "8", "0")
	loser.Redeemable = true
	src := &fakeSources{cash: "50", marks: []polymarketdata.PositionMark{winner, loser}, chain: map[string]domain.Decimal{"w": "0"}}
	r := runAt(t, store, src, n, base)
	// Already redeemed on chain: the 10 pUSD is in cash, not double counted.
	if r.Sample.Equity != "50.000000" || r.Sample.PositionsValue != "0.000000" || len(src.chainRead) != 1 || src.chainRead[0] != "w" {
		t.Fatalf("sample = %#v reads=%v", r.Sample, src.chainRead)
	}
}

func TestRerunSameSlotDoesNotResampleAndRetriesFailedDelivery(t *testing.T) {
	store, n := &memStore{}, &fakeNotifier{}
	src := &fakeSources{cash: "0", marks: []polymarketdata.PositionMark{mark("t1", "A", "100", "1")}}
	runAt(t, store, src, n, base)
	src.marks[0].CurrentPrice = "0.3"
	n.err = errors.New("feishu down")
	if _, err := Run(context.Background(), testConfig(), store, src, n, base.Add(10*time.Minute), false); err == nil {
		t.Fatal("want delivery error")
	}
	n.err = nil
	src.marks[0].CurrentPrice = "0.99" // a rerun must not resample
	r := runAt(t, store, src, n, base.Add(12*time.Minute))
	if r.Outcome != "slot_already_sampled" || r.Notify != "sent" || len(n.cards) != 1 || len(store.samples) != 2 {
		t.Fatalf("report = %#v samples=%d", r, len(store.samples))
	}
	r = runAt(t, store, src, n, base.Add(14*time.Minute))
	if r.Notify != "none" || len(n.cards) != 1 {
		t.Fatalf("third = %#v", r)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	store, n := &memStore{}, &fakeNotifier{}
	src := &fakeSources{cash: "1", marks: nil}
	r, err := Run(context.Background(), testConfig(), store, src, n, base, true)
	if err != nil || r.Outcome != "dry_run" || len(store.samples) != 0 {
		t.Fatalf("report=%#v err=%v", r, err)
	}
}

func TestConfigValidation(t *testing.T) {
	c := testConfig()
	c.Window = 5 * time.Minute
	if c.Validate() == nil {
		t.Fatal("window shorter than slot must fail")
	}
	c = testConfig()
	c.Threshold = big.NewRat(0, 1)
	if c.Validate() == nil {
		t.Fatal("zero threshold must fail")
	}
}

func flatten(v any) string {
	var b strings.Builder
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case map[string]any:
			for _, value := range t {
				walk(value)
			}
		case []any:
			for _, value := range t {
				walk(value)
			}
		case string:
			b.WriteString(t + "\n")
		}
	}
	walk(v)
	return b.String()
}

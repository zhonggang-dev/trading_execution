package accountpricehistory

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/service/consolev2"
)

func requirementLot(account, id, token, qty, remaining string, at time.Time, flows ...consolev2.CohortReplayDisposition) consolev2.CohortReplayLot {
	return consolev2.CohortReplayLot{LotID: id, OriginalShares: qty, RemainingShares: remaining, OpenedAt: at, Dispositions: flows, Asset: consolev2.CoreAssetFact{PortfolioHolding: consolev2.PortfolioHolding{ExecutionAccountID: account, MarketSource: "POLYMARKET", MarketID: token, ConditionID: "condition-" + token, TokenID: token}}}
}
func requirementFacts(now time.Time, groups map[string][]consolev2.CohortReplayLot) consolev2.CohortMetricFacts {
	f := consolev2.CohortMetricFacts{CoreMetricFacts: consolev2.CoreMetricFacts{ObservedAt: now}, Replay: map[string]consolev2.CohortReplayFact{}}
	for id, lots := range groups {
		f.Accounts = append(f.Accounts, consolev2.CoreAccountFact{Metrics: consolev2.AccountPerformance{ExecutionAccountID: id, MarketSource: "POLYMARKET"}})
		f.Replay[id] = consolev2.CohortReplayFact{Lots: lots}
	}
	return f
}
func requiredFor(windows []Window, token string) []time.Time {
	result := []time.Time{}
	for _, w := range windows {
		if w.Asset.TokenID == token {
			result = append(result, RequiredTimes(w)...)
		}
	}
	return result
}

func TestRequirementsIncludeOtherAssetAndCurrentHourCashEvents(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	bOpen, bClose, cOpen := at.Add(65*time.Minute), at.Add(70*time.Minute), at.Add(125*time.Minute)
	now := at.Add(137 * time.Minute)
	facts := requirementFacts(now, map[string][]consolev2.CohortReplayLot{"pm": {
		requirementLot("pm", "a", "a", "1", "1", at),
		requirementLot("pm", "b", "b", "1", "0", bOpen, consolev2.CohortReplayDisposition{ID: "sold", At: bClose, Shares: "1"}),
		requirementLot("pm", "c", "c", "1", "1", cOpen),
	}})
	windows, err := BuildRequirements(context.Background(), facts)
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Time{at, at.Add(time.Hour), bOpen, bClose, at.Add(2 * time.Hour), cOpen}
	if got := requiredFor(windows, "a"); !reflect.DeepEqual(got, want) {
		t.Fatal("portfolio cash events or current partial hour omitted", got, want)
	}
	if got := requiredFor(windows, "b"); !reflect.DeepEqual(got, []time.Time{bOpen}) {
		t.Fatal("closed asset needs no price after its disposition", got)
	}
	for _, w := range windows {
		for _, target := range w.RequiredAt {
			if !target.Before(now) {
				t.Fatal("latest current-price endpoint became a historical requirement", target)
			}
		}
	}
}

func TestRequirementsKeepSameAssetInternalEventsAndAtomicNetting(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	inside, paired := at.Add(5*time.Minute), at.Add(10*time.Minute)
	facts := requirementFacts(at.Add(17*time.Minute), map[string][]consolev2.CohortReplayLot{"pm": {
		requirementLot("pm", "a-first", "a", "1", "1", at),
		requirementLot("pm", "a-add", "a", "1", "0.5", inside, consolev2.CohortReplayDisposition{ID: "partial", At: paired, Shares: "0.5"}),
		requirementLot("pm", "instant", "no", "1", "0", paired, consolev2.CohortReplayDisposition{ID: "paired", At: paired, Shares: "1"}),
	}})
	windows, err := BuildRequirements(context.Background(), facts)
	if err != nil || !reflect.DeepEqual(requiredFor(windows, "a"), []time.Time{at, inside, paired}) {
		t.Fatal(windows, err)
	}
	if len(requiredFor(windows, "no")) != 0 {
		t.Fatal("intermediate immediately netted holdings required a false mark", windows)
	}
}

func TestRequirementsUnionSharedPricesButNotAccountCashClocks(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	one, two := at.Add(5*time.Minute), at.Add(7*time.Minute)
	facts := requirementFacts(at.Add(17*time.Minute), map[string][]consolev2.CohortReplayLot{
		"one": {requirementLot("one", "one-a", "shared", "1", "1", at), requirementLot("one", "one-b", "b", "1", "1", one)},
		"two": {requirementLot("two", "two-a", "shared", "1", "1", at), requirementLot("two", "two-c", "c", "1", "1", two)},
	})
	windows, err := BuildRequirements(context.Background(), facts)
	if err != nil || !reflect.DeepEqual(requiredFor(windows, "shared"), []time.Time{at, one, two}) {
		t.Fatal(windows, err)
	}
	if got := requiredFor(windows, "b"); !reflect.DeepEqual(got, []time.Time{one}) {
		t.Fatal("unrelated account clock contaminated asset requirements", got)
	}
}

func TestRequirementsExcludeFlatIntervalsAndRefuseIncompleteEvidence(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	facts := requirementFacts(at.Add(3*time.Hour), map[string][]consolev2.CohortReplayLot{"pm": {
		requirementLot("pm", "a-old", "a", "1", "0", at, consolev2.CohortReplayDisposition{ID: "sold", At: at.Add(30 * time.Minute), Shares: "1"}),
		requirementLot("pm", "a-new", "a", "1", "1", at.Add(2*time.Hour)),
		requirementLot("pm", "other", "b", "1", "1", at.Add(time.Hour)),
	}})
	windows, err := BuildRequirements(context.Background(), facts)
	if err != nil || !reflect.DeepEqual(requiredFor(windows, "a"), []time.Time{at, at.Add(2 * time.Hour)}) {
		t.Fatal("unheld interval was sampled", windows, err)
	}
	facts.UnavailableAccounts = []consolev2.CohortUnavailableAccount{{ExecutionAccountID: "pm"}}
	if _, err = BuildRequirements(context.Background(), facts); err == nil {
		t.Fatal("partial account was used as complete requirements")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	facts.UnavailableAccounts = nil
	if _, err = BuildRequirements(ctx, facts); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel not honored", err)
	}
}

func TestRequiredTimesExplicitSetDoesNotInventClippingBoundaries(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	w := Window{From: at, To: at.Add(time.Hour), RequiredAt: []time.Time{at.Add(7 * time.Minute), at.Add(5 * time.Minute), at.Add(7 * time.Minute), at.Add(2 * time.Hour)}}
	if got := RequiredTimes(w); !reflect.DeepEqual(got, []time.Time{at.Add(5 * time.Minute), at.Add(7 * time.Minute)}) {
		t.Fatal(got)
	}
}

package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const pnlTestUser = "11111111-2222-3333-4444-555555555555"
const pnlValid = `{"points":[{"ts":"2026-11-05T06:00:00Z","cumulative_pnl":"-1.123456789","portfolio_value":"99.876543211"},{"ts":"2026-11-05T07:00:00.123Z","cumulative_pnl":"0","portfolio_value":"101.00"}],"period_interval":60,"processed_through":"2026-11-05T06:40:00Z"}`

func pnlTestSource(t *testing.T, handler http.HandlerFunc) *PnLSource {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := testClient(t, server.URL, "key", testPrivateKey(t), false)
	client.now = func() time.Time { return time.Date(2026, 11, 5, 7, 0, 1, 0, time.UTC) }
	source, err := NewPnLSource(client, pnlTestUser)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func TestPnLSourceSignsStrictReadAndRetainsProcessedThrough(t *testing.T) {
	var source *PnLSource
	source = pnlTestSource(t, func(w http.ResponseWriter, r *http.Request) {
		verifySignature(t, r, "key", &source.client.privateKey.PublicKey)
		if r.Method != "GET" || r.URL.Path != "/v1/users/"+pnlTestUser+"/portfolio/pnl_history" || r.URL.Query().Get("subaccount_number") != "0" || r.URL.Query().Get("range") != "1w" || len(r.URL.Query()) != 2 {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		fmt.Fprint(w, pnlValid)
	})
	series, err := source.Series(context.Background(), "1w")
	if err != nil {
		t.Fatal(err)
	}
	if len(series.Points) != 2 || series.Points[0].CumulativePnL != "-1.123456789" || series.Points[1].Timestamp != 1793862000 || series.PeriodIntervalMinutes != 60 || series.ProcessedThrough.Format(time.RFC3339) != "2026-11-05T06:40:00Z" {
		t.Fatalf("series=%+v", series)
	}
	if source.client.liveTradingEnabled {
		t.Fatal("read source enabled writes")
	}
}

func TestPnLSourceRejectsInvalidResponseAndQuery(t *testing.T) {
	cases := map[string]string{
		"missing": `{}`, "null_points": `{"points":null,"period_interval":60,"processed_through":"2026-11-05T06:40:00Z"}`,
		"point_null":          strings.Replace(pnlValid, `"-1.123456789"`, `null`, 1),
		"point_numeric":       strings.Replace(pnlValid, `"-1.123456789"`, `1`, 1),
		"bad_decimal":         strings.Replace(pnlValid, `"-1.123456789"`, `"1/2"`, 1),
		"missing_point_field": strings.Replace(pnlValid, `"cumulative_pnl":"-1.123456789",`, "", 1),
		"future_point":        strings.Replace(pnlValid, `06:00:00Z`, `07:02:00Z`, 1),
		"bad_processed":       strings.Replace(pnlValid, `06:40:00Z`, `07:02:00Z`, 1),
		"bad_interval":        strings.Replace(pnlValid, `"period_interval":60`, `"period_interval":0`, 1),
		"negative_value":      strings.Replace(pnlValid, `"99.876543211"`, `"-99"`, 1),
		"trailing":            pnlValid + ` {}`,
		"conflict":            strings.Replace(pnlValid, `07:00:00.123Z`, `06:00:00Z`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := pnlTestSource(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
			if _, err := s.Series(context.Background(), "1w"); err == nil {
				t.Fatal("accepted invalid series")
			}
		})
	}
	var calls atomic.Int32
	s := pnlTestSource(t, func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); fmt.Fprint(w, pnlValid) })
	for _, interval := range []string{"", "custom", "ytd", "1h", "1w&subaccount_number=2"} {
		if _, err := s.Series(context.Background(), interval); err == nil {
			t.Fatalf("accepted %q", interval)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid range reached source")
	}
	for _, id := range []string{"", "../secret", "user?x=1"} {
		if _, err := NewPnLSource(s.client, id); err == nil {
			t.Fatalf("accepted user %q", id)
		}
	}
}

func TestPnLSourceEmptySortedDuplicateAndBudgets(t *testing.T) {
	for _, count := range []int{0, 2, 10001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var data map[string]any
			if err := json.Unmarshal([]byte(pnlValid), &data); err != nil {
				t.Fatal(err)
			}
			base := data["points"].([]any)
			points := make([]any, count)
			for i := range points {
				points[i] = base[0]
			}
			data["points"] = points
			body, _ := json.Marshal(data)
			s := pnlTestSource(t, func(w http.ResponseWriter, _ *http.Request) { w.Write(body) })
			got, err := s.Series(context.Background(), "1w")
			if count > 10000 {
				if err == nil {
					t.Fatal("accepted excessive points")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if count > 0 {
				want = 1
			}
			if len(got.Points) != want || got.Points == nil {
				t.Fatalf("points=%v", got.Points)
			}
		})
	}
	s := pnlTestSource(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, strings.Repeat(" ", maxAuthenticatedResponseBytes+1))
	})
	if _, err := s.Series(context.Background(), "1d"); err == nil {
		t.Fatal("accepted excessive body")
	}
}

func TestPnLSourceNeverFollowsRedirectOrIgnoresCancellation(t *testing.T) {
	var leaked atomic.Bool
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { leaked.Store(true); fmt.Fprint(w, pnlValid) }))
	defer dest.Close()
	s := pnlTestSource(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, dest.URL, 302) })
	if _, err := s.Series(context.Background(), "1w"); err == nil || leaked.Load() {
		t.Fatal("redirect followed or accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Series(ctx, "1w"); err == nil {
		t.Fatal("canceled request accepted")
	}
}

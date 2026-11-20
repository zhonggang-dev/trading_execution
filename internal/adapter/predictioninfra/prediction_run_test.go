package predictioninfra

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

func predictionRunSnapshotBody(runs ...string) string {
	predictions := make([]string, 0, len(runs))
	for index, run := range runs {
		field := ""
		if run != "" {
			field = `,"prediction_run":` + run
		}
		predictions = append(predictions, fmt.Sprintf(`{
			"prediction_id":"pred-%d","source_job_id":"job-%d","sandbox_id":"sandbox-1",
			"market_id":"market-1","condition_id":"condition-1","question":"Will it happen?",
			"domains":["World"],"neg_risk":false,
			"outcomes":[
				{"index":0,"name":"Yes","token_id":"yes-token","probability":0.7},
				{"index":1,"name":"No","token_id":"no-token","probability":0.3}
			],
			"prediction_as_of":"2026-08-18T04:00:00Z","completed_at":"2026-08-18T04:10:00Z",
			"available_at":"2026-08-18T04:10:01Z","model":{"name":"test"}%s
		}`, index, index, field))
	}
	return `{"code":"LIVE_PREDICTION_SNAPSHOT_FOUND","data":{
		"schema_version":"prediction.live_snapshot.v1","snapshot_id":"predsnap-test",
		"decision_at":"2026-08-18T04:20:00Z","completed_after":"2026-08-18T01:20:00Z",
		"generated_at":"2026-08-18T04:20:15Z","predictions":[` + strings.Join(predictions, ",") + `]}}`
}

func snapshotFromBody(t *testing.T, body string) (domain.PredictionSnapshot, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(body))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "test-token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return client.Snapshot(t.Context(), time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC), 3*time.Hour)
}

// TestSnapshotReadsPredictionRun reads prediction_run per record; a missing
// field or 0 (a producer released before the field existed) is the first run.
func TestSnapshotReadsPredictionRun(t *testing.T) {
	snapshot, err := snapshotFromBody(t, predictionRunSnapshotBody("", "0", "2", "3"))
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	want := []int{1, 1, 2, 3}
	for index, prediction := range snapshot.Predictions {
		if prediction.PredictionRun != want[index] {
			t.Fatalf("prediction %d run = %d, want %d", index, prediction.PredictionRun, want[index])
		}
	}
	// The run never reaches the strategy payload.
	payload, err := json.Marshal(snapshot.Predictions[2])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "prediction_run") {
		t.Fatalf("strategy prediction payload exposes prediction_run: %s", payload)
	}
}

func TestSnapshotRejectsNegativePredictionRun(t *testing.T) {
	if _, err := snapshotFromBody(t, predictionRunSnapshotBody("1", "-1")); err == nil ||
		!strings.Contains(err.Error(), "prediction_run") {
		t.Fatalf("Snapshot() error = %v, want invalid prediction_run", err)
	}
}

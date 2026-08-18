package predictioninfra

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestSnapshotReadsPointInTimeResponse 验证 Snapshot Reads Point In Time Response 场景下的行为。
func TestSnapshotReadsPointInTimeResponse(t *testing.T) {
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		if request.URL.Query().Get("decision_at") != "2026-08-18T04:20:00Z" || request.URL.Query().Get("lookback_seconds") != "10800" {
			t.Fatalf("query = %q, want exact decision boundary", request.URL.RawQuery)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{
			"code":"LIVE_PREDICTION_SNAPSHOT_FOUND",
			"data":{
				"schema_version":"prediction.live_snapshot.v1",
				"snapshot_id":"predsnap-test",
				"decision_at":"2026-08-18T04:20:00Z",
				"completed_after":"2026-08-18T01:20:00Z",
				"generated_at":"2026-08-18T04:20:15Z",
				"predictions":[{
					"prediction_id":"pred-1",
					"source_job_id":"job-1",
					"sandbox_id":"sandbox-1",
					"market_id":"market-1",
					"condition_id":"condition-1",
					"question":"Will it happen?",
					"domains":["World/Geopolitics"],
					"neg_risk":false,
					"outcomes":[
						{"index":0,"name":"Yes","token_id":"yes-token","probability":0.7},
						{"index":1,"name":"No","token_id":"no-token","probability":0.3}
					],
					"prediction_as_of":"2026-08-18T04:00:00Z",
					"completed_at":"2026-08-18T04:10:00Z",
					"available_at":"2026-08-18T04:10:01Z",
					"model":{"name":"test"}
				}]
			}
		}`))
	}))
	defer server.Close()

	client, err := New(Params{BaseURL: server.URL, BearerToken: "test-token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	snapshot, err := client.Snapshot(t.Context(), decisionAt, 3*time.Hour)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.SnapshotID != "predsnap-test" || len(snapshot.Predictions) != 1 {
		t.Fatalf("snapshot = %#v, want one prediction", snapshot)
	}
}

// TestSnapshotRejectsMismatchedDecisionBoundary 验证 Snapshot Rejects Mismatched Decision Boundary 场景下的行为。
func TestSnapshotRejectsMismatchedDecisionBoundary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{
			"code":"LIVE_PREDICTION_SNAPSHOT_FOUND",
			"data":{
				"schema_version":"prediction.live_snapshot.v1",
				"snapshot_id":"predsnap-test",
				"decision_at":"2026-08-18T04:30:00Z",
				"completed_after":"2026-08-18T01:30:00Z",
				"generated_at":"2026-08-18T04:30:01Z",
				"predictions":[]
			}
		}`))
	}))
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "test-token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	decisionAt := time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)
	if _, err := client.Snapshot(t.Context(), decisionAt, 3*time.Hour); err == nil {
		t.Fatal("Snapshot() error = nil, want mismatched decision boundary error")
	}
}

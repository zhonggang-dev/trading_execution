package predictioninfra

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/port"
)

var availabilityDecisionAt = time.Date(2026, 8, 18, 4, 20, 0, 0, time.UTC)

func snapshotWithHandler(t *testing.T, handler http.HandlerFunc) error {
	t.Helper()
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := New(Params{BaseURL: server.URL, BearerToken: "test-token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = client.Snapshot(t.Context(), availabilityDecisionAt, 3*time.Hour)
	return err
}

// TestSnapshotMarksRateLimitAndServerErrorsUnavailable keeps the exit-only
// degradation limited to availability failures: 429 and 5xx are unavailable,
// 401/403 and every other 4xx stay hard errors.
func TestSnapshotMarksRateLimitAndServerErrorsUnavailable(t *testing.T) {
	cases := map[int]bool{
		http.StatusTooManyRequests:     true,
		http.StatusInternalServerError: true,
		http.StatusBadGateway:          true,
		http.StatusServiceUnavailable:  true,
		http.StatusGatewayTimeout:      true,
		http.StatusUnauthorized:        false,
		http.StatusForbidden:           false,
		http.StatusBadRequest:          false,
		http.StatusNotFound:            false,
	}
	for status, unavailable := range cases {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			err := snapshotWithHandler(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(status)
				_, _ = writer.Write([]byte(`{"code":"FAILED","message":"failure"}`))
			})
			if err == nil {
				t.Fatal("Snapshot() error = nil")
			}
			if got := errors.Is(err, port.ErrPredictionSourceUnavailable); got != unavailable {
				t.Fatalf("errors.Is(unavailable) = %v, want %v (error %v)", got, unavailable, err)
			}
		})
	}
}

func TestSnapshotMarksInvalidDataAsHardError(t *testing.T) {
	for name, body := range map[string]string{
		"invalid JSON":  `{not json`,
		"wrong code":    `{"code":"SOMETHING_ELSE","data":{}}`,
		"invalid data":  `{"code":"LIVE_PREDICTION_SNAPSHOT_FOUND","data":{"schema_version":"prediction.live_snapshot.v1","snapshot_id":"predsnap-test","decision_at":"2026-08-18T04:30:00Z","completed_after":"2026-08-18T01:30:00Z","generated_at":"2026-08-18T04:30:01Z","predictions":[]}}`,
		"missing field": `{"code":"LIVE_PREDICTION_SNAPSHOT_FOUND","data":{"schema_version":"prediction.live_snapshot.v1"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			err := snapshotWithHandler(t, func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(body))
			})
			if err == nil {
				t.Fatal("Snapshot() error = nil")
			}
			if errors.Is(err, port.ErrPredictionSourceUnavailable) {
				t.Fatalf("invalid data was classified as unavailable: %v", err)
			}
		})
	}
}

func TestSnapshotMarksConnectionFailureUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	client, err := New(Params{BaseURL: "http://" + address, BearerToken: "test-token", HTTPClient: &http.Client{Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Snapshot(t.Context(), availabilityDecisionAt, 3*time.Hour)
	if err == nil || !errors.Is(err, port.ErrPredictionSourceUnavailable) {
		t.Fatalf("Snapshot() error = %v, want unavailable", err)
	}
}

func TestSnapshotMarksDeadlineUnavailableAndKeepsContextError(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := New(Params{BaseURL: server.URL, BearerToken: "test-token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = client.Snapshot(ctx, availabilityDecisionAt, 3*time.Hour)
	if err == nil || !errors.Is(err, port.ErrPredictionSourceUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Snapshot() error = %v, want unavailable wrapping context.DeadlineExceeded", err)
	}
}

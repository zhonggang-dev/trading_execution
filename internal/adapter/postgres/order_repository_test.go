package postgres

import (
	"errors"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/port"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLiveSubmitRiskRejectionMapsOnlyKnownLocalTriggerErrors(t *testing.T) {
	err := &pgconn.PgError{Code: "P0001", Message: "GLOBAL_KILL_SWITCH blocks order ord-1"}
	mapped := liveSubmitRiskRejection(err)
	var rejection *port.Rejection
	if !errors.As(mapped, &rejection) || rejection.Code != "GLOBAL_KILL_SWITCH" {
		t.Fatalf("mapped error = %#v", mapped)
	}
	for _, candidate := range []error{
		&pgconn.PgError{Code: "P0001", Message: "unrelated trigger failure"},
		&pgconn.PgError{Code: "23514", Message: "GLOBAL_KILL_SWITCH blocks order ord-1"},
		errors.New("GLOBAL_KILL_SWITCH blocks order ord-1"),
	} {
		if mapped := liveSubmitRiskRejection(candidate); mapped != nil {
			t.Fatalf("unexpected mapping for %T: %v", candidate, mapped)
		}
	}
}

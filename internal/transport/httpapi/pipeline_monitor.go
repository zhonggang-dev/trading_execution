package httpapi

import (
	"net/http"
)

// getPipelineMonitor reads only the monitor's immutable cache. No HTTP request
// to this diagnostic route can trigger a collection, notification or trade.
func (server *Server) getPipelineMonitor(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if server.pipelineMonitor == nil {
		writeError(w, http.StatusServiceUnavailable, "PIPELINE_MONITOR_DISABLED", "链路监控未启用")
		return
	}
	snapshot, err := server.pipelineMonitor.Snapshot(r.Context())
	status := http.StatusOK
	code := "PIPELINE_MONITOR_FOUND"
	if err != nil || snapshot.Stale || snapshot.SampledAt.IsZero() {
		status = http.StatusServiceUnavailable
		code = "PIPELINE_MONITOR_UNAVAILABLE"
	}
	writeJSON(w, status, map[string]any{"code": code, "data": snapshot})
}

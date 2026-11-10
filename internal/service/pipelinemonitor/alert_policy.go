package pipelinemonitor

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// Both selection and card layout use the same operational ordering. Critical
// faults remain first; known live warnings stay ahead of monitoring blindness.
// Loss of observation must, however, be visible ahead of independent QA warnings.
func alertBefore(left, right Check) bool {
	if a, b := alertDisplayRank(left), alertDisplayRank(right); a != b {
		return a > b
	}
	return alertPriority(left) > alertPriority(right)
}

func ancillaryCheck(c Check) bool {
	return c.ID == "worker.qa.state" ||
		(c.ID == "host.service" || c.ID == "host.restart_storm") &&
			(strings.Contains(c.Scope, "qa-live") || strings.Contains(c.Scope, "dataset-mask") || strings.Contains(c.Scope, "dataset-sanitize"))
}

// A scalar rank, rather than pairwise exceptions, keeps sorting transitive.
// This only affects display/selection; detector status and escalation are intact.
func alertDisplayRank(c Check) int {
	if c.Status == Warning && ancillaryCheck(c) {
		return 3
	}
	if c.Status == Unknown && (strings.HasPrefix(c.ID, "monitor.") || c.ID == "worker.observation" || c.ID == "worker.round.observation" || c.ID == "host.monitor") {
		return 3
	}
	return 2 * rank(c.Status)
}

func alertPriority(c Check) int {
	if ancillaryCheck(c) {
		return 0
	}
	switch {
	case strings.HasPrefix(c.ID, "monitor."), c.ID == "worker.observation", c.ID == "worker.round.observation", c.ID == "host.monitor":
		return 5
	case c.ID == "decision.schedule", c.ID == "intent.delivery", c.ID == "order.recovery", c.ID == "fill.application", c.ID == "venue.execution_errors":
		return 4
	case c.ID == "host.service", c.ID == "host.restart_storm", strings.HasPrefix(c.ID, "host.application"), strings.HasSuffix(c.ID, ".process"):
		return 4
	case strings.HasPrefix(c.ID, "publication."), strings.HasPrefix(c.ID, "sandbox."), strings.HasPrefix(c.ID, "worker."), strings.HasPrefix(c.ID, "account."), strings.HasPrefix(c.ID, "strategy."), strings.HasPrefix(c.ID, "dependency."), strings.HasPrefix(c.ID, "prediction."):
		return 3
	default:
		return 2
	}
}

// A fresh daily round may fail again inside yesterday's reminder cooldown.
// Only durable batch identity participates: counters, timestamps and snapshots
// that change every sample must not generate a notification every minute.
// Hash the identity to keep the persisted state bounded even for long values.
func alertIncident(c Check) string {
	if !strings.HasPrefix(c.ID, "publication.") && !strings.HasPrefix(c.ID, "sandbox.") && c.ID != "worker.round.execution" {
		return ""
	}
	day := c.Evidence["expected_business_date"]
	if day == "" {
		day = c.Evidence["business_date"]
	}
	domain := c.Evidence["domain"]
	if domain == "" && c.ID == "worker.round.execution" {
		_, domain, _ = strings.Cut(c.Scope, "/")
	}
	var identity string
	if day != "" && domain != "" {
		identity = strings.Join([]string{day, domain, c.Evidence["round_index"]}, "\x00")
	} else if generation := c.Evidence["generation_id"]; generation != "" {
		identity = generation
	}
	if identity == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(digest[:])
}

func sameIncident(old alertState, c Check) bool {
	return old.IncidentKey == alertIncident(c)
}

// Successful same-incident reminders back off by 1x, 4x, then 16x the
// configured base cooldown (default 15m, 1h, 4h). Legacy state has count zero
// and retains the base cooldown until its next successful reminder.
func reminderCooldown(base time.Duration, successes int) time.Duration {
	multiplier := time.Duration(1)
	if successes >= 3 {
		multiplier = 16
	} else if successes == 2 {
		multiplier = 4
	}
	const maximum = time.Duration(1<<63 - 1)
	if base > maximum/multiplier {
		return maximum
	}
	return base * multiplier
}

func deliveredState(old alertState, c Check, at time.Time, recovery bool) alertState {
	state := alertState{Level: c.Status, SentAt: at, Active: !recovery, IncidentKey: alertIncident(c)}
	if recovery {
		return state
	}
	state.RepeatCount = 1
	if old.Active && old.Level == c.Status && sameIncident(old, c) {
		state.RepeatCount = min(3, max(1, old.RepeatCount)+1)
	}
	return state
}

package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// OrderBookSnapshotBatch is the immutable shared market-data evidence for one
// global decision boundary. Snapshots are canonicalized and sorted before the
// content identity is computed.
type OrderBookSnapshotBatch struct {
	DecisionAt           time.Time
	PredictionSnapshotID string
	SnapshotSetID        string
	TargetCount          int
	SnapshotCount        int
	OKCount              int
	EmptyCount           int
	MissingCount         int
	ErrorCount           int
	Snapshots            []OrderBookSnapshot
	CreatedAt            time.Time
}

// NewOrderBookSnapshotBatch validates, canonicalizes, and freezes a complete
// shared orderbook capture. Empty captures remain valid evidence.
func NewOrderBookSnapshotBatch(
	decisionAt time.Time,
	predictionSnapshotID string,
	targetCount int,
	snapshots []OrderBookSnapshot,
) (OrderBookSnapshotBatch, error) {
	decisionAt = decisionAt.UTC()
	predictionSnapshotID = strings.TrimSpace(predictionSnapshotID)
	if decisionAt.IsZero() || predictionSnapshotID == "" {
		return OrderBookSnapshotBatch{}, fmt.Errorf("orderbook snapshot batch decision and prediction identities are required")
	}
	if targetCount < 0 || targetCount != len(snapshots) {
		return OrderBookSnapshotBatch{}, fmt.Errorf("orderbook snapshot batch target count must equal snapshot count")
	}

	canonical := make([]OrderBookSnapshot, len(snapshots))
	seen := make(map[string]struct{}, len(snapshots))
	counts := map[OrderBookStatus]int{}
	for index, snapshot := range snapshots {
		snapshot = canonicalOrderBookSnapshot(snapshot)
		if err := snapshot.Validate(); err != nil {
			return OrderBookSnapshotBatch{}, fmt.Errorf("orderbook snapshot %d: %w", index, err)
		}
		if (snapshot.Status == OrderBookStatusMissing || snapshot.Status == OrderBookStatusError) &&
			(snapshot.ErrorCode == "" || len(snapshot.Bids) != 0 || len(snapshot.Asks) != 0 ||
				!snapshot.BestBid.IsEmpty() || !snapshot.BestAsk.IsEmpty()) {
			return OrderBookSnapshotBatch{}, fmt.Errorf("orderbook snapshot %d: failed status requires an error code and empty levels", index)
		}
		key := string(snapshot.MarketSource.Normalize()) + "\x00" + snapshot.TokenID
		if _, duplicate := seen[key]; duplicate {
			return OrderBookSnapshotBatch{}, fmt.Errorf("orderbook snapshot batch contains duplicate token identity %q", snapshot.TokenID)
		}
		seen[key] = struct{}{}
		counts[snapshot.Status]++
		canonical[index] = snapshot
	}
	sort.Slice(canonical, func(i, j int) bool {
		left, right := canonical[i], canonical[j]
		if left.MarketSource != right.MarketSource {
			return left.MarketSource < right.MarketSource
		}
		if left.ConditionID != right.ConditionID {
			return left.ConditionID < right.ConditionID
		}
		if left.OutcomeIndex != right.OutcomeIndex {
			return left.OutcomeIndex < right.OutcomeIndex
		}
		return left.TokenID < right.TokenID
	})

	identity := struct {
		DecisionAt           time.Time           `json:"decision_at"`
		PredictionSnapshotID string              `json:"prediction_snapshot_id"`
		Snapshots            []OrderBookSnapshot `json:"snapshots"`
	}{
		DecisionAt:           decisionAt,
		PredictionSnapshotID: predictionSnapshotID,
		Snapshots:            canonical,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return OrderBookSnapshotBatch{}, fmt.Errorf("encode orderbook snapshot set identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return OrderBookSnapshotBatch{
		DecisionAt: decisionAt, PredictionSnapshotID: predictionSnapshotID,
		SnapshotSetID: "orderbook-snapshot-set-" + hex.EncodeToString(digest[:]),
		TargetCount:   targetCount, SnapshotCount: len(canonical),
		OKCount: counts[OrderBookStatusOK], EmptyCount: counts[OrderBookStatusEmpty],
		MissingCount: counts[OrderBookStatusMissing], ErrorCount: counts[OrderBookStatusError],
		Snapshots: canonical,
	}, nil
}

func canonicalOrderBookSnapshot(snapshot OrderBookSnapshot) OrderBookSnapshot {
	snapshot.MarketSource = snapshot.MarketSource.Normalize()
	snapshot.MarketID = strings.TrimSpace(snapshot.MarketID)
	snapshot.ConditionID = strings.TrimSpace(snapshot.ConditionID)
	snapshot.TokenID = strings.TrimSpace(snapshot.TokenID)
	snapshot.OutcomeID = strings.ToUpper(strings.TrimSpace(snapshot.OutcomeID))
	snapshot.ErrorCode = strings.TrimSpace(snapshot.ErrorCode)
	snapshot.SourceAt = snapshot.SourceAt.UTC()
	snapshot.ObservedAt = snapshot.ObservedAt.UTC()
	snapshot.TickSize = snapshot.TickSize.Canonical()
	snapshot.MinOrderSize = snapshot.MinOrderSize.Canonical()
	snapshot.BestBid = snapshot.BestBid.Canonical()
	snapshot.BestAsk = snapshot.BestAsk.Canonical()
	snapshot.Bids = append([]PriceLevel(nil), snapshot.Bids...)
	snapshot.Asks = append([]PriceLevel(nil), snapshot.Asks...)
	if snapshot.Bids == nil {
		snapshot.Bids = []PriceLevel{}
	}
	if snapshot.Asks == nil {
		snapshot.Asks = []PriceLevel{}
	}
	for index := range snapshot.Bids {
		snapshot.Bids[index].Price = snapshot.Bids[index].Price.Canonical()
		snapshot.Bids[index].Size = snapshot.Bids[index].Size.Canonical()
	}
	for index := range snapshot.Asks {
		snapshot.Asks[index].Price = snapshot.Asks[index].Price.Canonical()
		snapshot.Asks[index].Size = snapshot.Asks[index].Size.Canonical()
	}
	return snapshot
}

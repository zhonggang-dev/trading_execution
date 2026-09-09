package domain

import (
	"slices"
	"testing"
	"time"
)

func TestNewOrderBookSnapshotBatchCanonicalizesOrderAndCountsStatuses(t *testing.T) {
	decisionAt := time.Date(2026, 9, 9, 4, 20, 0, 0, time.UTC)
	books := []OrderBookSnapshot{
		testBatchBook(decisionAt, "token-error", 1, OrderBookStatusError),
		testBatchBook(decisionAt, "token-ok", 0, OrderBookStatusOK),
		testBatchBook(decisionAt, "token-missing", 1, OrderBookStatusMissing),
		testBatchBook(decisionAt, "token-empty", 0, OrderBookStatusEmpty),
	}
	batch, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-1", len(books), books)
	if err != nil {
		t.Fatal(err)
	}
	if batch.OKCount != 1 || batch.EmptyCount != 1 || batch.MissingCount != 1 || batch.ErrorCount != 1 {
		t.Fatalf("status counts = %d/%d/%d/%d", batch.OKCount, batch.EmptyCount, batch.MissingCount, batch.ErrorCount)
	}
	wantTokens := []string{"token-empty", "token-ok", "token-error", "token-missing"}
	gotTokens := make([]string, len(batch.Snapshots))
	for index, snapshot := range batch.Snapshots {
		gotTokens[index] = snapshot.TokenID
		if snapshot.MarketSource != MarketSourcePolymarket {
			t.Fatalf("market source = %q, want canonical Polymarket", snapshot.MarketSource)
		}
	}
	if !slices.Equal(gotTokens, wantTokens) {
		t.Fatalf("sorted tokens = %#v, want %#v", gotTokens, wantTokens)
	}

	reversed := slices.Clone(books)
	slices.Reverse(reversed)
	retry, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-1", len(reversed), reversed)
	if err != nil {
		t.Fatal(err)
	}
	if retry.SnapshotSetID != batch.SnapshotSetID {
		t.Fatalf("snapshot set hash changed with input order: %q != %q", retry.SnapshotSetID, batch.SnapshotSetID)
	}
	equivalentDecimals := slices.Clone(books)
	equivalentDecimals[1].TickSize = "0.010"
	equivalentDecimals[1].MinOrderSize = "1.00"
	equivalentDecimals[1].BestBid = "0.490"
	equivalentDecimals[1].BestAsk = "0.510"
	equivalentDecimals[1].Bids[0] = PriceLevel{Price: "0.490", Size: "10.00"}
	equivalentDecimals[1].Asks[0] = PriceLevel{Price: "0.510", Size: "11.00"}
	equivalent, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-1", len(equivalentDecimals), equivalentDecimals)
	if err != nil {
		t.Fatal(err)
	}
	if equivalent.SnapshotSetID != batch.SnapshotSetID {
		t.Fatalf("snapshot set hash changed for equivalent decimals: %q != %q", equivalent.SnapshotSetID, batch.SnapshotSetID)
	}
	lexicallyDifferent := slices.Clone(books)
	lexicallyDifferent[1].TickSize = "+00.010"
	lexicallyDifferent[1].MinOrderSize = "01.00"
	lexicallyDifferent[1].BestBid = "+00.490"
	lexicallyDifferent[1].BestAsk = "00.510"
	lexicallyDifferent[1].Bids[0] = PriceLevel{Price: "+00.490", Size: "010.00"}
	lexicallyDifferent[1].Asks[0] = PriceLevel{Price: "00.510", Size: "011.00"}
	lexicallyEquivalent, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-1", len(lexicallyDifferent), lexicallyDifferent)
	if err != nil {
		t.Fatal(err)
	}
	if lexicallyEquivalent.SnapshotSetID != batch.SnapshotSetID {
		t.Fatalf("snapshot set hash changed for lexically equivalent decimals: %q != %q", lexicallyEquivalent.SnapshotSetID, batch.SnapshotSetID)
	}
	changed := slices.Clone(books)
	changed[1].ObservedAt = changed[1].ObservedAt.Add(time.Nanosecond)
	conflict, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-1", len(changed), changed)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.SnapshotSetID == batch.SnapshotSetID {
		t.Fatal("snapshot set hash did not cover observed_at")
	}
}

func TestNewOrderBookSnapshotBatchAcceptsEmptyCapture(t *testing.T) {
	batch, err := NewOrderBookSnapshotBatch(
		time.Date(2026, 9, 9, 4, 30, 0, 0, time.UTC), "predsnap-empty", 0, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.SnapshotCount != 0 || batch.Snapshots == nil || batch.SnapshotSetID == "" {
		t.Fatalf("empty batch = %#v", batch)
	}
}

func TestNewOrderBookSnapshotBatchRejectsDuplicateAndConflictingShape(t *testing.T) {
	decisionAt := time.Date(2026, 9, 9, 4, 40, 0, 0, time.UTC)
	book := testBatchBook(decisionAt, "token-duplicate", 0, OrderBookStatusOK)
	if _, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-duplicate", 2, []OrderBookSnapshot{book, book}); err == nil {
		t.Fatal("duplicate token identity was accepted")
	}
	if _, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-count", 2, []OrderBookSnapshot{book}); err == nil {
		t.Fatal("target/snapshot count mismatch was accepted")
	}
	failed := testBatchBook(decisionAt, "token-error", 1, OrderBookStatusError)
	failed.Bids = []PriceLevel{{Price: "0.49", Size: "1"}}
	if _, err := NewOrderBookSnapshotBatch(decisionAt, "predsnap-failed-shape", 1, []OrderBookSnapshot{failed}); err == nil {
		t.Fatal("failed snapshot with price levels was accepted")
	}
}

func testBatchBook(decisionAt time.Time, tokenID string, outcomeIndex int, status OrderBookStatus) OrderBookSnapshot {
	book := OrderBookSnapshot{
		MarketID: "market-1", ConditionID: "condition-1", OutcomeIndex: outcomeIndex,
		TokenID: tokenID, Status: status, ObservedAt: decisionAt.Add(time.Second),
		DepthLimit: StrategyOrderBookDepth, Bids: []PriceLevel{}, Asks: []PriceLevel{},
	}
	switch status {
	case OrderBookStatusOK:
		book.SourceAt = decisionAt
		book.TickSize = "0.01"
		book.MinOrderSize = "1"
		book.Bids = []PriceLevel{{Price: "0.49", Size: "10"}}
		book.Asks = []PriceLevel{{Price: "0.51", Size: "11"}}
		book.BestBid = "0.49"
		book.BestAsk = "0.51"
	case OrderBookStatusMissing, OrderBookStatusError:
		book.ErrorCode = "TEST_" + string(status)
	}
	return book
}

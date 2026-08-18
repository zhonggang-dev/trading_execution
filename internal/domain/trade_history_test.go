package domain

import (
	"testing"
	"time"
)

// TestTradeHistoryFilterNormalizesAndValidates 验证交易历史筛选条件能够规范化并通过校验。
func TestTradeHistoryFilterNormalizesAndValidates(t *testing.T) {
	from := time.Date(2026, 8, 17, 8, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	filter := (TradeHistoryFilter{Side: " sell ", From: &from}).Normalize()
	if filter.Limit != DefaultTradeHistoryLimit || filter.Side != SideSell {
		t.Fatalf("normalized filter = %#v", filter)
	}
	if filter.From == nil || filter.From.Location() != time.UTC {
		t.Fatalf("normalized from = %v", filter.From)
	}
	if err := filter.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

// TestTradeHistoryFilterRejectsUnsafePaginationAndRange 验证交易历史筛选拒绝不安全分页和时间范围。
func TestTradeHistoryFilterRejectsUnsafePaginationAndRange(t *testing.T) {
	from := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	to := from.Add(-time.Minute)
	for _, filter := range []TradeHistoryFilter{
		{Limit: MaxTradeHistoryLimit + 1},
		{Limit: 10, Offset: -1},
		{Limit: 10, Side: "HOLD"},
		{Limit: 10, From: &from, To: &to},
	} {
		if err := filter.Validate(); err == nil {
			t.Fatalf("Validate(%#v) unexpectedly succeeded", filter)
		}
	}
}

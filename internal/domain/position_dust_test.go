package domain

import "testing"

// TestIsDustSharesUsesSellPrecisionThreshold 验证低于 0.01 股的正数零头被判定为 dust。
func TestIsDustSharesUsesSellPrecisionThreshold(t *testing.T) {
	tests := []struct {
		shares Decimal
		want   bool
	}{
		{shares: "0.008163", want: true},
		{shares: "0.009999", want: true},
		{shares: "0.01", want: false},
		{shares: "0.40", want: false},
		{shares: "30.147057", want: false},
		{shares: "0", want: false},
		{shares: "-0.001", want: false},
		{shares: "", want: false},
	}
	for _, test := range tests {
		if got := IsDustShares(test.shares); got != test.want {
			t.Fatalf("IsDustShares(%q) = %v, want %v", test.shares, got, test.want)
		}
	}
}

// TestPositionLotDerivesDustWithoutChangingShares 验证 dust 只是派生标记，不改变真实剩余数量。
func TestPositionLotDerivesDustWithoutChangingShares(t *testing.T) {
	lot := PositionLot{LotID: "lot-1", RemainingShares: "0.007057", Status: PositionLotOpen}.WithDerivedDust()
	if !lot.IsDust || !lot.HasDustRemainder() || lot.RemainingShares != "0.007057" {
		t.Fatalf("dust lot = %#v", lot)
	}
	sellable := PositionLot{LotID: "lot-2", RemainingShares: "30.14", Status: PositionLotOpen}.WithDerivedDust()
	if sellable.IsDust || sellable.HasDustRemainder() {
		t.Fatalf("sellable lot = %#v", sellable)
	}
}

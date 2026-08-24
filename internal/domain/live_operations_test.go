package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestLiveNumberEncodesAsExactJSONNumber 验证资金数值不会被编码成字符串或 float64。
func TestLiveNumberEncodesAsExactJSONNumber(t *testing.T) {
	number, err := NewLiveNumber("1234567890.123456789012")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(struct {
		Value LiveNumber `json:"value"`
	}{Value: number})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"value":1234567890.123456789012}` {
		t.Fatalf("payload = %s", payload)
	}
}

func TestLiveWalletEncodesFinancialValuesAsNumbersAndUndefinedReturnAsNull(t *testing.T) {
	payload, err := json.Marshal(LiveWallet{
		ExecutionAccountID: "wallet-1", PositionCount: 0,
		PeakCashUsed: "0", CumulativeInvestedCost: "12.5",
		RealizedPnL: "-1.25", UnrealizedPnL: "0.5", TotalPnL: "-0.75",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"peakCashUsed":0`, `"cumulativeInvestedCost":12.5`,
		`"totalPnl":-0.75`, `"return":null`,
	} {
		if !strings.Contains(string(payload), fragment) {
			t.Fatalf("wallet payload %s does not contain %s", payload, fragment)
		}
	}
}

func TestCloneLiveOperationsSnapshotDeepCopiesWalletReturn(t *testing.T) {
	returnRate := LiveNumber("0.5")
	snapshot := LiveOperationsSnapshot{
		Capital: LiveCapital{
			Equity: "0", AvailableCash: "0", GrossExposure: "0", ExposureLimit: "0",
			RealizedPnLToday: "0", UnrealizedPnL: "0", FeeToday: "0",
		},
		Wallets: []LiveWallet{{
			ExecutionAccountID: "wallet-1", PeakCashUsed: "10", CumulativeInvestedCost: "10",
			RealizedPnL: "5", UnrealizedPnL: "0", TotalPnL: "5", ReturnRate: &returnRate,
		}},
	}
	cloned, err := CloneLiveOperationsSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	*cloned.Wallets[0].ReturnRate = "0.25"
	if snapshot.Wallets[0].ReturnRate == nil || *snapshot.Wallets[0].ReturnRate != "0.5" {
		t.Fatalf("source wallet return was mutated: %#v", snapshot.Wallets[0])
	}
}

// TestLiveNumberRejectsNonCanonicalJSONNotation 验证加号、前导零和省略整数位不会生成非法 JSON。
func TestLiveNumberRejectsNonCanonicalJSONNotation(t *testing.T) {
	for _, value := range []Decimal{"+1", "01", ".5", "1."} {
		if _, err := NewLiveNumber(value); err == nil {
			t.Fatalf("NewLiveNumber(%q) error = nil", value)
		}
	}
}

package liveoperations

import (
	"math/big"
	"testing"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestMakeRiskReturnsReadOnlyTargetSemantics 验证金额目标返回显式阈值且不会伪装成执行硬上限。
func TestMakeRiskReturnsReadOnlyTargetSemantics(t *testing.T) {
	risk, err := makeRisk(riskDefinition{
		id: "exposure", name: "总敞口", current: rat("318.15171674"), referenceTarget: rat("84.2"),
		thresholdType: domain.LiveRiskTarget, unit: "$",
	})
	if err != nil {
		t.Fatal(err)
	}
	if risk.WarningThreshold != "67.36" || risk.HardLimit != "84.2" || risk.Limit != risk.HardLimit || risk.UsagePercentage == nil {
		t.Fatalf("risk thresholds = %#v", risk)
	}
	if *risk.UsagePercentage != "377.852395178147" || risk.State != domain.LiveFlowDanger || risk.HardLimitEnforced || risk.ThresholdType != domain.LiveRiskTarget {
		t.Fatalf("risk semantics = %#v", risk)
	}
}

// TestMakeRiskTreatsZeroTargetAsCountBreach 验证目标为零时不生成除零百分比。
func TestMakeRiskTreatsZeroTargetAsCountBreach(t *testing.T) {
	risk, err := makeRisk(riskDefinition{
		id: "stale", name: "预测过期", current: big.NewRat(6, 1), referenceTarget: new(big.Rat),
		thresholdType: domain.LiveRiskTarget, unit: "count",
	})
	if err != nil {
		t.Fatal(err)
	}
	if risk.UsagePercentage != nil || risk.State != domain.LiveFlowDanger || risk.HardLimitEnforced {
		t.Fatalf("zero target risk = %#v", risk)
	}
}

// TestRiskStateUsesWarningAndTargetBoundaries 验证 80% 提醒线和 100% 目标边界。
func TestRiskStateUsesWarningAndTargetBoundaries(t *testing.T) {
	referenceTarget := rat("100")
	warning := riskWarningThreshold(referenceTarget)
	tests := []struct {
		current string
		want    domain.LiveFlowState
	}{
		{current: "79.99", want: domain.LiveFlowSafe},
		{current: "80", want: domain.LiveFlowWarning},
		{current: "99.99", want: domain.LiveFlowWarning},
		{current: "100", want: domain.LiveFlowDanger},
		{current: "120", want: domain.LiveFlowDanger},
	}
	for _, test := range tests {
		if got := riskState(rat(test.current), warning, referenceTarget); got != test.want {
			t.Fatalf("riskState(%s)=%s, want %s", test.current, got, test.want)
		}
	}
}

// rat 把测试十进制文本转换成精确有理数。
func rat(value string) *big.Rat {
	result, ok := new(big.Rat).SetString(value)
	if !ok {
		panic("invalid test rational: " + value)
	}
	return result
}

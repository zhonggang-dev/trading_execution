package domain

import "testing"

// TestDecimalIsMultipleOf 验证 Decimal Is Multiple Of 场景下的行为。
func TestDecimalIsMultipleOf(t *testing.T) {
	tests := []struct {
		value string
		step  string
		want  bool
	}{
		{value: "0.52", step: "0.01", want: true},
		{value: "0.525", step: "0.01", want: false},
		{value: "1", step: "0.001", want: true},
	}
	for _, test := range tests {
		got, err := Decimal(test.value).IsMultipleOf(Decimal(test.step))
		if err != nil || got != test.want {
			t.Fatalf("Decimal(%q).IsMultipleOf(%q) = %v, %v; want %v", test.value, test.step, got, err, test.want)
		}
	}
}

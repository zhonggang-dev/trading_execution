package domain

import (
	"encoding/json"
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

// TestLiveNumberRejectsNonCanonicalJSONNotation 验证加号、前导零和省略整数位不会生成非法 JSON。
func TestLiveNumberRejectsNonCanonicalJSONNotation(t *testing.T) {
	for _, value := range []Decimal{"+1", "01", ".5", "1."} {
		if _, err := NewLiveNumber(value); err == nil {
			t.Fatalf("NewLiveNumber(%q) error = nil", value)
		}
	}
}

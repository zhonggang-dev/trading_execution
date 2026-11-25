package consolev2

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

func TestOrderPredictedProbability(t *testing.T) {
	cases := []struct {
		name string
		raw  sql.NullString
		want *string
	}{
		{name: "valid", raw: sql.NullString{String: "0.91", Valid: true}, want: stringPointer("0.91")},
		{name: "normalized", raw: sql.NullString{String: "0.9100", Valid: true}, want: stringPointer("0.91")},
		{name: "whitespace", raw: sql.NullString{String: "  0.5\n", Valid: true}, want: stringPointer("0.5")},
		{name: "zero", raw: sql.NullString{String: "0", Valid: true}, want: stringPointer("0")},
		{name: "negative zero", raw: sql.NullString{String: "-0", Valid: true}, want: stringPointer("0")},
		{name: "one", raw: sql.NullString{String: "1", Valid: true}, want: stringPointer("1")},
		{name: "missing", raw: sql.NullString{}, want: nil},
		{name: "empty", raw: sql.NullString{String: "", Valid: true}, want: nil},
		{name: "blank", raw: sql.NullString{String: "   ", Valid: true}, want: nil},
		{name: "below range", raw: sql.NullString{String: "-0.1", Valid: true}, want: nil},
		{name: "above range", raw: sql.NullString{String: "1.5", Valid: true}, want: nil},
		{name: "not numeric", raw: sql.NullString{String: "high", Valid: true}, want: nil},
		{name: "nan", raw: sql.NullString{String: "NaN", Valid: true}, want: nil},
		{name: "inf", raw: sql.NullString{String: "Inf", Valid: true}, want: nil},
		{name: "negative inf", raw: sql.NullString{String: "-Inf", Valid: true}, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := orderPredictedProbability(tc.raw)
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("orderPredictedProbability(%q) = %v, want %v", tc.raw.String, derefString(got), derefString(tc.want))
			}
		})
	}
}

func TestPopulateOrderBaseMapsPredictedProbabilityToJSON(t *testing.T) {
	var present, absent Order
	empty := sql.NullString{}
	populateOrderBase(&present, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, sql.NullString{String: " 0.75 ", Valid: true})
	populateOrderBase(&absent, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, sql.NullString{String: "1.5", Valid: true})

	encoded, err := json.Marshal(present)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"predicted_probability":"0.75"`) {
		t.Fatalf("order JSON = %s", encoded)
	}
	encoded, err = json.Marshal(absent)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"predicted_probability":null`) {
		t.Fatalf("order JSON = %s", encoded)
	}
}

func stringPointer(value string) *string { return &value }

func derefString(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}

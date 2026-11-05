package postgres

import (
	"encoding/json"
	"testing"
)

func TestHistoricalManifestRejectsAmbiguousIdentityAndAllocation(t *testing.T) {
	for _, mutate := range []func(*RebasedSaleManifest){
		func(m *RebasedSaleManifest) { m.Sales = m.Sales[:1] },
		func(m *RebasedSaleManifest) { m.Sales[1].Account = m.Sales[0].Account },
		func(m *RebasedSaleManifest) { m.Sales[0].Gross = "0.1157" }, // Entire chain sale cannot be assigned to a 0.14/0.89 lot.
		func(m *RebasedSaleManifest) { m.Sales[0].Fee = "0.01" },
		func(m *RebasedSaleManifest) { m.Sales[0].Principal = "-1" },
		func(m *RebasedSaleManifest) { m.Sales[0].Shares = "1e-2" },
		func(m *RebasedSaleManifest) { m.Sales[0].OfficialActivityTimestamp++ },
		func(m *RebasedSaleManifest) { m.Sales[0].OfficialActivitySHA256 = "" },
	} {
		m := historicalManifestFixture()
		mutate(&m)
		raw, _ := json.Marshal(m)
		if _, err := DecodeRebasedSaleManifest(raw); err == nil {
			t.Fatal("invalid reviewed manifest accepted", string(raw))
		}
	}
	m := historicalManifestFixture()
	raw, _ := json.Marshal(m)
	if _, err := DecodeRebasedSaleManifest(append(raw, []byte(` {"another":true}`)...)); err == nil {
		t.Fatal("trailing manifest accepted")
	}
	if _, err := DecodeRebasedSaleManifest([]byte(`{"sales":[],"wallet":"hidden"}`)); err == nil {
		t.Fatal("unrecognized private payload field accepted")
	}
}

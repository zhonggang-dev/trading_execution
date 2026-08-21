package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPositionModelOriginFieldsStayOutOfJSON(t *testing.T) {
	lotPayload, err := json.Marshal(PositionLot{
		LotID: "lot-1", OriginModelID: "gemini-3.6-flash", ModelID: "gemini_masked",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(lotPayload), "origin_model_id") || strings.Contains(string(lotPayload), "gemini-3.6-flash") {
		t.Fatalf("position lot leaked origin model: %s", lotPayload)
	}
	var lotFields map[string]json.RawMessage
	if err := json.Unmarshal(lotPayload, &lotFields); err != nil {
		t.Fatal(err)
	}
	if string(lotFields["model_id"]) != `"gemini_masked"` {
		t.Fatalf("position lot logical model JSON = %s", lotFields["model_id"])
	}

	tradePayload, err := json.Marshal(PositionExitTrade{
		LotID: "lot-1", OriginModelID: "gemini-3.6-flash", ModelID: "gemini_masked",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(tradePayload), "origin_model_id") || strings.Contains(string(tradePayload), "model_id") ||
		strings.Contains(string(tradePayload), "gemini-3.6-flash") || strings.Contains(string(tradePayload), "gemini_masked") {
		t.Fatalf("position exit strategy JSON leaked internal model identity: %s", tradePayload)
	}
}

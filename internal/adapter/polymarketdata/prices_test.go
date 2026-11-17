package polymarketdata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPriceHistoryParsesBucketsAndRejectsTruncation(t *testing.T) {
	var query string
	body := `{"data":[{"timestamp":200,"price":0.62},{"timestamp":100,"price":"0.5"}],"pagination":{"has_more":false}}`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		query = request.URL.Path + "?" + request.URL.RawQuery
		writer.Write([]byte(body))
	}))
	defer server.Close()
	client, err := NewPnLClient(PnLClientParams{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	points, err := client.PriceHistory(context.Background(), "123", 50, 300, 300)
	if err != nil {
		t.Fatal(err)
	}
	if query != "/v2/prices-history?bucket_seconds=300&end=300&start=50&token_id=123" {
		t.Fatalf("query = %s", query)
	}
	if len(points) != 2 || points[0].Timestamp != 100 || points[0].Price != "0.5" || points[1].Price != "0.62" {
		t.Fatalf("points = %#v", points)
	}
	for name, raw := range map[string]string{
		"has more":  `{"data":[],"pagination":{"has_more":true}}`,
		"no data":   `{"pagination":{"has_more":false}}`,
		"bad price": `{"data":[{"timestamp":1,"price":1.5}]}`,
	} {
		body = raw
		if _, err := client.PriceHistory(context.Background(), "123", 50, 300, 300); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
	if _, err := client.PriceHistory(context.Background(), "123", 1, 1+MaxPriceHistorySpanSeconds+1, 300); err == nil {
		t.Fatal("expected span error")
	}
	if _, err := client.PriceHistory(context.Background(), "0xab", 1, 2, 300); err == nil {
		t.Fatal("expected token error")
	}
}

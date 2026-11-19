package polymarketdata

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestListPositionMarksKeepsTitleDropsZeroAndRestatesFilters(t *testing.T) {
	calls := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		query := request.URL.Query()
		if request.URL.Path != "/v2/positions" || query.Get("user") != "0xabc" || query.Get("filter_amount") != "0" ||
			query.Get("include_archived") != "true" || query.Get("filter_type") != "TOKENS" {
			t.Fatalf("request URL = %s", request.URL)
		}
		body := `{"data":[{"condition_id":"c1","token_id":"t1","outcome":"No","title":"Will X happen?","current_size":26,"current_price":0.35,"redeemable":false},{"condition_id":"c2","token_id":"t2","outcome":"Yes","title":"Zero","current_size":0,"current_price":0.5},{"condition_id":"c3","token_id":"t3","outcome":"Yes","title":"Settled","current_size":10,"current_price":1,"redeemable":true}],"pagination":{"has_more":false,"next_cursor":null}}`
		if query.Get("cursor") == "" {
			body = strings.Replace(body, `"has_more":false,"next_cursor":null`, `"has_more":true,"next_cursor":"p2"`, 1)
		} else {
			body = `{"data":[],"pagination":{"has_more":false,"next_cursor":null}}`
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}
	client, err := NewPositionClient(PositionClientParams{BaseURL: "https://data-api.example", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	marks, err := client.ListPositionMarks(context.Background(), "0xAbC")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(marks) != 2 {
		t.Fatalf("calls=%d marks=%#v", calls, marks)
	}
	if marks[0].Title != "Will X happen?" || marks[0].Outcome != "No" || marks[0].Shares != "26" || marks[0].CurrentPrice != "0.35" || marks[0].Redeemable {
		t.Fatalf("first mark = %#v", marks[0])
	}
	if marks[1].TokenID != "t3" || !marks[1].Redeemable || marks[1].CurrentPrice != "1" {
		t.Fatalf("second mark = %#v", marks[1])
	}
}

func TestListPositionMarksFailsClosedWithoutPrice(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"data":[{"condition_id":"c1","token_id":"t1","outcome":"No","current_size":5,"current_price":null}],"pagination":{"has_more":false,"next_cursor":null}}`
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}
	client, err := NewPositionClient(PositionClientParams{BaseURL: "https://data-api.example", HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	if marks, err := client.ListPositionMarks(context.Background(), "0xabc"); err == nil {
		t.Fatalf("marks = %#v, want error", marks)
	}
}

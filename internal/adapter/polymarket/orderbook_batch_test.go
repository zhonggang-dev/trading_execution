package polymarket

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
)

// TestCaptureBatchSplitsRequestsAndKeepsOrder 验证批量模式按 BatchSize 切分，结果与目标同序，
// 且没有被接口返回的 token 记为 MISSING 而不是空盘。
func TestCaptureBatchSplitsRequestsAndKeepsOrder(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/books" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		requests.Add(1)
		var asked []struct {
			TokenID string `json:"token_id"`
		}
		if err := json.NewDecoder(request.Body).Decode(&asked); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if len(asked) > 2 {
			t.Errorf("batch has %d tokens, want at most 2", len(asked))
		}
		var response []map[string]any
		for _, item := range asked {
			if item.TokenID == "t2" {
				continue
			}
			response = append(response, map[string]any{
				"asset_id": item.TokenID, "timestamp": "1787026800123", "tick_size": "0.01", "min_order_size": "5",
				"bids": []map[string]string{},
				"asks": []map[string]string{{"price": "0.9", "size": "1"}, {"price": "0.02", "size": "100"}},
			})
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(response)
	}))
	defer server.Close()
	source, err := NewOrderBookSource(OrderBookParams{BaseURL: server.URL, HTTPClient: server.Client(), Workers: 2, BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	targets := []domain.BookTarget{{TokenID: "t1"}, {TokenID: "t2"}, {TokenID: "t3"}}
	books, err := source.Capture(t.Context(), time.Now(), targets)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if requests.Load() != 2 || len(books) != 3 {
		t.Fatalf("requests=%d books=%d", requests.Load(), len(books))
	}
	for index, want := range []string{"t1", "t2", "t3"} {
		if books[index].TokenID != want {
			t.Fatalf("books[%d].TokenID = %q, want %q", index, books[index].TokenID, want)
		}
	}
	if books[1].Status != domain.OrderBookStatusMissing || books[1].ErrorCode != "CLOB_BOOK_NOT_RETURNED" {
		t.Fatalf("missing book = %+v", books[1])
	}
	for _, index := range []int{0, 2} {
		book := books[index]
		if book.Status != domain.OrderBookStatusEmpty || len(book.Asks) != 2 || book.Asks[0].Price != "0.02" ||
			book.Asks[1].Price != "0.9" || book.TickSize != "0.01" || book.MinOrderSize != "5" {
			t.Fatalf("books[%d] = %+v", index, book)
		}
	}
}

// TestCaptureBatchRepresentsBatchFailureInBand 验证整批请求失败时该批每个 token 都带 HTTP 错误码。
func TestCaptureBatchRepresentsBatchFailureInBand(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	source, err := NewOrderBookSource(OrderBookParams{BaseURL: server.URL, HTTPClient: server.Client(), BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	books, err := source.Capture(t.Context(), time.Now(), []domain.BookTarget{{TokenID: "a"}, {TokenID: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, book := range books {
		if book.Status != domain.OrderBookStatusError || book.ErrorCode != "CLOB_HTTP_429" {
			t.Fatalf("book = %+v", book)
		}
	}
}

// TestNewOrderBookSourceRejectsInvalidBatchSize 验证批量大小越界被拒绝。
func TestNewOrderBookSourceRejectsInvalidBatchSize(t *testing.T) {
	for _, size := range []int{-1, 501} {
		if _, err := NewOrderBookSource(OrderBookParams{BaseURL: "https://clob.example", BatchSize: size}); err == nil {
			t.Errorf("BatchSize %d accepted", size)
		}
	}
}

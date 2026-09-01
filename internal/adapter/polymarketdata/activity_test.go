package polymarketdata

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestListRedeemActivitiesFiltersAndNormalizes(t *testing.T) {
	wallet := "0x1111111111111111111111111111111111111111"
	condition := "0x" + fmt.Sprintf("%064x", 2)
	txHash := "0x" + fmt.Sprintf("%064x", 3)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if request.URL.Path != "/activity" || query.Get("user") != wallet || query.Get("market") != condition ||
			query.Get("type") != "REDEEM" || query.Get("start") != "1788307200" || query.Get("sortDirection") != "ASC" {
			t.Fatalf("unexpected request %s?%s", request.URL.Path, request.URL.RawQuery)
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(writer, `[{"proxyWallet":%q,"timestamp":1788307201,"conditionId":%q,"type":"REDEEM","transactionHash":%q,"future":"ignored"}]`, wallet, condition, txHash)
	}))
	defer server.Close()
	client, err := NewPositionClient(PositionClientParams{BaseURL: server.URL, HTTPClient: server.Client(), RequestsPerSecond: 15})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	values, err := client.ListRedeemActivities(context.Background(), wallet, condition, start)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].TransactionHash != txHash || values[0].ConditionID != condition || values[0].WalletAddress != wallet {
		t.Fatalf("activities = %#v", values)
	}
}

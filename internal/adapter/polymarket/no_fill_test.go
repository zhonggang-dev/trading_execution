package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

func newIOCTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/version":
			writeTestJSON(writer, map[string]any{"version": 2})
		case "/tick-size":
			writeTestJSON(writer, map[string]any{"minimum_tick_size": 0.01})
		case "/neg-risk":
			writeTestJSON(writer, map[string]any{"neg_risk": false})
		default:
			http.NotFound(writer, request)
		}
	}))
}

func newIOCTestClient(t *testing.T, baseURL string, now time.Time, orderType string, lifetime time.Duration) *TradingClient {
	t.Helper()
	base := newTestTradingClient(t, baseURL, now)
	client, err := NewTradingClient(TradingClientParams{
		BaseURL: baseURL, Credentials: base.credentials, RequestTimeout: 2 * time.Second,
		RequestsPerSecond: 100, Burst: 20, Now: func() time.Time { return now },
		Random:       strings.NewReader(strings.Repeat("x", 32)),
		IOCOrderType: orderType, IOCOrderLifetime: lifetime,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func iocTestOrder() domain.Order {
	order := adapterOrder()
	order.Intent.TimeInForce = domain.TimeInForceIOC
	order.Intent.Size = "42"
	order.MarketValidation.WorstPrice = "0.50"
	order.MarketValidation.ExecutableSize = "7"
	return order
}

func preparedPayload(t *testing.T, client *TradingClient, order domain.Order) (*preparedCLOBPlacement, postOrderPayload) {
	t.Helper()
	prepared, err := client.PreparePlace(context.Background(), order)
	if err != nil {
		t.Fatalf("PreparePlace() error = %v", err)
	}
	placement := prepared.(*preparedCLOBPlacement)
	var payload postOrderPayload
	if err := json.Unmarshal(placement.body, &payload); err != nil {
		t.Fatal(err)
	}
	return placement, payload
}

// TestEmulatedIOCDefaultIsUnchangedGTC: with the switch off (default) the
// signed request is byte-for-byte the historical GTC request without expiry.
func TestEmulatedIOCDefaultIsUnchangedGTC(t *testing.T) {
	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	server := newIOCTestServer(t)
	defer server.Close()
	defaultPlacement, defaultPayload := preparedPayload(t, newIOCTestClient(t, server.URL, now, "", 0), iocTestOrder())
	explicitPlacement, _ := preparedPayload(t, newIOCTestClient(t, server.URL, now, "GTC", 0), iocTestOrder())
	historicalPlacement, _ := preparedPayload(t, newTestTradingClient(t, server.URL, now), iocTestOrder())
	if defaultPayload.OrderType != "GTC" || defaultPayload.Order.Expiration != "0" {
		t.Fatalf("default payload = %s/%s, want GTC/0", defaultPayload.OrderType, defaultPayload.Order.Expiration)
	}
	if string(defaultPlacement.body) != string(explicitPlacement.body) || string(defaultPlacement.body) != string(historicalPlacement.body) {
		t.Fatal("default emulated IOC request differs from the historical GTC request")
	}
	if _, ok := port.PreparedVenueExpiry(defaultPlacement); ok {
		t.Fatal("GTC placement must not report a venue expiry")
	}
}

// TestEmulatedIOCAsGTDCarriesExchangeClockExpiryWithSameHash: GTD adds only
// the order type and the expiry; the expiry is outside the EIP-712 struct, so
// the order hash is identical.
func TestEmulatedIOCAsGTDCarriesExchangeClockExpiryWithSameHash(t *testing.T) {
	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	server := newIOCTestServer(t)
	defer server.Close()
	gtcPlacement, gtcPayload := preparedPayload(t, newIOCTestClient(t, server.URL, now, "GTC", 0), iocTestOrder())
	gtdClient := newIOCTestClient(t, server.URL, now, "GTD", 6*time.Minute)
	gtdClient.clockOffset = 2 * time.Second // exchange clock ahead of the local clock
	gtdPlacement, gtdPayload := preparedPayload(t, gtdClient, iocTestOrder())
	wantExpiry := now.Add(2 * time.Second).Add(6 * time.Minute)
	if gtdPayload.OrderType != "GTD" || gtdPayload.Order.Expiration != strconv.FormatInt(wantExpiry.Unix(), 10) {
		t.Fatalf("GTD payload = %s/%s, want GTD/%d", gtdPayload.OrderType, gtdPayload.Order.Expiration, wantExpiry.Unix())
	}
	expiresAt, ok := port.PreparedVenueExpiry(gtdPlacement)
	if !ok || !expiresAt.Equal(wantExpiry) {
		t.Fatalf("prepared expiry = %s, %v; want %s", expiresAt, ok, wantExpiry)
	}
	gtdHash := signedOrderIDForTest(t, gtdPayload.Order, false)
	if gtdHash != gtdPlacement.expectedOrderID || signedOrderIDForTest(t, gtcPayload.Order, false) != gtcPlacement.expectedOrderID {
		t.Fatal("expected order id does not match the signed order")
	}
	// Same salt, timestamp and amounts: only the timestamp clock offset differs.
	gtcPayload.Order.Timestamp, gtcPayload.Order.Signature = gtdPayload.Order.Timestamp, gtdPayload.Order.Signature
	gtcPayload.Order.Expiration, gtcPayload.OrderType = gtdPayload.Order.Expiration, gtdPayload.OrderType
	if gtcPayload != gtdPayload {
		t.Fatalf("GTD changed more than order type and expiry:\nGTC %#v\nGTD %#v", gtcPayload, gtdPayload)
	}
	if signedOrderIDForTest(t, gtcPayload.Order, false) != gtdHash {
		t.Fatal("the expiry changed the order hash")
	}
}

func TestEmulatedIOCGTDDoesNotAffectExplicitTimeInForce(t *testing.T) {
	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	server := newIOCTestServer(t)
	defer server.Close()
	order := adapterOrder()
	order.Intent.TimeInForce = domain.TimeInForceGTC
	_, payload := preparedPayload(t, newIOCTestClient(t, server.URL, now, "GTD", 0), order)
	if payload.OrderType != "GTC" || payload.Order.Expiration != "0" {
		t.Fatalf("explicit GTC payload = %s/%s, want GTC/0", payload.OrderType, payload.Order.Expiration)
	}
}

func TestTradingClientRejectsInvalidIOCCarrier(t *testing.T) {
	base := newTestTradingClient(t, "https://clob.example", time.Now())
	for _, params := range []TradingClientParams{
		{IOCOrderType: "FOK"},
		{IOCOrderType: "GTD", IOCOrderLifetime: 3 * time.Minute},
		{IOCOrderType: "GTD", IOCOrderLifetime: 16 * time.Minute},
	} {
		params.BaseURL, params.Credentials = "https://clob.example", base.credentials
		if _, err := NewTradingClient(params); err == nil {
			t.Fatalf("NewTradingClient(%s, %s) error = nil", params.IOCOrderType, params.IOCOrderLifetime)
		}
	}
}

func TestPreparedWrappersForwardVenueExpiry(t *testing.T) {
	expiresAt := time.Date(2026, 11, 5, 9, 5, 0, 0, time.UTC)
	inner := &preparedCLOBPlacement{expectedOrderID: "0xabc", venueExpiresAt: expiresAt}
	for name, wrapped := range map[string]port.PreparedPlacement{
		"eligibility":        eligibilityPrepared{inner: inner},
		"conditionalFunding": conditionalFundingPrepared{inner: inner},
	} {
		got, ok := port.PreparedVenueExpiry(wrapped)
		if !ok || !got.Equal(expiresAt) {
			t.Fatalf("%s wrapper expiry = %s, %v", name, got, ok)
		}
	}
	if _, ok := port.PreparedVenueExpiry(eligibilityPrepared{inner: &preparedCLOBPlacement{expectedOrderID: "0xabc"}}); ok {
		t.Fatal("a GTC placement must not gain an expiry through a wrapper")
	}
}

type fakeNoFillCLOB struct {
	getErr     error
	trades     []Trade
	tradesErr  error
	open       []OpenOrder
	openErr    error
	tradeQuery TradeFilter
}

func (clob *fakeNoFillCLOB) Get(context.Context, domain.Order) (port.VenueOrder, error) {
	return port.VenueOrder{}, clob.getErr
}

func (clob *fakeNoFillCLOB) ListTrades(_ context.Context, _ string, filter TradeFilter) ([]Trade, error) {
	clob.tradeQuery = filter
	return clob.trades, clob.tradesErr
}

func (clob *fakeNoFillCLOB) ListOpenOrders(context.Context, string, OpenOrderFilter) ([]OpenOrder, error) {
	return clob.open, clob.openErr
}

func noFillTestOrder() domain.Order {
	order := adapterOrder()
	order.VenueOrderID = "0x" + strings.Repeat("ab", 32)
	order.Intent.ConditionID = "0xcondition"
	return order
}

func TestNoFillEvidenceReaderOrderRead(t *testing.T) {
	order := noFillTestOrder()
	cases := []struct {
		name     string
		err      error
		notFound bool
		wantErr  bool
	}{
		{name: "200 null", err: &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_ORDER_NOT_FOUND"}, notFound: true},
		{name: "visible", err: nil},
		{name: "404 is not the null answer", err: &port.VenueError{Kind: port.VenueErrorRejected, Code: "CLOB_ORDER_NOT_FOUND"}, wantErr: true},
		{name: "server error", err: &port.VenueError{Kind: port.VenueErrorUnavailable, Code: "CLOB_SERVER_ERROR"}, wantErr: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			reader, _ := NewNoFillEvidenceReader(&fakeNoFillCLOB{getErr: testCase.err}, nil)
			read, err := reader.ReadOrder(context.Background(), order)
			if (err != nil) != testCase.wantErr || (err == nil && read.NotFound != testCase.notFound) {
				t.Fatalf("ReadOrder() = %#v, %v", read, err)
			}
		})
	}
}

func TestNoFillEvidenceReaderCountsTradeReferencesOfAnyStatus(t *testing.T) {
	order := noFillTestOrder()
	after := time.Date(2026, 11, 5, 8, 50, 0, 0, time.UTC)
	clob := &fakeNoFillCLOB{trades: []Trade{
		{ID: "t-other", TakerOrderID: "0x" + strings.Repeat("cd", 32), Status: "CONFIRMED"},
		{ID: "t-failed", TakerOrderID: strings.ToUpper(order.VenueOrderID), Status: "FAILED"},
		{ID: "t-maker", TakerOrderID: "0xother", Status: "RETRYING", MakerOrders: []MakerOrder{{OrderID: order.VenueOrderID}}},
	}}
	reader, _ := NewNoFillEvidenceReader(clob, nil)
	scan, err := reader.TradeReferences(context.Background(), order, after)
	if err != nil || scan.Scanned != 3 || len(scan.References) != 2 {
		t.Fatalf("TradeReferences() = %#v, %v; want FAILED taker and RETRYING maker references", scan, err)
	}
	if clob.tradeQuery.Market != "0xcondition" || clob.tradeQuery.After != after.Unix() {
		t.Fatalf("trade query = %#v", clob.tradeQuery)
	}
	clob.tradesErr = errors.New("pagination exceeded")
	if _, err := reader.TradeReferences(context.Background(), order, after); err == nil {
		t.Fatal("a failed trade listing must be an error, never an empty scan")
	}
}

func TestNoFillEvidenceReaderOpenOrders(t *testing.T) {
	order := noFillTestOrder()
	reader, _ := NewNoFillEvidenceReader(&fakeNoFillCLOB{open: []OpenOrder{{ID: "0xother"}, {ID: order.VenueOrderID}}}, nil)
	scan, err := reader.OpenOrders(context.Background(), order)
	if err != nil || !scan.Contains || scan.Returned != 2 {
		t.Fatalf("OpenOrders() = %#v, %v", scan, err)
	}
	reader, _ = NewNoFillEvidenceReader(&fakeNoFillCLOB{openErr: errors.New("timeout")}, nil)
	if _, err := reader.OpenOrders(context.Background(), order); err == nil {
		t.Fatal("a failed open-order listing must be an error")
	}
}

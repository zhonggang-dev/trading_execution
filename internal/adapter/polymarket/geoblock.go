package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/UniPat-AI/trading_execution/internal/domain"
	"github.com/UniPat-AI/trading_execution/internal/port"
)

const maxGeoblockResponseBytes = 32 << 10

// GeographicEligibility is Polymarket's view of the server egress address.
// It intentionally contains no wallet or account information.
type GeographicEligibility struct {
	Blocked bool
	IP      string
	Country string
	Region  string
	Reason  string
}

// GeographicEligibilityChecker checks whether a new order may be submitted
// from the current egress address. Implementations must fail closed.
type GeographicEligibilityChecker interface {
	Check(ctx context.Context, executionAccountID string) (GeographicEligibility, error)
}

// GeoblockClientParams configures the official Polymarket geoblock endpoint.
type GeoblockClientParams struct {
	URL        string
	HTTPClient *http.Client
	Timeout    time.Duration
}

// GeoblockClient calls the official endpoint immediately before placement.
// It deliberately does not cache an allow result: a later order must not rely
// on an eligibility decision made for an older network route.
type GeoblockClient struct {
	url        *url.URL
	httpClient *http.Client
	timeout    time.Duration
}

// NewGeoblockClient constructs a strict, fail-closed eligibility client.
func NewGeoblockClient(params GeoblockClientParams) (*GeoblockClient, error) {
	endpoint := strings.TrimSpace(params.URL)
	if endpoint == "" {
		endpoint = "https://polymarket.com/api/geoblock"
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("Polymarket geoblock URL is invalid")
	}
	if params.Timeout == 0 {
		params.Timeout = 3 * time.Second
	}
	if params.Timeout < 250*time.Millisecond || params.Timeout > 30*time.Second {
		return nil, fmt.Errorf("Polymarket geoblock timeout must be between 250ms and 30s")
	}
	if params.HTTPClient == nil {
		params.HTTPClient = &http.Client{Timeout: params.Timeout}
	}
	return &GeoblockClient{url: parsed, httpClient: params.HTTPClient, timeout: params.Timeout}, nil
}

// Check returns a validated decision for the caller's public egress address.
func (client *GeoblockClient) Check(ctx context.Context, _ string) (GeographicEligibility, error) {
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, client.url.String(), nil)
	if err != nil {
		return GeographicEligibility{}, fmt.Errorf("build Polymarket geoblock request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return GeographicEligibility{}, fmt.Errorf("call Polymarket geoblock endpoint: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxGeoblockResponseBytes+1))
	if err != nil {
		return GeographicEligibility{}, fmt.Errorf("read Polymarket geoblock response: %w", err)
	}
	if len(body) > maxGeoblockResponseBytes {
		return GeographicEligibility{}, fmt.Errorf("Polymarket geoblock response exceeds %d bytes", maxGeoblockResponseBytes)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return GeographicEligibility{}, fmt.Errorf("Polymarket geoblock endpoint returned HTTP %d", response.StatusCode)
	}
	var payload struct {
		Blocked *bool  `json:"blocked"`
		IP      string `json:"ip"`
		Country string `json:"country"`
		Region  string `json:"region"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return GeographicEligibility{}, fmt.Errorf("decode Polymarket geoblock response: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return GeographicEligibility{}, fmt.Errorf("decode Polymarket geoblock response: trailing JSON value")
	}
	ip := strings.TrimSpace(payload.IP)
	country := strings.ToUpper(strings.TrimSpace(payload.Country))
	region := strings.TrimSpace(payload.Region)
	if payload.Blocked == nil || net.ParseIP(ip) == nil || !validCountryCode(country) {
		return GeographicEligibility{}, fmt.Errorf("Polymarket geoblock response omitted a valid blocked, ip, or country field")
	}
	return GeographicEligibility{Blocked: *payload.Blocked, IP: ip, Country: country, Region: region}, nil
}

// CLOBEligibilityCheckerParams combines the public egress signal with the
// authenticated account-level closed-only status used by CLOB V2.
type CLOBEligibilityCheckerParams struct {
	Geoblock           GeographicEligibilityChecker
	ClosedOnly         ClosedOnlyChecker
	APIExemptCountries []string
}

// ClosedOnlyChecker reads the authenticated CLOB account restriction.
type ClosedOnlyChecker interface {
	ClosedOnly(ctx context.Context, executionAccountID string) (bool, error)
}

// CLOBEligibilityChecker resolves the official documentation's frontend-only
// country exception without treating the public website flag as an API ban.
type CLOBEligibilityChecker struct {
	geoblock           GeographicEligibilityChecker
	closedOnly         ClosedOnlyChecker
	apiExemptCountries map[string]struct{}
}

// NewCLOBEligibilityChecker constructs the two-source placement policy. The
// exemption list should contain only countries that current official policy
// explicitly labels frontend-only; it cannot override account closed-only.
func NewCLOBEligibilityChecker(params CLOBEligibilityCheckerParams) (*CLOBEligibilityChecker, error) {
	if params.Geoblock == nil || params.ClosedOnly == nil {
		return nil, fmt.Errorf("geoblock and authenticated closed-only checkers are required")
	}
	exemptions := make(map[string]struct{}, len(params.APIExemptCountries))
	for _, raw := range params.APIExemptCountries {
		country := strings.ToUpper(strings.TrimSpace(raw))
		if !validCountryCode(country) {
			return nil, fmt.Errorf("invalid API-exempt country code %q", raw)
		}
		exemptions[country] = struct{}{}
	}
	return &CLOBEligibilityChecker{
		geoblock: params.Geoblock, closedOnly: params.ClosedOnly, apiExemptCountries: exemptions,
	}, nil
}

// Check requires both public egress evidence and a successful authenticated
// account check. closed_only always wins. A public blocked result is ignored
// only for an explicitly configured frontend-only country.
func (checker *CLOBEligibilityChecker) Check(
	ctx context.Context,
	executionAccountID string,
) (GeographicEligibility, error) {
	geographic, err := checker.geoblock.Check(ctx, executionAccountID)
	if err != nil {
		return GeographicEligibility{}, err
	}
	closedOnly, err := checker.closedOnly.ClosedOnly(ctx, executionAccountID)
	if err != nil {
		return GeographicEligibility{}, fmt.Errorf("check authenticated CLOB closed-only status: %w", err)
	}
	if closedOnly {
		geographic.Blocked = true
		geographic.Reason = "CLOB_ACCOUNT_CLOSED_ONLY"
		return geographic, nil
	}
	if !geographic.Blocked {
		geographic.Reason = "ELIGIBLE"
		return geographic, nil
	}
	if _, frontendOnly := checker.apiExemptCountries[geographic.Country]; frontendOnly {
		geographic.Blocked = false
		geographic.Reason = "FRONTEND_ONLY_COUNTRY_API_ELIGIBLE"
		return geographic, nil
	}
	geographic.Reason = "PUBLIC_GEOBLOCK"
	return geographic, nil
}

func validCountryCode(value string) bool {
	return len(value) == 2 && value[0] >= 'A' && value[0] <= 'Z' && value[1] >= 'A' && value[1] <= 'Z'
}

// EligibilityVenue prevents only new placements when location eligibility is
// blocked or unavailable. Cancels and read-only order reconciliation always
// bypass this gate so operators can reduce risk during an incident.
type EligibilityVenue struct {
	venue   port.Venue
	checker GeographicEligibilityChecker
}

// NewEligibilityVenue decorates a live venue with the placement-only gate.
func NewEligibilityVenue(venue port.Venue, checker GeographicEligibilityChecker) (*EligibilityVenue, error) {
	if venue == nil || checker == nil {
		return nil, fmt.Errorf("venue and geographic eligibility checker are required")
	}
	if strings.TrimSpace(venue.Name()) == "" {
		return nil, fmt.Errorf("venue name is required")
	}
	return &EligibilityVenue{venue: venue, checker: checker}, nil
}

func (venue *EligibilityVenue) Name() string { return venue.venue.Name() }

// Place checks the actual server egress immediately before any venue mutation.
func (venue *EligibilityVenue) Place(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	eligibility, err := venue.checker.Check(ctx, order.Intent.ExecutionAccountID)
	if err != nil {
		return port.VenueOrder{}, &port.VenueError{
			Kind:    port.VenueErrorRejected,
			Code:    "POLYMARKET_GEO_CHECK_UNAVAILABLE",
			Message: "new order rejected locally because geographic eligibility could not be proven",
			Cause:   err,
		}
	}
	if eligibility.Blocked {
		location := eligibility.Country
		if eligibility.Region != "" {
			location += "-" + eligibility.Region
		}
		return port.VenueOrder{}, &port.VenueError{
			Kind:    port.VenueErrorRejected,
			Code:    "POLYMARKET_GEO_BLOCKED",
			Message: fmt.Sprintf("new order rejected locally because Polymarket reports the server egress as blocked in %s", location),
		}
	}
	return venue.venue.Place(ctx, order)
}

// Cancel deliberately bypasses the placement gate.
func (venue *EligibilityVenue) Cancel(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Cancel(ctx, order)
}

// Get deliberately bypasses the placement gate.
func (venue *EligibilityVenue) Get(ctx context.Context, order domain.Order) (port.VenueOrder, error) {
	return venue.venue.Get(ctx, order)
}

var _ GeographicEligibilityChecker = (*GeoblockClient)(nil)
var _ GeographicEligibilityChecker = (*CLOBEligibilityChecker)(nil)
var _ ClosedOnlyChecker = (*TradingClient)(nil)
var _ port.Venue = (*EligibilityVenue)(nil)

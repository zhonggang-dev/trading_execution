package polymarketdata

import (
	"encoding/json"
	"fmt"
	"strings"
)

// maxDataAPIPages bounds one cursor walk; exceeding it fails closed.
const maxDataAPIPages = 20

// decodeDataAPIPage decodes the Data API v2 `{ data, pagination }` envelope.
// `data: null` is a documented empty result. A missing pagination object, or
// has_more without a cursor, fails closed so a partial walk is never mistaken
// for a complete snapshot. The returned cursor is empty on the last page.
func decodeDataAPIPage[T any](body []byte, resource string) ([]T, string, error) {
	var envelope struct {
		Data       []T `json:"data"`
		Pagination *struct {
			HasMore    *bool   `json:"has_more"`
			NextCursor *string `json:"next_cursor"`
		} `json:"pagination"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, "", fmt.Errorf("decode Data API %s: %w", resource, err)
	}
	if envelope.Pagination == nil || envelope.Pagination.HasMore == nil {
		return nil, "", fmt.Errorf("decode Data API %s: pagination.has_more is required", resource)
	}
	next := ""
	if envelope.Pagination.NextCursor != nil {
		next = strings.TrimSpace(*envelope.Pagination.NextCursor)
	}
	if *envelope.Pagination.HasMore != (next != "") {
		return nil, "", fmt.Errorf("decode Data API %s: has_more disagrees with next_cursor", resource)
	}
	return envelope.Data, next, nil
}

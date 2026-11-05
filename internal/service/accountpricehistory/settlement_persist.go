package accountpricehistory

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// PersistKnownSettlements makes verified local observations durable even when
// all price windows are complete and no network Fetch is needed. The zero-width
// evidence window uses the original confirmation time, never today's read time.
// Run must call this only outside dry-run mode, while holding its job lock.
func PersistKnownSettlements(ctx context.Context, store Store, targets []Window, data Dataset) error {
	assets := map[string]Asset{}
	for _, w := range targets {
		assets[w.Asset.Key()] = w.Asset
	}
	keys := make([]string, 0, len(assets))
	for key := range assets {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		verified, err := MergeSettlements(nil, data.Series[key].Settlements, data.ObservedAt)
		if err != nil {
			return err
		}
		for _, v := range verified {
			if !strings.HasPrefix(v.SourceReference, "LOCAL_POSITION:") {
				continue
			}
			if err = store.Save(ctx, Result{Window: Window{Asset: assets[key], From: v.EffectiveAt, To: v.EffectiveAt}, Series: Series{Settlements: []Settlement{v}}}); err != nil {
				return fmt.Errorf("persist local settlement: %w", err)
			}
		}
	}
	return nil
}

package usagemonitoring

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	monitoringrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagemonitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

const fallbackLogIntervalMS = int64(5 * time.Minute / time.Millisecond)

type Reader struct {
	store             *store.Store
	lastFallbackLogMS atomic.Int64
}

func New(store *store.Store) *Reader { return &Reader{store: store} }

func SupportsStatsFilter(filter store.AnalyticsFilter) bool {
	return monitoringrepo.SupportsStatsFilter(filter)
}

func SupportsEventProjectionFilter(filter store.AnalyticsFilter) bool {
	return monitoringrepo.SupportsEventProjectionFilter(filter)
}

func SupportsSelectorFilter(filter store.AnalyticsFilter) bool {
	return monitoringrepo.SupportsSelectorFilter(filter)
}

func PrefersEventProjection(filter store.AnalyticsFilter) bool {
	return monitoringrepo.PrefersEventProjection(filter)
}

// readDerived distinguishes unsupported/unavailable sources from failed reads.
// Only the former permit a raw fallback. In particular a terminal SQL, coverage
// or cancellation error must not start a second expensive query or silently
// replace retained history with the smaller live-event population.
func readDerived[T any](ctx context.Context, r *Reader, stage string, supported bool, load func() (T, store.UsageMonitoringState, bool, error)) (T, bool, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	if r == nil || r.store == nil || !supported {
		return zero, false, nil
	}
	rows, _, available, err := load()
	if err != nil {
		return zero, false, fmt.Errorf("%s: %w", stage, err)
	}
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	if !available {
		r.logFallback(stage + " unavailable")
		return zero, false, nil
	}
	return rows, true, nil
}

func (r *Reader) AccountStats(ctx context.Context, filter store.AnalyticsFilter) ([]store.AccountModelStat, bool, error) {
	return readDerived(ctx, r, "account_stats", SupportsEventProjectionFilter(filter), func() ([]store.AccountModelStat, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringAccountStats(ctx, filter)
	})
}

func (r *Reader) AccountWindowStats(ctx context.Context, windows []store.AccountWindowUsageQuery) ([]store.AccountWindowModelStat, bool, error) {
	return readDerived(ctx, r, "account_windows", true, func() ([]store.AccountWindowModelStat, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringAccountWindowStats(ctx, windows)
	})
}

func (r *Reader) APIKeyStats(ctx context.Context, filter store.AnalyticsFilter) ([]store.APIKeyModelStat, bool, error) {
	return readDerived(ctx, r, "api_key_stats", SupportsEventProjectionFilter(filter), func() ([]store.APIKeyModelStat, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringAPIKeyStats(ctx, filter)
	})
}

func (r *Reader) FilterOptions(ctx context.Context, filter store.AnalyticsFilter) (store.FilterOptionValues, bool, error) {
	return readDerived(ctx, r, "filter_options", SupportsEventProjectionFilter(filter), func() (store.FilterOptionValues, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringFilterOptions(ctx, filter)
	})
}

func (r *Reader) FilterSelectors(ctx context.Context, filter store.AnalyticsFilter) (store.FilterSelectorValues, bool, error) {
	return readDerived(ctx, r, "filter_selectors", SupportsEventProjectionFilter(filter), func() (store.FilterSelectorValues, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringFilterSelectors(ctx, filter)
	})
}

func (r *Reader) Aggregate(ctx context.Context, filter store.AnalyticsFilter) (store.Aggregate, bool, error) {
	return readDerived(ctx, r, "aggregate", SupportsEventProjectionFilter(filter), func() (store.Aggregate, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringAggregate(ctx, filter)
	})
}

func (r *Reader) ModelStats(ctx context.Context, filter store.AnalyticsFilter) ([]store.ModelStat, bool, error) {
	return readDerived(ctx, r, "model_stats", SupportsEventProjectionFilter(filter), func() ([]store.ModelStat, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringModelStats(ctx, filter)
	})
}

func (r *Reader) EventsCount(ctx context.Context, filter store.AnalyticsFilter) (int64, bool, error) {
	return readDerived(ctx, r, "events_count", SupportsEventProjectionFilter(filter) && PrefersEventProjection(filter), func() (int64, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringEventsCount(ctx, filter)
	})
}

func (r *Reader) EventsPage(ctx context.Context, filter store.AnalyticsFilter, beforeMS, beforeID int64, limit int) (store.EventsPage, bool, error) {
	return readDerived(ctx, r, "events_page", SupportsEventProjectionFilter(filter) && PrefersEventProjection(filter), func() (store.EventsPage, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringEventsPage(ctx, filter, beforeMS, beforeID, limit)
	})
}

func (r *Reader) HeaderSnapshots(ctx context.Context, sinceMS int64, limit int) ([]store.HeaderSnapshot, bool, error) {
	return readDerived(ctx, r, "header_snapshots", true, func() ([]store.HeaderSnapshot, store.UsageMonitoringState, bool, error) {
		return r.store.UsageMonitoringHeaderSnapshots(ctx, sinceMS, limit)
	})
}

func (r *Reader) logFallback(message string) {
	nowMS := time.Now().UnixMilli()
	lastMS := r.lastFallbackLogMS.Load()
	if lastMS > 0 && nowMS-lastMS < fallbackLogIntervalMS {
		return
	}
	if r.lastFallbackLogMS.CompareAndSwap(lastMS, nowMS) {
		log.Printf("[usage-monitoring] %s; falling back to usage_events", message)
	}
}

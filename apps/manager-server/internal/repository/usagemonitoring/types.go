package usagemonitoring

import (
	"context"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
)

const dayMS = int64(24 * time.Hour / time.Millisecond)

type AnalyticsFilter = usageevent.AnalyticsFilter
type Aggregate = usageevent.Aggregate
type ModelStat = usageevent.ModelStat
type AccountModelStat = usageevent.AccountModelStat
type APIKeyModelStat = usageevent.APIKeyModelStat
type FilterOptionValues = usageevent.FilterOptionValues
type FilterSelectorValues = usageevent.FilterSelectorValues
type AccountSelectorValue = usageevent.AccountSelectorValue
type APIKeySelectorValue = usageevent.APIKeySelectorValue
type EventsPage = usageevent.EventsPage
type EventPageItem = usageevent.EventPageItem
type HeaderSnapshot = usageevent.HeaderSnapshot
type AccountWindowUsageQuery = usageevent.AccountWindowUsageQuery
type AccountWindowModelStat = usageevent.AccountWindowModelStat

// StatsBuildVersion versions the statistics algorithm independently of the
// retained projection schema and the pricing structure. Version 2 repairs
// historical raw-only rebuilds and success/failure grouping.
const StatsBuildVersion = "stats-build-2"

// StatsStructureRevision binds a statistics generation to both its build
// algorithm and the pricing structure. Numeric rate edits do not change it.
func StatsStructureRevision(pricingRevision string) string {
	return StatsBuildVersion + ":" + pricingRevision
}

func CurrentStatsStructureRevision(ctx context.Context, db usagepricing.RowQuerier) (string, error) {
	revision, err := usagepricing.StructureRevision(ctx, db)
	if err != nil {
		return "", err
	}
	return StatsStructureRevision(revision), nil
}

func currentStructureRevision(ctx context.Context, db usagepricing.RowQuerier) (string, error) {
	return CurrentStatsStructureRevision(ctx, db)
}

func floorDayMS(value int64) int64 {
	return floorIntervalMS(value, dayMS)
}

func ceilDayMS(value int64) int64 {
	floor := floorDayMS(value)
	if floor == value {
		return value
	}
	return floor + dayMS
}

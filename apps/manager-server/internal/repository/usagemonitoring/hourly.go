package usagemonitoring

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

const hourMS = int64(3_600_000)

func (r *repository) CatchUpHourlyStats(ctx context.Context, limit int, nowMS int64) (CatchUpResult, error) {
	if !r.mysqlHourly {
		return CatchUpResult{}, nil
	}
	if limit <= 0 || limit > defaultBatchLimit {
		limit = defaultBatchLimit
	}
	return r.catchUp(ctx, HourlyStatsRollupName, limit, nowMS, func(ctx context.Context, tx *sql.Tx, revision, source string, afterID, throughID, updatedAt int64) error {
		if err := upsertAccountBatch(ctx, tx, "usage_monitoring_account_hourly_rollups_v1", hourMS, revision, source, afterID, throughID, updatedAt); err != nil {
			return err
		}
		return upsertAPIKeyBatch(ctx, tx, "usage_monitoring_api_key_hourly_rollups_v1", hourMS, revision, source, afterID, throughID, updatedAt)
	})
}

func (r *repository) CatchUpHourlyMetadata(ctx context.Context, limit int, nowMS int64) (CatchUpResult, error) {
	if !r.mysqlHourly {
		return CatchUpResult{}, nil
	}
	if limit <= 0 || limit > defaultBatchLimit {
		limit = defaultBatchLimit
	}
	return r.catchUp(ctx, HourlyMetadataRollupName, limit, nowMS, func(ctx context.Context, tx *sql.Tx, _, source string, afterID, throughID, updatedAt int64) error {
		return upsertSelectorBatch(ctx, tx, "usage_monitoring_selector_hourly_rollups_v1", hourMS, source, afterID, throughID, updatedAt)
	})
}

func (r *repository) hourlyReadState(ctx context.Context, tx *sql.Tx, name, revision string) (State, bool, error) {
	if !r.mysqlHourly {
		return State{}, false, nil
	}
	state, err := stateQuery(ctx, tx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	ready := state.SchemaVersion == SchemaVersion && state.StructureRevision == revision && (state.Status == "ready" || state.Status == "catching_up")
	return state, ready, nil
}

type monitoringSpan struct {
	from, to, grain int64
	state           State
}

// monitoringSpans is a disjoint time partition. Daily interiors keep their own
// watermark; hourly fringes use the independent hourly watermark. Event tails
// are selected by ID inside each span, never by timestamp, so late arrivals
// cannot be omitted or double counted. A grain of zero is an exact event edge.
func monitoringSpans(from, to int64, daily State, useDaily bool, hourly State, useHourly bool) []monitoringSpan {
	var spans []monitoringSpan
	var appendHours func(int64, int64)
	appendHours = func(start, end int64) {
		if start >= end {
			return
		}
		a, b := ceilIntervalMS(start, hourMS), floorIntervalMS(end, hourMS)
		if !useHourly || a >= b {
			spans = append(spans, monitoringSpan{from: start, to: end})
			return
		}
		if start < a {
			spans = append(spans, monitoringSpan{from: start, to: a})
		}
		spans = append(spans, monitoringSpan{from: a, to: b, grain: hourMS, state: hourly})
		if b < end {
			spans = append(spans, monitoringSpan{from: b, to: end})
		}
	}
	a, b := ceilIntervalMS(from, dayMS), floorIntervalMS(to, dayMS)
	if !useDaily || a >= b {
		appendHours(from, to)
		return spans
	}
	appendHours(from, a)
	spans = append(spans, monitoringSpan{from: a, to: b, grain: dayMS, state: daily})
	appendHours(b, to)
	return spans
}

func floorIntervalMS(value, interval int64) int64 {
	return value - ((value%interval + interval) % interval)
}
func ceilIntervalMS(value, interval int64) int64 {
	floor := floorIntervalMS(value, interval)
	if floor == value {
		return floor
	}
	return floor + interval
}

func (r *repository) loadAccountComposed(ctx context.Context, tx *sql.Tx, daily State, dailyAvailable bool, projection State, complete bool, revision string, filter AnalyticsFilter, grouped map[accountStatKey]*accountStatAccumulator) error {
	hourly, available, err := r.hourlyReadState(ctx, tx, HourlyStatsRollupName, revision)
	if err != nil {
		return err
	}
	for _, span := range monitoringSpans(filter.FromMS, filter.ToMS, daily, dailyAvailable, hourly, available) {
		part := filter
		part.FromMS, part.ToMS = span.from, span.to
		if span.grain == 0 {
			err = mergeProjectedAccountStats(ctx, tx, projection.CoverageEventID, complete, part, eventSourceOptions{}, grouped)
		} else {
			table := "usage_monitoring_account_daily_rollups_v1"
			if span.grain == hourMS {
				table = "usage_monitoring_account_hourly_rollups_v1"
			}
			err = loadAccountGranularRange(ctx, tx, span.state, projection.CoverageEventID, complete, revision, part, grouped, table, span.grain)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *repository) loadAPIKeyComposed(ctx context.Context, tx *sql.Tx, daily State, dailyAvailable bool, projection State, complete bool, revision string, filter AnalyticsFilter, grouped map[apiKeyStatKey]*apiKeyStatAccumulator) error {
	hourly, available, err := r.hourlyReadState(ctx, tx, HourlyStatsRollupName, revision)
	if err != nil {
		return err
	}
	for _, span := range monitoringSpans(filter.FromMS, filter.ToMS, daily, dailyAvailable, hourly, available) {
		part := filter
		part.FromMS, part.ToMS = span.from, span.to
		if span.grain == 0 {
			err = mergeProjectedAPIKeyStats(ctx, tx, projection.CoverageEventID, complete, part, eventSourceOptions{}, grouped)
		} else {
			table := "usage_monitoring_api_key_daily_rollups_v1"
			if span.grain == hourMS {
				table = "usage_monitoring_api_key_hourly_rollups_v1"
			}
			err = loadAPIKeyGranularRange(ctx, tx, span.state, projection.CoverageEventID, complete, revision, part, grouped, table, span.grain)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *repository) loadSelectorsComposed(ctx context.Context, tx *sql.Tx, daily, projection State, complete bool, filter AnalyticsFilter, grouped map[selectorKey]*selectorRow) error {
	hourly, available, err := r.hourlyReadState(ctx, tx, HourlyMetadataRollupName, usageidentity.ModelFormatVersion)
	if err != nil {
		return err
	}
	dailyAvailable := daily.SchemaVersion == SchemaVersion && daily.StructureRevision == usageidentity.ModelFormatVersion && daily.Status != "clearing"
	for _, span := range monitoringSpans(filter.FromMS, filter.ToMS, daily, dailyAvailable, hourly, available) {
		part := filter
		part.FromMS, part.ToMS = span.from, span.to
		if span.grain != 0 {
			table := "usage_monitoring_selector_daily_rollups_v1"
			if span.grain == hourMS {
				table = "usage_monitoring_selector_hourly_rollups_v1"
			}
			if err := mergeStoredSelectorTableRows(ctx, tx, span.from, span.to, grouped, table); err != nil {
				return err
			}
		}
		if err := mergeProjectedSelectorRows(ctx, tx, projection.CoverageEventID, complete, part, eventSourceOptions{AfterID: span.state.CoverageEventID, UseAfter: span.grain != 0, DirectProjectedSearch: mysql.IsMySQL(r.db)}, grouped); err != nil {
			return err
		}
	}
	return nil
}

// Digest predicates narrow the new compact indexes; full text equality remains
// mandatory in every caller. Framing is byte length (native OCTET_LENGTH), not
// the compatibility layer's character-length interpretation of LENGTH.
func revisionDigest(value string) []byte { sum := sha256.Sum256([]byte(value)); return sum[:] }
func framedScopeDigest(first, second string) []byte {
	return revisionDigest(fmt.Sprintf("%d:%s;%d:%s;", len(first), first, len(second), second))
}

// Core totals/models share the account grain, including the Codex provenance
// residual. The callbacks only read; every span uses the caller's snapshot.
func (r *repository) composeCoreSpans(ctx context.Context, tx *sql.Tx, daily State, dailyAvailable bool, revision string, filter AnalyticsFilter, stored func(AnalyticsFilter, string) error, projected func(AnalyticsFilter, eventSourceOptions) error) error {
	hourly, available, err := r.hourlyReadState(ctx, tx, HourlyStatsRollupName, revision)
	if err != nil {
		return err
	}
	for _, span := range monitoringSpans(filter.FromMS, filter.ToMS, daily, dailyAvailable, hourly, available) {
		part := filter
		part.FromMS, part.ToMS = span.from, span.to
		if span.grain == 0 {
			if err := projected(part, eventSourceOptions{}); err != nil {
				return err
			}
			continue
		}
		table := "usage_monitoring_account_daily_rollups_v1"
		if span.grain == hourMS {
			table = "usage_monitoring_account_hourly_rollups_v1"
		}
		if err := stored(part, table); err != nil {
			return err
		}
		if err := projected(part, eventSourceOptions{AfterID: span.state.CoverageEventID, UseAfter: true}); err != nil {
			return err
		}
		if err := projected(part, eventSourceOptions{MaxID: span.state.CoverageEventID, UseMax: true, CodexMarkerOnly: true}); err != nil {
			return err
		}
	}
	return nil
}

func (r *repository) loadComposedAggregate(ctx context.Context, tx *sql.Tx, daily State, dailyAvailable bool, projectionCoverage int64, complete bool, revision string, filter AnalyticsFilter) (Aggregate, error) {
	var accumulator dailyAggregateAccumulator
	err := r.composeCoreSpans(ctx, tx, daily, dailyAvailable, revision, filter,
		func(part AnalyticsFilter, table string) error {
			return mergeStoredTableAggregate(ctx, tx, revision, part, part.FromMS, part.ToMS, &accumulator, table)
		},
		func(part AnalyticsFilter, options eventSourceOptions) error {
			return mergeProjectedAggregate(ctx, tx, projectionCoverage, complete, part, options, &accumulator)
		})
	return accumulator.result(), err
}

func (r *repository) loadComposedModelStats(ctx context.Context, tx *sql.Tx, daily State, dailyAvailable bool, projectionCoverage int64, complete bool, revision string, filter AnalyticsFilter) ([]ModelStat, error) {
	grouped := map[dailyModelStatKey]*ModelStat{}
	err := r.composeCoreSpans(ctx, tx, daily, dailyAvailable, revision, filter,
		func(part AnalyticsFilter, table string) error {
			return mergeStoredTableModelStats(ctx, tx, revision, part, part.FromMS, part.ToMS, grouped, table)
		},
		func(part AnalyticsFilter, options eventSourceOptions) error {
			return mergeProjectedModelStats(ctx, tx, projectionCoverage, complete, part, options, grouped)
		})
	return sortedDailyModelStats(grouped), err
}

// Obsolete hourly generations have no readers after revision publication. Delete
// them in bounded pieces; never clear projection/history or the live generation.
func cleanupHourlyRevisionRows(ctx context.Context, tx *sql.Tx, name, revision string, limit int, native bool) (bool, error) {
	if revision == "" {
		return false, nil
	}
	var tables []string
	column := "structure_revision"
	switch name {
	case HourlyStatsRollupName:
		tables = []string{"usage_monitoring_account_hourly_rollups_v1", "usage_monitoring_api_key_hourly_rollups_v1"}
	case HourlyMetadataRollupName:
		tables = []string{"usage_monitoring_selector_hourly_rollups_v1"}
		column = "model_format_revision"
	default:
		return false, nil
	}
	remaining := limit
	for _, table := range tables {
		if native {
			deleted, pending, err := mysql.CleanupHourlyRevisionBatch(ctx, tx, table, revision, remaining)
			if err != nil {
				return false, err
			}
			remaining -= int(deleted)
			if pending {
				return true, nil
			}
			continue
		}
		// SQLite's test-only hourly tables retain the portable reference path;
		// runtime MySQL must never translate this unindexed full-text predicate.
		if remaining > 0 {
			query := `delete from ` + table + ` where rowid in (select rowid from ` + table + ` where ` + column + ` <> ? limit ?)`
			result, err := tx.ExecContext(ctx, query, revision, remaining)
			if err != nil {
				return false, err
			}
			deleted, err := result.RowsAffected()
			if err != nil {
				return false, err
			}
			remaining -= int(deleted)
		}
		var pending bool
		if err := tx.QueryRowContext(ctx, `select exists(select 1 from `+table+` where `+column+` <> ? limit 1)`, revision).Scan(&pending); err != nil {
			return false, err
		}
		if pending {
			return true, nil
		}
	}
	return false, nil
}

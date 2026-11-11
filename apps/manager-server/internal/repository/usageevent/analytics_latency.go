package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// LatencyMetrics contains independent positive latency and TTFT populations.
// Retained projections cannot supply historical TTFT; these metrics intentionally
// retain the raw-history coverage of the legacy readers.
type LatencyMetrics struct {
	Summary  LatencySummary
	Timeline []LatencyPercentiles
}

func (r *repository) LatencyMetricsWithFilter(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location, needSummary, needBuckets bool) (LatencyMetrics, error) {
	if !needSummary && !needBuckets {
		return LatencyMetrics{}, ctx.Err()
	}
	// Disabling MySQL acceleration must select a different execution strategy,
	// not merely leave the same window query for the adapter to compile later.
	if r.mysqlBackend && !r.mysqlAnalytics {
		return r.latencyMetricsReference(ctx, filter, granularity, location, needSummary, needBuckets)
	}
	plan := analyticsSQLBuckets{bucket: "0"}
	available := true
	var err error
	if needBuckets {
		plan, available, err = analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, granularity, location, false)
	}
	if err != nil {
		return LatencyMetrics{}, err
	}
	if available {
		return r.latencyMetricsOn(ctx, r.db, filter, plan, needSummary, needBuckets)
	}
	return r.latencyMetricsMapped(ctx, filter, granularity, location, needSummary)
}

func (r *repository) latencyMetricsOn(ctx context.Context, queryer SQLQueryer, filter AnalyticsFilter, plan analyticsSQLBuckets, summary, buckets bool) (LatencyMetrics, error) {
	query, args := latencyMetricsSQL(filter, plan, summary, buckets)
	if r.mysqlAnalytics && r.mysqlBackend {
		var err error
		query, args, err = mysql.CompileSQL(query, args...)
		if err != nil {
			return LatencyMetrics{}, err
		}
		query = mysql.Native(query)
	}
	return readLatencyMetricsOn(ctx, queryer, query, args)
}

// latencyMetricsMapped handles ranges too large for a calendar interval map.
// Discover only timestamp runs, not sample values, in one snapshot. Every run
// with the same Go bucket stays in the same SQL rank partition, including a
// repeated hour/day whose samples are not consecutive. The map is O(output
// buckets + transitions), never O(latency/TTFT samples). We never combine P95s
// from separate chunks, nor retain two copies of all samples on the Go heap.
func (r *repository) latencyMetricsMapped(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location, summary bool) (LatencyMetrics, error) {
	opts := &sql.TxOptions{ReadOnly: true}
	if r.mysqlBackend {
		opts.Isolation = sql.LevelRepeatableRead
	}
	tx, err := sqlwrite.BeginTx(ctx, r.db, opts)
	if err != nil {
		return LatencyMetrics{}, err
	}
	defer tx.Rollback()
	where, args := analyticsWhere(filter)
	rows, err := tx.QueryContext(ctx, `select distinct timestamp_ms from usage_events `+where+` and (latency_ms > 0 or ttft_ms > 0) order by timestamp_ms`, args...)
	if err != nil {
		return LatencyMetrics{}, err
	}
	type interval struct{ from, to int64 }
	byBucket := make(map[int64][]interval)
	var run interval
	var lastBucket int64
	hasRun := false
	for rows.Next() {
		var timestamp int64
		if err := rows.Scan(&timestamp); err != nil {
			rows.Close()
			return LatencyMetrics{}, err
		}
		bucket := usage.AnalyticsBucketMS(timestamp, granularity, location)
		if !hasRun || bucket != lastBucket {
			if hasRun {
				byBucket[lastBucket] = append(byBucket[lastBucket], run)
			}
			lastBucket, run, hasRun = bucket, interval{from: timestamp, to: timestamp}, true
		} else {
			run.to = timestamp
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return LatencyMetrics{}, err
	}
	if hasRun {
		byBucket[lastBucket] = append(byBucket[lastBucket], run)
	}
	result := LatencyMetrics{Timeline: []LatencyPercentiles{}}
	if summary {
		global, err := r.latencyMetricsOn(ctx, tx, filter, analyticsSQLBuckets{bucket: "0"}, true, false)
		if err != nil {
			return LatencyMetrics{}, err
		}
		result.Summary = global.Summary
	}
	keys := make([]int64, 0, len(byBucket))
	for bucket := range byBucket {
		keys = append(keys, bucket)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for start := 0; start < len(keys); {
		values := []string{}
		bindings := []any{}
		partFilter := filter
		partFilter.FromMS, partFilter.ToMS = filter.ToMS, filter.FromMS
		end := start
		for end < len(keys) {
			intervals := byBucket[keys[end]]
			if len(values)+len(intervals) > maxAnalyticsBucketMapHours {
				break
			}
			for _, span := range intervals {
				values = append(values, "(?, ?, ?)")
				bindings = append(bindings, span.from, span.to, keys[end])
				partFilter.FromMS = min(partFilter.FromMS, span.from)
				// span.to < filter.ToMS, so this inclusive-to-exclusive
				// conversion cannot overflow even at extreme request bounds.
				partFilter.ToMS = max(partFilter.ToMS, span.to+1)
			}
			end++
		}
		if end == start {
			return LatencyMetrics{}, fmt.Errorf("analytics bucket has too many timezone intervals")
		}
		plan := analyticsSQLBuckets{
			cte:    ", analytics_bucket_map(from_ms, to_ms, bucket_ms) as (values " + strings.Join(values, ",") + ")",
			join:   " join analytics_bucket_map on timestamp_ms >= analytics_bucket_map.from_ms and timestamp_ms <= analytics_bucket_map.to_ms",
			bucket: "analytics_bucket_map.bucket_ms", args: bindings,
		}
		part, err := r.latencyMetricsOn(ctx, tx, partFilter, plan, false, true)
		if err != nil {
			return LatencyMetrics{}, err
		}
		result.Timeline = append(result.Timeline, part.Timeline...)
		start = end
	}
	if err := tx.Commit(); err != nil {
		return LatencyMetrics{}, err
	}
	return result, nil
}

// latencyMetricsSQL expands each positive metric into its own sample population
// and requested scopes before ranking. This is exact nearest rank, not a claim
// of one physical scan/sort: a backend may materialize and sort partitions.
func latencyMetricsSQL(filter AnalyticsFilter, plan analyticsSQLBuckets, summary, buckets bool) (string, []any) {
	where, args := analyticsWhere(filter)
	scopes := "select 0 as scope"
	if !summary {
		scopes = "select 1 as scope"
	} else if buckets {
		scopes += " union all select 1"
	}
	query := `with filtered_events as (select timestamp_ms, latency_ms, ttft_ms from usage_events ` + where + ` and (latency_ms > 0 or ttft_ms > 0))` + plan.cte + `, metric_kinds as (select 0 as kind union all select 1), scopes as (` + scopes + `), samples as (
		select scope, case when scope = 0 then 0 else ` + plan.bucket + ` end as bucket, kind,
			case when kind = 0 then latency_ms else ttft_ms end as value
		from filtered_events` + plan.join + ` cross join metric_kinds cross join scopes
		where (kind = 0 and latency_ms > 0) or (kind = 1 and ttft_ms > 0)
	), ranked as (
		select scope, bucket, kind, value,
			row_number() over (partition by scope, bucket, kind order by value) as sample_number,
			count(*) over (partition by scope, bucket, kind) as sample_count
		from samples
	) select scope, bucket,
		max(case when kind = 0 then value end), max(case when kind = 1 then value end)
		from ranked where sample_number = (sample_count * 95 + 99) / 100
		group by scope, bucket order by scope, bucket`
	return query, append(args, plan.args...)
}

func (r *repository) readLatencyMetricsSQL(ctx context.Context, query string, args []any) (LatencyMetrics, error) {
	return readLatencyMetricsOn(ctx, r.db, query, args)
}

func readLatencyMetricsOn(ctx context.Context, queryer SQLQueryer, query string, args []any) (LatencyMetrics, error) {
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return LatencyMetrics{}, err
	}
	defer rows.Close()
	result := LatencyMetrics{Timeline: []LatencyPercentiles{}}
	for rows.Next() {
		var scope int
		var point LatencyPercentiles
		if err := rows.Scan(&scope, &point.BucketMS, &point.P95LatencyMS, &point.P95TTFTMS); err != nil {
			return LatencyMetrics{}, err
		}
		switch scope {
		case 0:
			result.Summary = LatencySummary{P95LatencyMS: point.P95LatencyMS, P95TTFTMS: point.P95TTFTMS}
		case 1:
			result.Timeline = append(result.Timeline, point)
		default:
			return LatencyMetrics{}, fmt.Errorf("invalid percentile scope %d", scope)
		}
	}
	return result, rows.Err()
}

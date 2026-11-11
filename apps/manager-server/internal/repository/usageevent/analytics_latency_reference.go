package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// latencyReferencePopulation keeps only a count, an ordinal and the chosen sample,
// never an array of raw samples. The two metrics have independent populations.
type latencyReferencePopulation struct {
	count int64
	seen  int64
	p95   sql.NullFloat64
}

// latencyReferenceRank is ceil(count * 95 / 100), without multiplication overflow.
func latencyReferenceRank(count int64) int64 { return count - count/20 }

func (p *latencyReferencePopulation) observe(value float64) {
	p.seen++
	if p.seen == latencyReferenceRank(p.count) {
		p.p95 = sql.NullFloat64{Float64: value, Valid: true}
	}
}

type latencyReferencePopulations [2]latencyReferencePopulation

// latencyMetricsReference is the non-accelerated MySQL route. Count positive
// populations by Go bucket, then stream each metric in value order and keep only
// its nearest-rank sample. Global ordering also orders every bucket's subsequence,
// even for a repeated hour/day whose timestamps form disconnected intervals.
//
// All three passes share one read-only repeatable-read snapshot on MySQL, so
// ingestion/retention cannot change counts between passes. This intentionally
// transfers samples and may require database sorts; it is not a performance claim
// or the opt-in scoped/window plan. Go state is O(output buckets), independent of
// the number of samples per bucket. There is no P95 averaging or history bound.
func (r *repository) latencyMetricsReference(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location, summary, buckets bool) (LatencyMetrics, error) {
	if !summary && !buckets {
		return LatencyMetrics{}, ctx.Err()
	}
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
	global := latencyReferencePopulations{}
	var byBucket map[int64]*latencyReferencePopulations
	if buckets {
		byBucket = make(map[int64]*latencyReferencePopulations)
	}
	if err := countLatencyReference(ctx, tx, where, args, granularity, location, &global, byBucket); err != nil {
		return LatencyMetrics{}, err
	}
	for metric, column := range []string{"latency_ms", "ttft_ms"} {
		if global[metric].count == 0 {
			continue
		}
		if err := rankLatencyReference(ctx, tx, where, args, column, metric, granularity, location, &global, byBucket); err != nil {
			return LatencyMetrics{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return LatencyMetrics{}, err
	}
	if err := tx.Commit(); err != nil {
		return LatencyMetrics{}, err
	}

	result := LatencyMetrics{Timeline: []LatencyPercentiles{}}
	if summary {
		result.Summary = LatencySummary{P95LatencyMS: global[0].p95, P95TTFTMS: global[1].p95}
	}
	for bucket, populations := range byBucket {
		if err := ctx.Err(); err != nil {
			return LatencyMetrics{}, err
		}
		result.Timeline = append(result.Timeline, LatencyPercentiles{
			BucketMS: bucket, P95LatencyMS: populations[0].p95, P95TTFTMS: populations[1].p95,
		})
	}
	if err := ctx.Err(); err != nil {
		return LatencyMetrics{}, err
	}
	// Only the output buckets are sorted in Go, not raw metric populations.
	sort.Slice(result.Timeline, func(i, j int) bool { return result.Timeline[i].BucketMS < result.Timeline[j].BucketMS })
	if err := ctx.Err(); err != nil {
		return LatencyMetrics{}, err
	}
	return result, nil
}

func countLatencyReference(ctx context.Context, tx *sql.Tx, where string, args []any, granularity string, location *time.Location, global *latencyReferencePopulations, byBucket map[int64]*latencyReferencePopulations) error {
	rows, err := tx.QueryContext(ctx, `select timestamp_ms,
		case when latency_ms > 0 then 1 else 0 end,
		case when ttft_ms > 0 then 1 else 0 end
		from usage_events `+where+` and (latency_ms > 0 or ttft_ms > 0)`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var timestamp int64
		var positive [2]int64
		if err := rows.Scan(&timestamp, &positive[0], &positive[1]); err != nil {
			return err
		}
		for metric := range positive {
			global[metric].count += positive[metric]
		}
		if byBucket != nil {
			bucket := usage.AnalyticsBucketMS(timestamp, granularity, location)
			populations := byBucket[bucket]
			if populations == nil {
				populations = &latencyReferencePopulations{}
				byBucket[bucket] = populations
			}
			for metric := range positive {
				populations[metric].count += positive[metric]
			}
		}
	}
	return rows.Err()
}

func rankLatencyReference(ctx context.Context, tx *sql.Tx, where string, args []any, column string, metric int, granularity string, location *time.Location, global *latencyReferencePopulations, byBucket map[int64]*latencyReferencePopulations) error {
	// column is selected only from the two internal literals above; all filters
	// remain bound and identical to the population-count pass.
	rows, err := tx.QueryContext(ctx, `select timestamp_ms, `+column+` from usage_events `+where+` and `+column+` > 0 order by `+column, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var timestamp int64
		var value float64
		if err := rows.Scan(&timestamp, &value); err != nil {
			return err
		}
		global[metric].observe(value)
		if byBucket != nil {
			bucket := usage.AnalyticsBucketMS(timestamp, granularity, location)
			populations := byBucket[bucket]
			if populations == nil || populations[metric].seen >= populations[metric].count {
				return fmt.Errorf("percentile reference population changed within snapshot")
			}
			populations[metric].observe(value)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if global[metric].seen != global[metric].count {
		return fmt.Errorf("percentile reference population changed within snapshot")
	}
	return nil
}

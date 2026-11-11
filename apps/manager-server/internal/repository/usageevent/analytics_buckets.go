package usageevent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// analyticsSQLBuckets describes a partition of the *entire* requested range.
// An unavailable partition must use the reference reader, never a partial map.
// Expressions intentionally use the adapter's integer arithmetic vocabulary.
type analyticsSQLBuckets struct {
	cte     string
	join    string
	bucket  string
	hour    string
	weekday string
	args    []any
}

// analyticsSQLBucketPlan uses arithmetic only where it agrees with Go's calendar
// bucket construction. In a repeated hour, time.Date may choose the other offset;
// similarly a day's midnight may have a different offset from its last hour.
// All other ranges get exact, disjoint Go-derived intervals, including partial
// hours, fractional offsets and transitions at a non-hour boundary. No MySQL
// timezone tables or SQL timestamp conversion are involved.
func analyticsSQLBucketPlan(ctx context.Context, fromMS, toMS int64, granularity string, location *time.Location, clock bool) (analyticsSQLBuckets, bool, error) {
	if err := ctx.Err(); err != nil {
		return analyticsSQLBuckets{}, false, err
	}
	if location == nil {
		location = time.UTC
	}
	if fromMS >= toMS {
		return analyticsSQLBuckets{bucket: "0", hour: "0", weekday: "0"}, true, nil
	}
	// Avoid arithmetic/time.Date overflow for exotic int64 endpoints. These
	// unsupported ranges retain the exact reference implementation.
	const safeTimeMS = int64(1 << 62)
	if fromMS < -safeTimeMS || toMS > safeTimeMS {
		return analyticsSQLBuckets{}, false, nil
	}
	const hourMS = int64(time.Hour / time.Millisecond)
	const dayMS = 24 * hourMS
	sizeMS := hourMS
	if granularity == "day" && !clock {
		sizeMS = dayMS
	}
	first := time.UnixMilli(fromMS).In(location)
	_, offsetSeconds := first.Zone()
	offsetMS := int64(offsetSeconds) * 1000
	_, zoneEnd := first.ZoneBounds()
	if zoneEnd.IsZero() || zoneEnd.UnixMilli() >= toMS {
		floor := func(value int64) int64 {
			return value - ((value%sizeMS + sizeMS) % sizeMS)
		}
		if clock || (floor(fromMS+offsetMS)-offsetMS == usage.AnalyticsBucketMS(fromMS, granularity, location) &&
			floor(toMS-1+offsetMS)-offsetMS == usage.AnalyticsBucketMS(toMS-1, granularity, location)) {
			localMS := fmt.Sprintf("(timestamp_ms + (%d))", offsetMS)
			dayRemainder := fmt.Sprintf("((%s %% %d + %d) %% %d)", localMS, dayMS, dayMS, dayMS)
			return analyticsSQLBuckets{
				bucket:  fmt.Sprintf("(%s - ((%s %% %d + %d) %% %d) - (%d))", localMS, localMS, sizeMS, sizeMS, sizeMS, offsetMS),
				hour:    fmt.Sprintf("(%s / %d)", dayRemainder, hourMS),
				weekday: fmt.Sprintf("((((%s - %s) / %d + 4) %% 7 + 7) %% 7)", localMS, dayRemainder, dayMS),
			}, true, nil
		}
	}

	// The bound matches the existing credential timeline map. Stop before
	// issuing SQL if it is exceeded; callers explicitly fall back, without
	// hiding errors from an attempted query or truncating the requested range.
	values := make([]string, 0)
	args := make([]any, 0)
	for startMS := fromMS; startMS < toMS; {
		if err := ctx.Err(); err != nil {
			return analyticsSQLBuckets{}, false, err
		}
		if len(values) == maxAnalyticsBucketMapHours {
			return analyticsSQLBuckets{}, false, nil
		}
		tm := time.UnixMilli(startMS).In(location)
		elapsedMS := int64(tm.Minute())*60_000 + int64(tm.Second())*1000 + int64(tm.Nanosecond()/1_000_000)
		if sizeMS == dayMS {
			elapsedMS += int64(tm.Hour()) * hourMS
		}
		endMS := min(startMS+sizeMS-elapsedMS, toMS)
		_, end := tm.ZoneBounds()
		if !end.IsZero() && end.UnixMilli() > startMS {
			endMS = min(endMS, end.UnixMilli())
		}
		if endMS <= startMS {
			return analyticsSQLBuckets{}, false, nil
		}
		values = append(values, "(?, ?, ?, ?, ?)")
		args = append(args, startMS, endMS, usage.AnalyticsBucketMS(startMS, granularity, location), tm.Hour(), int(tm.Weekday()))
		startMS = endMS
	}
	return analyticsSQLBuckets{
		cte:     ", analytics_bucket_map(from_ms, to_ms, bucket_ms, local_hour, local_weekday) as (values " + strings.Join(values, ", ") + ")",
		join:    " join analytics_bucket_map on timestamp_ms >= analytics_bucket_map.from_ms and timestamp_ms < analytics_bucket_map.to_ms",
		bucket:  "analytics_bucket_map.bucket_ms",
		hour:    "analytics_bucket_map.local_hour",
		weekday: "analytics_bucket_map.local_weekday",
		args:    args,
	}, true, nil
}

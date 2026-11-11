package usage

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"
)

const analyticsHourMS = int64(time.Hour / time.Millisecond)

// AnalyticsBucketMS resolves an event timestamp to the start of its local
// analytics hour or day bucket.
func AnalyticsBucketMS(timestampMS int64, granularity string, location *time.Location) int64 {
	if location == nil {
		location = time.UTC
	}
	tm := time.UnixMilli(timestampMS).In(location)
	if granularity == "day" {
		return time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, location).UnixMilli()
	}
	return time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), 0, 0, 0, location).UnixMilli()
}

// AnalyticsBucketInterval is one half-open UTC interval belonging to a bucket.
// A backwards transition across midnight can give a day disconnected membership.
// Bucket IDs are time.Date results, not necessarily the first matching instant.
type AnalyticsBucketInterval struct {
	FromMS int64 `json:"from_ms"`
	ToMS   int64 `json:"to_ms"`
}

// AnalyticsBucketIntervals inverts AnalyticsBucketMS without assuming that
// membership is monotone in UTC. It intersects candidate civil hours/days with
// constant-offset timezone intervals, then coalesces adjacent matching pieces.
// This also respects time.Date's choice of offset at ambiguous/missing boundaries.
func AnalyticsBucketIntervals(ctx context.Context, bucketMS int64, granularity string, location *time.Location) ([]AnalyticsBucketInterval, error) {
	intervals, err := AnalyticsBucketIntervalsForBuckets(ctx, []int64{bucketMS}, granularity, location)
	return intervals[bucketMS], err
}

// AnalyticsBucketIntervalsForBuckets shares transition discovery across a
// response's bucket IDs. Its memory scales with timezone transitions and output
// intervals, never raw events. It does not fill or truncate the supplied buckets.
func AnalyticsBucketIntervalsForBuckets(ctx context.Context, buckets []int64, granularity string, location *time.Location) (map[int64][]AnalyticsBucketInterval, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[int64][]AnalyticsBucketInterval, len(buckets))
	if len(buckets) == 0 {
		return result, nil
	}
	if location == nil {
		location = time.UTC
	}
	width := time.Hour
	if granularity == "day" {
		width = 24 * time.Hour
	}
	zoneStart, zoneEnd := time.UnixMilli(buckets[0]).In(location).ZoneBounds()
	if zoneStart.IsZero() && zoneEnd.IsZero() {
		// UTC and FixedZone need no transition enumeration, including FixedZone
		// offsets outside the signed 32-bit TZif range.
		for _, bucketMS := range buckets {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if AnalyticsBucketMS(bucketMS, granularity, location) != bucketMS || bucketMS > math.MaxInt64-width.Milliseconds() {
				return nil, errors.New("invalid analytics bucket boundary")
			}
			result[bucketMS] = []AnalyticsBucketInterval{{FromMS: bucketMS, ToMS: bucketMS + width.Milliseconds()}}
		}
		return result, nil
	}

	// TZif offsets are signed 32-bit seconds. If Date(civil)=bucket, then
	// civil=bucket+one such offset; a matching event subtracts another offset.
	// Their difference is strictly less than 2^32 seconds. This is a format
	// bound, not a guessed DST window: even a date-line jump or a custom TZif
	// cannot hide another matching interval outside it. Enumerate transitions,
	// never the hours/days (or event samples) throughout this search range.
	const reachMS = int64(1<<32) * 1000
	ranges := make([]AnalyticsBucketInterval, 0, len(buckets))
	for _, bucketMS := range buckets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fromMS, toMS := int64(math.MinInt64), int64(math.MaxInt64)
		if bucketMS >= math.MinInt64+reachMS {
			fromMS = bucketMS - reachMS
		}
		if bucketMS <= math.MaxInt64-reachMS-width.Milliseconds() {
			toMS = bucketMS + reachMS + width.Milliseconds()
		}
		ranges = append(ranges, AnalyticsBucketInterval{FromMS: fromMS, ToMS: toMS})
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].FromMS < ranges[j].FromMS })
	// Sparse distant bucket IDs must not enumerate every intervening year.
	merged := ranges[:0]
	for _, interval := range ranges {
		if n := len(merged); n > 0 && merged[n-1].ToMS >= interval.FromMS {
			merged[n-1].ToMS = max(merged[n-1].ToMS, interval.ToMS)
		} else {
			merged = append(merged, interval)
		}
	}
	zones := make([]analyticsZoneInterval, 0)
	offsets := make(map[int]struct{})
	for _, interval := range merged {
		var err error
		zones, err = appendAnalyticsZoneIntervals(ctx, zones, offsets, interval, location)
		if err != nil {
			return nil, err
		}
	}
	for _, bucketMS := range buckets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, exists := result[bucketMS]; exists {
			continue
		}
		intervals, err := analyticsBucketIntervalsInZones(ctx, bucketMS, width, location, zones, offsets)
		if err != nil {
			return nil, err
		}
		result[bucketMS] = intervals
	}
	return result, nil
}

type analyticsZoneInterval struct {
	from, to time.Time
	offset   int
}

func appendAnalyticsZoneIntervals(ctx context.Context, zones []analyticsZoneInterval, offsets map[int]struct{}, interval AnalyticsBucketInterval, location *time.Location) ([]analyticsZoneInterval, error) {
	limit := time.UnixMilli(interval.ToMS)
	for cursor := time.UnixMilli(interval.FromMS).In(location); cursor.Before(limit); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, offset := cursor.Zone()
		_, end := cursor.ZoneBounds()
		if end.IsZero() || end.After(limit) {
			end = limit
		}
		if !end.After(cursor) {
			// Go's POSIX-extension lookup can report Jan 1 + 365 days as
			// the end of a leap year, even on Dec 31. In this post-transition
			// branch the offset lasts until the next UTC year, not that stale
			// bound. Do not sample or step over any actual DST transition.
			utc := cursor.UTC()
			yearStart := time.Date(utc.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
			yearEnd := yearStart.AddDate(1, 0, 0)
			if !end.Equal(yearStart.AddDate(0, 0, 365)) || yearEnd.Sub(end) != 24*time.Hour || !cursor.Before(yearEnd) {
				return nil, errors.New("non-advancing analytics timezone interval")
			}
			end = yearEnd
			if end.After(limit) {
				end = limit
			}
		}
		zones = append(zones, analyticsZoneInterval{from: cursor, to: end, offset: offset})
		offsets[offset] = struct{}{}
		cursor = end.In(location)
	}
	return zones, nil
}

func analyticsBucketIntervalsInZones(ctx context.Context, bucketMS int64, width time.Duration, location *time.Location, zones []analyticsZoneInterval, offsets map[int]struct{}) ([]AnalyticsBucketInterval, error) {
	bucket := time.UnixMilli(bucketMS).In(location)
	// Date may normalize a missing midnight into the previous date, so using
	// bucket.In(location)'s calendar fields alone would lose valid members.
	// Instead try every possible Date offset and verify the canonical ID.
	civilStarts := make([]time.Time, 0, len(offsets))
	for offset := range offsets {
		civil := bucket.UTC().Add(time.Duration(offset) * time.Second)
		if civil.Minute() != 0 || civil.Second() != 0 || civil.Nanosecond() != 0 || (width == 24*time.Hour && civil.Hour() != 0) {
			continue
		}
		if time.Date(civil.Year(), civil.Month(), civil.Day(), civil.Hour(), 0, 0, 0, location).Equal(bucket) {
			civilStarts = append(civilStarts, civil)
		}
	}
	if len(civilStarts) == 0 {
		return nil, errors.New("invalid analytics bucket boundary")
	}
	sort.Slice(civilStarts, func(i, j int) bool { return civilStarts[i].Before(civilStarts[j]) })
	minOffset, maxOffset := math.MaxInt, math.MinInt
	for offset := range offsets {
		minOffset, maxOffset = min(minOffset, offset), max(maxOffset, offset)
	}
	// After discovering the location's offsets once, most buckets need only
	// one or two of its transition intervals. Skip the surrounding centuries.
	earliest := civilStarts[0].Add(-time.Duration(maxOffset) * time.Second)
	latest := civilStarts[len(civilStarts)-1].Add(width - time.Duration(minOffset)*time.Second)
	firstZone := sort.Search(len(zones), func(i int) bool { return zones[i].to.After(earliest) })
	intervals := make([]AnalyticsBucketInterval, 0, 2)
	for _, zone := range zones[firstZone:] {
		if !zone.from.Before(latest) {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, civil := range civilStarts {
			from := civil.Add(-time.Duration(zone.offset) * time.Second)
			to := from.Add(width)
			if from.Before(zone.from) {
				from = zone.from
			}
			if to.After(zone.to) {
				to = zone.to
			}
			if !from.Before(to) {
				continue
			}
			interval := AnalyticsBucketInterval{FromMS: from.UnixMilli(), ToMS: to.UnixMilli()}
			if n := len(intervals); n > 0 && intervals[n-1].ToMS >= interval.FromMS {
				intervals[n-1].ToMS = max(intervals[n-1].ToMS, interval.ToMS)
			} else {
				intervals = append(intervals, interval)
			}
		}
	}
	if len(intervals) == 0 {
		return nil, errors.New("invalid analytics bucket boundary")
	}
	return intervals, nil
}

// AnalyticsFullUTCHourRange returns the complete UTC hours contained by the
// half-open analytics range [fromMS, toMS).
func AnalyticsFullUTCHourRange(fromMS, toMS int64) (int64, int64) {
	startMS := fromMS - fromMS%analyticsHourMS
	if fromMS%analyticsHourMS != 0 {
		startMS += analyticsHourMS
	}
	endMS := toMS - toMS%analyticsHourMS
	return startMS, endMS
}

// CanMapUTCWholeHours reports whether every complete UTC hour in the supplied
// aligned range maps to one local analytics bucket without being split.
func CanMapUTCWholeHours(fromMS, toMS int64, granularity string, location *time.Location) bool {
	if fromMS >= toMS || fromMS%analyticsHourMS != 0 || toMS%analyticsHourMS != 0 {
		return false
	}
	if location == nil {
		location = time.UTC
	}
	if granularity != "day" {
		granularity = "hour"
	}
	for hourMS := fromMS; hourMS < toMS; hourMS += analyticsHourMS {
		if AnalyticsBucketMS(hourMS, granularity, location) != AnalyticsBucketMS(hourMS+analyticsHourMS-1, granularity, location) {
			return false
		}
	}
	return true
}

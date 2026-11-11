package usage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestAnalyticsBucketMSAcrossDST(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	tests := []struct {
		name        string
		timestampMS int64
		granularity string
		wantMS      int64
	}{
		{
			name:        "hour before spring transition",
			timestampMS: time.Date(2026, time.March, 8, 6, 30, 0, 0, time.UTC).UnixMilli(),
			granularity: "hour",
			wantMS:      time.Date(2026, time.March, 8, 6, 0, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name:        "hour after spring transition",
			timestampMS: time.Date(2026, time.March, 8, 7, 30, 0, 0, time.UTC).UnixMilli(),
			granularity: "hour",
			wantMS:      time.Date(2026, time.March, 8, 7, 0, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name:        "local day",
			timestampMS: time.Date(2026, time.March, 8, 18, 0, 0, 0, time.UTC).UnixMilli(),
			granularity: "day",
			wantMS:      time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC).UnixMilli(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := AnalyticsBucketMS(test.timestampMS, test.granularity, location); got != test.wantMS {
				t.Fatalf("bucket = %d, want %d", got, test.wantMS)
			}
		})
	}
}

func TestAnalyticsBucketIntervalsRepeatedHistoricalDay(t *testing.T) {
	location, err := time.LoadLocation("Antarctica/Casey")
	if err != nil {
		t.Fatal(err)
	}
	parse := func(value string) int64 {
		tm, err := time.Parse(time.RFC3339, value)
		if err != nil {
			t.Fatal(err)
		}
		return tm.UnixMilli()
	}
	for _, tc := range []struct {
		event string
		want  []AnalyticsBucketInterval
	}{
		{"2010-03-04T23:30:00+08:00", []AnalyticsBucketInterval{
			{FromMS: parse("2010-03-03T13:00:00Z"), ToMS: parse("2010-03-04T13:00:00Z")},
			{FromMS: parse("2010-03-04T15:00:00Z"), ToMS: parse("2010-03-04T16:00:00Z")},
		}},
		{"2010-03-05T01:30:00+11:00", []AnalyticsBucketInterval{
			{FromMS: parse("2010-03-04T13:00:00Z"), ToMS: parse("2010-03-04T15:00:00Z")},
			{FromMS: parse("2010-03-04T16:00:00Z"), ToMS: parse("2010-03-05T16:00:00Z")},
		}},
	} {
		bucket := AnalyticsBucketMS(parse(tc.event), "day", location)
		got, err := AnalyticsBucketIntervals(context.Background(), bucket, "day", location)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("event %s intervals=%v want=%v", tc.event, got, tc.want)
		}
	}
}

func TestAnalyticsBucketIntervalsAgreeWithCalendarMapping(t *testing.T) {
	for _, tc := range []struct {
		zone, event, granularity string
	}{
		{"UTC", "2026-03-08T04:30:00Z", "hour"},
		{"America/New_York", "2026-03-08T12:30:00-04:00", "day"},
		{"America/New_York", "2026-11-01T12:30:00-05:00", "day"},
		{"America/New_York", "2026-11-01T01:30:00-05:00", "hour"},
		{"America/New_York", "2040-12-31T12:30:00-05:00", "day"},
		{"Australia/Lord_Howe", "2040-12-31T12:30:00+11:00", "day"},
		{"Asia/Kathmandu", "2026-03-08T04:30:00+05:45", "hour"},
		{"Asia/Kathmandu", "1986-01-01T00:30:00+05:45", "hour"},
		{"Australia/Lord_Howe", "2026-04-05T01:45:00+10:30", "hour"},
		{"Australia/Lord_Howe", "2026-04-05T12:00:00+10:30", "day"},
		{"Australia/Lord_Howe", "2026-10-04T02:45:00+11:00", "hour"},
		{"Antarctica/Casey", "2010-03-04T23:30:00+08:00", "day"},
		{"Antarctica/Casey", "2010-03-05T01:30:00+11:00", "day"},
		{"Pacific/Kwajalein", "1969-09-30T12:00:00-12:00", "day"},
		{"Pacific/Apia", "2011-12-31T12:00:00+14:00", "day"},
		{"America/Santiago", "2026-09-06T01:30:00-03:00", "day"},
	} {
		t.Run(tc.zone+tc.event+tc.granularity, func(t *testing.T) {
			location, err := time.LoadLocation(tc.zone)
			if err != nil {
				t.Fatal(err)
			}
			event, err := time.Parse(time.RFC3339, tc.event)
			if err != nil {
				t.Fatal(err)
			}
			bucket := AnalyticsBucketMS(event.UnixMilli(), tc.granularity, location)
			intervals, err := AnalyticsBucketIntervals(context.Background(), bucket, tc.granularity, location)
			if err != nil {
				t.Fatal(err)
			}
			contains := func(ms int64) bool {
				for _, interval := range intervals {
					if ms >= interval.FromMS && ms < interval.ToMS {
						return true
					}
				}
				return false
			}
			if !contains(event.UnixMilli()) {
				t.Fatalf("event %s omitted: %v", tc.event, intervals)
			}
			for i, interval := range intervals {
				if interval.FromMS >= interval.ToMS || (i > 0 && intervals[i-1].ToMS >= interval.FromMS) {
					t.Fatalf("not disjoint maximal intervals: %v", intervals)
				}
				for _, ms := range []int64{interval.FromMS - 1, interval.FromMS, interval.ToMS - 1, interval.ToMS} {
					if contains(ms) != (AnalyticsBucketMS(ms, tc.granularity, location) == bucket) {
						t.Fatalf("boundary mismatch at %s in %v", time.UnixMilli(ms).In(location), intervals)
					}
				}
			}
			for ms := bucket - 72*time.Hour.Milliseconds(); ms < bucket+72*time.Hour.Milliseconds(); ms += time.Minute.Milliseconds() {
				if contains(ms) != (AnalyticsBucketMS(ms, tc.granularity, location) == bucket) {
					t.Fatalf("membership mismatch at %s in %v", time.UnixMilli(ms).In(location), intervals)
				}
			}
		})
	}
}

func TestAnalyticsBucketIntervalsDoNotAssumeSmallOffsetChanges(t *testing.T) {
	// A valid TZif with a ten-day backwards jump proves that membership search
	// is bounded by the format, not by a guessed 24/48-hour DST window.
	var data bytes.Buffer
	data.WriteString("TZif")
	data.Write(make([]byte, 16)) // v1 and reserved header bytes
	write := func(value any) {
		t.Helper()
		if err := binary.Write(&data, binary.BigEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	write([6]uint32{0, 0, 0, 1, 2, 4})
	write(int32(time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC).Unix()))
	data.WriteByte(1)
	write(int32(10 * 24 * 60 * 60))
	data.Write([]byte{0, 0})
	write(int32(0))
	data.Write([]byte{0, 2})
	data.WriteString("A\x00B\x00")
	location, err := time.LoadLocationFromTZData("ten-day-rollback", data.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2020, 1, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
	second := time.Date(2020, 1, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	bucket := AnalyticsBucketMS(first, "day", location)
	if AnalyticsBucketMS(second, "day", location) != bucket {
		t.Fatal("fixture does not repeat a local day")
	}
	intervals, err := AnalyticsBucketIntervals(context.Background(), bucket, "day", location)
	if err != nil {
		t.Fatal(err)
	}
	want := []AnalyticsBucketInterval{{FromMS: first, ToMS: first + 24*time.Hour.Milliseconds()}, {FromMS: second, ToMS: second + 24*time.Hour.Milliseconds()}}
	if !reflect.DeepEqual(intervals, want) {
		t.Fatalf("large rollback intervals=%v want=%v", intervals, want)
	}
}

func TestAnalyticsBucketIntervalsBatchMatchesSingle(t *testing.T) {
	location, err := time.LoadLocation("Antarctica/Casey")
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2010, 3, 4, 12, 0, 0, 0, location).UnixMilli()
	second := time.Date(2010, 3, 5, 12, 0, 0, 0, location).UnixMilli()
	ids := []int64{AnalyticsBucketMS(first, "day", location), AnalyticsBucketMS(second, "day", location)}
	got, err := AnalyticsBucketIntervalsForBuckets(context.Background(), []int64{ids[1], ids[0], ids[1]}, "day", location)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("unexpected buckets: %v", got)
	}
	for _, id := range ids {
		want, err := AnalyticsBucketIntervals(context.Background(), id, "day", location)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got[id], want) {
			t.Fatalf("batch intervals=%v single=%v", got[id], want)
		}
	}
}

type cancelBucketContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelBucketContext) Err() error {
	c.checks++
	if c.checks == 10 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestAnalyticsBucketIntervalsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := AnalyticsBucketIntervals(ctx, 0, "day", time.UTC); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	duringScan := &cancelBucketContext{Context: ctx, cancel: cancel}
	bucket := AnalyticsBucketMS(time.Date(2026, 3, 8, 12, 0, 0, 0, location).UnixMilli(), "day", location)
	intervals, err := AnalyticsBucketIntervals(duringScan, bucket, "day", location)
	if !errors.Is(err, context.Canceled) || len(intervals) != 0 || duringScan.checks != 10 {
		t.Fatalf("mid-scan cancellation returned partial membership: intervals=%v err=%v checks=%d", intervals, err, duringScan.checks)
	}
}

func TestAnalyticsFullUTCHourRange(t *testing.T) {
	hourMS := int64(time.Hour / time.Millisecond)
	fromMS := int64(1_800_000_000_000) + 15*60*1000
	toMS := fromMS + 3*hourMS + 20*60*1000
	startMS, endMS := AnalyticsFullUTCHourRange(fromMS, toMS)
	if startMS%hourMS != 0 || endMS%hourMS != 0 {
		t.Fatalf("range = [%d, %d), want UTC-hour alignment", startMS, endMS)
	}
	if startMS < fromMS || startMS-fromMS >= hourMS {
		t.Fatalf("start = %d, from = %d", startMS, fromMS)
	}
	if endMS > toMS || toMS-endMS >= hourMS {
		t.Fatalf("end = %d, to = %d", endMS, toMS)
	}
}

func TestCanMapUTCWholeHours(t *testing.T) {
	hourMS := int64(time.Hour / time.Millisecond)
	fromMS := time.Date(2026, time.March, 7, 0, 0, 0, 0, time.UTC).UnixMilli()
	toMS := fromMS + 72*hourMS

	for _, name := range []string{"UTC", "Asia/Shanghai", "America/New_York"} {
		location, err := time.LoadLocation(name)
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		if !CanMapUTCWholeHours(fromMS, toMS, "hour", location) {
			t.Fatalf("%s unexpectedly not representable", name)
		}
	}

	for _, name := range []string{"Asia/Kolkata", "Australia/Eucla"} {
		location, err := time.LoadLocation(name)
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		if CanMapUTCWholeHours(fromMS, toMS, "hour", location) {
			t.Fatalf("%s unexpectedly representable", name)
		}
	}

	if CanMapUTCWholeHours(fromMS+1, toMS, "hour", time.UTC) {
		t.Fatal("unaligned range unexpectedly representable")
	}
}

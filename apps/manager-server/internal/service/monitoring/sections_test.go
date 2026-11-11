package monitoring

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

type latencyCountingRepository struct {
	usageevent.Repository
	combined     atomic.Int64
	standalone   atomic.Int64
	tasks        atomic.Int64
	needSummary  bool
	needTimeline bool
}

func (r *latencyCountingRepository) LatencyMetricsWithFilter(ctx context.Context, filter usageevent.AnalyticsFilter, granularity string, location *time.Location, summary, timeline bool) (usageevent.LatencyMetrics, error) {
	r.combined.Add(1)
	r.needSummary, r.needTimeline = summary, timeline
	return r.Repository.LatencyMetricsWithFilter(ctx, filter, granularity, location, summary, timeline)
}
func (r *latencyCountingRepository) LatencySummaryWithFilter(context.Context, usageevent.AnalyticsFilter) (usageevent.LatencySummary, error) {
	r.standalone.Add(1)
	return usageevent.LatencySummary{}, errors.New("standalone summary must not run")
}
func (r *latencyCountingRepository) LatencyPercentilesWithFilter(context.Context, usageevent.AnalyticsFilter, string, *time.Location) ([]usageevent.LatencyPercentiles, error) {
	r.standalone.Add(1)
	return nil, errors.New("standalone timeline must not run")
}
func (r *latencyCountingRepository) TaskBucketsWithFilter(ctx context.Context, filter usageevent.AnalyticsFilter) ([]usageevent.TaskBucket, error) {
	r.tasks.Add(1)
	return r.Repository.TaskBucketsWithFilter(ctx, filter)
}

func TestAnalyticsSectionDependencyPlanAndLegacyParity(t *testing.T) {
	st := newMonitoringTestStore(t)
	const hour = int64(time.Hour / time.Millisecond)
	from := int64(1_800_000_000_000)
	var events []usage.Event
	for i, latency := range []int64{100, 200, 300, 400, 500, 5000} {
		event := monitoringEvent(string(rune('a'+i)), from+int64(i)*hour+1000, "model", "auth", "source", false, 10, 20, 0, 0, 30, &latency)
		ttft := latency / 10
		event.TTFTMS = &ttft
		events = append(events, event)
	}
	if _, err := st.InsertEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	spy := &latencyCountingRepository{Repository: st.UsageEvents}
	st.UsageEvents = spy
	svc := New(st)
	request := Request{FromMS: from, ToMS: from + 6*hour, NowMS: from + 6*hour}
	request.Include = Include{Summary: true, Timeline: true, AnomalyPoints: true, Granularity: "hour"}
	full, err := svc.Analytics(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if spy.combined.Load() != 1 || spy.standalone.Load() != 0 || !spy.needSummary || !spy.needTimeline {
		t.Fatal("legacy overview did not use exactly one combined computation")
	}
	if full.Summary.P95LatencyMS == nil || *full.Summary.P95LatencyMS != 5000 {
		t.Fatalf("summary = %+v", full.Summary)
	}
	if len(full.AnomalyPoints) == 0 {
		t.Fatal("fixture did not produce anomalies")
	}

	spy.combined.Store(0)
	spy.tasks.Store(0)
	request.Include = Include{Summary: true, SummaryProfile: "compact", Timeline: true, TimelineProfile: "core", Granularity: "hour"}
	core, err := svc.Analytics(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if spy.combined.Load() != 0 || spy.tasks.Load() != 0 || core.Percentiles != nil || core.Summary.P95LatencyMS != nil {
		t.Fatal("core launched exact/full diagnostics")
	}
	if len(core.Timeline) != len(full.Timeline) {
		t.Fatal("core timeline lost buckets")
	}
	for i, point := range core.Timeline {
		if point.P95LatencyMS != nil || point.P95TTFTMS != nil || point.BucketEndMS <= point.BucketMS || point.Calls != full.Timeline[i].Calls {
			t.Fatalf("core point = %+v", point)
		}
	}

	spy.combined.Store(0)
	spy.tasks.Store(0)
	request.Include = Include{Percentiles: &PercentilesInclude{Summary: true, Timeline: true}, AnomalyPoints: true, Granularity: "hour"}
	exact, err := svc.Analytics(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if spy.combined.Load() != 1 || spy.tasks.Load() != 0 || exact.Summary != nil || exact.Timeline != nil {
		t.Fatal("exact section forced whole summary/timeline")
	}
	if !reflect.DeepEqual(full.AnomalyPoints, exact.AnomalyPoints) {
		t.Fatalf("standalone anomalies changed: %v != %v", full.AnomalyPoints, exact.AnomalyPoints)
	}
	if !reflect.DeepEqual(exact.Percentiles.Summary.P95LatencyMS, full.Summary.P95LatencyMS) || len(*exact.Percentiles.Timeline) != len(full.Timeline) {
		t.Fatal("explicit percentile section differs from legacy")
	}

	spy.combined.Store(0)
	request.Include = Include{AnomalyPoints: true, Granularity: "hour"}
	if _, err := svc.Analytics(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if spy.combined.Load() != 1 || spy.needSummary || !spy.needTimeline {
		t.Fatal("anomalies require bucket percentiles only")
	}

	spy.combined.Store(0)
	request.Include = Include{DrilldownPreview: &DrilldownPreview{FromMS: from, ToMS: from + hour, Limit: 2}}
	preview, err := svc.Analytics(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if preview.DrilldownPreview == nil || spy.combined.Load() != 0 || preview.Summary != nil || len(preview.Timeline) != 0 {
		t.Fatal("preview repeated overview")
	}
}

func TestAnalyticsPlanAllPercentileProfiles(t *testing.T) {
	for _, tc := range []struct {
		name                                             string
		include                                          Include
		summary, timeline, legacySummary, legacyTimeline bool
	}{
		{"none", Include{}, false, false, false, false},
		{"summary legacy", Include{Summary: true}, true, false, true, false},
		{"summary compact", Include{Summary: true, SummaryProfile: "compact"}, false, false, false, false},
		{"summary compact exact", Include{Summary: true, SummaryProfile: "compact", SummaryPercentiles: true}, true, false, true, false},
		{"timeline legacy", Include{Timeline: true}, false, true, false, true},
		{"timeline full", Include{Timeline: true, TimelineProfile: "full"}, false, true, false, true},
		{"timeline core", Include{Timeline: true, TimelineProfile: "core"}, false, false, false, false},
		{"explicit summary", Include{Percentiles: &PercentilesInclude{Summary: true}}, true, false, false, false},
		{"explicit timeline", Include{Percentiles: &PercentilesInclude{Timeline: true}}, false, true, false, false},
		{"empty explicit", Include{Percentiles: &PercentilesInclude{}}, false, false, false, false},
		{"anomalies", Include{AnomalyPoints: true}, false, true, false, false},
		{"core and anomalies", Include{Timeline: true, TimelineProfile: "core", AnomalyPoints: true}, false, true, false, false},
		{"core and explicit", Include{Summary: true, SummaryProfile: "compact", Timeline: true, TimelineProfile: "core", Percentiles: &PercentilesInclude{Summary: true, Timeline: true}}, true, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := planAnalytics(tc.include)
			if plan.latencySummary != tc.summary || plan.latencyTimeline != tc.timeline || plan.legacySummaryPercentiles != tc.legacySummary || plan.legacyTimelinePercentiles != tc.legacyTimeline {
				t.Fatalf("plan=%+v", plan)
			}
		})
	}
}

func TestExplicitPercentilesNullAndEmptyJSON(t *testing.T) {
	response := buildPercentiles(&PercentilesInclude{Summary: true, Timeline: true}, store.LatencySummary{}, nil)
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"summary":{"p95_latency_ms":null,"p95_ttft_ms":null},"timeline":[]}` {
		t.Fatalf("JSON=%s", data)
	}
	response = buildPercentiles(&PercentilesInclude{Summary: true}, store.LatencySummary{P95LatencyMS: sql.NullFloat64{Valid: true, Float64: 100}}, nil)
	data, _ = json.Marshal(response)
	if strings.Contains(string(data), "timeline") {
		t.Fatal("unrequested section serialized")
	}
}

func TestAnalyticsBucketEndsAcrossTimeZones(t *testing.T) {
	for _, tc := range []struct {
		zone, date, granularity string
		width                   time.Duration
	}{
		{"America/New_York", "2026-03-08T00:00:00-05:00", "day", 23 * time.Hour},
		{"America/New_York", "2026-11-01T00:00:00-04:00", "day", 25 * time.Hour},
		{"America/New_York", "2026-11-01T01:00:00-04:00", "hour", 2 * time.Hour},
		{"Asia/Kathmandu", "2026-03-08T04:00:00+05:45", "hour", time.Hour},
		{"Australia/Lord_Howe", "2026-04-05T00:00:00+11:00", "day", 24*time.Hour + 30*time.Minute},
	} {
		t.Run(tc.zone+tc.date, func(t *testing.T) {
			location, err := time.LoadLocation(tc.zone)
			if err != nil {
				t.Fatal(err)
			}
			start, err := time.Parse(time.RFC3339, tc.date)
			if err != nil {
				t.Fatal(err)
			}
			bucket := usage.AnalyticsBucketMS(start.UnixMilli(), tc.granularity, location)
			points, err := buildTimeline(context.Background(), []store.TimelinePoint{{BucketMS: bucket}}, nil, tc.granularity, location, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !points[0].BucketRangeExact || len(points[0].BucketIntervals) != 0 {
				t.Fatalf("ordinary bucket unexpectedly non-contiguous: %+v", points[0])
			}
			end := points[0].BucketEndMS
			if end-bucket != tc.width.Milliseconds() {
				t.Fatalf("width=%s want=%s", time.Duration(end-bucket)*time.Millisecond, tc.width)
			}
			if usage.AnalyticsBucketMS(end-1, tc.granularity, location) != bucket || usage.AnalyticsBucketMS(end, tc.granularity, location) == bucket {
				t.Fatal("end is not authoritative")
			}
		})
	}
}

func TestAnalyticsHistoricalRepeatedDayResponseIntervals(t *testing.T) {
	st := newMonitoringTestStore(t)
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
	var events []usage.Event
	for i, timestamp := range []string{
		"2010-03-03T12:00:00+11:00",
		"2010-03-04T12:00:00+11:00",
		"2010-03-04T23:30:00+11:00",
		"2010-03-05T01:30:00+11:00", // In the gap of March 4's membership.
		"2010-03-04T23:30:00+08:00", // Back in March 4 after entering March 5.
	} {
		events = append(events, monitoringEvent(string(rune('a'+i)), parse(timestamp), "model", "auth", "source", false, 10, 20, 0, 0, 30, nil))
	}
	if _, err := st.InsertEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	response, err := New(st).Analytics(context.Background(), Request{
		FromMS: parse("2010-03-03T00:00:00+11:00"), ToMS: parse("2010-03-06T00:00:00+08:00"),
		TimeZone: "Antarctica/Casey", Include: Include{Timeline: true, TimelineProfile: "core", AnomalyPoints: true, Granularity: "day"},
	})
	if err != nil {
		t.Fatal(err)
	}
	bucket := usage.AnalyticsBucketMS(events[4].TimestampMS, "day", location)
	var repeated *TimelinePoint
	for i := range response.Timeline {
		if response.Timeline[i].BucketMS == bucket {
			repeated = &response.Timeline[i]
		}
	}
	if repeated == nil || repeated.Calls != 3 || repeated.BucketEndMS != parse("2010-03-05T00:00:00+08:00") {
		t.Fatalf("repeated day lost event or end: %+v", repeated)
	}
	if repeated.BucketRangeExact || len(repeated.BucketIntervals) != 2 {
		t.Fatalf("non-contiguous day advertised as an exact single range: %+v", repeated)
	}
	if events[3].TimestampMS < repeated.BucketMS || events[3].TimestampMS >= repeated.BucketEndMS {
		t.Fatal("fixture does not exercise an intervening other-bucket event")
	}
	for _, point := range response.Timeline {
		intervals := point.BucketIntervals
		if point.BucketRangeExact {
			intervals = []usage.AnalyticsBucketInterval{{FromMS: point.BucketMS, ToMS: point.BucketEndMS}}
		}
		var calls int64
		for _, interval := range intervals {
			count, err := st.EventsCountWithFilter(context.Background(), store.AnalyticsFilter{
				FromMS: interval.FromMS, ToMS: interval.ToMS, IncludeFailed: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			calls += count
		}
		if calls != point.Calls {
			t.Fatalf("exact membership does not match calls: %+v interval calls=%d", point, calls)
		}
	}
	var anomaly *AnomalyPoint
	for i := range response.AnomalyPoints {
		if response.AnomalyPoints[i].BucketMS == bucket {
			anomaly = &response.AnomalyPoints[i]
		}
	}
	if anomaly == nil || anomaly.BucketRangeExact || anomaly.BucketEndMS != repeated.BucketEndMS || !reflect.DeepEqual(anomaly.BucketIntervals, repeated.BucketIntervals) {
		t.Fatalf("anomaly lost precise intervals: %+v", anomaly)
	}
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"bucket_range_exact":false`) || !strings.Contains(string(data), `"bucket_intervals":[{"from_ms":`) {
		t.Fatalf("missing non-exact range metadata: %s", data)
	}
}

func TestBuildTimelineIntervalResolutionRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := buildTimeline(ctx, []store.TimelinePoint{{BucketMS: 0}}, nil, "hour", time.UTC, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("timeline interval cancellation=%v", err)
	}
}

func TestAnalyticsCoverageForProgressivePercentiles(t *testing.T) {
	coverage := store.UsageArchiveRawCoverage{RawDeletedEventCount: 1}
	core := buildAnalyticsCoverage(Request{Include: Include{Timeline: true, TimelineProfile: "core"}}, coverage, nil, nil, analyticsCoverageSources{Timeline: true})
	if len(core.FidelityLimitations) != 0 {
		t.Fatalf("core advertises unrequested limitations: %v", core.FidelityLimitations)
	}
	exact := buildAnalyticsCoverage(Request{Include: Include{Percentiles: &PercentilesInclude{Summary: true, Timeline: true}}}, coverage, nil, nil, analyticsCoverageSources{})
	if len(exact.FidelityLimitations) != 2 {
		t.Fatalf("exact coverage lost warnings: %v", exact.FidelityLimitations)
	}
}

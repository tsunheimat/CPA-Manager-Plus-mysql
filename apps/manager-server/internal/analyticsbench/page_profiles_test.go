package analyticsbench_test

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	monitoring "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

type pageSectionRequest struct {
	name    string
	request monitoring.Request
}

// Representative overview request bodies, rather than calling a percentiles-only
// microbenchmark the entire legacy "exact" page. These are separate snapshots;
// sequential service calls measure their work, not browser scheduling/HTTP time.
func overviewPageRequests(fromMS, toMS int64, timeZone string, v2 bool) []pageSectionRequest {
	core := monitoring.Include{Summary: true, SummaryProfile: "compact", SummaryComparison: true,
		ModelStats: true, Timeline: true, TimelineProfile: "core", Granularity: "hour"}
	exact := monitoring.Include{Granularity: "hour", Percentiles: &monitoring.PercentilesInclude{Summary: true, Timeline: true}, AnomalyPoints: !v2}
	if v2 {
		version := 1
		core.AnomalyBasisVersion = &version
	}
	sections := []struct {
		name    string
		include monitoring.Include
	}{
		{"selectors", monitoring.Include{FilterOptions: true, FilterSelectors: true}},
		{"core", core},
		{"dimensions", monitoring.Include{ChannelShare: true, APIKeyStats: true, Granularity: "hour"}},
		{"exact", exact},
	}
	result := make([]pageSectionRequest, 0, len(sections))
	for _, section := range sections {
		result = append(result, pageSectionRequest{section.name, monitoring.Request{FromMS: fromMS, ToMS: toMS, NowMS: toMS, TimeZone: timeZone, Include: section.include}})
	}
	return result
}

type pageBookSpy struct {
	modelprice.Repository
	reads int
}

func (r *pageBookSpy) LoadRates(ctx context.Context) (map[string]model.ModelPrice, error) {
	r.reads++
	return r.Repository.LoadRates(ctx)
}

type pageReadSpy struct {
	usageevent.Repository
	timeline, models int
}

func (r *pageReadSpy) TimelineWithFilter(ctx context.Context, f usageevent.AnalyticsFilter, g string, l *time.Location) ([]usageevent.TimelinePoint, error) {
	r.timeline++
	return r.Repository.TimelineWithFilter(ctx, f, g, l)
}
func (r *pageReadSpy) ModelStatsWithFilter(ctx context.Context, f usageevent.AnalyticsFilter, limit int) ([]usageevent.ModelStat, error) {
	r.models++
	return r.Repository.ModelStatsWithFilter(ctx, f, limit)
}

func TestAnalyticsPageProfilesAndUnpricedDependencies(t *testing.T) {
	f := newPageSentinel(t, 64, 1, "low", "mixed")
	checkOverviewPageProfiles(t, f)
}

func checkOverviewPageProfiles(t testing.TB, f pageSentinel) {
	t.Helper()
	books := &pageBookSpy{Repository: f.store.ModelPrices}
	reads := &pageReadSpy{Repository: f.store.UsageEvents}
	f.store.ModelPrices, f.store.UsageEvents = books, reads
	defer func() { f.store.ModelPrices, f.store.UsageEvents = books.Repository, reads.Repository }()
	svc := monitoring.New(f.store)
	for _, v2 := range []bool{false, true} {
		var core monitoring.Response
		for _, section := range overviewPageRequests(f.fromMS, f.toMS, f.location.String(), v2) {
			beforeBook, beforeTimeline, beforeModels := books.reads, reads.timeline, reads.models
			started := time.Now()
			result, err := svc.Analytics(context.Background(), section.request)
			if err != nil {
				t.Fatalf("v2=%t section=%s: %v", v2, section.name, err)
			}
			switch section.name {
			case "selectors":
				if result.FilterOptions == nil || books.reads != beforeBook {
					t.Fatalf("selector book dependency: result=%+v reads=%d", result.FilterOptions, books.reads-beforeBook)
				}
			case "core":
				core = result
				checkOverviewCoreOracle(t, f, result)
				if result.Summary == nil || result.Summary.TotalCalls != int64(f.count) || len(result.Timeline) == 0 || len(result.ModelStats) == 0 || result.SummaryComparison == nil {
					t.Fatalf("core fields incomplete: %+v", result)
				}
				if v2 && (result.AnomalyBasis == nil || result.AnomalyBasis.Version != 1 || result.AnomalyBasis.CoreID == "" || result.AnomalyBasis.Semantics != "core_additive_exact_latency" || len(result.AnomalyBasis.Candidates) != len(result.Timeline)-1) {
					t.Fatalf("v2 basis incomplete: %+v", result.AnomalyBasis)
				}
			case "dimensions":
				if len(result.ChannelShare) == 0 || len(result.APIKeyStats) == 0 {
					t.Fatal("missing priced dimension results")
				}
			case "exact":
				if result.Percentiles == nil || result.Percentiles.Summary == nil || result.Percentiles.Timeline == nil || len(*result.Percentiles.Timeline) != len(core.Timeline) {
					t.Fatalf("exact fields incomplete: %+v", result.Percentiles)
				}
				checkOverviewPercentileOracle(t, f, result.Percentiles)
				for i, point := range *result.Percentiles.Timeline {
					if point.BucketMS != core.Timeline[i].BucketMS {
						t.Fatal("exact/core bucket mismatch")
					}
				}
				if v2 && (books.reads != beforeBook || reads.timeline != beforeTimeline || reads.models != beforeModels) {
					t.Fatalf("v2 exact rebuilt priced dependencies book/timeline/models=%d/%d/%d", books.reads-beforeBook, reads.timeline-beforeTimeline, reads.models-beforeModels)
				}
				if !v2 && (books.reads == beforeBook || reads.timeline == beforeTimeline) {
					t.Fatal("legacy exact profile did not include anomaly timeline work")
				}
			}
			t.Logf("overview_v%d section=%s service_elapsed=%s book_reads=%d raw_timeline_calls=%d raw_model_calls=%d", map[bool]int{false: 1, true: 2}[v2], section.name, time.Since(started), books.reads-beforeBook, reads.timeline-beforeTimeline, reads.models-beforeModels)
		}
	}
	// Preview has a bounded row count, not a full event population count, and
	// the v2 exact profile above is not a substitute for this separate request.
	before := books.reads
	preview, err := svc.Analytics(context.Background(), f.request(monitoring.Include{DrilldownPreview: &monitoring.DrilldownPreview{FromMS: f.fromMS, ToMS: f.toMS, Limit: 12}}))
	if err != nil || preview.DrilldownPreview == nil || books.reads != before {
		t.Fatalf("preview book dependency: %+v err=%v", preview.DrilldownPreview, err)
	}
}

func checkOverviewCoreOracle(t testing.TB, f pageSentinel, response monitoring.Response) {
	t.Helper()
	want := monitoring.Summary{}
	var micro int64
	for _, p := range f.groups {
		want.TotalCalls += p.Calls
		want.SuccessCalls += p.SuccessCalls
		want.FailureCalls += p.FailureCalls
		want.InputTokens += p.InputTokens
		want.OutputTokens += p.OutputTokens
		want.CachedTokens += p.CachedTokens
		want.CacheReadTokens += p.CacheReadTokens
		want.CacheCreationTokens += p.CacheCreationTokens
		want.TotalTokens += p.TotalTokens
		micro += sentinelMicrodollars(p)
	}
	want.TotalCost = float64(micro) / 1e6
	got := response.Summary
	if got == nil || got.TotalCalls != want.TotalCalls || got.SuccessCalls != want.SuccessCalls || got.FailureCalls != want.FailureCalls ||
		got.InputTokens != want.InputTokens || got.OutputTokens != want.OutputTokens || got.CachedTokens != want.CachedTokens ||
		got.CacheReadTokens != want.CacheReadTokens || got.CacheCreationTokens != want.CacheCreationTokens || got.TotalTokens != want.TotalTokens ||
		got.ReasoningTokens != 0 || math.Abs(got.TotalCost-want.TotalCost) > 1e-9*math.Max(1, want.TotalCost) {
		t.Fatalf("overview summary=%+v expected additive/cost fields=%+v", got, want)
	}
	var calls, tokens, success, failed int64
	var cost float64
	for _, point := range response.Timeline {
		calls += point.Calls
		tokens += point.TotalTokens
		success += point.Success
		failed += point.Failure
		cost += point.Cost
		// UTC and modern Hong Kong both have whole-hour, non-DST offsets.
		// The first/last buckets retain their hour bounds, not request clipping.
		if !point.BucketRangeExact || point.BucketMS%3600000 != 0 || point.BucketEndMS != point.BucketMS+3600000 {
			t.Fatalf("%s bucket lost exact hourly membership: %+v", f.location, point)
		}
	}
	if calls != want.TotalCalls || tokens != want.TotalTokens || success != want.SuccessCalls || failed != want.FailureCalls || math.Abs(cost-want.TotalCost) > 1e-9*math.Max(1, want.TotalCost) {
		t.Fatalf("timeline totals=%d/%d/%d/%d/%g want=%+v", calls, tokens, success, failed, cost, want)
	}
}

func checkOverviewPercentileOracle(t testing.TB, f pageSentinel, response *monitoring.PercentilesResponse) {
	t.Helper()
	type samples struct{ latency, ttft []int64 }
	all := &samples{}
	buckets := map[int64]*samples{}
	for i := 0; i < f.count; i++ {
		timestamp := f.fromMS + int64(i/8)*(f.toMS-f.fromMS)/int64((f.count+7)/8)
		bucket := timestamp - timestamp%3600000
		values := buckets[bucket]
		if values == nil {
			values = &samples{}
			buckets[bucket] = values
		}
		if i%13 != 0 {
			value := int64(20 + i%1700)
			values.latency = append(values.latency, value)
			all.latency = append(all.latency, value)
		}
		if i%17 != 0 {
			value := int64(1 + (i*19)%380)
			values.ttft = append(values.ttft, value)
			all.ttft = append(all.ttft, value)
		}
	}
	check := func(label string, got *float64, values []int64) {
		t.Helper()
		if len(values) == 0 {
			if got != nil {
				t.Fatalf("%s=%v with no positive samples", label, *got)
			}
			return
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		want := float64(values[(95*len(values)+99)/100-1])
		if got == nil || *got != want {
			t.Fatalf("%s=%v want nearest-rank %g", label, got, want)
		}
	}
	check("summary latency", response.Summary.P95LatencyMS, all.latency)
	check("summary ttft", response.Summary.P95TTFTMS, all.ttft)
	for _, point := range *response.Timeline {
		values, ok := buckets[point.BucketMS]
		if !ok {
			t.Fatalf("unexpected percentile bucket %d", point.BucketMS)
		}
		check("bucket latency", point.P95LatencyMS, values.latency)
		check("bucket ttft", point.P95TTFTMS, values.ttft)
	}
}

func TestOverviewProfilesDescribeTheirActualWork(t *testing.T) {
	// A frozen, non-midnight rolling 24h request, not a calendar-day range.
	const fromMS, toMS int64 = 1794293710560, 1794380110560
	for _, v2 := range []bool{false, true} {
		sections := overviewPageRequests(fromMS, toMS, "Asia/Hong_Kong", v2)
		if len(sections) != 4 {
			t.Fatal("unexpected section count")
		}
		for _, section := range sections {
			r := section.request
			if r.FromMS != fromMS || r.ToMS != toMS || r.NowMS != toMS || r.ToMS-r.FromMS != 86400000 || r.TimeZone != "Asia/Hong_Kong" {
				t.Fatalf("%s changed frozen request bounds/clock/zone: %+v", section.name, r)
			}
			if (!v2 || section.name != "core") && r.Include.AnomalyBasisVersion != nil {
				t.Fatalf("v2=%t section=%s must omit anomaly_basis_version", v2, section.name)
			}
		}
		core, exact := sections[1].request.Include, sections[3].request.Include
		if !core.SummaryComparison || !core.ModelStats || !core.Timeline || exact.AnomalyPoints == v2 || exact.Percentiles == nil || !exact.Percentiles.Summary || !exact.Percentiles.Timeline {
			t.Fatalf("mislabelled v2=%t core=%+v exact=%+v", v2, core, exact)
		}
		if v2 && (core.AnomalyBasisVersion == nil || *core.AnomalyBasisVersion != 1) {
			t.Fatal("v2 core must explicitly request anomaly basis version 1")
		}
		if exact.Timeline || exact.ModelStats || exact.Summary || exact.Heatmap {
			t.Fatal("exact profile unexpectedly requests core or dimension fields")
		}
	}
}

func TestAnalyticsPageFrozenHongKong24Hours(t *testing.T) {
	if testdb.MySQL() {
		t.Skip("small SQLite request/oracle check; native sentinel covers this zone separately")
	}
	f := newPageSentinel(t, 256, 1, "low", "mixed").inHongKong(t)
	if f.fromMS != 1794293710560 || f.toMS != 1794380110560 {
		t.Fatal("fixture no longer matches frozen Hong Kong request")
	}
	checkPageSentinel(t, f, false)
	checkOverviewPageProfiles(t, f)
}

package monitoring

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

type failingAnalyticsPriceBook struct {
	modelprice.Repository
	reads atomic.Int64
	err   error
}

func (r *failingAnalyticsPriceBook) LoadAll(context.Context) (map[string]store.ModelPrice, error) {
	r.reads.Add(1)
	return nil, errors.New("analytics used management book")
}

func (r *failingAnalyticsPriceBook) LoadRates(context.Context) (map[string]store.ModelPrice, error) {
	r.reads.Add(1)
	return nil, r.err
}

func TestAnalyticsPriceDependencyPlan(t *testing.T) {
	for _, include := range unpricedAnalyticsIncludes() {
		for _, exact := range []*PercentilesInclude{nil, {Summary: true}, {Timeline: true}, {Summary: true, Timeline: true}} {
			include.Percentiles = exact
			plan := planAnalytics(include)
			if plan.priceBook || plan.priceSnapshot {
				t.Fatalf("unpriced sections depend on prices: %+v -> %+v", include, plan)
			}
		}
	}
	for _, include := range []Include{
		{Summary: true}, {Timeline: true}, {Timeline: true, TimelineProfile: "core", AnomalyBasisVersion: new(1)},
		{ModelShare: true}, {ChannelShare: true}, {ModelStats: true},
		{AccountStats: true}, {CredentialStats: true}, {CredentialTimeline: true},
		{APIKeyTimeline: true}, {APIKeyStats: true}, {FilterOptions: true},
		{Heatmap: true}, {AnomalyPoints: true}, {FilterSelectors: true, Summary: true},
	} {
		include.Percentiles = &PercentilesInclude{Summary: true, Timeline: true}
		plan := planAnalytics(include)
		if !plan.priceBook || !plan.priceSnapshot {
			t.Fatalf("priced section lost price dependency: %+v -> %+v", include, plan)
		}
	}
	plan := planAnalytics(Include{EventsPage: &EventsPage{}})
	if plan.priceBook || !plan.priceSnapshot {
		t.Fatalf("events page requires gate but not book: %+v", plan)
	}
}

func unpricedAnalyticsIncludes() []Include {
	return []Include{
		{}, {FilterSelectors: true}, {FilterSelectors: true, FilterOptions: true},
		{DrilldownPreview: &DrilldownPreview{FromMS: 1, ToMS: 2, Limit: 2}},
		{HourlyDistribution: true}, {FailureSources: true}, {TaskBuckets: true}, {RecentFailures: 1},
		{FilterSelectors: true, FilterOptions: true, HourlyDistribution: true, FailureSources: true,
			TaskBuckets: true, RecentFailures: 1, DrilldownPreview: &DrilldownPreview{FromMS: 1, ToMS: 2, Limit: 2}},
	}
}

func TestAnalyticsUnpricedSectionsDoNotReadPriceBook(t *testing.T) {
	st := newMonitoringTestStore(t)
	prices := &failingAnalyticsPriceBook{Repository: st.ModelPrices, err: errors.New("unexpected price book")}
	st.ModelPrices = prices
	svc := New(st)
	for _, include := range append(unpricedAnalyticsIncludes(), Include{EventsPage: &EventsPage{Limit: 2}}) {
		for _, exact := range []*PercentilesInclude{nil, {Summary: true, Timeline: true}} {
			include.Percentiles = exact
			if _, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: include}); err != nil {
				t.Fatalf("include=%+v: %v", include, err)
			}
		}
	}
	if prices.reads.Load() != 0 {
		t.Fatalf("unpriced book reads=%d", prices.reads.Load())
	}
}

func TestAnalyticsStandalonePercentilesDoNotReadPriceBook(t *testing.T) {
	st := newMonitoringTestStore(t)
	const from = int64(1_800_000_000_000)
	latency, ttft := int64(250), int64(25)
	event := monitoringEvent("price-independent", from+1, "model", "auth", "source", false, 10, 20, 0, 0, 30, &latency)
	event.TTFTMS = &ttft
	if _, err := st.InsertEvents(context.Background(), []usage.Event{event}); err != nil {
		t.Fatal(err)
	}
	prices := &failingAnalyticsPriceBook{Repository: st.ModelPrices, err: errors.New("price book unavailable")}
	st.ModelPrices = prices
	svc := New(st)
	for _, include := range []PercentilesInclude{{Summary: true}, {Timeline: true}, {Summary: true, Timeline: true}} {
		response, err := svc.Analytics(context.Background(), Request{
			FromMS: from, ToMS: from + 1000, Include: Include{Percentiles: &include, Granularity: "hour"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if prices.reads.Load() != 0 {
			t.Fatal("standalone percentiles read prices")
		}
		if response.Percentiles == nil || response.Summary != nil || response.Timeline != nil {
			t.Fatalf("unrequested price-dependent sections: %+v", response)
		}
		assertExact := func(gotLatency, gotTTFT *float64) {
			t.Helper()
			if gotLatency == nil || *gotLatency != 250 || gotTTFT == nil || *gotTTFT != 25 {
				t.Fatalf("percentile values: latency=%v ttft=%v", gotLatency, gotTTFT)
			}
		}
		if include.Summary {
			if response.Percentiles.Summary == nil {
				t.Fatal("missing exact summary")
			}
			assertExact(response.Percentiles.Summary.P95LatencyMS, response.Percentiles.Summary.P95TTFTMS)
		}
		if include.Timeline {
			if response.Percentiles.Timeline == nil || len(*response.Percentiles.Timeline) != 1 {
				t.Fatal("missing exact timeline")
			}
			point := (*response.Percentiles.Timeline)[0]
			assertExact(point.P95LatencyMS, point.P95TTFTMS)
		}
	}
}

func TestAnalyticsPricedSectionsStillReadPriceBook(t *testing.T) {
	st := newMonitoringTestStore(t)
	want := errors.New("price book unavailable")
	prices := &failingAnalyticsPriceBook{Repository: st.ModelPrices, err: want}
	st.ModelPrices = prices
	svc := New(st)
	for _, include := range []Include{
		{Summary: true, SummaryProfile: "compact"},
		{Timeline: true, TimelineProfile: "core"},
		{AnomalyPoints: true},
		{Percentiles: &PercentilesInclude{Summary: true, Timeline: true}, AnomalyPoints: true},
		{Percentiles: &PercentilesInclude{Summary: true}, ModelStats: true},
	} {
		before := prices.reads.Load()
		_, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: include})
		if !errors.Is(err, want) || prices.reads.Load() != before+1 {
			t.Fatalf("priced request bypassed price book: include=%+v err=%v", include, err)
		}
	}
}

func TestAnalyticsStandalonePercentilesDoNotWaitForPriceWriter(t *testing.T) {
	st := newMonitoringTestStore(t)
	release := make(chan struct{})
	prices := &blockingPriceRepository{Repository: st.ModelPrices, entered: make(chan struct{}), release: release}
	st.ModelPrices = prices
	writerDone := make(chan error, 1)
	go func() { writerDone <- st.SaveModelPrices(context.Background(), nil) }()
	<-prices.entered
	defer func() {
		close(release)
		if err := <-writerDone; err != nil {
			t.Error(err)
		}
	}()
	svc := NewWithOptions(st, Options{Timeout: time.Second})
	for _, include := range unpricedAnalyticsIncludes() {
		for _, exact := range []*PercentilesInclude{nil, {Summary: true}, {Timeline: true}, {Summary: true, Timeline: true}} {
			include.Percentiles = exact
			_, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: include})
			if err != nil {
				t.Fatalf("unpriced request waited for price writer: include=%+v err=%v", include, err)
			}
		}
	}
	// Adding priced core or anomalies must still wait for that same writer,
	// with the wait counted against the analytics budget.
	svc = NewWithOptions(st, Options{Timeout: 20 * time.Millisecond})
	for _, include := range []Include{
		{Summary: true, SummaryProfile: "compact", Percentiles: &PercentilesInclude{Summary: true}},
		{AnomalyPoints: true, Percentiles: &PercentilesInclude{Timeline: true}},
		{Timeline: true, TimelineProfile: "core", AnomalyBasisVersion: new(1), FilterSelectors: true},
		{Heatmap: true, DrilldownPreview: &DrilldownPreview{}},
		{FilterOptions: true}, {EventsPage: &EventsPage{Limit: 2}},
	} {
		_, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: include})
		if !errors.Is(err, ErrAnalyticsDeadlineExceeded) {
			t.Fatalf("priced request bypassed active writer: include=%+v err=%v", include, err)
		}
	}
}

type pausedAnalyticsPercentiles struct {
	usageevent.Repository
	started chan struct{}
	release <-chan struct{}
}

func (r *pausedAnalyticsPercentiles) LatencyMetricsWithFilter(ctx context.Context, _ usageevent.AnalyticsFilter, _ string, _ *time.Location, _, _ bool) (usageevent.LatencyMetrics, error) {
	close(r.started)
	select {
	case <-r.release:
		return usageevent.LatencyMetrics{}, nil
	case <-ctx.Done():
		return usageevent.LatencyMetrics{}, ctx.Err()
	}
}

func TestAnalyticsPriceSnapshotHeldOnlyForDependentRequests(t *testing.T) {
	for _, priced := range []bool{false, true} {
		st := newMonitoringTestStore(t)
		release := make(chan struct{})
		latency := &pausedAnalyticsPercentiles{Repository: st.UsageEvents, started: make(chan struct{}), release: release}
		st.UsageEvents = latency
		svc := NewWithOptions(st, Options{Timeout: 2 * time.Second})
		include := Include{Summary: priced, SummaryProfile: "compact", Percentiles: &PercentilesInclude{Summary: true}}
		done := make(chan error, 1)
		go func() {
			_, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: include})
			done <- err
		}()
		select {
		case <-latency.started:
		case err := <-done:
			t.Fatalf("request exited before percentile stage: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		writerErr := st.SaveModelPrices(ctx, nil)
		cancel()
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if priced && !errors.Is(writerErr, context.DeadlineExceeded) {
			t.Fatalf("dependent request released snapshot too early: %v", writerErr)
		}
		if !priced && writerErr != nil {
			t.Fatalf("independent exact request delayed price mutation: %v", writerErr)
		}
		if err := st.SaveModelPrices(context.Background(), nil); err != nil {
			t.Fatalf("price permit leaked: %v", err)
		}
	}
}

func TestAnalyticsStandalonePercentilesKeepExecutionBudgets(t *testing.T) {
	for _, stage := range []string{"request", "percentile", "query", "backend", "parent"} {
		t.Run(stage, func(t *testing.T) {
			st := newMonitoringTestStore(t)
			svc := NewWithOptions(st, Options{Timeout: 25 * time.Millisecond, MaxConcurrent: 1, QueueSize: 1, AdmissionTimeout: time.Second})
			ctx := context.Background()
			var release func()
			switch stage {
			case "request":
				var err error
				release, err = svc.execution.admit(ctx)
				if err != nil {
					t.Fatal(err)
				}
			case "percentile":
				svc.execution.percentiles <- struct{}{}
				release = func() { <-svc.execution.percentiles }
			case "query":
				for range cap(svc.execution.queries) {
					svc.execution.queries <- struct{}{}
				}
				release = func() {
					for range cap(svc.execution.queries) {
						<-svc.execution.queries
					}
				}
			case "backend", "parent":
				st.UsageEvents = &pausedAnalyticsPercentiles{Repository: st.UsageEvents, started: make(chan struct{})}
				if stage == "parent" {
					svc.execution.timeout = time.Second
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 25*time.Millisecond)
					defer cancel()
				}
			}
			_, err := svc.Analytics(ctx, Request{FromMS: 1, ToMS: 2, Include: Include{Percentiles: &PercentilesInclude{Summary: true}}})
			if release != nil {
				release()
			}
			if !errors.Is(err, ErrAnalyticsDeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline at %s: %v", stage, err)
			}
			diagnostics := svc.Diagnostics()
			if diagnostics.Active != 0 || diagnostics.Queued != 0 || diagnostics.PercentilesActive != 0 || diagnostics.BackgroundQueriesActive != 0 {
				t.Fatalf("permits leaked at %s: %+v", stage, diagnostics)
			}
		})
	}
}

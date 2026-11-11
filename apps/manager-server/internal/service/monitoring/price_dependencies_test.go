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
	return nil, r.err
}

func TestAnalyticsPriceDependencyPlan(t *testing.T) {
	for _, percentiles := range []PercentilesInclude{{Summary: true}, {Timeline: true}, {Summary: true, Timeline: true}} {
		if planAnalytics(Include{Percentiles: &percentiles}).modelPrices {
			t.Fatalf("standalone percentiles depend on prices: %+v", percentiles)
		}
	}
	// The exemption is deliberately narrow: do not change any existing section's
	// consistency contract just because it is combined with explicit percentiles.
	for _, include := range []Include{
		{Summary: true}, {Timeline: true}, {Timeline: true, TimelineProfile: "core"},
		{HourlyDistribution: true}, {ModelShare: true}, {ChannelShare: true}, {ModelStats: true},
		{FailureSources: true}, {AccountStats: true}, {CredentialStats: true}, {CredentialTimeline: true},
		{APIKeyTimeline: true}, {APIKeyStats: true}, {FilterOptions: true}, {FilterSelectors: true},
		{Heatmap: true}, {AnomalyPoints: true}, {TaskBuckets: true}, {RecentFailures: 1},
		{EventsPage: &EventsPage{}}, {DrilldownPreview: &DrilldownPreview{}},
	} {
		include.Percentiles = &PercentilesInclude{Summary: true, Timeline: true}
		if !planAnalytics(include).modelPrices {
			t.Fatalf("existing section lost price snapshot: %+v", include)
		}
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
	for _, include := range []PercentilesInclude{{Summary: true}, {Timeline: true}, {Summary: true, Timeline: true}} {
		_, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: Include{Percentiles: &include}})
		if err != nil {
			t.Fatalf("standalone exact request waited for price writer: %v", err)
		}
	}
	// Adding priced core or anomalies must still wait for that same writer,
	// with the wait counted against the analytics budget.
	svc = NewWithOptions(st, Options{Timeout: 20 * time.Millisecond})
	for _, include := range []Include{
		{Summary: true, SummaryProfile: "compact", Percentiles: &PercentilesInclude{Summary: true}},
		{AnomalyPoints: true, Percentiles: &PercentilesInclude{Timeline: true}},
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

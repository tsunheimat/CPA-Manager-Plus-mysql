package monitoring

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

func waitExecutionCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("execution condition did not become true")
		case <-ticker.C:
		}
	}
}

func TestAnalyticsExecutionDefaultsAndPoolReservation(t *testing.T) {
	for _, options := range []Options{Options{}, OptionsFromConfig(config.Config{})} {
		e := newAnalyticsExecution(options)
		if e.timeout != 20*time.Second || cap(e.requests) != 2 || cap(e.population) != 6 || e.admissionTimeout != 500*time.Millisecond {
			t.Fatalf("defaults=%+v", e)
		}
	}
	e := newAnalyticsExecution(Options{MaxConcurrent: 8}, 10)
	if cap(e.requests)+cap(e.queries) > 8 {
		t.Fatal("analytics consumed reserved pool capacity")
	}
	e = newAnalyticsExecution(OptionsFromConfig(config.Config{AnalyticsMaxConcurrent: 2, AnalyticsQueueSize: 0}))
	if cap(e.population) != 2 {
		t.Fatal("explicit no-queue configuration ignored")
	}
}

func TestAnalyticsAdmissionBoundedQueueCancellationAndTimeout(t *testing.T) {
	e := newAnalyticsExecution(Options{MaxConcurrent: 1, QueueSize: 1, AdmissionTimeout: time.Second})
	release, err := e.admit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	queued, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		permit, err := e.admit(queued)
		if permit != nil {
			permit()
		}
		done <- err
	}()
	waitExecutionCondition(t, func() bool { return len(e.population) == 2 })
	if _, err := e.admit(context.Background()); !errors.Is(err, ErrAnalyticsOverloaded) {
		t.Fatalf("queue overflow=%v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission=%v", err)
	}
	if len(e.population) != 1 {
		t.Fatal("cancellation leaked queue slot")
	}
	e.admissionTimeout = 10 * time.Millisecond
	if _, err := e.admit(context.Background()); !errors.Is(err, ErrAnalyticsOverloaded) {
		t.Fatalf("admission timeout=%v", err)
	}
	ctx, cancelDeadline := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelDeadline()
	if _, err := e.admit(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parent deadline=%v", err)
	}
	release()
	release()
	if len(e.requests) != 0 || len(e.population) != 0 {
		t.Fatal("admission permit leaked")
	}
}

func TestAnalyticsQueryGroupSuccessfulWaitKeepsDependentContext(t *testing.T) {
	group := newAnalyticsQueryGroup(context.Background(), 2)
	group.Go(func(ctx context.Context) error { return ctx.Err() })
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := group.ctx.Err(); err != nil {
		t.Fatalf("later stages inherited cancellation: %v", err)
	}
	group.Close()
	if !errors.Is(group.ctx.Err(), context.Canceled) {
		t.Fatal("Close did not release context")
	}
}

func TestAnalyticsSharedBackgroundAndPercentileAdmission(t *testing.T) {
	e := newAnalyticsExecution(Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	groups := []*analyticsQueryGroup{newAnalyticsQueryGroup(ctx, 2), newAnalyticsQueryGroup(ctx, 2)}
	var active, maximum atomic.Int64
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	for _, g := range groups {
		g.execution = e
		for range 3 {
			g.GoStage("percentiles", func(ctx context.Context) error {
				current := active.Add(1)
				defer active.Add(-1)
				maximum.Store(max(maximum.Load(), current))
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}
	}
	<-started
	// Waiting expensive jobs may not block ordinary background work.
	ordinary := make(chan struct{})
	groups[1].GoStage("heatmap", func(context.Context) error { close(ordinary); return nil })
	select {
	case <-ordinary:
	case <-ctx.Done():
		t.Fatal("percentile waiters consumed shared query admission")
	}
	if len(e.queries) != 1 {
		t.Fatalf("active shared queries=%d", len(e.queries))
	}
	close(release)
	for _, g := range groups {
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		g.Close()
	}
	if maximum.Load() != 1 || len(e.queries) != 0 || len(e.percentiles) != 0 {
		t.Fatal("percentile execution was not serialized or leaked permits")
	}
}

type blockingPriceRepository struct {
	modelprice.Repository
	entered chan struct{}
	release <-chan struct{}
}

func (r *blockingPriceRepository) ReplaceAll(ctx context.Context, prices map[string]store.ModelPrice) error {
	close(r.entered)
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestAnalyticsTotalDeadlineIncludesPriceGate(t *testing.T) {
	st := newMonitoringTestStore(t)
	release := make(chan struct{})
	prices := &blockingPriceRepository{Repository: st.ModelPrices, entered: make(chan struct{}), release: release}
	st.ModelPrices = prices
	writerDone := make(chan error, 1)
	go func() { writerDone <- st.SaveModelPrices(context.Background(), nil) }()
	<-prices.entered
	svc := NewWithOptions(st, Options{Timeout: 20 * time.Millisecond})
	_, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: Include{Summary: true, SummaryProfile: "compact"}})
	close(release)
	if writerErr := <-writerDone; writerErr != nil {
		t.Fatal(writerErr)
	}
	if !errors.Is(err, ErrAnalyticsDeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("price gate deadline=%v", err)
	}
	if svc.Diagnostics().Active != 0 || svc.Diagnostics().Deadlines != 1 {
		t.Fatalf("diagnostics=%+v", svc.Diagnostics())
	}
	if _, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2}); err != nil {
		t.Fatalf("permit leaked after deadline: %v", err)
	}
}

func TestAnalyticsTotalDeadlineIncludesAdmissionQueue(t *testing.T) {
	svc := NewWithOptions(newMonitoringTestStore(t), Options{Timeout: 15 * time.Millisecond, MaxConcurrent: 1, QueueSize: 1, AdmissionTimeout: time.Second})
	release, err := svc.execution.admit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2})
	if !errors.Is(err, ErrAnalyticsDeadlineExceeded) {
		t.Fatalf("queue budget=%v", err)
	}
	if len(svc.execution.population) != 1 {
		t.Fatal("queued deadline leaked population")
	}
}

func TestAnalyticsSourceErrorsNeverRetryRaw(t *testing.T) {
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, store.ErrUsagePricingCoverageIncomplete, errors.New("SQL unavailable")} {
		called := false
		_, _, err := analyticsReadSource(context.Background(), "aggregate", func() (int, bool, error) { return 0, false, want }, func() (int, error) { called = true; return 1, nil })
		if !errors.Is(err, want) || called {
			t.Fatalf("err=%v raw=%v", err, called)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	called := false
	_, _, err := analyticsReadSource(ctx, "aggregate", func() (int, bool, error) { cancel(); return 0, false, nil }, func() (int, error) { called = true; return 1, nil })
	if !errors.Is(err, context.Canceled) || called {
		t.Fatal("fallback ignored exhausted budget")
	}
	value, available, err := analyticsReadSource(context.Background(), "aggregate", func() (int, bool, error) { return 0, false, nil }, func() (int, error) { return 7, nil })
	if err != nil || available || value != 7 {
		t.Fatal("unavailable derived source did not use compatible fallback")
	}
}

type coordinatedFailureRepository struct {
	usageevent.Repository
	backgroundFails bool
	started         chan struct{}
	exited          chan struct{}
	failure         error
}

func (r *coordinatedFailureRepository) LatencyMetricsWithFilter(ctx context.Context, _ usageevent.AnalyticsFilter, _ string, _ *time.Location, _, _ bool) (usageevent.LatencyMetrics, error) {
	close(r.started)
	defer close(r.exited)
	if r.backgroundFails {
		return usageevent.LatencyMetrics{}, r.failure
	}
	<-ctx.Done()
	return usageevent.LatencyMetrics{}, ctx.Err()
}

func (r *coordinatedFailureRepository) TaskBucketsWithFilter(ctx context.Context, _ usageevent.AnalyticsFilter) ([]usageevent.TaskBucket, error) {
	select {
	case <-r.started:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if !r.backgroundFails {
		return nil, r.failure
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAnalyticsForegroundAndBackgroundCancelEachOtherAndJoin(t *testing.T) {
	for _, backgroundFails := range []bool{false, true} {
		st := newMonitoringTestStore(t)
		want := errors.New("terminal stage failure")
		fake := &coordinatedFailureRepository{Repository: st.UsageEvents, backgroundFails: backgroundFails, started: make(chan struct{}), exited: make(chan struct{}), failure: want}
		st.UsageEvents = fake
		svc := NewWithOptions(st, Options{Timeout: time.Second})
		_, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 2, Include: Include{Summary: true}})
		if !errors.Is(err, want) {
			t.Fatalf("original terminal error lost: %v", err)
		}
		select {
		case <-fake.exited:
		default:
			t.Fatal("analytics returned with abandoned background query")
		}
		if len(svc.execution.queries) != 0 || len(svc.execution.percentiles) != 0 || svc.Diagnostics().Active != 0 {
			t.Fatal("stage failure leaked admission")
		}
	}
}

func TestAnalyticsObservationsDoNotLogErrorOrRequestInputs(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	ctx := WithRequestID(context.Background(), "forged\nsecret")
	logAnalyticsStage(ctx, "aggregate", time.Now(), errors.New("password=secret search=private"))
	if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "private") || strings.Contains(output.String(), "forged") {
		t.Fatalf("unsanitized log: %s", output.String())
	}
	if !strings.Contains(output.String(), "outcome=query_error") {
		t.Fatal("missing stable observation")
	}
}

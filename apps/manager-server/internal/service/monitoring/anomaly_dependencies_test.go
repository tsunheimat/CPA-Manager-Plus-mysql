package monitoring

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageaggregate"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

type countedAnomalyEvents struct {
	usageevent.Repository
	timelines, models, exact atomic.Int64
	exactErr                 error
}

func (r *countedAnomalyEvents) TimelineWithFilter(ctx context.Context, filter usageevent.AnalyticsFilter, granularity string, location *time.Location) ([]usageevent.TimelinePoint, error) {
	r.timelines.Add(1)
	return r.Repository.TimelineWithFilter(ctx, filter, granularity, location)
}
func (r *countedAnomalyEvents) ModelStatsWithFilter(ctx context.Context, filter usageevent.AnalyticsFilter, limit int) ([]usageevent.ModelStat, error) {
	r.models.Add(1)
	return r.Repository.ModelStatsWithFilter(ctx, filter, limit)
}
func (r *countedAnomalyEvents) LatencyMetricsWithFilter(ctx context.Context, filter usageevent.AnalyticsFilter, granularity string, location *time.Location, summary, timeline bool) (usageevent.LatencyMetrics, error) {
	r.exact.Add(1)
	if r.exactErr != nil {
		return usageevent.LatencyMetrics{}, r.exactErr
	}
	return r.Repository.LatencyMetricsWithFilter(ctx, filter, granularity, location, summary, timeline)
}

type countedAnomalyBook struct {
	modelprice.Repository
	rates, txRates, management atomic.Int64
}

func (r *countedAnomalyBook) LoadRates(ctx context.Context) (map[string]store.ModelPrice, error) {
	r.rates.Add(1)
	return r.Repository.LoadRates(ctx)
}
func (r *countedAnomalyBook) LoadRatesTx(ctx context.Context, tx *sql.Tx) (map[string]store.ModelPrice, error) {
	r.txRates.Add(1)
	return r.Repository.LoadRatesTx(ctx, tx)
}
func (r *countedAnomalyBook) LoadAll(context.Context) (map[string]store.ModelPrice, error) {
	r.management.Add(1)
	return nil, errors.New("management book forbidden in analytics")
}
func (r *countedAnomalyBook) LoadAllTx(context.Context, *sql.Tx) (map[string]store.ModelPrice, error) {
	r.management.Add(1)
	return nil, errors.New("management book forbidden in analytics")
}

type forbiddenAnomalyHourly struct {
	usageaggregate.Repository
	reads atomic.Int64
}

func (r *forbiddenAnomalyHourly) LoadRowsTx(context.Context, *sql.Tx, usageaggregate.Filter) ([]usageaggregate.Row, usageaggregate.State, bool, error) {
	r.reads.Add(1)
	return nil, usageaggregate.State{}, false, errors.New("hourly core must not run for exact")
}

type forbiddenAnomalyPricing struct {
	usagepricing.Repository
	reads atomic.Int64
}

func (r *forbiddenAnomalyPricing) LoadHourlyRowsTx(context.Context, *sql.Tx, usagepricing.HourlyFilter) ([]usagepricing.HourlyRow, usagepricing.State, bool, error) {
	r.reads.Add(1)
	return nil, usagepricing.State{}, false, errors.New("hourly pricing must not run for exact")
}

func TestAnomalyV2FrozenCoreAcrossMutationsAndArchive(t *testing.T) {
	st, _, request, _ := pricingCoverageFixture(t)
	ctx := context.Background()
	request.Include = Include{Timeline: true, TimelineProfile: "core", AnomalyBasisVersion: new(1), Granularity: "hour"}
	svc := New(st, true)
	core, err := svc.Analytics(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if core.Coverage == nil || !core.Coverage.CoreAggregateUsed || core.Coverage.RawComplete {
		t.Fatalf("core provenance=%+v", core.Coverage)
	}
	frozen := append([]AnomalyBasisCandidate(nil), core.AnomalyBasis.Candidates...)
	prices, err := st.LoadModelPrices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	price := prices["tiered"]
	price.Prompt *= 10
	price.ContextTiers[0].Prompt *= 10
	prices["tiered"] = price
	if err := st.SaveModelPrices(ctx, prices); err != nil {
		t.Fatal(err)
	}
	latency := int64(9999)
	event := monitoringEvent("post-core", request.ToMS-1, "service", "b", "b", false, 100, 10, 0, 0, 110, &latency)
	if _, err := st.InsertEvents(ctx, []usage.Event{event}); err != nil {
		t.Fatal(err)
	}
	catchUpMonitoringArchiveDeleteReadiness(t, ctx, st)
	archiveMonitoringEventsThrough(t, ctx, st, request.FromMS+3*time.Hour.Milliseconds())
	request.Include = Include{Percentiles: &PercentilesInclude{Summary: true, Timeline: true}, Granularity: "hour"}
	exact, err := svc.Analytics(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if exact.Coverage == nil || exact.Coverage.CoreAggregateUsed || exact.Coverage.RawComplete ||
		exact.Coverage.RawDeletedEventCount <= core.Coverage.RawDeletedEventCount || len(*exact.Percentiles.Timeline) >= len(core.Timeline) {
		t.Fatalf("independent exact provenance=%+v core=%+v", exact.Coverage, core.Coverage)
	}
	if !reflect.DeepEqual(frozen, core.AnomalyBasis.Candidates) {
		t.Fatal("core basis mutated")
	}
	request.Include = Include{Timeline: true, TimelineProfile: "core", AnomalyBasisVersion: new(1), Granularity: "hour"}
	fresh, err := svc.Analytics(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(frozen, fresh.AnomalyBasis.Candidates) || core.AnomalyBasis.CoreID == fresh.AnomalyBasis.CoreID {
		t.Fatal("fresh core must reflect price/insert mutations and have its own response identity")
	}
}

func TestAnomalyV2ExecutedDependencies(t *testing.T) {
	st := newMonitoringTestStore(t)
	const hour = int64(time.Hour / time.Millisecond)
	from := int64(1_800_000_000_000) / hour * hour
	for i, latency := range []int64{100, 200} {
		event := monitoringEvent(string(rune('a'+i)), from+int64(i)*2*hour+1000, "model", "auth", "source", false, 10, 20, 0, 0, 30, &latency)
		if _, err := st.InsertEvents(context.Background(), []usage.Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	events := &countedAnomalyEvents{Repository: st.UsageEvents}
	book := &countedAnomalyBook{Repository: st.ModelPrices}
	st.UsageEvents, st.ModelPrices = events, book
	request := Request{FromMS: from, ToMS: from + 3*hour, Include: Include{Timeline: true, TimelineProfile: "core", AnomalyBasisVersion: new(1), Granularity: "hour"}}
	core, err := New(st, false).Analytics(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if events.timelines.Load() != 1 || events.exact.Load() != 0 || events.models.Load() != 0 || book.rates.Load() != 1 || book.management.Load() != 0 {
		t.Fatalf("core executed timelines=%d exact=%d models=%d book=%d management=%d", events.timelines.Load(), events.exact.Load(), events.models.Load(), book.rates.Load(), book.management.Load())
	}
	if core.AnomalyBasis == nil || len(core.AnomalyBasis.Candidates) != 1 || core.AnomalyBasis.Candidates[0].RequestChange != 0 {
		t.Fatalf("missing latency-only candidate: %+v", core.AnomalyBasis)
	}
	hourly := &forbiddenAnomalyHourly{Repository: st.UsageAggregates}
	pricing := &forbiddenAnomalyPricing{Repository: st.UsagePricing}
	st.UsageAggregates, st.UsagePricing = hourly, pricing
	request.Include = Include{Percentiles: &PercentilesInclude{Summary: true, Timeline: true}, Granularity: "hour"}
	exact, err := New(st, true).Analytics(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if events.exact.Load() != 1 || events.timelines.Load() != 1 || events.models.Load() != 0 || book.rates.Load() != 1 || book.txRates.Load() != 0 || book.management.Load() != 0 || hourly.reads.Load() != 0 || pricing.reads.Load() != 0 {
		t.Fatal("v2 exact repeated priced work")
	}
	if exact.Percentiles == nil || len(*exact.Percentiles.Timeline) != 2 {
		t.Fatal("missing exact samples")
	}
	// Legacy exact still builds one additional priced timeline, using the same
	// finalizer. Do not optimize capability 0/1 into the v2 snapshot semantics.
	request.Include.AnomalyPoints = true
	legacy, err := New(st, false).Analytics(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if events.exact.Load() != 2 || events.timelines.Load() != 2 || book.rates.Load() != 2 || len(legacy.AnomalyPoints) != 1 {
		t.Fatal("legacy dependency/latency-only anomaly changed")
	}
	want := errors.New("attempted exact query failed")
	events.exactErr = want
	request.Include.AnomalyPoints = false
	if _, err := New(st, true).Analytics(context.Background(), request); !errors.Is(err, want) {
		t.Fatalf("exact error=%v", err)
	}
	if events.exact.Load() != 3 || events.timelines.Load() != 2 {
		t.Fatal("attempted query was retried/fell back")
	}
}

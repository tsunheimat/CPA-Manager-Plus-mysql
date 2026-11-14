package usagemonitoring

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/pricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// Literal credential identities keep window expectations independent of the
// rewritten source builders and the production AccountKey helper.
const derivedAccountA = "usage-account-history:3:file-index:612E6A736F6E:69"
const derivedAccountB = "usage-account-history:3:file-index:622E6A736F6E:69"

type derivedEvent struct {
	id, at, input, output, cached, cache, read, creation int64
	latency                                              any
	file                                                 string
}

func derivedMonitoringDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "derived-validation.sqlite")
	db, err := testdb.Open(t, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func derivedExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlwrite.ExecContext(context.Background(), db, query, args...); err != nil {
		t.Fatal(err)
	}
}

func insertDerivedEvents(t *testing.T, db *sql.DB, events ...derivedEvent) {
	t.Helper()
	for _, event := range events {
		file := event.file
		if file == "" {
			file = "a.json"
		}
		// input_tokens deliberately disagrees with the normalized value. Readers
		// must use normalized input, including its explicit zero.
		derivedExec(t, db, `insert into usage_events(id,event_hash,timestamp_ms,timestamp,model,requested_model,resolved_model,
			service_tier,auth_file_snapshot,auth_index,account_snapshot,auth_label_snapshot,source,source_hash,api_key_hash,
			input_tokens,normalized_total_input_tokens,output_tokens,cached_tokens,cache_tokens,cache_read_tokens,cache_creation_tokens,total_tokens,latency_ms,created_at_ms)
			values(?,?,?,'','display(high)','display(high)','bill','default',?,'i','account','label','source','source-hash','key',999999,?,?,?,?,?,?,?,?,0)`,
			event.id, fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprint(event.id)))), event.at, file,
			event.input, event.output, event.cached, event.cache, event.read, event.creation, event.input+event.output, event.latency)
	}
}

func projectDerivedEvents(t *testing.T, db *sql.DB, through int64) {
	t.Helper()
	result, err := New(db).CatchUpProjection(context.Background(), 100, 10000)
	if err != nil || result.Pending || result.CoverageEventID != through {
		t.Fatalf("projection checkpoint=%+v err=%v; want %d", result, err, through)
	}
}

func derivedTokens(band usage.PricingBand, long usage.LongContextTokens, input, output, cached, read, creation int64) pricing.ModelTokens {
	return pricing.ModelTokens{
		PricingModel: band.PricingModel, ContextThresholdTokens: band.ContextThresholdTokens,
		InputTokens: input, OutputTokens: output, CachedTokens: cached, CacheReadTokens: read, CacheCreationTokens: creation,
		LongInputTokens: long.LongInputTokens, LongOutputTokens: long.LongOutputTokens, LongCachedTokens: long.LongCachedTokens,
		LongCacheReadTokens: long.LongCacheReadTokens, LongCacheCreationTokens: long.LongCacheCreationTokens,
	}
}

// Only adapts returned fields; the expected values below are hand sums, never a
// second rewritten query, classifier, cache-normalization or monetary helper.
func assertDerivedMonitoringReaders(t *testing.T, db *sql.DB, want pricing.ModelTokens, calls, total int64, wantLatency sql.NullFloat64, latencySum, latencySamples int64, wantCost float64) {
	t.Helper()
	ctx := context.Background()
	repo := New(db)
	filter := AnalyticsFilter{FromMS: 1, ToMS: 100, IncludeFailed: true}
	book := map[string]model.ModelPrice{"bill": {Prompt: 2, Completion: 3, Cache: 0.5, CacheRead: 0.25, CacheCreation: 4}}
	check := func(name, modelName, billing, tier string, got pricing.ModelTokens, gotCalls, success, gotTotal int64) {
		t.Helper()
		if modelName != "display" || billing != "bill" || tier != "default" || gotCalls != calls || success != calls || gotTotal != total || got != want {
			t.Fatalf("%s identity=%s/%s/%s calls=%d success=%d total=%d tokens=%+v; want calls=%d total=%d tokens=%+v", name, modelName, billing, tier, gotCalls, success, gotTotal, got, calls, total, want)
		}
		cost := pricing.CostForModelCandidatesWithServiceTier([]string{billing, modelName}, tier, got, book)
		if math.Abs(cost-wantCost) > 1e-12 {
			t.Fatalf("%s cost=%g want hand-computed %g", name, cost, wantCost)
		}
	}
	models, state, available, err := repo.LoadModelStats(ctx, filter)
	if err != nil || !available || len(models) != 1 || state.CoverageEventID == 0 {
		t.Fatalf("models=%+v available=%v state=%+v err=%v", models, available, state, err)
	}
	m := models[0]
	check("models", m.Model, m.BillingModel, m.ServiceTier, derivedTokens(m.PricingBand, m.LongContextTokens, m.InputTokens, m.OutputTokens, m.CachedTokens, m.CacheReadTokens, m.CacheCreationTokens), m.Calls, m.SuccessCalls, m.TotalTokens)
	accounts, _, available, err := repo.LoadAccountStats(ctx, filter)
	if err != nil || !available || len(accounts) != 1 {
		t.Fatalf("accounts=%+v available=%v err=%v", accounts, available, err)
	}
	a := accounts[0]
	check("accounts", a.Model, a.BillingModel, a.ServiceTier, derivedTokens(a.PricingBand, a.LongContextTokens, a.InputTokens, a.OutputTokens, a.CachedTokens, a.CacheReadTokens, a.CacheCreationTokens), a.Calls, a.SuccessCalls, a.TotalTokens)
	if a.AvgLatencyMS != wantLatency || a.LatencySumMS != latencySum || a.LatencySamples != latencySamples || a.FailureCalls != 0 {
		t.Fatalf("account latency/success population=%+v", a)
	}
	keys, _, available, err := repo.LoadAPIKeyStats(ctx, filter)
	if err != nil || !available || len(keys) != 1 {
		t.Fatalf("keys=%+v available=%v err=%v", keys, available, err)
	}
	k := keys[0]
	check("keys", k.Model, k.BillingModel, k.ServiceTier, derivedTokens(k.PricingBand, k.LongContextTokens, k.InputTokens, k.OutputTokens, k.CachedTokens, k.CacheReadTokens, k.CacheCreationTokens), k.Calls, k.SuccessCalls, k.TotalTokens)
	if k.AvgLatencyMS != wantLatency || k.LatencySamples != latencySamples || k.FailureCalls != 0 {
		t.Fatalf("key latency/success population=%+v", k)
	}
	windows, _, available, err := repo.LoadAccountWindowStats(ctx, []AccountWindowUsageQuery{{RequestIndex: 7, FromMS: 1, ToMS: 100, AccountKey: derivedAccountA, AuthFileSnapshot: "a.json", AuthIndex: "i"}})
	if err != nil || !available || len(windows) != 1 {
		t.Fatalf("windows=%+v available=%v err=%v", windows, available, err)
	}
	w := windows[0]
	check("windows", w.Model, w.BillingModel, w.ServiceTier, derivedTokens(w.PricingBand, w.LongContextTokens, w.InputTokens, w.OutputTokens, w.CachedTokens, w.CacheReadTokens, w.CacheCreationTokens), w.Calls, w.SuccessCalls, w.TotalTokens)
	if w.RequestIndex != 7 || w.FailureCalls != 0 {
		t.Fatalf("window correlation/success=%+v", w)
	}
}

func TestDerivedMonitoringFinalGrainBeforeMonetaryClamp(t *testing.T) {
	db, _ := derivedMonitoringDB(t)
	derivedExec(t, db, `insert into model_prices(model,prompt_per_1m,completion_per_1m,cache_per_1m,updated_at_ms) values('bill',2,3,0.5,0)`)
	// The (input,cache) pair (0,100) is projected; (100,0) is raw. Three
	// token-free rows make the latency populations unequal, including NULL/0.
	insertDerivedEvents(t, db, derivedEvent{id: 1, at: 10, cached: 100, latency: int64(10)}, derivedEvent{id: 2, at: 11, latency: int64(20)}, derivedEvent{id: 3, at: 12})
	projectDerivedEvents(t, db, 3)
	insertDerivedEvents(t, db, derivedEvent{id: 4, at: 13, input: 100, latency: int64(90)}, derivedEvent{id: 5, at: 14, latency: int64(0)})
	want := pricing.ModelTokens{PricingModel: "bill", ContextThresholdTokens: -1, InputTokens: 100, CachedTokens: 100}
	// At the final grain prompt=max(100-100,0)=0, cache=100*0.5/1M.
	// Costing arms separately would add 100*2/1M, yielding 0.00025.
	assertDerivedMonitoringReaders(t, db, want, 5, 100, sql.NullFloat64{Float64: 40, Valid: true}, 120, 3, 0.00005)
}

func TestDerivedMonitoringAllLongMeasuresAtStrictBoundary(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input, sum int64
		long       usage.LongContextTokens
		cost       float64
	}{
		{"below", 271999, 543998, usage.LongContextTokens{}, 1.088020},
		{"equal", 272000, 544000, usage.LongContextTokens{}, 1.088024},
		{"above", 272001, 544002, usage.LongContextTokens{LongInputTokens: 544002, LongOutputTokens: 42, LongCachedTokens: 82, LongCacheReadTokens: 20, LongCacheCreationTokens: 28}, 1.088028},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := derivedMonitoringDB(t)
			derivedExec(t, db, `insert into model_prices(model,prompt_per_1m,completion_per_1m,cache_per_1m,updated_at_ms) values('bill',2,3,0.5,0)`)
			// Per-event compatibility cache is 100-7-11=82 and
			// max(10-13-17,0)=0, not max(110-20-28,0)=62.
			insertDerivedEvents(t, db, derivedEvent{id: 1, at: 10, input: tc.input, output: 19, cached: 60, cache: 100, read: 7, creation: 11})
			projectDerivedEvents(t, db, 1)
			insertDerivedEvents(t, db, derivedEvent{id: 2, at: 11, input: tc.input, output: 23, cached: 8, cache: 10, read: 13, creation: 17})
			want := pricing.ModelTokens{PricingModel: "bill", ContextThresholdTokens: -1, InputTokens: tc.sum, OutputTokens: 42, CachedTokens: 82, CacheReadTokens: 20, CacheCreationTokens: 28,
				LongInputTokens: tc.long.LongInputTokens, LongOutputTokens: tc.long.LongOutputTokens, LongCachedTokens: tc.long.LongCachedTokens, LongCacheReadTokens: tc.long.LongCacheReadTokens, LongCacheCreationTokens: tc.long.LongCacheCreationTokens}
			// Hand cost: (sum-130)*2 + 42*3 + 82*.5 + 20*.25 + 28*4, per million.
			assertDerivedMonitoringReaders(t, db, want, 2, tc.sum+42, sql.NullFloat64{}, 0, 0, tc.cost)
		})
	}
}

func TestDerivedMonitoringContextBandsDoNotMergeAcrossArms(t *testing.T) {
	db, _ := derivedMonitoringDB(t)
	derivedExec(t, db, `insert into model_prices(model,prompt_per_1m,completion_per_1m,cache_per_1m,updated_at_ms) values('bill',2,3,0.5,0)`)
	derivedExec(t, db, `insert into model_price_context_tiers(model,threshold_tokens) values('bill',272000)`)
	insertDerivedEvents(t, db, derivedEvent{id: 1, at: 10, input: 272000, output: 7}, derivedEvent{id: 2, at: 11, input: 272001, output: 11})
	projectDerivedEvents(t, db, 2)
	insertDerivedEvents(t, db, derivedEvent{id: 3, at: 12, input: 272000, output: 13}, derivedEvent{id: 4, at: 13, input: 272001, output: 17})
	rows, _, available, err := New(db).LoadModelStats(context.Background(), AnalyticsFilter{FromMS: 1, ToMS: 100, IncludeFailed: true})
	if err != nil || !available || len(rows) != 2 {
		t.Fatalf("rows=%+v available=%v err=%v", rows, available, err)
	}
	got := map[int64]pricing.ModelTokens{}
	for _, row := range rows {
		if row.Calls != 2 {
			t.Fatalf("band lost a source arm: %+v", row)
		}
		got[row.ContextThresholdTokens] = derivedTokens(row.PricingBand, row.LongContextTokens, row.InputTokens, row.OutputTokens, row.CachedTokens, row.CacheReadTokens, row.CacheCreationTokens)
	}
	want := map[int64]pricing.ModelTokens{
		-1:     {PricingModel: "bill", ContextThresholdTokens: -1, InputTokens: 544000, OutputTokens: 20},
		272000: {PricingModel: "bill", ContextThresholdTokens: 272000, InputTokens: 544002, OutputTokens: 28, LongInputTokens: 544002, LongOutputTokens: 28},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bands=%+v want=%+v", got, want)
	}
}

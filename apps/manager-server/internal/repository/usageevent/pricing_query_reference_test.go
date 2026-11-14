package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/pricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

// Frozen from 87d975a2. Deliberately independent of the production builder,
// projections, identity-expression variables and catalog-interval helper. Keep
// the old scalar classifier and unguarded fallback joins in this test oracle.
const frozenScalarPricingCTE = `with pricing_base_events as (
 select usage_events.*,
 coalesce(nullif(requested_model, ''), model, '') as requested_model_value,
 cpamp_analytics_model(coalesce(coalesce(nullif(requested_model, ''), model, ''), '')) as analytics_model_value,
 coalesce(nullif(resolved_model, ''), cpamp_analytics_model(coalesce(coalesce(nullif(requested_model, ''), model, ''), ''))) as billing_model_value,
 coalesce(normalized_total_input_tokens, input_tokens, 0) as normalized_input_tokens_value
 from usage_events
), pricing_resolved_events as (
 select pricing_base_events.*,
 case when billing_price.model is not null then billing_model_value
 when analytics_price.model is not null then pricing_base_events.analytics_model_value
 when display_price.model is not null then pricing_base_events.requested_model_value
 else billing_model_value end as pricing_model_value
 from pricing_base_events
 left join model_prices billing_price on billing_price.model = pricing_base_events.billing_model_value
 left join model_prices analytics_price on analytics_price.model = pricing_base_events.analytics_model_value
 left join model_prices display_price on display_price.model = pricing_base_events.requested_model_value
), banded_usage_events as (
 select pricing_resolved_events.*,
 coalesce((select max(tier.threshold_tokens) from model_price_context_tiers tier
 where tier.model = pricing_resolved_events.pricing_model_value
 and pricing_resolved_events.normalized_input_tokens_value > tier.threshold_tokens), -1) as context_threshold_tokens_value
 from pricing_resolved_events
)`

const frozenModelPricingAggregate = `select analytics_model_value, billing_model_value,
 pricing_model_value, context_threshold_tokens_value, coalesce(service_tier,''), count(*),
 sum(case when failed = 0 then 1 else 0 end),
 coalesce(sum(coalesce(normalized_total_input_tokens,input_tokens)),0),
 coalesce(sum(output_tokens),0), coalesce(sum(reasoning_tokens),0),
 coalesce(sum(max(max(cached_tokens,cache_tokens)-max(cache_read_tokens,0)-max(cache_creation_tokens,0),0)),0),
 coalesce(sum(cache_read_tokens),0), coalesce(sum(cache_creation_tokens),0),
 coalesce(sum(case when coalesce(normalized_total_input_tokens,input_tokens)>272000 then coalesce(normalized_total_input_tokens,input_tokens) else 0 end),0),
 coalesce(sum(case when coalesce(normalized_total_input_tokens,input_tokens)>272000 then output_tokens else 0 end),0),
 coalesce(sum(case when coalesce(normalized_total_input_tokens,input_tokens)>272000 then max(max(cached_tokens,cache_tokens)-max(cache_read_tokens,0)-max(cache_creation_tokens,0),0) else 0 end),0),
 coalesce(sum(case when coalesce(normalized_total_input_tokens,input_tokens)>272000 then cache_read_tokens else 0 end),0),
 coalesce(sum(case when coalesce(normalized_total_input_tokens,input_tokens)>272000 then cache_creation_tokens else 0 end),0),
 coalesce(sum(total_tokens),0)
 from banded_usage_events where timestamp_ms >= ? and timestamp_ms < ?
 group by analytics_model_value,billing_model_value,pricing_model_value,context_threshold_tokens_value,coalesce(service_tier,'')`

func pricingTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "pricing.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func pricingFixtureExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlwrite.ExecContext(context.Background(), db, query, args...); err != nil {
		t.Fatal(err)
	}
}

// Direct fixture inserts preserve imported raw/normalized disagreements and
// extreme integer values rather than passing through ingestion normalization.
func insertPricingEvent(t *testing.T, db *sql.DB, timestamp int64, display, requested, billing, tier string, input int64, normalized any) {
	t.Helper()
	pricingFixtureExec(t, db, `insert into usage_events(event_hash,timestamp_ms,timestamp,model,requested_model,resolved_model,service_tier,input_tokens,normalized_total_input_tokens,created_at_ms)
 values(?,?,'fixture',?,?,?,?,?,?,1)`, canonicalTestHash(fmt.Sprint(timestamp)), timestamp, display, requested, billing, tier, input, normalized)
}

func readPricingModelStats(t *testing.T, db *sql.DB, query string, args ...any) []ModelStat {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []ModelStat{}
	for rows.Next() {
		var s ModelStat
		if err := rows.Scan(&s.Model, &s.BillingModel, &s.PricingModel, &s.ContextThresholdTokens, &s.ServiceTier, &s.Calls, &s.SuccessCalls, &s.InputTokens, &s.OutputTokens, &s.ReasoningTokens, &s.CachedTokens, &s.CacheReadTokens, &s.CacheCreationTokens, &s.LongInputTokens, &s.LongOutputTokens, &s.LongCachedTokens, &s.LongCacheReadTokens, &s.LongCacheCreationTokens, &s.TotalTokens); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPricingIntervalsMatchFrozenScalarAndHandBands(t *testing.T) {
	db := pricingTestDB(t)
	longA, longB := strings.Repeat("界", 300)+"a", strings.Repeat("界", 300)+"b"
	for _, name := range []string{"billed", "analytic", "requested(high)", "zero", "none", "many", "extreme", longA, longB} {
		pricingFixtureExec(t, db, `insert into model_prices(model,prompt_per_1m,completion_per_1m,cache_per_1m,updated_at_ms) values(?,0,0,0,1)`, name)
	}
	for name, thresholds := range map[string][]int64{
		"billed": {100, 200}, "analytic": {50}, "requested(high)": {75}, "zero": {0},
		"extreme": {math.MaxInt64 - 1, math.MaxInt64}, longA: {100}, longB: {200},
	} {
		for _, threshold := range thresholds {
			pricingFixtureExec(t, db, `insert into model_price_context_tiers(model,threshold_tokens) values(?,?)`, name, threshold)
		}
	}
	for i := int64(1); i <= 128; i++ {
		pricingFixtureExec(t, db, `insert into model_price_context_tiers(model,threshold_tokens) values('many',?)`, i*10)
	}
	type band struct {
		Timestamp               int64
		Model, Billing, Pricing string
		Threshold, Input        int64
	}
	cases := []struct {
		display, requested, billing string
		raw                         int64
		normalized                  any
		want                        band
	}{
		{"display", "analytic(high)", "billed", 9, int64(99), band{Model: "analytic", Billing: "billed", Pricing: "billed", Threshold: -1, Input: 99}},
		{"display", "analytic(high)", "billed", 9, int64(100), band{Model: "analytic", Billing: "billed", Pricing: "billed", Threshold: -1, Input: 100}},
		{"display", "analytic(high)", "billed", 9, int64(101), band{Model: "analytic", Billing: "billed", Pricing: "billed", Threshold: 100, Input: 101}},
		{"display", "analytic(high)", "billed", 9, int64(200), band{Model: "analytic", Billing: "billed", Pricing: "billed", Threshold: 100, Input: 200}},
		{"display", "analytic(high)", "billed", 9, int64(201), band{Model: "analytic", Billing: "billed", Pricing: "billed", Threshold: 200, Input: 201}},
		{"display", "analytic(high)", "missing", 51, nil, band{Model: "analytic", Billing: "missing", Pricing: "analytic", Threshold: 50, Input: 51}},
		{"display", "requested(high)", "missing", 76, nil, band{Model: "requested", Billing: "missing", Pricing: "requested(high)", Threshold: 75, Input: 76}},
		{"display", "absent(unknown)", "missing", 1000, nil, band{Model: "absent(unknown)", Billing: "missing", Pricing: "missing", Threshold: -1, Input: 1000}},
		{"none", "", "", 1000, nil, band{Model: "none", Billing: "none", Pricing: "none", Threshold: -1, Input: 1000}},
		{"zero", "", "", 0, nil, band{Model: "zero", Billing: "zero", Pricing: "zero", Threshold: -1, Input: 0}},
		{"zero", "", "", 1, nil, band{Model: "zero", Billing: "zero", Pricing: "zero", Threshold: 0, Input: 1}},
		{"many", "", "", 1280, nil, band{Model: "many", Billing: "many", Pricing: "many", Threshold: 1270, Input: 1280}},
		{"many", "", "", 1281, nil, band{Model: "many", Billing: "many", Pricing: "many", Threshold: 1280, Input: 1281}},
		{"extreme", "", "", math.MinInt64, nil, band{Model: "extreme", Billing: "extreme", Pricing: "extreme", Threshold: -1, Input: math.MinInt64}},
		{"extreme", "", "", math.MaxInt64 - 1, nil, band{Model: "extreme", Billing: "extreme", Pricing: "extreme", Threshold: -1, Input: math.MaxInt64 - 1}},
		{"extreme", "", "", math.MaxInt64, nil, band{Model: "extreme", Billing: "extreme", Pricing: "extreme", Threshold: math.MaxInt64 - 1, Input: math.MaxInt64}},
		{longA, "", "", 150, nil, band{Model: longA, Billing: longA, Pricing: longA, Threshold: 100, Input: 150}},
		{longB, "", "", 150, nil, band{Model: longB, Billing: longB, Pricing: longB, Threshold: -1, Input: 150}},
		{"BILLED", "", "", 201, nil, band{Model: "BILLED", Billing: "BILLED", Pricing: "BILLED", Threshold: -1, Input: 201}},
	}
	want := make([]band, 0, len(cases))
	for i, c := range cases {
		c.want.Timestamp = int64(i + 1)
		insertPricingEvent(t, db, c.want.Timestamp, c.display, c.requested, c.billing, "", c.raw, c.normalized)
		want = append(want, c.want)
	}
	selectBands := ` select timestamp_ms,analytics_model_value,billing_model_value,pricing_model_value,context_threshold_tokens_value,coalesce(normalized_total_input_tokens,input_tokens) from banded_usage_events order by timestamp_ms`
	for name, query := range map[string]string{"scalar": frozenScalarPricingCTE + selectBands, "interval": pricingBandedUsageEventsCTEWithBaseFilter("timestamp_ms >= ? and timestamp_ms < ?", pricingTimelineFields) + selectBands} {
		t.Run(name, func(t *testing.T) {
			var args []any
			if name == "interval" {
				args = []any{int64(1), int64(len(cases) + 1)}
			}
			rows, err := db.QueryContext(context.Background(), query, args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			got := []band{}
			for rows.Next() {
				var b band
				if err := rows.Scan(&b.Timestamp, &b.Model, &b.Billing, &b.Pricing, &b.Threshold, &b.Input); err != nil {
					t.Fatal(err)
				}
				got = append(got, b)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("bands or cardinality changed\ngot=%+v\nwant=%+v", got, want)
			}
		})
	}
}

func TestPricingModelBandsNormalizedLimitAndSufficientStatistics(t *testing.T) {
	db := pricingTestDB(t)
	ctx := context.Background()
	repo := New(db).(*repository)
	book := map[string]model.ModelPrice{
		"billed": {Prompt: 1, Completion: 3, Cache: 0, ContextTiers: []model.ModelPriceContextTier{
			{ThresholdTokens: 100, Prompt: 0, PromptConfigured: true, Completion: 9, CompletionConfigured: true},
			{ThresholdTokens: 200, Prompt: 2, PromptConfigured: true},
		}, ServiceTiers: []model.ModelPriceServiceTier{{Mode: "fast", ServiceTier: "priority", Prompt: 7, PromptConfigured: true}}},
		"clamp": {Prompt: 1},
	}
	if err := modelprice.New(db).ReplaceAll(ctx, book); err != nil {
		t.Fatal(err)
	}
	for i, input := range []int64{100, 101, 200, 201, 272000, 272001} {
		insertPricingEvent(t, db, int64(i+1), "chosen(high)", "", "billed", "priority", 10, input)
		pricingFixtureExec(t, db, `update usage_events set output_tokens=3,reasoning_tokens=2,cached_tokens=40,cache_tokens=50,cache_read_tokens=10,cache_creation_tokens=5,total_tokens=? where timestamp_ms=?`, input+5, int64(i+1))
	}
	insertPricingEvent(t, db, 7, "other", "", "", "", 2, nil)
	insertPricingEvent(t, db, 8, "chosen(high)", "", "other-billing", "flex", 12, int64(20))
	filter := AnalyticsFilter{FromMS: 1, ToMS: 9, IncludeFailed: true}
	all, err := repo.ModelStatsWithFilter(ctx, filter, 0)
	if err != nil {
		t.Fatal(err)
	}
	old := readPricingModelStats(t, db, frozenScalarPricingCTE+frozenModelPricingAggregate, int64(1), int64(9))
	if !reflect.DeepEqual(sortedAnalyticsJSON(all), sortedAnalyticsJSON(old)) {
		t.Fatalf("full grain/metrics differ\nnew=%+v\nold=%+v", all, old)
	}
	limited, err := repo.ModelStatsWithFilter(ctx, filter, 1)
	if err != nil {
		t.Fatal(err)
	}
	var input, calls int64
	for _, s := range limited {
		if s.Model != "chosen" {
			t.Fatalf("wrong top model: %+v", limited)
		}
		input += s.InputTokens
		calls += s.Calls
	}
	if input != 544623 || calls != 7 || len(limited) != 4 {
		t.Fatalf("limited must retain every band and normalized input: input=%d calls=%d rows=%+v", input, calls, limited)
	}
	between, err := repo.TopModelsBetween(ctx, 1, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedAnalyticsJSON(between), sortedAnalyticsJSON(limited)) {
		t.Fatal("Between top bands differ")
	}
	between, err = repo.ModelStatsBetween(ctx, 1, 9)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedAnalyticsJSON(between), sortedAnalyticsJSON(all)) {
		t.Fatal("Between full bands differ")
	}
	defaults, err := repo.TopModelsBetween(ctx, 1, 9, 0)
	if err != nil || len(defaults) != len(all) {
		t.Fatal("default top limit lost", defaults, err)
	}
	for _, s := range all {
		if s.ContextThresholdTokens == 200 && s.BillingModel == "billed" {
			if s.LongInputTokens != 272001 || s.LongOutputTokens != 3 || s.LongCachedTokens != 35 || s.LongCacheReadTokens != 10 || s.LongCacheCreationTokens != 5 {
				t.Fatalf("long-context sufficient statistics changed: %+v", s)
			}
		}
	}
	// Cost is applied only after aggregation. Two events in one group deliberately
	// demonstrate why event-level cost cannot replace these sufficient statistics.
	insertPricingEvent(t, db, 20, "clamp", "", "", "", 0, nil)
	insertPricingEvent(t, db, 21, "clamp", "", "", "", 100, nil)
	pricingFixtureExec(t, db, `update usage_events set cached_tokens=100 where timestamp_ms=20`)
	clamped, err := repo.ModelStatsBetween(ctx, 20, 22)
	if err != nil || len(clamped) != 1 {
		t.Fatal(clamped, err)
	}
	s := clamped[0]
	if s.InputTokens != 100 || s.CachedTokens != 100 || pricingModelStatCost(s, book) != 0 {
		t.Fatalf("group-before-cost clamping changed: %+v", s)
	}
	// Context 100 has explicitly configured zero prompt and completion=9; the
	// higher context overrides base, not the preceding tier, and beats service.
	for _, s := range limited {
		if s.BillingModel != "billed" {
			continue
		}
		var expected float64
		switch s.ContextThresholdTokens {
		case -1:
			expected = float64(100-50)*7/1e6 + 3*3.0/1e6 + 10*.7/1e6 + 5*7.0/1e6
		case 100:
			expected = 6 * 9.0 / 1e6
		case 200:
			expected = float64(201+272000+272001-150)*2/1e6 + 9*3.0/1e6 + 30*.2/1e6 + 15*2.0/1e6
		}
		if got := pricingModelStatCost(s, book); math.Abs(got-expected) > 1e-12 {
			t.Fatalf("band %d cost=%v want=%v stat=%+v", s.ContextThresholdTokens, got, expected, s)
		}
	}
}

func pricingModelStatCost(s ModelStat, book map[string]model.ModelPrice) float64 {
	return pricing.CostForModelCandidatesWithServiceTier([]string{s.BillingModel, s.Model}, s.ServiceTier, pricing.ModelTokens{
		PricingModel: s.PricingModel, ContextThresholdTokens: s.ContextThresholdTokens, InputTokens: s.InputTokens, OutputTokens: s.OutputTokens, CachedTokens: s.CachedTokens, CacheReadTokens: s.CacheReadTokens, CacheCreationTokens: s.CacheCreationTokens, LongInputTokens: s.LongInputTokens, LongOutputTokens: s.LongOutputTokens, LongCachedTokens: s.LongCachedTokens, LongCacheReadTokens: s.LongCacheReadTokens, LongCacheCreationTokens: s.LongCacheCreationTokens,
	}, book)
}

func TestPricingProjectionAndCalendarBindingContract(t *testing.T) {
	ctx := context.Background()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	filter := AnalyticsFilter{FromMS: from, ToMS: from + 24*3600000, SearchQuery: "bound'%_", SearchAPIKeyHash: "hash", Models: []string{"m(high)", strings.Repeat("界", 300)}, Providers: []string{"p"}, Accounts: []string{"a"}, CredentialIDs: []string{"c"}, AuthFiles: []string{"f"}, AuthIndices: []string{"i"}, APIKeyHashes: []string{"k"}, SourceHashes: []string{"s"}, ProjectIDs: []string{"project"}, RequestTypes: []string{"r"}, IncludeFailed: true, MinLatencyMS: 17, CacheStatus: "read", HeaderErrorKinds: []string{"kind"}, HeaderErrorCodes: []string{"code"}, HeaderQuotaPlans: []string{"plan"}, HeaderTraceIDs: []string{"trace"}}
	where, args := analyticsWhere(filter)
	for fields := pricingModelFields; fields <= pricingAccountWindowFields; fields++ {
		if fields == pricingAccountWindowFields {
			continue
		}
		query := pricingBandedUsageEventsCTEWithBaseFilter(where, fields)
		if strings.Contains(query, ".*") || strings.Contains(query, "select *") || strings.Contains(query, "max(tier.threshold_tokens)") {
			t.Fatal("wide/dependent pricing query", query)
		}
		baseEnd := strings.Index(query, "), pricing_resolved_events")
		if !strings.Contains(query[:baseEnd], where) {
			t.Fatal("full source predicate missing")
		}
		for _, forbidden := range []string{"raw_json", "fail_body", "response_metadata_json", "fail_summary", "header_trace_id", "requested_model_value", "normalized_input_tokens_value"} {
			final := query[strings.Index(query, ", banded_usage_events as ("):]
			// The classifier input is required only in ON, never in the final projection.
			projection := final[:strings.Index(final, "from pricing_resolved_events")]
			if strings.Contains(projection, forbidden) {
				t.Fatalf("projection %d leaks %s", fields, forbidden)
			}
		}
		if fields == pricingHeatmapFields {
			if strings.Contains(query[baseEnd:], "latency_ms") || strings.Contains(query[baseEnd:], "reasoning_tokens") {
				t.Fatal("heatmap projection widened by filter")
			}
		}
		compiled, bindings, err := mysql.CompileSQL(query+" select count(*) from banded_usage_events", args...)
		if err != nil || !reflect.DeepEqual(bindings, args) {
			t.Fatal("projection compilation/bindings", fields, err)
		}
		if !strings.Contains(compiled, "lead(") || !strings.Contains(compiled, "over(") || !strings.Contains(compiled, "longtext") {
			t.Fatal("missing interval/window/full-width list compilation", compiled)
		}
	}
	for _, mode := range []string{"timeline", "api", "credential", "heatmap"} {
		plan, ok, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, "hour", loc, mode == "heatmap")
		if err != nil || !ok || len(plan.args) == 0 {
			t.Fatal(ok, err)
		}
		query, bindings := groupedAnalyticsSQL(filter, plan, mode)
		want := append(append([]any{}, args...), plan.args...)
		_, compiledArgs, err := mysql.CompileSQL(query, bindings...)
		if err != nil || !reflect.DeepEqual(bindings, want) || !reflect.DeepEqual(compiledArgs, want) {
			t.Fatalf("%s binding order: %v", mode, err)
		}
	}
	query, bindings, ok := credentialTimelineHourlySQL(filter, "day", loc)
	if !ok {
		t.Fatal("missing credential map")
	}
	_, mapArgs, ok := credentialBucketMapSQL(filter.FromMS, filter.ToMS, "day", loc)
	if !ok {
		t.Fatal("missing hourly map")
	}
	want := append(append([]any{}, args...), mapArgs...)
	_, compiledArgs, err := mysql.CompileSQL(query, bindings...)
	if err != nil || !reflect.DeepEqual(bindings, want) || !reflect.DeepEqual(compiledArgs, want) {
		t.Fatal("hourly source filters must precede calendar map bindings", err)
	}
	top := topModelPricingBandsSQL(where)
	countEnd := strings.Index(top, "), pricing_base_events")
	if countEnd < 0 || strings.Count(top, "from banded_usage_events") != 1 || !strings.Contains(top[:countEnd], "count(*) as model_calls") || strings.Contains(top[:countEnd], "sum(") || strings.Contains(top[:countEnd], "model_prices") || !strings.Contains(top, "usage_events join top_models") || strings.Count(top, strings.TrimPrefix(where, "where ")) != 2 {
		t.Fatal("top models must count before pricing with two fully filtered raw scans")
	}
	want = append(append(append([]any{}, args...), 3), args...)
	bindings = topModelPricingArgs(args, 3)
	_, compiledArgs, err = mysql.CompileSQL(top, bindings...)
	if err != nil || !reflect.DeepEqual(bindings, want) || !reflect.DeepEqual(compiledArgs, want) {
		t.Fatal("top-model count/limit/pricing binding order changed", err)
	}
}

func TestPricingAccountWindowsScopeBeforeClassification(t *testing.T) {
	db := pricingTestDB(t)
	ctx := context.Background()
	repo := New(db)
	if err := modelprice.New(db).ReplaceAll(ctx, map[string]model.ModelPrice{
		"billed": {Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 100, Prompt: 2, PromptConfigured: true}, {ThresholdTokens: 200, Prompt: 3, PromptConfigured: true}}},
	}); err != nil {
		t.Fatal(err)
	}
	for i, input := range []int64{100, 101, 201} {
		insertPricingEvent(t, db, int64(i+1)*100, "display", "", "billed", "priority", 1, input)
	}
	insertPricingEvent(t, db, 250, "unrelated", "", "billed", "", 999, nil)
	pricingFixtureExec(t, db, `update usage_events set auth_file_snapshot='file',auth_index='index',source='file',account_snapshot='account',auth_provider_snapshot='provider' where timestamp_ms != 250`)
	first := AccountWindowUsageQuery{RequestIndex: 7, FromMS: 100, ToMS: 300, AuthFileSnapshot: "file", AuthIndex: "index", Source: "file", AccountSnapshot: "account", AuthProviderSnapshot: "provider"}
	second := first
	second.RequestIndex, second.FromMS, second.ToMS = 9, 200, 400
	empty := first
	empty.RequestIndex, empty.FromMS, empty.ToMS = 11, 400, 500
	got, err := repo.AccountWindowModelStats(ctx, []AccountWindowUsageQuery{first, second, empty})
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		index                    int
		band, calls, input, last int64
	}
	actual := make([]row, 0, len(got))
	for _, stat := range got {
		if stat.Model != "display" || stat.BillingModel != "billed" || stat.PricingModel != "billed" || stat.ServiceTier != "priority" {
			t.Fatal("lost pricing grain", stat)
		}
		actual = append(actual, row{stat.RequestIndex, stat.ContextThresholdTokens, stat.Calls, stat.InputTokens, stat.LastSeenMS})
	}
	want := []row{{7, 100, 1, 101, 200}, {7, -1, 1, 100, 100}, {9, 200, 1, 201, 300}, {9, 100, 1, 101, 200}}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("window overlap/identity/boundaries/order changed: got=%v want=%v", actual, want)
	}
}

func TestPricingRepresentativeSnapshotsAndLegacyPopulations(t *testing.T) {
	db := pricingTestDB(t)
	repo := New(db).(*repository)
	ctx := context.Background()
	for i := int64(1); i <= 3; i++ {
		insertPricingEvent(t, db, i, "model", "", "", "", 10, nil)
	}
	pricingFixtureExec(t, db, `update usage_events set auth_file_snapshot='file',auth_index='index',source_hash='hash',latency_ms=9`)
	pricingFixtureExec(t, db, `update usage_events set source='',account_snapshot='',auth_label_snapshot='',auth_account_id_snapshot='',latency_ms=-3 where timestamp_ms=1`)
	pricingFixtureExec(t, db, `update usage_events set source='z-source',account_snapshot='z-account',auth_label_snapshot='z-label',auth_account_id_snapshot='z-id',failed=1 where timestamp_ms=2`)
	pricingFixtureExec(t, db, `update usage_events set source='a-source',account_snapshot='a-account',auth_label_snapshot='a-label',auth_account_id_snapshot='a-id',latency_ms=0,failed=2 where timestamp_ms=3`)
	filter := AnalyticsFilter{FromMS: 1, ToMS: 4, IncludeFailed: true}
	stats, err := repo.CredentialModelStatsWithFilter(ctx, filter)
	if err != nil || len(stats) != 1 {
		t.Fatal(stats, err)
	}
	if stats[0].Source != "z-source" || stats[0].AccountSnapshot != "z-account" || stats[0].AuthAccountIDSnapshot != "z-id" || stats[0].Calls != 3 || stats[0].FailureCalls != 1 || stats[0].LatencySamples != 2 || stats[0].AvgLatencyMS.Float64 != 3 {
		t.Fatal("MAX provenance/failed=1/nonzero latency population changed", stats)
	}
	points, err := repo.credentialTimelineRawWithFilter(ctx, filter, "hour", time.UTC)
	if err != nil || len(points) != 1 {
		t.Fatal(points, err)
	}
	if points[0].Source != "" || points[0].AccountSnapshot != "" || points[0].AuthAccountIDSnapshot != "" || points[0].Failure != 2 || points[0].LatencySamples != 1 || points[0].AvgLatencyMS.Float64 != 9 {
		t.Fatal("first-empty provenance/nonzero failure/positive latency population changed", points)
	}
	plan, available, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, "hour", time.UTC, false)
	if err != nil || !available {
		t.Fatal(available, err)
	}
	query, args := groupedAnalyticsSQL(filter, plan, "credential")
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	grouped, err := readGroupedCredentialTimeline(rows)
	if err != nil || !reflect.DeepEqual(grouped, points) {
		t.Fatal("grouped representative snapshots changed", grouped, points, err)
	}
}

func TestPricingEmptyBookAndNoMatches(t *testing.T) {
	db := pricingTestDB(t)
	repo := New(db)
	ctx := context.Background()
	insertPricingEvent(t, db, 1, "gpt-5.6-sol", "", "", "", 272001, nil)
	stats, err := repo.ModelStatsBetween(ctx, 0, 2)
	if err != nil || len(stats) != 1 {
		t.Fatal(stats, err)
	}
	if stats[0].ContextThresholdTokens != -1 || stats[0].LongInputTokens != 272001 || pricingModelStatCost(stats[0], nil) <= 0 {
		t.Fatal("unpriced event or built-in fallback lost", stats)
	}
	empty, err := repo.ModelStatsWithFilter(ctx, AnalyticsFilter{FromMS: 2, ToMS: 3}, 1)
	if err != nil || len(empty) != 0 {
		t.Fatal(empty, err)
	}
}

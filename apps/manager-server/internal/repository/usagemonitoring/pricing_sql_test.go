package usagemonitoring

import (
	"context"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

// Frozen dependent-MAX classifier: do not build this reference with the shared
// interval helper or production banding builders.
const frozenProjectedClassifier = `with base_events as (%s), priced_events as (
 select base_events.*,
 coalesce(nullif(resolved_model, ''), analytics_model) as billing_model_value,
 case when b.model is not null then coalesce(nullif(resolved_model, ''), analytics_model)
 when a.model is not null then analytics_model when d.model is not null then base_events.model
 else coalesce(nullif(resolved_model, ''), analytics_model) end as pricing_model_value
 from base_events
 left join model_prices b on b.model = coalesce(nullif(resolved_model, ''), analytics_model)
 left join model_prices a on a.model = analytics_model
 left join model_prices d on d.model = base_events.model
), banded_events as (
 select priced_events.*, coalesce((select max(t.threshold_tokens) from model_price_context_tiers t
 where t.model = pricing_model_value and normalized_total_input_tokens > t.threshold_tokens), -1) as context_threshold_tokens_value
 from priced_events
)`

func TestMonitoringIntervalClassifiersAgainstFrozenMAX(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "bands.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := sqlwrite.ExecContext(ctx, db, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	long := strings.Repeat("模型", 200)
	for _, name := range []string{"bill", "analytics", "display(high)", "no-tiers", long + "a", long + "b"} {
		exec(`insert into model_prices(model, prompt_per_1m, completion_per_1m, cache_per_1m, updated_at_ms) values(?, 0, 0, 0, 0)`, name)
	}
	for _, tier := range []struct {
		model string
		value int64
	}{
		{"bill", 100}, {"bill", 200}, {"bill", math.MaxInt64 - 1}, {"bill", math.MaxInt64},
		{"analytics", 300}, {"display(high)", 10}, {long + "a", 12}, {long + "b", 15},
	} {
		exec(`insert into model_price_context_tiers(model, threshold_tokens) values(?, ?)`, tier.model, tier.value)
	}
	type band struct {
		at               int64
		billing, pricing string
		threshold, input int64
	}
	cases := []struct {
		requested, billing, pricing string
		input, threshold            int64
	}{
		{"analytics(high)", "bill", "bill", 100, -1},
		{"analytics(high)", "bill", "bill", 101, 100},
		{"analytics(high)", "bill", "bill", 200, 100},
		{"analytics(high)", "bill", "bill", 201, 200},
		{"analytics(high)", "absent", "analytics", 301, 300},
		{"display(high)", "absent", "display(high)", 11, 10},
		{"missing", "unknown", "unknown", 999, -1},
		{"no-tiers", "no-tiers", "no-tiers", 999, -1},
		{"analytics(high)", "bill", "bill", math.MaxInt64, math.MaxInt64 - 1},
		{"missing", long + "a", long + "a", 14, 12},
		{"missing", long + "b", long + "b", 14, -1},
	}
	want := make([]band, 0, len(cases))
	for i, item := range cases {
		at := int64(i + 1)
		exec(`insert into usage_events(event_hash,timestamp_ms,timestamp,model,requested_model,resolved_model,input_tokens,normalized_total_input_tokens,created_at_ms)
		 values(?,?,'','ignored',?,?,9,?,0)`, item.requested+item.billing+string(rune(i)), at, item.requested, item.billing, item.input)
		want = append(want, band{at, item.billing, item.pricing, item.threshold, item.input})
	}
	// Fixed source expressions keep fallback/normalization independent of the
	// new projection-profile builder; raw input intentionally differs from input.
	source := `select timestamp_ms, requested_model as model, cpamp_analytics_model(requested_model) as analytics_model,
	 resolved_model, service_tier, failed, normalized_total_input_tokens, output_tokens, cached_tokens, cache_tokens,
	 cache_read_tokens, cache_creation_tokens, total_tokens from usage_events where timestamp_ms >= ? and timestamp_ms < ?`
	selectBands := ` select timestamp_ms, billing_model_value, pricing_model_value, context_threshold_tokens_value, normalized_total_input_tokens from banded_events order by timestamp_ms`
	queries := []string{
		strings.Replace(frozenProjectedClassifier, "%s", source, 1) + selectBands,
		monitoringBandedProjectedEventsCTE(source, "timestamp_ms") + selectBands,
		monitoringBandedEventsCTE("usage_events", "e.timestamp_ms >= ? and e.timestamp_ms < ?") + strings.ReplaceAll(selectBands, "normalized_total_input_tokens", "normalized_input_tokens_value"),
	}
	for _, query := range queries {
		rows, err := db.QueryContext(ctx, query, int64(1), int64(len(cases)+1))
		if err != nil {
			t.Fatal(err)
		}
		var got []band
		for rows.Next() {
			var b band
			if err := rows.Scan(&b.at, &b.billing, &b.pricing, &b.threshold, &b.input); err != nil {
				t.Fatal(err)
			}
			got = append(got, b)
		}
		err = rows.Err()
		rows.Close()
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("bands = %#v, want %#v; err=%v", got, want, err)
		}
	}
}

func TestMonitoringPricingArmsAndMaintenanceCompile(t *testing.T) {
	filter := AnalyticsFilter{FromMS: 123, ToMS: 456, IncludeFailed: true, SearchQuery: "needle", Models: []string{"model"}}
	projected, raw := monitoringPricingSourceSelects([]string{"reasoning_tokens"})
	arms := filteredEventSourceArms(filter, 19, projected, raw, eventSourceOptions{DirectProjectedSearch: true, AfterID: 11, UseAfter: true})
	query, args := monitoringPricingArmsQuery(arms, []string{"reasoning_tokens"}, ` select analytics_model, count(*) from banded_events group by analytics_model`, nil, []usageprojection.AggregateColumn{{Name: "model"}, {Name: "calls", Merge: "sum"}})
	compiled, bound, err := mysql.CompileSQL(query, args...)
	if err != nil || !reflect.DeepEqual(bound, args) {
		t.Fatalf("compile bindings=%v want=%v err=%v", bound, args, err)
	}
	for _, forbidden := range []string{"select max(tier.threshold_tokens)", "base_events.*", "priced_events.*", "raw_json", usageprojection.SearchIndexTable} {
		if strings.Contains(query, forbidden) || strings.Contains(compiled, forbidden) {
			t.Fatalf("unexpected %s", forbidden)
		}
	}
	if strings.Count(query, "group by analytics_model") != 2 || strings.Count(query, "union all") != 1 {
		t.Fatalf("source arms did not aggregate before union: %s", query)
	}
	for _, source := range []string{"usage_events", usageprojection.RetainedEdgeSource(123, 456)} {
		for _, statement := range []string{
			monitoringAccountBatchSQL("usage_monitoring_account_daily_rollups_v1", dayMS, source),
			monitoringAPIKeyBatchSQL("usage_monitoring_api_key_hourly_rollups_v1", hourMS, source),
		} {
			args := []any{int64(11), int64(19), "revision", int64(789)}
			compiled, bound, err := mysql.CompileSQL(statement, args...)
			if err != nil || !reflect.DeepEqual(bound, args) {
				t.Fatalf("maintenance compile err=%v bound=%v", err, bound)
			}
			if !strings.HasPrefix(compiled, "insert into usage_monitoring_") || !strings.Contains(compiled, "with pricing_context_intervals as") || !strings.Contains(compiled, "on duplicate key update") {
				t.Fatalf("invalid CTE INSERT relocation: %s", compiled)
			}
		}
	}
}

func TestProjectedFilterPredicatesAndSearchModes(t *testing.T) {
	for _, models := range [][]string{{"model(high)"}, {"model(high)", "other"}} {
		filter := AnalyticsFilter{FromMS: 123, ToMS: 456, IncludeFailed: true, Models: models, SearchQuery: "Needle", SearchAPIKeyHash: "KEY"}
		for _, direct := range []bool{false, true} {
			conditions, args := eventFilterConditionsWithSearch(filter, "p.", true, direct)
			query := "select p.event_id from usage_monitoring_event_projection_v1 p where " + strings.Join(conditions, " and ")
			if strings.Contains(query, "coalesce(p.analytics_model") {
				t.Fatal(query)
			}
			want := "p.analytics_model = ?"
			if len(models) > 1 {
				want = "p.analytics_model in (select value from json_each(?))"
			}
			if !strings.Contains(query, want) {
				t.Fatal(query)
			}
			if strings.Contains(query, usageprojection.SearchIndexTable) == direct {
				t.Fatal(query)
			}
			if !strings.Contains(query, " or lower(coalesce(p.api_key_hash, '')) = ?") || args[0] != int64(123) || args[1] != int64(456) {
				t.Fatal(query, args)
			}
			compiled, _, err := mysql.CompileSQL(query, args...)
			if err != nil {
				t.Fatal(err)
			}
			if len(models) > 1 && (!strings.Contains(compiled, "json_table") || !strings.Contains(compiled, "longtext")) {
				t.Fatal(compiled)
			}
		}
	}
	for _, query := range []string{"ab", "a\nb", "雪", "back\\slash"} {
		conditions, _ := eventFilterConditionsWithSearch(AnalyticsFilter{SearchQuery: query}, "p.", true, true)
		if strings.Contains(strings.Join(conditions, " "), usageprojection.SearchIndexTable) {
			t.Fatal(conditions)
		}
	}
	for _, query := range []string{"%wild", "under_score", "a\x1fb"} {
		if SupportsEventProjectionFilter(AnalyticsFilter{SearchQuery: query}) {
			t.Fatalf("wildcard/separator lost raw fallback: %q", query)
		}
	}
}

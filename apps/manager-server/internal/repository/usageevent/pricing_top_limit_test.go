package usageevent

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

// Frozen count-before-sum shape from 87d975a2, using the independent scalar
// classifier above. The window is applied to a reusable event relation just as
// in the baseline, NOT to production's two-scan/count-only implementation.
// Input is normalized, as in baseline TopModelsBetween; the limited filtered
// reader's intentional normalized-input correction is asserted separately.
const frozenTopModelPricingSQL = frozenScalarPricingCTE + `, filtered as (
 select * from banded_usage_events where timestamp_ms >= ? and timestamp_ms < ?
), top_models as (
 select analytics_model_value as model, count(*) as model_calls
 from filtered group by analytics_model_value order by model_calls desc limit ?
)
select e.analytics_model_value, e.billing_model_value, e.pricing_model_value,
 e.context_threshold_tokens_value, coalesce(e.service_tier,''), count(*) as calls,
 sum(case when e.failed = 0 then 1 else 0 end),
 coalesce(sum(coalesce(e.normalized_total_input_tokens,e.input_tokens)),0),
 coalesce(sum(e.output_tokens),0), coalesce(sum(e.reasoning_tokens),0),
 coalesce(sum(max(max(e.cached_tokens,e.cache_tokens)-max(e.cache_read_tokens,0)-max(e.cache_creation_tokens,0),0)),0),
 coalesce(sum(e.cache_read_tokens),0), coalesce(sum(e.cache_creation_tokens),0),
 coalesce(sum(case when coalesce(e.normalized_total_input_tokens,e.input_tokens)>272000 then coalesce(e.normalized_total_input_tokens,e.input_tokens) else 0 end),0),
 coalesce(sum(case when coalesce(e.normalized_total_input_tokens,e.input_tokens)>272000 then e.output_tokens else 0 end),0),
 coalesce(sum(case when coalesce(e.normalized_total_input_tokens,e.input_tokens)>272000 then max(max(e.cached_tokens,e.cache_tokens)-max(e.cache_read_tokens,0)-max(e.cache_creation_tokens,0),0) else 0 end),0),
 coalesce(sum(case when coalesce(e.normalized_total_input_tokens,e.input_tokens)>272000 then e.cache_read_tokens else 0 end),0),
 coalesce(sum(case when coalesce(e.normalized_total_input_tokens,e.input_tokens)>272000 then e.cache_creation_tokens else 0 end),0),
 coalesce(sum(e.total_tokens),0)
from filtered e join top_models t on t.model=e.analytics_model_value
 group by e.analytics_model_value,e.billing_model_value,e.pricing_model_value,e.context_threshold_tokens_value,coalesce(e.service_tier,'')
 order by max(t.model_calls) desc,e.analytics_model_value,calls desc`

func TestPricingLimitedModelsExcludeOverflowBeforeSums(t *testing.T) {
	db := pricingTestDB(t)
	ctx := context.Background()
	repo := New(db)
	for i := int64(1); i <= 3; i++ {
		insertPricingEvent(t, db, i, "A", "", "", "", 1, nil)
	}
	insertPricingEvent(t, db, 4, "B", "", "", "", math.MaxInt64, nil)
	insertPricingEvent(t, db, 5, "B", "", "", "", 1, nil)
	want := readPricingModelStats(t, db, frozenTopModelPricingSQL, int64(1), int64(6), 1)
	if len(want) != 1 || want[0].Model != "A" || want[0].Calls != 3 || want[0].InputTokens != 3 {
		t.Fatalf("frozen reference does not reproduce baseline: %+v", want)
	}
	readers := map[string]func(int) ([]ModelStat, error){
		"filtered": func(limit int) ([]ModelStat, error) {
			return repo.ModelStatsWithFilter(ctx, AnalyticsFilter{FromMS: 1, ToMS: 6, IncludeFailed: true}, limit)
		},
		"between": func(limit int) ([]ModelStat, error) { return repo.TopModelsBetween(ctx, 1, 6, limit) },
	}
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			got, err := read(1)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("excluded B must not overflow A: got=%+v want=%+v err=%v", got, want, err)
			}
			// Once B is returned, integer overflow remains terminal; do not fix
			// excluded groups by coercing SUM to floating point or dropping errors.
			if _, err := read(2); err == nil || !mysql.IsMySQL(db) && !strings.Contains(err.Error(), "integer overflow") {
				t.Fatalf("returned B must retain its integer overflow: %v", err)
			}
		})
	}
	args := []any{int64(1), int64(6), 1, int64(1), int64(6)}
	compiled, bindings, err := mysql.CompileSQL(topModelsSQL, args...)
	if err != nil || !reflect.DeepEqual(bindings, args) {
		t.Fatal("native count/limit/pricing bindings", bindings, err)
	}
	if mysql.IsMySQL(db) {
		got := readPricingModelStats(t, db, mysql.Native(compiled), bindings...)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("native excluded-model result: got=%+v want=%+v", got, want)
		}
	}
}

func TestPricingLimitedModelsTiesDefaultAndFilters(t *testing.T) {
	db := pricingTestDB(t)
	repo := New(db)
	ctx := context.Background()
	var timestamp int64
	// Tie both inside the selected population and at the cutoff; the SQL keeps
	// baseline's count-only cutoff (no invented deterministic secondary key).
	for i, calls := range []int{7, 6, 5, 4, 3, 3, 2} {
		name := string(rune('A' + i))
		for j := 0; j < calls; j++ {
			timestamp++
			insertPricingEvent(t, db, timestamp, name, name+"(high)", "billing", "priority", 1, nil)
		}
	}
	for _, limit := range []int{1, 4, 5, 6, 7} {
		want := readPricingModelStats(t, db, frozenTopModelPricingSQL, int64(1), timestamp+1, limit)
		filtered, err := repo.ModelStatsWithFilter(ctx, AnalyticsFilter{FromMS: 1, ToMS: timestamp + 1, IncludeFailed: true}, limit)
		if err != nil {
			t.Fatal(err)
		}
		between, err := repo.TopModelsBetween(ctx, 1, timestamp+1, limit)
		if err != nil {
			t.Fatal(err)
		}
		// Tied cutoff membership is unspecified on MySQL. Check the exact frozen
		// result whenever membership is unique (and always on SQLite), then the
		// tied population's count/rank rather than pinning an engine's tie choice.
		for _, got := range [][]ModelStat{filtered, between} {
			if limit != 5 || !mysql.IsMySQL(db) {
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("limit %d: got=%+v want=%+v", limit, got, want)
				}
			} else if len(got) != 5 || got[4].Calls != 3 || (got[4].Model != "E" && got[4].Model != "F") || !reflect.DeepEqual(got[:4], want[:4]) {
				t.Fatalf("tied cutoff changed: %+v", got)
			}
		}
	}
	for _, limit := range []int{0, -1} {
		got, err := repo.TopModelsBetween(ctx, 1, timestamp+1, limit)
		if err != nil || len(got) != 5 || got[4].Calls != 3 {
			t.Fatalf("default limit must be five models: %+v, %v", got, err)
		}
	}
	// The count and pricing scans must BOTH apply all filter values. Without
	// the count predicate A wins; without the pricing predicate both G rows sum.
	pricingFixtureExec(t, db, `update usage_events set auth_file_snapshot='included' where timestamp_ms=?`, timestamp)
	got, err := repo.ModelStatsWithFilter(ctx, AnalyticsFilter{FromMS: 1, ToMS: timestamp + 1, IncludeFailed: true, Models: []string{"G(high)"}, AuthFiles: []string{"included"}}, 1)
	if err != nil || len(got) != 1 || got[0].Model != "G" || got[0].Calls != 1 {
		t.Fatalf("filtered counts/pricing disagree: %+v %v", got, err)
	}
}

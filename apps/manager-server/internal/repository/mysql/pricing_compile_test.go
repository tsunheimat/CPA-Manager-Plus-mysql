package mysql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usagepricingsql"
)

// This tests the shared fragment/compiler contract. Full generated reader-plan
// tests belong beside usageevent's private builders, not a second exported SQL API.
func TestPricingIntervalsCompileAndExecuteBindings(t *testing.T) {
	query := `with ` + usagepricingsql.ContextIntervalsCTE("pricing_intervals") + `,
	filtered as (
		select cpamp_analytics_model(?) as pricing_model_value,
			max(coalesce(normalized_total_input_tokens, input_tokens, 0), 0) as normalized_input,
			timestamp_ms
		from usage_events where timestamp_ms >= ? and timestamp_ms < ?
		and model in (select value from json_each(?))
	), bucket_map as (select ? as from_ms, ? as to_ms, ? as bucket_ms)
	select coalesce(tier.threshold_tokens, -1), f.timestamp_ms / 3600000, b.bucket_ms
	from filtered f` + usagepricingsql.ContextBandJoin("pricing_intervals", "tier", "f.pricing_model_value", "f.normalized_input") + `
	join bucket_map b on f.timestamp_ms >= b.from_ms and f.timestamp_ms < b.to_ms
	order by b.bucket_ms limit ?`
	args := []any{"模型(high)", int64(1001), int64(86401001), `["模型","long-model"]`, int64(1001), int64(3600000), int64(0), int64(5)}
	compiled, bound, err := CompileSQL(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	flat := strings.ToLower(compiled)
	for _, want := range []string{"lead(", "over( partition by", "greatest(", " div ", "json_table(", "longtext", "next_threshold_tokens", "limit ?"} {
		if !strings.Contains(flat, want) {
			t.Fatalf("missing %q in compiled pricing fragment: %s", want, compiled)
		}
	}
	for _, forbidden := range []string{"cpamp_analytics_model", "select max(", "`window`", "raw_json", "fail_body", "模型"} {
		if strings.Contains(flat, forbidden) {
			t.Fatalf("unexpected %q in compiled SQL: %s", forbidden, compiled)
		}
	}
	if len(bound) < len(args) || !reflect.DeepEqual(bound[len(bound)-7:], args[1:]) {
		t.Fatalf("filter/calendar/limit bindings moved: %v", bound)
	}
	for _, value := range bound[:len(bound)-7] {
		if value != args[0] {
			t.Fatalf("analytics-model argument expansion: %v", bound)
		}
	}
	for _, mode := range []string{"shared", "prepared", "compiled-native"} {
		t.Run(mode, func(t *testing.T) {
			db, owner, backend := admissionDB(t)
			var got int
			switch mode {
			case "shared":
				err = db.QueryRowContext(context.Background(), query, args...).Scan(&got)
			case "prepared":
				stmt, prepareErr := db.PrepareContext(context.Background(), query)
				if prepareErr != nil {
					t.Fatal(prepareErr)
				}
				defer stmt.Close()
				err = stmt.QueryRowContext(context.Background(), args...).Scan(&got)
			case "compiled-native":
				err = db.QueryRowContext(context.Background(), Native(compiled), bound...).Scan(&got)
			}
			if err != nil || got != 1 {
				t.Fatalf("execution: value=%d err=%v", got, err)
			}
			if len(backend.queries) != 1 || backend.queries[0] != compiled {
				t.Fatalf("executed SQL differs: %v", backend.queries)
			}
			actual := make([]any, len(backend.arguments[0]))
			for i, value := range backend.arguments[0] {
				actual[i] = value.Value
			}
			if !reflect.DeepEqual(actual, bound) {
				t.Fatalf("executed bindings=%v want=%v", actual, bound)
			}
			assertReleased(t, db, owner)
		})
	}
}

func TestPricingIntervalsCTEInsertAndUnionBindingOrder(t *testing.T) {
	query := `with ` + usagepricingsql.ContextIntervalsCTE("bands") + `,
	arms as (select ? as model, ? as input_tokens union all select ?, ?)
	insert into settings(key, value, updated_at_ms)
	select a.model, coalesce(t.threshold_tokens, -1), ? from arms a` +
		usagepricingsql.ContextBandJoin("bands", "t", "a.model", "a.input_tokens") + `
	on conflict(key) do update set value = excluded.value`
	args := []any{"left", int64(101), "right", int64(201), int64(1234)}
	compiled, bound, err := CompileSQL(query, args...)
	if err != nil || !reflect.DeepEqual(bound, args) {
		t.Fatalf("CTE INSERT compile=%s args=%v err=%v", compiled, bound, err)
	}
	if !strings.HasPrefix(compiled, "insert into settings") || !strings.Contains(compiled, "with bands as") || !strings.Contains(compiled, "on duplicate key update") {
		t.Fatalf("CTE INSERT was not relocated safely: %s", compiled)
	}
	s, err := compile(query)
	if err != nil || !s.write || s.txControl || s.ddl {
		t.Fatalf("CTE INSERT admission classification: %+v err=%v", s, err)
	}
}

func TestPricingPlanInspectionCannotBypassRuntimeAdmission(t *testing.T) {
	db, owner, backend := admissionDB(t)
	for _, query := range []string{"EXPLAIN FORMAT=JSON select 1", Native("EXPLAIN ANALYZE select 1")} {
		if rows, err := db.QueryContext(context.Background(), query); !errors.Is(err, ErrTransactionControl) {
			if rows != nil {
				rows.Close()
			}
			t.Fatalf("runtime EXPLAIN admitted: %v", err)
		}
	}
	if len(backend.queries) != 0 {
		t.Fatalf("admin SQL reached runtime backend: %v", backend.queries)
	}
	assertReleased(t, db, owner)
}

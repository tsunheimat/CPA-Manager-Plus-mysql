package usagepricing

import (
	"context"
	"database/sql"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

func pricingSQLFixture(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	columns := []string{"timestamp_ms integer", "model text default ''", "requested_model text default ''", "resolved_model text default ''", "service_tier text default ''", "failed integer default 0", "latency_ms integer"}
	for _, field := range []string{"input_tokens", "normalized_total_input_tokens", "output_tokens", "reasoning_tokens", "cached_tokens", "cache_tokens", "cache_read_tokens", "cache_creation_tokens", "total_tokens"} {
		columns = append(columns, field+" integer default 0")
	}
	for _, statement := range []string{
		`create table usage_events(id integer primary key, event_hash text, ` + strings.Join(columns, ",") + `)`,
		`create table usage_monitoring_event_projection_v1(event_id integer primary key, ` + strings.Join(columns, ",") + `)`,
		`create table model_prices(model text primary key)`,
		`create table model_price_context_tiers(model text, threshold_tokens integer, primary key(model, threshold_tokens))`,
		`create table usage_monitoring_rollup_state(rollup_name text, schema_version integer, structure_revision text, status text, coverage_event_id integer)`,
		`create table usage_archive_event_refs(event_hash text,raw_event_id integer,timestamp_ms integer,raw_deleted_at_ms integer)`,
	} {
		pricingSQLExec(t, db, statement)
	}
	pricingSQLExec(t, db, `insert into usage_monitoring_rollup_state values('projection_v1',1,?,'ready',100)`, usageidentity.MonitoringProjectionStructureRevision())
	return db
}

func pricingSQLExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlwrite.ExecContext(context.Background(), db, query, args...); err != nil {
		t.Fatal(err)
	}
}

// Frozen old classifier keeps its original unguarded price probes and scalar
// tier MAX. No production helper participates in this reference.
const frozenPricingClassifier = `with base_events as (
 select e.timestamp_ms, coalesce(nullif(e.requested_model,''), e.model) as requested_model_value,
 cpamp_analytics_model(coalesce(nullif(e.requested_model,''),e.model)) as analytics_model_value,
 coalesce(nullif(e.resolved_model,''),cpamp_analytics_model(coalesce(nullif(e.requested_model,''),e.model))) as billing_model_value,
 coalesce(e.normalized_total_input_tokens,e.input_tokens,0) as normalized_input_tokens_value
 from usage_events e where e.timestamp_ms >= ? and e.timestamp_ms < ?
), priced_events as (
 select base_events.*, case when b.model is not null then billing_model_value
 when a.model is not null then analytics_model_value when d.model is not null then requested_model_value
 else billing_model_value end as pricing_model_value
 from base_events left join model_prices b on b.model=billing_model_value
 left join model_prices a on a.model=analytics_model_value left join model_prices d on d.model=requested_model_value
), banded_events as (
 select priced_events.*, coalesce((select max(t.threshold_tokens) from model_price_context_tiers t
 where t.model=pricing_model_value and normalized_input_tokens_value>t.threshold_tokens),-1) as context_threshold_tokens_value
 from priced_events
)`

func TestPricingIntervalRawAgainstFrozenMAX(t *testing.T) {
	db := pricingSQLFixture(t)
	for _, name := range []string{"bill", "analytics", "display(high)", "no-tiers"} {
		pricingSQLExec(t, db, `insert into model_prices values(?)`, name)
	}
	for _, tier := range []struct {
		model     string
		threshold int64
	}{{"bill", 100}, {"bill", 200}, {"bill", math.MaxInt64 - 1}, {"bill", math.MaxInt64}, {"analytics", 100}, {"display(high)", 10}} {
		pricingSQLExec(t, db, `insert into model_price_context_tiers values(?,?)`, tier.model, tier.threshold)
	}
	type band struct {
		at        int64
		pricing   string
		threshold int64
	}
	var want []band
	for i, item := range []struct {
		requested, billing, pricing string
		input, threshold            int64
	}{
		{"analytics(high)", "bill", "bill", 100, -1}, {"analytics(high)", "bill", "bill", 101, 100},
		{"analytics(high)", "bill", "bill", 200, 100}, {"analytics(high)", "bill", "bill", 201, 200},
		{"analytics(high)", "bill", "bill", math.MaxInt64, math.MaxInt64 - 1},
		{"analytics(high)", "missing", "analytics", 101, 100}, {"display(high)", "missing", "display(high)", 11, 10},
		{"missing", "unknown", "unknown", 100, -1}, {"no-tiers", "no-tiers", "no-tiers", 100, -1},
	} {
		at := int64(i + 1)
		pricingSQLExec(t, db, `insert into usage_events(timestamp_ms,model,requested_model,resolved_model,input_tokens,normalized_total_input_tokens) values(?,'ignored',?,?,999,?)`, at, item.requested, item.billing, item.input)
		want = append(want, band{at, item.pricing, item.threshold})
	}
	for _, cte := range []string{frozenPricingClassifier, bandedEventsFromSourceFieldsCTE("e.timestamp_ms >= ? and e.timestamp_ms < ?", "usage_events", false)} {
		rows, err := db.Query(cte+` select timestamp_ms,pricing_model_value,context_threshold_tokens_value from banded_events order by timestamp_ms`, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		var got []band
		for rows.Next() {
			var b band
			if err := rows.Scan(&b.at, &b.pricing, &b.threshold); err != nil {
				t.Fatal(err)
			}
			got = append(got, b)
		}
		err = rows.Err()
		rows.Close()
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%#v want=%#v err=%v", got, want, err)
		}
	}
}

func TestRetainedHourlyArmsPreserveMutationAndMultiset(t *testing.T) {
	db := pricingSQLFixture(t)
	pricingSQLExec(t, db, `insert into model_prices values('bill')`)
	pricingSQLExec(t, db, `insert into model_price_context_tiers values('bill',100),('bill',200)`)
	// Live rows win over stale projections even with deletion evidence. Event 4
	// is a late ID with an old timestamp; it must survive the source watermark.
	pricingSQLExec(t, db, `insert into usage_events(id,event_hash,timestamp_ms,model,resolved_model,normalized_total_input_tokens,cached_tokens,cache_read_tokens,latency_ms)
	 values(1,'live',1000,'m','bill',100,20,5,10),(4,'late',1001,'m','bill',201,0,0,30)`)
	pricingSQLExec(t, db, `insert into usage_monitoring_event_projection_v1(event_id,timestamp_ms,model,resolved_model,normalized_total_input_tokens,cached_tokens,cache_read_tokens,latency_ms)
	 values(1,1000,'stale','bill',999,0,0,999),(2,1002,'m','bill',101,20,5,-5),(3,1003,'orphan','bill',999,0,0,0)`)
	pricingSQLExec(t, db, `insert into usage_archive_event_refs values('live',1,1000,1),('deleted',2,1002,1),('duplicate-proof',2,1002,2)`)
	for _, mutated := range []bool{false, true} {
		if mutated {
			pricingSQLExec(t, db, `update model_price_context_tiers set threshold_tokens=150 where threshold_tokens=100`)
		}
		tx, err := sqlwrite.BeginTx(context.Background(), db, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		arms, err := retainedEventSourceArmsTx(context.Background(), tx)
		if err != nil {
			t.Fatal(err)
		}
		filter := HourlyFilter{FromMS: 1000, ToMS: 2000, IncludeFailed: true, CollapseBuckets: true}
		query, args := hourlyStatementFromArms(filter, 1000, 2000, 0, false, arms)
		compiled, bound, err := mysql.CompileSQL(query, args...)
		if err != nil || !reflect.DeepEqual(bound, args) {
			t.Fatalf("compile=%s args=%v err=%v", compiled, bound, err)
		}
		if strings.Count(query, "group by 1, 2, 3, 4, 5, 6, 7") != 2 {
			t.Fatal("arms not grouped independently")
		}
		rows, err := tx.QueryContext(context.Background(), query, args...)
		if err != nil {
			t.Fatal(err)
		}
		grouped := map[hourlyKey]*HourlyRow{}
		err = scanAndMergeHourlyRows(rows, grouped)
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		var calls, input, cache, latency, samples int64
		bands := map[int64]int64{}
		for _, row := range grouped {
			calls += row.Calls
			input += row.InputTokens
			cache += row.CachedTokens
			latency += row.LatencySumMS
			samples += row.LatencySamples
			bands[row.ContextThresholdTokens] += row.Calls
		}
		if calls != 4 || input != 503 || cache != 45 || latency != 30 || samples != 4 {
			t.Fatalf("totals=%d,%d,%d,%d,%d", calls, input, cache, latency, samples)
		}
		want := map[int64]int64{-1: 1, 100: 2, 200: 1}
		if mutated {
			want = map[int64]int64{-1: 3, 200: 1}
		}
		if !reflect.DeepEqual(bands, want) {
			t.Fatalf("bands=%v want=%v", bands, want)
		}
		// Frozen source shape preserves its deliberate archive-join multiplicity.
		frozenSource := `(select e.id,e.timestamp_ms,e.model,e.requested_model,e.resolved_model,e.service_tier,e.failed,e.input_tokens,e.normalized_total_input_tokens,e.output_tokens,e.reasoning_tokens,e.cached_tokens,e.cache_tokens,e.cache_read_tokens,e.cache_creation_tokens,e.total_tokens,e.latency_ms from usage_events e
		 union all select p.event_id,p.timestamp_ms,p.model,p.requested_model,p.resolved_model,p.service_tier,p.failed,p.input_tokens,p.normalized_total_input_tokens,p.output_tokens,p.reasoning_tokens,p.cached_tokens,p.cache_tokens,p.cache_read_tokens,p.cache_creation_tokens,p.total_tokens,p.latency_ms
		 from usage_archive_event_refs archived join usage_monitoring_event_projection_v1 p on p.event_id=archived.raw_event_id and p.timestamp_ms=archived.timestamp_ms
		 where archived.raw_deleted_at_ms is not null and p.event_id<=100 and not exists(select 1 from usage_events live where live.event_hash=archived.event_hash))`
		oldQuery, oldArgs := hourlyStatementFromEvents(filter, 1000, 2000, 0, false, frozenSource)
		oldRows, err := tx.QueryContext(context.Background(), oldQuery, oldArgs...)
		if err != nil {
			t.Fatal(err)
		}
		oldGrouped := map[hourlyKey]*HourlyRow{}
		err = scanAndMergeHourlyRows(oldRows, oldGrouped)
		oldRows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(sortedHourlyRows(grouped), sortedHourlyRows(oldGrouped)) {
			t.Fatal("retained arm aggregation changed measures")
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRetainedAccountArmsMergeMaxSnapshotsBeforeScanning(t *testing.T) {
	db := pricingSQLFixture(t)
	for _, table := range []string{"usage_events", "usage_monitoring_event_projection_v1"} {
		for _, column := range []string{"provider", "auth_index", "source", "source_hash", "account_snapshot", "auth_label_snapshot", "auth_file_snapshot", "auth_provider_snapshot", "auth_project_id_snapshot", "auth_account_id_snapshot"} {
			pricingSQLExec(t, db, "alter table "+table+" add column "+column+" text default ''")
		}
	}
	pricingSQLExec(t, db, "alter table usage_monitoring_event_projection_v1 add column account_key text")
	key, ok := usageidentity.AccountKey(usageidentity.Fields{AuthFileSnapshot: "team.json", AuthIndex: "index"})
	if !ok {
		t.Fatal("invalid fixture identity")
	}
	pricingSQLExec(t, db, `insert into usage_events(id,event_hash,timestamp_ms,model,normalized_total_input_tokens,auth_file_snapshot,auth_index,account_snapshot,source) values(1,'live',1000,'m',100,'team.json','index','alpha','alpha')`)
	pricingSQLExec(t, db, `insert into usage_monitoring_event_projection_v1(event_id,timestamp_ms,model,normalized_total_input_tokens,auth_file_snapshot,auth_index,account_snapshot,source,account_key) values(2,900,'m',50,'team.json','index','zeta','zeta',?)`, key)
	pricingSQLExec(t, db, `insert into usage_archive_event_refs values('deleted',2,900,1),('duplicate-evidence',2,900,2)`)
	tx, err := sqlwrite.BeginTx(context.Background(), db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	arms, err := retainedAccountEventSourceArmsTx(context.Background(), tx, []string{key})
	if err != nil {
		t.Fatal(err)
	}
	query, args := accountStatementFromArms(0, []string{key}, arms)
	_, bound, err := mysql.CompileSQL(query, args...)
	if err != nil || !reflect.DeepEqual(bound, args) {
		t.Fatalf("bindings=%v err=%v", bound, err)
	}
	rows, err := tx.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	grouped := map[accountKey]*AccountRow{}
	err = scanAndMergeAccountRows(rows, grouped)
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	result := sortedAccountRows(grouped)
	if len(result) != 1 {
		t.Fatalf("rows=%#v", result)
	}
	row := result[0]
	if row.Calls != 2 || row.InputTokens != 150 || row.AccountSnapshot != "zeta" || row.Source != "zeta" || row.FirstSeenMS != 900 || row.LastSeenMS != 1000 {
		t.Fatalf("retained account multiset/MAX/minimum changed: %#v", row)
	}
}

func TestPricingMaintenanceCTEInsertCompilation(t *testing.T) {
	for _, source := range []string{"usage_events", usageprojection.RetainedEdgeSource(123, 456)} {
		for _, query := range []string{hourlyBatchSQL(source), accountBatchSQL(source)} {
			args := []any{int64(1), int64(9), "revision", int64(77)}
			compiled, bound, err := mysql.CompileSQL(query, args...)
			if err != nil || !reflect.DeepEqual(bound, args) {
				t.Fatalf("bound=%v err=%v", bound, err)
			}
			if !strings.HasPrefix(compiled, "insert into usage_pricing_") || !strings.Contains(compiled, "with pricing_context_intervals as") || !strings.Contains(compiled, "on duplicate key update") {
				t.Fatal(compiled)
			}
			for _, bad := range []string{"e.*", "base_events.*", "priced_events.*", "select max(tier.threshold_tokens)", "raw_json"} {
				if strings.Contains(query, bad) {
					t.Fatal(bad)
				}
			}
		}
	}
}

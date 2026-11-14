package usageevent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

type pricingQueryFunc func(context.Context, string, ...any) (*sql.Rows, error)

func (f pricingQueryFunc) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return f(ctx, query, args...)
}

func pricingCredentialNativeFixture(t *testing.T, db *sql.DB, from, to int64) (AnalyticsFilter, int64) {
	t.Helper()
	book := map[string]model.ModelPrice{"billed": {Prompt: 1, ContextTiers: []model.ModelPriceContextTier{
		{ThresholdTokens: 100, Prompt: 2, PromptConfigured: true},
		{ThresholdTokens: 200, Prompt: 3, PromptConfigured: true},
	}}}
	if err := modelprice.New(db).ReplaceAll(context.Background(), book); err != nil {
		t.Fatal(err)
	}
	// Include exact request, UTC-hour, and fractional local-hour boundaries, and
	// explicit rows just outside the half-open request. No ingestion normalization
	// can hide the intentionally different raw and normalized input populations.
	times := []int64{from - 1, from, from + 1, to - 1, to}
	for ts := (from/900000 + 1) * 900000; ts < to; ts += 900000 {
		times = append(times, ts)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	var calls int64
	for i, ts := range times {
		input := []int64{100, 101, 201, 272001}[i%4]
		insertPricingEvent(t, db, ts, "display", "calendar(high)", "billed", "priority", 1, input)
		pricingFixtureExec(t, db, `update usage_events set auth_file_snapshot='calendar.json',auth_index='calendar-index',source='calendar-source',source_hash='calendar-hash',auth_provider_snapshot='provider',account_snapshot='calendar-account',auth_label_snapshot='calendar-label',auth_account_id_snapshot='calendar-id',auth_project_id_snapshot='calendar-project',api_key_hash='calendar-key',latency_ms=9,output_tokens=2,reasoning_tokens=1,cached_tokens=40,cache_tokens=50,cache_read_tokens=10,cache_creation_tokens=5,total_tokens=?,failed=? where timestamp_ms=?`, input+2, i%2, ts)
		if ts >= from && ts < to {
			calls++
		}
	}
	insertPricingEvent(t, db, from+2, "excluded", "", "", "", 999, nil)
	return AnalyticsFilter{FromMS: from, ToMS: to, IncludeFailed: true, Models: []string{"calendar(high)"}, AuthFiles: []string{"calendar.json"}, SearchQuery: "calendar"}, calls
}

func pricingNativeCredentialReaders(t *testing.T, db *sql.DB) (*repository, *repository) {
	t.Helper()
	native := New(db, WithMySQLAnalytics(true)).(*repository)
	reference := New(db, WithMySQLAnalytics(false)).(*repository)
	if !mysql.IsMySQL(db) || !native.mysqlAnalytics || reference.mysqlAnalytics {
		t.Fatal("fixture must use real MySQL with explicit native=true and reference=false")
	}
	return native, reference
}

func TestCredentialPricingCalendarBindingsCompileAndReference(t *testing.T) {
	// Exercise every fixture's generated SQL/bindings and Go reference offline;
	// this is not a substitute for the explicitly native test below.
	pricingCredentialCalendarBindings(t, false)
}

func TestCredentialPricingNativeCalendarBindings(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set; native credential calendar acceptance remains pending")
	}
	pricingCredentialCalendarBindings(t, true)
}

func pricingCredentialCalendarBindings(t *testing.T, requireNative bool) {
	t.Helper()
	for _, tc := range []struct {
		name, zone, grain, from, to string
		split, mapped               bool
	}{
		{"dst-core", "America/New_York", "hour", "2026-11-01T04:13:00Z", "2026-11-01T08:17:00Z", true, true},
		{"dst-repeated-left-edge", "America/New_York", "hour", "2026-11-01T06:13:00Z", "2026-11-01T08:17:00Z", true, true},
		{"dst-day", "America/New_York", "day", "2026-11-01T04:13:00Z", "2026-11-02T06:17:00Z", true, true},
		{"fractional-offset", "Asia/Kathmandu", "hour", "2026-11-01T00:13:00Z", "2026-11-01T04:17:00Z", false, false},
		{"fractional-transition", "Australia/Lord_Howe", "hour", "2026-04-04T14:13:00Z", "2026-04-04T17:17:00Z", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			db := pricingTestDB(t)
			native := New(db, WithMySQLAnalytics(true)).(*repository)
			reference := New(db, WithMySQLAnalytics(false)).(*repository)
			if requireNative {
				native, reference = pricingNativeCredentialReaders(t, db)
			}
			from, err := time.Parse(time.RFC3339, tc.from)
			if err != nil {
				t.Fatal(err)
			}
			to, err := time.Parse(time.RFC3339, tc.to)
			if err != nil {
				t.Fatal(err)
			}
			location, err := time.LoadLocation(tc.zone)
			if err != nil {
				t.Fatal(err)
			}
			filter, wantCalls := pricingCredentialNativeFixture(t, db, from.UnixMilli(), to.UnixMilli())
			coreFrom, coreTo := usage.AnalyticsFullUTCHourRange(filter.FromMS, filter.ToMS)
			if split := usage.CanMapUTCWholeHours(coreFrom, coreTo, tc.grain, location); split != tc.split {
				t.Fatalf("wrong fixture path: split=%v want=%v", split, tc.split)
			}
			plan, ok, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, tc.grain, location, false)
			if err != nil || !ok || (len(plan.args) > 0) != tc.mapped {
				t.Fatalf("fixture calendar map: available=%v mapped=%v err=%v", ok, len(plan.args) > 0, err)
			}
			query, args := groupedAnalyticsSQL(filter, plan, "credential")
			_, sourceArgs := analyticsWhere(filter)
			wantArgs := append(append([]any{}, sourceArgs...), plan.args...)
			compiled, bindings, err := mysql.CompileSQL(query, args...)
			if err != nil || !reflect.DeepEqual(bindings, wantArgs) {
				t.Fatal("source/DST/fractional calendar binding order", bindings, err)
			}
			if mysql.IsMySQL(db) {
				query, args = mysql.Native(compiled), bindings
			}
			rows, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				t.Fatal(err)
			}
			grouped, err := readGroupedCredentialTimeline(rows)
			if err != nil {
				t.Fatal(err)
			}
			// Independent raw Go folding, explicitly NOT native SQL grouping or
			// hourly SQL. Constant integral latency avoids the pre-existing AVG
			// decimal rounding discrepancy in the hourly reader.
			want, err := reference.credentialTimelineRawWithFilter(ctx, filter, tc.grain, location)
			if err != nil {
				t.Fatal(err)
			}
			got, err := native.CredentialTimelineWithFilter(ctx, filter, tc.grain, location)
			if err != nil {
				t.Fatal(err)
			}
			refPublic, err := reference.CredentialTimelineWithFilter(ctx, filter, tc.grain, location)
			if err != nil || !reflect.DeepEqual(sortedAnalyticsJSON(got), sortedAnalyticsJSON(want)) || !reflect.DeepEqual(sortedAnalyticsJSON(grouped), sortedAnalyticsJSON(want)) || !reflect.DeepEqual(sortedAnalyticsJSON(refPublic), sortedAnalyticsJSON(want)) {
				t.Fatalf("native/public/reference calendar mismatch: native=%+v reference=%+v err=%v", got, want, err)
			}
			var calls int64
			for _, p := range got {
				calls += p.Calls
				if p.Model != "calendar" || p.AuthFileSnapshot != "calendar.json" || !p.AvgLatencyMS.Valid || p.AvgLatencyMS.Float64 != 9 {
					t.Fatalf("lost credential/pricing/latency grain: %+v", p)
				}
			}
			if calls != wantCalls {
				t.Fatalf("calendar intervals duplicated/lost events: calls=%d want=%d", calls, wantCalls)
			}
		})
	}
}

func TestCredentialPricingNativeMultipartSnapshot(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set; native credential snapshot acceptance remains pending")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := pricingTestDB(t)
	native, reference := pricingNativeCredentialReaders(t, db)
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 11, 1, 4, 13, 0, 0, time.UTC).UnixMilli()
	to := time.Date(2026, 11, 1, 8, 17, 0, 0, time.UTC).UnixMilli()
	filter, _ := pricingCredentialNativeFixture(t, db, from, to)
	want, err := reference.CredentialTimelineWithFilter(ctx, filter, "hour", location)
	if err != nil || len(want) == 0 {
		t.Fatal(want, err)
	}
	before, err := native.CredentialTimelineWithFilter(ctx, filter, "hour", location)
	if err != nil || !reflect.DeepEqual(sortedAnalyticsJSON(before), sortedAnalyticsJSON(want)) {
		t.Fatal("public native/reference before mutation", before, want, err)
	}
	tx, err := sqlwrite.BeginTx(ctx, db, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	coreFrom, coreTo := usage.AnalyticsFullUTCHourRange(from, to)
	stage := 0
	queryer := pricingQueryFunc(func(queryCtx context.Context, query string, args ...any) (*sql.Rows, error) {
		assertPricingNativeCredentialPart(t, stage, filter, "hour", location, query, args)
		if stage == 1 {
			// The first edge has been consumed/closed and established the InnoDB
			// read view. These writes commit on another connection, before core
			// and tail reads; neither the event nor catalog update may leak in.
			if _, err := sqlwrite.ExecContext(ctx, db, `update usage_events set output_tokens=output_tokens+1000,total_tokens=total_tokens+1000 where timestamp_ms>=? and timestamp_ms<?`, coreFrom, to); err != nil {
				t.Fatal(err)
			}
			if err := modelprice.New(db).ReplaceAll(ctx, map[string]model.ModelPrice{"billed": {Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 50, Prompt: 4, PromptConfigured: true}}}}); err != nil {
				t.Fatal(err)
			}
		}
		stage++
		return tx.QueryContext(queryCtx, query, args...)
	})
	// This is the same multipart reader called by the public method; only the
	// queryer is decorated. The existing offline driver matrix verifies public
	// begin/commit options and failures without adding a runtime mutation hook.
	parts, err := native.credentialTimelinePartsOn(ctx, queryer, filter, "hour", location, coreFrom, coreTo)
	if err != nil || stage != 3 {
		t.Fatalf("multipart native snapshot: stages=%d err=%v", stage, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got := mergeCredentialTimelineParts(parts)
	if !reflect.DeepEqual(sortedAnalyticsJSON(got), sortedAnalyticsJSON(want)) {
		t.Fatalf("committed between-part writes escaped repeatable read: got=%+v want=%+v", got, want)
	}
	fresh, err := native.CredentialTimelineWithFilter(ctx, filter, "hour", location)
	if err != nil || reflect.DeepEqual(sortedAnalyticsJSON(fresh), sortedAnalyticsJSON(want)) {
		t.Fatal("mutation was not visible to a new snapshot", fresh, err)
	}
	freshReference, err := reference.CredentialTimelineWithFilter(ctx, filter, "hour", location)
	if err != nil || !reflect.DeepEqual(sortedAnalyticsJSON(fresh), sortedAnalyticsJSON(freshReference)) {
		t.Fatal("public native/reference after mutation", fresh, freshReference, err)
	}
}

func assertPricingNativeCredentialPart(t *testing.T, stage int, filter AnalyticsFilter, grain string, location *time.Location, query string, args []any) {
	t.Helper()
	coreFrom, coreTo := usage.AnalyticsFullUTCHourRange(filter.FromMS, filter.ToMS)
	var wantQuery string
	var wantArgs []any
	if stage == 1 {
		filter.FromMS, filter.ToMS = coreFrom, coreTo
		var ok bool
		wantQuery, wantArgs, ok = credentialTimelineHourlySQL(filter, grain, location)
		if !ok || !strings.Contains(wantQuery, "bucket_map") {
			t.Fatal("snapshot fixture must exercise the mapped DST hourly core")
		}
	} else {
		if stage == 0 {
			filter.ToMS = coreFrom
		} else if stage == 2 {
			filter.FromMS = coreTo
		} else {
			t.Fatal("unexpected fallback/retry query", stage)
		}
		plan, ok, err := analyticsSQLBucketPlan(context.Background(), filter.FromMS, filter.ToMS, grain, location, false)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		wantQuery, wantArgs = groupedAnalyticsSQL(filter, plan, "credential")
		wantQuery, wantArgs, err = mysql.CompileSQL(wantQuery, wantArgs...)
		if err != nil {
			t.Fatal(err)
		}
		wantQuery = mysql.Native(wantQuery)
		if !strings.HasPrefix(query, mysql.Native("")) {
			t.Fatal("edge must execute compiled native grouping, not the raw fallback")
		}
	}
	if query != wantQuery || !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("multipart query/bindings differ at stage %d", stage)
	}
}

func TestCredentialPricingNativeMultipartTerminalErrors(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set; native credential terminal-error acceptance remains pending")
	}
	db := pricingTestDB(t)
	native, _ := pricingNativeCredentialReaders(t, db)
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 11, 1, 4, 13, 0, 0, time.UTC).UnixMilli()
	to := time.Date(2026, 11, 1, 8, 17, 0, 0, time.UTC).UnixMilli()
	filter, _ := pricingCredentialNativeFixture(t, db, from, to)
	coreFrom, coreTo := usage.AnalyticsFullUTCHourRange(from, to)
	for _, mode := range []string{"query", "scan", "cancel"} {
		for failStage := 0; failStage < 3; failStage++ {
			t.Run(fmt.Sprintf("%s/%d", mode, failStage), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				tx, err := sqlwrite.BeginTx(ctx, db, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				stage := 0
				queryer := pricingQueryFunc(func(queryCtx context.Context, query string, args ...any) (*sql.Rows, error) {
					assertPricingNativeCredentialPart(t, stage, filter, "hour", location, query, args)
					current := stage
					stage++
					if current == failStage {
						switch mode {
						case "query":
							return tx.QueryContext(queryCtx, mysql.Native(`select missing_fixture_column from usage_events`))
						case "scan":
							return tx.QueryContext(queryCtx, mysql.Native(`select 1`)) // real rows, wrong scan shape
						case "cancel":
							cancel()
						}
					}
					return tx.QueryContext(queryCtx, query, args...)
				})
				parts, err := native.credentialTimelinePartsOn(ctx, queryer, filter, "hour", location, coreFrom, coreTo)
				if err == nil || parts != nil || stage != failStage+1 {
					t.Fatalf("native error lost or retried: stages=%d parts=%+v err=%v", stage, parts, err)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation must remain terminal", err)
				}
			})
		}
	}
}

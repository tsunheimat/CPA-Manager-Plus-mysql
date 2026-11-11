package usageevent

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestCombinedLatencyAndGroupedTimelineDifferential(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "analytics.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	repo := New(db).(*repository)
	base := time.Date(2026, 11, 1, 0, 13, 0, 0, time.UTC).UnixMilli()
	events := make([]usage.Event, 0, 300)
	for i := 0; i < 300; i++ {
		latency, ttft := int64(i%23), int64(i%11)
		if i%9 == 0 {
			latency = -2
		}
		if i%13 == 0 {
			ttft = -1
		}
		events = append(events, usage.Event{EventHash: canonicalTestHash(fmt.Sprint("combined", i)), TimestampMS: base + int64(i)*171_000, Timestamp: "synthetic", Model: fmt.Sprintf("model-%d", i%3), APIKeyHash: fmt.Sprintf("api-%d", i%2), AuthFileSnapshot: "file", AuthIndex: "index", Provider: "openai", Failed: i%7 == 0, InputTokens: int64(100 + i), OutputTokens: 2, TotalTokens: int64(102 + i), LatencyMS: &latency, TTFTMS: &ttft, CreatedAtMS: 1})
	}
	if _, err := repo.InsertBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	for _, zone := range []string{"UTC", "Asia/Kathmandu", "America/New_York", "Australia/Lord_Howe"} {
		location, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatal(err)
		}
		for _, granularity := range []string{"hour", "day"} {
			for _, filtered := range []bool{false, true} {
				filter := AnalyticsFilter{FromMS: base, ToMS: base + 15*3_600_000, IncludeFailed: true}
				if filtered {
					filter.Models = []string{"model-1"}
					filter.APIKeyHashes = []string{usage.NormalizeOpaqueHashForPersistence("api-1")}
					filter.MinLatencyMS = 5
				}
				plan, ok, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, granularity, location, false)
				if err != nil || !ok {
					t.Fatalf("plan %s %s available=%v err=%v", zone, granularity, ok, err)
				}
				want, err := repo.LatencyMetricsWithFilter(ctx, filter, granularity, location, true, true)
				if err != nil {
					t.Fatal(err)
				}
				if len(want.Timeline) == 0 {
					t.Fatal("differential fixture unexpectedly has no positive samples")
				}
				mapped, err := repo.latencyMetricsMapped(ctx, filter, granularity, location, true)
				if err != nil || !reflect.DeepEqual(mapped, want) {
					t.Fatalf("bounded interval fallback %s %s got=%#v want=%#v err=%v", zone, granularity, mapped, want, err)
				}
				standalone, err := repo.LatencySummaryWithFilter(ctx, filter)
				if err != nil || standalone != want.Summary {
					t.Fatalf("standalone=%#v combined=%#v err=%v", standalone, want.Summary, err)
				}
				for _, scope := range [][2]bool{{true, true}, {true, false}, {false, true}} {
					query, args := latencyMetricsSQL(filter, plan, scope[0], scope[1])
					native, bindings, err := mysql.CompileSQL(query, args...)
					if err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(strings.ToLower(native), "div") || len(bindings) != len(args) {
						t.Fatalf("native rank not integral or bindings changed: %s", native)
					}
					if mysql.IsMySQL(db) {
						query, args = mysql.Native(native), bindings
					}
					got, err := repo.readLatencyMetricsSQL(ctx, query, args)
					if err != nil {
						t.Fatal(err)
					}
					if scope[0] && got.Summary != want.Summary {
						t.Fatalf("summary %s %s got=%#v want=%#v", zone, granularity, got.Summary, want.Summary)
					}
					if scope[1] && !reflect.DeepEqual(got.Timeline, want.Timeline) {
						t.Fatalf("timeline %s %s got=%#v want=%#v", zone, granularity, got.Timeline, want.Timeline)
					}
				}
				query, args := groupedAnalyticsSQL(filter, plan, "timeline")
				compiled, bindings, err := mysql.CompileSQL(query, args...)
				if err != nil {
					t.Fatal(err)
				}
				if mysql.IsMySQL(db) {
					query, args = mysql.Native(compiled), bindings
				}
				rows, err := db.QueryContext(ctx, query, args...)
				if err != nil {
					t.Fatal(err)
				}
				got := []TimelinePoint{}
				for rows.Next() {
					var point TimelinePoint
					if err := scanGroupedTimeline(rows, &point); err != nil {
						t.Fatal(err)
					}
					got = append(got, point)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				reference, err := repo.TimelineWithFilter(ctx, filter, granularity, location)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(sortedAnalyticsJSON(got), sortedAnalyticsJSON(reference)) {
					t.Fatalf("grouped %s %s filtered=%v\ngot=%v\nwant=%v", zone, granularity, filtered, got, reference)
				}
			}
		}
	}
	empty, err := repo.LatencyMetricsWithFilter(ctx, AnalyticsFilter{FromMS: 1, ToMS: 2}, "hour", time.UTC, true, true)
	if err != nil || empty.Summary.P95LatencyMS.Valid || empty.Summary.P95TTFTMS.Valid || len(empty.Timeline) != 0 {
		t.Fatalf("empty=%#v err=%v", empty, err)
	}
}

func sortedAnalyticsJSON[T any](values []T) []string {
	out := make([]string, len(values))
	for i, v := range values {
		encoded, _ := json.Marshal(v)
		out[i] = string(encoded)
	}
	sort.Strings(out)
	return out
}

func TestAnalyticsBucketPlanTransitionsAndBoundedFallback(t *testing.T) {
	ctx := context.Background()
	for _, zone := range []string{"America/New_York", "Australia/Lord_Howe", "Asia/Kathmandu"} {
		location, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatal(err)
		}
		for _, base := range []time.Time{time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC)} {
			from, to := base.UnixMilli(), base.Add(48*time.Hour).UnixMilli()
			for _, grain := range []string{"hour", "day"} {
				plan, available, err := analyticsSQLBucketPlan(ctx, from, to, grain, location, false)
				if err != nil || !available {
					t.Fatal(available, err)
				}
				if len(plan.args) > 0 {
					last := from
					for i := 0; i < len(plan.args); i += 5 {
						start, end, bucket := plan.args[i].(int64), plan.args[i+1].(int64), plan.args[i+2].(int64)
						if start != last || end <= start {
							t.Fatalf("not a partition: %v", plan.args)
						}
						for sample := start; sample < end; sample += 60_000 {
							if usage.AnalyticsBucketMS(sample, grain, location) != bucket {
								t.Fatalf("bucket mismatch %s %s %v", zone, grain, time.UnixMilli(sample))
							}
						}
						last = end
					}
					if last != to {
						t.Fatal("truncated map")
					}
				}
			}
		}
	}
	zone, _ := time.LoadLocation("America/New_York")
	_, ok, err := analyticsSQLBucketPlan(ctx, 0, 20*365*86_400_000, "hour", zone, false)
	if err != nil || ok {
		t.Fatal("oversized map must explicitly fall back", ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := analyticsSQLBucketPlan(canceled, 0, 1, "hour", time.UTC, false); err == nil {
		t.Fatal("cancellation lost")
	}
}

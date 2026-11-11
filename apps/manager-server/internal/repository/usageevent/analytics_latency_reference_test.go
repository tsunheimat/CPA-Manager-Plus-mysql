package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

type latencyReferenceFixture struct {
	timestamp     int64
	latency, ttft sql.NullInt64
	model         string
	failed        bool
	apiKey        string
	provider      string
	authProvider  string
	account       string
	source        string
}

// This oracle is intentionally independent of the production percentile helper,
// SQL rank/count builder and bucket planner. Its arrays are bounded test data;
// the reference implementation under test must not retain raw-sample arrays.
func latencyReferenceOracle(events []latencyReferenceFixture, from, to int64, grain string, location *time.Location, accept func(latencyReferenceFixture) bool) LatencyMetrics {
	if location == nil {
		location = time.UTC
	}
	type samples [2][]int64
	all := samples{}
	byBucket := map[int64]*samples{}
	for _, event := range events {
		if event.timestamp < from || event.timestamp >= to || !accept(event) {
			continue
		}
		if (!event.latency.Valid || event.latency.Int64 <= 0) && (!event.ttft.Valid || event.ttft.Int64 <= 0) {
			continue
		}
		local := time.UnixMilli(event.timestamp).In(location)
		hour := local.Hour()
		if grain == "day" {
			hour = 0
		}
		bucket := time.Date(local.Year(), local.Month(), local.Day(), hour, 0, 0, 0, location).UnixMilli()
		if byBucket[bucket] == nil {
			byBucket[bucket] = &samples{}
		}
		for metric, value := range []sql.NullInt64{event.latency, event.ttft} {
			if value.Valid && value.Int64 > 0 {
				all[metric] = append(all[metric], value.Int64)
				byBucket[bucket][metric] = append(byBucket[bucket][metric], value.Int64)
			}
		}
	}
	percentile := func(values []int64) sql.NullFloat64 {
		if len(values) == 0 {
			return sql.NullFloat64{}
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return sql.NullFloat64{Float64: float64(values[(19*len(values)+19)/20-1]), Valid: true}
	}
	result := LatencyMetrics{Summary: LatencySummary{P95LatencyMS: percentile(all[0]), P95TTFTMS: percentile(all[1])}, Timeline: []LatencyPercentiles{}}
	for bucket, values := range byBucket {
		result.Timeline = append(result.Timeline, LatencyPercentiles{BucketMS: bucket, P95LatencyMS: percentile(values[0]), P95TTFTMS: percentile(values[1])})
	}
	sort.Slice(result.Timeline, func(i, j int) bool { return result.Timeline[i].BucketMS < result.Timeline[j].BucketMS })
	return result
}

func TestLatencyReferenceIndependentDifferential(t *testing.T) {
	// These tests execute portable reference SQL locally, even when native suites
	// have been opted into elsewhere. Native MySQL acceptance remains separate.
	db, err := sqliterepo.Open(filepath.Join(t.TempDir(), "reference.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := New(db).(*repository)
	ctx := context.Background()
	value := func(n int64) sql.NullInt64 { return sql.NullInt64{Int64: n, Valid: true} }
	for _, calendar := range []struct{ zone, start string }{
		{"", "1969-12-31T00:13:00Z"},
		{"Asia/Kathmandu", "2026-01-01T00:13:00Z"},
		{"America/New_York", "2026-03-07T00:13:00Z"},
		{"America/New_York", "2026-10-31T00:13:00Z"},
		{"Australia/Lord_Howe", "2026-04-04T00:13:00Z"},
		{"Antarctica/Casey", "2010-03-03T00:13:00Z"},
	} {
		t.Run(calendar.zone+"/"+calendar.start, func(t *testing.T) {
			var location *time.Location
			if calendar.zone != "" {
				location, err = time.LoadLocation(calendar.zone)
				if err != nil {
					t.Fatal(err)
				}
			}
			start, err := time.Parse(time.RFC3339, calendar.start)
			if err != nil {
				t.Fatal(err)
			}
			from, to := start.UnixMilli(), start.Add(96*time.Hour).UnixMilli()
			events := make([]latencyReferenceFixture, 0, 320)
			for i := int64(0); i < 260; i++ {
				event := latencyReferenceFixture{
					timestamp: from + (i*17*60_000)%(to-from),
					latency:   value((i*7)%53 - 2), ttft: value((i*11)%29 - 3),
					model: "model-a", apiKey: "key-a", provider: "provider-a", account: "account-a",
					failed: i%7 == 0,
				}
				if i%3 == 0 {
					event.latency = sql.NullInt64{}
				}
				if i%5 == 0 {
					event.ttft = sql.NullInt64{}
				}
				if i%2 == 0 {
					event.model, event.apiKey = "model-b", "key-b"
				}
				if i%4 == 0 {
					event.provider, event.authProvider = "other-provider", "provider-a"
				}
				if i%6 == 0 {
					event.account, event.source = "other-account", "account-a"
				}
				if i%11 == 0 {
					event.provider, event.authProvider, event.account, event.source = "other-provider", "", "other-account", ""
				}
				events = append(events, event)
			}
			// More than twenty independent samples in a single bucket exercises
			// nearest rank rather than MAX and makes averaging bucket P95s wrong.
			for i := int64(1); i <= 41; i++ {
				events = append(events, latencyReferenceFixture{timestamp: from + 42*3_600_000 + i, latency: value(i * 13), ttft: value(42 - i), model: "dense", provider: "provider-a", account: "account-a"})
			}
			// Exact half-open edges plus buckets with only one metric or neither.
			for _, event := range []latencyReferenceFixture{
				{timestamp: from - 1, latency: value(1_000_000), ttft: value(1_000_000)},
				{timestamp: from, latency: value(2), ttft: value(0)},
				{timestamp: to - 1, latency: sql.NullInt64{}, ttft: value(3)},
				{timestamp: to, latency: value(1_000_000), ttft: value(1_000_000)},
				{timestamp: to - 2*3_600_000, latency: value(0), ttft: value(-1)},
			} {
				events = append(events, event)
			}
			if calendar.zone == "Antarctica/Casey" {
				// Local March 5 rolls back to March 4: the March 4 population is
				// disconnected in timestamp order and must still be ranked whole.
				for _, stamp := range []string{"2010-03-04T21:30:00+11:00", "2010-03-05T01:30:00+11:00", "2010-03-04T23:30:00+08:00"} {
					tm, err := time.Parse(time.RFC3339, stamp)
					if err != nil {
						t.Fatal(err)
					}
					events = append(events, latencyReferenceFixture{timestamp: tm.UnixMilli(), latency: value(int64(len(events) * 17)), ttft: value(1)})
				}
			}
			tx, err := sqlwrite.BeginTx(ctx, db, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, `delete from usage_events`); err != nil {
				t.Fatal(err)
			}
			stmt, err := tx.PrepareContext(ctx, `insert into usage_events(event_hash,timestamp_ms,timestamp,model,latency_ms,ttft_ms,failed,api_key_hash,provider,auth_provider_snapshot,account_snapshot,source,created_at_ms) values (?,?,'synthetic',?,?,?,?,?,?,?,?,?,1)`)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			// Deliberately reverse insertion order: neither IDs nor cursor order
			// may be mistaken for chronological or metric order.
			for i := len(events) - 1; i >= 0; i-- {
				event := events[i]
				if _, err := stmt.ExecContext(ctx, fmt.Sprintf("%064x", i+1), event.timestamp, event.model, event.latency, event.ttft, event.failed, event.apiKey, event.provider, event.authProvider, event.account, event.source); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			for _, scenario := range []struct {
				name   string
				filter AnalyticsFilter
				accept func(latencyReferenceFixture) bool
			}{
				{"all", AnalyticsFilter{IncludeFailed: true}, func(latencyReferenceFixture) bool { return true }},
				{"success", AnalyticsFilter{}, func(e latencyReferenceFixture) bool { return !e.failed }},
				{"failure", AnalyticsFilter{IncludeFailed: true, FailedOnly: true}, func(e latencyReferenceFixture) bool { return e.failed }},
				{"combined-filters", AnalyticsFilter{Models: []string{"model-a"}, APIKeyHashes: []string{"key-a"}, Providers: []string{"PROVIDER-A"}, Accounts: []string{"ACCOUNT-A"}, MinLatencyMS: 20}, func(e latencyReferenceFixture) bool {
					return !e.failed && e.model == "model-a" && e.apiKey == "key-a" && (e.provider == "provider-a" || e.authProvider == "provider-a") && (e.account == "account-a" || e.source == "account-a") && e.latency.Valid && e.latency.Int64 >= 20
				}},
				{"substring", AnalyticsFilter{IncludeFailed: true, SearchQuery: "DENSE"}, func(e latencyReferenceFixture) bool { return e.model == "dense" }},
				{"empty", AnalyticsFilter{IncludeFailed: true, Models: []string{"absent"}}, func(latencyReferenceFixture) bool { return false }},
			} {
				for _, grain := range []string{"hour", "day"} {
					t.Run(scenario.name+"/"+grain, func(t *testing.T) {
						filter := scenario.filter
						filter.FromMS, filter.ToMS = from, to
						want := latencyReferenceOracle(events, from, to, grain, location, scenario.accept)
						for _, scope := range [][2]bool{{true, true}, {true, false}, {false, true}} {
							got, err := repo.latencyMetricsReference(ctx, filter, grain, location, scope[0], scope[1])
							expected := want
							if !scope[0] {
								expected.Summary = LatencySummary{}
							}
							if !scope[1] {
								expected.Timeline = []LatencyPercentiles{}
							}
							if err != nil || !reflect.DeepEqual(got, expected) {
								t.Fatalf("scope %v\nreference=%#v\noracle=%#v\nerror=%v", scope, got, expected, err)
							}
						}
						ranked, err := repo.LatencyMetricsWithFilter(ctx, filter, grain, location, true, true)
						if err != nil || !reflect.DeepEqual(ranked, want) {
							t.Fatalf("portable rank/reference differential\nranked=%#v\noracle=%#v\nerror=%v", ranked, want, err)
						}
					})
				}
			}
		})
	}
}

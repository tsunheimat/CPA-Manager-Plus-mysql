package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestGroupedHeatmapFinalGrain(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "heatmap-grain.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	repo := New(db).(*repository)
	if err := modelprice.New(db).ReplaceAll(ctx, map[string]model.ModelPrice{
		"billed": {Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 100, Prompt: 2, PromptConfigured: true}}},
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 25, 5, 13, 0, 0, time.UTC).UnixMilli()
	var events []usage.Event
	for week := 0; week < 4; week++ {
		for band, input := range []int64{100, 101, usage.LongContextInputTokenThreshold + 1} {
			events = append(events, usage.Event{
				EventHash: fmt.Sprintf("%064x", week*3+band+1), TimestampMS: base + int64(week)*7*24*3_600_000 + int64(band)*1000,
				Timestamp: "synthetic", Model: "display", ResolvedModel: "billed", APIKeyHash: "key", Provider: "provider",
				InputTokens: input, OutputTokens: 3, ReasoningTokens: 2, CachedTokens: 40, CacheReadTokens: 10, CacheCreationTokens: 5,
				TotalTokens: input + 3, Failed: week%2 != 0, CreatedAtMS: 1,
			})
		}
	}
	if _, err := repo.InsertBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	for _, zone := range []string{"Asia/Hong_Kong", "Asia/Kathmandu", "America/New_York", "Australia/Lord_Howe"} {
		t.Run(zone, func(t *testing.T) {
			location, err := time.LoadLocation(zone)
			if err != nil {
				t.Fatal(err)
			}
			for _, includeFailed := range []bool{false, true} {
				filter := AnalyticsFilter{FromMS: base - 123, ToMS: base + 22*24*3_600_000 + 123, IncludeFailed: includeFailed}
				plan, available, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, "hour", location, true)
				if err != nil || !available {
					t.Fatal(available, err)
				}
				query, args := groupedAnalyticsSQL(filter, plan, "heatmap")
				if strings.Contains(query, "sum(reasoning_tokens)") || strings.Contains(query, "latency_ms") {
					t.Fatal("heatmap computes unused timeline metrics")
				}
				native, bindings, err := mysql.CompileSQL(query, args...)
				if err != nil {
					t.Fatal(err)
				}
				if mysql.IsMySQL(db) {
					query, args = mysql.Native(native), bindings
				}
				rows, err := db.QueryContext(ctx, query, args...)
				if err != nil {
					t.Fatal(err)
				}
				got, err := readGroupedHeatmap(rows)
				if err != nil {
					t.Fatal(err)
				}
				want, err := repo.HeatmapWithFilter(ctx, filter, location)
				if err != nil || len(got) == 0 || !reflect.DeepEqual(got, want) {
					t.Fatalf("heatmap grain, ordering, or bands changed: got=%+v want=%+v err=%v", got, want, err)
				}
			}
		})
	}
}

func TestGroupedScopedReadersDifferential(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "grouped.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	repo := New(db).(*repository)
	if err := modelprice.New(db).ReplaceAll(ctx, map[string]model.ModelPrice{"model": {Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 100, Prompt: 2, PromptConfigured: true}, {ThresholdTokens: 200, Prompt: 3, PromptConfigured: true}}}}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 31, 0, 13, 0, 0, time.UTC).UnixMilli()
	var events []usage.Event
	for i := 0; i < 240; i++ {
		latency := int64(i%19 - 1)
		event := usage.Event{EventHash: fmt.Sprintf("%064x", i+1), TimestampMS: base + int64(i/3)*3_600_000 + int64(i%3)*1000, Timestamp: "synthetic", Model: "model", ResolvedModel: "model", APIKeyHash: fmt.Sprintf("api-%d", i%2), AuthFileSnapshot: strings.Repeat("界", 60) + ".json", AuthIndex: "index", Provider: "codex", AuthProviderSnapshot: "codex", AuthAccountIDSnapshot: "workspace", AccountSnapshot: "alice@example.com", Failed: i%7 == 0, InputTokens: 100 + int64(i%3)*100, OutputTokens: 3, ReasoningTokens: 2, CachedTokens: 40, CacheReadTokens: 10, CacheCreationTokens: 5, TotalTokens: 105 + int64(i%3)*100, LatencyMS: &latency, CreatedAtMS: 1}
		if i%4 == 0 {
			event.AuthLabelSnapshot = "label"
			event.AuthProjectIDSnapshot = "codex-account-id:v1:workspace"
		}
		if i%5 == 0 {
			event.ServiceTier = "priority"
		}
		events = append(events, event)
	}
	if _, err := repo.InsertBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	queryRows := func(query string, args []any) *sql.Rows {
		t.Helper()
		native, bindings, err := mysql.CompileSQL(query, args...)
		if err != nil {
			t.Fatal(err)
		}
		if mysql.IsMySQL(db) {
			query, args = mysql.Native(native), bindings
		}
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	for _, zone := range []string{"UTC", "Asia/Hong_Kong", "Asia/Kathmandu", "America/New_York", "Australia/Lord_Howe"} {
		location, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatal(err)
		}
		for _, grain := range []string{"hour", "day"} {
			filter := AnalyticsFilter{FromMS: base + 123, ToMS: base + 80*3_600_000 - 123, IncludeFailed: true, APIKeyHashes: []string{usage.NormalizeOpaqueHashForPersistence("api-0"), usage.NormalizeOpaqueHashForPersistence("api-1")}}
			plan, ok, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, grain, location, false)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			query, args := groupedAnalyticsSQL(filter, plan, "api")
			api, err := readGroupedAPIKeyTimeline(queryRows(query, args))
			if err != nil {
				t.Fatal(err)
			}
			apiWant, err := repo.APIKeyTimelineWithFilter(ctx, filter, grain, location)
			if err != nil || len(api) == 0 || !reflect.DeepEqual(sortedAnalyticsJSON(api), sortedAnalyticsJSON(apiWant)) {
				t.Fatalf("API %s %s mismatch got=%#v want=%#v err=%v", zone, grain, api, apiWant, err)
			}
			query, args = groupedAnalyticsSQL(filter, plan, "credential")
			credentials, err := readGroupedCredentialTimeline(queryRows(query, args))
			if err != nil {
				t.Fatal(err)
			}
			credentialWant, err := repo.credentialTimelineRawWithFilter(ctx, filter, grain, location)
			if err != nil || len(credentials) == 0 || !reflect.DeepEqual(sortedAnalyticsJSON(credentials), sortedAnalyticsJSON(credentialWant)) {
				t.Fatalf("credential %s %s mismatch got=%#v want=%#v err=%v", zone, grain, credentials, credentialWant, err)
			}
			clock, ok, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, "hour", location, true)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			query, args = groupedAnalyticsSQL(filter, clock, "heatmap")
			heatmap, err := readGroupedHeatmap(queryRows(query, args))
			if err != nil {
				t.Fatal(err)
			}
			heatmapWant, err := repo.HeatmapWithFilter(ctx, filter, location)
			if err != nil || len(heatmap) == 0 || !reflect.DeepEqual(sortedAnalyticsJSON(heatmap), sortedAnalyticsJSON(heatmapWant)) {
				t.Fatalf("heatmap %s mismatch got=%#v want=%#v err=%v", zone, heatmap, heatmapWant, err)
			}
			query, args = hourlyDistributionSQL(filter, clock)
			rows := queryRows(query, args)
			hours := []HourlyPoint{}
			for rows.Next() {
				var p HourlyPoint
				if err := rows.Scan(&p.Hour, &p.Calls, &p.Tokens); err != nil {
					t.Fatal(err)
				}
				hours = append(hours, p)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			hoursWant, err := repo.HourlyDistributionWithFilter(ctx, filter, location)
			if err != nil || !reflect.DeepEqual(hours, hoursWant) {
				t.Fatalf("hours %s mismatch got=%#v want=%#v err=%v", zone, hours, hoursWant, err)
			}
		}
	}
}

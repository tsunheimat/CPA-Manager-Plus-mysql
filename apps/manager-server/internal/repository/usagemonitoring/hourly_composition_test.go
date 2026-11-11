package usagemonitoring

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

func TestHourlyUsesReadyRowsAndFallsBackBeforePublication(t *testing.T) {
	db, repo := hourlyFixture(t)
	ctx := context.Background()
	event := usage.Event{EventHash: fmt.Sprintf("%064x", 1), TimestampMS: hourMS + 1000, Timestamp: "synthetic", Model: "model", APIKeyHash: "key", InputTokens: 10, TotalTokens: 10, CreatedAtMS: 1}
	if _, err := usageevent.New(db).InsertBatch(ctx, []usage.Event{event}); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func(context.Context, int, int64) (CatchUpResult, error){repo.CatchUpProjection, repo.CatchUpStats, repo.CatchUpHourlyStats, repo.CatchUpHourlyMetadata} {
		if result, err := fn(ctx, 100, 1000); err != nil || result.Pending {
			t.Fatal(result, err)
		}
	}
	fillFixtureDigests(t, db)
	// Deliberate fixture mutation proves the stored-hour branch was taken;
	// a not-ready generation must instead use the exact event reader.
	hourlyExec(t, db, `update usage_monitoring_event_projection_v1 set normalized_total_input_tokens=999`)
	filter := AnalyticsFilter{FromMS: hourMS, ToMS: 2 * hourMS, IncludeFailed: true}
	for _, status := range []string{"ready", "catching_up", "pending", "clearing", "failed", "rebuilding"} {
		hourlyExec(t, db, `update usage_monitoring_rollup_state set status=? where rollup_name=?`, status, HourlyStatsRollupName)
		rows, _, ok, err := repo.LoadAPIKeyStats(ctx, filter)
		want := int64(999)
		if status == "ready" || status == "catching_up" {
			want = 10
		}
		if err != nil || !ok || len(rows) != 1 || rows[0].InputTokens != want {
			t.Fatalf("%s rows=%#v available=%v err=%v", status, rows, ok, err)
		}
	}
	hourlyExec(t, db, `update usage_monitoring_rollup_state set status='ready',structure_revision='obsolete' where rollup_name=?`, HourlyStatsRollupName)
	rows, _, ok, err := repo.LoadAPIKeyStats(ctx, filter)
	if err != nil || !ok || len(rows) != 1 || rows[0].InputTokens != 999 {
		t.Fatal(rows, ok, err)
	}
}

func TestHourlyLargeScopeBindingsKeepFullEquality(t *testing.T) {
	db, repo := hourlyFixture(t)
	ctx := context.Background()
	if _, err := usageevent.New(db).InsertBatch(ctx, []usage.Event{{EventHash: fmt.Sprintf("%064x", 1), TimestampMS: 1000, Timestamp: "synthetic", Model: "model", APIKeyHash: "key", InputTokens: 1, TotalTokens: 1, CreatedAtMS: 1}}); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func(context.Context, int, int64) (CatchUpResult, error){repo.CatchUpProjection, repo.CatchUpHourlyStats} {
		if _, err := fn(ctx, 100, 1000); err != nil {
			t.Fatal(err)
		}
	}
	fillFixtureDigests(t, db)
	filter := AnalyticsFilter{FromMS: 0, ToMS: hourMS, IncludeFailed: true, APIKeyHashes: make([]string, 35000)}
	for i := range filter.APIKeyHashes {
		filter.APIKeyHashes[i] = fmt.Sprintf("key-%d", i)
	}
	filter.APIKeyHashes[34999] = usage.NormalizeOpaqueHashForPersistence("key")
	revision, err := CurrentStatsStructureRevision(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	conditions, args := storedTableStatsConditions(filter, revision, 0, hourMS, "usage_monitoring_api_key_hourly_rollups_v1")
	query := `select count(*) from usage_monitoring_api_key_hourly_rollups_v1 where ` + strings.Join(conditions, " and ")
	if len(args) > 10 || !strings.Contains(query, "coalesce(api_key_hash, '') in") || !strings.Contains(query, "_api_key_hash in") {
		t.Fatalf("full equality lost or %d unbounded bindings", len(args))
	}
	if _, _, err := mysql.CompileSQL(query, args...); err != nil {
		t.Fatal(err)
	}
	rows, _, ok, err := repo.LoadAPIKeyStats(ctx, filter)
	if err != nil || !ok || len(rows) != 1 || rows[0].Calls != 1 {
		t.Fatal(rows, ok, err)
	}
	if !mysql.IsMySQL(db) {
		// A digest match is only a narrowing predicate, never identity authority.
		hourlyExec(t, db, `update usage_monitoring_api_key_hourly_rollups_v1 set api_key_hash='not-in-filter'`)
		rows, _, ok, err = repo.LoadAPIKeyStats(ctx, filter)
		if err != nil || !ok || len(rows) != 0 {
			t.Fatal("digest-only equality", rows, ok, err)
		}
	}
}

func TestHourlyCodexResidualAndBatchedCredentialWindows(t *testing.T) {
	db, repo := hourlyFixture(t)
	ctx := context.Background()
	valid := usage.Event{EventHash: fmt.Sprintf("%064x", 1), TimestampMS: hourMS + 1000, Timestamp: "synthetic", Model: "model", APIKeyHash: "key", Provider: "codex", AuthProviderSnapshot: "codex", AuthIndex: "idx", AuthFileSnapshot: "alice.json", Source: "alice.json", AccountSnapshot: "alice@example.com", AuthAccountIDSnapshot: "workspace-1", InputTokens: 100, TotalTokens: 100, CreatedAtMS: 1}
	conflict := valid
	conflict.EventHash = fmt.Sprintf("%064x", 2)
	conflict.TimestampMS++
	conflict.AuthProjectIDSnapshot = usageidentity.CodexAccountIDSnapshot("workspace-2")
	conflict.InputTokens = 200
	conflict.TotalTokens = 200
	if _, err := usageevent.New(db).InsertBatch(ctx, []usage.Event{valid, conflict}); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func(context.Context, int, int64) (CatchUpResult, error){repo.CatchUpProjection, repo.CatchUpStats, repo.CatchUpHourlyStats, repo.CatchUpCodexLegacyIdentityEvidence} {
		if result, err := fn(ctx, 100, 1000); err != nil || result.Pending {
			t.Fatal(result, err)
		}
	}
	fillFixtureDigests(t, db)
	var stored int
	if err := db.QueryRow(`select sum(calls) from usage_monitoring_account_hourly_rollups_v1`).Scan(&stored); err != nil || stored != 1 {
		t.Fatal("marker merged into stored grain", stored, err)
	}
	// More than one parameter batch, including repeated windows, edges, and
	// a conflicting marker window. Each request index is independent.
	windows := make([]AccountWindowUsageQuery, 205)
	for i := range windows {
		windows[i] = AccountWindowUsageQuery{RequestIndex: i, FromMS: hourMS + int64(i%2)*500, ToMS: 3 * hourMS, AccountSnapshot: "alice@example.com", AuthFileSnapshot: "alice.json", Source: "alice.json", AuthIndex: "idx", AuthProviderSnapshot: "codex", AuthAccountIDSnapshot: "workspace-1"}
		if i%3 == 0 {
			windows[i].AuthProjectIDSnapshot = usageidentity.CodexAccountIDSnapshot("workspace-2")
		}
	}
	got, _, ok, err := repo.LoadAccountWindowStats(ctx, windows)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	want, _, ok, err := New(db).LoadAccountWindowStats(ctx, windows)
	if err != nil || !ok || !reflect.DeepEqual(got, want) || len(got) != 205 {
		t.Fatalf("batched residual mismatch got=%#v want=%#v err=%v", got, want, err)
	}
	filter := AnalyticsFilter{FromMS: hourMS, ToMS: 3 * hourMS, IncludeFailed: true}
	totals, _, ok, err := repo.LoadAggregate(ctx, filter)
	if err != nil || !ok || totals.TotalCalls != 2 {
		t.Fatal("core marker residual lost", totals, ok, err)
	}
}

func TestMonitoringSpansAreDisjointAtEveryBoundary(t *testing.T) {
	for _, from := range []int64{-dayMS - 17, -hourMS, -1, 0, 17, hourMS, dayMS + 17} {
		for _, length := range []int64{1, hourMS - 1, hourMS, dayMS, 3*dayMS + 123} {
			for _, daily := range []bool{false, true} {
				for _, hourly := range []bool{false, true} {
					spans := monitoringSpans(from, from+length, State{CoverageEventID: 7}, daily, State{CoverageEventID: 11}, hourly)
					cursor := from
					for _, span := range spans {
						if span.from != cursor || span.to <= span.from {
							t.Fatal("overlap or gap", spans)
						}
						if span.grain != 0 && (span.from%span.grain != 0 || span.to%span.grain != 0) {
							t.Fatal("partial aggregate", span)
						}
						if span.grain == hourMS && span.state.CoverageEventID != 11 || span.grain == dayMS && span.state.CoverageEventID != 7 {
							t.Fatal("mixed checkpoint", span)
						}
						cursor = span.to
					}
					if cursor != from+length {
						t.Fatal("truncated partition", spans)
					}
				}
			}
		}
	}
}

package usagearchive

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/datamigration"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageaggregate"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagemonitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// Permanent regressions promoted from the retained-history lifecycle audit.
func TestCacheMigrationGenerationAllowsArchiveAfterCatchUp(t *testing.T) {
	ctx := context.Background()
	db := openArchiveTestDB(t)
	hash := canonicalArchiveTestHash("audit-legacy-cache")
	archiveTestExec(t, db, `insert into usage_events (
        event_hash, timestamp_ms, timestamp, provider, executor_type, model,
        input_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens, total_tokens, created_at_ms
    ) values (?, 1000, '1970-01-01T00:00:01Z', 'anthropic', '', 'audit-cache-model', 100, 30, 20, 10, 0, 1000)`, hash)
	catchUpHourlyAggregate(t, ctx, db, 10000)
	archiveTestExec(t, db, `update usage_data_migrations set
        status = 'discovering', last_event_id = 0, target_event_id = 0,
        processed_rows = 0, changed_rows = 0, applied_rows = 0, started_at_ms = null,
        updated_at_ms = 0, finished_at_ms = null, last_error = null where name = ?`, datamigration.UsageCacheAccountingMigrationName)
	migration := datamigration.New(db)
	if _, err := migration.DiscoverUsageCacheAccounting(ctx); err != nil {
		t.Fatal(err)
	}
	var completed datamigration.BatchResult
	for i := 0; i < 30; i++ {
		result, err := migration.RunUsageCacheAccountingBatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if result.Completed {
			completed = result
			break
		}
	}
	if !completed.Completed || completed.State.ChangedRows != 1 || completed.State.AppliedRows != 1 {
		t.Fatalf("migration result=%#v", completed)
	}
	catchUpHourlyAggregate(t, ctx, db, 20000)
	catchUpDeleteReadiness(t, ctx, db, 21000)
	hourly := usageaggregate.New(db)
	state, err := hourly.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "ready" || state.CoverageEventID != 1 || state.TargetEventID != 1 || !strings.Contains(state.StructureRevision, ":cache-accounting-v2-") {
		t.Fatalf("unexpected hourly state=%#v", state)
	}
	if !usageaggregate.IsCurrentStructureRevision(state.StructureRevision) {
		t.Fatal("cache revision rejected")
	}
	// An idle catch-up must preserve the exact ledger/state generation.
	if _, err := hourly.CatchUp(ctx, 10, 22000); err != nil {
		t.Fatal(err)
	}
	after, err := hourly.State(ctx)
	if err != nil || after.StructureRevision != state.StructureRevision {
		t.Fatalf("revision changed on retry=%#v err=%v", after, err)
	}
	var ledgerRevision string
	var inputTokens int64
	if err := db.QueryRow(`select aggregate_structure_revision from usage_event_identity_ledger where event_hash = ?`, hash).Scan(&ledgerRevision); err != nil {
		t.Fatal(err)
	}
	if ledgerRevision != state.StructureRevision {
		t.Fatalf("ledger revision=%q hourly=%q", ledgerRevision, state.StructureRevision)
	}
	if err := db.QueryRow(`select normalized_total_input_tokens from usage_events where event_hash = ?`, hash).Scan(&inputTokens); err != nil {
		t.Fatal(err)
	}
	if inputTokens != 130 {
		t.Fatalf("corrected input=%d", inputTokens)
	}
	archive := New(db)
	run := auditArchiveOnly(t, ctx, archive, "audit-cache-revision", 2000)
	if _, err := archive.BeginVerification(ctx, run.ID, 30000); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.MarkVerified(ctx, run.ID, 31000); err != nil {
		t.Fatal(err)
	}
	tx, err := sqlwrite.BeginTx(ctx, db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = validateCurrentDeleteReadiness(ctx, tx, run)
	_ = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = archive.BeginDelete(ctx, run.ID, 32000); err != nil {
		t.Fatal(err)
	}
	deleted, err := archive.DeleteBatch(ctx, run.ID, 10, 33000)
	if err != nil || deleted.Deleted != 1 {
		t.Fatalf("delete=%#v err=%v", deleted, err)
	}

}

func TestMonitoringRetainedHistoryAfterPriceChange(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		deleteHighest, structural bool
		wantCalls, wantCoverage   int64
	}{
		{name: "archived_below_watermark_structural", structural: true, wantCalls: 2, wantCoverage: 2},
		{name: "archived_above_watermark_structural_control", deleteHighest: true, structural: true, wantCalls: 2, wantCoverage: 2},
		{name: "numeric_rate_only_control", wantCalls: 2, wantCoverage: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := openArchiveTestDB(t)
			dayStart := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
			dayEnd := dayStart + 86400000
			firstTime, secondTime := dayStart+3600000, dayStart+7200000
			if tc.deleteHighest {
				firstTime, secondTime = secondTime, firstTime
			}
			price := model.ModelPrice{Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 100, Prompt: 2, PromptConfigured: true}}}
			prices := modelprice.New(db)
			if err := prices.ReplaceAll(ctx, map[string]model.ModelPrice{"audit-model": price}); err != nil {
				t.Fatal(err)
			}
			events := []usage.Event{auditOrdinaryEvent("audit-first", firstTime, 150), auditOrdinaryEvent("audit-second", secondTime, 250)}
			if _, err := usageevent.New(db).InsertBatch(ctx, events); err != nil {
				t.Fatal(err)
			}
			catchUpHourlyAggregate(t, ctx, db, 10000)
			catchUpDeleteReadiness(t, ctx, db, 11000)
			mon := usagemonitoring.New(db)
			beforeState, err := mon.State(ctx, usagemonitoring.StatsRollupName)
			if err != nil {
				t.Fatal(err)
			}
			full := usagemonitoring.AnalyticsFilter{FromMS: dayStart, ToMS: dayEnd, IncludeFailed: true}
			auditAssertMonitoringCalls(t, ctx, mon, full, 2)
			archive := New(db)
			run := auditArchiveOnly(t, ctx, archive, "audit-retained-price", dayStart+5400000)
			if run.EventCount != 1 {
				t.Fatalf("archive event count=%d", run.EventCount)
			}
			if _, err := archive.BeginVerification(ctx, run.ID, 30000); err != nil {
				t.Fatal(err)
			}
			if _, err := archive.MarkVerified(ctx, run.ID, 31000); err != nil {
				t.Fatal(err)
			}
			if _, err := archive.BeginDelete(ctx, run.ID, 32000); err != nil {
				t.Fatal(err)
			}
			deleted, err := archive.DeleteBatch(ctx, run.ID, 10, 33000)
			if err != nil || deleted.Deleted != 1 {
				t.Fatalf("delete result=%#v err=%v", deleted, err)
			}
			var retained, raw int64
			if err := db.QueryRow(`select count(*) from usage_monitoring_event_projection_v1`).Scan(&retained); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`select count(*) from usage_events`).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if retained != 2 || raw != 1 {
				t.Fatalf("retained=%d raw=%d", retained, raw)
			}
			auditAssertMonitoringCalls(t, ctx, mon, full, 2)
			if tc.structural {
				price.ContextTiers[0].ThresholdTokens = 200
			} else {
				price.Prompt = 9
			}
			// Use the real guarded public price repository, not direct fixture mutation.
			if err := prices.ReplaceAll(ctx, map[string]model.ModelPrice{"audit-model": price}); err != nil {
				t.Fatalf("guard rejected intact retained history: %v", err)
			}
			// Before rebuilding daily stats, revision mismatch takes the intact projection path.
			auditAssertMonitoringCalls(t, ctx, mon, full, 2)
			pr := usagepricing.New(db)
			for i := 0; i < 20; i++ {
				result, err := pr.CatchUp(ctx, 1, 40000+int64(i))
				if err != nil {
					t.Fatal(err)
				}
				if !result.Pending {
					break
				}
				if i == 19 {
					t.Fatal("pricing remains pending")
				}
			}
			for i := 0; i < 20; i++ {
				result, err := mon.CatchUpStats(ctx, 1, 50000+int64(i))
				if err != nil {
					t.Fatal(err)
				}
				if !result.Pending {
					break
				}
				if i == 19 {
					t.Fatal("monitoring remains pending")
				}
			}
			state, err := mon.State(ctx, usagemonitoring.StatsRollupName)
			if err != nil {
				t.Fatal(err)
			}
			if state.Status != "ready" || state.CoverageEventID != tc.wantCoverage || state.TargetEventID != tc.wantCoverage {
				t.Fatalf("stats state=%#v", state)
			}
			if (beforeState.StructureRevision != state.StructureRevision) != tc.structural {
				t.Fatalf("revision behavior before=%q after=%q", beforeState.StructureRevision, state.StructureRevision)
			}
			auditAssertMonitoringCalls(t, ctx, mon, full, tc.wantCalls)
			// Both records remain visible via the edge-only projection reader.
			partial := full
			partial.FromMS++
			partial.ToMS--
			auditAssertMonitoringCalls(t, ctx, mon, partial, 2)
			hourlyRows, _, available, err := usageaggregate.New(db).LoadRows(ctx, usageaggregate.Filter{FromMS: dayStart, ToMS: dayEnd, IncludeFailed: true})
			var hourlyCalls int64
			for _, row := range hourlyRows {
				hourlyCalls += row.Calls
			}
			if err != nil || !available || hourlyCalls != 2 {
				t.Fatalf("hourly calls=%d available=%v err=%v", hourlyCalls, available, err)
			}
			pricingRows, _, available, err := pr.LoadHourlyRows(ctx, usagepricing.HourlyFilter{FromMS: dayStart, ToMS: dayEnd, IncludeFailed: true})
			var pricingCalls int64
			for _, row := range pricingRows {
				pricingCalls += row.Calls
			}
			if err != nil || !available || pricingCalls != 2 {
				t.Fatalf("pricing calls=%d available=%v err=%v", pricingCalls, available, err)
			}
			if tc.deleteHighest {
				// Advancing past a retained high ID must not drop its rebuilt totals.
				if _, err := usageevent.New(db).InsertBatch(ctx, []usage.Event{auditOrdinaryEvent("audit-third", dayStart+10800000, 350)}); err != nil {
					t.Fatal(err)
				}
				if result, err := mon.CatchUpProjection(ctx, 10, 60000); err != nil || result.Pending || result.CoverageEventID != 3 {
					t.Fatalf("projection=%#v err=%v", result, err)
				}
				if result, err := mon.CatchUpStats(ctx, 10, 61000); err != nil || result.Pending || result.CoverageEventID != 3 {
					t.Fatalf("stats=%#v err=%v", result, err)
				}
				auditAssertMonitoringCalls(t, ctx, mon, full, 3)
				auditAssertMonitoringCalls(t, ctx, mon, partial, 3)
			}
		})
	}
}

func TestPricingRevalidatesAlreadyCoveredRetainedHistoryEachBatch(t *testing.T) {
	ctx := context.Background()
	db := openArchiveTestDB(t)
	price := model.ModelPrice{Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 100, Prompt: 2, PromptConfigured: true}}}
	prices := modelprice.New(db)
	if err := prices.ReplaceAll(ctx, map[string]model.ModelPrice{"audit-model": price}); err != nil {
		t.Fatal(err)
	}
	if _, err := usageevent.New(db).InsertBatch(ctx, []usage.Event{auditOrdinaryEvent("revalidation-old", 1000, 150), auditOrdinaryEvent("revalidation-live", 3000, 250)}); err != nil {
		t.Fatal(err)
	}
	catchUpHourlyAggregate(t, ctx, db, 10000)
	catchUpDeleteReadiness(t, ctx, db, 11000)
	archive := New(db)
	run := auditArchiveOnly(t, ctx, archive, "audit-revalidation", 2000)
	if _, err := archive.BeginVerification(ctx, run.ID, 30000); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.MarkVerified(ctx, run.ID, 31000); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.BeginDelete(ctx, run.ID, 32000); err != nil {
		t.Fatal(err)
	}
	if result, err := archive.DeleteBatch(ctx, run.ID, 10, 33000); err != nil || result.Deleted != 1 {
		t.Fatalf("delete=%#v err=%v", result, err)
	}
	price.ContextTiers[0].ThresholdTokens = 200
	if err := prices.ReplaceAll(ctx, map[string]model.ModelPrice{"audit-model": price}); err != nil {
		t.Fatal(err)
	}
	pr := usagepricing.New(db)
	first, err := pr.CatchUp(ctx, 1, 40000)
	if err != nil || !first.Rebuilt || !first.Pending || first.CoverageEventID != 1 || first.TargetEventID != 2 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	// Deliberately corrupt only this throwaway fixture, below the saved cursor.
	// A proof bounded to the next batch would not visit archived event 1.
	archiveTestExec(t, db, `delete from usage_monitoring_event_projection_v1 where event_id = 1`)
	_, secondErr := pr.CatchUp(ctx, 1, 41000)
	if !errors.Is(secondErr, usagepricing.ErrRetainedPricingHistoryIncomplete) {
		t.Fatalf("second batch err=%v", secondErr)
	}
	state, err := pr.State(ctx)
	if err != nil || state.CoverageEventID != 1 || state.TargetEventID != 2 {
		t.Fatalf("state after failed proof=%#v err=%v", state, err)
	}
}

func auditOrdinaryEvent(hash string, timestampMS, input int64) usage.Event {
	return usage.Event{EventHash: canonicalArchiveTestHash(hash), TimestampMS: timestampMS, Timestamp: time.UnixMilli(timestampMS).UTC().Format(time.RFC3339Nano), Provider: "openai", AuthProviderSnapshot: "openai", Model: "audit-model", AccountSnapshot: "audit-account", AuthFileSnapshot: "audit-account.json", AuthIndex: "audit-auth", APIKeyHash: "audit-api-key", InputTokens: input, OutputTokens: 10, TotalTokens: input + 10, CreatedAtMS: timestampMS}
}

func auditAssertMonitoringCalls(t *testing.T, ctx context.Context, mon usagemonitoring.Repository, filter usagemonitoring.AnalyticsFilter, want int64) {
	t.Helper()
	account, _, available, err := mon.LoadAccountStats(ctx, filter)
	var accountCalls int64
	for _, row := range account {
		accountCalls += row.Calls
	}
	if err != nil || !available || accountCalls != want {
		t.Fatalf("account calls=%d want=%d available=%v err=%v rows=%#v", accountCalls, want, available, err, account)
	}
	api, _, available, err := mon.LoadAPIKeyStats(ctx, filter)
	var apiCalls int64
	for _, row := range api {
		apiCalls += row.Calls
	}
	if err != nil || !available || apiCalls != want {
		t.Fatalf("api calls=%d want=%d available=%v err=%v rows=%#v", apiCalls, want, available, err, api)
	}
}

func auditArchiveOnly(t *testing.T, ctx context.Context, repo *Repository, name string, cutoff int64) Run {
	t.Helper()
	run, err := repo.CreateRun(ctx, name, cutoff, 25000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginArchive(ctx, run.ID, 25001); err != nil {
		t.Fatal(err)
	}
	records, err := repo.Records(ctx, run.ID, 0, 100, 1<<30)
	if err != nil || len(records) == 0 {
		t.Fatalf("records=%d err=%v", len(records), err)
	}
	if _, err := repo.RecordSegment(ctx, run.ID, archiveTestSegment(run.ID, records), archiveRecordRefs(records), 25002); err != nil {
		t.Fatal(err)
	}
	run, err = repo.MarkArchived(ctx, run.ID, "audit-digest", name+"/manifest.json", "audit-manifest-digest", 25003)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

package usagemonitoring

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

// SQLite installs test-only counterparts to exercise composition without claiming
// native execution. Production SQLite never installs or opts into hourly tables.
func hourlyFixture(t *testing.T) (*sql.DB, *repository) {
	t.Helper()
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "hourly.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	repo := New(db, WithMySQLHourly(true)).(*repository)
	if mysql.IsMySQL(db) {
		return db, repo
	}
	if repo.mysqlHourly {
		t.Fatal("public SQLite option must be no-op")
	}
	for _, kind := range []string{"account", "api_key", "selector"} {
		daily := "usage_monitoring_" + kind + "_daily_rollups_v1"
		hourly := strings.Replace(daily, "_daily_", "_hourly_", 1)
		var ddl string
		if err := db.QueryRow(`select sql from sqlite_master where type='table' and name=?`, daily).Scan(&ddl); err != nil {
			t.Fatal(err)
		}
		ddl = strings.ReplaceAll(ddl, daily, hourly)
		if kind == "selector" {
			ddl = strings.Replace(ddl, "primary key (", "primary key (model_format_revision,", 1)
		}
		hourlyExec(t, db, ddl)
		for _, column := range []string{"_revision_hash", "_api_key_hash", "_credential_hash", "_legacy_hash"} {
			hourlyExec(t, db, `alter table `+hourly+` add column `+column+` blob`)
		}
	}
	for _, name := range []string{HourlyStatsRollupName, HourlyMetadataRollupName} {
		hourlyExec(t, db, `insert into usage_monitoring_rollup_state(rollup_name,schema_version,structure_revision,status) values (?,1,'','pending')`, name)
	}
	repo.mysqlHourly = true // Exercise internals only, never bypass public runtime opt-in.
	return db, repo
}

func hourlyExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlwrite.ExecContext(context.Background(), db, query, args...); err != nil {
		t.Fatal(err)
	}
}

func fillFixtureDigests(t *testing.T, db *sql.DB) {
	if mysql.IsMySQL(db) {
		return
	}
	revision, err := CurrentStatsStructureRevision(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"account", "api_key"} {
		table := "usage_monitoring_" + kind + "_hourly_rollups_v1"
		rows, err := db.Query(`select rowid,api_key_hash,trim(auth_file_snapshot),trim(auth_index),trim(source) from ` + table)
		if err != nil {
			t.Fatal(err)
		}
		type digestRow struct {
			id                       int64
			key, file, index, source string
		}
		var values []digestRow
		for rows.Next() {
			var row digestRow
			if err := rows.Scan(&row.id, &row.key, &row.file, &row.index, &row.source); err != nil {
				t.Fatal(err)
			}
			values = append(values, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		for _, row := range values {
			hourlyExec(t, db, `update `+table+` set _revision_hash=?,_api_key_hash=?,_credential_hash=?,_legacy_hash=? where rowid=?`, revisionDigest(revision), revisionDigest(row.key), framedScopeDigest(row.file, row.index), framedScopeDigest(row.source, row.index), row.id)
		}
	}
	hourlyExec(t, db, `update usage_monitoring_selector_hourly_rollups_v1 set _revision_hash=?`, revisionDigest(usageidentity.ModelFormatVersion))
}

func TestHourlyCheckpointCompositionEdgesLateIDsAndRestart(t *testing.T) {
	db, repo := hourlyFixture(t)
	ctx := context.Background()
	events := usageevent.New(db)
	base := int64(1_800_057_600_000)
	base = floorDayMS(base)
	var batch []usage.Event
	for i := 0; i < 80; i++ {
		latency := int64(i + 1)
		batch = append(batch, usage.Event{EventHash: fmt.Sprintf("%064x", i+1), TimestampMS: base + int64(i)*hourMS + 17*60_000, Timestamp: "synthetic", Model: "model", Provider: "openai", AuthProviderSnapshot: "openai", AuthIndex: "idx", AuthFileSnapshot: "credential.json", Source: "source", AccountSnapshot: "account", APIKeyHash: "key", Failed: i%2 == 0, InputTokens: int64(100 + i), TotalTokens: int64(100 + i), LatencyMS: &latency, CreatedAtMS: 1})
	}
	if _, err := events.InsertBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	catch := func(fn func(context.Context, int, int64) (CatchUpResult, error)) {
		t.Helper()
		for i := 0; i < 100; i++ {
			result, err := fn(ctx, 7, 1000+int64(i))
			if err != nil {
				t.Fatal(err)
			}
			if !result.Pending {
				return
			}
		}
		t.Fatal("backfill never completed")
	}
	catch(repo.CatchUpProjection)
	catch(repo.CatchUpStats)
	catch(repo.CatchUpMetadata)
	// Independent checkpoints start at zero, despite completed daily coverage.
	for _, name := range []string{HourlyStatsRollupName, HourlyMetadataRollupName} {
		state, err := repo.State(ctx, name)
		if err != nil || state.CoverageEventID != 0 {
			t.Fatalf("inherited watermark=%#v err=%v", state, err)
		}
	}
	first, err := repo.CatchUpHourlyStats(ctx, 3, 2000)
	if err != nil || !first.Pending {
		t.Fatal(first, err)
	}
	restarted := New(db).(*repository)
	restarted.mysqlHourly = true
	catch(restarted.CatchUpHourlyStats)
	catch(restarted.CatchUpHourlyMetadata)
	fillFixtureDigests(t, db)
	// Late arrival has a new ID but falls into an already-covered hour.
	late := batch[3]
	late.EventHash = fmt.Sprintf("%064x", 1000)
	if _, err := events.InsertBatch(ctx, []usage.Event{late}); err != nil {
		t.Fatal(err)
	}
	reference := New(db).(*repository)
	for _, span := range [][2]int64{{base + 13*60_000, base + 24*hourMS + 29*60_000}, {base + 13*60_000, base + 78*hourMS + 29*60_000}, {base + hourMS, base + 2*hourMS}} {
		for _, outcome := range []int{0, 1, 2} {
			filter := AnalyticsFilter{FromMS: span[0], ToMS: span[1], IncludeFailed: outcome != 1, FailedOnly: outcome == 2}
			if outcome == 2 {
				filter.APIKeyHashes = []string{usage.NormalizeOpaqueHashForPersistence("key")}
			}
			totals, _, ok, err := restarted.LoadAggregate(ctx, filter)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			totalsWant, _, _, err := reference.LoadAggregate(ctx, filter)
			if err != nil || !reflect.DeepEqual(totals, totalsWant) {
				t.Fatal("core totals", totals, totalsWant, err)
			}
			models, _, ok, err := restarted.LoadModelStats(ctx, filter)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			modelsWant, _, _, err := reference.LoadModelStats(ctx, filter)
			if err != nil || !reflect.DeepEqual(models, modelsWant) {
				t.Fatal("core models", models, modelsWant, err)
			}
			got, _, ok, err := restarted.LoadAccountStats(ctx, filter)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			want, _, _, err := reference.LoadAccountStats(ctx, filter)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("account got=%#v want=%#v err=%v", got, want, err)
			}
			api, _, ok, err := restarted.LoadAPIKeyStats(ctx, filter)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			apiWant, _, _, err := reference.LoadAPIKeyStats(ctx, filter)
			if err != nil || !reflect.DeepEqual(api, apiWant) {
				t.Fatalf("API got=%#v want=%#v err=%v", api, apiWant, err)
			}
			if outcome == 0 {
				selectors, _, ok, err := restarted.LoadFilterSelectors(ctx, filter)
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				selectorsWant, _, _, err := reference.LoadFilterSelectors(ctx, filter)
				if err != nil || !reflect.DeepEqual(selectors, selectorsWant) {
					t.Fatal("selectors", selectors, selectorsWant, err)
				}
			}
		}
	}
	// Credential windows use hourly fringes without dropping identity residuals.
	windows := []AccountWindowUsageQuery{{RequestIndex: 1, FromMS: base + 13*60_000, ToMS: base + 29*hourMS, AuthFileSnapshot: "credential.json", AuthIndex: "idx", AuthProviderSnapshot: "openai", AccountSnapshot: "account"}}
	got, _, ok, err := restarted.LoadAccountWindowStats(ctx, windows)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	want, _, _, err := reference.LoadAccountWindowStats(ctx, windows)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("windows", got, want, err)
	}
	// A -> B -> A revisions clear the destination generation before additive replay.
	prices := modelprice.New(db)
	price := model.ModelPrice{Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 120, Prompt: 2, PromptConfigured: true}}}
	for _, threshold := range []int64{120, 150, 120} {
		price.ContextTiers[0].ThresholdTokens = threshold
		if err := prices.ReplaceAll(ctx, map[string]model.ModelPrice{"model": price}); err != nil {
			t.Fatal(err)
		}
		catch(restarted.CatchUpHourlyStats)
		fillFixtureDigests(t, db)
		filter := AnalyticsFilter{FromMS: base, ToMS: base + 80*hourMS, IncludeFailed: true}
		got, _, ok, err := restarted.LoadAccountStats(ctx, filter)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		var calls int64
		for _, row := range got {
			calls += row.Calls
		}
		if calls != 81 {
			t.Fatalf("revision replay calls=%d want=81", calls)
		}
	}
}

func TestStatsOutcomeGrainAndBuildRevision(t *testing.T) {
	db, repo := hourlyFixture(t)
	ctx := context.Background()
	batch := []usage.Event{}
	for i := 0; i < 20; i++ {
		batch = append(batch, usage.Event{EventHash: fmt.Sprintf("%064x", i+1), TimestampMS: 1000, Timestamp: "synthetic", Model: "model", Failed: i%2 == 0, InputTokens: 1, TotalTokens: 1, CreatedAtMS: 1})
	}
	if _, err := usageevent.New(db).InsertBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func(context.Context, int, int64) (CatchUpResult, error){repo.CatchUpProjection, repo.CatchUpStats, repo.CatchUpHourlyStats} {
		for i := 0; i < 10; i++ {
			result, err := fn(ctx, 100, 1000+int64(i))
			if err != nil {
				t.Fatal(err)
			}
			if !result.Pending {
				break
			}
		}
	}
	for _, table := range []string{"usage_monitoring_account_daily_rollups_v1", "usage_monitoring_api_key_daily_rollups_v1", "usage_monitoring_account_hourly_rollups_v1", "usage_monitoring_api_key_hourly_rollups_v1"} {
		for _, failed := range []int{0, 1} {
			var calls int64
			if err := db.QueryRow(`select sum(calls) from `+table+` where failed=?`, failed).Scan(&calls); err != nil || calls != 10 {
				t.Fatalf("%s failed=%d calls=%d err=%v", table, failed, calls, err)
			}
		}
	}
	state, err := repo.State(ctx, StatsRollupName)
	if err != nil || !strings.HasPrefix(state.StructureRevision, StatsBuildVersion+":") {
		t.Fatal(state, err)
	}
}

func TestHourlyRebuildAllRawDeletedAndRetainedProofRollback(t *testing.T) {
	db, repo := hourlyFixture(t)
	ctx := context.Background()
	events := []usage.Event{{EventHash: fmt.Sprintf("%064x", 1), TimestampMS: 1000, Timestamp: "synthetic", Model: "model", Provider: "openai", AuthProviderSnapshot: "openai", APIKeyHash: "key", InputTokens: 150, TotalTokens: 150, CreatedAtMS: 1}, {EventHash: fmt.Sprintf("%064x", 2), TimestampMS: hourMS + 1000, Timestamp: "synthetic", Model: "model", Provider: "openai", AuthProviderSnapshot: "openai", APIKeyHash: "key", Failed: true, InputTokens: 250, TotalTokens: 250, CreatedAtMS: 1}}
	if _, err := usageevent.New(db).InsertBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CatchUpProjection(ctx, 100, 1000); err != nil {
		t.Fatal(err)
	}
	// Valid deletion evidence with original IDs. The archive package separately
	// exercises the real publication/verification/delete lifecycle.
	hourlyExec(t, db, `insert into usage_archive_runs(id,schema_version,format,status,cutoff_timestamp_ms,target_event_id,event_count,created_at_ms,updated_at_ms) values ('retained',1,'gzip-jsonl-v1','completed',10000000,2,2,1,1)`)
	hourlyExec(t, db, `insert into usage_archive_segments(run_id,sequence,status,file_name,first_event_id,last_event_id,min_timestamp_ms,max_timestamp_ms,event_count,uncompressed_bytes,compressed_bytes,content_sha256,event_hash_digest,created_at_ms) values ('retained',1,'verified','retained/one',1,2,1000,3601000,2,1,1,'content','hashes',1)`)
	hourlyExec(t, db, `insert into usage_archive_event_refs(event_hash,run_id,segment_sequence,raw_event_id,timestamp_ms,archived_at_ms,raw_deleted_at_ms) select event_hash,'retained',1,id,timestamp_ms,1,2 from usage_events`)
	hourlyExec(t, db, `delete from usage_events`)
	hourlyExec(t, db, `update usage_event_identity_ledger set raw_event_id=null`)
	price := model.ModelPrice{Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 200, Prompt: 2, PromptConfigured: true}}}
	if err := modelprice.New(db).ReplaceAll(ctx, map[string]model.ModelPrice{"model": price}); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func(context.Context, int, int64) (CatchUpResult, error){repo.CatchUpStats, repo.CatchUpHourlyStats, repo.CatchUpHourlyMetadata} {
		for i := 0; i < 10; i++ {
			result, err := fn(ctx, 1, 2000+int64(i))
			if err != nil {
				t.Fatal(err)
			}
			if !result.Pending {
				if result.TargetEventID != 2 || result.CoverageEventID != 2 {
					t.Fatal(result)
				}
				break
			}
		}
	}
	fillFixtureDigests(t, db)
	filter := AnalyticsFilter{FromMS: 0, ToMS: 2 * hourMS, IncludeFailed: true}
	rows, _, ok, err := repo.LoadAPIKeyStats(ctx, filter)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	var calls int64
	for _, row := range rows {
		calls += row.Calls
	}
	if calls != 2 {
		t.Fatalf("all raw deleted calls=%d", calls)
	}
	price.ContextTiers[0].ThresholdTokens = 100
	if err := modelprice.New(db).ReplaceAll(ctx, map[string]model.ModelPrice{"model": price}); err != nil {
		t.Fatal(err)
	}
	first, err := repo.CatchUpHourlyStats(ctx, 1, 3000)
	if err != nil || first.CoverageEventID != 1 {
		t.Fatal(first, err)
	}
	hourlyExec(t, db, `delete from usage_monitoring_event_projection_v1 where event_id=1`)
	_, err = repo.CatchUpHourlyStats(ctx, 1, 3001)
	if !errors.Is(err, usageprojection.ErrRetainedCoverageIncomplete) {
		t.Fatalf("corrupt retained proof error=%v", err)
	}
	state, err := repo.State(ctx, HourlyStatsRollupName)
	if err != nil || state.CoverageEventID != 1 {
		t.Fatal(state, err)
	}
}

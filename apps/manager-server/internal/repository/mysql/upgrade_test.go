package mysql_test

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

// These tests deliberately use a native administrative pool and isolated schemas.
// No server is provisioned; without CPA_TEST_MYSQL_DSN every test skips.
//
//go:embed schema.sql
var frozenV1Schema string

//go:embed migrations/002_monitoring_hourly.sql
var versionTwoSchema string

func nativeVersionOne(t *testing.T) (string, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	dsn := testdb.DSN(t, t.TempDir())
	db, err := mysqlrepo.OpenNative(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, part := range strings.Split(frozenV1Schema, "-- statement")[1:] {
		if _, err := db.ExecContext(ctx, strings.TrimSpace(part)); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"create table manager_schema_version(id bigint primary key,version bigint not null) engine=InnoDB",
		"insert into manager_schema_version values(1,1)",
		"insert into settings(`key`,value,updated_at_ms) values('preserved','encrypted-value',1)",
		"insert into usage_events(id,event_hash,timestamp_ms,timestamp,model,created_at_ms) values(7000,'archived',1,'time','model',1)",
		"insert into usage_event_identity_ledger(event_hash,raw_event_id,timestamp_ms,bucket_ms,first_seen_at_ms,updated_at_ms) values('archived',null,1,0,1,1)",
		"insert into usage_archive_runs(id,schema_version,format,status,cutoff_timestamp_ms,target_event_id,event_count,created_at_ms,updated_at_ms) values('archive',1,'jsonl','completed',2,7000,1,1,1)",
		"insert into usage_archive_segments(run_id,sequence,status,file_name,first_event_id,last_event_id,min_timestamp_ms,max_timestamp_ms,event_count,uncompressed_bytes,compressed_bytes,content_sha256,event_hash_digest,created_at_ms) values('archive',1,'verified','segment.gz',7000,7000,1,1,1,10,8,'content','hashes',1)",
		"insert into usage_archive_event_refs(event_hash,run_id,segment_sequence,raw_event_id,timestamp_ms,archived_at_ms,raw_deleted_at_ms) values('archived','archive',1,7000,1,2,3)",
		"delete from usage_events",
		"update usage_monitoring_rollup_state set status='complete',structure_revision='historical',coverage_event_id=7000,backfill_last_event_id=7000 where rollup_name='stats_v1'",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("v1 fixture: %v", err)
		}
	}
	return dsn, db
}

func physicalLayout(t *testing.T, db *sql.DB) [][]string {
	t.Helper()
	var result [][]string
	for _, q := range []string{
		`select table_name,column_name,column_type,is_nullable,coalesce(column_default,'<NULL>'),extra,generation_expression,coalesce(collation_name,'') from information_schema.columns where table_schema=database() order by table_name,ordinal_position`,
		`select table_name,index_name,cast(non_unique as char),cast(seq_in_index as char),coalesce(column_name,''),coalesce(cast(sub_part as char),''),coalesce(collation,''),index_type,is_visible,coalesce(expression,'') from information_schema.statistics where table_schema=database() order by table_name,index_name,seq_in_index`,
	} {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]string, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			result = append(result, values)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}
	return result
}

func TestMySQLPopulatedV1UpgradeRestartAndFreshEquivalence(t *testing.T) {
	dsn, native := nativeVersionOne(t)
	ctx := context.Background()
	// Simulate a crash after the first auto-committed DDL, before seeds/marker.
	first := strings.TrimSpace(strings.Split(versionTwoSchema, "-- statement")[1])
	if _, err := native.ExecContext(ctx, first); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		db, err := mysqlrepo.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	var version int
	if err := native.QueryRow("select version from manager_schema_version where id=1").Scan(&version); err != nil || version != 2 {
		t.Fatalf("schema marker %d %v", version, err)
	}
	for _, name := range []string{"stats_hourly_v1", "metadata_hourly_v1"} {
		var status, revision string
		var coverage int64
		if err := native.QueryRow("select status,structure_revision,coverage_event_id from usage_monitoring_rollup_state where rollup_name=?", name).Scan(&status, &revision, &coverage); err != nil || status != "pending" || revision != "" || coverage != 0 {
			t.Fatalf("inherited readiness %s %s %s %d %v", name, status, revision, coverage, err)
		}
	}
	var value string
	if err := native.QueryRow("select value from settings where `key`='preserved'").Scan(&value); err != nil || value != "encrypted-value" {
		t.Fatalf("data changed: %s %v", value, err)
	}
	fresh := openMySQL(t)
	if !reflect.DeepEqual(physicalLayout(t, native), physicalLayout(t, fresh)) {
		t.Fatal("fresh and upgraded layouts differ")
	}
	// Progress must survive another open; marker alone cannot conceal index drift.
	if _, err := native.Exec("update usage_monitoring_rollup_state set status='ready',structure_revision='hourly-generation',coverage_event_id=7000 where rollup_name='stats_hourly_v1'"); err != nil {
		t.Fatal(err)
	}
	db, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	var coverage int64
	if err := native.QueryRow("select coverage_event_id from usage_monitoring_rollup_state where rollup_name='stats_hourly_v1'").Scan(&coverage); err != nil || coverage != 7000 {
		t.Fatalf("progress reset %d %v", coverage, err)
	}
	if _, err := native.Exec("alter table usage_monitoring_account_hourly_rollups_v1 drop index idx_hourly_revision_bucket, add index idx_hourly_revision_bucket(bucket_ms)"); err != nil {
		t.Fatal(err)
	}
	if db, err := mysqlrepo.Open(ctx, dsn); err == nil {
		db.Close()
		t.Fatal("same-named incompatible index accepted")
	}
}

func TestMySQLV1BackupMapsIntoV2AndPreservesArchiveSequence(t *testing.T) {
	dsn, native := nativeVersionOne(t)
	ctx := context.Background()
	var backup bytes.Buffer
	if err := mysqlrepo.WriteBackup(ctx, native, &backup); err != nil {
		t.Fatal(err)
	}
	runtimeDB, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	runtimeDB.Close()
	for _, q := range []string{
		"update settings set value='changed' where `key`='preserved'",
		"update usage_monitoring_rollup_state set status='ready',structure_revision='stale',coverage_event_id=99999 where rollup_name in ('stats_hourly_v1','metadata_hourly_v1')",
		"insert into usage_monitoring_selector_hourly_rollups_v1(model_format_revision,bucket_ms,model,api_key_hash,provider,auth_file_snapshot,account_snapshot,auth_label_snapshot,auth_index,source,source_hash,updated_at_ms) values('stale',0,'m','','','','','','','','',0)",
	} {
		if _, err := native.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for repeat := 0; repeat < 2; repeat++ {
		restore, err := mysqlrepo.BeginRestore(ctx, native, bytes.NewReader(backup.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		if err := restore.Tx.Commit(); err != nil {
			restore.Close()
			t.Fatal(err)
		}
		restore.Close()
	}
	var version, rows, states int
	if err := native.QueryRow("select version from manager_schema_version where id=1").Scan(&version); err != nil || version != 2 {
		t.Fatalf("downgraded marker %d %v", version, err)
	}
	if err := native.QueryRow("select count(*) from usage_monitoring_selector_hourly_rollups_v1").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("stale acceleration %d %v", rows, err)
	}
	if err := native.QueryRow("select count(*) from usage_monitoring_rollup_state where rollup_name in ('stats_hourly_v1','metadata_hourly_v1') and status='pending' and structure_revision='' and coverage_event_id=0 and backfill_last_event_id=0 and target_event_id=0 and processed_events=0").Scan(&states); err != nil || states != 2 {
		t.Fatalf("bad seeds %d %v", states, err)
	}
	var id int64
	var deleted sql.NullInt64
	if err := native.QueryRow("select raw_event_id,raw_deleted_at_ms from usage_archive_event_refs where event_hash='archived'").Scan(&id, &deleted); err != nil || id != 7000 || !deleted.Valid {
		t.Fatalf("archive evidence lost %d %+v %v", id, deleted, err)
	}
	var ledger int
	if err := native.QueryRow("select count(*) from usage_event_identity_ledger where event_hash='archived' and raw_event_id is null").Scan(&ledger); err != nil || ledger != 1 {
		t.Fatalf("ledger lost %d %v", ledger, err)
	}
	result, err := native.Exec("insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms) values('after-restore',4,'time','m',4)")
	if err != nil {
		t.Fatal(err)
	}
	id, err = result.LastInsertId()
	if err != nil || id <= 7000 {
		t.Fatalf("reused archived id %d %v", id, err)
	}
}

func TestMySQLV1StaleSequenceRestoreIntoFreshV2(t *testing.T) {
	_, source := nativeVersionOne(t)
	ctx := context.Background()
	var backup bytes.Buffer
	if err := mysqlrepo.WriteBackup(ctx, source, &backup); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(backup.Bytes()))
	var header mysqlrepo.BackupHeader
	if err := dec.Decode(&header); err != nil {
		t.Fatal(err)
	}
	for i := range header.Tables {
		if header.Tables[i].Name == "usage_events" {
			header.Tables[i].NextID = 1 // cached value from the original v1 writer
		}
	}
	var stale bytes.Buffer
	if err := json.NewEncoder(&stale).Encode(header); err != nil {
		t.Fatal(err)
	}
	stale.Write(backup.Bytes()[dec.InputOffset():])
	dsn := testdb.DSN(t, t.TempDir())
	initialized, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	initialized.Close()
	target, err := mysqlrepo.OpenNative(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	restore, err := mysqlrepo.BeginRestore(ctx, target, &stale)
	if err != nil {
		t.Fatal(err)
	}
	defer restore.Close()
	if err := restore.Tx.Commit(); err != nil {
		t.Fatal(err)
	}
	result, err := target.ExecContext(ctx, "insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms) values('after-stale-restore',4,'time','model',4)")
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil || id <= 7000 {
		t.Fatalf("legacy cached sequence reused retained history ID: %d %v", id, err)
	}
}

func TestMySQLHourlyDigestsUseFullUnicodeGrain(t *testing.T) {
	db := openMySQL(t)
	ctx := context.Background()
	for i, key := range []string{"Key", "key", "key ", strings.Repeat("界", 400) + "a", strings.Repeat("界", 400) + "b"} {
		for _, revision := range []string{"A", "B"} {
			_, err := sqlwrite.ExecContext(ctx, db, mysqlrepo.Native(`insert into usage_monitoring_selector_hourly_rollups_v1(model_format_revision,bucket_ms,model,api_key_hash,provider,auth_file_snapshot,account_snapshot,auth_label_snapshot,auth_index,source,source_hash,updated_at_ms) values(?,0,'m',?,'p','auth','account','label','idx','source','sourcehash',?)`), revision, key, i)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var count int
	if err := db.QueryRow("select count(*) from usage_monitoring_selector_hourly_rollups_v1").Scan(&count); err != nil || count != 10 {
		t.Fatalf("digest collision %d %v", count, err)
	}
	var digest string
	if err := db.QueryRowContext(ctx, mysqlrepo.Native("select hex(_api_key_hash) from usage_monitoring_selector_hourly_rollups_v1 where model_format_revision='A' and api_key_hash=?"), strings.Repeat("界", 400)+"a").Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if len(digest) != 64 {
		t.Fatal(fmt.Sprintf("invalid compact digest %q", digest))
	}
}

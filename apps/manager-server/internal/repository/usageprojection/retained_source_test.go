package usageprojection

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

func retainedSourceFixture(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	columns := make([]string, len(retainedRebuildColumns))
	for i, column := range retainedRebuildColumns {
		columns[i] = column + " text default ''"
		if column == "timestamp_ms" {
			columns[i] = column + " integer default 0"
		}
	}
	for _, statement := range []string{
		`create table usage_events(id integer primary key, event_hash text, ` + strings.Join(columns, ",") + `)`,
		`create table usage_monitoring_event_projection_v1(event_id integer primary key, ` + strings.Join(columns, ",") + `)`,
		`create table usage_monitoring_rollup_state(rollup_name text, schema_version integer, structure_revision text, status text, coverage_event_id integer)`,
		`create table usage_archive_event_refs(event_hash text, raw_event_id integer, timestamp_ms integer, raw_deleted_at_ms integer)`,
		`insert into usage_events(id,event_hash,timestamp_ms,api_key_hash,executor_type) values(1,'live',1000,'authoritative','responses'),(3,'tail',3000,'tail-key','chat')`,
		`insert into usage_monitoring_event_projection_v1(event_id,timestamp_ms,api_key_hash,executor_type) values(1,1000,'stale','stale'),(2,2000,'retained-key','retained-executor'),(4,4000,'orphan','orphan')`,
		// Duplicate archive evidence cannot multiply a retained event. A
		// restored live identity must win over its old projection, too.
		`insert into usage_archive_event_refs values('live',1,1000,1),('archived',2,2000,1),('archived',2,2000,2)`,
	} {
		retainedSourceExec(t, db, statement)
	}
	retainedSourceExec(t, db, `insert into usage_monitoring_rollup_state values('projection_v1',1,?,'ready',4)`, usageidentity.MonitoringProjectionStructureRevision())
	return db
}

func retainedSourceExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := sqlwrite.ExecContext(context.Background(), db, query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestVerifiedRetainedSourceOriginalIDsAndFullDimensions(t *testing.T) {
	db := retainedSourceFixture(t)
	ctx := context.Background()
	tx, err := sqlwrite.BeginTx(ctx, db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	source, target, err := VerifiedRetainedEventSourceTx(ctx, tx)
	if err != nil || target != 3 {
		t.Fatal(target, err)
	}
	query := `select id,api_key_hash,executor_type from ` + source + ` e order by id`
	if _, _, err := mysql.CompileSQL(query); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type event struct {
		id            int64
		key, executor string
	}
	var got []event
	for rows.Next() {
		var value event
		if err := rows.Scan(&value.id, &value.key, &value.executor); err != nil {
			t.Fatal(err)
		}
		got = append(got, value)
	}
	want := []event{{1, "authoritative", "responses"}, {2, "retained-key", "retained-executor"}, {3, "tail-key", "chat"}}
	if err := rows.Err(); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
}

func TestVerifiedRetainedSourceRejectsEveryIncompleteProof(t *testing.T) {
	cases := map[string]string{
		"missing state":  `delete from usage_monitoring_rollup_state`,
		"schema":         `update usage_monitoring_rollup_state set schema_version=2`,
		"revision":       `update usage_monitoring_rollup_state set structure_revision='obsolete'`,
		"clearing":       `update usage_monitoring_rollup_state set status='clearing'`,
		"coverage":       `update usage_monitoring_rollup_state set coverage_event_id=1`,
		"missing row":    `delete from usage_monitoring_event_projection_v1 where event_id=2`,
		"timestamp":      `update usage_monitoring_event_projection_v1 set timestamp_ms=2001 where event_id=2`,
		"nonpositive ID": `update usage_archive_event_refs set raw_event_id=0 where raw_event_id=2`,
		"ID collision":   `insert into usage_events(id,event_hash,timestamp_ms) values(2,'different-live-identity',2000)`,
	}
	for _, column := range retainedRebuildColumns {
		if column != "latency_ms" {
			cases["missing "+column] = `update usage_monitoring_event_projection_v1 set ` + column + `=null where event_id=2`
		}
	}
	for name, mutation := range cases {
		t.Run(name, func(t *testing.T) {
			db := retainedSourceFixture(t)
			retainedSourceExec(t, db, mutation)
			tx, err := sqlwrite.BeginTx(context.Background(), db, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			source, _, err := VerifiedRetainedEventSourceTx(context.Background(), tx)
			if source != "" || !errors.Is(err, ErrRetainedCoverageIncomplete) {
				t.Fatalf("incomplete proof returned source=%q err=%v", source, err)
			}
		})
	}
}

func TestVerifiedRetainedSourceAllRawDeletedAndNullableLatency(t *testing.T) {
	db := retainedSourceFixture(t)
	retainedSourceExec(t, db, `delete from usage_events`)
	retainedSourceExec(t, db, `update usage_monitoring_event_projection_v1 set latency_ms=null`)
	tx, err := sqlwrite.BeginTx(context.Background(), db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	source, target, err := VerifiedRetainedEventSourceTx(context.Background(), tx)
	if err != nil || target != 2 {
		t.Fatal(target, err)
	}
	var count int
	if err := tx.QueryRow(`select count(*) from ` + source + ` e`).Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
}

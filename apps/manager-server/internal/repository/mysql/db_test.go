package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

func openMySQL(t *testing.T) *sql.DB {
	t.Helper()
	dsn := testdb.DSN(t, t.TempDir())
	db, err := mysqlrepo.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMySQLStrictDeduplicationAndTransactions(t *testing.T) {
	db := openMySQL(t)
	ctx := context.Background()
	insert := `insert or ignore into usage_event_identity_ledger(event_hash, timestamp_ms, bucket_ms, aggregate_schema_version, first_seen_at_ms, updated_at_ms) values (?, ?, 0, 0, 0, 0)`
	hash := strings.Repeat("a", 64)
	result, err := db.Exec(insert, hash, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := result.RowsAffected(); n != 1 {
		t.Fatalf("insert count = %d", n)
	}
	result, err = db.Exec(insert, hash, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := result.RowsAffected(); n != 0 {
		t.Fatalf("duplicate count = %d", n)
	}
	if _, err := db.Exec(insert, strings.Repeat("b", 64), nil); err == nil {
		t.Fatal("NOT NULL error was ignored")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(insert)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(strings.Repeat("c", 64), 200); err != nil {
		t.Fatal(err)
	}
	stmt.Close()
	tx.Rollback()
	var count int
	if err := db.QueryRow("select count(*) from usage_event_identity_ledger").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rollback count=%d err=%v", count, err)
	}
	// Bulk deduplication may ignore duplicate keys, but must roll back the
	// entire statement on any other warning (including coerced NULLs).
	bulk := `insert or ignore into usage_event_identity_ledger(event_hash, timestamp_ms, bucket_ms, aggregate_schema_version, first_seen_at_ms, updated_at_ms) select ?, ?, 0, 0, 0, 0 union all select ?, ?, 0, 0, 0, 0`
	if _, err := db.Exec(bulk, strings.Repeat("d", 64), 300, strings.Repeat("e", 64), nil); err == nil {
		t.Fatal("invalid bulk row accepted")
	}
	if err := db.QueryRow("select count(*) from usage_event_identity_ledger").Scan(&count); err != nil || count != 1 {
		t.Fatalf("partial bulk data persisted: count=%d err=%v", count, err)
	}
	if _, err := db.Exec(bulk, hash, 100, strings.Repeat("d", 64), 300); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("select count(*) from usage_event_identity_ledger").Scan(&count); err != nil || count != 2 {
		t.Fatalf("bulk dedup count=%d err=%v", count, err)
	}
}

func TestMySQLFullTextIdentityAndLiteralSafety(t *testing.T) {
	db := openMySQL(t)
	keys := []string{"Key", "key", "key ", strings.Repeat("x", 1000) + "a", strings.Repeat("x", 1000) + "b", "你好😀"}
	for _, key := range keys {
		if _, err := db.Exec(`insert into settings(key,value,updated_at_ms) values(?,?,0) on conflict(key) do update set value=excluded.value`, key, `on conflict(key) do nothing; ' ? \\ json_each`); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range keys {
		var value string
		if err := db.QueryRow("select value from settings where key=?", key).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value != `on conflict(key) do nothing; ' ? \\ json_each` {
			t.Fatal("parameter contents changed")
		}
	}
	var count int
	if err := db.QueryRow("select count(*) from settings where updated_at_ms=0 and value=?", `on conflict(key) do nothing; ' ? \\ json_each`).Scan(&count); err != nil || count != len(keys) {
		t.Fatalf("identities collided: %d %v", count, err)
	}
}

func TestMySQLTruncatedBulkWarningsCannotHideInvalidRows(t *testing.T) {
	db := openMySQL(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Only the test administrator changes this restricted variable. Runtime
	// application connections work with the server's configured default.
	if _, err := conn.ExecContext(ctx, "set max_error_count=1"); err != nil {
		t.Fatal(err)
	}
	insert := `insert or ignore into usage_event_identity_ledger(event_hash,timestamp_ms,bucket_ms,aggregate_schema_version,first_seen_at_ms,updated_at_ms) `
	if _, err := conn.ExecContext(ctx, insert+`values('existing',1,0,0,0,0)`); err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, insert+`select 'existing',1,0,0,0,0 union all select 'valid',1,0,0,0,0 union all select 'invalid',null,0,0,0,0`)
	if err == nil || !strings.Contains(err.Error(), "truncated bulk insert warnings") {
		t.Fatalf("hidden warning accepted: %v", err)
	}
	var count int
	if err := conn.QueryRowContext(ctx, "select count(*) from usage_event_identity_ledger").Scan(&count); err != nil || count != 1 {
		t.Fatalf("partial bulk rows committed: %d %v", count, err)
	}
}

func TestMySQLWriterCancellationAndReadSnapshot(t *testing.T) {
	db := openMySQL(t)
	if _, err := db.Exec(`insert into settings(key,value,updated_at_ms) values('snapshot','before',0)`); err != nil {
		t.Fatal(err)
	}
	reader, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var value string
	if err := reader.QueryRow("select value from settings where key='snapshot'").Scan(&value); err != nil {
		t.Fatal(err)
	}
	writer, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := db.BeginTx(ctx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked writer cancellation = %v", err)
	}
	if _, err := writer.Exec("update settings set value='after' where key='snapshot'"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow("select value from settings where key='snapshot'").Scan(&value); err != nil || value != "before" {
		t.Fatalf("inconsistent reader snapshot: %q %v", value, err)
	}
}

func TestMySQLSchemaRestart(t *testing.T) {
	dsn := testdb.DSN(t, t.TempDir())
	for i := 0; i < 2; i++ {
		db, err := mysqlrepo.Open(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("insert into settings(key,value,updated_at_ms) values('restart','kept',1) on conflict(key) do nothing"); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestMySQLModelIdentityMatchesGo(t *testing.T) {
	db := openMySQL(t)
	for _, value := range []string{"", "(high)", "custom(model)(high)", "你好😀(HIGH)", "model(0)", "model(-000)", "model(+08192)", "model(-01)", "model(-1)", "model( high)", "model(high )", " model(max) ", "model(9223372036854775807)", "model(09223372036854775807)", "model(9223372036854775808)", "model(" + strings.Repeat("0", 100) + "1)", "model(42\n)", "model(region-us)"} {
		var got string
		query := "select " + usageidentity.SQLAnalyticsModelExpression("v.value") + " from (select ? as value) v"
		if err := db.QueryRow(query, value).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if want := usageidentity.AnalyticsModel(value); got != want {
			t.Errorf("model %q: got %q want %q", value, got, want)
		}
	}
}

func TestMySQLExpandedPreparedBindingsAndJSONNull(t *testing.T) {
	db := openMySQL(t)
	stmt, err := db.Prepare(`select cpamp_analytics_model(?), json_extract(?, '$.value'), ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	const marker = `unchanged ? ' \`
	for _, payload := range []string{`{"value":null}`, `{"value":"null"}`} {
		var model, last string
		var extracted sql.NullString
		if err := stmt.QueryRow("model(+08192)", payload, marker).Scan(&model, &extracted, &last); err != nil {
			t.Fatal(err)
		}
		if model != "model" || last != marker {
			t.Fatalf("bindings changed: %q %q", model, last)
		}
		if extracted.Valid != (payload == `{"value":"null"}`) {
			t.Fatalf("JSON null conversion: %s -> %+v", payload, extracted)
		}
	}
}

func TestMySQLRefusesUnrelatedAndNewerSchemas(t *testing.T) {
	ctx := context.Background()
	t.Run("unrelated", func(t *testing.T) {
		dsn := testdb.DSN(t, t.TempDir())
		db, err := mysqlrepo.OpenNative(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec("create table unrelated(id bigint primary key)"); err != nil {
			t.Fatal(err)
		}
		if opened, err := mysqlrepo.Open(ctx, dsn); err == nil {
			opened.Close()
			t.Fatal("unrelated database accepted")
		}
		var count int
		if err := db.QueryRow("select count(*) from information_schema.tables where table_schema=database()").Scan(&count); err != nil || count != 1 {
			t.Fatalf("unrelated schema changed: %d %v", count, err)
		}
	})
	t.Run("newer", func(t *testing.T) {
		dsn := testdb.DSN(t, t.TempDir())
		db, err := mysqlrepo.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("update manager_schema_version set version=999"); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if opened, err := mysqlrepo.Open(ctx, dsn); err == nil {
			opened.Close()
			t.Fatal("newer schema accepted")
		}
	})
}

func TestMySQLDerivedCleanupPreservesActiveRevisionAndBatchLimit(t *testing.T) {
	db := openMySQL(t)
	if _, err := db.Exec("update usage_pricing_rollup_state set structure_revision='active' where rollup_name='pricing_v1'"); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ revision, model string }{{"old", "a"}, {"old", "b"}, {"active", "a"}} {
		if _, err := db.Exec(`insert into usage_pricing_hourly_rollups_v1
		(structure_revision,bucket_ms,model,billing_model,pricing_model,service_tier,context_threshold_tokens,failed,updated_at_ms)
		values(?,0,?,'billing','pricing','',0,0,0)`, row.revision, row.model); err != nil {
			t.Fatal(err)
		}
	}
	if removed, err := mysqlrepo.CleanupDerivedBatch(context.Background(), db, 1); err != nil || removed != 1 {
		t.Fatalf("bounded cleanup: %d %v", removed, err)
	}
	var active, total int
	if err := db.QueryRow("select count(*),sum(structure_revision='active') from usage_pricing_hourly_rollups_v1").Scan(&total, &active); err != nil || total != 2 || active != 1 {
		t.Fatalf("cleanup rows total=%d active=%d error=%v", total, active, err)
	}
}

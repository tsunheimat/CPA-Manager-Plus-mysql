package mysql_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

// Capture genuine metadata BEFORE Open runs the validator. Starting with Open
// would reproduce the startup error without showing the server representation.
// Only testdb's disposable schema is touched; no server is provisioned, and the
// test skips when CPA_TEST_MYSQL_DSN is absent.
func TestMySQLHourlyDefaultMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	dsn := testdb.DSN(t, t.TempDir())
	native, err := mysqlrepo.OpenNative(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { native.Close() })
	executeFixture := func(script string) {
		t.Helper()
		for i, part := range strings.Split(script, "-- statement")[1:] {
			if query := strings.TrimSpace(part); query != "" {
				if _, err := native.ExecContext(ctx, query); err != nil {
					t.Fatalf("embedded schema statement %d: %v", i+1, err)
				}
			}
		}
	}
	// Reuse the unchanged embedded files from upgrade_test.go, not hand-written
	// lookalike DDL or a schema reconstructed from the validator's parser.
	executeFixture(frozenV1Schema)
	for _, query := range []string{
		"create table manager_schema_version(id bigint primary key,version bigint not null) engine=InnoDB",
		"insert into manager_schema_version values(1,1)",
	} {
		if _, err := native.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	executeFixture(versionTwoSchema)

	var serverVersion, sqlMode, connectionCollation string
	if err := native.QueryRowContext(ctx, "select version(),@@session.sql_mode,@@session.collation_connection").Scan(&serverVersion, &sqlMode, &connectionCollation); err != nil {
		t.Fatal(err)
	}
	t.Logf("native metadata environment: version=%.256q sql_mode=%.256q collation_connection=%.256q", serverVersion, sqlMode, connectionCollation)
	rows, err := native.QueryContext(ctx, `select c.table_name,c.column_name,c.column_type,c.is_nullable,c.column_default,c.generation_expression,c.extra,coalesce(c.collation_name,''),t.engine,t.table_collation
		from information_schema.columns c join information_schema.tables t on t.table_schema=c.table_schema and t.table_name=c.table_name
		where c.table_schema=database() and (
			(c.table_name in ('usage_monitoring_account_hourly_rollups_v1','usage_monitoring_api_key_hourly_rollups_v1') and c.column_name='auth_account_id_snapshot') or
			(c.table_name='usage_monitoring_selector_hourly_rollups_v1' and c.column_name='model_format_revision'))
		order by c.table_name,c.ordinal_position`)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		var table, name, kind, nullable, generation, extra, collation, engine, tableCollation string
		var def sql.NullString
		if err := rows.Scan(&table, &name, &kind, &nullable, &def, &generation, &extra, &collation, &engine, &tableCollation); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		// These bounded logs contain schema attributes only, not the DSN, schema
		// name, application rows or event/credential values. Valid distinguishes
		// SQL NULL from the empty Go string carried by sql.NullString.
		t.Logf("native hourly metadata: table=%.256q name=%.256q type=%.256q nullable=%.256q default.Valid=%t default=%.256q default.bytes=%d generation=%.256q extra=%.256q collation=%.256q engine=%.256q table_collation=%.256q",
			table, name, kind, nullable, def.Valid, def.String, len(def.String), generation, extra, collation, engine, tableCollation)
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("captured %d default columns, want all three before validation", count)
	}

	// This must fail, not skip or merely log, if the production validator rejects
	// a table MySQL itself just created from the unchanged migration.
	runtimeDB, validationErr := mysqlrepo.Open(ctx, dsn)
	if runtimeDB != nil {
		runtimeDB.Close()
	}
	var version, states int
	if err := native.QueryRowContext(ctx, "select version from manager_schema_version where id=1").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := native.QueryRowContext(ctx, "select count(*) from usage_monitoring_rollup_state where rollup_name in ('stats_hourly_v1','metadata_hourly_v1')").Scan(&states); err != nil {
		t.Fatal(err)
	}
	if validationErr != nil {
		if version != 1 || states != 0 {
			t.Errorf("failed validation published marker or seeds: version=%d states=%d", version, states)
		}
		t.Fatalf("validator rejected unchanged native hourly DDL after metadata capture: %v", validationErr)
	}
	if version != 2 || states != 2 {
		t.Fatalf("successful validation did not publish marker and seeds: version=%d states=%d", version, states)
	}
	if _, err := native.ExecContext(ctx, "update usage_monitoring_rollup_state set status='ready',structure_revision='metadata-round-trip',coverage_event_id=17 where rollup_name='stats_hourly_v1'"); err != nil {
		t.Fatal(err)
	}
	runtimeDB, err = mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("v2 restart validation: %v", err)
	}
	runtimeDB.Close()
	var status, revision string
	var coverage int64
	if err := native.QueryRowContext(ctx, "select status,structure_revision,coverage_event_id from usage_monitoring_rollup_state where rollup_name='stats_hourly_v1'").Scan(&status, &revision, &coverage); err != nil {
		t.Fatal(err)
	}
	if status != "ready" || revision != "metadata-round-trip" || coverage != 17 {
		t.Fatal("v2 restart reset durable rollup progress")
	}
}

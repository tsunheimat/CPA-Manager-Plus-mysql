package mysqlmigrate

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// A small administrative-driver shim runs copyTables' real streaming and
// transaction paths offline. Only MySQL metadata/session/sequence/upsert syntax
// is simulated; row insertion, counting and rollback use actual SQLite storage.
// Native MySQL import SQL remains covered by the opt-in command_test.go tests.
type copyConnector struct {
	base       driver.Driver
	path       string
	columns    map[string][]string
	sequences  map[string]int64
	foreignKey bool
	failSeed   bool
	batches    int
}

func (c *copyConnector) Driver() driver.Driver { return c.base }
func (c *copyConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(c.path)
	if err != nil {
		return nil, err
	}
	return &copyConn{Conn: conn, owner: c}, nil
}

type copyConn struct {
	driver.Conn
	owner *copyConnector
}

func (c *copyConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	switch {
	case strings.HasPrefix(q, "set foreign_key_checks="):
		c.owner.foreignKey = strings.HasSuffix(q, "=1")
		return driver.RowsAffected(0), nil
	case strings.HasPrefix(q, "alter table "):
		parts := strings.Split(q, " ")
		next, err := strconv.ParseInt(strings.TrimPrefix(parts[3], "auto_increment="), 10, 64)
		c.owner.sequences[strings.Trim(parts[2], "`")] = next
		return driver.RowsAffected(0), err
	case strings.Contains(q, "on duplicate key update"):
		if c.owner.failSeed {
			return nil, errors.New("injected seed failure")
		}
		q = strings.Split(q, " on duplicate key update")[0] + " on conflict(rollup_name) do nothing"
	}
	if strings.HasPrefix(q, "insert into `usage_events`") {
		c.owner.batches++
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}

func (c *copyConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "information_schema.columns") {
		if strings.HasPrefix(q, "select count(*)") {
			auto := int64(0)
			if args[0].Value == "usage_events" {
				auto = 1
			}
			return &copyRowsResult{values: []driver.Value{auto}}, nil
		}
		var values []driver.Value
		for _, col := range c.owner.columns[args[0].Value.(string)] {
			values = append(values, col)
		}
		return &copyRowsResult{values: values}, nil
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}

type copyRowsResult struct{ values []driver.Value }

func (*copyRowsResult) Columns() []string { return []string{"value"} }
func (*copyRowsResult) Close() error      { return nil }
func (r *copyRowsResult) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	dest[0], r.values = r.values[0], r.values[1:]
	return nil
}

func offlineCopyDatabases(t *testing.T) (*sql.DB, *sql.DB, *copyConnector) {
	t.Helper()
	open := func(name string) (*sql.DB, string) {
		path := filepath.Join(t.TempDir(), name)
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db, path
	}
	source, _ := open("source.sqlite")
	dest, path := open("destination.sqlite")
	columns := map[string][]string{
		"settings":                      {"key", "value", "updated_at_ms"},
		"usage_events":                  {"id", "event_hash"},
		"usage_event_identity_ledger":   {"event_hash", "raw_event_id", "aggregate_structure_revision"},
		"usage_monitoring_rollup_state": {"rollup_name", "schema_version", "structure_revision", "status", "backfill_last_event_id", "coverage_event_id", "target_event_id", "processed_events", "updated_at_ms"},
	}
	for _, db := range []*sql.DB{source, dest} {
		for _, q := range []string{
			"create table settings(`key` text primary key,value text not null,updated_at_ms integer not null)",
			"create table usage_events(id integer primary key autoincrement,event_hash text not null unique)",
			"create table usage_event_identity_ledger(event_hash text primary key,raw_event_id integer,aggregate_structure_revision text not null default '')",
			"create table usage_monitoring_rollup_state(rollup_name text primary key,schema_version integer not null,structure_revision text not null,status text not null,backfill_last_event_id integer not null,coverage_event_id integer not null,target_event_id integer not null,processed_events integer not null,updated_at_ms integer not null)",
		} {
			if _, err := db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, q := range []string{
		"insert into settings values('encrypted','ciphertext-界',12)",
		"insert into usage_events values(7000,'archived')",
		"delete from usage_events",
		"insert into usage_event_identity_ledger values('archived',null,'retained-generation')",
		"insert into usage_monitoring_rollup_state values('stats_v1',1,'daily-generation','ready',7000,7000,7000,7000,12)",
		// Even a source with unexpected hourly rows cannot lend readiness to an
		// empty MySQL-only cache after the shared state table is copied.
		"insert into usage_monitoring_rollup_state values('stats_hourly_v1',1,'wrong','ready',7000,7000,7000,7000,12)",
	} {
		if _, err := source.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"insert into settings values('before','rollback-sentinel',1)",
		"insert into usage_monitoring_rollup_state values('metadata_hourly_v1',1,'','pending',0,0,0,0,0)",
	} {
		if _, err := dest.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	shim := &copyConnector{base: dest.Driver(), path: path, columns: columns, sequences: map[string]int64{}, foreignKey: true}
	target := sql.OpenDB(shim)
	target.SetMaxOpenConns(1)
	t.Cleanup(func() { target.Close() })
	return source, target, shim
}

func TestCopyTablesReseedsHourlyStateAndPreservesHistoryOffline(t *testing.T) {
	source, target, shim := offlineCopyDatabases(t)
	// Force multiple copyRows flushes without committing partial batches.
	for i := 1; i <= 300; i++ {
		if _, err := source.Exec("insert into usage_events values(?,?)", i, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := copyTables(context.Background(), source, target, &output, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if shim.sequences["usage_events"] != 7001 || !shim.foreignKey || shim.batches != 2 {
		t.Fatalf("copy sequence/session/batches: %+v", shim)
	}
	var value, status, revision string
	var coverage, backfill, targetID, processed int64
	if err := target.QueryRow("select value from settings where `key`='encrypted'").Scan(&value); err != nil || value != "ciphertext-界" {
		t.Fatalf("encrypted value changed: %q %v", value, err)
	}
	if err := target.QueryRow("select aggregate_structure_revision from usage_event_identity_ledger where event_hash='archived' and raw_event_id is null").Scan(&revision); err != nil || revision != "retained-generation" {
		t.Fatalf("retained ledger changed: %q %v", revision, err)
	}
	for _, name := range []string{"stats_hourly_v1", "metadata_hourly_v1"} {
		if err := target.QueryRow("select status,structure_revision,coverage_event_id,backfill_last_event_id,target_event_id,processed_events from usage_monitoring_rollup_state where rollup_name=?", name).Scan(&status, &revision, &coverage, &backfill, &targetID, &processed); err != nil || status != "pending" || revision != "" || coverage != 0 || backfill != 0 || targetID != 0 || processed != 0 {
			t.Fatalf("inherited hourly progress %s: %s %q %d %d %d %d %v", name, status, revision, coverage, backfill, targetID, processed, err)
		}
	}
	if err := target.QueryRow("select structure_revision,coverage_event_id from usage_monitoring_rollup_state where rollup_name='stats_v1'").Scan(&revision, &coverage); err != nil || revision != "daily-generation" || coverage != 7000 {
		t.Fatalf("daily coverage lost: %q %d %v", revision, coverage, err)
	}
}

func TestCopyTablesFailureRollsBackAllRowsOffline(t *testing.T) {
	for _, failure := range []string{"seed", "ownership"} {
		t.Run(failure, func(t *testing.T) {
			source, target, shim := offlineCopyDatabases(t)
			shim.failSeed = failure == "seed"
			checks := 0
			err := copyTables(context.Background(), source, target, io.Discard, func() error {
				checks++
				if failure == "ownership" && checks == len(shim.columns)+1 {
					return errors.New("ownership lost before commit")
				}
				return nil
			})
			if err == nil {
				t.Fatal("failed import committed")
			}
			var value string
			if err := target.QueryRow("select value from settings where `key`='before'").Scan(&value); err != nil || value != "rollback-sentinel" {
				t.Fatalf("partial import replaced original rows: %q %v", value, err)
			}
			var copied int
			if err := target.QueryRow("select count(*) from usage_event_identity_ledger").Scan(&copied); err != nil || copied != 0 {
				t.Fatalf("ledger partially committed: %d %v", copied, err)
			}
			if !shim.foreignKey {
				t.Fatal("foreign key checks were not restored")
			}
		})
	}
}

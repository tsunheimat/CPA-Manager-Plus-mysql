package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type restoreBackend struct {
	layout                []BackupTable
	hourly                *migrationBackend
	writes                []string
	args                  [][]driver.NamedValue
	committed, rolledBack bool
	readOnly              bool
	fail                  string
}
type restoreConnector struct{ b *restoreBackend }

func (restoreConnector) Driver() driver.Driver { return admissionDriver{} }
func (c restoreConnector) Connect(context.Context) (driver.Conn, error) {
	return &restoreConn{b: c.b}, nil
}

type restoreConn struct{ b *restoreBackend }

func (*restoreConn) Close() error                            { return nil }
func (c *restoreConn) Prepare(q string) (driver.Stmt, error) { return &restoreStmt{c: c, q: q}, nil }
func (c *restoreConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *restoreConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.b.readOnly = opts.ReadOnly
	return restoreTx{c.b}, nil
}
func (c *restoreConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.b.writes = append(c.b.writes, q)
	c.b.args = append(c.b.args, append([]driver.NamedValue(nil), args...))
	if c.b.fail != "" && strings.Contains(q, c.b.fail) {
		return nil, errors.New("injected restore failure")
	}
	return driver.RowsAffected(1), nil
}
func (c *restoreConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(q, "select version from manager_schema_version"):
		return &admissionRows{cols: []string{"version"}, values: [][]driver.Value{{int64(2)}}}, nil
	case strings.Contains(q, "select table_name,coalesce(auto_increment,0)"):
		r := &admissionRows{cols: []string{"table", "next"}}
		for _, table := range c.b.layout {
			r.values = append(r.values, []driver.Value{table.Name, int64(1)})
		}
		return r, nil
	case strings.Contains(q, "select column_name from information_schema.columns"):
		r := &admissionRows{cols: []string{"column"}}
		for _, table := range c.b.layout {
			if table.Name == args[0].Value {
				for _, col := range table.Columns {
					r.values = append(r.values, []driver.Value{col})
				}
			}
		}
		return r, nil
	case strings.Contains(q, "information_schema.key_column_usage"):
		return &admissionRows{cols: []string{"table", "constraint", "column", "parent", "target"}}, nil
	case strings.Contains(q, "information_schema"):
		return (&migrationConn{backend: c.b.hourly}).QueryContext(ctx, q, args)
	case strings.HasPrefix(q, "select count(*) from"):
		return &admissionRows{cols: []string{"count"}, values: [][]driver.Value{{int64(0)}}}, nil
	case strings.HasPrefix(q, "select "):
		return &admissionRows{cols: []string{"empty"}}, nil
	}
	return nil, fmt.Errorf("unexpected restore query %s", q)
}

type restoreStmt struct {
	c *restoreConn
	q string
}

func (*restoreStmt) Close() error  { return nil }
func (*restoreStmt) NumInput() int { return -1 }
func (s *restoreStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.c.ExecContext(context.Background(), s.q, valuesToNamed(args))
}
func (*restoreStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("unexpected prepared read")
}

type restoreTx struct{ b *restoreBackend }

func (t restoreTx) Commit() error   { t.b.committed = true; return nil }
func (t restoreTx) Rollback() error { t.b.rolledBack = true; return nil }
func restoreDB(t *testing.T) (*sql.DB, *restoreBackend) {
	t.Helper()
	layout, err := versionedBackupLayout(2)
	if err != nil {
		t.Fatal(err)
	}
	tables, err := schemaTables(hourlySchema)
	if err != nil {
		t.Fatal(err)
	}
	hourly := &migrationBackend{tables: map[string]schemaTable{}}
	for _, table := range tables {
		hourly.tables[table.name] = table
	}
	b := &restoreBackend{layout: layout, hourly: hourly}
	db := sql.OpenDB(restoreConnector{b})
	t.Cleanup(func() { db.Close() })
	return db, b
}
func historicalBackup(t *testing.T) []byte {
	t.Helper()
	tables, err := versionedBackupLayout(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range tables {
		switch tables[i].Name {
		case "manager_schema_version", "settings", "usage_event_identity_ledger", "usage_archive_event_refs":
			tables[i].Rows = 1
		case "usage_events":
			tables[i].NextID = 7001
		}
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	if err := enc.Encode(BackupHeader{Version: 1, SchemaVersion: 1, Tables: tables}); err != nil {
		t.Fatal(err)
	}
	for _, table := range tables {
		for n := int64(0); n < table.Rows; n++ {
			values := make([][]byte, len(table.Columns))
			for i, col := range table.Columns {
				values[i] = []byte("1")
				switch col {
				case "event_hash":
					values[i] = []byte("archived-hash")
				case "value":
					values[i] = []byte("encrypted-setting")
				case "raw_event_id":
					if table.Name == "usage_event_identity_ledger" {
						values[i] = nil
					} else {
						values[i] = []byte("7000")
					}
				}
			}
			if err := enc.Encode(values); err != nil {
				t.Fatal(err)
			}
		}
	}
	return out.Bytes()
}
func TestV1RestoreMappingOffline(t *testing.T) {
	db, b := restoreDB(t)
	restore, err := BeginRestore(context.Background(), db, bytes.NewReader(historicalBackup(t)))
	if err != nil {
		t.Fatal(err)
	}
	if b.committed || b.readOnly {
		t.Fatal("restore was committed early or read-only")
	}
	if err := restore.Tx.Commit(); err != nil {
		t.Fatal(err)
	}
	restore.Close()
	joined := strings.Join(b.writes, "\n")
	for _, name := range []string{"account", "api_key", "selector"} {
		if !strings.Contains(joined, "delete from `usage_monitoring_"+name+"_hourly_rollups_v1`") {
			t.Fatalf("destination-only %s cache not cleared", name)
		}
	}
	if strings.Contains(joined, "delete from `manager_schema_version`") || strings.Contains(joined, "insert into `manager_schema_version`") {
		t.Fatal("old physical marker was restored")
	}
	if !strings.Contains(joined, "alter table `usage_events` auto_increment=7001") {
		t.Fatal("archived high-water not restored")
	}
	if !strings.Contains(joined, "delete from usage_monitoring_rollup_state where rollup_name in ('stats_hourly_v1','metadata_hourly_v1')") {
		t.Fatal("old stream can inherit false hourly coverage")
	}
	seeds := 0
	ledger, archives, setting := false, false, false
	for i, q := range b.writes {
		if strings.Contains(q, "insert into usage_monitoring_rollup_state") {
			seeds++
		}
		if strings.HasPrefix(q, "insert into `usage_event_identity_ledger`") {
			ledger = true
			found := false
			for _, arg := range b.args[i] {
				if bytes.Equal(byteValue(arg.Value), []byte("archived-hash")) {
					found = true
				}
			}
			if !found {
				t.Fatal("ledger row lost")
			}
		}
		if strings.HasPrefix(q, "insert into `usage_archive_event_refs`") {
			archives = true
		}
		if strings.HasPrefix(q, "insert into `settings`") {
			setting = true
		}
	}
	if seeds != 2 || !ledger || !archives || !setting || !b.committed {
		t.Fatalf("restore mapping incomplete seeds=%d ledger=%v archive=%v settings=%v", seeds, ledger, archives, setting)
	}
}
func byteValue(v any) []byte {
	if b, ok := v.([]byte); ok {
		return b
	}
	return nil
}
func TestRestoreFailureRollsBackAndResetsConnectionOffline(t *testing.T) {
	db, b := restoreDB(t)
	b.fail = "insert into `usage_event_identity_ledger`"
	if _, err := BeginRestore(context.Background(), db, bytes.NewReader(historicalBackup(t))); err == nil {
		t.Fatal("failed restore accepted")
	}
	if b.committed || !b.rolledBack {
		t.Fatal("restore failure not rolled back")
	}
	if b.writes[len(b.writes)-1] != "set foreign_key_checks=1" {
		t.Fatal("restore leaked disabled foreign keys")
	}
}
func TestBackupUsesReadOnlySnapshotOffline(t *testing.T) {
	db, b := restoreDB(t)
	var out bytes.Buffer
	if err := WriteBackup(context.Background(), db, &out); err != nil {
		t.Fatal(err)
	}
	if !b.readOnly || !b.committed || len(b.writes) != 0 {
		t.Fatal("backup is not an ungated read-only snapshot")
	}
}

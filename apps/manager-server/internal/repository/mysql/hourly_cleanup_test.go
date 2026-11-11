package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	_ "modernc.org/sqlite"
)

var cleanupTables = []string{"usage_monitoring_account_hourly_rollups_v1", "usage_monitoring_api_key_hourly_rollups_v1", "usage_monitoring_selector_hourly_rollups_v1"}

// This adapter exercises the actual runtime compiler/admission driver, then runs
// the bounded predicates against a SQLite reference. It is not native MySQL SQL
// or plan validation. Only FORCE INDEX and the redundant DELETE LIMIT differ.
type cleanupReference struct {
	db      *sql.DB
	queries []string
	args    [][]driver.NamedValue
	fail    bool
}

func (*cleanupReference) Driver() driver.Driver { return admissionDriver{} }
func (c *cleanupReference) Connect(context.Context) (driver.Conn, error) {
	return &cleanupReferenceConn{owner: c}, nil
}

type cleanupReferenceConn struct {
	owner *cleanupReference
	tx    *sql.Tx
}

func (*cleanupReferenceConn) Close() error { return nil }
func (*cleanupReferenceConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected cleanup prepare")
}
func (c *cleanupReferenceConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *cleanupReferenceConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	tx, err := c.owner.db.BeginTx(ctx, nil)
	c.tx = tx
	return tx, err
}
func cleanupValues(args []driver.NamedValue) []any {
	values := make([]any, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return values
}
func (c *cleanupReferenceConn) record(q string, args []driver.NamedValue) {
	c.owner.queries = append(c.owner.queries, q)
	c.owner.args = append(c.owner.args, append([]driver.NamedValue(nil), args...))
}
func (c *cleanupReferenceConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	c.record(q, args)
	q = strings.ReplaceAll(q, " force index (idx_hourly_revision_bucket)", "")
	rows, err := c.tx.QueryContext(ctx, q, cleanupValues(args)...)
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}
	return &cleanupReferenceRows{Rows: rows, cols: cols}, nil
}
func (c *cleanupReferenceConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.record(q, args)
	if !strings.Contains(q, "_identity_hash in (") {
		// The pre-existing daily/pricing cleanup is outside this regression.
		return driver.RowsAffected(0), nil
	}
	if c.owner.fail {
		return nil, errors.New("injected cleanup delete failure")
	}
	q = strings.TrimSuffix(q, " limit ?")
	return c.tx.ExecContext(ctx, q, cleanupValues(args[:len(args)-1])...)
}

type cleanupReferenceRows struct {
	*sql.Rows
	cols []string
}

func (r *cleanupReferenceRows) Columns() []string { return r.cols }
func (r *cleanupReferenceRows) Next(out []driver.Value) error {
	if !r.Rows.Next() {
		if err := r.Rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	values := make([]any, len(out))
	for i := range values {
		values[i] = &out[i]
	}
	return r.Rows.Scan(values...)
}

func cleanupReferenceDB(t *testing.T, revision string) (*sql.DB, *cleanupReference, *connector) {
	t.Helper()
	ref, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "cleanup.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ref.Close() })
	for _, table := range cleanupTables {
		col, _ := hourlyCleanupColumn(table)
		for _, q := range []string{
			"create table " + table + " (_identity_hash blob primary key, _revision_hash blob not null, " + col + " text not null, bucket_ms integer not null)",
			"create index " + table + "_revision_bucket on " + table + " (_revision_hash,bucket_ms)",
		} {
			if _, err := ref.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, state := range []string{"usage_monitoring_rollup_state", "usage_pricing_rollup_state"} {
		if _, err := ref.Exec("create table " + state + " (rollup_name text primary key, structure_revision text)"); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"stats_hourly_v1", "metadata_hourly_v1"} {
		if _, err := ref.Exec("insert into usage_monitoring_rollup_state values (?,?)", name, revision); err != nil {
			t.Fatal(err)
		}
	}
	backend := &cleanupReference{db: ref}
	owner := &connector{base: backend, writer: make(chan struct{}, 1)}
	db := sql.OpenDB(owner)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db, backend, owner
}

func seedCleanupRow(t *testing.T, ref *sql.DB, table, revision, identity string, bucket int) {
	t.Helper()
	column, _ := hourlyCleanupColumn(table)
	id, digest := sha256.Sum256([]byte(identity)), sha256.Sum256([]byte(revision))
	if _, err := ref.Exec("insert into "+table+" values (?,?,?,?)", id[:], digest[:], revision, bucket); err != nil {
		t.Fatalf("seed %s: %v", column, err)
	}
}

func TestHourlyCleanupNativeBuildersPreserveDigestAndInequalities(t *testing.T) {
	revision := " 版本:" + strings.Repeat("x", 300) + " "
	digest := sha256.Sum256([]byte(revision))
	for _, table := range cleanupTables {
		column, _ := hourlyCleanupColumn(table)
		for _, op := range []string{"<", ">"} {
			q := hourlyCleanupSelection(table, column, op)
			args := []any{digest[:], revision, 1001}
			physical, bound, err := CompileSQL(q, args...)
			if err != nil || physical != strings.TrimPrefix(q, nativePrefix) || !reflect.DeepEqual(bound, args) {
				t.Fatalf("native cleanup changed SQL/bindings: %q %#v %v", physical, bound, err)
			}
			for _, want := range []string{"force index (idx_hourly_revision_bucket)", "_revision_hash " + op + " ?", QuoteIdentifier(column) + " <> ?", "order by _revision_hash,bucket_ms limit ?"} {
				if !strings.Contains(physical, want) {
					t.Fatalf("missing bounded indexed predicate %q: %s", want, physical)
				}
			}
		}
		q := hourlyCleanupDeletion(table, column, 2)
		args := []any{digest[:], digest[:], revision, digest[:], 2}
		physical, bound, err := CompileSQL(q, args...)
		if err != nil || physical != strings.TrimPrefix(q, nativePrefix) || !reflect.DeepEqual(bound, args) {
			t.Fatalf("native deletion changed SQL/bindings: %q %#v %v", physical, bound, err)
		}
		if !strings.Contains(physical, "_identity_hash in (?,?)") || !strings.Contains(physical, QuoteIdentifier(column)+" <> ? and _revision_hash <> ? limit ?") {
			t.Fatal("delete lost unique key bound or full revision residual")
		}
	}
}

func TestPeriodicHourlyCleanupUsesBoundedNativePathAndResumes(t *testing.T) {
	ctx := context.Background()
	current := " current-版本 " // retain exact state value, not TRIM(state)
	db, backend, owner := cleanupReferenceDB(t, current)
	for _, table := range cleanupTables {
		seedCleanupRow(t, backend.db, table, current, "current", 0)
		for i := 0; i < 3; i++ {
			seedCleanupRow(t, backend.db, table, fmt.Sprintf("obsolete-%d", i), fmt.Sprint(i), i)
		}
	}
	var total int64
	for batch := 0; batch < 10; batch++ {
		removed, err := CleanupDerivedBatch(ctx, db, 2)
		if err != nil || removed > 2 || len(owner.writer) != 0 {
			t.Fatalf("batch=%d removed=%d permit=%d err=%v", batch, removed, len(owner.writer), err)
		}
		total += removed
		if removed == 0 {
			break
		}
	}
	if total != 9 {
		t.Fatalf("obsolete rows removed=%d", total)
	}
	for _, table := range cleanupTables {
		var remaining int
		if err := backend.db.QueryRow("select count(*) from " + table).Scan(&remaining); err != nil || remaining != 1 {
			t.Fatalf("current generation not preserved for %s: %d %v", table, remaining, err)
		}
	}
	for i, q := range backend.queries {
		if strings.Contains(q, "_identity_hash from") {
			args := backend.args[i]
			digest := sha256.Sum256([]byte(current))
			if !strings.Contains(q, "force index (idx_hourly_revision_bucket)") || !strings.HasSuffix(q, "limit ?") || !bytes.Equal(args[0].Value.([]byte), digest[:]) || args[1].Value != current || args[2].Value.(int64) > 3 {
				t.Fatalf("physical driver lost native bound/digest: %s %#v", q, args)
			}
		} else if strings.Contains(q, "delete from `usage_monitoring_") && strings.Contains(q, "_hourly_") && !strings.Contains(q, "_identity_hash in (") {
			t.Fatalf("unbounded hourly deletion reached driver: %s", q)
		}
	}
}

func TestHourlyCleanupNoOpUsesTwoIndexRangesAndNoDelete(t *testing.T) {
	ctx := context.Background()
	db, backend, _ := cleanupReferenceDB(t, "active")
	table := cleanupTables[0]
	for i := 0; i < 200; i++ {
		seedCleanupRow(t, backend.db, table, "active", fmt.Sprint(i), i)
	}
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	removed, pending, err := CleanupHourlyRevisionBatch(ctx, tx, table, "active", 1)
	if err != nil || removed != 0 || pending {
		t.Fatalf("active-only cleanup = %d %v %v", removed, pending, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(backend.queries) != 2 {
		t.Fatalf("no-op issued non-range work: %v", backend.queries)
	}
	for i, q := range backend.queries {
		portable := strings.ReplaceAll(q, " force index (idx_hourly_revision_bucket)", "")
		rows, err := backend.db.Query("explain query plan "+portable, cleanupValues(backend.args[i])...)
		if err != nil {
			t.Fatal(err)
		}
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			details = append(details, detail)
		}
		rows.Close()
		// SQLite reference evidence only, never a claim about MySQL EXPLAIN.
		if len(details) != 1 || !strings.Contains(details[0], "SEARCH") || !strings.Contains(details[0], "_revision_hash") {
			t.Fatalf("reference is not an indexed range: %v", details)
		}
	}
}

func TestHourlyCleanupResidualAndRollbackSafety(t *testing.T) {
	ctx := context.Background()
	db, backend, owner := cleanupReferenceDB(t, "active")
	table := cleanupTables[0]
	seedCleanupRow(t, backend.db, table, "active", "current", 0)
	seedCleanupRow(t, backend.db, table, "old", "old", 1)
	// Simulate a digest collision: obsolete row sharing the active hash must be
	// conservatively retained. Also a corrupt digest must not authorize deletion
	// of a full-value active row. Real MySQL computes these columns itself.
	seedCleanupRow(t, backend.db, table, "colliding-old", "collision", 2)
	active, bad := sha256.Sum256([]byte("active")), sha256.Sum256([]byte("bad"))
	if _, err := backend.db.Exec("update "+table+" set _revision_hash=? where structure_revision='colliding-old'", active[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.db.Exec("update "+table+" set _revision_hash=? where structure_revision='active'", bad[:]); err != nil {
		t.Fatal(err)
	}
	backend.fail = true
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := CleanupHourlyRevisionBatch(ctx, tx, table, "active", 1); err == nil {
		t.Fatal("delete failure was hidden")
	}
	tx.Rollback()
	if len(owner.writer) != 0 {
		t.Fatal("cleanup failure leaked writer admission")
	}
	backend.fail = false
	tx, err = sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	removed, pending, err := CleanupHourlyRevisionBatch(ctx, tx, table, "active", 1)
	if err != nil || removed != 1 || pending {
		t.Fatalf("resume cleanup=%d %v %v", removed, pending, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := backend.db.QueryRow("select count(*) from " + table).Scan(&count); err != nil || count != 2 {
		t.Fatalf("residual/hash collision safety: %d %v", count, err)
	}
}

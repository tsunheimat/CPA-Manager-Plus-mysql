package usagemonitoring

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	_ "modernc.org/sqlite"
)

func TestHourlyCleanupPortableBoundResumesAndRollsBack(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "hourly-cleanup.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, kind := range []string{"account", "api_key", "selector"} {
		col := "structure_revision"
		if kind == "selector" {
			col = "model_format_revision"
		}
		table := "usage_monitoring_" + kind + "_hourly_rollups_v1"
		if _, err := db.Exec("create table " + table + " (" + col + " text)"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("insert into " + table + " values ('current'),('old-A'),('old-B'),('old-C')"); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	for _, name := range []string{HourlyStatsRollupName, HourlyMetadataRollupName} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := cleanupHourlyRevisionRows(ctx, tx, name, "current", 1, false)
		if err != nil || !pending {
			t.Fatalf("rollback pass pending=%v err=%v", pending, err)
		}
		tx.Rollback()
		var batches int
		for ; batches < 10; batches++ {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			pending, err = cleanupHourlyRevisionRows(ctx, tx, name, "current", 1, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if !pending {
				break
			}
		}
		want := 2
		if name == HourlyStatsRollupName {
			want = 5
		}
		if batches != want {
			t.Fatalf("cleanup %s batches=%d want=%d", name, batches+1, want+1)
		}
	}
	for _, kind := range []string{"account", "api_key", "selector"} {
		var count int
		if err := db.QueryRow("select count(*) from usage_monitoring_" + kind + "_hourly_rollups_v1").Scan(&count); err != nil || count != 1 {
			t.Fatalf("current %s generation changed: count=%d err=%v", kind, count, err)
		}
	}
}

type hourlyCleanupSpy struct {
	t      *testing.T
	counts map[string]int
}

func (*hourlyCleanupSpy) Driver() driver.Driver                          { return hourlyCleanupSpyDriver{} }
func (s *hourlyCleanupSpy) Connect(context.Context) (driver.Conn, error) { return s, nil }
func (*hourlyCleanupSpy) Close() error                                   { return nil }
func (*hourlyCleanupSpy) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (s *hourlyCleanupSpy) Begin() (driver.Tx, error) { return s, nil }
func (*hourlyCleanupSpy) Commit() error               { return nil }
func (*hourlyCleanupSpy) Rollback() error             { return nil }

type hourlyCleanupSpyDriver struct{}

func (hourlyCleanupSpyDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector only")
}

func (s *hourlyCleanupSpy) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	s.t.Helper()
	if !strings.HasPrefix(q, mysql.Native("select _identity_hash")) || !strings.Contains(q, "force index (idx_hourly_revision_bucket)") || !strings.HasSuffix(q, "limit ?") {
		s.t.Fatalf("catch-up used unbounded/translated cleanup query: %s", q)
	}
	if len(args) != 3 || !bytes.Equal(args[0].Value.([]byte), revisionDigest("current")) || args[1].Value != "current" {
		s.t.Fatalf("cleanup lost digest/full revision: %#v", args)
	}
	r := &hourlyCleanupSpyRows{}
	// Fake all obsolete hashes in the lower range; upper range is empty.
	if strings.Contains(q, "_revision_hash < ?") {
		for table, count := range s.counts {
			if strings.Contains(q, "`"+table+"`") {
				for i := 0; i < count && int64(i) < args[2].Value.(int64); i++ {
					r.ids = append(r.ids, revisionDigest(table+strings.Repeat("x", i)))
				}
			}
		}
	}
	return r, nil
}
func (s *hourlyCleanupSpy) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	s.t.Helper()
	if !strings.HasPrefix(q, mysql.Native("delete from")) || !strings.Contains(q, "_identity_hash in (") || !strings.Contains(q, "<> ? and _revision_hash <> ? limit ?") {
		s.t.Fatalf("catch-up used unbounded/translated deletion: %s", q)
	}
	n := int(args[len(args)-1].Value.(int64))
	for table := range s.counts {
		if strings.Contains(q, "`"+table+"`") {
			s.counts[table] -= n
		}
	}
	return driver.RowsAffected(n), nil
}

type hourlyCleanupSpyRows struct{ ids [][]byte }

func (*hourlyCleanupSpyRows) Columns() []string { return []string{"_identity_hash"} }
func (*hourlyCleanupSpyRows) Close() error      { return nil }
func (r *hourlyCleanupSpyRows) Next(out []driver.Value) error {
	if len(r.ids) == 0 {
		return io.EOF
	}
	out[0] = r.ids[0]
	r.ids = r.ids[1:]
	return nil
}

func TestHourlyCatchUpCleanupNativeUsesIndexedSharedBudget(t *testing.T) {
	ctx := context.Background()
	spy := &hourlyCleanupSpy{t: t, counts: map[string]int{
		"usage_monitoring_account_hourly_rollups_v1": 2,
		"usage_monitoring_api_key_hourly_rollups_v1": 1,
	}}
	db := sql.OpenDB(spy)
	defer db.Close()
	for batch := 0; batch < 3; batch++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := cleanupHourlyRevisionRows(ctx, tx, HourlyStatsRollupName, "current", 1, true)
		if err != nil || pending != (batch < 2) {
			t.Fatalf("batch=%d pending=%v err=%v", batch, pending, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		remaining := 0
		for _, count := range spy.counts {
			remaining += count
		}
		if remaining != 2-batch {
			t.Fatalf("shared budget exceeded: remaining=%d batch=%d", remaining, batch)
		}
	}
}

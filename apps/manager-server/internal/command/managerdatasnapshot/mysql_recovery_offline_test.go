package managerdatasnapshot

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
)

// Recovery is a filesystem protocol tied to a locking transaction-token read.
// Exercise it offline too, rather than skipping all journal coverage without a
// MySQL server. The native snapshot tests verify the same token with InnoDB.
type journalConnector struct {
	token    string
	readErr  error
	locked   bool
	finished bool
}

func (c *journalConnector) Driver() driver.Driver { return journalDriver{} }
func (c *journalConnector) Connect(context.Context) (driver.Conn, error) {
	return &journalConn{owner: c}, nil
}

type journalDriver struct{}

func (journalDriver) Open(string) (driver.Conn, error) { return nil, errors.New("connector required") }

type journalConn struct{ owner *journalConnector }

func (*journalConn) Close() error { return nil }
func (*journalConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected recovery prepare")
}
func (c *journalConn) Begin() (driver.Tx, error) { return journalTx{c.owner}, nil }
func (c *journalConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if q == "select @@server_uuid,database()" {
		return &journalRows{columns: []string{"server", "database"}, values: []driver.Value{"offline-server", "offline-schema"}}, nil
	}
	if strings.Contains(q, "from settings") && strings.HasSuffix(q, "for update") {
		c.owner.locked = true
		if c.owner.readErr != nil {
			return nil, c.owner.readErr
		}
		r := &journalRows{columns: []string{"token"}}
		if c.owner.token != "" {
			r.values = []driver.Value{c.owner.token}
		}
		return r, nil
	}
	return nil, errors.New("unexpected recovery read")
}

type journalTx struct{ owner *journalConnector }

func (t journalTx) Commit() error   { t.owner.finished = true; return nil }
func (t journalTx) Rollback() error { return nil }

type journalRows struct {
	columns []string
	values  []driver.Value
}

func (r *journalRows) Columns() []string { return r.columns }
func (*journalRows) Close() error        { return nil }
func (r *journalRows) Next(dest []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(dest, r.values)
	r.values = nil
	return nil
}

func TestMySQLFileJournalRecoveryOffline(t *testing.T) {
	for _, outcome := range []string{"committed", "rolled-back", "uncertain", "wrong-database"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			backend := &journalConnector{}
			if outcome == "committed" {
				backend.token = "snapshot-token"
			}
			if outcome == "uncertain" {
				backend.readErr = errors.New("database unavailable")
			}
			db := sql.OpenDB(backend)
			defer db.Close()
			dir := t.TempDir()
			cfg := config.Config{DataKeyPath: filepath.Join(dir, "data.key"), UsageArchiveDir: filepath.Join(dir, "archives")}
			fingerprint, err := mysqlFingerprint(ctx, db)
			if err != nil {
				t.Fatal(err)
			}
			j := mysqlJournal{Fingerprint: fingerprint, Token: "snapshot-token", Items: []mysqlJournalItem{
				{Target: cfg.DataKeyPath, Old: filepath.Join(dir, "old.key"), Stage: filepath.Join(dir, "staged.key"), Existed: true},
				{Target: cfg.UsageArchiveDir, Old: filepath.Join(dir, "old-archives"), Stage: filepath.Join(dir, "staged-archives"), Existed: true},
			}}
			if outcome == "wrong-database" {
				j.Fingerprint = "another-database"
			}
			write := func(path, value string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for i, item := range j.Items {
				for _, location := range []struct{ path, contents string }{{item.Target, "new"}, {item.Old, "old"}, {item.Stage, "unused-stage"}} {
					path := location.path
					if i == 1 {
						path = filepath.Join(path, "retained.jsonl.gz")
					}
					write(path, location.contents)
				}
			}
			data, err := json.Marshal(j)
			if err != nil {
				t.Fatal(err)
			}
			write(pendingPath(cfg), string(data))
			err = recoverMySQLFiles(ctx, db, cfg)
			unresolved := outcome == "uncertain" || outcome == "wrong-database"
			if (err != nil) != unresolved {
				t.Fatalf("recovery outcome: %v", err)
			}
			want := "new"
			if outcome == "rolled-back" {
				want = "old"
			}
			for _, path := range []string{cfg.DataKeyPath, filepath.Join(cfg.UsageArchiveDir, "retained.jsonl.gz")} {
				data, err := os.ReadFile(path)
				if err != nil || string(data) != want {
					t.Fatalf("recovered file %s = %q, want %q: %v", path, data, want, err)
				}
			}
			for _, path := range []string{pendingPath(cfg), j.Items[0].Old, j.Items[1].Old, j.Items[0].Stage, j.Items[1].Stage} {
				_, err := os.Stat(path)
				if unresolved && err != nil || !unresolved && !os.IsNotExist(err) {
					t.Fatalf("journal/recovery evidence %s: %v", path, err)
				}
			}
			if !unresolved {
				if !backend.locked || !backend.finished {
					t.Fatal("recovery did not resolve outcome with a locking transaction")
				}
				if err := recoverMySQLFiles(ctx, db, cfg); err != nil {
					t.Fatalf("repeated recovery: %v", err)
				}
			}
		})
	}
}

package sqlwrite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	_ "modernc.org/sqlite"
)

func TestSQLitePassThroughTransactionsStatementsAndReturning(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "write.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := sqlwrite.ExecContext(ctx, db, "create table example(id integer primary key, value text)"); err != nil {
		t.Fatal(err)
	}
	row := sqlwrite.QueryRowContext(ctx, db, "insert into example(value) values(?) returning id", "one")
	var count, id int
	if err := db.QueryRow("select count(*) from example").Scan(&count); err != nil || count != 0 {
		t.Fatal("unscanned returning write executed")
	}
	if err := row.Scan(&id); err != nil || id != 1 {
		t.Fatalf("returning: %d %v", id, err)
	}
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "insert into example(value) values(?)", "rolled back"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.PrepareContext(ctx, "insert into example(value) values(?) returning id")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if err := sqlwrite.QueryRowStmtContext(ctx, db, stmt, "two").Scan(&id); err != nil || id != 2 {
		t.Fatalf("prepared returning: %d %v", id, err)
	}
	update, err := db.PrepareContext(ctx, "update example set value=? where id=?")
	if err != nil {
		t.Fatal(err)
	}
	defer update.Close()
	if _, err := sqlwrite.ExecStmtContext(ctx, db, update, "changed", 2); err != nil {
		t.Fatal(err)
	}
	if err := sqlwrite.WithConn(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		_, err := sqlwrite.ExecContext(ctx, conn, "insert into example(value) values(?)", "three")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("select count(*) from example").Scan(&count); err != nil || count != 3 {
		t.Fatalf("SQLite pass-through changed behavior: %d %v", count, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := sqlwrite.BeginTx(canceled, db, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled begin: %v", err)
	}
	if err := sqlwrite.QueryRowContext(canceled, db, "insert into example(value) values(?) returning id", "canceled").Scan(&id); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled returning: %v", err)
	}
}

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

func TestNativeCommentsCannotHideLockingOrTransactionControls(t *testing.T) {
	db, owner, _ := admissionDB(t)
	ctx := context.Background()
	for _, query := range []string{
		"select 1--1 from example for update",
		"select * from (select * from example for update) locked_rows",
		"with locked_rows as (select * from example for share) select * from locked_rows",
		"with locked_rows as (select * from example lock in share mode) select * from locked_rows",
		"-- comment\rselect * from example for update",
	} {
		query = Native(query)
		if _, err := db.ExecContext(ctx, query); !errors.Is(err, ErrWriteAdmissionRequired) {
			t.Fatalf("unadmitted locking exec %q: %v", query, err)
		}
		if rows, err := db.QueryContext(ctx, query); !errors.Is(err, ErrWriteAdmissionRequired) {
			if rows != nil {
				rows.Close()
			}
			t.Fatalf("unadmitted locking query %q: %v", query, err)
		}
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if rows, err := stmt.QueryContext(ctx); !errors.Is(err, ErrWriteAdmissionRequired) {
			if rows != nil {
				rows.Close()
			}
			t.Fatalf("unadmitted prepared locking query %q: %v", query, err)
		}
		stmt.Close()
	}
	for _, native := range []bool{false, true} {
		query := "set @n=1--1, autocommit=0"
		if native {
			query = Native(query)
		}
		if _, err := sqlwrite.ExecContext(ctx, db, query); !errors.Is(err, ErrTransactionControl) {
			t.Fatalf("hidden transaction control accepted: %v", err)
		}
	}
	for _, query := range []string{"select 1--1; delete from example", "select 1; --comment\ndelete from example"} {
		if _, _, err := CompileSQL(Native(query)); err == nil {
			t.Fatalf("multiple native statements accepted: %q", query)
		}
	}
	for _, query := range []string{
		"select ?--?, '?' /* ? */",
		"select ?, ? -- ignored ?\r",
		"select ?, ? # ignored ?\r",
	} {
		compiled, args, err := CompileSQL(Native(query), 8, 3)
		if err != nil || compiled != query || !reflect.DeepEqual(args, []any{8, 3}) {
			t.Fatalf("native comment binding %q: %q %v %v", query, compiled, args, err)
		}
	}
	// Shared SQL retains SQLite's no-whitespace comment semantics when rendered.
	if s, err := compile("select 1--comment"); err != nil || s.write || s.sql != "select 1" {
		t.Fatalf("shared SQL comment: %+v %v", s, err)
	}
	assertReleased(t, db, owner)
}

func TestNativeIgnoreModifiersStillVerifyWarningsAtomically(t *testing.T) {
	for _, query := range []string{
		"insert ignore into example(v) values(?)",
		"insert low_priority ignore into example(v) values(?)",
		"insert high_priority ignore into example(v) select ?",
		"update low_priority ignore example set v=?",
		"with x as (select 1) update ignore example set v=?",
		"delete low_priority quick ignore from example where v=?",
	} {
		for _, useQuery := range []bool{false, true} {
			t.Run(query+map[bool]string{false: "/exec", true: "/query"}[useQuery], func(t *testing.T) {
				db, owner, backend := admissionDB(t)
				backend.warningCount, backend.warningCode = 1, 1048
				var err error
				if useQuery {
					var ignored int
					err = sqlwrite.QueryRowContext(context.Background(), db, Native(query), 1).Scan(&ignored)
				} else {
					_, err = sqlwrite.ExecContext(context.Background(), db, Native(query), 1)
				}
				if err == nil || errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("invalid-data warning accepted: %v", err)
				}
				if !strings.Contains(strings.Join(backend.queries, "\n"), "ROLLBACK") {
					t.Fatal("warning failure was not rolled back")
				}
				assertReleased(t, db, owner)
			})
		}
	}
}

func TestAccelerationSeedsAdmitRuntimePoolAndKeepTransactionPermit(t *testing.T) {
	db, owner, _ := admissionDB(t)
	ctx := context.Background()
	if err := EnsureAccelerationState(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertReleased(t, db, owner)
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := EnsureAccelerationState(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if len(owner.writer) != 1 {
		t.Fatal("seeding released transaction ownership")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertReleased(t, db, owner)
}

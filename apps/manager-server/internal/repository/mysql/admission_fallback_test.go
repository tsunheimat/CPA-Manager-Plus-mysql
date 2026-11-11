package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

func TestBulkIgnoreErrSkipPreservesPreparedFallback(t *testing.T) {
	for _, inTransaction := range []bool{false, true} {
		for _, queryReturning := range []bool{false, true} {
			name := map[bool]string{false: "autocommit", true: "transaction"}[inTransaction] + map[bool]string{false: "/exec", true: "/query"}[queryReturning]
			t.Run(name, func(t *testing.T) {
				db, owner, backend := admissionDB(t)
				ctx := context.Background()
				var exec sqlwrite.Execer = db
				var querier sqlwrite.QueryRower = db
				var tx *sql.Tx
				var err error
				if inTransaction {
					tx, err = sqlwrite.BeginTx(ctx, db, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					exec, querier = tx, tx
					backend.execErrors = []error{nil, driver.ErrSkip} // SAVEPOINT, interpolation declines
				} else {
					backend.execErrors = []error{nil, nil, driver.ErrSkip} // BEGIN, SAVEPOINT, decline
				}
				const query = "insert or ignore into example(v) values(?),(?)"
				if queryReturning {
					var ignored int
					err = sqlwrite.QueryRowContext(ctx, querier, query, "one", "two").Scan(&ignored)
					// An INSERT through QueryContext has no rows, but must have executed
					// successfully through the prepared fallback, not leaked ErrSkip.
					if !errors.Is(err, sql.ErrNoRows) {
						t.Fatalf("write-query prepared fallback: %v", err)
					}
				} else if _, err = sqlwrite.ExecContext(ctx, exec, query, "one", "two"); err != nil {
					t.Fatalf("exec prepared fallback: %v", err)
				}
				inserts := 0
				for _, physical := range backend.queries {
					if strings.HasPrefix(physical, "insert ignore") {
						inserts++
					}
				}
				if inserts != 2 {
					t.Fatalf("missing prepared attempt: %q", backend.queries)
				}
				if tx != nil {
					if len(owner.writer) != 1 {
						t.Fatal("prepared fallback released transaction admission")
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				assertReleased(t, db, owner)
			})
		}
	}
}

func TestBulkIgnoreErrSkipCleanupFailureDoesNotFallBack(t *testing.T) {
	db, owner, backend := admissionDB(t)
	backend.execErrors = []error{nil, nil, driver.ErrSkip}
	backend.cleanupError = true
	_, err := sqlwrite.ExecContext(context.Background(), db, "insert or ignore into example(v) values(?),(?)", 1, 2)
	if err == nil || err == driver.ErrSkip || !strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("cleanup error lost: %v", err)
	}
	inserts := 0
	for _, physical := range backend.queries {
		if strings.HasPrefix(physical, "insert ignore") {
			inserts++
		}
	}
	if inserts != 1 {
		t.Fatal("prepared fallback ran on a failed-cleanup connection")
	}
	backend.cleanupError = false
	assertReleased(t, db, owner)
}

func TestRuntimeImplicitCommitAndOpaqueSessionCommandsAreRejected(t *testing.T) {
	for _, query := range []string{
		"FLUSH TABLES", "RESET REPLICA", "CALL commit_inside_procedure()",
		"SET PASSWORD = 'not-a-real-password'", "INSTALL PLUGIN example SONAME 'example.so'",
	} {
		t.Run(query, func(t *testing.T) {
			db, owner, backend := admissionDB(t)
			ctx := context.Background()
			if _, err := sqlwrite.ExecContext(ctx, db, Native(query)); !errors.Is(err, ErrTransactionControl) {
				t.Fatalf("opaque runtime operation accepted: %v", err)
			}
			tx, err := sqlwrite.BeginTx(ctx, db, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, Native(query)); !errors.Is(err, ErrTransactionControl) {
				t.Fatalf("implicit transaction control reached driver: %v", err)
			}
			stmt, err := tx.PrepareContext(ctx, Native(query))
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			if rows, err := stmt.QueryContext(ctx); !errors.Is(err, ErrTransactionControl) {
				if rows != nil {
					rows.Close()
				}
				t.Fatalf("prepared implicit transaction control reached driver: %v", err)
			}
			if len(backend.queries) != 0 || len(owner.writer) != 1 {
				t.Fatalf("opaque statement changed transaction ownership: %q", backend.queries)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			assertReleased(t, db, owner)
		})
	}
}

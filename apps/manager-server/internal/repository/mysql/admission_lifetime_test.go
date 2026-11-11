package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

func TestAdmissionQueryRowsOwnPermitUntilClosedOrCanceled(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, cancelRows := range []bool{false, true} {
			t.Run(map[bool]string{false: "direct", true: "prepared"}[prepared]+map[bool]string{false: "-close", true: "-cancel"}[cancelRows], func(t *testing.T) {
				db, owner, _ := admissionDB(t)
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx, release, err := db.Driver().(sqlwrite.Admitter).AdmitWrite(parent)
				if err != nil {
					t.Fatal(err)
				}
				var rows *sql.Rows
				if prepared {
					stmt, err := db.PrepareContext(ctx, Native("update example set v=?"))
					if err != nil {
						t.Fatal(err)
					}
					defer stmt.Close()
					rows, err = stmt.QueryContext(ctx, 1)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					rows, err = db.QueryContext(ctx, Native("with x as (select 1) update example set v=?"), 1)
					if err != nil {
						t.Fatal(err)
					}
				}
				release() // cannot return the permit while the driver's result is open
				if len(owner.writer) != 1 {
					t.Fatal("write rows released admission early")
				}
				if cancelRows {
					cancel()
					waitAdmission(t, func() bool { return len(owner.writer) == 0 })
				} else if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				release()
				assertReleased(t, db, owner)
			})
		}
	}
}

func TestAdmissionWithConnAndScanFailuresRelease(t *testing.T) {
	db, owner, _ := admissionDB(t)
	if err := sqlwrite.WithConn(context.Background(), db, func(context.Context, *sql.Conn) error { return errors.New("callback failed") }); err == nil {
		t.Fatal("missing callback error")
	}
	assertReleased(t, db, owner)
	var id int
	if err := sqlwrite.QueryRowContext(context.Background(), db, "insert into example(v) values(?) returning id").Scan(&id); err == nil {
		t.Fatal("missing bind error")
	}
	if err := sqlwrite.QueryRowContext(context.Background(), db, "insert into example(v) values(1) returning id").Scan(); err == nil {
		t.Fatal("missing scan error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	row := sqlwrite.QueryRowContext(ctx, db, "insert into example(v) values(1) returning id")
	cancel()
	if err := row.Scan(&id); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan %v", err)
	}
	assertReleased(t, db, owner)
}

func TestAdmissionWarningFailuresCannotCommitOrRetryAmbiguousWrites(t *testing.T) {
	t.Run("failed savepoint rollback", func(t *testing.T) {
		db, owner, b := admissionDB(t)
		ctx := context.Background()
		tx, err := sqlwrite.BeginTx(ctx, db, nil)
		if err != nil {
			t.Fatal(err)
		}
		b.warningCount, b.warningCode, b.cleanupError = 1, 1048, true
		if _, err := tx.ExecContext(ctx, "insert or ignore into example(v) values(1),(2)"); err == nil {
			t.Fatal("bad warning accepted")
		}
		if err := tx.Commit(); err == nil {
			t.Fatal("committed despite failed rollback")
		}
		b.cleanupError = false
		assertReleased(t, db, owner)
	})
	t.Run("ambiguous internal commit", func(t *testing.T) {
		db, owner, b := admissionDB(t)
		// START, SAVEPOINT, INSERT succeed; COMMIT loses the connection.
		b.execErrors = []error{nil, nil, nil, driver.ErrBadConn}
		_, err := sqlwrite.ExecContext(context.Background(), db, "insert or ignore into example(v) values(1),(2)")
		if err == nil || errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("retryable commit outcome: %v", err)
		}
		writes := 0
		for _, query := range b.queries {
			if strings.HasPrefix(query, "insert") {
				writes++
			}
		}
		if writes != 1 {
			t.Fatalf("ambiguous write retried %d times", writes)
		}
		assertReleased(t, db, owner)
	})
}

func TestAdmissionNativeCancellationDoesNotReachDriver(t *testing.T) {
	db, owner, b := admissionDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sqlwrite.ExecContext(ctx, db, Native("insert into example(v) values(?)"), "bound"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation %v", err)
	}
	if len(b.queries) != 0 || len(owner.writer) != 0 {
		t.Fatal("canceled native write executed or retained permit")
	}
}

func TestAdmissionPermitIsNotReleasedDuringPhysicalAttempt(t *testing.T) {
	_, owner, _ := admissionDB(t)
	ctx, release, err := (&dialectDriver{owner: owner}).AdmitWrite(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn := &connection{owner: owner}
	permit, err := conn.borrowPermit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if len(owner.writer) != 1 {
		t.Fatal("in-flight operation lost permit")
	}
	permit.finishAttempt(false)
	if len(owner.writer) != 0 {
		t.Fatal("completed operation retained abandoned permit")
	}
	if _, err := conn.borrowPermit(ctx); !errors.Is(err, ErrWritePermitUsed) {
		t.Fatalf("expired permit reused: %v", err)
	}
	canceled, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-canceled.Done()
	if _, _, err := (&dialectDriver{owner: owner}).AdmitWrite(canceled); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-cancel %v", err)
	}
}

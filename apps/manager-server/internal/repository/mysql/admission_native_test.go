package mysql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

func TestMySQLQueuedWritersKeepNativeReadCapacity(t *testing.T) {
	db := openMySQL(t) // explicit CPA_TEST_MYSQL_DSN opt-in; never provisions a server
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	before := db.Stats().WaitCount
	queueCtx, cancelQueue := context.WithCancel(ctx)
	defer cancelQueue()
	const writers = 24
	done := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() {
			_, err := sqlwrite.ExecContext(queueCtx, db, "delete from settings where `key`='missing-admission-test'")
			done <- err
		}()
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats, ok := mysqlrepo.WriteAdmissionStats(db)
		if ok && stats.Waiting == writers {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("native writers did not reach pre-checkout admission")
		}
	}
	if stats := db.Stats(); stats.InUse != 1 || stats.WaitCount != before {
		t.Fatalf("writer waiters checked out native connections: %+v", stats)
	}
	readCtx, cancelRead := context.WithTimeout(ctx, time.Second)
	defer cancelRead()
	var value int
	if err := db.QueryRowContext(readCtx, mysqlrepo.Native("select 1")).Scan(&value); err != nil || value != 1 {
		t.Fatalf("native cheap read starved: %d %v", value, err)
	}
	cancelQueue()
	for i := 0; i < writers; i++ {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("queued writer cancellation: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlwrite.ExecContext(ctx, db, mysqlrepo.Native("delete from settings where `key`=?"), "missing-admission-test"); err != nil {
		t.Fatalf("native post-commit write: %v", err)
	}
}

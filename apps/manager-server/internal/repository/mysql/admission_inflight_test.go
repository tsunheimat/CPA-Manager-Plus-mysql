package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

// Simulate a physical driver which is still draining/cleaning up after its
// context is canceled. Admission belongs to the operation, not a ctx watcher.
type inflightConnector struct {
	admissionConnector
	started chan<- struct{}
	finish  <-chan struct{}
}

func (c inflightConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.admissionConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &inflightConn{admissionConn: conn.(*admissionConn), started: c.started, finish: c.finish}, nil
}

type inflightConn struct {
	*admissionConn
	started chan<- struct{}
	finish  <-chan struct{}
}

func (c *inflightConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(q, "42") {
		c.started <- struct{}{}
		<-c.finish
	}
	return c.admissionConn.ExecContext(ctx, q, args)
}

func TestAdmissionCanceledNativeWriteHoldsPermitUntilDriverReturns(t *testing.T) {
	started, finish := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(finish) }) }
	defer unblock()
	owner := &connector{base: inflightConnector{admissionConnector{&admissionBackend{}}, started, finish}, writer: make(chan struct{}, 1)}
	db := sql.OpenDB(owner)
	db.SetMaxOpenConns(2)
	defer func() { unblock(); db.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := sqlwrite.ExecContext(ctx, db, Native("update example set v=42"))
		first <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not start")
	}
	cancel()
	queuedCtx, cancelQueued := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelQueued()
	second := make(chan error, 1)
	go func() {
		_, err := sqlwrite.ExecContext(queuedCtx, db, "update example set v=2")
		second <- err
	}()
	waitAdmission(t, func() bool { return owner.waiting.Load() == 1 })
	if len(owner.writer) != 1 || db.Stats().InUse != 1 {
		t.Fatal("cancellation returned admission while the driver was still active")
	}
	var value int
	if err := db.QueryRowContext(queuedCtx, Native("select 1")).Scan(&value); err != nil || value != 1 {
		t.Fatalf("cleanup/writer waiter starved reads: %d %v", value, err)
	}
	unblock()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight cancellation: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("next writer: %v", err)
	}
	assertReleased(t, db, owner)
}

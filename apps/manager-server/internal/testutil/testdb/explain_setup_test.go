package testdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

const explainFixtureDSN = "registered-isolated-plan-fixture"

func registeredExplainFixture(t *testing.T) *sql.DB {
	t.Helper()
	// Registry-only fixture: no real driver, DSN parsing, network or schema work.
	db := &sql.DB{}
	paths.Lock()
	paths.pools[db] = explainFixtureDSN
	paths.Unlock()
	t.Cleanup(func() {
		paths.Lock()
		delete(paths.pools, db)
		paths.Unlock()
	})
	return db
}

func TestExplainMySQLSetupRejectsUnregisteredPool(t *testing.T) {
	t.Setenv("CPA_TEST_MYSQL_DSN", "must-not-be-used")
	called := false
	conn, cleanup, err := openExplainConnection(context.Background(), &sql.DB{}, func(context.Context, string) (*sql.DB, error) {
		called = true
		return nil, errors.New("unexpected opener")
	})
	if err == nil || !strings.Contains(err.Error(), "requires a pool opened by testdb.Open") || called || conn != nil || cleanup != nil {
		t.Fatalf("unregistered target escaped isolation: called=%t conn=%v err=%v", called, conn, err)
	}
}

func TestExplainMySQLSetupCanceledBeforeOpen(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "canceled"
		if expired {
			name = "deadline_expired"
		}
		t.Run(name, func(t *testing.T) {
			db := registeredExplainFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			want := context.Canceled
			if expired {
				ctx, cancel = context.WithDeadline(context.Background(), time.Unix(0, 0))
				defer cancel()
				want = context.DeadlineExceeded
			}
			called := false
			conn, cleanup, err := openExplainConnection(ctx, db, func(context.Context, string) (*sql.DB, error) {
				called = true
				return nil, errors.New("unexpected opener")
			})
			if !errors.Is(err, want) || called || conn != nil || cleanup != nil {
				t.Fatalf("expired setup attempted open: called=%t conn=%v err=%v want=%v", called, conn, err, want)
			}
		})
	}
}

func TestExplainMySQLSetupDeadlineDuringOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := registeredExplainFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		deadline, _ := ctx.Deadline()
		calls := 0
		conn, cleanup, err := openExplainConnection(ctx, db, func(got context.Context, dsn string) (*sql.DB, error) {
			calls++
			gotDeadline, hasDeadline := got.Deadline()
			if got != ctx || !hasDeadline || gotDeadline != deadline || dsn != explainFixtureDSN {
				t.Fatalf("native opener lost request context or isolated target: deadline=%v target=%q", gotDeadline, dsn)
			}
			// synctest advances time only when this blocks: no real sleeps,
			// network stall, machine-speed assumption or short timing assertion.
			<-got.Done()
			return nil, got.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || conn != nil || cleanup != nil {
			t.Fatalf("deadline not honored during native setup: calls=%d conn=%v err=%v", calls, conn, err)
		}
	})
}

func TestExplainMySQLSetupPreservesOpenError(t *testing.T) {
	db := registeredExplainFixture(t)
	failure := errors.New("injected native opener failure")
	conn, cleanup, err := openExplainConnection(context.Background(), db, func(context.Context, string) (*sql.DB, error) {
		return nil, failure
	})
	if !errors.Is(err, failure) || conn != nil || cleanup != nil {
		t.Fatalf("native opener failure lost: conn=%v err=%v", conn, err)
	}
}

func TestExplainMySQLSetupClosesPoolOnCheckoutDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := registeredExplainFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		calls := 0
		admin := sql.OpenDB(explainConnector{connect: func(got context.Context) (driver.Conn, error) {
			calls++
			if got != ctx {
				t.Fatal("connection checkout lost the caller context")
			}
			<-got.Done()
			return nil, got.Err()
		}})
		defer admin.Close()
		conn, cleanup, err := openExplainConnection(ctx, db, func(got context.Context, dsn string) (*sql.DB, error) {
			if got != ctx || dsn != explainFixtureDSN {
				t.Fatal("opener lost context or target")
			}
			return admin, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || conn != nil || cleanup != nil {
			t.Fatalf("checkout deadline not honored: calls=%d conn=%v err=%v", calls, conn, err)
		}
		// A closed pool rejects a fresh live context without another Connect.
		if _, err := admin.Conn(context.Background()); err == nil || calls != 1 {
			t.Fatalf("failed checkout leaked admin pool: calls=%d err=%v", calls, err)
		}
	})
}

func TestExplainMySQLSetupCleansUpConnectionAndPool(t *testing.T) {
	db := registeredExplainFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connects, closes := 0, 0
	admin := sql.OpenDB(explainConnector{connect: func(got context.Context) (driver.Conn, error) {
		connects++
		if got != ctx {
			t.Fatal("connection checkout lost the caller context")
		}
		return &explainDriverConn{closes: &closes}, nil
	}})
	defer admin.Close()
	conn, cleanup, err := openExplainConnection(ctx, db, func(got context.Context, dsn string) (*sql.DB, error) {
		if got != ctx || dsn != explainFixtureDSN {
			t.Fatal("opener lost context or target")
		}
		return admin, nil
	})
	if err != nil || conn == nil || cleanup == nil {
		t.Fatalf("fixture setup failed: conn=%v err=%v", conn, err)
	}
	defer cleanup()
	if err := conn.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	cleanup()
	if err := conn.PingContext(ctx); !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("inspection connection still usable: %v", err)
	}
	if _, err := admin.Conn(context.Background()); err == nil || connects != 1 || closes != 1 {
		t.Fatalf("inspection leaked pool/connection: connects=%d closes=%d err=%v", connects, closes, err)
	}
}

// Minimal in-memory driver only exercises database/sql acquisition/cleanup.
// It cannot execute statements, open a native connection or mutate a schema.
type explainConnector struct {
	connect func(context.Context) (driver.Conn, error)
}

func (c explainConnector) Connect(ctx context.Context) (driver.Conn, error) { return c.connect(ctx) }
func (explainConnector) Driver() driver.Driver                              { return explainDriver{} }

type explainDriver struct{}

func (explainDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("direct driver opens are not supported")
}

type explainDriverConn struct{ closes *int }

func (c *explainDriverConn) Close() error {
	*c.closes++
	return nil
}
func (*explainDriverConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("statements are not supported")
}
func (*explainDriverConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

type admissionBackend struct {
	mu                                     sync.Mutex
	connectErrors, beginErrors, execErrors []error
	commitError, rollbackError             error
	queries                                []string
	arguments                              [][]driver.NamedValue
	rollbacks                              int
	warningCount                           int64
	warningCode                            int64
	cleanupError                           bool
}
type admissionConnector struct{ backend *admissionBackend }

func (c admissionConnector) Driver() driver.Driver { return admissionDriver{} }
func (c admissionConnector) Connect(context.Context) (driver.Conn, error) {
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	if err := popError(&c.backend.connectErrors); err != nil {
		return nil, err
	}
	return &admissionConn{backend: c.backend}, nil
}

type admissionDriver struct{}

func (admissionDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector required")
}

type admissionConn struct{ backend *admissionBackend }

func (*admissionConn) Close() error                       { return nil }
func (*admissionConn) ResetSession(context.Context) error { return nil }
func (*admissionConn) IsValid() bool                      { return true }
func (c *admissionConn) Prepare(q string) (driver.Stmt, error) {
	return &admissionStmt{conn: c, query: q}, nil
}
func (c *admissionConn) PrepareContext(_ context.Context, q string) (driver.Stmt, error) {
	return c.Prepare(q)
}
func (c *admissionConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *admissionConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	if err := popError(&c.backend.beginErrors); err != nil {
		return nil, err
	}
	return admissionTx{backend: c.backend}, nil
}
func popError(queue *[]error) error {
	if len(*queue) == 0 {
		return nil
	}
	err := (*queue)[0]
	*queue = (*queue)[1:]
	return err
}
func (c *admissionConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.backend.queries = append(c.backend.queries, q)
	c.backend.arguments = append(c.backend.arguments, append([]driver.NamedValue(nil), args...))
	if c.backend.cleanupError && strings.HasPrefix(q, "ROLLBACK") {
		return nil, errors.New("rollback failed")
	}
	if err := popError(&c.backend.execErrors); err != nil {
		return nil, err
	}
	return admissionResult{}, nil
}
func (c *admissionConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.backend.mu.Lock()
	defer c.backend.mu.Unlock()
	c.backend.queries = append(c.backend.queries, q)
	c.backend.arguments = append(c.backend.arguments, append([]driver.NamedValue(nil), args...))
	switch q {
	case "SHOW COUNT(*) WARNINGS":
		return &admissionRows{cols: []string{"count"}, values: [][]driver.Value{{c.backend.warningCount}}}, nil
	case "SHOW WARNINGS":
		return &admissionRows{cols: []string{"level", "code", "message"}, values: [][]driver.Value{{"Warning", c.backend.warningCode, "detail"}}}, nil
	}
	return &admissionRows{cols: []string{"value"}, values: [][]driver.Value{{int64(1)}}}, nil
}

type admissionTx struct{ backend *admissionBackend }

func (t admissionTx) Commit() error {
	t.backend.mu.Lock()
	defer t.backend.mu.Unlock()
	return t.backend.commitError
}
func (t admissionTx) Rollback() error {
	t.backend.mu.Lock()
	defer t.backend.mu.Unlock()
	t.backend.rollbacks++
	return t.backend.rollbackError
}

type admissionResult struct{}

func (admissionResult) LastInsertId() (int64, error) { return 17, nil }
func (admissionResult) RowsAffected() (int64, error) { return 1, nil }

type admissionStmt struct {
	conn  *admissionConn
	query string
}

func (*admissionStmt) Close() error  { return nil }
func (*admissionStmt) NumInput() int { return -1 }
func (s *admissionStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}
func (s *admissionStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}
func (s *admissionStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}
func (s *admissionStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

type admissionRows struct {
	cols   []string
	values [][]driver.Value
}

func (r *admissionRows) Columns() []string { return r.cols }
func (*admissionRows) Close() error        { return nil }
func (r *admissionRows) Next(out []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(out, r.values[0])
	r.values = r.values[1:]
	return nil
}

func admissionDB(t *testing.T) (*sql.DB, *connector, *admissionBackend) {
	t.Helper()
	backend := &admissionBackend{}
	owner := &connector{base: admissionConnector{backend}, writer: make(chan struct{}, 1)}
	db := sql.OpenDB(owner)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	t.Cleanup(func() { db.Close() })
	return db, owner, backend
}
func waitAdmission(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for admission state")
		}
		time.Sleep(time.Millisecond)
	}
}
func assertReleased(t *testing.T, db *sql.DB, owner *connector) {
	t.Helper()
	waitAdmission(t, func() bool { return len(owner.writer) == 0 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := sqlwrite.ExecContext(ctx, db, "update example set v=1"); err != nil {
		t.Fatalf("post-completion write: %v", err)
	}
	if len(owner.writer) != 0 {
		t.Fatal("write leaked its permit")
	}
}

func TestAdmissionWaitersNeverConsumeReadConnections(t *testing.T) {
	db, owner, _ := admissionDB(t)
	tx, err := sqlwrite.BeginTx(context.Background(), db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const writers = 20
	done := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() { _, err := sqlwrite.ExecContext(ctx, db, "update example set v=2"); done <- err }()
	}
	waitAdmission(t, func() bool { return owner.waiting.Load() == writers })
	if stats := db.Stats(); stats.InUse != 1 || stats.WaitCount != 0 {
		t.Fatalf("queued writers checked out pool connections: %+v", stats)
	}
	readCtx, readCancel := context.WithTimeout(context.Background(), time.Second)
	defer readCancel()
	var value int
	if err := db.QueryRowContext(readCtx, "select 1").Scan(&value); err != nil || value != 1 {
		t.Fatalf("read starved: %d %v", value, err)
	}
	cancel()
	for i := 0; i < writers; i++ {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("writer cancellation: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertReleased(t, db, owner)
	stats, ok := WriteAdmissionStats(db)
	if !ok || stats.Canceled != writers || stats.Waiting != 0 || stats.HoldDuration <= 0 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestAdmissionTransactionCancellationAndFinishErrors(t *testing.T) {
	for _, mode := range []string{"cancel", "commit", "rollback", "commit-error", "rollback-error"} {
		t.Run(mode, func(t *testing.T) {
			db, owner, backend := admissionDB(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx, err := sqlwrite.BeginTx(ctx, db, nil)
			if err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			if mode == "commit-error" {
				backend.commitError = errors.New("commit failed")
			}
			if mode == "rollback-error" {
				backend.rollbackError = errors.New("rollback failed")
			}
			backend.mu.Unlock()
			switch mode {
			case "cancel":
				cancel()
			case "commit", "commit-error":
				err = tx.Commit()
			default:
				err = tx.Rollback()
			}
			if strings.HasSuffix(mode, "error") && err == nil {
				t.Fatal("missing finish error")
			}
			assertReleased(t, db, owner)
			_ = tx.Rollback()
			if len(owner.writer) != 0 {
				t.Fatal("double finish changed admission")
			}
		})
	}
}

func TestAdmissionPoolBeginAndRetryFailures(t *testing.T) {
	for _, mode := range []string{"closed", "connect", "begin", "begin-bad-connection", "exec-bad-connection", "exec-error", "pool-cancel"} {
		t.Run(mode, func(t *testing.T) {
			db, owner, backend := admissionDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			sentinel := errors.New("injected")
			switch mode {
			case "closed":
				db.Close()
			case "connect":
				backend.connectErrors = []error{sentinel}
			case "begin":
				backend.beginErrors = []error{sentinel}
			case "begin-bad-connection":
				backend.beginErrors = []error{driver.ErrBadConn}
			case "exec-bad-connection":
				backend.execErrors = []error{driver.ErrBadConn}
			case "exec-error":
				backend.execErrors = []error{sentinel}
			case "pool-cancel":
				db.SetMaxOpenConns(1)
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
			}
			var err error
			if strings.HasPrefix(mode, "exec") {
				_, err = sqlwrite.ExecContext(ctx, db, "insert into example(v) values(?)", 1)
			} else {
				var tx *sql.Tx
				tx, err = sqlwrite.BeginTx(ctx, db, nil)
				if err == nil {
					err = tx.Commit()
				}
			}
			if strings.Contains(mode, "bad-connection") {
				if err != nil {
					t.Fatalf("retry: %v", err)
				}
			} else if err == nil {
				t.Fatal("expected failure")
			}
			if len(owner.writer) != 0 {
				t.Fatal("failure/retry leaked permit")
			}
		})
	}
}

func TestAdmissionFailClosedAcrossNativeCTEsAndPreparedQueries(t *testing.T) {
	db, owner, _ := admissionDB(t)
	ctx := context.Background()
	for _, query := range []string{"update example set v=1", "with x as (select 1) update example set v=1", "/* select */ delete from example", "# read?\ninsert into example(v) values(1)", Native("with x as (select 1) delete from example"), Native("insert into example(v) values(?)")} {
		args := []any{}
		if strings.Contains(query, "?") {
			if strings.HasPrefix(query, nativePrefix) {
				args = []any{1}
			}
		}
		if _, err := db.ExecContext(ctx, query, args...); !errors.Is(err, ErrWriteAdmissionRequired) {
			t.Fatalf("exec %q: %v", query, err)
		}
		rows, err := db.QueryContext(ctx, query, args...)
		if rows != nil {
			rows.Close()
		}
		if !errors.Is(err, ErrWriteAdmissionRequired) {
			t.Fatalf("query %q: %v", query, err)
		}
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stmt.ExecContext(ctx, args...); !errors.Is(err, ErrWriteAdmissionRequired) {
			t.Fatalf("prepared exec: %v", err)
		}
		rows, err = stmt.QueryContext(ctx, args...)
		if rows != nil {
			rows.Close()
		}
		if !errors.Is(err, ErrWriteAdmissionRequired) {
			t.Fatalf("prepared query: %v", err)
		}
		if _, err := sqlwrite.ExecStmtContext(ctx, db, stmt, args...); err != nil {
			t.Fatal(err)
		}
		stmt.Close()
	}
	if _, err := db.BeginTx(ctx, nil); !errors.Is(err, ErrWriteAdmissionRequired) {
		t.Fatalf("direct begin: %v", err)
	}
	reader, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ExecContext(ctx, Native("delete from example")); !errors.Is(err, ErrReadOnlyTransaction) {
		t.Fatalf("read-only write: %v", err)
	}
	reader.Rollback()
	for _, q := range []string{"BEGIN", "START TRANSACTION", "COMMIT", "SET autocommit=0", Native("ROLLBACK"), "set session transaction read write"} {
		if _, err := sqlwrite.ExecContext(ctx, db, q); !errors.Is(err, ErrTransactionControl) {
			t.Fatalf("transaction escape %q: %v", q, err)
		}
	}
	assertReleased(t, db, owner)
}

func TestAdmissionNativeStatementScopeAndBinding(t *testing.T) {
	db, owner, backend := admissionDB(t)
	ctx := context.Background()
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, Native("update example set v = ? / length(?)"), 7, "界"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "update example set v = ? / length(?)", 7, "界"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	queries := append([]string(nil), backend.queries...)
	args := append([]driver.NamedValue(nil), backend.arguments[0]...)
	backend.mu.Unlock()
	if len(queries) != 2 || queries[0] != "update example set v = ? / length(?)" || !strings.Contains(queries[1], "div char_length") {
		t.Fatalf("native leaked to nested call: %v", queries)
	}
	if !reflect.DeepEqual(args, []driver.NamedValue{{Ordinal: 1, Value: int64(7)}, {Ordinal: 2, Value: "界"}}) {
		t.Fatalf("bound args: %v", args)
	}
	assertReleased(t, db, owner)
}

func TestAdmissionReturningAndExplicitConnectionLifetime(t *testing.T) {
	db, owner, _ := admissionDB(t)
	ctx := context.Background()
	row := sqlwrite.QueryRowContext(ctx, db, "insert into example(v) values(?) returning id", 1)
	if len(owner.writer) != 0 || db.Stats().InUse != 0 {
		t.Fatal("unscanned row obtained admission")
	}
	var id int
	if err := row.Scan(&id); err != nil || id != 17 {
		t.Fatalf("returning: %d %v", id, err)
	}
	if err := row.Scan(&id); err == nil {
		t.Fatal("double scan succeeded")
	}
	stmt, err := db.PrepareContext(ctx, "insert into example(v) values(?) returning id")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if err := sqlwrite.QueryRowStmtContext(ctx, db, stmt, 2).Scan(&id); err != nil || id != 17 {
		t.Fatalf("prepared returning: %d %v", id, err)
	}
	err = sqlwrite.WithConn(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, Native("insert into example(v) values(1)")); err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, "delete from example"); !errors.Is(err, ErrWritePermitUsed) {
			t.Fatalf("reused transaction permit: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	other, _, _ := admissionDB(t)
	err = sqlwrite.WithConn(ctx, db, func(ctx context.Context, _ *sql.Conn) error {
		_, err := other.ExecContext(ctx, "delete from example")
		return err
	})
	if !errors.Is(err, ErrWriteAdmissionRequired) {
		t.Fatalf("cross connector permit: %v", err)
	}
	assertReleased(t, db, owner)
}

func TestAdmissionWarningRollbackAndDuplicateOnly(t *testing.T) {
	for _, mode := range []string{"duplicate", "invalid", "truncated", "cleanup-error", "single-duplicate", "single-invalid"} {
		t.Run(mode, func(t *testing.T) {
			db, owner, backend := admissionDB(t)
			backend.warningCount, backend.warningCode = 1, 1062
			if mode == "invalid" || mode == "cleanup-error" {
				backend.warningCode = 1048
			}
			if mode == "truncated" {
				backend.warningCount = 2
			}
			if mode == "cleanup-error" {
				backend.cleanupError = true
			}
			query := "insert or ignore into example(v) values(1),(2)"
			if strings.HasPrefix(mode, "single") {
				query = "insert or ignore into example(v) values(1)"
				code := uint16(1062)
				if mode == "single-invalid" {
					code = 1048
				}
				backend.execErrors = []error{&mysqldriver.MySQLError{Number: code}}
			}
			_, err := sqlwrite.ExecContext(context.Background(), db, query)
			if mode == "duplicate" || mode == "single-duplicate" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("unsafe warning was accepted")
			}
			backend.mu.Lock()
			queries := append([]string(nil), backend.queries...)
			backend.cleanupError = false
			backend.mu.Unlock()
			if mode == "invalid" || mode == "truncated" || mode == "cleanup-error" {
				if !strings.Contains(strings.Join(queries, "\n"), "ROLLBACK") {
					t.Fatal("failed bulk not rolled back")
				}
			}
			assertReleased(t, db, owner)
		})
	}
}

func TestNativeCompilerAndSQLClassification(t *testing.T) {
	q, args, err := CompileSQL(Native("select ? / length(?), '?' /* ? */"), 5, "界")
	if err != nil || q != "select ? / length(?), '?' /* ? */" || !reflect.DeepEqual(args, []any{5, "界"}) {
		t.Fatalf("native compile: %q %v %v", q, args, err)
	}
	q, args, err = CompileSQL("select cpamp_analytics_model(?)", "m(high)")
	tokens, _ := lex(q)
	placeholders := 0
	for _, token := range tokens {
		if token == "?" {
			placeholders++
		}
	}
	if err != nil || len(args) < 2 || placeholders != len(args) {
		t.Fatalf("expanded compile: %q %v %v", q, args, err)
	}
	for _, bad := range []string{Native("select 1; delete from example"), Native("/*!80000 delete from example */"), Native("select ?")} {
		if _, _, err := CompileSQL(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	for _, query := range []string{"with x as (select 1), y(z) as (select 2) select * from y", "/* read */ select 'update'", "select 1; -- trailing"} {
		s, err := compile(query)
		if err != nil || s.write {
			t.Fatalf("read classification: %q %+v %v", query, s, err)
		}
	}
}

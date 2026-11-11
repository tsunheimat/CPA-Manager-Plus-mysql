package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type connector struct {
	base         driver.Connector
	writer       chan struct{}
	cache        sync.Map
	cacheEntries atomic.Int32
	waiting      atomic.Int64
	acquired     atomic.Uint64
	canceled     atomic.Uint64
	waitNanos    atomic.Int64
	holdNanos    atomic.Int64
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	base, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &connection{Conn: base, owner: c}, nil
}

func (c *connector) Driver() driver.Driver { return &dialectDriver{owner: c} }

type dialectDriver struct{ owner *connector }

func (*dialectDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use MySQL connector")
}

type connection struct {
	driver.Conn
	owner   *connector
	inTx    bool
	writeTx bool
	invalid bool
}

func (c *connection) compile(query string) (statement, error) {
	if cached, ok := c.owner.cache.Load(query); ok {
		return cached.(statement), nil
	}
	s, err := compile(query)
	if err == nil {
		// Most dynamically built analytics statements contain literals. Keep
		// the cache bounded by caching only short, reusable queries.
		if len(query) <= 8192 && c.owner.cacheEntries.Load() < 512 && c.owner.cacheEntries.Add(1) <= 512 {
			// Cache misses affect CPU only, never correctness.
			c.owner.cache.Store(query, s)
		}
	}
	return s, err
}

func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.invalid {
		return nil, driver.ErrBadConn
	}
	if c.inTx {
		return nil, errors.New("MySQL connection already has a transaction")
	}
	// Admission happened before database/sql checked out this connection.
	// Borrow across this physical attempt; on ErrBadConn the helper retains
	// the same permit for database/sql's retry on another connection.
	var permit *writePermit
	var err error
	if !opts.ReadOnly {
		permit, err = c.borrowPermit(ctx)
		if err != nil {
			return nil, err
		}
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if permit != nil {
		permit.finishAttempt(err == nil)
	}
	if err != nil {
		return nil, err
	}
	c.inTx, c.writeTx = true, !opts.ReadOnly
	return &transaction{Tx: tx, conn: c, permit: permit}, nil
}

type transaction struct {
	driver.Tx
	conn   *connection
	permit *writePermit
	once   sync.Once
}

func (t *transaction) done(err error) {
	t.once.Do(func() {
		// A failed finish must not return a potentially still-open transaction
		// to the pool. Preserve its error (do not return ErrBadConn and retry a
		// possibly committed operation), but make Validator discard the conn.
		if err != nil {
			t.conn.invalid = true
			_ = t.conn.Conn.Close()
		}
		t.conn.inTx, t.conn.writeTx = false, false
		if t.permit != nil {
			t.permit.finishTransaction()
		}
	})
}
func (t *transaction) Commit() (err error) {
	defer func() { t.done(err) }()
	if t.conn.invalid {
		return errors.New("MySQL transaction connection was discarded during cleanup")
	}
	return t.Tx.Commit()
}
func (t *transaction) Rollback() (err error) {
	defer func() { t.done(err) }()
	return t.Tx.Rollback()
}

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := c.compile(query)
	if err != nil {
		return nil, err
	}
	prepared, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, s.sql)
	if err != nil {
		return nil, err
	}
	return &preparedStatement{Stmt: prepared, conn: c, statement: s}, nil
}

func (c *connection) CheckNamedValue(v *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(v)
	}
	return driver.ErrSkip
}
func (c *connection) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return ctx.Err()
}
func (c *connection) ResetSession(ctx context.Context) error {
	if c.invalid {
		return driver.ErrBadConn
	}
	if resetter, ok := c.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return ctx.Err()
}
func (c *connection) IsValid() bool {
	if c.invalid {
		return false
	}
	if validator, ok := c.Conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	s, err := c.compile(query)
	if err != nil {
		return nil, err
	}
	args, err = s.bind(args)
	if err != nil {
		return nil, err
	}
	return c.execute(ctx, s, func() (driver.Result, error) { return c.Conn.(driver.ExecerContext).ExecContext(ctx, s.sql, args) })
}

func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	s, err := c.compile(query)
	if err != nil {
		return nil, err
	}
	args, err = s.bind(args)
	if err != nil {
		return nil, err
	}
	if s.returningID || s.ignore {
		result, err := c.execute(ctx, s, func() (driver.Result, error) { return c.Conn.(driver.ExecerContext).ExecContext(ctx, s.sql, args) })
		if s.returningID {
			return returnedID(result, err)
		}
		return &emptyRows{}, err
	}
	return c.query(ctx, s, func() (driver.Rows, error) { return c.Conn.(driver.QueryerContext).QueryContext(ctx, s.sql, args) })
}

func (c *connection) rawExec(ctx context.Context, query string) error {
	_, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, nil)
	return err
}

// enter never queues: a missing permit is a programming error, not a reason to
// hold a pooled connection waiting behind the writer. Reads need no permit.
func (c *connection) enter(ctx context.Context, s statement) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.invalid {
		return nil, driver.ErrBadConn
	}
	if s.txControl || s.ddl && c.inTx {
		return nil, ErrTransactionControl
	}
	if !s.write {
		return func() {}, nil
	}
	if c.inTx {
		if !c.writeTx {
			return nil, ErrReadOnlyTransaction
		}
		return func() {}, nil
	}
	permit, err := c.borrowPermit(ctx)
	if err != nil {
		return nil, err
	}
	return func() { permit.finishAttempt(false) }, nil
}

func (c *connection) discard() {
	c.invalid = true
	_ = c.Conn.Close()
}

// cleanup must finish before returning admission, even after request cancellation.
// A broken rollback/savepoint makes the connection unusable; never put it back.
func (c *connection) cleanup(query string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.rawExec(ctx, query); err != nil {
		c.discard()
		return fmt.Errorf("MySQL %s cleanup failed: %v", query, err)
	}
	return nil
}

// database/sql recognizes driver.ErrSkip by identity, not errors.Is. Successful
// cleanup must preserve it so interpolation/packet-size fallbacks can execute a
// prepared statement under the same admission. Failed cleanup is never retried.
func joinCleanupError(err, cleanupErr error) error {
	if cleanupErr == nil {
		return err
	}
	return errors.Join(err, cleanupErr)
}

func (c *connection) execute(ctx context.Context, s statement, run func() (driver.Result, error)) (driver.Result, error) {
	finish, err := c.enter(ctx, s)
	if err != nil {
		return nil, err
	}
	defer finish()
	if s.bulkIgnore {
		if !c.inTx {
			if err := c.rawExec(ctx, "START TRANSACTION"); err != nil {
				return nil, err
			}
		}
		if err := c.rawExec(ctx, "SAVEPOINT manager_ignore"); err != nil {
			if !c.inTx {
				err = joinCleanupError(err, c.cleanup("ROLLBACK"))
			}
			return nil, err
		}
	}
	result, err := run()
	if s.ignore && !s.bulkIgnore {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return ignoredResult{}, nil
		}
	}
	if s.bulkIgnore {
		if err == nil {
			err = c.checkIgnoredWarnings(ctx)
		}
		if err != nil {
			if !c.inTx {
				err = joinCleanupError(err, c.cleanup("ROLLBACK"))
			} else {
				err = joinCleanupError(err, c.cleanup("ROLLBACK TO SAVEPOINT manager_ignore"))
			}
		} else if !c.inTx {
			if commitErr := c.rawExec(ctx, "COMMIT"); commitErr != nil {
				c.discard()
				// An ambiguous commit must never trigger an ErrBadConn retry.
				err = fmt.Errorf("MySQL bulk insert commit failed: %v", commitErr)
			}
		}
		if c.inTx && !c.invalid {
			err = joinCleanupError(err, c.cleanup("RELEASE SAVEPOINT manager_ignore"))
		}
	}
	return result, err
}

func (c *connection) query(ctx context.Context, s statement, run func() (driver.Rows, error)) (driver.Rows, error) {
	finish, err := c.enter(ctx, s)
	if err != nil {
		return nil, err
	}
	rows, err := run()
	if err != nil {
		finish()
		return nil, err
	}
	if !s.write || c.inTx {
		finish()
		return rows, nil
	}
	return &permitRows{Rows: rows, finish: finish, discard: c.discard}, nil
}

type permitRows struct {
	driver.Rows
	finish  func()
	discard func()
	once    sync.Once
	err     error
}

func (r *permitRows) Close() error {
	r.once.Do(func() {
		r.err = r.Rows.Close()
		if r.err != nil {
			r.discard()
		}
		r.finish()
	})
	return r.err
}

func (r *permitRows) HasNextResultSet() bool {
	rows, ok := r.Rows.(driver.RowsNextResultSet)
	return ok && rows.HasNextResultSet()
}
func (r *permitRows) NextResultSet() error {
	if rows, ok := r.Rows.(driver.RowsNextResultSet); ok {
		return rows.NextResultSet()
	}
	return io.EOF
}

type emptyRows struct{}

func (*emptyRows) Columns() []string         { return nil }
func (*emptyRows) Close() error              { return nil }
func (*emptyRows) Next([]driver.Value) error { return io.EOF }

func (c *connection) checkIgnoredWarnings(ctx context.Context) error {
	countRows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, "SHOW COUNT(*) WARNINGS", nil)
	if err != nil {
		return err
	}
	countValue := make([]driver.Value, 1)
	err = countRows.Next(countValue)
	closeErr := countRows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	countText := fmt.Sprint(countValue[0])
	if raw, ok := countValue[0].([]byte); ok {
		countText = string(raw)
	}
	count, err := strconv.Atoi(countText)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	if count > 65535 {
		return errors.New("too many MySQL bulk insert warnings to verify safely")
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, "SHOW WARNINGS", nil)
	if err != nil {
		return err
	}
	defer rows.Close()
	values := make([]driver.Value, 3)
	seen := 0
	for {
		err = rows.Next(values)
		if errors.Is(err, io.EOF) {
			if seen != count {
				return fmt.Errorf("MySQL truncated bulk insert warnings (%d of %d); reduce the batch size or raise the server's max_error_count", seen, count)
			}
			return rows.Close()
		}
		if err != nil {
			return err
		}
		seen++
		code := fmt.Sprint(values[1])
		if raw, ok := values[1].([]byte); ok {
			code = string(raw)
		}
		if code != "1062" {
			return fmt.Errorf("MySQL rejected bulk insert: warning %v", values[1])
		}
	}
}

type ignoredResult struct{}

func (ignoredResult) LastInsertId() (int64, error) { return 0, nil }
func (ignoredResult) RowsAffected() (int64, error) { return 0, nil }

type preparedStatement struct {
	driver.Stmt
	conn      *connection
	statement statement
}

func (s statement) bind(args []driver.NamedValue) ([]driver.NamedValue, error) {
	if len(args) != s.parameterCount {
		return nil, fmt.Errorf("expected %d SQL parameters, got %d", s.parameterCount, len(args))
	}
	bound := make([]driver.NamedValue, len(s.bindings))
	for i, original := range s.bindings {
		bound[i] = driver.NamedValue{Ordinal: i + 1, Value: args[original].Value}
	}
	return bound, nil
}

func (s *preparedStatement) NumInput() int { return s.statement.parameterCount }

func valuesToNamed(values []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(values))
	for i, v := range values {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}
func (s *preparedStatement) Exec(values []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(values))
}
func (s *preparedStatement) Query(values []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(values))
}
func (s *preparedStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	args, err := s.statement.bind(args)
	if err != nil {
		return nil, err
	}
	return s.conn.execute(ctx, s.statement, func() (driver.Result, error) { return s.Stmt.(driver.StmtExecContext).ExecContext(ctx, args) })
}
func (s *preparedStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.statement.returningID || s.statement.ignore {
		result, err := s.ExecContext(ctx, args)
		if s.statement.returningID {
			return returnedID(result, err)
		}
		return &emptyRows{}, err
	}
	args, err := s.statement.bind(args)
	if err != nil {
		return nil, err
	}
	return s.conn.query(ctx, s.statement, func() (driver.Rows, error) {
		return s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
	})
}

type idRows struct {
	id   int64
	read bool
}

func returnedID(result driver.Result, err error) (driver.Rows, error) {
	if err != nil {
		return nil, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	return &idRows{id: id}, nil
}
func (*idRows) Columns() []string { return []string{"id"} }
func (*idRows) Close() error      { return nil }
func (r *idRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	dest[0] = r.id
	r.read = true
	return nil
}

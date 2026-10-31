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

	mysqldriver "github.com/go-sql-driver/mysql"
)

type connector struct {
	base         driver.Connector
	writer       chan struct{}
	cache        sync.Map
	cacheEntries atomic.Int32
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	base, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &connection{Conn: base, owner: c}, nil
}

func (c *connector) Driver() driver.Driver { return &dialectDriver{} }

type dialectDriver struct{}

func (*dialectDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use MySQL connector")
}

type connection struct {
	driver.Conn
	owner   *connector
	inTx    bool
	writeTx bool
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

func (c *connection) acquire(ctx context.Context) error {
	select {
	case c.owner.writer <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *connection) release() { <-c.owner.writer }

func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Preserve the repositories' SQLite BEGIN IMMEDIATE contract: a writer
	// reserves its turn before any read/modify/write work. Readers still use
	// independent repeatable-read InnoDB snapshots.
	if !opts.ReadOnly {
		if err := c.acquire(ctx); err != nil {
			return nil, err
		}
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		if !opts.ReadOnly {
			c.release()
		}
		return nil, err
	}
	c.inTx, c.writeTx = true, !opts.ReadOnly
	return &transaction{Tx: tx, conn: c}, nil
}

type transaction struct {
	driver.Tx
	conn *connection
}

func (t *transaction) done() {
	if t.conn.writeTx {
		t.conn.release()
	}
	t.conn.inTx, t.conn.writeTx = false, false
}
func (t *transaction) Commit() error   { defer t.done(); return t.Tx.Commit() }
func (t *transaction) Rollback() error { defer t.done(); return t.Tx.Rollback() }

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
func (c *connection) Ping(ctx context.Context) error { return c.Conn.(driver.Pinger).Ping(ctx) }
func (c *connection) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c *connection) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

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
	if s.returningID {
		result, err := c.execute(ctx, s, func() (driver.Result, error) { return c.Conn.(driver.ExecerContext).ExecContext(ctx, s.sql, args) })
		return returnedID(result, err)
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, s.sql, args)
}

func (c *connection) rawExec(ctx context.Context, query string) error {
	_, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, nil)
	return err
}

func (c *connection) execute(ctx context.Context, s statement, run func() (driver.Result, error)) (driver.Result, error) {
	if !c.inTx {
		if err := c.acquire(ctx); err != nil {
			return nil, err
		}
		defer c.release()
	}
	if s.bulkIgnore {
		if !c.inTx {
			if err := c.rawExec(ctx, "START TRANSACTION"); err != nil {
				return nil, err
			}
		}
		if err := c.rawExec(ctx, "SAVEPOINT manager_ignore"); err != nil {
			if !c.inTx {
				_ = c.rawExec(context.Background(), "ROLLBACK")
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
			// Do not use a cancelled request context for rollback.
			if !c.inTx {
				_ = c.rawExec(context.Background(), "ROLLBACK")
			} else {
				_ = c.rawExec(context.Background(), "ROLLBACK TO SAVEPOINT manager_ignore")
			}
		} else if !c.inTx {
			err = c.rawExec(ctx, "COMMIT")
		}
		if c.inTx {
			_ = c.rawExec(context.Background(), "RELEASE SAVEPOINT manager_ignore")
		}
	}
	return result, err
}

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
			return nil
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
	if s.statement.returningID {
		result, err := s.ExecContext(ctx, args)
		return returnedID(result, err)
	}
	args, err := s.statement.bind(args)
	if err != nil {
		return nil, err
	}
	return s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
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

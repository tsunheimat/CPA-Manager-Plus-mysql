// Package sqlwrite admits runtime writes before database/sql checks out a pooled
// connection. Drivers without admission (including SQLite) retain their ordinary
// behavior. Transaction methods still use the standard *sql.Tx API.
package sqlwrite

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// Admitter is implemented by the driver returned by DB.Driver, not a global
// registry of *sql.DB pointers. The returned context carries a connector-bound
// permit. Release abandons an untransferred permit; a successful BeginTx transfers
// it to the actual driver transaction, which releases it at commit or rollback.
// A driver must retain admission across database/sql's ErrBadConn retries.
type Admitter interface {
	AdmitWrite(context.Context) (context.Context, func(), error)
}

// Execer is satisfied by *sql.DB, *sql.Tx and *sql.Conn.
type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// QueryRower is satisfied by *sql.DB, *sql.Tx and *sql.Conn.
type QueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func admit(ctx context.Context, target any) (context.Context, func(), error) {
	// Transactions already own their permit. Explicit connections must have
	// obtained theirs with WithConn, BEFORE checkout. Never acquire here for
	// either kind of checked-out object: waiting now can exhaust the pool.
	if db, ok := target.(*sql.DB); ok {
		if admission, ok := db.Driver().(Admitter); ok {
			return admission.AdmitWrite(ctx)
		}
	}
	return ctx, func() {}, nil
}

// BeginTx starts a write transaction with pre-checkout admission. Read-only
// transactions bypass admission. The returned transaction is owned by
// database/sql, including its cancellation-driven rollback.
func BeginTx(ctx context.Context, db *sql.DB, opts *sql.TxOptions) (*sql.Tx, error) {
	if opts != nil && opts.ReadOnly {
		return db.BeginTx(ctx, opts)
	}
	ctx, release, err := admit(ctx, db)
	if err != nil {
		return nil, err
	}
	defer release()
	return db.BeginTx(ctx, opts)
}

// ExecContext executes an autocommit write or a write on an admitted transaction
// or explicit connection. A *sql.Conn must come from WithConn; its callback
// context must be used. Unknown wrappers are passed through, never late-admitted;
// drivers requiring admission fail closed for an unadmitted write.
func ExecContext(ctx context.Context, target Execer, query string, args ...any) (sql.Result, error) {
	ctx, release, err := admit(ctx, target)
	if err != nil {
		return nil, err
	}
	defer release()
	return target.ExecContext(ctx, query, args...)
}

// RowScanner supports the usual QueryRowContext(...).Scan(...) call shape.
// Execution is deliberately deferred to Scan, so forgetting to scan cannot leak
// admission or a checked-out connection. Scan may be called only once.
type RowScanner interface {
	Scan(...any) error
}

type rowScanner struct {
	mu  sync.Mutex
	run func(...any) error
}

func (r *rowScanner) Scan(dest ...any) error {
	r.mu.Lock()
	run := r.run
	r.run = nil
	r.mu.Unlock()
	if run == nil {
		return errors.New("sqlwrite: row has already been scanned")
	}
	return run(dest...)
}

// QueryRowContext executes a write-returning query when Scan is called, holding
// admission through scanning and closing its rows. Use ordinary QueryRowContext
// for reads; those need no writer permit.
func QueryRowContext(ctx context.Context, target QueryRower, query string, args ...any) RowScanner {
	args = append([]any(nil), args...)
	return &rowScanner{run: func(dest ...any) error {
		ctx, release, err := admit(ctx, target)
		if err != nil {
			return err
		}
		defer release()
		return target.QueryRowContext(ctx, query, args...).Scan(dest...)
	}}
}

// ExecStmtContext executes a DB-scoped prepared write. db must be the DB that
// prepared stmt. For a transaction-scoped statement, call stmt.ExecContext
// directly: that transaction already owns admission. For a connection-scoped
// statement, use stmt.ExecContext with the WithConn callback context.
func ExecStmtContext(ctx context.Context, db *sql.DB, stmt *sql.Stmt, args ...any) (sql.Result, error) {
	ctx, release, err := admit(ctx, db)
	if err != nil {
		return nil, err
	}
	defer release()
	return stmt.ExecContext(ctx, args...)
}

// QueryRowStmtContext is QueryRowContext for a DB-scoped prepared statement.
// The same ownership restrictions as ExecStmtContext apply.
func QueryRowStmtContext(ctx context.Context, db *sql.DB, stmt *sql.Stmt, args ...any) RowScanner {
	args = append([]any(nil), args...)
	return &rowScanner{run: func(dest ...any) error {
		ctx, release, err := admit(ctx, db)
		if err != nil {
			return err
		}
		defer release()
		return stmt.QueryRowContext(ctx, args...).Scan(dest...)
	}}
}

// WithConn admits a write BEFORE reserving an explicit connection. fn must use
// the callback context for writes and must finish any transaction before it
// returns. Starting a write transaction transfers the permit to that transaction;
// after commit/rollback the context cannot authorize more writes. Return from
// WithConn and use a new helper call for post-transaction work. Reads are allowed
// throughout. The connection and any untransferred permit are always released.
func WithConn(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Conn) error) (err error) {
	ctx, release, err := admit(ctx, db)
	if err != nil {
		return err
	}
	defer release()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	return fn(ctx, conn)
}

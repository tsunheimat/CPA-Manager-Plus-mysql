package testdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

// MySQLPlan is evidence from a generated statement, not a handwritten EXPLAIN
// surrogate. Analyze executes that statement, so callers should use it only for
// bounded fixtures and pass the same request deadline as the measured reader.
// Work contains same-connection session-status deltas, not global server totals.
// The two SHOW probes may contribute instrumentation overhead to those counters;
// use EXPLAIN ANALYZE's actual rows/loops for iterator-specific work.
// DSNs are never included. MySQL plans can contain bound fixture values; this
// helper is only for synthetic disposable fixtures, never production data.
type MySQLPlan struct {
	SQL            string            `json:"sql"`
	Explain        json.RawMessage   `json:"explain"`
	Analyze        string            `json:"analyze,omitempty"`
	AnalyzeElapsed time.Duration     `json:"analyze_elapsed_ns,omitempty"`
	Work           map[string]uint64 `json:"session_work,omitempty"`
}

// ExplainMySQL compiles the actual repository query and reordered bindings and
// explains it on the fixture's native admin connection. It never weakens runtime
// admission or falls back to the supplied environment's database. Private query
// builders can call this from same-package tests without exporting runtime SQL.
func ExplainMySQL(t testing.TB, ctx context.Context, db *sql.DB, analyze bool, query string, args ...any) MySQLPlan {
	t.Helper()
	if !mysqlrepo.IsMySQL(db) {
		t.Skip("native MySQL plan requires CPA_TEST_MYSQL_DSN")
	}
	compiled, bindings, err := mysqlrepo.CompileSQL(query, args...)
	if err != nil {
		t.Fatalf("compile plan query: %v", err)
	}
	conn, cleanup, err := openExplainConnection(ctx, db, mysqlrepo.OpenNative)
	if err != nil {
		t.Fatalf("open plan fixture connection: %v", err)
	}
	defer cleanup()
	result := MySQLPlan{SQL: compiled}
	var explanation string
	if err := conn.QueryRowContext(ctx, "EXPLAIN FORMAT=JSON "+compiled, bindings...).Scan(&explanation); err != nil {
		t.Fatalf("explain compiled query: %v", err)
	}
	if !json.Valid([]byte(explanation)) {
		t.Fatal("EXPLAIN FORMAT=JSON did not return JSON")
	}
	result.Explain = json.RawMessage(explanation)
	if analyze {
		before, err := statementWork(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		err = conn.QueryRowContext(ctx, "EXPLAIN ANALYZE "+compiled, bindings...).Scan(&result.Analyze)
		result.AnalyzeElapsed = time.Since(start)
		if err != nil {
			t.Fatalf("analyze compiled query: %v", err)
		}
		after, err := statementWork(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		result.Work = make(map[string]uint64, len(before))
		for name, initial := range before {
			if final := after[name]; final >= initial {
				result.Work[name] = final - initial
			} else {
				t.Fatalf("session counter reset during analysis: %s", name)
			}
		}
	}
	return result
}

// openExplainConnection keeps the caller's context on the entire setup path,
// including OpenNative's handshake/PingContext, not just the later EXPLAIN.
// Injection is local to tests; registration is still checked by the same opener
// as Admin, and no environment DSN fallback or additional target is accepted.
func openExplainConnection(ctx context.Context, db *sql.DB, open func(context.Context, string) (*sql.DB, error)) (*sql.Conn, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	admin, err := openFixtureAdmin(ctx, db, open)
	if err != nil {
		return nil, nil, err
	}
	conn, err := admin.Conn(ctx)
	if err != nil {
		_ = admin.Close()
		return nil, nil, err
	}
	return conn, func() {
		_ = conn.Close()
		_ = admin.Close()
	}, nil
}

func statementWork(ctx context.Context, conn *sql.Conn) (map[string]uint64, error) {
	rows, err := conn.QueryContext(ctx, `SHOW SESSION STATUS WHERE Variable_name IN (
		'Handler_read_first', 'Handler_read_key', 'Handler_read_next', 'Handler_read_prev',
		'Handler_read_rnd', 'Handler_read_rnd_next', 'Created_tmp_tables', 'Created_tmp_disk_tables',
		'Select_scan', 'Select_full_join', 'Sort_rows', 'Sort_scan', 'Sort_merge_passes')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	work := map[string]uint64{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("session counter %s: %w", name, err)
		}
		work[name] = n
	}
	return work, rows.Err()
}

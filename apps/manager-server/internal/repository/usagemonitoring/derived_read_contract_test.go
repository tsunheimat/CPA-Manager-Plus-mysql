package usagemonitoring

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

var errDerivedRead = errors.New("injected derived read failure")

// Instrument the actual driver boundary, not a second copy of a query builder.
// Configuration is immutable after OpenDB; cancellation/rollback accounting is
// synchronized because database/sql may roll a cancelled transaction back.
type derivedReadDriver struct {
	base                                 driver.Driver
	dsn                                  string
	mode                                 string
	entered                              chan struct{}
	mu                                   sync.Mutex
	queries                              []string
	bindings                             [][]any
	commits, rollbacks, rowCount, closes atomic.Int64
}

func (d *derivedReadDriver) Driver() driver.Driver { return d.base }
func (d *derivedReadDriver) Connect(context.Context) (driver.Conn, error) {
	conn, err := d.base.Open(d.dsn)
	if err != nil {
		return nil, err
	}
	return &derivedReadConn{Conn: conn, trace: d}, nil
}

type derivedReadConn struct {
	driver.Conn
	trace *derivedReadDriver
}

func (c *derivedReadConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &derivedReadTx{Tx: tx, trace: c.trace}, nil
}
func (c *derivedReadConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	d := c.trace
	if !strings.Contains(query, "from banded_events") {
		return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	}
	values := make([]any, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	d.mu.Lock()
	d.queries = append(d.queries, query)
	d.bindings = append(d.bindings, values)
	d.mu.Unlock()
	if d.mode == "query" {
		return nil, errDerivedRead
	}
	if d.mode == "cancel" {
		close(d.entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &derivedReadRows{Rows: rows, trace: d}, nil
}

type derivedReadTx struct {
	driver.Tx
	trace *derivedReadDriver
}

func (tx *derivedReadTx) Commit() error {
	tx.trace.commits.Add(1)
	if tx.trace.mode == "commit" {
		// A failing read-only commit must still release the fixture connection.
		_ = tx.Tx.Rollback()
		return errDerivedRead
	}
	return tx.Tx.Commit()
}
func (tx *derivedReadTx) Rollback() error {
	tx.trace.rollbacks.Add(1)
	return tx.Tx.Rollback()
}

type derivedReadRows struct {
	driver.Rows
	trace *derivedReadDriver
	seen  int
}

func (r *derivedReadRows) Close() error {
	r.trace.closes.Add(1)
	return r.Rows.Close()
}
func (r *derivedReadRows) Next(dest []driver.Value) error {
	if r.trace.mode == "rows" && r.seen == 1 {
		return errDerivedRead
	}
	if err := r.Rows.Next(dest); err != nil {
		return err
	}
	r.seen++
	r.trace.rowCount.Add(1)
	if r.trace.mode == "scan" {
		for i, value := range dest {
			if _, ok := value.(int64); ok {
				dest[i] = "not-an-integer"
				break
			}
		}
	}
	return nil
}

func traceDerivedDB(t *testing.T, original *sql.DB, path, mode string) (*sql.DB, *derivedReadDriver) {
	t.Helper()
	trace := &derivedReadDriver{base: original.Driver(), dsn: path, mode: mode, entered: make(chan struct{})}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(trace)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db, trace
}

func TestDerivedAccountWindowActualTwoArmSQLAndBindings(t *testing.T) {
	if testdb.MySQL() {
		t.Skip("physical SQLite driver capture; MySQL compilation is checked offline")
	}
	db, path := derivedMonitoringDB(t)
	insertDerivedEvents(t, db,
		derivedEvent{id: 1, at: 3600100, input: 10},
		derivedEvent{id: 2, at: 7200000, input: 20},
		derivedEvent{id: 3, at: 10800000, input: 30},
		derivedEvent{id: 4, at: 10800200, input: 400},
		derivedEvent{id: 5, at: 3600099, input: 500},
		derivedEvent{id: 6, at: 3600101, input: 60, file: "b.json"})
	projectDerivedEvents(t, db, 6)
	insertDerivedEvents(t, db,
		derivedEvent{id: 7, at: 7200000, input: 700},
		derivedEvent{id: 8, at: 10800000, input: 80},
		derivedEvent{id: 9, at: 7200001, input: 90},
		derivedEvent{id: 10, at: 10799999, input: 100},
		derivedEvent{id: 11, at: 10800200, input: 1100},
		derivedEvent{id: 12, at: 3600102, input: 120, file: "b.json"})
	windows := []AccountWindowUsageQuery{
		{RequestIndex: 7, FromMS: 3600100, ToMS: 10800200, AccountKey: derivedAccountA, AuthFileSnapshot: "a.json", AuthIndex: "i"},
		{RequestIndex: 8, FromMS: 3600100, ToMS: 10800200, AccountKey: derivedAccountA, AuthFileSnapshot: "a.json", AuthIndex: "i"},
		{RequestIndex: 9, FromMS: 7200000, ToMS: 10800000, AccountKey: derivedAccountA, AuthFileSnapshot: "a.json", AuthIndex: "i"},
		{RequestIndex: 11, FromMS: 3600100, ToMS: 10800200, AccountKey: derivedAccountB, AuthFileSnapshot: "b.json", AuthIndex: "i"},
	}
	db, trace := traceDerivedDB(t, db, path, "")
	tx, err := sqlwrite.BeginTx(context.Background(), db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	grouped := map[accountWindowStatKey]*AccountWindowModelStat{}
	if err := mergeProjectedAccountWindowGrainStats(context.Background(), tx, windows, 6, false, 8, true, grouped, 3600000); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	want := []AccountWindowModelStat{
		{PricingBand: usage.PricingBand{PricingModel: "bill", ContextThresholdTokens: -1}, RequestIndex: 7, Model: "display", BillingModel: "bill", ServiceTier: "default", Calls: 5, SuccessCalls: 5, InputTokens: 310, TotalTokens: 310, LastSeenMS: 10800000},
		{PricingBand: usage.PricingBand{PricingModel: "bill", ContextThresholdTokens: -1}, RequestIndex: 8, Model: "display", BillingModel: "bill", ServiceTier: "default", Calls: 5, SuccessCalls: 5, InputTokens: 310, TotalTokens: 310, LastSeenMS: 10800000},
		{PricingBand: usage.PricingBand{PricingModel: "bill", ContextThresholdTokens: -1}, RequestIndex: 9, Model: "display", BillingModel: "bill", ServiceTier: "default", Calls: 2, SuccessCalls: 2, InputTokens: 190, TotalTokens: 190, LastSeenMS: 10799999},
		{PricingBand: usage.PricingBand{PricingModel: "bill", ContextThresholdTokens: -1}, RequestIndex: 11, Model: "display", BillingModel: "bill", ServiceTier: "default", Calls: 2, SuccessCalls: 2, InputTokens: 180, TotalTokens: 180, LastSeenMS: 3600102},
	}
	if got := sortedAccountWindowStats(grouped); !reflect.DeepEqual(got, want) {
		t.Fatalf("windows=%+v want=%+v", got, want)
	}
	if len(trace.queries) != 1 || trace.rowCount.Load() != 4 {
		t.Fatalf("physical queries=%d scanned rows=%d; expected one SQL merge and four final-grain rows", len(trace.queries), trace.rowCount.Load())
	}
	query := trace.queries[0]
	for _, fragment := range []string{"with window_targets(", "from banded_events", "group by request_index, analytics_model"} {
		if strings.Count(query, fragment) != 2 {
			t.Fatalf("each arm must contain %q: %s", fragment, query)
		}
	}
	for _, fragment := range []string{"p.event_id <= ?", "e.id > ?", "p.event_id > ?", "p.timestamp_ms >= w.from_ms and p.timestamp_ms < w.to_ms", "e.timestamp_ms >= w.from_ms and e.timestamp_ms < w.to_ms"} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("missing predicate %q", fragment)
		}
	}
	// Independent literal targets, ceil/floor boundaries, watermarks and all five
	// long-context parameters must be duplicated in arm order, not interleaved.
	oneArm := []any{
		int64(7), int64(3600100), int64(10800200), int64(7200000), int64(10800000), derivedAccountA, derivedAccountA, true,
		int64(8), int64(3600100), int64(10800200), int64(7200000), int64(10800000), derivedAccountA, derivedAccountA, true,
		int64(9), int64(7200000), int64(10800000), int64(7200000), int64(10800000), derivedAccountA, derivedAccountA, true,
		int64(11), int64(3600100), int64(10800200), int64(7200000), int64(10800000), derivedAccountB, derivedAccountB, true,
		int64(6), int64(8), int64(272000), int64(272000), int64(272000), int64(272000), int64(272000),
	}
	wantArgs := append(append([]any{}, oneArm...), oneArm...)
	if !reflect.DeepEqual(trace.bindings[0], wantArgs) {
		t.Fatalf("actual bindings=%v want=%v", trace.bindings[0], wantArgs)
	}
	compiled, bound, err := mysql.CompileSQL(query, trace.bindings[0]...)
	// The renderer spaces SQL tokens. A bare '?' token is a binding; question
	// marks inside identity/model regular-expression literals are not bindings.
	placeholders := 0
	for _, token := range strings.Fields(compiled) {
		if token == "?" {
			placeholders++
		}
	}
	if err != nil || !reflect.DeepEqual(bound, wantArgs) || placeholders != len(wantArgs) {
		t.Fatalf("MySQL compile args=%v placeholders=%d want=%d err=%v", bound, placeholders, len(wantArgs), err)
	}
	if strings.Count(query, " union all ") != 1 || strings.Contains(compiled, "as( values") {
		t.Fatal("window VALUES was not translated or source arms were not merged once")
	}
}

func TestDerivedReadFailuresAreTerminal(t *testing.T) {
	if testdb.MySQL() {
		t.Skip("fault injection wraps the offline SQLite driver")
	}
	type result struct {
		rows      int
		available bool
		err       error
	}
	type reader struct {
		name         string
		callerOwnsTx bool
		read         func(context.Context, *sql.DB) result
	}
	filter := AnalyticsFilter{FromMS: 1, ToMS: 100, IncludeFailed: true}
	readers := []reader{
		{name: "models", read: func(ctx context.Context, db *sql.DB) result {
			rows, _, ok, err := New(db).LoadModelStats(ctx, filter)
			return result{len(rows), ok, err}
		}},
		{name: "accounts", read: func(ctx context.Context, db *sql.DB) result {
			rows, _, ok, err := New(db).LoadAccountStats(ctx, filter)
			return result{len(rows), ok, err}
		}},
		{name: "keys", read: func(ctx context.Context, db *sql.DB) result {
			rows, _, ok, err := New(db).LoadAPIKeyStats(ctx, filter)
			return result{len(rows), ok, err}
		}},
		{name: "windows", read: func(ctx context.Context, db *sql.DB) result {
			rows, _, ok, err := New(db).LoadAccountWindowStats(ctx, []AccountWindowUsageQuery{{RequestIndex: 7, FromMS: 1, ToMS: 100, AccountKey: derivedAccountA}})
			return result{len(rows), ok, err}
		}},
		{name: "pricing-hourly", read: func(ctx context.Context, db *sql.DB) result {
			rows, _, ok, err := usagepricing.New(db).LoadHourlyRows(ctx, usagepricing.HourlyFilter{FromMS: 1, ToMS: 100, IncludeFailed: true})
			return result{len(rows), ok, err}
		}},
		{name: "pricing-accounts", read: func(ctx context.Context, db *sql.DB) result {
			rows, _, ok, err := usagepricing.New(db).LoadAccountRows(ctx, []string{derivedAccountA})
			return result{len(rows), ok, err}
		}},
		{name: "retained-hourly", callerOwnsTx: true, read: func(ctx context.Context, db *sql.DB) result {
			tx, err := sqlwrite.BeginTx(ctx, db, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				return result{err: err}
			}
			defer tx.Rollback()
			rows, err := usagepricing.New(db).LoadHourlyRowsFromEventsTx(ctx, tx, usagepricing.HourlyFilter{FromMS: 1, ToMS: 100, IncludeFailed: true})
			return result{len(rows), err == nil, err}
		}},
		{name: "retained-accounts", callerOwnsTx: true, read: func(ctx context.Context, db *sql.DB) result {
			tx, err := sqlwrite.BeginTx(ctx, db, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				return result{err: err}
			}
			defer tx.Rollback()
			rows, err := usagepricing.New(db).LoadAccountRowsFromEventsTx(ctx, tx, []string{derivedAccountA})
			return result{len(rows), err == nil, err}
		}},
	}
	// One immutable on-disk fixture is reused sequentially, with a fresh driver
	// connection and transaction for every injected failure.
	original, path := derivedMonitoringDB(t)
	insertDerivedEvents(t, original, derivedEvent{id: 1, at: 10, input: 17})
	projectDerivedEvents(t, original, 1)
	insertDerivedEvents(t, original, derivedEvent{id: 2, at: 11, input: 19})
	base := original.Driver()
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range readers {
		for _, mode := range []string{"query", "cancel", "scan", "rows", "commit"} {
			if mode == "commit" && read.callerOwnsTx {
				continue
			}
			t.Run(read.name+"/"+mode, func(t *testing.T) {
				trace := &derivedReadDriver{base: base, dsn: path, mode: mode, entered: make(chan struct{})}
				db := sql.OpenDB(trace)
				db.SetMaxOpenConns(1)
				defer db.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var got result
				if mode == "cancel" {
					finished := make(chan result, 1)
					go func() { finished <- read.read(ctx, db) }()
					select {
					case <-trace.entered:
						cancel()
					case got = <-finished:
						t.Fatalf("reader ended before in-flight cancellation: %+v", got)
					case <-time.After(5 * time.Second):
						t.Fatal("reader did not reach the aggregate query")
					}
					select {
					case got = <-finished:
					case <-time.After(5 * time.Second):
						t.Fatal("reader ignored cancellation")
					}
				} else {
					got = read.read(ctx, db)
				}
				if got.err == nil || got.available || got.rows != 0 {
					t.Fatalf("error became partial success: %+v", got)
				}
				switch mode {
				case "cancel":
					if !errors.Is(got.err, context.Canceled) {
						t.Fatalf("cancel error=%v", got.err)
					}
				case "scan":
					if !strings.Contains(got.err.Error(), "not-an-integer") {
						t.Fatalf("scan error=%v", got.err)
					}
				default:
					if !errors.Is(got.err, errDerivedRead) {
						t.Fatalf("injected error=%v", got.err)
					}
				}
				// Acquiring the sole connection also synchronizes with automatic
				// cancellation rollback, without sleeps or unchecked goroutines.
				probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer probeCancel()
				if err := db.PingContext(probeCtx); err != nil {
					t.Fatalf("read leaked connection: %v", err)
				}
				trace.mu.Lock()
				queryCount := len(trace.queries)
				trace.mu.Unlock()
				if queryCount != 1 {
					t.Fatalf("error triggered retry/fallback: %d aggregate queries", queryCount)
				}
				if mode == "commit" {
					if trace.commits.Load() != 1 || trace.rowCount.Load() == 0 {
						t.Fatal("commit failure was not injected after scanning")
					}
				} else if trace.commits.Load() != 0 || trace.rollbacks.Load() != 1 {
					t.Fatalf("failed read transaction commits=%d rollbacks=%d", trace.commits.Load(), trace.rollbacks.Load())
				}
				if mode == "scan" || mode == "rows" || mode == "commit" {
					if trace.closes.Load() != 1 || trace.rowCount.Load() == 0 {
						t.Fatalf("rows not scanned/closed: rows=%d closes=%d", trace.rowCount.Load(), trace.closes.Load())
					}
				}
				if mode == "rows" && trace.rowCount.Load() != 1 {
					t.Fatal(fmt.Sprint("iteration failure did not follow a successfully scanned row: ", trace.rowCount.Load()))
				}
			})
		}
	}
}

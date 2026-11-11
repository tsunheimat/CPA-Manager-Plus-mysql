package usageevent

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

type latencyRoutingDriver struct{ conn *latencyRoutingConn }

func (latencyRoutingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use the offline connector")
}
func (d latencyRoutingDriver) AdmitWrite(context.Context) (context.Context, func(), error) {
	d.conn.admissions.Add(1)
	return nil, nil, errors.New("percentile reads must not acquire write admission")
}

type latencyRoutingConnector struct{ conn *latencyRoutingConn }

func (c latencyRoutingConnector) Driver() driver.Driver { return latencyRoutingDriver{c.conn} }
func (c latencyRoutingConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

type latencyRoutingConn struct {
	query                      func(context.Context, int, string, []driver.NamedValue) (*latencyRoutingRows, error)
	queries                    []string
	bindings                   [][]driver.NamedValue
	options                    driver.TxOptions
	beginError, commitError    error
	begins, commits, rollbacks atomic.Int32
	admissions, openRows       atomic.Int32
	active                     atomic.Bool
	finished                   chan struct{}
	finishOnce                 sync.Once
}

func (*latencyRoutingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*latencyRoutingConn) Close() error { return nil }
func (*latencyRoutingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("expected explicit read-only transaction options")
}
func (c *latencyRoutingConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	c.begins.Add(1)
	c.options = options
	if c.beginError != nil {
		return nil, c.beginError
	}
	c.active.Store(true)
	return c, nil
}
func (c *latencyRoutingConn) finish() {
	c.active.Store(false)
	c.finishOnce.Do(func() { close(c.finished) })
}
func (c *latencyRoutingConn) Commit() error {
	c.commits.Add(1)
	c.finish()
	return c.commitError
}
func (c *latencyRoutingConn) Rollback() error {
	c.rollbacks.Add(1)
	c.finish()
	return nil
}
func (c *latencyRoutingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.openRows.Load() != 0 {
		return nil, errors.New("previous sample stream still owns its rows")
	}
	c.queries = append(c.queries, query)
	c.bindings = append(c.bindings, append([]driver.NamedValue(nil), args...))
	rows, err := c.query(ctx, len(c.queries)-1, query, args)
	if err != nil {
		return nil, err
	}
	c.openRows.Add(1)
	rows.closed = func() { c.openRows.Add(-1) }
	return rows, nil
}

type latencyRoutingRows struct {
	columns   []string
	values    [][]driver.Value
	index     int
	next      func(int, []driver.Value) error
	closed    func()
	closeOnce sync.Once
}

func (r *latencyRoutingRows) Columns() []string { return r.columns }
func (r *latencyRoutingRows) Close() error {
	r.closeOnce.Do(func() { r.closed() })
	return nil
}
func (r *latencyRoutingRows) Next(dest []driver.Value) error {
	if r.next != nil {
		err := r.next(r.index, dest)
		r.index++
		return err
	}
	if r.index == len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func newLatencyRoutingRepository(t *testing.T, options ...Option) (*repository, *latencyRoutingConn) {
	t.Helper()
	conn := &latencyRoutingConn{finished: make(chan struct{})}
	db := sql.OpenDB(latencyRoutingConnector{conn})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	repo := New(db, options...).(*repository)
	// The actual adapter's driver type is private to package mysql. Emulate its
	// fixed dialect here, without credentials/network or changing public options;
	// all public percentile entry points and their SQL execution run unchanged.
	repo.mysqlBackend = true
	return repo, conn
}

func latencyRoutingFixture(stage int) *latencyRoutingRows {
	const hour = int64(3_600_000)
	switch stage {
	case 0:
		return &latencyRoutingRows{columns: []string{"timestamp_ms", "latency_positive", "ttft_positive"}, values: [][]driver.Value{
			{hour + 300, int64(0), int64(1)}, {int64(200), int64(1), int64(0)},
			{hour + 100, int64(1), int64(1)}, {int64(100), int64(1), int64(1)}, {hour + 200, int64(1), int64(0)},
		}}
	case 1:
		return &latencyRoutingRows{columns: []string{"timestamp_ms", "latency_ms"}, values: [][]driver.Value{
			{int64(100), int64(1)}, {int64(200), int64(2)}, {hour + 100, int64(10)}, {hour + 200, int64(20)},
		}}
	default:
		return &latencyRoutingRows{columns: []string{"timestamp_ms", "ttft_ms"}, values: [][]driver.Value{
			{int64(100), int64(4)}, {hour + 100, int64(100)}, {hour + 300, int64(200)},
		}}
	}
}

func TestMySQLLatencyFlagRoutesReferenceAndNativeQueriesOffline(t *testing.T) {
	filter := AnalyticsFilter{FromMS: 0, ToMS: 7_200_000, IncludeFailed: true, Models: []string{"bound-'model"}}
	where, bindings := analyticsWhere(filter)
	for _, mode := range []string{"default", "disabled", "enabled"} {
		for _, entry := range []string{"combined", "summary-only", "buckets-only", "standalone-summary", "standalone-buckets", "none"} {
			t.Run(mode+"/"+entry, func(t *testing.T) {
				var options []Option
				if mode != "default" {
					options = []Option{WithMySQLAnalytics(mode == "enabled")}
				}
				repo, conn := newLatencyRoutingRepository(t, options...)
				summary := entry == "combined" || entry == "summary-only" || entry == "standalone-summary"
				buckets := entry == "combined" || entry == "buckets-only" || entry == "standalone-buckets"
				conn.query = func(_ context.Context, stage int, query string, args []driver.NamedValue) (*latencyRoutingRows, error) {
					if mode == "enabled" {
						if !strings.HasPrefix(query, mysql.Native("")) || !strings.Contains(query, "row_number") || !strings.Contains(query, "scopes") || !strings.Contains(query, "metric_kinds") {
							t.Fatalf("enabled reader did not issue the native combined rank plan: %s", query)
						}
						rows := &latencyRoutingRows{columns: []string{"scope", "bucket", "latency", "ttft"}}
						if summary {
							rows.values = append(rows.values, []driver.Value{int64(0), int64(0), int64(20), int64(200)})
						}
						if buckets {
							rows.values = append(rows.values,
								[]driver.Value{int64(1), int64(0), int64(2), int64(4)},
								[]driver.Value{int64(1), int64(3_600_000), int64(20), int64(200)})
						}
						return rows, nil
					}
					if !conn.active.Load() || !conn.options.ReadOnly || conn.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) {
						t.Fatal("reference passes must share one read-only repeatable-read transaction")
					}
					for _, forbidden := range []string{mysql.Native(""), "row_number", " over ", "scopes", "analytics_bucket_map"} {
						if strings.Contains(query, forbidden) {
							t.Fatalf("disabled reader still uses accelerated SQL (%q): %s", forbidden, query)
						}
					}
					if !strings.Contains(query, where) || strings.Contains(query, filter.Models[0]) {
						t.Fatal("reference pass lost or interpolated a filter", query)
					}
					if stage > 0 && !strings.HasSuffix(query, []string{"", "order by latency_ms", "order by ttft_ms"}[stage]) {
						t.Fatal("reference metric stream is not ordered", query)
					}
					// Exercise the actual adapter compiler offline too: no native marker
					// or window plan is introduced during ordinary translation.
					compiled, _, err := mysql.CompileSQL(query, bindings...)
					if err != nil || strings.Contains(compiled, "row_number") {
						t.Fatal("reference SQL compilation", err, compiled)
					}
					return latencyRoutingFixture(stage), nil
				}
				var got LatencyMetrics
				var err error
				switch entry {
				case "standalone-summary":
					got.Summary, err = repo.LatencySummaryWithFilter(context.Background(), filter)
				case "standalone-buckets":
					got.Timeline, err = repo.LatencyPercentilesWithFilter(context.Background(), filter, "hour", time.UTC)
				default:
					got, err = repo.LatencyMetricsWithFilter(context.Background(), filter, "hour", time.UTC, summary, buckets)
				}
				if err != nil {
					t.Fatal(err)
				}
				want := LatencyMetrics{}
				if summary {
					want.Summary = LatencySummary{P95LatencyMS: sql.NullFloat64{Float64: 20, Valid: true}, P95TTFTMS: sql.NullFloat64{Float64: 200, Valid: true}}
				}
				if buckets {
					want.Timeline = []LatencyPercentiles{
						{BucketMS: 0, P95LatencyMS: sql.NullFloat64{Float64: 2, Valid: true}, P95TTFTMS: sql.NullFloat64{Float64: 4, Valid: true}},
						{BucketMS: 3_600_000, P95LatencyMS: sql.NullFloat64{Float64: 20, Valid: true}, P95TTFTMS: sql.NullFloat64{Float64: 200, Valid: true}},
					}
				} else if entry == "summary-only" {
					want.Timeline = []LatencyPercentiles{}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %#v, want %#v", got, want)
				}
				wantQueries, wantBegins := 3, int32(1)
				if mode == "enabled" {
					wantQueries, wantBegins = 1, 0
				}
				if entry == "none" {
					wantQueries, wantBegins = 0, 0
				}
				if len(conn.queries) != wantQueries || conn.begins.Load() != wantBegins || conn.commits.Load() != wantBegins || conn.rollbacks.Load() != 0 || conn.openRows.Load() != 0 || conn.admissions.Load() != 0 {
					t.Fatalf("queries=%d begin=%d commit=%d rollback=%d openRows=%d admission=%d", len(conn.queries), conn.begins.Load(), conn.commits.Load(), conn.rollbacks.Load(), conn.openRows.Load(), conn.admissions.Load())
				}
				for _, args := range conn.bindings {
					if len(args) != len(bindings) {
						t.Fatalf("changed binding count: %v vs %v", args, bindings)
					}
					for i := range args {
						if args[i].Value != bindings[i] {
							t.Fatalf("changed filter binding %d: %v vs %v", i, args[i].Value, bindings[i])
						}
					}
				}
			})
		}
	}
}

func TestLatencyReferenceFailuresAndCancellationDiscardPartialResults(t *testing.T) {
	sentinel := errors.New("injected database failure")
	for _, mode := range []string{"query", "scan", "rows", "cancel"} {
		for stage := 0; stage < 3; stage++ {
			t.Run(fmt.Sprintf("%s/pass-%d", mode, stage), func(t *testing.T) {
				repo, conn := newLatencyRoutingRepository(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				conn.query = func(_ context.Context, current int, _ string, _ []driver.NamedValue) (*latencyRoutingRows, error) {
					rows := latencyRoutingFixture(current)
					if current != stage {
						return rows, nil
					}
					switch mode {
					case "query":
						return nil, sentinel
					case "scan":
						rows.values[0][0] = "not a timestamp"
					case "rows", "cancel":
						rows.next = func(index int, dest []driver.Value) error {
							if index == 1 {
								if mode == "cancel" {
									cancel()
									// Return a valid row anyway; cancellation must be checked
									// by the consumer, not only by a cooperative driver.
								} else {
									return sentinel
								}
							}
							if index >= len(rows.values) {
								return io.EOF
							}
							copy(dest, rows.values[index])
							return nil
						}
					}
					return rows, nil
				}
				got, err := repo.LatencyMetricsWithFilter(ctx, AnalyticsFilter{ToMS: 7_200_000, IncludeFailed: true}, "hour", nil, true, true)
				if err == nil || !reflect.DeepEqual(got, LatencyMetrics{}) {
					t.Fatalf("partial success escaped: %#v, %v", got, err)
				}
				if (mode == "query" || mode == "rows") && !errors.Is(err, sentinel) {
					t.Fatalf("database error was replaced: %v", err)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation was replaced: %v", err)
				}
				select {
				case <-conn.finished:
				case <-time.After(time.Second):
					t.Fatal("snapshot not released after failure")
				}
				if len(conn.queries) != stage+1 || conn.openRows.Load() != 0 || conn.rollbacks.Load() != 1 || conn.commits.Load() != 0 {
					t.Fatalf("continued/retried or leaked failed snapshot: queries=%d open=%d rollback=%d commit=%d", len(conn.queries), conn.openRows.Load(), conn.rollbacks.Load(), conn.commits.Load())
				}
			})
		}
	}
	for _, phase := range []string{"begin", "commit"} {
		t.Run(phase, func(t *testing.T) {
			repo, conn := newLatencyRoutingRepository(t)
			if phase == "begin" {
				conn.beginError = sentinel
			} else {
				conn.commitError = sentinel
			}
			conn.query = func(_ context.Context, stage int, _ string, _ []driver.NamedValue) (*latencyRoutingRows, error) {
				return latencyRoutingFixture(stage), nil
			}
			got, err := repo.LatencyMetricsWithFilter(context.Background(), AnalyticsFilter{ToMS: 7_200_000}, "hour", nil, true, true)
			if !errors.Is(err, sentinel) || !reflect.DeepEqual(got, LatencyMetrics{}) || conn.openRows.Load() != 0 {
				t.Fatalf("transaction failure lost: %#v, %v", got, err)
			}
		})
	}
}

func TestLatencyReferenceEmptyPopulationsSkipUnavailableMetrics(t *testing.T) {
	for _, population := range []string{"empty", "latency-only", "ttft-only"} {
		t.Run(population, func(t *testing.T) {
			repo, conn := newLatencyRoutingRepository(t)
			conn.query = func(_ context.Context, stage int, query string, _ []driver.NamedValue) (*latencyRoutingRows, error) {
				if stage == 0 {
					rows := &latencyRoutingRows{columns: []string{"timestamp_ms", "latency_positive", "ttft_positive"}}
					if population == "latency-only" {
						rows.values = [][]driver.Value{{int64(1), int64(1), int64(0)}}
					} else if population == "ttft-only" {
						rows.values = [][]driver.Value{{int64(1), int64(0), int64(1)}}
					}
					return rows, nil
				}
				if population == "empty" || stage != 1 {
					t.Fatal("empty metric population was queried")
				}
				column := "latency_ms"
				if population == "ttft-only" {
					column = "ttft_ms"
				}
				if !strings.HasSuffix(query, "order by "+column) {
					t.Fatal("queried wrong metric", query)
				}
				return &latencyRoutingRows{columns: []string{"timestamp_ms", column}, values: [][]driver.Value{{int64(1), int64(17)}}}, nil
			}
			got, err := repo.LatencyMetricsWithFilter(context.Background(), AnalyticsFilter{ToMS: 2}, "hour", nil, true, true)
			want := LatencyMetrics{Timeline: []LatencyPercentiles{}}
			if population != "empty" {
				point := LatencyPercentiles{BucketMS: 0}
				if population == "latency-only" {
					point.P95LatencyMS = sql.NullFloat64{Float64: 17, Valid: true}
				} else {
					point.P95TTFTMS = sql.NullFloat64{Float64: 17, Valid: true}
				}
				want.Summary = LatencySummary{P95LatencyMS: point.P95LatencyMS, P95TTFTMS: point.P95TTFTMS}
				want.Timeline = append(want.Timeline, point)
			}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, want %#v, error %v", got, want, err)
			}
		})
	}
}

func TestLatencyReferenceRejectsChangedSnapshotPopulations(t *testing.T) {
	for _, change := range []string{"missing", "extra", "different-bucket"} {
		t.Run(change, func(t *testing.T) {
			repo, conn := newLatencyRoutingRepository(t)
			conn.query = func(_ context.Context, stage int, _ string, _ []driver.NamedValue) (*latencyRoutingRows, error) {
				rows := latencyRoutingFixture(stage)
				if stage == 1 {
					switch change {
					case "missing":
						rows.values = rows.values[:len(rows.values)-1]
					case "extra":
						rows.values = append(rows.values, rows.values[len(rows.values)-1])
					case "different-bucket":
						rows.values[0][0] = int64(10 * 3_600_000)
					}
				}
				return rows, nil
			}
			got, err := repo.LatencyMetricsWithFilter(context.Background(), AnalyticsFilter{ToMS: 7_200_000}, "hour", nil, true, true)
			if err == nil || !strings.Contains(err.Error(), "population changed") || !reflect.DeepEqual(got, LatencyMetrics{}) || len(conn.queries) != 2 || conn.commits.Load() != 0 || conn.rollbacks.Load() != 1 {
				t.Fatalf("snapshot inconsistency returned partial values: %#v, %v", got, err)
			}
		})
	}
}

func TestLatencyReferenceNearestRankIntegerBoundaries(t *testing.T) {
	for _, count := range []int64{0, 1, 2, 19, 20, 21, 39, 40, 41, 100, 101, math.MaxInt64} {
		got := latencyReferenceRank(count)
		// For 95/100 = 19/20, this decomposition independently avoids overflow.
		want := (count/20)*19 + ((count%20)*19+19)/20
		if got != want {
			t.Fatalf("count %d: rank %d, want %d", count, got, want)
		}
	}
}

func TestLatencyReferenceLargePopulationStreamsWithoutSampleBuffers(t *testing.T) {
	const count = 40_001
	repo, conn := newLatencyRoutingRepository(t)
	conn.query = func(_ context.Context, stage int, _ string, _ []driver.NamedValue) (*latencyRoutingRows, error) {
		columns := []string{"timestamp_ms", "latency_positive", "ttft_positive"}
		if stage > 0 {
			columns = columns[:2]
		}
		return &latencyRoutingRows{columns: columns, next: func(index int, dest []driver.Value) error {
			if index == count {
				return io.EOF
			}
			dest[0] = int64(1)
			if stage == 0 {
				dest[1], dest[2] = int64(1), int64(1)
			} else {
				dest[1] = int64(index+1) * int64(stage)
			}
			return nil
		}}, nil
	}
	got, err := repo.LatencyMetricsWithFilter(context.Background(), AnalyticsFilter{ToMS: 2}, "hour", nil, true, true)
	if err != nil || len(got.Timeline) != 1 || got.Summary.P95LatencyMS != (sql.NullFloat64{Float64: 38_001, Valid: true}) || got.Summary.P95TTFTMS != (sql.NullFloat64{Float64: 76_002, Valid: true}) || got.Timeline[0].P95LatencyMS != got.Summary.P95LatencyMS || got.Timeline[0].P95TTFTMS != got.Summary.P95TTFTMS {
		t.Fatalf("streamed ranks %#v, %v", got, err)
	}
}

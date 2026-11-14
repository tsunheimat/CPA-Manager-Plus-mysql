package usagemonitoring

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

type monitoringReadTrace struct {
	base                                          driver.Driver
	dsn                                           string
	queries                                       []string
	stats, metadata, projection, latest, revision int
	fail                                          string
}

func (trace *monitoringReadTrace) Connect(context.Context) (driver.Conn, error) {
	conn, err := trace.base.Open(trace.dsn)
	if err != nil {
		return nil, err
	}
	return &monitoringTraceConn{Conn: conn, trace: trace}, nil
}
func (trace *monitoringReadTrace) Driver() driver.Driver { return trace.base }

type monitoringTraceConn struct {
	driver.Conn
	trace *monitoringReadTrace
}

func (conn *monitoringTraceConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return conn.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}
func (conn *monitoringTraceConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	trace := conn.trace
	trace.queries = append(trace.queries, query)
	if strings.Contains(query, "from usage_monitoring_rollup_state where rollup_name = ?") && len(args) > 0 {
		switch args[0].Value {
		case StatsRollupName:
			trace.stats++
		case MetadataRollupName:
			trace.metadata++
		case ProjectionRollupName:
			trace.projection++
		}
	}
	if strings.Contains(query, "select coalesce(max(id), 0) from usage_events") {
		trace.latest++
	}
	if strings.Contains(query, "select p.model, t.threshold_tokens") {
		trace.revision++
	}
	if trace.fail != "" && strings.Contains(query, trace.fail) {
		return nil, errors.New("injected attempted read failure")
	}
	return conn.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestMonitoringReadsPrepareOnlyEligibleStates(t *testing.T) {
	if testdb.MySQL() {
		t.Skip("physical statement instrumentation is SQLite-only")
	}
	path := filepath.Join(t.TempDir(), "read-dependencies.sqlite")
	original, err := testdb.Open(t, path)
	if err != nil {
		t.Fatal(err)
	}
	trace := &monitoringReadTrace{base: original.Driver(), dsn: path}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(trace)
	defer db.Close()
	db.SetMaxOpenConns(1)
	repo := New(db).(*repository)
	readers := map[string]func(context.Context, AnalyticsFilter) error{
		"aggregate": func(ctx context.Context, f AnalyticsFilter) error {
			_, _, _, err := repo.LoadAggregate(ctx, f)
			return err
		},
		"models": func(ctx context.Context, f AnalyticsFilter) error {
			_, _, _, err := repo.LoadModelStats(ctx, f)
			return err
		},
		"accounts": func(ctx context.Context, f AnalyticsFilter) error {
			_, _, _, err := repo.LoadAccountStats(ctx, f)
			return err
		},
		"keys": func(ctx context.Context, f AnalyticsFilter) error {
			_, _, _, err := repo.LoadAPIKeyStats(ctx, f)
			return err
		},
		"selectors": func(ctx context.Context, f AnalyticsFilter) error {
			_, _, _, err := repo.LoadFilterSelectors(ctx, f)
			return err
		},
	}
	for name, read := range readers {
		for _, eligible := range []bool{false, true} {
			*trace = monitoringReadTrace{base: trace.base, dsn: path}
			filter := AnalyticsFilter{FromMS: 1, ToMS: 100, IncludeFailed: true}
			if !eligible {
				filter.SearchQuery = "needle"
			}
			if err := read(context.Background(), filter); err != nil {
				t.Fatalf("%s eligible=%v: %v", name, eligible, err)
			}
			if trace.projection != 1 || trace.latest != 1 {
				t.Fatalf("%s lost transactional preparation: %+v", name, trace)
			}
			if !eligible && (trace.stats != 0 || trace.revision != 0 || trace.metadata != 0) {
				t.Fatalf("%s prepared unused state: %+v", name, trace)
			}
			if eligible && name == "selectors" && trace.metadata != 1 {
				t.Fatal("missing eligible metadata lookup")
			}
			if eligible && name != "selectors" && (trace.stats != 1 || trace.revision != 1) {
				t.Fatalf("%s missing eligible stats preparation: %+v", name, trace)
			}
		}
		trace.fail = "from usage_monitoring_rollup_state where rollup_name = ?"
		if err := read(context.Background(), AnalyticsFilter{FromMS: 1, ToMS: 100, IncludeFailed: true, SearchQuery: "needle"}); err == nil {
			t.Fatalf("%s swallowed attempted query failure", name)
		}
		trace.fail = ""
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := read(ctx, AnalyticsFilter{FromMS: 1, ToMS: 100}); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s cancellation = %v", name, err)
		}
	}
}

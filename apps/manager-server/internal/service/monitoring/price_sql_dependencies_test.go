package monitoring

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

// Trace physical executed SQL, not response omissions or service planner bits.
// No production instrumentation or driver registration is needed.
type priceSQLTrace struct {
	sync.Mutex
	queries []string
}

func (t *priceSQLTrace) record(query string) {
	t.Lock()
	defer t.Unlock()
	t.queries = append(t.queries, strings.Join(strings.Fields(strings.ToLower(query)), " "))
}
func (t *priceSQLTrace) reset() { t.Lock(); defer t.Unlock(); t.queries = nil }
func (t *priceSQLTrace) books() (base, contextTiers, serviceTiers, metadata int) {
	t.Lock()
	defer t.Unlock()
	for _, query := range t.queries {
		// Restrict to standalone catalog SELECTs, not pricing joins in event SQL.
		if !strings.HasPrefix(query, "select ") || !strings.Contains(query, "prompt_per_1m") {
			continue
		}
		if strings.Contains(query, "from model_prices order by model") {
			base++
			if strings.Contains(query, "raw_json") {
				metadata++
			}
		}
		if strings.Contains(query, "from model_price_context_tiers") {
			contextTiers++
		}
		if strings.Contains(query, "from model_price_service_tiers") {
			serviceTiers++
		}
	}
	return
}

type priceTraceDriver struct {
	driver.Driver
	trace *priceSQLTrace
}

func (d priceTraceDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &priceTraceConn{Conn: conn, trace: d.trace}, nil
}

type priceTraceConnector struct {
	driver priceTraceDriver
	dsn    string
}

func (c priceTraceConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}
func (c priceTraceConnector) Driver() driver.Driver { return c.driver }

type priceTraceConn struct {
	driver.Conn
	trace *priceSQLTrace
}

func (c *priceTraceConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.trace.record(query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *priceTraceConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *priceTraceConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}

func TestAnalyticsPhysicalPriceQueries(t *testing.T) {
	if testdb.MySQL() {
		t.Skip("physical trace is SQLite-only; native uses isolated integration harness")
	}
	path := filepath.Join(t.TempDir(), "trace.sqlite")
	schema, err := sqliterepo.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	trace := &priceSQLTrace{}
	wrapped := priceTraceDriver{Driver: schema.Driver(), trace: trace}
	if err := schema.Close(); err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(priceTraceConnector{driver: wrapped, dsn: path + "?_pragma=busy_timeout(5000)"})
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = db.Close() })
	svc := New(store.New(db))
	for _, include := range unpricedAnalyticsIncludes() {
		include.Percentiles = &PercentilesInclude{Summary: true, Timeline: true}
		trace.reset()
		if _, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 3600001, Include: include}); err != nil {
			t.Fatal(err)
		}
		base, tiers, services, metadata := trace.books()
		if base+tiers+services+metadata != 0 {
			t.Fatalf("unpriced catalog SQL executed: include=%+v base=%d context=%d service=%d metadata=%d", include, base, tiers, services, metadata)
		}
	}
	for _, include := range []Include{
		{Timeline: true, TimelineProfile: "core", AnomalyBasisVersion: new(1)},
		{FilterOptions: true}, {Summary: true, SummaryProfile: "compact", FilterSelectors: true},
	} {
		trace.reset()
		if _, err := svc.Analytics(context.Background(), Request{FromMS: 1, ToMS: 3600001, Include: include}); err != nil {
			t.Fatal(err)
		}
		base, tiers, services, metadata := trace.books()
		if base != 1 || tiers != 1 || services != 1 || metadata != 0 {
			t.Fatalf("lightweight catalog SQL: base=%d context=%d service=%d metadata=%d", base, tiers, services, metadata)
		}
	}
}

package modelprice_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
)

// A narrowly scripted driver verifies physical SELECT count/projections and
// failures which are difficult to arrange deterministically with a real DB.
type rateScript struct {
	queries                                         []string
	begins, commits, rollbacks, closes              int
	options                                         driver.TxOptions
	beginErr, commitErr, queryErr, rowErr, closeErr error
	failQuery                                       int
	badFlag                                         bool
}

type rateConnector struct{ script *rateScript }

func (c rateConnector) Driver() driver.Driver                        { return rateDriver{} }
func (c rateConnector) Connect(context.Context) (driver.Conn, error) { return rateConn{c.script}, nil }

type rateDriver struct{}

func (rateDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type rateConn struct{ script *rateScript }

func (c rateConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (c rateConn) Close() error                        { return nil }
func (c rateConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c rateConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	c.script.begins++
	c.script.options = options
	if c.script.beginErr != nil {
		return nil, c.script.beginErr
	}
	return rateTx{c.script}, nil
}
func (c rateConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	s := c.script
	s.queries = append(s.queries, query)
	if s.failQuery == len(s.queries) && s.queryErr != nil {
		return nil, s.queryErr
	}
	var names []string
	var values []driver.Value
	switch {
	case strings.Contains(query, "from model_prices "):
		names = []string{"model", "prompt_per_1m", "completion_per_1m", "cache_per_1m", "cache_read_per_1m", "cache_creation_per_1m", "prompt_configured", "completion_configured", "cache_read_configured", "cache_creation_configured"}
		values = []driver.Value{"m", float64(1), float64(2), float64(3), float64(4), float64(5), int64(1), float64(0), int64(1), int64(1)}
		if s.badFlag {
			values[6] = "invalid flag"
		}
	case strings.Contains(query, "from model_price_context_tiers "):
		names = []string{"model", "threshold_tokens", "prompt_per_1m", "completion_per_1m", "cache_per_1m", "cache_read_per_1m", "cache_creation_per_1m", "prompt_configured", "completion_configured", "cache_configured", "cache_read_configured", "cache_creation_configured"}
		values = []driver.Value{"m", int64(100), float64(0), float64(7), float64(8), float64(9), float64(10), int64(1), int64(1), int64(1), int64(1), int64(1)}
	case strings.Contains(query, "from model_price_service_tiers "):
		names = []string{"model", "mode", "service_tier", "prompt_per_1m", "completion_per_1m", "cache_per_1m", "cache_read_per_1m", "cache_creation_per_1m", "prompt_configured", "completion_configured", "cache_configured", "cache_read_configured", "cache_creation_configured"}
		values = []driver.Value{"m", "fast", "priority", float64(11), float64(12), float64(13), float64(14), float64(15), int64(1), int64(1), int64(1), int64(1), int64(1)}
	default:
		return nil, errors.New("unexpected query")
	}
	return &rateRows{script: s, names: names, values: values, fail: s.failQuery == len(s.queries)}, nil
}

type rateTx struct{ script *rateScript }

func (tx rateTx) Commit() error   { tx.script.commits++; return tx.script.commitErr }
func (tx rateTx) Rollback() error { tx.script.rollbacks++; return nil }

type rateRows struct {
	script *rateScript
	names  []string
	values []driver.Value
	fail   bool
}

func (r *rateRows) Columns() []string { return r.names }
func (r *rateRows) Close() error {
	r.script.closes++
	if r.fail {
		return r.script.closeErr
	}
	return nil
}
func (r *rateRows) Next(out []driver.Value) error {
	if r.values != nil {
		copy(out, r.values)
		r.values = nil
		return nil
	}
	if r.fail && r.script.rowErr != nil {
		return r.script.rowErr
	}
	return io.EOF
}

func TestLoadRatesPhysicalQueriesAndTransactionOwnership(t *testing.T) {
	script := &rateScript{}
	db := sql.OpenDB(rateConnector{script})
	defer db.Close()
	prices, err := modelprice.New(db).LoadRates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(script.queries) != 3 || script.begins != 1 || script.commits != 1 || script.rollbacks != 0 || script.closes != 3 || !script.options.ReadOnly {
		t.Fatalf("physical book work=%+v", script)
	}
	base := script.queries[0]
	for _, metadata := range []string{"raw_json", "source_model_id", "updated_at_ms", "synced_at_ms", "source,"} {
		if strings.Contains(base, metadata) {
			t.Fatalf("analytics selected %s: %s", metadata, base)
		}
	}
	projection := strings.Split(strings.Split(base, " from ")[0], "select ")[1]
	if len(strings.Split(projection, ",")) != 10 {
		t.Fatalf("base must project exactly ten rate fields: %s", base)
	}
	p := prices["m"]
	if !p.PromptConfigured || p.CompletionConfigured || p.CacheCreation != 5 || len(p.ContextTiers) != 1 || !p.ContextTiers[0].PromptConfigured || p.ContextTiers[0].Prompt != 0 || len(p.ServiceTiers) != 1 || p.ServiceTiers[0].CacheCreation != 15 {
		t.Fatalf("shared configured scanner/child hydration=%+v", p)
	}
}

func TestLoadRatesAttemptedFailuresAreTerminal(t *testing.T) {
	failure := errors.New("injected database failure")
	cases := map[string]*rateScript{
		"begin":         {beginErr: failure},
		"base-query":    {failQuery: 1, queryErr: failure},
		"context-query": {failQuery: 2, queryErr: failure},
		"service-query": {failQuery: 3, queryErr: failure},
		"base-row":      {failQuery: 1, rowErr: failure},
		"context-row":   {failQuery: 2, rowErr: failure},
		"service-row":   {failQuery: 3, rowErr: failure},
		"base-close":    {failQuery: 1, closeErr: failure},
		"context-close": {failQuery: 2, closeErr: failure},
		"service-close": {failQuery: 3, closeErr: failure},
		"commit":        {commitErr: failure},
	}
	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			db := sql.OpenDB(rateConnector{script})
			defer db.Close()
			prices, err := modelprice.New(db).LoadRates(context.Background())
			if !errors.Is(err, failure) || prices != nil {
				t.Fatalf("partial/success result on error: %+v %v", prices, err)
			}
			if script.failQuery > 0 && len(script.queries) != script.failQuery {
				t.Fatalf("retried or continued after failure: %+v", script)
			}
			if name != "begin" && name != "commit" && script.rollbacks != 1 {
				t.Fatalf("failed load did not rollback: %+v", script)
			}
		})
	}
}

func TestLoadRatesTxScanFailureClosesRowsWithoutEndingCallerTransaction(t *testing.T) {
	script := &rateScript{badFlag: true}
	db := sql.OpenDB(rateConnector{script})
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	prices, err := modelprice.New(db).LoadRatesTx(context.Background(), tx)
	if err == nil || !strings.Contains(err.Error(), "invalid configured flag") || prices != nil {
		t.Fatalf("invalid scanner value accepted: %+v %v", prices, err)
	}
	if script.closes != 1 || script.commits != 0 || script.rollbacks != 0 || len(script.queries) != 1 {
		t.Fatalf("scan failure leaked rows or ended caller transaction: %+v", script)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

package usageevent

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

type bulkTestDriver struct{}

func (bulkTestDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type bulkTestConnector struct{ conn *bulkTestConn }

func (c bulkTestConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c bulkTestConnector) Driver() driver.Driver                        { return bulkTestDriver{} }

type bulkTestConn struct {
	raw, pendingRaw       map[string]bulkIdentity
	ledger, pendingLedger map[string]int64
	fail                  string
	failAfter             int
	nextID                int64
	committed, rolledBack bool
	statements            []string
}

func (c *bulkTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *bulkTestConn) Close() error { return nil }
func (c *bulkTestConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *bulkTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.pendingRaw = map[string]bulkIdentity{}
	for k, v := range c.raw {
		c.pendingRaw[k] = v
	}
	c.pendingLedger = map[string]int64{}
	for k, v := range c.ledger {
		c.pendingLedger[k] = v
	}
	return c, nil
}
func (c *bulkTestConn) Commit() error {
	c.raw = c.pendingRaw
	c.ledger = c.pendingLedger
	c.committed = true
	return nil
}
func (c *bulkTestConn) Rollback() error { c.rolledBack = true; return nil }
func (c *bulkTestConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.statements = append(c.statements, query)
	q := strings.ToLower(strings.ReplaceAll(query, "`", ""))
	if strings.Contains(q, "ignore") || strings.Contains(q, "last_insert_id") {
		return nil, errors.New("unsafe bulk SQL")
	}
	phase := ""
	width := 0
	switch {
	case strings.Contains(q, "insert into usage_event_identity_ledger"):
		phase, width = "claim", 5
	case strings.Contains(q, "insert into usage_events"):
		phase, width = "raw", 65
	case strings.Contains(q, "update usage_event_identity_ledger"):
		phase, width = "attach", 5
	default:
		return nil, fmt.Errorf("unexpected bulk statement %s", query)
	}
	if c.fail == phase && len(c.statements) > c.failAfter {
		return nil, fmt.Errorf("injected %s failure", phase)
	}
	if c.fail == "count" && phase == "attach" {
		return bulkTestResult(0), nil
	}
	for i := 0; i < len(args); i += width {
		switch phase {
		case "claim":
			c.pendingLedger[args[i].Value.(string)] = 0
		case "raw":
			hash := args[i+1].Value.(string)
			if _, ok := c.pendingRaw[hash]; ok {
				return nil, errors.New("duplicate raw identity")
			}
			c.nextID += 17 // Deliberately non-contiguous IDs; no LastInsertId inference.
			c.pendingRaw[hash] = bulkIdentity{c.nextID, args[i+2].Value.(int64), args[i+64].Value.(int64)}
		case "attach":
			c.pendingLedger[args[i].Value.(string)] = args[i+1].Value.(int64)
		}
	}
	return bulkTestResult(len(args) / width), nil
}
func (c *bulkTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &bulkTestRows{}
	if strings.Contains(query, "from usage_event_identity_ledger") {
		result.columns = []string{"event_hash"}
		for _, arg := range args {
			hash := arg.Value.(string)
			if _, ok := c.pendingLedger[hash]; ok {
				result.data = append(result.data, []driver.Value{hash})
			}
		}
	} else if strings.Contains(query, "from usage_events") {
		result.columns = []string{"event_hash", "id", "timestamp_ms", "created_at_ms"}
		for _, arg := range args {
			hash := arg.Value.(string)
			if identity, ok := c.pendingRaw[hash]; ok {
				result.data = append(result.data, []driver.Value{hash, identity.id, identity.timestamp, identity.created})
			}
		}
	} else {
		return nil, fmt.Errorf("unexpected bulk query %s", query)
	}
	return result, nil
}

type bulkTestResult int64

func (bulkTestResult) LastInsertId() (int64, error)   { panic("bulk must resolve IDs by full identity") }
func (r bulkTestResult) RowsAffected() (int64, error) { return int64(r), nil }

type bulkTestRows struct {
	columns  []string
	data     [][]driver.Value
	position int
}

func (r *bulkTestRows) Columns() []string { return r.columns }
func (r *bulkTestRows) Close() error      { return nil }
func (r *bulkTestRows) Next(dest []driver.Value) error {
	if r.position == len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.position])
	r.position++
	return nil
}

func TestBulkContractRollsBackEarlierChunks(t *testing.T) {
	for _, phase := range []string{"claim", "raw", "attach"} {
		t.Run(phase, func(t *testing.T) {
			conn := &bulkTestConn{raw: map[string]bulkIdentity{}, ledger: map[string]int64{}, fail: phase, failAfter: 3}
			db := sql.OpenDB(bulkTestConnector{conn})
			defer db.Close()
			repo := New(db).(*repository)
			input := make([]preparedUsageEvent, bulkMaxRows+1)
			for i := range input {
				var err error
				input[i], err = repo.prepareUsageEvent(usage.Event{EventHash: fmt.Sprintf("%064x", i+1), TimestampMS: int64(i), Timestamp: "synthetic", Model: "model", TotalTokens: 1, CreatedAtMS: 1})
				if err != nil {
					t.Fatal(err)
				}
			}
			result, err := repo.insertPreparedBulk(context.Background(), input, model.InsertResult{})
			if err == nil || !conn.rolledBack || conn.committed || len(conn.raw) != 0 || len(conn.ledger) != 0 || result.Inserted != 0 || len(conn.statements) < 4 {
				t.Fatalf("chunk committed before %s failure: result=%#v err=%v", phase, result, err)
			}
		})
	}
}

func TestBulkContractIdentityResolutionAndEveryPhaseRollback(t *testing.T) {
	for _, fail := range []string{"", "claim", "raw", "attach", "count"} {
		t.Run(fail, func(t *testing.T) {
			archived := fmt.Sprintf("%064x", 900)
			legacy := "long-legacy-" + strings.Repeat("界", 100)
			conn := &bulkTestConn{raw: map[string]bulkIdentity{legacy: {7, 77, 17}}, ledger: map[string]int64{archived: 0}, fail: fail, nextID: 100}
			db := sql.OpenDB(bulkTestConnector{conn})
			defer db.Close()
			repo := New(db).(*repository)
			input := []preparedUsageEvent{}
			for _, hash := range []string{archived, fmt.Sprintf("%064x", 1), fmt.Sprintf("%064x", 1), fmt.Sprintf("%064x", 2), legacy} {
				p, err := repo.prepareUsageEvent(usage.Event{EventHash: hash, TimestampMS: 1000, Timestamp: "synthetic", Model: "model", InputTokens: 1, TotalTokens: 1, CreatedAtMS: 1})
				if err != nil {
					t.Fatal(err)
				}
				input = append(input, p)
			}
			result, err := repo.insertPreparedBulk(context.Background(), input, model.InsertResult{})
			if fail != "" {
				if err == nil || !conn.rolledBack || conn.committed || len(conn.raw) != 1 || len(conn.ledger) != 1 || result.Inserted != 0 || result.Skipped != 0 {
					t.Fatalf("partial failure %s result=%#v err=%v conn=%#v", fail, result, err, conn)
				}
				return
			}
			if err != nil || !conn.committed || result.Inserted != 2 || result.Skipped != 3 || !reflect.DeepEqual(result.InsertedEventHashes, []string{fmt.Sprintf("%064x", 1), fmt.Sprintf("%064x", 2)}) {
				t.Fatal(result, err)
			}
			if _, ok := conn.raw[archived]; ok {
				t.Fatal("archived ledger identity resurrected")
			}
			for hash, identity := range conn.raw {
				if conn.ledger[hash] != identity.id {
					t.Fatal("inferred or missing ID", hash, conn.ledger[hash], identity.id)
				}
			}
			if conn.raw[legacy].timestamp != 77 || conn.raw[legacy].created != 17 {
				t.Fatal("legacy raw evidence overwritten")
			}
			for _, statement := range conn.statements {
				if strings.Contains(statement, legacy) {
					t.Fatal("identity interpolated instead of bound")
				}
			}
		})
	}
}

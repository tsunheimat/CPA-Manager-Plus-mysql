package usagearchive

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

type archiveContractDriver struct{}

func (archiveContractDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type archiveContractConnector struct{ conn *archiveContractConn }

func (c archiveContractConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c archiveContractConnector) Driver() driver.Driver                        { return archiveContractDriver{} }

type archiveContractConn struct {
	fail                  string
	wrongCount            bool
	statements            int
	phases                []string
	committed, rolledBack bool
}

func (c *archiveContractConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *archiveContractConn) Close() error              { return nil }
func (c *archiveContractConn) Begin() (driver.Tx, error) { return c, nil }
func (c *archiveContractConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c, nil
}
func (c *archiveContractConn) Commit() error   { c.committed = true; return nil }
func (c *archiveContractConn) Rollback() error { c.rolledBack = true; return nil }
func (c *archiveContractConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !strings.Contains(q, "select count(*) from usage_events") {
		return nil, fmt.Errorf("unexpected archive query %s", q)
	}
	return &archiveContractRows{}, nil // Publication starts with all ledger claims present.
}
func (c *archiveContractConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q = strings.ToLower(strings.ReplaceAll(q, "`", ""))
	if strings.Contains(q, "ignore") {
		return nil, errors.New("archive bulk must be strict")
	}
	phase, offset := "", 0
	switch {
	case strings.Contains(q, "insert into usage_event_identity_ledger"):
		phase, offset = "claim", 2
	case strings.Contains(q, "insert into usage_archive_event_refs"):
		phase, offset = "publish", 3
	case strings.Contains(q, "delete from usage_events"):
		phase = "raw"
	case strings.Contains(q, "update usage_event_identity_ledger l join"):
		phase, offset = "attach", 1
	case strings.Contains(q, "update usage_event_identity_ledger"):
		phase, offset = "ledger", 1
	case strings.Contains(q, "update usage_archive_event_refs"):
		phase, offset = "reference", 2
	default:
		return nil, fmt.Errorf("unexpected archive statement %s", q)
	}
	c.statements++
	c.phases = append(c.phases, phase)
	count := int64((len(args) - offset) / 2)
	if phase == "claim" {
		count = 0
	}
	// Inject only in the second chunk, after the first chunk's complete DML.
	if c.statements > 3 && phase == c.fail {
		if c.wrongCount {
			return driver.RowsAffected(count + 1), nil
		}
		return nil, errors.New("injected archive failure")
	}
	return driver.RowsAffected(count), nil
}

type archiveContractRows struct{ done bool }

func (*archiveContractRows) Columns() []string { return []string{"count"} }
func (*archiveContractRows) Close() error      { return nil }
func (r *archiveContractRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(0)
	return nil
}

func TestArchiveBulkEveryPhaseFailsAtomicallyAcrossChunks(t *testing.T) {
	for _, publish := range []bool{false, true} {
		phases := []string{"", "raw", "ledger", "reference"}
		if publish {
			phases = []string{"", "claim", "attach", "publish"}
		}
		for _, phase := range phases {
			for _, wrongCount := range []bool{false, true} {
				t.Run(fmt.Sprintf("publication=%t/phase=%s/count=%t", publish, phase, wrongCount), func(t *testing.T) {
					conn := &archiveContractConn{fail: phase, wrongCount: wrongCount}
					db := sql.OpenDB(archiveContractConnector{conn})
					defer db.Close()
					ctx := context.Background()
					tx, err := sqlwrite.BeginTx(ctx, db, nil)
					if err != nil {
						t.Fatal(err)
					}
					refs := make([]RecordRef, archiveBulkRows+5)
					candidates := make([]deleteCandidate, len(refs))
					for i := range refs {
						hash := fmt.Sprintf("%064x", i+1)
						refs[i] = RecordRef{EventID: int64(i + 1), EventHash: hash}
						candidates[i] = deleteCandidate{ID: int64(i + 1), EventHash: hash, TimestampMS: 1000}
					}
					if publish {
						err = recordSegmentBulk(ctx, tx, "run", 1, refs, 2000)
					} else {
						err = deleteCandidatesBulk(ctx, tx, "run", candidates, 2000)
					}
					if err == nil {
						err = tx.Commit()
					} else {
						_ = tx.Rollback()
					}
					if phase != "" {
						if err == nil || conn.committed || !conn.rolledBack || conn.statements <= 3 {
							t.Fatal("partial application batch", phase, err, conn)
						}
						if wrongCount && !errors.Is(err, ErrCoverageIncomplete) {
							t.Fatal("count mismatch lost typed coverage error", err)
						}
					} else if err != nil || !conn.committed || conn.statements != 6 {
						t.Fatal("success", err, conn)
					}
					if !publish && strings.Join(conn.phases[:3], ",") != "raw,ledger,reference" {
						t.Fatal("delete ordering changed", conn.phases)
					}
				})
			}
		}
	}
}

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

var (
	// ErrWriteAdmissionRequired indicates an unmigrated runtime write boundary.
	// The driver never waits for admission after checking out a connection.
	ErrWriteAdmissionRequired = errors.New("MySQL write requires pre-checkout admission; use internal/sqlwrite")
	ErrWritePermitUsed        = errors.New("MySQL write permit is in use, transferred, or released")
	ErrReadOnlyTransaction    = errors.New("MySQL write in read-only transaction")
	ErrTransactionControl     = errors.New("MySQL runtime transaction control requires database/sql BeginTx, Commit, or Rollback")
)

var _ sqlwrite.Admitter = (*dialectDriver)(nil)

type permitKey struct{}

type permitState uint8

const (
	permitReady permitState = iota
	permitExecuting
	permitTransaction
	permitReleased
)

// A permit is connector-bound, never DB-pointer-bound. The helper owns it until
// an actual driver transaction successfully begins. Failed/bad-connection begin
// attempts and autocommit retries temporarily borrow it and return it to ready.
// No context watcher releases a permit while SQL or transaction cleanup is still
// running. database/sql owns cancellation-driven transaction rollback.
type writePermit struct {
	owner            *connector
	mu               sync.Mutex
	state            permitState
	releaseRequested bool
	acquired         time.Time
}

func (d *dialectDriver) AdmitWrite(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if existing, ok := ctx.Value(permitKey{}).(*writePermit); ok {
		existing.mu.Lock()
		usable := existing.owner == d.owner && existing.state == permitReady && !existing.releaseRequested
		existing.mu.Unlock()
		if !usable {
			// In particular, never queue recursively behind our own transaction.
			return nil, nil, ErrWritePermitUsed
		}
		return ctx, func() {}, nil
	}
	if d.owner == nil || d.owner.writer == nil {
		return nil, nil, ErrWriteAdmissionRequired
	}
	start := time.Now()
	d.owner.waiting.Add(1)
	select {
	case d.owner.writer <- struct{}{}:
		d.owner.waiting.Add(-1)
		d.owner.waitNanos.Add(int64(time.Since(start)))
		// A select can choose the channel even when cancellation is ready.
		if err := ctx.Err(); err != nil {
			<-d.owner.writer
			d.owner.canceled.Add(1)
			return nil, nil, err
		}
	case <-ctx.Done():
		d.owner.waiting.Add(-1)
		d.owner.waitNanos.Add(int64(time.Since(start)))
		d.owner.canceled.Add(1)
		return nil, nil, ctx.Err()
	}
	d.owner.acquired.Add(1)
	permit := &writePermit{owner: d.owner, acquired: time.Now()}
	return context.WithValue(ctx, permitKey{}, permit), permit.releaseUntransferred, nil
}

func (c *connection) borrowPermit(ctx context.Context) (*writePermit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	permit, ok := ctx.Value(permitKey{}).(*writePermit)
	if !ok || permit.owner != c.owner {
		return nil, ErrWriteAdmissionRequired
	}
	permit.mu.Lock()
	defer permit.mu.Unlock()
	if permit.state != permitReady || permit.releaseRequested {
		return nil, ErrWritePermitUsed
	}
	permit.state = permitExecuting
	return permit, nil
}

// finishAttempt transfers only a successful begin, never a retry/error. A
// concurrent release request is delayed until the physical attempt completes.
func (p *writePermit) finishAttempt(transaction bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if transaction {
		p.state = permitTransaction
	} else if p.releaseRequested {
		p.releaseLocked()
	} else {
		p.state = permitReady
	}
}

func (p *writePermit) releaseUntransferred() {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.state {
	case permitReady:
		p.releaseLocked()
	case permitExecuting:
		p.releaseRequested = true
	}
}

func (p *writePermit) finishTransaction() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == permitTransaction {
		p.releaseLocked()
	}
}

func (p *writePermit) releaseLocked() {
	p.state = permitReleased
	p.owner.holdNanos.Add(int64(time.Since(p.acquired)))
	<-p.owner.writer
}

// AdmissionStats contains connector-scoped cumulative timings, not per-request
// pool wait estimates. It contains no SQL, parameters or connection credentials.
type AdmissionStats struct {
	Waiting      int64
	Active       int
	Acquired     uint64
	Canceled     uint64
	WaitDuration time.Duration
	HoldDuration time.Duration
}

// WriteAdmissionStats reports the runtime MySQL connector's admission counters.
// Native administrative pools and non-MySQL pools return false.
func WriteAdmissionStats(db *sql.DB) (AdmissionStats, bool) {
	if db == nil {
		return AdmissionStats{}, false
	}
	d, ok := db.Driver().(*dialectDriver)
	if !ok || d.owner == nil {
		return AdmissionStats{}, false
	}
	c := d.owner
	return AdmissionStats{
		Waiting: c.waiting.Load(), Active: len(c.writer), Acquired: c.acquired.Load(), Canceled: c.canceled.Load(),
		WaitDuration: time.Duration(c.waitNanos.Load()), HoldDuration: time.Duration(c.holdNanos.Load()),
	}, true
}

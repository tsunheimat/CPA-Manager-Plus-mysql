package usagearchive

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

const archiveBulkRows = 200
const archiveBulkBytes = 1 << 20

type deleteCandidate struct {
	ID          int64
	EventHash   string
	TimestampMS int64
}

// archiveRefChunks caps rows, placeholders and conservatively encoded bytes.
// The caller owns one transaction across every chunk and publication phase.
func archiveRefChunks(refs []RecordRef, fn func([]RecordRef) error) error {
	for start := 0; start < len(refs); {
		end := start
		size := 0
		for end < len(refs) && end-start < archiveBulkRows {
			n := len(refs[end].EventHash)*2 + 128
			if n > archiveBulkBytes {
				return fmt.Errorf("archive identity exceeds native statement byte budget")
			}
			if end > start && size+n > archiveBulkBytes {
				break
			}
			size += n
			end++
		}
		if err := fn(refs[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func archivePairs(refs []RecordRef) (string, []any) {
	values := make([]string, len(refs))
	args := make([]any, 0, len(refs)*2)
	for i, ref := range refs {
		values[i] = "(?,?)"
		args = append(args, ref.EventID, ref.EventHash)
	}
	return strings.Join(values, ","), args
}

func archiveBulkExec(ctx context.Context, tx *sql.Tx, query string, args []any, want int64) error {
	// Even shared scalar expressions must be compiled BEFORE attaching the native
	// statement marker; execution remains on this same admitted transaction.
	query, args, err := mysql.CompileSQL(query, args...)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, mysql.Native(query), args...)
	if err != nil {
		return err
	}
	got, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: bulk archive affected %d rows, expected %d", ErrCoverageIncomplete, got, want)
	}
	return nil
}

func recordSegmentBulk(ctx context.Context, tx *sql.Tx, runID string, sequence int, refs []RecordRef, nowMS int64) error {
	return archiveRefChunks(refs, func(chunk []RecordRef) error {
		pairs, args := archivePairs(chunk)
		scope := `(e.id,e.event_hash) in (` + pairs + `)`
		var missing int64
		if err := tx.QueryRowContext(ctx, `select count(*) from usage_events e where `+scope+` and not exists(select 1 from usage_event_identity_ledger l where l.event_hash=e.event_hash)`, args...).Scan(&missing); err != nil {
			return err
		}
		query := `insert into usage_event_identity_ledger(event_hash,raw_event_id,timestamp_ms,bucket_ms,aggregate_schema_version,first_seen_at_ms,updated_at_ms)
   select e.event_hash,e.id,e.timestamp_ms,e.timestamp_ms - (e.timestamp_ms % 3600000),0,case when e.created_at_ms > 0 then e.created_at_ms else ? end,?
   from usage_events e where ` + scope + ` and not exists(select 1 from usage_event_identity_ledger l where l.event_hash=e.event_hash)`
		if err := archiveBulkExec(ctx, tx, query, append([]any{nowMS, nowMS}, args...), missing); err != nil {
			return err
		}
		// Native JOIN update uses deliberate MySQL arithmetic, not SQLite syntax.
		result, err := tx.ExecContext(ctx, mysql.Native(`update usage_event_identity_ledger l join usage_events e on e.event_hash=l.event_hash
   set l.raw_event_id=e.id,l.timestamp_ms=e.timestamp_ms,l.bucket_ms=e.timestamp_ms-mod(e.timestamp_ms,3600000),l.updated_at_ms=?
   where `+scope+` and (l.raw_event_id is null or l.raw_event_id=e.id)`), append([]any{nowMS}, args...)...)
		if err != nil {
			return err
		}
		got, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if got != int64(len(chunk)) {
			return fmt.Errorf("%w: bulk ledger attachment matched %d of %d", ErrCoverageIncomplete, got, len(chunk))
		}
		query = `insert into usage_archive_event_refs(event_hash,run_id,segment_sequence,raw_event_id,timestamp_ms,archived_at_ms,raw_deleted_at_ms)
   select l.event_hash,?,?,l.raw_event_id,l.timestamp_ms,?,null from usage_event_identity_ledger l where (l.raw_event_id,l.event_hash) in (` + pairs + `)`
		return archiveBulkExec(ctx, tx, query, append([]any{runID, sequence, nowMS}, args...), int64(len(chunk)))
	})
}

func deleteCandidatesBulk(ctx context.Context, tx *sql.Tx, runID string, candidates []deleteCandidate, nowMS int64) error {
	refs := make([]RecordRef, len(candidates))
	for i, candidate := range candidates {
		refs[i] = RecordRef{EventID: candidate.ID, EventHash: candidate.EventHash}
	}
	return archiveRefChunks(refs, func(chunk []RecordRef) error {
		pairs, args := archivePairs(chunk)
		want := int64(len(chunk))
		if err := archiveBulkExec(ctx, tx, `delete from usage_events where (id,event_hash) in (`+pairs+`)`, args, want); err != nil {
			return err
		}
		if err := archiveBulkExec(ctx, tx, `update usage_event_identity_ledger set raw_event_id=null,updated_at_ms=? where (raw_event_id,event_hash) in (`+pairs+`)`, append([]any{nowMS}, args...), want); err != nil {
			return err
		}
		return archiveBulkExec(ctx, tx, `update usage_archive_event_refs set raw_deleted_at_ms=? where run_id=? and raw_deleted_at_ms is null and (raw_event_id,event_hash) in (`+pairs+`)`, append([]any{nowMS, runID}, args...), want)
	})
}

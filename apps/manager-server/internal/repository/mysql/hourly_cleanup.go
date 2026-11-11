package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const maxHourlyCleanupRows = 1000

func hourlyCleanupColumn(table string) (string, error) {
	switch table {
	case "usage_monitoring_account_hourly_rollups_v1", "usage_monitoring_api_key_hourly_rollups_v1":
		return "structure_revision", nil
	case "usage_monitoring_selector_hourly_rollups_v1":
		return "model_format_revision", nil
	default:
		return "", fmt.Errorf("unsupported hourly cleanup table %q", table)
	}
}

func hourlyCleanupSelection(table, column, comparison string) string {
	// Separate index ranges skip the entire active digest, including when there
	// are no obsolete rows. LIMIT on an unindexed full-text <> does not do that.
	return Native("select _identity_hash from " + QuoteIdentifier(table) +
		" force index (idx_hourly_revision_bucket) where _revision_hash " + comparison +
		" ? and " + QuoteIdentifier(column) + " <> ? order by _revision_hash,bucket_ms limit ?")
}

func hourlyCleanupDeletion(table, column string, count int) string {
	return Native("delete from " + QuoteIdentifier(table) + " where _identity_hash in (" +
		strings.TrimSuffix(strings.Repeat("?,", count), ",") + ") and " + QuoteIdentifier(column) +
		" <> ? and _revision_hash <> ? limit ?")
}

// CleanupHourlyRevisionBatch must run in the caller's admitted write transaction,
// using the current revision read/published in that transaction. Both periodic
// maintenance and catch-up use these same indexed, bounded probes. Deleting by
// the selected unique identity keys makes another invocation resume naturally;
// one extra candidate answers Pending without a separate full-generation EXISTS.
// A zero limit only probes for pending work. Full revisions remain residuals, so
// hashes are never the sole authority for deletion. A digest collision with the
// current generation conservatively retains obsolete rows rather than scanning
// or risking deletion of current data.
func CleanupHourlyRevisionBatch(ctx context.Context, tx *sql.Tx, table, revision string, limit int) (removed int64, pending bool, err error) {
	column, err := hourlyCleanupColumn(table)
	if err != nil {
		return 0, false, err
	}
	if revision == "" {
		return 0, false, nil
	}
	if limit < 0 {
		limit = 0
	}
	if limit > maxHourlyCleanupRows {
		limit = maxHourlyCleanupRows
	}
	digest := sha256.Sum256([]byte(revision))
	ids := make([][]byte, 0, limit+1)
	for _, comparison := range []string{"<", ">"} {
		rows, err := tx.QueryContext(ctx, hourlyCleanupSelection(table, column, comparison), digest[:], revision, limit+1-len(ids))
		if err != nil {
			return 0, false, err
		}
		for rows.Next() {
			var id []byte
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return 0, false, err
			}
			if len(id) != sha256.Size {
				rows.Close()
				return 0, false, errors.New("invalid hourly cleanup identity digest")
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return 0, false, err
		}
		if len(ids) > limit {
			break
		}
	}
	pending = len(ids) > limit
	if pending {
		ids = ids[:limit]
	}
	if len(ids) == 0 {
		return 0, pending, nil
	}
	args := make([]any, 0, len(ids)+3)
	for _, id := range ids {
		args = append(args, id)
	}
	args = append(args, revision, digest[:], len(ids))
	result, err := tx.ExecContext(ctx, hourlyCleanupDeletion(table, column, len(ids)), args...)
	if err != nil {
		return 0, false, err
	}
	removed, err = result.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if removed != int64(len(ids)) {
		return 0, false, errors.New("hourly cleanup candidates changed within the write transaction")
	}
	return removed, pending, nil
}

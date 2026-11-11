package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

const bulkMaxRows = 100
const bulkMaxBytes = 4 << 20

type bulkIdentity struct{ id, timestamp, created int64 }

// insertPreparedBulk never treats a ledger-only identity as a missing raw row.
// All preflight decisions are rechecked after writer admission; the insert count
// comes from proven new identities, not affected rows or auto-increment math.
func (r *repository) insertPreparedBulk(ctx context.Context, prepared []preparedUsageEvent, result model.InsertResult) (model.InsertResult, error) {
	tx, err := sqlwrite.BeginTx(ctx, r.db, nil)
	if err != nil {
		return model.InsertResult{}, err
	}
	defer tx.Rollback()
	unique := make([]preparedUsageEvent, 0, len(prepared))
	seen := map[string]bool{}
	for _, p := range prepared {
		if seen[p.event.EventHash] {
			result.Skipped++
			continue
		}
		seen[p.event.EventHash] = true
		unique = append(unique, p)
	}
	for start := 0; start < len(unique); {
		// Only identity parameters are sent by the preflight. Do not reject or
		// size payloads for archived/legacy duplicates that will not be inserted.
		end, identityBytes := start, 0
		for end < len(unique) && end-start < bulkMaxRows {
			n := bulkArgsSize([]any{unique[end].event.EventHash})
			if end > start && identityBytes+n > bulkMaxBytes {
				break
			}
			identityBytes += n
			end++
		}
		chunk := unique[start:end]
		hashes := make([]any, len(chunk))
		slots := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		for i, p := range chunk {
			hashes[i] = p.event.EventHash
		}
		ledger := map[string]bool{}
		rows, err := tx.QueryContext(ctx, `select event_hash from usage_event_identity_ledger where event_hash in (`+slots+`)`, hashes...)
		if err != nil {
			return model.InsertResult{}, err
		}
		for rows.Next() {
			var hash string
			if err := rows.Scan(&hash); err != nil {
				rows.Close()
				return model.InsertResult{}, err
			}
			ledger[hash] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return model.InsertResult{}, err
		}
		raw, err := bulkRawIdentities(ctx, tx, slots, hashes)
		if err != nil {
			return model.InsertResult{}, err
		}
		claims := [][]any{}
		inserts := [][]any{}
		claimed := []preparedUsageEvent{}
		inserted := map[string]bool{}
		for _, p := range chunk {
			hash := p.event.EventHash
			if ledger[hash] {
				result.Skipped++
				continue
			}
			identity, exists := raw[hash]
			if !exists && !usage.IsCanonicalSHA256Hex(hash) {
				return model.InsertResult{}, ErrInvalidEventHash
			}
			timestamp, created := p.event.TimestampMS, p.ledgerNowMS
			if exists {
				timestamp = identity.timestamp
				if identity.created > 0 {
					created = identity.created
				}
				result.Skipped++
			} else {
				inserts = append(inserts, preparedInsertArgs(p))
				inserted[hash] = true
			}
			claims = append(claims, []any{hash, timestamp, timestamp - timestamp%3_600_000, created, p.ledgerNowMS})
			claimed = append(claimed, p)
		}
		if err := bulkInsertRows(ctx, tx, "usage_event_identity_ledger", "event_hash,timestamp_ms,bucket_ms,first_seen_at_ms,updated_at_ms", claims); err != nil {
			return model.InsertResult{}, err
		}
		if err := bulkInsertRows(ctx, tx, "usage_events", usageInsertColumns, inserts); err != nil {
			return model.InsertResult{}, err
		}
		resolved, err := bulkRawIdentities(ctx, tx, slots, hashes)
		if err != nil {
			return model.InsertResult{}, err
		}
		if len(claimed) > 0 {
			values := make([]string, 0, len(claimed))
			args := []any{}
			for _, p := range claimed {
				identity, ok := resolved[p.event.EventHash]
				if !ok || identity.id <= 0 {
					return model.InsertResult{}, fmt.Errorf("bulk usage identity resolution failed")
				}
				if inserted[p.event.EventHash] && identity.timestamp != p.event.TimestampMS {
					return model.InsertResult{}, fmt.Errorf("bulk usage identity changed during insert")
				}
				values = append(values, "(?, ?, ?, ?, ?)")
				args = append(args, p.event.EventHash, identity.id, identity.timestamp, identity.timestamp-identity.timestamp%3_600_000, p.ledgerNowMS)
			}
			// Compile the bound relation separately, then use native joined UPDATE on
			// the same admitted transaction. No adapter rewrite of native expressions.
			relation, bind, err := mysql.CompileSQL(`with attached(event_hash,id,timestamp_ms,bucket_ms,updated_at_ms) as (values `+strings.Join(values, ",")+`) select * from attached`, args...)
			if err != nil {
				return model.InsertResult{}, err
			}
			query := `update usage_event_identity_ledger l join (` + relation + `) a on l.event_hash=a.event_hash set l.raw_event_id=a.id,l.timestamp_ms=a.timestamp_ms,l.bucket_ms=a.bucket_ms,l.updated_at_ms=a.updated_at_ms where l.raw_event_id is null`
			updated, err := tx.ExecContext(ctx, mysql.Native(query), bind...)
			if err := bulkExactResult(updated, err, len(claimed)); err != nil {
				return model.InsertResult{}, err
			}
		}
		for _, p := range claimed {
			if inserted[p.event.EventHash] {
				result.Inserted++
				result.InsertedEventHashes = append(result.InsertedEventHashes, p.event.EventHash)
			}
		}
		start = end
	}
	if err := tx.Commit(); err != nil {
		return model.InsertResult{}, err
	}
	return result, nil
}

func bulkRawIdentities(ctx context.Context, tx *sql.Tx, slots string, args []any) (map[string]bulkIdentity, error) {
	rows, err := tx.QueryContext(ctx, `select event_hash,id,timestamp_ms,coalesce(created_at_ms,0) from usage_events where event_hash in (`+slots+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]bulkIdentity{}
	for rows.Next() {
		var hash string
		var identity bulkIdentity
		if err := rows.Scan(&hash, &identity.id, &identity.timestamp, &identity.created); err != nil {
			return nil, err
		}
		result[hash] = identity
	}
	return result, rows.Err()
}

func bulkArgsSize(args []any) int {
	size := len(args) * 32
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			size += len(v) * 2
		case []byte:
			size += len(v) * 2
		}
	}
	return size
}

func bulkInsertRows(ctx context.Context, tx *sql.Tx, table, columns string, rows [][]any) error {
	for start := 0; start < len(rows); {
		end := start
		size := 0
		for end < len(rows) && end-start < bulkMaxRows {
			n := bulkArgsSize(rows[end])
			// The budget limits batching, not accepted event size. A single row
			// larger than the budget is sent alone, still strictly and inside this
			// transaction; the server's packet limit remains authoritative.
			if end > start && size+n > bulkMaxBytes {
				break
			}
			size += n
			end++
		}
		args := []any{}
		values := []string{}
		for _, row := range rows[start:end] {
			values = append(values, "("+strings.TrimRight(strings.Repeat("?,", len(row)), ",")+")")
			args = append(args, row...)
		}
		query, bind, err := mysql.CompileSQL(`insert into `+table+` (`+columns+`) values `+strings.Join(values, ","), args...)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, mysql.Native(query), bind...)
		if err := bulkExactResult(res, err, end-start); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func bulkExactResult(result sql.Result, err error, want int) error {
	if err != nil {
		return err
	}
	got, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if got != int64(want) {
		return fmt.Errorf("native bulk affected %d rows, expected %d", got, want)
	}
	return nil
}

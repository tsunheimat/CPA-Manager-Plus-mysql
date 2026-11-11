package mysql

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

// StartDerivedMaintenance removes obsolete projection generations in bounded
// transactions. MySQL owns physical page reuse; this never runs VACUUM or DDL.
func StartDerivedMaintenance(ctx context.Context, db *sql.DB) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			for {
				removed, err := CleanupDerivedBatch(ctx, db, 1000)
				if err != nil {
					if ctx.Err() == nil {
						log.Printf("[derived-migration] MySQL cleanup failed: %v", err)
					}
					break
				}
				if removed < 1000 {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func CleanupDerivedBatch(ctx context.Context, db *sql.DB, limit int) (int64, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var removed int64
	for _, target := range []struct{ table, state, name, column, revision string }{
		{"usage_monitoring_selector_daily_rollups_v1", "", "", "model_format_revision", usageidentity.ModelFormatVersion},
		{"usage_pricing_hourly_rollups_v1", "usage_pricing_rollup_state", "pricing_v1", "structure_revision", ""},
		{"usage_pricing_account_rollups_v1", "usage_pricing_rollup_state", "pricing_v1", "structure_revision", ""},
		{"usage_monitoring_account_daily_rollups_v1", "usage_monitoring_rollup_state", "stats_v1", "structure_revision", ""},
		{"usage_monitoring_api_key_daily_rollups_v1", "usage_monitoring_rollup_state", "stats_v1", "structure_revision", ""},
		{"usage_monitoring_account_hourly_rollups_v1", "usage_monitoring_rollup_state", "stats_hourly_v1", "structure_revision", ""},
		{"usage_monitoring_api_key_hourly_rollups_v1", "usage_monitoring_rollup_state", "stats_hourly_v1", "structure_revision", ""},
		{"usage_monitoring_selector_hourly_rollups_v1", "usage_monitoring_rollup_state", "metadata_hourly_v1", "model_format_revision", ""},
	} {
		if target.state != "" {
			err := tx.QueryRowContext(ctx, "select structure_revision from "+target.state+" where rollup_name=?", target.name).Scan(&target.revision)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return 0, err
			}
		}
		if target.revision == "" {
			continue
		}
		var count int64
		if strings.HasPrefix(target.table, "usage_monitoring_") && strings.Contains(target.table, "_hourly_") {
			count, _, err = CleanupHourlyRevisionBatch(ctx, tx, target.table, target.revision, limit-int(removed))
		} else {
			var result sql.Result
			result, err = tx.ExecContext(ctx, "delete from "+target.table+" where "+target.column+"<>? limit ?", target.revision, limit-int(removed))
			if err == nil {
				count, err = result.RowsAffected()
			}
		}
		if err != nil {
			return 0, err
		}
		removed += count
		if removed >= int64(limit) {
			break
		}
	}
	return removed, tx.Commit()
}

// Compact rebuilds InnoDB tables while the offline command holds the process
// lock. OPTIMIZE may need additional disk space and commits table by table.
func Compact(ctx context.Context, db *sql.DB, progress func(string) error) (map[string]any, error) {
	rows, err := db.QueryContext(ctx, "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE' order by table_name")
	if err != nil {
		return nil, err
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		tables = append(tables, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, name := range tables {
		if err := progress(name); err != nil {
			return nil, err
		}
		rows, err := db.QueryContext(ctx, "optimize table "+QuoteIdentifier(name))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var table, op, kind, message string
			if err := rows.Scan(&table, &op, &kind, &message); err != nil {
				rows.Close()
				return nil, err
			}
			if kind == "error" {
				rows.Close()
				return nil, errors.New(message)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return map[string]any{"driver": "mysql", "optimized_tables": tables}, nil
}

// Package mysqlmigrate copies a stopped Manager's database into an empty MySQL
// database. Schema upgrades are applied to a private snapshot, never the source.
package mysqlmigrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/database"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/processlock"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
)

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("migrate-to-mysql", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("sqlite-path", "", "existing SQLite database; stop Manager Server before migrating")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*source) == "" {
		return errors.New("usage: cpa-manager-plus migrate-to-mysql --sqlite-path PATH (configure the MySQL destination with USAGE_DB_DRIVER=mysql and USAGE_DB_DSN or USAGE_DB_DSN_FILE)")
	}
	cfg, err := config.LoadWithoutCreatingDefault()
	if err != nil {
		return err
	}
	if database.Driver(cfg) != "mysql" {
		return errors.New("migrate-to-mysql requires a MySQL destination configuration")
	}
	return Copy(ctx, *source, cfg, stdout)
}

func Copy(ctx context.Context, source string, cfg config.Config, stdout io.Writer) error {
	info, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("inspect source SQLite database: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return errors.New("source must be a non-empty SQLite database file")
	}
	sourceLock, err := processlock.Acquire(source)
	if err != nil {
		return fmt.Errorf("lock SQLite source; stop Manager Server and retry: %w", err)
	}
	defer sourceLock.Close()
	targetLock, err := database.Acquire(ctx, &cfg)
	if err != nil {
		return fmt.Errorf("lock MySQL destination: %w", err)
	}
	defer targetLock.Close()
	target, err := mysqlrepo.OpenNative(ctx, cfg.DBDSN)
	if err != nil {
		return err
	}
	defer target.Close()
	var existing int
	if err := target.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema=database()").Scan(&existing); err != nil {
		return err
	}
	if existing != 0 {
		return errors.New("MySQL destination must be an empty database; existing tables will not be overwritten")
	}

	dir, err := os.MkdirTemp("", "cpamp-mysql-migration-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	snapshotPath := filepath.Join(dir, "snapshot.sqlite")
	uri := url.URL{Scheme: "file", Path: filepath.ToSlash(sourceLock.DatabasePath()), RawQuery: "mode=ro"}
	input, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer input.Close()
	var isManager int
	if err := input.QueryRowContext(ctx, "select count(*) from sqlite_schema where type='table' and name in ('settings','usage_events')").Scan(&isManager); err != nil {
		return err
	}
	if isManager != 2 {
		return errors.New("source is not a CPA Manager Plus database")
	}
	if _, err := input.ExecContext(ctx, "vacuum into ?", snapshotPath); err != nil {
		return fmt.Errorf("create consistent private SQLite snapshot: %w", err)
	}
	snapshot, err := sqliterepo.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("upgrade private SQLite snapshot: %w", err)
	}
	defer snapshot.Close()
	violations, err := snapshot.QueryContext(ctx, "pragma foreign_key_check")
	if err != nil {
		return err
	}
	hasViolation := violations.Next()
	checkErr := violations.Err()
	violations.Close()
	if checkErr != nil {
		return checkErr
	}
	if hasViolation {
		return errors.New("SQLite snapshot has foreign key violations; migration stopped")
	}
	initialized, err := mysqlrepo.Open(ctx, cfg.DBDSN)
	if err != nil {
		return err
	}
	initialized.Close()
	if err := copyTables(ctx, snapshot, target, stdout, func() error { return targetLock.Check(ctx) }); err != nil {
		return fmt.Errorf("copy rolled back (source unchanged); retry with a new empty MySQL database: %w", err)
	}
	fmt.Fprintln(stdout, "Migration complete. Keep the original data.key and usage-archives directory with the MySQL-backed Manager Server. The source SQLite database is unchanged.")
	return nil
}

func copyTables(ctx context.Context, source, target *sql.DB, stdout io.Writer, checkLock func() error) error {
	conn, err := target.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// The private source passed foreign_key_check and every current table is
	// copied. Disabling checks on this one connection permits valid forward
	// self-references, without changing server-wide settings.
	if _, err := conn.ExecContext(ctx, "set foreign_key_checks=0"); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, "set foreign_key_checks=1"); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	// Preserve SQLite's high-water marks even when the highest raw rows were
	// archived and deleted. Reusing these IDs would skip rollup ingestion.
	sequences, err := source.QueryContext(ctx, "select name,seq from sqlite_sequence")
	if err != nil {
		return err
	}
	for sequences.Next() {
		var name string
		var seq int64
		if err := sequences.Scan(&name, &seq); err != nil {
			sequences.Close()
			return err
		}
		var auto int
		if err := conn.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema=database() and table_name=? and extra like '%auto_increment%'", name).Scan(&auto); err != nil {
			sequences.Close()
			return err
		}
		if auto > 0 {
			if seq == int64(^uint64(0)>>1) {
				sequences.Close()
				return errors.New("SQLite sequence exhausted")
			}
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("alter table %s auto_increment=%d", quote(name), seq+1)); err != nil {
				sequences.Close()
				return err
			}
		}
	}
	if err := sequences.Err(); err != nil {
		sequences.Close()
		return err
	}
	sequences.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tables, err := source.QueryContext(ctx, `select name from sqlite_schema where type='table' and name not like 'sqlite_%' and name not like 'usage_monitoring_event_search_v1%' order by name`)
	if err != nil {
		return err
	}
	var names []string
	for tables.Next() {
		var name string
		if err := tables.Scan(&name); err != nil {
			tables.Close()
			return err
		}
		names = append(names, name)
	}
	err = tables.Err()
	tables.Close()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := checkLock(); err != nil {
			return err
		}
		columnRows, err := tx.QueryContext(ctx, "select column_name from information_schema.columns where table_schema=database() and table_name=? and generation_expression = '' order by ordinal_position", name)
		if err != nil {
			return err
		}
		var columns []string
		for columnRows.Next() {
			var col string
			if err := columnRows.Scan(&col); err != nil {
				columnRows.Close()
				return err
			}
			columns = append(columns, quote(col))
		}
		err = columnRows.Err()
		columnRows.Close()
		if err != nil {
			return err
		}
		if len(columns) == 0 {
			// Parked legacy tables are superseded by their current copies.
			if strings.Contains(name, "_legacy_") {
				continue
			}
			return fmt.Errorf("source table %s has no MySQL schema equivalent", name)
		}
		if _, err := tx.ExecContext(ctx, "delete from "+quote(name)); err != nil {
			return err
		}
		rows, err := source.QueryContext(ctx, "select "+strings.Join(columns, ",")+" from "+quote(name))
		if err != nil {
			return fmt.Errorf("read source table %s: %w", name, err)
		}
		count, copyErr := copyRows(ctx, tx, name, columns, rows)
		rows.Close()
		if copyErr != nil {
			return fmt.Errorf("copy table %s: %w", name, copyErr)
		}
		var copied int64
		if err := tx.QueryRowContext(ctx, "select count(*) from "+quote(name)).Scan(&copied); err != nil {
			return err
		}
		if copied != count {
			return fmt.Errorf("row count mismatch in %s: source %d, target %d", name, count, copied)
		}
		fmt.Fprintf(stdout, "Copied %s: %d rows\n", name, count)
	}
	// SQLite has no hourly read models. Copying its shared state table must not
	// erase destination-only seeds or import coverage for empty MySQL caches.
	if _, err := tx.ExecContext(ctx, "delete from usage_monitoring_rollup_state where rollup_name in ('stats_hourly_v1','metadata_hourly_v1')"); err != nil {
		return err
	}
	if err := mysqlrepo.EnsureAccelerationState(ctx, tx); err != nil {
		return err
	}
	if err := checkLock(); err != nil {
		return err
	}
	return tx.Commit()
}

func copyRows(ctx context.Context, tx *sql.Tx, name string, columns []string, rows *sql.Rows) (int64, error) {
	const maxRows, maxBytes = 256, 2 * 1024 * 1024
	rowSQL := "(" + strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",") + ")"
	prefix := "insert into " + quote(name) + " (" + strings.Join(columns, ",") + ") values "
	var values []any
	batchRows, batchBytes := 0, 0
	var copied int64
	flush := func() error {
		if batchRows == 0 {
			return nil
		}
		query := prefix + strings.TrimSuffix(strings.Repeat(rowSQL+",", batchRows), ",")
		if _, err := tx.ExecContext(ctx, query, values...); err != nil {
			return err
		}
		copied += int64(batchRows)
		values = nil
		batchRows, batchBytes = 0, 0
		return nil
	}
	for rows.Next() {
		row := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range row {
			dest[i] = &row[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return copied, err
		}
		size := 0
		for _, value := range row {
			switch v := value.(type) {
			case string:
				size += len(v)
			case []byte:
				size += len(v)
			default:
				size += 16
			}
		}
		if batchRows > 0 && (batchRows >= maxRows || batchBytes+size > maxBytes) {
			if err := flush(); err != nil {
				return copied, err
			}
		}
		values = append(values, row...)
		batchRows++
		batchBytes += size
	}
	if err := rows.Err(); err != nil {
		return copied, err
	}
	if err := flush(); err != nil {
		return copied, err
	}
	return copied, nil
}

func quote(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

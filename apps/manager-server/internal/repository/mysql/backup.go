package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// BackupHeader describes the portable, lossless row stream. Generated columns
// are reconstructed by MySQL. Values are base64-encoded bytes; NULL stays NULL.
type BackupHeader struct {
	Version       int           `json:"version"`
	SchemaVersion int           `json:"schema_version"`
	Tables        []BackupTable `json:"tables"`
}
type BackupTable struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Rows    int64    `json:"rows"`
	NextID  int64    `json:"next_id,omitempty"`
}

func QuoteIdentifier(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func backupLayout(ctx context.Context, db *sql.DB) (BackupHeader, error) {
	h := BackupHeader{Version: 1}
	if err := db.QueryRowContext(ctx, "select version from manager_schema_version where id=1").Scan(&h.SchemaVersion); err != nil {
		return h, err
	}
	if h.SchemaVersion < 1 || h.SchemaVersion > schemaVersion {
		return h, fmt.Errorf("unsupported MySQL schema version %d", h.SchemaVersion)
	}
	rows, err := db.QueryContext(ctx, "select table_name,coalesce(auto_increment,0) from information_schema.tables where table_schema=database() and table_type='BASE TABLE' order by table_name")
	if err != nil {
		return h, err
	}
	for rows.Next() {
		var t BackupTable
		if err := rows.Scan(&t.Name, &t.NextID); err != nil {
			rows.Close()
			return h, err
		}
		h.Tables = append(h.Tables, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return h, err
	}
	for i := range h.Tables {
		t := &h.Tables[i]
		cols, err := db.QueryContext(ctx, "select column_name from information_schema.columns where table_schema=database() and table_name=? and generation_expression='' order by ordinal_position", t.Name)
		if err != nil {
			return h, err
		}
		for cols.Next() {
			var c string
			if err := cols.Scan(&c); err != nil {
				cols.Close()
				return h, err
			}
			t.Columns = append(t.Columns, c)
		}
		err = cols.Err()
		cols.Close()
		if err != nil {
			return h, err
		}
	}
	if err := validateBackupLayout(h); err != nil {
		return h, err
	}
	if h.SchemaVersion >= 2 {
		conn, err := db.Conn(ctx)
		if err != nil {
			return h, err
		}
		err = validateHourlySchema(ctx, conn)
		conn.Close()
		if err != nil {
			return h, err
		}
	}
	return h, nil
}

// Validate against the historical source schema, never a lenient subset of the
// destination. Unknown future versions and missing/extra/reordered columns are
// rejected before any DELETE, ALTER, or filesystem publication.
func validateBackupLayout(h BackupHeader) error {
	if h.Version != 1 {
		return errors.New("unsupported MySQL backup format")
	}
	expected, err := versionedBackupLayout(h.SchemaVersion)
	if err != nil {
		return err
	}
	if len(h.Tables) != len(expected) {
		return errors.New("backup table layout does not match its schema version")
	}
	for i, t := range h.Tables {
		if t.Name != expected[i].Name || !reflect.DeepEqual(t.Columns, expected[i].Columns) || t.Rows < 0 || t.NextID < 0 {
			return errors.New("backup table layout does not match its schema version")
		}
	}
	return nil
}

func columnList(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = QuoteIdentifier(c)
	}
	return strings.Join(out, ",")
}

// WriteBackup must run while the Manager process lock is held. All row counts
// and rows belong to one repeatable-read snapshot, including empty tables.
func WriteBackup(ctx context.Context, db *sql.DB, output io.Writer) error {
	h, err := backupLayout(ctx, db)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := range h.Tables {
		if err := tx.QueryRowContext(ctx, "select count(*) from "+QuoteIdentifier(h.Tables[i].Name)).Scan(&h.Tables[i].Rows); err != nil {
			return err
		}
	}
	enc := json.NewEncoder(output)
	if err := enc.Encode(h); err != nil {
		return err
	}
	for _, t := range h.Tables {
		rows, err := tx.QueryContext(ctx, "select "+columnList(t.Columns)+" from "+QuoteIdentifier(t.Name))
		if err != nil {
			return err
		}
		var count int64
		for rows.Next() {
			values := make([][]byte, len(t.Columns))
			dest := make([]any, len(values))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				return err
			}
			if err := enc.Encode(values); err != nil {
				rows.Close()
				return err
			}
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if count != t.Rows {
			return fmt.Errorf("backup row count changed for %s", t.Name)
		}
	}
	return tx.Commit()
}

type RestoreTransaction struct {
	Tx   *sql.Tx
	conn *sql.Conn
}

func (r *RestoreTransaction) Close() {
	_ = r.Tx.Rollback()
	resetRestoreConnection(r.conn)
}

// A connection whose session checks could not be restored must not re-enter the
// native administrative pool with foreign-key validation disabled.
func resetRestoreConnection(conn *sql.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, "set foreign_key_checks=1"); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	_ = conn.Close()
}

// PreparedRestore owns a validated, private row spool. Preparation never opens a
// destination connection, so offline callers can reject corrupt/unknown snapshots
// before an implicitly committing schema upgrade. Close must be called on every
// outcome, including an upgrade failure. The spool also avoids reopening mutable
// snapshot payloads after validation.
type PreparedRestore struct {
	header BackupHeader
	rows   *os.File
}

func (p *PreparedRestore) Close() {
	if p.rows != nil {
		name := p.rows.Name()
		_ = p.rows.Close()
		_ = os.Remove(name)
		p.rows = nil
	}
}

// PrepareRestore checks the supported header/layout and the entire recoverable
// stream (row widths, encoding, physical marker, counts and EOF). Database value
// constraints and foreign keys are still checked by the restoring transaction.
func PrepareRestore(ctx context.Context, input io.Reader) (*PreparedRestore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(input)
	dec.DisallowUnknownFields()
	var h BackupHeader
	if err := dec.Decode(&h); err != nil {
		return nil, err
	}
	if err := validateBackupLayout(h); err != nil {
		return nil, err
	}
	for _, table := range h.Tables {
		if table.Name == "manager_schema_version" && table.Rows != 1 {
			return nil, errors.New("backup physical schema marker is missing")
		}
	}
	rows, err := prepareRestoreRows(ctx, &h, dec)
	if err != nil {
		return nil, err
	}
	return &PreparedRestore{header: h, rows: rows}, nil
}

// BeginRestore validates the complete stream before any destination DDL, and all
// foreign keys before the caller can commit. Existing rows are replaced in one
// InnoDB transaction. AUTO_INCREMENT is only advanced, never rewound, even if a
// restore rolls back. Offline schema-upgrading callers use PrepareRestore first.
func BeginRestore(ctx context.Context, db *sql.DB, input io.Reader) (*RestoreTransaction, error) {
	prepared, err := PrepareRestore(ctx, input)
	if err != nil {
		return nil, err
	}
	defer prepared.Close()
	return prepared.BeginRestore(ctx, db)
}

func (p *PreparedRestore) BeginRestore(ctx context.Context, db *sql.DB) (result *RestoreTransaction, returnErr error) {
	if p.rows == nil {
		return nil, errors.New("prepared MySQL restore is closed")
	}
	if _, err := p.rows.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	h := p.header
	dec := json.NewDecoder(p.rows)
	current, err := backupLayout(ctx, db)
	if err != nil {
		return nil, err
	}
	if current.SchemaVersion != schemaVersion {
		return nil, errors.New("upgrade the MySQL destination before restoring a backup")
	}
	destination := make(map[string]BackupTable, len(current.Tables))
	for _, table := range current.Tables {
		destination[table.Name] = table
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			resetRestoreConnection(conn)
		}
	}()
	for _, t := range h.Tables {
		if t.NextID > destination[t.Name].NextID {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("alter table %s auto_increment=%d", QuoteIdentifier(t.Name), t.NextID)); err != nil {
				return nil, err
			}
		}
	}
	if _, err := conn.ExecContext(ctx, "set foreign_key_checks=0"); err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			_ = tx.Rollback()
		}
	}()
	// Clear destination-only acceleration rows as well. Never downgrade the
	// physical schema marker by copying its old backup row.
	for _, t := range current.Tables {
		if t.Name == "manager_schema_version" {
			continue
		}
		if _, err := tx.ExecContext(ctx, "delete from "+QuoteIdentifier(t.Name)); err != nil {
			return nil, err
		}
	}
	for _, t := range h.Tables {
		query := "insert into " + QuoteIdentifier(t.Name) + " (" + columnList(t.Columns) + ") values (" + strings.TrimSuffix(strings.Repeat("?,", len(t.Columns)), ",") + ")"
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		for n := int64(0); n < t.Rows; n++ {
			var values [][]byte
			if err := dec.Decode(&values); err != nil {
				stmt.Close()
				return nil, err
			}
			if len(values) != len(t.Columns) {
				stmt.Close()
				return nil, errors.New("backup row width mismatch")
			}
			args := make([]any, len(values))
			for i, v := range values {
				if v != nil {
					args[i] = v
				}
			}
			if t.Name == "manager_schema_version" {
				if t.Rows != 1 || string(values[0]) != "1" || string(values[1]) != fmt.Sprint(h.SchemaVersion) {
					stmt.Close()
					return nil, errors.New("backup physical schema marker is invalid")
				}
				continue
			}
			if _, err := stmt.ExecContext(ctx, args...); err != nil {
				stmt.Close()
				return nil, fmt.Errorf("restore %s row %d: %w", t.Name, n, err)
			}
		}
		if err := stmt.Close(); err != nil {
			return nil, err
		}
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("backup has trailing or corrupt data")
	}
	if h.SchemaVersion < 2 {
		if _, err := tx.ExecContext(ctx, "delete from usage_monitoring_rollup_state where rollup_name in ('stats_hourly_v1','metadata_hourly_v1')"); err != nil {
			return nil, err
		}
	}
	if err := EnsureAccelerationState(ctx, tx); err != nil {
		return nil, err
	}
	if err := validateForeignKeys(ctx, tx); err != nil {
		return nil, err
	}
	return &RestoreTransaction{Tx: tx, conn: conn}, nil
}

// prepareRestoreRows validates each row, retaining the stream on a 0600 temporary
// file rather than in memory. V1 writers used cached AUTO_INCREMENT values, so
// recover their usage_events lower bound from retained IDs/checkpoints as well.
// The caller deletes the spool on every outcome, including failed destination DDL.
func prepareRestoreRows(ctx context.Context, h *BackupHeader, dec *json.Decoder) (file *os.File, err error) {
	file, err = os.CreateTemp("", fmt.Sprintf("cpamp-mysql-v%d-restore-*", h.SchemaVersion))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			file.Close()
			os.Remove(file.Name())
		}
	}()
	enc := json.NewEncoder(file)
	var highest int64
	for _, table := range h.Tables {
		for n := int64(0); n < table.Rows; n++ {
			if err = ctx.Err(); err != nil {
				return file, err
			}
			var values [][]byte
			if err = dec.Decode(&values); err != nil {
				return file, err
			}
			if len(values) != len(table.Columns) {
				return file, errors.New("backup row width mismatch")
			}
			if table.Name == "manager_schema_version" && (string(values[0]) != "1" || string(values[1]) != strconv.Itoa(h.SchemaVersion)) {
				return file, errors.New("backup physical schema marker is invalid")
			}
			for i, col := range table.Columns {
				if h.SchemaVersion != 1 || values[i] == nil || !legacyEventSequenceColumn(table.Name, col) {
					continue
				}
				id, parseErr := strconv.ParseInt(string(values[i]), 10, 64)
				if parseErr != nil {
					return file, fmt.Errorf("invalid event sequence evidence in %s.%s", table.Name, col)
				}
				if id > highest {
					highest = id
				}
			}
			if err = enc.Encode(values); err != nil {
				return file, err
			}
		}
	}
	var extra any
	if err = dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return file, errors.New("backup has trailing or corrupt data")
	}
	if highest == math.MaxInt64 {
		return file, errors.New("backup event sequence is exhausted")
	}
	for i := range h.Tables {
		if h.SchemaVersion == 1 && h.Tables[i].Name == "usage_events" && h.Tables[i].NextID <= highest {
			h.Tables[i].NextID = highest + 1
		}
	}
	_, err = file.Seek(0, io.SeekStart)
	return file, err
}

func legacyEventSequenceColumn(table, column string) bool {
	if table == "usage_events" && column == "id" {
		return true
	}
	if !strings.HasPrefix(table, "usage_") {
		return false
	}
	switch column {
	case "event_id", "raw_event_id", "first_event_id", "last_event_id", "target_event_id", "backfill_last_event_id", "coverage_event_id", "last_archived_event_id", "last_deleted_event_id":
		return true
	}
	return false
}

func validateForeignKeys(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `select table_name,constraint_name,column_name,referenced_table_name,referenced_column_name from information_schema.key_column_usage where table_schema=database() and referenced_table_name is not null order by table_name,constraint_name,ordinal_position`)
	if err != nil {
		return err
	}
	type key struct {
		table, name, parent string
		from, to            []string
	}
	var keys []key
	for rows.Next() {
		var table, name, col, parent, target string
		if err := rows.Scan(&table, &name, &col, &parent, &target); err != nil {
			rows.Close()
			return err
		}
		if len(keys) == 0 || keys[len(keys)-1].table != table || keys[len(keys)-1].name != name {
			keys = append(keys, key{table: table, name: name, parent: parent})
		}
		k := &keys[len(keys)-1]
		k.from = append(k.from, col)
		k.to = append(k.to, target)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, k := range keys {
		var joins, nonnull []string
		for i, col := range k.from {
			joins = append(joins, "c."+QuoteIdentifier(col)+"=p."+QuoteIdentifier(k.to[i]))
			nonnull = append(nonnull, "c."+QuoteIdentifier(col)+" is not null")
		}
		q := "select exists(select 1 from " + QuoteIdentifier(k.table) + " c left join " + QuoteIdentifier(k.parent) + " p on " + strings.Join(joins, " and ") + " where " + strings.Join(nonnull, " and ") + " and p." + QuoteIdentifier(k.to[0]) + " is null)"
		var bad bool
		if err := tx.QueryRowContext(ctx, q).Scan(&bad); err != nil {
			return err
		}
		if bad {
			return fmt.Errorf("backup violates foreign key %s.%s", k.table, k.name)
		}
	}
	return nil
}

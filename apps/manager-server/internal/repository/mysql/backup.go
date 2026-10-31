package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
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
	if h.SchemaVersion != schemaVersion {
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
	return h, nil
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
	_, _ = r.conn.ExecContext(context.Background(), "set foreign_key_checks=1")
	_ = r.conn.Close()
}

// BeginRestore validates the complete stream and all foreign keys before the
// caller can commit. Existing rows are replaced in one InnoDB transaction.
// AUTO_INCREMENT is only advanced, never rewound, even if a restore rolls back.
func BeginRestore(ctx context.Context, db *sql.DB, input io.Reader) (result *RestoreTransaction, returnErr error) {
	dec := json.NewDecoder(input)
	dec.DisallowUnknownFields()
	var h BackupHeader
	if err := dec.Decode(&h); err != nil {
		return nil, err
	}
	current, err := backupLayout(ctx, db)
	if err != nil {
		return nil, err
	}
	if h.Version != 1 || h.SchemaVersion != current.SchemaVersion || len(h.Tables) != len(current.Tables) {
		return nil, errors.New("backup schema does not match this Manager version")
	}
	for i, t := range h.Tables {
		c := current.Tables[i]
		if t.Name != c.Name || !reflect.DeepEqual(t.Columns, c.Columns) || t.Rows < 0 || t.NextID < 0 {
			return nil, errors.New("backup table layout does not match the target database")
		}
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			_, _ = conn.ExecContext(context.Background(), "set foreign_key_checks=1")
			_ = conn.Close()
		}
	}()
	for i, t := range h.Tables {
		if t.NextID > current.Tables[i].NextID {
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
	for _, t := range h.Tables {
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
	if err := validateForeignKeys(ctx, tx); err != nil {
		return nil, err
	}
	return &RestoreTransaction{Tx: tx, conn: conn}, nil
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

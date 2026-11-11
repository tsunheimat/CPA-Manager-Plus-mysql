package mysql

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

//go:embed migrations/002_monitoring_hourly.sql
var hourlySchema string

// SchemaExecutor permits native administrative connections, runtime pools and
// already-admitted transactions. Seeds never overwrite durable progress.
type SchemaExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func EnsureAccelerationState(ctx context.Context, executor SchemaExecutor) error {
	for _, name := range []string{"stats_hourly_v1", "metadata_hourly_v1"} {
		_, err := sqlwrite.ExecContext(ctx, executor, `insert into usage_monitoring_rollup_state
			(rollup_name,schema_version,structure_revision,status,backfill_last_event_id,coverage_event_id,target_event_id,processed_events,updated_at_ms)
			values (?,1,'','pending',0,0,0,0,0) on duplicate key update rollup_name=rollup_name`, name)
		if err != nil {
			return fmt.Errorf("initialize MySQL hourly state: %w", err)
		}
	}
	return nil
}

func executeSchema(ctx context.Context, conn *sql.Conn, script string) error {
	for i, part := range strings.Split(script, "-- statement")[1:] {
		if query := strings.TrimSpace(part); query != "" {
			if _, err := conn.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("MySQL schema statement %d: %w", i+1, err)
			}
		}
	}
	return nil
}

// Called only under the schema advisory lock. DDL auto-commits, so CREATE IF NOT
// EXISTS plus verified postconditions is the restart boundary, not a transaction.
// A completed marker is never permission to silently recreate a lost table.
func upgradeSchema(ctx context.Context, conn *sql.Conn, current int) error {
	if current < 2 {
		if err := executeSchema(ctx, conn, hourlySchema); err != nil {
			return err
		}
	}
	if err := validateHourlySchema(ctx, conn); err != nil {
		return err
	}
	if err := EnsureAccelerationState(ctx, conn); err != nil {
		return err
	}
	if current < 2 {
		result, err := conn.ExecContext(ctx, "update manager_schema_version set version=2 where id=1 and version=1")
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fmt.Errorf("MySQL schema marker changed during migration")
		}
	}
	return nil
}

type schemaColumn struct {
	name, kind, generated string
	notNull               bool
	defaultValue          sql.NullString
}
type schemaIndex struct {
	name    string
	unique  bool
	columns []string
}
type schemaTable struct {
	name    string
	columns []schemaColumn
	indexes []schemaIndex
}

// Read the checked-in native DDL, not SQLite's evolving schema. This also defines
// exact historical backup layouts; generated columns never appear in row streams.
func schemaTables(script string) ([]schemaTable, error) {
	var tables []schemaTable
	for _, part := range strings.Split(script, "-- statement")[1:] {
		tokens, err := lex(part)
		if err != nil {
			return nil, err
		}
		if len(tokens) < 8 || !equal(tokens[0], "create") || !equal(tokens[1], "table") {
			continue
		}
		if !equal(tokens[2], "if") || tokens[6] != "(" {
			return nil, fmt.Errorf("unexpected schema table statement")
		}
		t := schemaTable{name: strings.Trim(tokens[5], "`")}
		end := closing(tokens, 6)
		if end < 0 {
			return nil, fmt.Errorf("invalid table %s", t.name)
		}
		for _, def := range split(tokens[7:end]) {
			if len(def) < 2 {
				return nil, fmt.Errorf("invalid definition in %s", t.name)
			}
			if strings.HasPrefix(def[0], "`") {
				c := schemaColumn{name: strings.Trim(def[0], "`"), kind: strings.ToLower(def[1]), notNull: topSequence(def, "not", "null") >= 0}
				if len(def) > 2 && def[2] == "(" {
					end := closing(def, 2)
					c.kind += strings.Join(def[2:end+1], "")
				}
				if at := topSequence(def, "generated", "always", "as"); at >= 0 {
					end := closing(def, at+3)
					if end < 0 {
						return nil, fmt.Errorf("invalid generated column %s", c.name)
					}
					c.generated = strings.Join(def[at+4:end], " ")
				}
				if at := topSequence(def, "default"); at >= 0 && at+1 < len(def) {
					value := def[at+1:]
					if value[0] == "(" {
						value = value[1:closing(value, 0)]
					}
					c.defaultValue = sql.NullString{Valid: true, String: strings.Trim(value[0], "'")}
				}
				t.columns = append(t.columns, c)
			} else if equal(def[0], "key") || equal(def[0], "unique") {
				i := 1
				idx := schemaIndex{unique: equal(def[0], "unique")}
				if idx.unique {
					i++
				}
				idx.name = strings.Trim(def[i], "`")
				for _, col := range split(def[i+2 : closing(def, i+1)]) {
					idx.columns = append(idx.columns, strings.Trim(col[0], "`"))
				}
				t.indexes = append(t.indexes, idx)
			}
		}
		tables = append(tables, t)
	}
	return tables, nil
}

// MySQL canonicalizes casts, literal charset introducers and the OCTET_LENGTH
// alias in generation_expression. Preserve function argument nesting: stripping
// all parentheses could mistake differently grouped digest frames for the same
// expression. Only redundant whole-expression parentheses may be removed.
func canonicalGeneration(s string) string {
	tokens, err := lex(s)
	if err != nil {
		return s
	}
	var normalized []string
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		if (equal(token, "_utf8mb4") || equal(token, "_utf8mb3")) && i+1 < len(tokens) && strings.HasPrefix(tokens[i+1], "'") {
			continue
		}
		if equal(token, "character") && i+1 < len(tokens) && equal(tokens[i+1], "set") {
			token = "charset"
			i++
		}
		if equal(token, "octet_length") && i+1 < len(tokens) && tokens[i+1] == "(" {
			token = "length" // Both count bytes in native MySQL, unlike char_length.
		}
		if !strings.HasPrefix(token, "'") {
			token = strings.ToLower(strings.Trim(token, "`"))
		}
		normalized = append(normalized, token)
	}
	return canonicalTokens(normalized)
}

func canonicalTokens(tokens []string) string {
	for len(tokens) > 1 && tokens[0] == "(" && closing(tokens, 0) == len(tokens)-1 {
		tokens = tokens[1 : len(tokens)-1]
	}
	var out []string
	for i := 0; i < len(tokens); i++ {
		if tokens[i] == "(" {
			end := closing(tokens, i)
			if end < 0 {
				return strings.Join(tokens, " ")
			}
			out = append(out, "(", canonicalTokens(tokens[i+1:end]), ")")
			i = end
		} else {
			out = append(out, tokens[i])
		}
	}
	return strings.Join(out, " ")
}

func validateHourlySchema(ctx context.Context, conn *sql.Conn) error {
	tables, err := schemaTables(hourlySchema)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := validateTable(ctx, conn, t); err != nil {
			return fmt.Errorf("MySQL schema drift in %s: %w", t.name, err)
		}
	}
	return nil
}

func validateTable(ctx context.Context, conn *sql.Conn, t schemaTable) error {
	var engine, collation string
	if err := conn.QueryRowContext(ctx, `select engine,table_collation from information_schema.tables where table_schema=database() and table_name=? and table_type='BASE TABLE'`, t.name).Scan(&engine, &collation); err != nil {
		return err
	}
	if engine != "InnoDB" || collation != "utf8mb4_0900_bin" {
		return fmt.Errorf("unexpected engine or collation")
	}
	rows, err := conn.QueryContext(ctx, `select column_name,column_type,is_nullable,column_default,generation_expression,extra,coalesce(collation_name,'') from information_schema.columns where table_schema=database() and table_name=? order by ordinal_position`, t.name)
	if err != nil {
		return err
	}
	i := 0
	for rows.Next() {
		var name, kind, nullable, generated, extra, collation string
		var def sql.NullString
		if err := rows.Scan(&name, &kind, &nullable, &def, &generated, &extra, &collation); err != nil {
			rows.Close()
			return err
		}
		if i >= len(t.columns) {
			rows.Close()
			return fmt.Errorf("unexpected column %s", name)
		}
		c := t.columns[i]
		i++
		if def.Valid && c.defaultValue.Valid && c.defaultValue.String == "" && canonicalGeneration(def.String) == "''" {
			def.String = ""
		}
		if name != c.name || kind != c.kind || (nullable == "NO") != c.notNull || def != c.defaultValue || canonicalGeneration(generated) != canonicalGeneration(c.generated) ||
			(c.generated != "" && extra != "STORED GENERATED") || (c.generated == "" && extra != "" && extra != "DEFAULT_GENERATED") ||
			(c.kind == "longtext" && collation != "utf8mb4_0900_bin") {
			rows.Close()
			return fmt.Errorf("incompatible column %s", name)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if i != len(t.columns) {
		return fmt.Errorf("missing columns")
	}
	rows, err = conn.QueryContext(ctx, `select index_name,non_unique,column_name,sub_part,collation,index_type,is_visible from information_schema.statistics where table_schema=database() and table_name=? order by index_name,seq_in_index`, t.name)
	if err != nil {
		return err
	}
	var actual []schemaIndex
	for rows.Next() {
		var name, col, direction, kind, visible string
		var nonUnique int
		var prefix sql.NullInt64
		if err := rows.Scan(&name, &nonUnique, &col, &prefix, &direction, &kind, &visible); err != nil {
			rows.Close()
			return err
		}
		if prefix.Valid || direction != "A" || kind != "BTREE" || visible != "YES" {
			rows.Close()
			return fmt.Errorf("incompatible index %s", name)
		}
		if len(actual) == 0 || actual[len(actual)-1].name != name {
			actual = append(actual, schemaIndex{name: name, unique: nonUnique == 0})
		}
		actual[len(actual)-1].columns = append(actual[len(actual)-1].columns, col)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	sort.Slice(t.indexes, func(i, j int) bool { return t.indexes[i].name < t.indexes[j].name })
	if !reflect.DeepEqual(actual, t.indexes) {
		return fmt.Errorf("index definitions differ")
	}
	return nil
}

func versionedBackupLayout(version int) ([]BackupTable, error) {
	if version < 1 || version > schemaVersion {
		return nil, fmt.Errorf("unsupported backup schema version %d", version)
	}
	script := schema
	if version >= 2 {
		script += "\n" + hourlySchema
	}
	tables, err := schemaTables(script)
	if err != nil {
		return nil, err
	}
	out := []BackupTable{{Name: "manager_schema_version", Columns: []string{"id", "version"}}}
	for _, t := range tables {
		b := BackupTable{Name: t.name}
		for _, c := range t.columns {
			if c.generated == "" {
				b.Columns = append(b.Columns, c.name)
			}
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

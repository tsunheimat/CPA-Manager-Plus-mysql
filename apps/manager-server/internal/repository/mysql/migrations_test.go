package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestHourlySchemaContractAndFrozenBaseline(t *testing.T) {
	const frozenV1 = "942bd09ece935eea2cec002f66178a3a1a8322c03e127a4266434bafec67f418"
	if fmt.Sprintf("%x", sha256.Sum256([]byte(schema))) != frozenV1 {
		t.Fatal("v1 schema is immutable; add a numbered migration instead")
	}
	base, err := schemaTables(schema)
	if err != nil {
		t.Fatal(err)
	}
	hourly, err := schemaTables(hourlySchema)
	if err != nil {
		t.Fatal(err)
	}
	if len(hourly) != 3 {
		t.Fatalf("hourly tables: %d", len(hourly))
	}
	byName := map[string]schemaTable{}
	for _, table := range base {
		byName[table.name] = table
	}
	for _, table := range hourly {
		daily := byName[strings.Replace(table.name, "_hourly_", "_daily_", 1)]
		var have, want []schemaColumn
		for _, column := range table.columns {
			if column.generated == "" {
				have = append(have, column)
			}
		}
		for _, column := range daily.columns {
			if column.generated == "" {
				want = append(want, column)
			}
		}
		if !reflect.DeepEqual(have, want) {
			t.Fatalf("writable layout differs: %s", table.name)
		}
		digests := map[string]bool{}
		for _, column := range table.columns {
			if column.generated == "" {
				continue
			}
			digests[column.name] = true
			if column.kind != "binary(32)" {
				t.Fatalf("wide digest %s.%s", table.name, column.name)
			}
			if strings.Contains(column.generated, " length ") || strings.Contains(column.generated, "char_length") {
				t.Fatal("digest framing must count UTF-8 bytes")
			}
			if column.name == "_identity_hash" {
				for _, dimension := range have {
					if dimension.name == "updated_at_ms" || dimension.name == "calls" {
						break
					}
					if dimension.name == "source" && strings.Contains(table.name, "selector") {
						continue
					} // source_hash is the selector's historical grain
					if !strings.Contains(column.generated, "`"+dimension.name+"`") {
						t.Fatalf("missing grain %s.%s", table.name, dimension.name)
					}
				}
			}
		}
		for _, name := range []string{"_identity_hash", "_revision_hash", "_api_key_hash"} {
			if !digests[name] {
				t.Fatalf("missing %s.%s", table.name, name)
			}
		}
		if strings.Contains(table.name, "_account_") {
			for _, name := range []string{"_credential_hash", "_legacy_hash"} {
				if !digests[name] {
					t.Fatalf("missing %s", name)
				}
			}
		}
	}
	// A historical v1 backup layout remains independent of later SQLite changes.
	v1, err := versionedBackupLayout(1)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := versionedBackupLayout(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(v2) != len(v1)+3 {
		t.Fatalf("unexpected migration delta: %d -> %d", len(v1), len(v2))
	}
	for _, table := range v1 {
		if strings.Contains(table.Name, "monitoring") && strings.Contains(table.Name, "hourly") {
			t.Fatal("v1 baseline changed")
		}
	}
	t.Logf("frozen v1 schema digest: %x", sha256.Sum256([]byte(schema)))
}

func TestVersionedBackupLayoutValidation(t *testing.T) {
	for _, version := range []int{1, 2} {
		tables, err := versionedBackupLayout(version)
		if err != nil {
			t.Fatal(err)
		}
		h := BackupHeader{Version: 1, SchemaVersion: version, Tables: tables}
		if err := validateBackupLayout(h); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*BackupHeader){
			func(h *BackupHeader) { h.Version = 2 }, func(h *BackupHeader) { h.SchemaVersion = 99 },
			func(h *BackupHeader) { h.Tables = h.Tables[1:] }, func(h *BackupHeader) { h.Tables[0].Name = "arbitrary_table" },
			func(h *BackupHeader) { h.Tables[0].Columns = []string{"wrong"} }, func(h *BackupHeader) { h.Tables[0].Rows = -1 },
		} {
			data, _ := json.Marshal(h)
			var bad BackupHeader
			json.Unmarshal(data, &bad)
			mutate(&bad)
			if err := validateBackupLayout(bad); err == nil {
				t.Fatalf("accepted malformed header: %+v", bad)
			}
			var input bytes.Buffer
			json.NewEncoder(&input).Encode(bad)
			// nil db proves invalid layouts are rejected before consulting or
			// mutating the destination, let alone replacing any rows.
			if _, err := BeginRestore(context.Background(), nil, &input); err == nil {
				t.Fatal("invalid header accepted")
			}
		}
	}
}

type migrationBackend struct {
	mu                 sync.Mutex
	tables             map[string]schemaTable
	states             map[string]string
	version            int
	failAfter, creates int
	seedError          bool
	// Literal information_schema rows override the DDL-derived state-machine fake.
	// Metadata contract tests must not derive both sides from schemaTables.
	columnMetadata map[[2]string][]driver.Value
}
type migrationConnector struct{ backend *migrationBackend }

func (m migrationConnector) Driver() driver.Driver { return admissionDriver{} }
func (m migrationConnector) Connect(context.Context) (driver.Conn, error) {
	return &migrationConn{m.backend}, nil
}

type migrationConn struct{ backend *migrationBackend }

func (*migrationConn) Close() error { return nil }
func (*migrationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*migrationConn) Begin() (driver.Tx, error) {
	return nil, errors.New("DDL must not be wrapped in a transaction")
}
func (c *migrationConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	b := c.backend
	b.mu.Lock()
	defer b.mu.Unlock()
	if strings.HasPrefix(strings.TrimSpace(q), "create table") {
		b.creates++
		if b.failAfter > 0 && b.creates > b.failAfter {
			return nil, errors.New("interrupted DDL")
		}
		tables, err := schemaTables("-- statement\n" + q)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			if _, ok := b.tables[table.name]; !ok {
				b.tables[table.name] = table
			}
		}
	} else if strings.Contains(q, "insert into usage_monitoring_rollup_state") {
		if b.seedError {
			return nil, errors.New("seed failure")
		}
		name := args[0].Value.(string)
		if _, ok := b.states[name]; !ok {
			b.states[name] = "pending"
		}
	} else if strings.Contains(q, "update manager_schema_version") {
		if b.version != 1 {
			return driver.RowsAffected(0), nil
		}
		b.version = 2
	} else {
		return nil, fmt.Errorf("unexpected migration exec %s", q)
	}
	return driver.RowsAffected(1), nil
}
func (c *migrationConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	b := c.backend
	b.mu.Lock()
	defer b.mu.Unlock()
	table, ok := b.tables[args[0].Value.(string)]
	if !ok {
		return &admissionRows{cols: []string{"engine", "collation"}}, nil
	}
	if strings.Contains(q, "information_schema.tables") {
		return &admissionRows{cols: []string{"engine", "collation"}, values: [][]driver.Value{{"InnoDB", "utf8mb4_0900_bin"}}}, nil
	}
	if strings.Contains(q, "information_schema.columns") {
		r := &admissionRows{cols: []string{"name", "kind", "nullable", "default", "generation", "extra", "collation"}}
		for _, col := range table.columns {
			if metadata, ok := b.columnMetadata[[2]string{table.name, col.name}]; ok {
				r.values = append(r.values, metadata)
				continue
			}
			nullable := "YES"
			if col.notNull {
				nullable = "NO"
			}
			extra := ""
			if col.generated != "" {
				extra = "STORED GENERATED"
			}
			var def driver.Value
			if col.defaultValue.Valid {
				def = col.defaultValue.String
			}
			collation := ""
			if col.kind == "longtext" {
				collation = "utf8mb4_0900_bin"
			}
			r.values = append(r.values, []driver.Value{col.name, col.kind, nullable, def, col.generated, extra, collation})
		}
		return r, nil
	}
	if strings.Contains(q, "information_schema.statistics") {
		r := &admissionRows{cols: []string{"name", "non_unique", "column", "prefix", "collation", "type", "visible"}}
		indexes := append([]schemaIndex(nil), table.indexes...)
		// SQL's ORDER BY is part of the schema inspection contract.
		for i := range indexes {
			for j := i + 1; j < len(indexes); j++ {
				if indexes[j].name < indexes[i].name {
					indexes[i], indexes[j] = indexes[j], indexes[i]
				}
			}
		}
		for _, idx := range indexes {
			nonUnique := int64(1)
			if idx.unique {
				nonUnique = 0
			}
			for _, col := range idx.columns {
				r.values = append(r.values, []driver.Value{idx.name, nonUnique, col, nil, "A", "BTREE", "YES"})
			}
		}
		return r, nil
	}
	return nil, fmt.Errorf("unexpected schema inspection %s", q)
}
func migrationDB(t *testing.T) (*sql.Conn, *migrationBackend) {
	t.Helper()
	b := &migrationBackend{tables: map[string]schemaTable{}, states: map[string]string{}, version: 1}
	db := sql.OpenDB(migrationConnector{b})
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, b
}
func TestMigrationRestartGuardsAndNoInheritedCoverage(t *testing.T) {
	ctx := context.Background()
	conn, b := migrationDB(t)
	b.failAfter = 1
	b.states["stats_v1"] = "complete:retained-coverage"
	if err := upgradeSchema(ctx, conn, 1); err == nil {
		t.Fatal("interrupted migration succeeded")
	}
	if b.version != 1 || len(b.tables) != 1 {
		t.Fatalf("premature marker %+v", b)
	}
	b.failAfter = 0
	b.seedError = true
	if err := upgradeSchema(ctx, conn, 1); err == nil || b.version != 1 {
		t.Fatal("seed error published version")
	}
	b.seedError = false
	if err := upgradeSchema(ctx, conn, 1); err != nil {
		t.Fatal(err)
	}
	if b.version != 2 || b.states["stats_hourly_v1"] != "pending" || b.states["metadata_hourly_v1"] != "pending" || b.states["stats_v1"] != "complete:retained-coverage" {
		t.Fatalf("state initialization: %+v", b.states)
	}
	b.states["stats_hourly_v1"] = "complete:hourly-coverage"
	creates := b.creates
	if err := upgradeSchema(ctx, conn, 2); err != nil {
		t.Fatal(err)
	}
	if b.creates != creates || b.states["stats_hourly_v1"] != "complete:hourly-coverage" {
		t.Fatal("restart erased progress or ran DDL")
	}
	delete(b.tables, "usage_monitoring_account_hourly_rollups_v1")
	if err := upgradeSchema(ctx, conn, 2); err == nil {
		t.Fatal("completed marker hid lost table")
	}
}
func TestMigrationRejectsUnexpectedMarkerChange(t *testing.T) {
	conn, backend := migrationDB(t)
	backend.version = 3
	if err := upgradeSchema(context.Background(), conn, 1); err == nil || !strings.Contains(err.Error(), "marker changed") {
		t.Fatalf("unexpected migration marker accepted: %v", err)
	}
	if backend.version != 3 {
		t.Fatal("migration overwrote concurrent schema marker")
	}
}

func TestGenerationCanonicalizationPreservesFullGrain(t *testing.T) {
	for _, pair := range [][2]string{
		{"unhex(sha2(concat(octet_length(cast(`key` as char character set utf8mb4)),':',`key`),256))", "(unhex(sha2(concat(length(cast(`key` as char charset utf8mb4)),_utf8mb4':',`key`),256)))"},
		{"unhex(sha2(`structure_revision`,256))", "(unhex(sha2(`structure_revision`,256)))"},
	} {
		if canonicalGeneration(pair[0]) != canonicalGeneration(pair[1]) {
			t.Fatalf("equivalent server expression rejected: %q != %q", canonicalGeneration(pair[0]), canonicalGeneration(pair[1]))
		}
	}
	for _, expression := range []string{
		"unhex(sha2(concat(char_length(k),':',k),256))",
		"unhex(sha2(concat(length(k),';',k),256))",
		"unhex(sha2(concat(length(trim(k)),':',k),256))",
		"unhex(sha2(concat(length(k),concat(':',k)),256))",
	} {
		if canonicalGeneration(expression) == canonicalGeneration("unhex(sha2(concat(octet_length(k),':',k),256))") {
			t.Fatalf("incompatible digest expression accepted: %s", expression)
		}
	}
}

func TestMigrationRejectsColumnAndIndexDrift(t *testing.T) {
	for _, mode := range []string{"column", "generation", "index", "missing"} {
		t.Run(mode, func(t *testing.T) {
			conn, b := migrationDB(t)
			tables, err := schemaTables(hourlySchema)
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range tables {
				b.tables[table.name] = table
			}
			table := b.tables[tables[0].name]
			switch mode {
			case "column":
				table.columns[0].kind = "varchar(64)"
			case "generation":
				for i := range table.columns {
					if table.columns[i].name == "_credential_hash" {
						table.columns[i].generated = "unhex(sha2(auth_index,256))"
					}
				}
			case "index":
				table.indexes[1].columns = []string{"bucket_ms"}
			case "missing":
				table.columns = table.columns[:len(table.columns)-1]
			}
			b.tables[table.name] = table
			if err := upgradeSchema(context.Background(), conn, 1); err == nil {
				t.Fatal("schema drift accepted")
			}
			if b.version != 1 {
				t.Fatal("schema drift published version")
			}
		})
	}
}
